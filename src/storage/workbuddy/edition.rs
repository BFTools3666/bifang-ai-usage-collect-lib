//! 独立 WorkBuddy 版本的根发现持久化和安全索引装配。

use super::{
    WorkbuddyIndex, WorkbuddyIndexOutcome, WorkbuddyRefreshMode, database_error,
    incompatible_refresh, schema::statement, source::source_error,
};
use crate::readers::{
    current_user_home,
    workbuddy::discovery::{discover_edition_sources, edition_source, resolve_edition_source},
};
use crate::storage::path_codec::{decode_path, encode_path};
use crate::{
    CancellationSignal, LocalError, RootDiscoveryScope, WorkbuddyEdition, WorkbuddyScanSource,
};
use sea_orm::{ConnectionTrait, TransactionTrait};
use std::{path::Path, sync::Arc};

#[cfg(test)]
#[path = "edition_tests.rs"]
mod tests;

impl WorkbuddyIndex {
    /// 先发现指定版本的新根，再用同一版本的已登记根更新派生统计。
    /// # Errors
    /// 版本不匹配、范围不支持、发现或索引失败时返回脱敏错误。
    pub async fn refresh_edition<C>(
        &mut self,
        edition: WorkbuddyEdition,
        scope: RootDiscoveryScope,
        since_epoch_ms: i64,
        mode: WorkbuddyRefreshMode,
        cancellation: C,
    ) -> Result<WorkbuddyIndexOutcome, LocalError>
    where
        C: CancellationSignal + Clone + 'static,
    {
        if scope == RootDiscoveryScope::ManualSubtree {
            return Err(incompatible_refresh());
        }
        self.refresh_edition_with_discovery(
            edition,
            since_epoch_ms,
            mode,
            cancellation,
            move |cancel| {
                discover_edition_sources(edition, scope, current_user_home().as_deref(), cancel)
            },
        )
        .await
    }

    /// 从调用者指定的用户目录核验内置根并增量更新同版已登记来源。
    /// # Errors
    /// 不可安全读取的目录或存储失败保留旧发布统计。
    pub async fn refresh_edition_from_user_home<C>(
        &mut self,
        edition: WorkbuddyEdition,
        user_home: &Path,
        since_epoch_ms: i64,
        mode: WorkbuddyRefreshMode,
        cancellation: C,
    ) -> Result<WorkbuddyIndexOutcome, LocalError>
    where
        C: CancellationSignal + Clone + 'static,
    {
        let home = user_home.to_path_buf();
        self.refresh_edition_with_discovery(
            edition,
            since_epoch_ms,
            mode,
            cancellation,
            move |cancel| {
                discover_edition_sources(
                    edition,
                    RootDiscoveryScope::UserPriority,
                    Some(&home),
                    cancel,
                )
            },
        )
        .await
    }

    /// 发现命中先独立登记，给来源页保留每个根；解析失败也不会擦除登记。
    async fn refresh_edition_with_discovery<C, F>(
        &mut self,
        edition: WorkbuddyEdition,
        since_epoch_ms: i64,
        mode: WorkbuddyRefreshMode,
        cancellation: C,
        discover: F,
    ) -> Result<WorkbuddyIndexOutcome, LocalError>
    where
        C: CancellationSignal + Clone + 'static,
        F: FnOnce(Arc<dyn CancellationSignal>) -> Result<Vec<WorkbuddyScanSource>, LocalError>
            + Send
            + 'static,
    {
        if self.edition != Some(edition) {
            return Err(incompatible_refresh());
        }
        let refresh_result = self
            .prepare_edition_sources(edition, &cancellation, discover)
            .await;
        let (sources, roots_discovered) = match refresh_result {
            Ok(prepared) => prepared,
            Err(error) => {
                self.record_failure(&[]).await?;
                return Err(error);
            }
        };
        let mut outcome = self
            .refresh_sources(&sources, since_epoch_ms, mode, cancellation)
            .await?;
        outcome.roots_discovered = roots_discovered;
        Ok(outcome)
    }

    /// 发现完成后登记候选，再在 blocking 岛复核所有已登记根的读边界。
    async fn prepare_edition_sources<C, F>(
        &self,
        edition: WorkbuddyEdition,
        cancellation: &C,
        discover: F,
    ) -> Result<(Vec<WorkbuddyScanSource>, u64), LocalError>
    where
        C: CancellationSignal + Clone + 'static,
        F: FnOnce(Arc<dyn CancellationSignal>) -> Result<Vec<WorkbuddyScanSource>, LocalError>
            + Send
            + 'static,
    {
        let worker_cancellation = cancellation.clone();
        let discovered =
            tokio::task::spawn_blocking(move || discover(Arc::new(worker_cancellation)))
                .await
                .map_err(|_| source_error())??;
        let roots_discovered = discovered.len() as u64;
        self.register_edition_sources(edition, &discovered, cancellation)
            .await?;
        let registered = self.sources().await?;
        let worker_cancellation = cancellation.clone();
        let readable = tokio::task::spawn_blocking(move || {
            let mut readable = Vec::new();
            for source in registered {
                if worker_cancellation.is_cancelled() {
                    return Err(source_error());
                }
                if let Some(verified) = resolve_edition_source(&source.path, edition, false)? {
                    if verified.root_id != source.root_id {
                        return Err(source_error());
                    }
                    readable.push(source);
                }
            }
            Ok(readable)
        })
        .await
        .map_err(|_| source_error())??;
        Ok((readable, roots_discovered))
    }

    /// 读取独立版本的所有已发现根。返回的原生路径不得进入界面或日志。
    /// # Errors
    /// 旧共享库没有 registry 或存储校验失败时返回错误。
    pub async fn sources(&self) -> Result<Vec<WorkbuddyScanSource>, LocalError> {
        Ok(self
            .sources_with_discovery()
            .await?
            .into_iter()
            .map(|(source, _)| source)
            .collect())
    }

    /// 读取每个已登记根及其是否为内置位置的实际登记值。
    /// # Errors
    /// 旧共享库没有 registry 或存储校验失败时返回错误。
    pub async fn sources_with_discovery(
        &self,
    ) -> Result<Vec<(WorkbuddyScanSource, bool)>, LocalError> {
        self.sources_on(&self.connection, false).await
    }

    /// 从当前事务读取来源身份；已索引标记与调用统计同一事务发布。
    pub(super) async fn sources_on<C: ConnectionTrait>(
        &self,
        connection: &C,
        indexed_only: bool,
    ) -> Result<Vec<(WorkbuddyScanSource, bool)>, LocalError> {
        let edition = self.edition.ok_or_else(incompatible_refresh)?;
        let rows = connection.query_all_raw(statement(
            "SELECT root_id, CASE WHEN length(access_path)<=131072 THEN access_path END, built_in
             FROM workbuddy_roots WHERE (?1=0 OR indexed=1) ORDER BY built_in DESC, root_id",
            vec![i64::from(indexed_only).into()],
        )).await.map_err(database_error)?;
        rows.into_iter()
            .map(|row| {
                let id: String = row.try_get_by_index(0).map_err(database_error)?;
                let path_bytes: Vec<u8> = row.try_get_by_index(1).map_err(database_error)?;
                let built_in: i64 = row.try_get_by_index(2).map_err(database_error)?;
                let path = decode_path(&path_bytes).ok_or_else(source_error)?;
                if !path.is_absolute()
                    || !edition.matches_directory(&path)
                    || !matches!(built_in, 0 | 1)
                {
                    return Err(source_error());
                }
                let source = edition_source(path, built_in == 1);
                if source.root_id != id {
                    return Err(source_error());
                }
                Ok((source, built_in == 1))
            })
            .collect()
    }

    /// 只按规范化根 ID 登记相同认证路径，保持不同路径分别存在。
    async fn register_edition_sources<C: CancellationSignal + ?Sized>(
        &self,
        edition: WorkbuddyEdition,
        discovered: &[WorkbuddyScanSource],
        cancellation: &C,
    ) -> Result<(), LocalError> {
        if cancellation.is_cancelled() {
            return Err(source_error());
        }
        if discovered.is_empty() {
            return Ok(());
        }
        let transaction = self.connection.begin().await.map_err(database_error)?;
        for source in discovered {
            if cancellation.is_cancelled() {
                transaction.rollback().await.map_err(database_error)?;
                return Err(source_error());
            }
            if !source.path.is_absolute() || !edition.matches_directory(&source.path) {
                transaction.rollback().await.map_err(database_error)?;
                return Err(source_error());
            }
            let access_path = encode_path(&source.path);
            let built_in = source
                .path
                .file_name()
                .is_some_and(|name| name == source.alias.as_str());
            let registered = transaction.execute_raw(statement(
                "INSERT INTO workbuddy_roots(root_id, access_path, built_in, indexed) VALUES (?1,?2,?3,0)
                 ON CONFLICT(root_id) DO UPDATE SET built_in=MAX(built_in, excluded.built_in)
                 WHERE access_path=excluded.access_path",
                vec![source.root_id.clone().into(), access_path.into(), i64::from(built_in).into()],
            )).await.map_err(database_error)?;
            if registered.rows_affected() != 1 {
                transaction.rollback().await.map_err(database_error)?;
                return Err(source_error());
            }
        }
        if cancellation.is_cancelled() {
            transaction.rollback().await.map_err(database_error)?;
            return Err(source_error());
        }
        transaction.commit().await.map_err(database_error)
    }

    /// 标记本次成功索引的所有来源，未索引来源仍留在发现列表。
    pub(super) async fn mark_indexed_roots<'a, C: CancellationSignal + ?Sized>(
        transaction: &sea_orm::DatabaseTransaction,
        root_ids: impl Iterator<Item = &'a String> + Send,
        cancellation: &C,
    ) -> Result<(), LocalError> {
        transaction
            .execute_raw(statement(
                "UPDATE workbuddy_roots SET indexed=0 WHERE indexed=1",
                vec![],
            ))
            .await
            .map_err(database_error)?;
        for root_id in root_ids {
            if cancellation.is_cancelled() {
                return Err(source_error());
            }
            let result = transaction
                .execute_raw(statement(
                    "UPDATE workbuddy_roots SET indexed=1 WHERE root_id=?1",
                    vec![root_id.clone().into()],
                ))
                .await
                .map_err(database_error)?;
            if result.rows_affected() != 1 {
                return Err(source_error());
            }
        }
        Ok(())
    }
}
