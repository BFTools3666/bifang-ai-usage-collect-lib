//! WorkBuddy 独立派生数据库：按完整行增量提交两根统计，不接管其他产品数据库。
mod schema;
mod snapshot;
mod source;
#[cfg(test)]
mod tests;

use crate::{
    CancellationSignal, CoverageReport, CoverageState, LocalError, LocalErrorKind,
    WorkbuddyScanSource,
};
use schema::statement;
use sea_orm::{ConnectionTrait, DatabaseConnection, TransactionTrait};
use serde::{Deserialize, Serialize};
use source::{Checkpoint, PreparedUpdate, source_error};
use std::{collections::BTreeMap, path::Path};

/// 一份独立 WorkBuddy 派生库连接，所有写入由宿主单一 writer 持有。
pub struct WorkbuddyIndex {
    connection: DatabaseConnection,
}
impl std::fmt::Debug for WorkbuddyIndex {
    /// 不输出数据库路径或连接选项。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkbuddyIndex").finish_non_exhaustive()
    }
}
/// 同一次发布的覆盖与固定根标签，数据库读取不再检查原始 JSONL。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SnapshotState {
    coverage: CoverageReport,
    root_aliases: BTreeMap<String, String>,
}
/// 显式区分增量续读与用户要求的全量重新索引。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkbuddyRefreshMode {
    /// 复用完整行检查点，仅解析变化的来源。
    Incremental,
    /// 从当前原始文件重新生成保留窗口内的全部安全事实。
    Rebuild,
}
/// 增量运行的实际计数，不把枚举或检查当作内容重读。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkbuddyIndexOutcome {
    /// 本轮枚举到的普通来源文件数。
    pub files_scanned: u64,
    /// 无需重新读取内容的文件数。
    pub unchanged_files: u64,
    /// 从头重建计量与检查点的文件数。
    pub rebuilt_files: u64,
    /// 本轮实际交给流式解析器的来源字节数。
    pub bytes_read: u64,
}
impl WorkbuddyIndex {
    /// 在调用者指定基准目录内创建或验证独立派生库；禁止路径逃逸与未知 schema。
    /// # Errors
    /// 路径、权限、格式或数据库错误返回稳定脱敏类别，不覆盖已有库。
    pub async fn open(base_dir: &Path, relative_path: &Path) -> Result<Self, LocalError> {
        let (connection, path) =
            super::private_sqlite::open_private_sqlite(base_dir, relative_path)
                .await
                .map_err(super::map_private_sqlite_error)?;
        if let Err(error) = schema::ensure_schema(&connection).await {
            let _ = connection.close().await;
            return Err(error);
        }
        super::private_sqlite::set_private_database_permissions(&path)
            .await
            .map_err(super::map_private_sqlite_error)?;
        super::enable_concurrent_snapshot_reads(&connection).await?;
        Ok(Self { connection })
    }

    /// 同一事务发布固定两根全部安全增量；错误或取消保留旧数据并标记覆盖不完整。
    /// # Errors
    /// 来源不完整、冲突或数据库错误不会发布半份统计或推进 checkpoint。
    pub async fn refresh<C>(
        &mut self,
        sources: &[WorkbuddyScanSource],
        since_epoch_ms: i64,
        mode: WorkbuddyRefreshMode,
        cancellation: C,
    ) -> Result<WorkbuddyIndexOutcome, LocalError>
    where
        C: CancellationSignal + Clone + 'static,
    {
        let result = self
            .refresh_inner(
                sources,
                since_epoch_ms,
                mode == WorkbuddyRefreshMode::Rebuild,
                cancellation,
            )
            .await;
        if result.is_err() {
            self.record_failure(sources).await?;
        }
        result
    }

    /// 先在受控 blocking 岛完成来源观察，再在短事务内写入整轮变化与覆盖。
    async fn refresh_inner<C>(
        &mut self,
        sources: &[WorkbuddyScanSource],
        since_epoch_ms: i64,
        force_rebuild: bool,
        cancellation: C,
    ) -> Result<WorkbuddyIndexOutcome, LocalError>
    where
        C: CancellationSignal + Clone + 'static,
    {
        if cancellation.is_cancelled() {
            return Err(source_error());
        }
        let known = self.checkpoints().await?;
        let sources = sources.to_vec();
        let worker_cancellation = cancellation.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            source::prepare_update(&sources, &known, force_rebuild, &worker_cancellation)
        })
        .await
        .map_err(|_| source_error())??;
        if cancellation.is_cancelled() {
            return Err(source_error());
        }
        let outcome = WorkbuddyIndexOutcome {
            files_scanned: prepared.files.len() as u64,
            unchanged_files: prepared.files.iter().filter(|file| file.unchanged).count() as u64,
            rebuilt_files: prepared.files.iter().filter(|file| file.rebuilt).count() as u64,
            bytes_read: prepared.bytes_read,
        };
        let transaction = self.connection.begin().await.map_err(database_error)?;
        let result = Self::publish(&transaction, prepared, since_epoch_ms, &cancellation).await;
        match result {
            Ok(()) => {
                transaction.commit().await.map_err(database_error)?;
                Ok(outcome)
            }
            Err(error) => {
                transaction.rollback().await.map_err(database_error)?;
                Err(error)
            }
        }
    }

    /// 完整枚举才删除消失来源，所有安全事件及检查点与覆盖一起提交。
    async fn publish<C: CancellationSignal + ?Sized>(
        transaction: &sea_orm::DatabaseTransaction,
        prepared: PreparedUpdate,
        since_epoch_ms: i64,
        cancellation: &C,
    ) -> Result<(), LocalError> {
        let retained = prepared
            .files
            .iter()
            .map(|file| file.checkpoint.source_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let old = transaction
            .query_all_raw(statement("SELECT source_id FROM workbuddy_files", vec![]))
            .await
            .map_err(database_error)?;
        for row in old {
            if cancellation.is_cancelled() {
                return Err(source_error());
            }
            let id: String = row.try_get_by_index(0).map_err(database_error)?;
            if !retained.contains(&id) {
                transaction
                    .execute_raw(statement(
                        "DELETE FROM workbuddy_files WHERE source_id=?1",
                        vec![id.into()],
                    ))
                    .await
                    .map_err(database_error)?;
            }
        }
        for file in prepared.files {
            if cancellation.is_cancelled() {
                return Err(source_error());
            }
            if file.unchanged {
                continue;
            }
            let mut checkpoint = file.checkpoint;
            if file.rebuilt {
                transaction
                    .execute_raw(statement(
                        "DELETE FROM workbuddy_events WHERE source_id=?1",
                        vec![checkpoint.source_id.clone().into()],
                    ))
                    .await
                    .map_err(database_error)?;
            }
            // 文件占位先于其事件；事务失败不会留下半份检查点。
            transaction.execute_raw(statement("INSERT INTO workbuddy_files VALUES (?1,?2) ON CONFLICT(source_id) DO UPDATE SET checkpoint_json=excluded.checkpoint_json",vec![checkpoint.source_id.clone().into(),encode(&checkpoint)?.into()])).await.map_err(database_error)?;
            for record in file.records {
                if cancellation.is_cancelled() {
                    return Err(source_error());
                }
                let (mut valid, quality) =
                    crate::workbuddy_facts::record::prepare_workbuddy_usage_records(&[record]);
                if quality.invalid_record_count > 0
                    || quality.conflicting_duplicate_record_count > 0
                {
                    return Err(source_error());
                }
                let record = valid.pop().ok_or_else(source_error)?;
                checkpoint.sequence = checkpoint
                    .sequence
                    .checked_add(1)
                    .ok_or_else(source_error)?;
                if record.occurred_at_epoch_ms < since_epoch_ms {
                    continue;
                }
                transaction
                    .execute_raw(statement(
                        "INSERT INTO workbuddy_events VALUES (?1,?2,?3,?4)",
                        vec![
                            checkpoint.source_id.clone().into(),
                            i64::try_from(checkpoint.sequence)
                                .map_err(|_| source_error())?
                                .into(),
                            record.occurred_at_epoch_ms.into(),
                            encode(&record)?.into(),
                        ],
                    ))
                    .await
                    .map_err(database_error)?;
            }
            transaction
                .execute_raw(statement(
                    "UPDATE workbuddy_files SET checkpoint_json=?2 WHERE source_id=?1",
                    vec![
                        checkpoint.source_id.clone().into(),
                        encode(&checkpoint)?.into(),
                    ],
                ))
                .await
                .map_err(database_error)?;
        }
        transaction
            .execute_raw(statement(
                "DELETE FROM workbuddy_events WHERE occurred_at<?1",
                vec![since_epoch_ms.into()],
            ))
            .await
            .map_err(database_error)?;
        let records = Self::records_on(transaction, cancellation).await?;
        let (_, quality) =
            crate::workbuddy_facts::record::prepare_workbuddy_usage_records(&records);
        if quality.invalid_record_count > 0
            || quality.conflicting_duplicate_record_count > 0
            || cancellation.is_cancelled()
        {
            return Err(source_error());
        }
        let root_count = prepared.root_aliases.len() as u64;
        let state = SnapshotState {
            coverage: CoverageReport {
                state: CoverageState::Complete,
                roots_scanned: root_count,
                roots_discovered: root_count,
                permission_denied_count: 0,
                skipped_count: 0,
                warning_count: 0,
            },
            root_aliases: prepared.root_aliases,
        };
        transaction.execute_raw(statement("INSERT INTO workbuddy_state VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET state_json=excluded.state_json",vec![encode(&state)?.into()])).await.map_err(database_error)?;
        Ok(())
    }

    /// 恢复安全的来源进度，不读取原始数据根或会话内容。
    async fn checkpoints(&self) -> Result<BTreeMap<String, Checkpoint>, LocalError> {
        let rows=self.connection.query_all_raw(statement("SELECT CASE WHEN length(checkpoint_json)<=4096 THEN checkpoint_json END FROM workbuddy_files",vec![])).await.map_err(database_error)?;
        rows.into_iter()
            .map(|row| {
                let json: String = row.try_get_by_index(0).map_err(database_error)?;
                let checkpoint: Checkpoint =
                    serde_json::from_str(&json).map_err(|_| source_error())?;
                Ok((checkpoint.source_id.clone(), checkpoint))
            })
            .collect()
    }
    /// 失败只降低覆盖，旧事件与文件检查点完全保留，上传继续失败关闭。
    async fn record_failure(&self, sources: &[WorkbuddyScanSource]) -> Result<(), LocalError> {
        let mut state = self
            .state_on(&self.connection)
            .await?
            .unwrap_or_else(|| SnapshotState {
                coverage: CoverageReport {
                    state: CoverageState::Partial,
                    roots_scanned: 0,
                    roots_discovered: sources.len() as u64,
                    permission_denied_count: 0,
                    skipped_count: 1,
                    warning_count: 0,
                },
                root_aliases: sources
                    .iter()
                    .map(|source| (source.root_id.clone(), source.alias.clone()))
                    .collect(),
            });
        state.coverage.state = CoverageState::Partial;
        state.coverage.warning_count = state.coverage.warning_count.max(1);
        self.connection.execute_raw(statement("INSERT INTO workbuddy_state VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET state_json=excluded.state_json",vec![encode(&state)?.into()])).await.map_err(database_error)?;
        Ok(())
    }
}
/// 编码已经通过计量与标签校验的元数据，不接受原始事件正文。
fn encode<T: Serialize>(value: &T) -> Result<String, LocalError> {
    serde_json::to_string(value).map_err(|_| source_error())
}
/// 数据库错误不包含 SQL 绑定值、文件位置或用户数据。
fn database_error(_: sea_orm::DbErr) -> LocalError {
    LocalError::new(
        LocalErrorKind::Database,
        "workbuddy statistics database operation failed",
    )
}
