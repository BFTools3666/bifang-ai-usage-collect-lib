//! 管理数据根 registry 的登记、启停、别名，以及会话区域内失效来源的对账。
//!
//! 物理路径去重（比较两个路径是否指向同一挂载点/卷）依赖平台卷分类，
//! 那部分逻辑留在 GUI backend 的发现层，让 SQLite 存储层不引入卷枚举
//! 依赖。因此“添加数据根时避免重复登记同一物理目录”的模糊去重检查由
//! 发现层在调用 [`LocalIndex::register_root_fields`] 之前自行
//! 完成；这里只保留一个不可绕过的硬保证：`root_id` 主键唯一，
//! [`LocalIndex::registered_root_paths`] 供 adapter 读取现状做去重判断。
//! adapter 侧的去重检查和这里的写入不在同一个事务里，理论上存在极窄的
//! 竞态窗口（两次几乎同时的手动添加）；最坏后果只是多出一行重复登记，
//! 用户可以随时移除，不是数据损坏或安全问题。

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use sea_orm::{ConnectionTrait, TransactionTrait};

use super::path_codec::{decode_path, encode_path};
use super::{
    DiscoveryMethod, IntoLocalResult, LocalError, LocalErrorKind, LocalIndex, RegisteredRoot,
};
use crate::RootActivationState;

pub(super) use crate::storage::sql::statement;

pub(super) use crate::storage::sql::in_placeholders;

/// 在调用方选择的事务边界内登记或更新一个数据根。
async fn register_root_fields_on<C: ConnectionTrait + ?Sized>(
    connection: &C,
    root_id: &str,
    access_path: Vec<u8>,
    alias: &str,
    discovery_method: DiscoveryMethod,
) -> Result<(), LocalError> {
    let existing = connection
        .query_one_raw(statement(
            "SELECT access_path, is_primary FROM source_roots WHERE root_id = ?1",
            vec![root_id.into()],
        ))
        .await
        .into_local_result()?
        .map(|row| {
            Ok::<_, LocalError>((
                row.try_get_by_index::<Vec<u8>>(0).into_local_result()?,
                row.try_get_by_index::<bool>(1).into_local_result()?,
            ))
        })
        .transpose()?;
    // 路径字节变化时保守地清除主根身份，避免旧路径继续被当作主数据根。
    let primary_path_changed = existing
        .as_ref()
        .is_some_and(|(stored_path, is_primary)| *is_primary && stored_path != &access_path);
    connection
        .execute_raw(statement(
            "INSERT INTO source_roots
             (root_id, access_path, alias, enabled, discovery_method)
             VALUES (?1, ?2, ?3, 1, ?4)
             ON CONFLICT(root_id) DO UPDATE SET
               access_path = excluded.access_path",
            vec![
                root_id.into(),
                access_path.into(),
                alias.into(),
                discovery_method.as_str().into(),
            ],
        ))
        .await
        .into_local_result()?;
    if primary_path_changed {
        connection
            .execute_raw(statement(
                "UPDATE source_roots SET is_primary = 0 WHERE root_id = ?1",
                vec![root_id.into()],
            ))
            .await
            .into_local_result()?;
    }
    Ok(())
}

/// 在调用方事务内认领首次验证和遗留 indexing 根。
async fn claim_background_index_roots_on<C: ConnectionTrait + ?Sized>(
    connection: &C,
) -> Result<Vec<String>, LocalError> {
    let rows = connection
        .query_all_raw(statement(
            "SELECT root_id FROM source_roots
             WHERE enabled = 1 AND activation_state IN ('confirmed_unindexed', 'indexing')
             ORDER BY root_id",
            vec![],
        ))
        .await
        .into_local_result()?;
    let root_ids = rows
        .into_iter()
        .map(|row| row.try_get_by_index(0))
        .collect::<Result<Vec<String>, _>>()
        .into_local_result()?;
    connection
        .execute_raw(statement(
            "UPDATE source_roots SET activation_state = 'indexing'
             WHERE enabled = 1 AND activation_state = 'confirmed_unindexed'",
            vec![],
        ))
        .await
        .into_local_result()?;
    Ok(root_ids)
}

/// 在调用方事务内只认领当前发现集合中的根。
async fn claim_discovered_roots_on<C: ConnectionTrait + ?Sized>(
    connection: &C,
    root_ids: &[String],
) -> Result<Vec<String>, LocalError> {
    let mut claimed = Vec::new();
    for root_id in root_ids.iter().map(String::as_str).collect::<BTreeSet<_>>() {
        let result = connection
            .execute_raw(statement(
                "UPDATE source_roots SET activation_state = 'indexing'
                 WHERE root_id = ?1 AND enabled = 1
                   AND activation_state IN ('confirmed_unindexed', 'validation_failed')",
                vec![root_id.into()],
            ))
            .await
            .into_local_result()?;
        if result.rows_affected() > 0 {
            claimed.push(root_id.to_owned());
        }
    }
    Ok(claimed)
}

/// 在调用方事务内一次性收口全部后台根；中途错误由同一事务整体回滚。
async fn finish_background_index_roots_on<C: ConnectionTrait + ?Sized>(
    connection: &C,
    root_ids: &[String],
    parser_version: u32,
) -> Result<(), LocalError> {
    for root_id in root_ids {
        connection
            .execute_raw(statement(
                "UPDATE source_roots
                 SET activation_state = CASE
                   WHEN EXISTS (
                     SELECT 1 FROM source_files f
                     WHERE f.root_id = source_roots.root_id AND f.ready = 1
                       AND f.parser_version = ?2
                   ) THEN 'ready' ELSE 'validation_failed' END
                 WHERE root_id = ?1 AND activation_state = 'indexing'",
                vec![root_id.clone().into(), i64::from(parser_version).into()],
            ))
            .await
            .into_local_result()?;
    }
    Ok(())
}

/// 在调用方事务内登记用户确认的根并移除同路径候选，重复登记保持已有启停和别名。
pub(super) async fn register_confirmed_root_on<C: ConnectionTrait>(
    connection: &C,
    root_id: &str,
    path: &Path,
    alias: &str,
    discovery_method: DiscoveryMethod,
) -> Result<bool, LocalError> {
    let result = connection.execute_raw(statement(
        "INSERT INTO source_roots (root_id, access_path, alias, enabled, discovery_method, activation_state) VALUES (?1, ?2, ?3, 1, ?4, ?5) ON CONFLICT(root_id) DO NOTHING",
        vec![root_id.into(), encode_path(path).into(), alias.into(), discovery_method.as_str().into(), RootActivationState::ConfirmedUnindexed.as_str().into()]
    )).await.into_local_result()?;
    connection
        .execute_raw(statement(
            "DELETE FROM discovery_candidates WHERE access_path = ?1",
            vec![encode_path(path).into()],
        ))
        .await
        .into_local_result()?;
    Ok(result.rows_affected() > 0)
}

/// 在同一事务内重排验证失败根并读取真实激活状态。
pub(super) async fn queue_confirmed_root_on<C: ConnectionTrait>(
    connection: &C,
    root_id: &str,
) -> Result<RootActivationState, LocalError> {
    connection.execute_raw(statement("UPDATE source_roots SET activation_state = 'confirmed_unindexed' WHERE root_id = ?1 AND enabled = 1 AND activation_state = 'validation_failed'", vec![root_id.into()])).await.into_local_result()?;
    let label: String = connection
        .query_one_raw(statement(
            "SELECT activation_state FROM source_roots WHERE root_id = ?1",
            vec![root_id.into()],
        ))
        .await
        .into_local_result()?
        .ok_or_else(|| LocalError::new(LocalErrorKind::Database, "root is not registered"))?
        .try_get_by_index(0)
        .into_local_result()?;
    RootActivationState::from_label(&label).ok_or_else(|| {
        LocalError::new(LocalErrorKind::Database, "root activation state is invalid")
    })
}

impl LocalIndex {
    /// 原子登记根的内部访问路径；若唯一主根的同一稳定 ID 被重绑到另一物理路径，
    /// 同一事务会清除主标记，避免旧主根身份继续关联到新路径。
    // root_id 由路径规范化生成。用户重装或移动挂载点后，同一 root_id
    // 可能被重新绑定到另一个物理目录。若该根仍是主数据目录，继续保留
    // 旧 is_primary 标记会把旧主根身份误绑到新路径。路径
    // 变化时同一事务清掉 primary 标记。
    pub async fn register_root_fields(
        &mut self,
        root_id: &str,
        path: &Path,
        alias: &str,
        discovery_method: DiscoveryMethod,
    ) -> Result<(), LocalError> {
        self.validate_root_agent(root_id)?;
        let access_path = encode_path(path);
        if let Some(transaction) = &self.active_scan_transaction {
            register_root_fields_on(transaction, root_id, access_path, alias, discovery_method)
                .await
        } else {
            let transaction = self.connection.begin().await.into_local_result()?;
            register_root_fields_on(&transaction, root_id, access_path, alias, discovery_method)
                .await?;
            transaction.commit().await.into_local_result()
        }
    }

    /// 只登记用户确认的元数据候选，不读取内容并保持首次索引未激活。
    pub async fn register_confirmed_root_fields(
        &mut self,
        root_id: &str,
        path: &Path,
        alias: &str,
        discovery_method: DiscoveryMethod,
    ) -> Result<bool, LocalError> {
        self.validate_root_agent(root_id)?;
        if let Some(transaction) = &self.active_scan_transaction {
            register_confirmed_root_on(transaction, root_id, path, alias, discovery_method).await
        } else {
            let transaction = self.connection.begin().await.into_local_result()?;
            let added =
                register_confirmed_root_on(&transaction, root_id, path, alias, discovery_method)
                    .await?;
            transaction.commit().await.into_local_result()?;
            Ok(added)
        }
    }

    /// 返回全部已登记根的 `(root_id, 内部访问路径)`；仅供 adapter 在写入前做
    /// 物理路径去重判断，不代表任何写入保证（见模块文档的竞态说明）。
    pub async fn registered_root_paths(&self) -> Result<Vec<(String, PathBuf)>, LocalError> {
        let rows = self
            .scan_connection()
            .query_all_raw(statement(
                "SELECT root_id, access_path FROM source_roots",
                vec![],
            ))
            .await
            .into_local_result()?;
        let mut roots = Vec::with_capacity(rows.len());
        for row in rows {
            let root_id: String = row.try_get_by_index(0).into_local_result()?;
            let encoded: Vec<u8> = row.try_get_by_index(1).into_local_result()?;
            let path = decode_path(&encoded).ok_or_else(|| {
                LocalError::new(LocalErrorKind::Database, "registered root path is invalid")
            })?;
            roots.push((root_id, path));
        }
        Ok(roots)
    }

    /// 返回全部已启用历史确认根，供启动快速发现补回主动发现候选。
    pub async fn known_roots(&self) -> Result<Vec<RegisteredRoot>, LocalError> {
        self.load_root_paths(false).await
    }

    /// 返回包括已停用项的全部历史根，只供主动发现复用精确身份与停用状态。
    pub async fn all_roots(&self) -> Result<Vec<RegisteredRoot>, LocalError> {
        self.load_root_paths(true).await
    }

    /// 返回唯一主数据根的内部访问位置；没有主根时保留进程继承的 Codex 上下文。
    #[cfg(any(test, feature = "test-util"))]
    pub async fn primary_root(&self) -> Result<Option<RegisteredRoot>, LocalError> {
        let Some(row) = self
            .connection
            .query_one_raw(statement(
                "SELECT root_id, access_path, alias, enabled
                 FROM source_roots WHERE is_primary = 1",
                vec![],
            ))
            .await
            .into_local_result()?
        else {
            return Ok(None);
        };
        let root_id: String = row.try_get_by_index(0).into_local_result()?;
        let encoded: Vec<u8> = row.try_get_by_index(1).into_local_result()?;
        let alias: String = row.try_get_by_index(2).into_local_result()?;
        let enabled: bool = row.try_get_by_index(3).into_local_result()?;
        if !enabled {
            return Err(LocalError::new(
                LocalErrorKind::Database,
                "primary root registry state is invalid",
            ));
        }
        let path = decode_path(&encoded).ok_or_else(|| {
            LocalError::new(LocalErrorKind::Database, "registered root path is invalid")
        })?;
        Ok(Some(RegisteredRoot {
            root_id: Some(root_id),
            path,
            alias,
            enabled,
        }))
    }

    /// 在单个写事务中切换唯一主数据根；`None` 表示恢复继承的 Codex 上下文。
    pub async fn set_primary_root(&mut self, root_id: Option<&str>) -> Result<bool, LocalError> {
        let transaction = self.connection.begin().await.into_local_result()?;
        let current: Option<String> = transaction
            .query_one_raw(statement(
                "SELECT root_id FROM source_roots WHERE is_primary = 1",
                vec![],
            ))
            .await
            .into_local_result()?
            .map(|row| row.try_get_by_index(0).into_local_result())
            .transpose()?;
        if current.as_deref() == root_id {
            return Ok(false);
        }
        if let Some(root_id) = root_id {
            let enabled: Option<bool> = transaction
                .query_one_raw(statement(
                    "SELECT enabled FROM source_roots WHERE root_id = ?1",
                    vec![root_id.into()],
                ))
                .await
                .into_local_result()?
                .map(|row| row.try_get_by_index(0).into_local_result())
                .transpose()?;
            if enabled != Some(true) {
                return Ok(false);
            }
        }
        transaction
            .execute_raw(statement(
                "UPDATE source_roots SET is_primary = 0 WHERE is_primary = 1",
                vec![],
            ))
            .await
            .into_local_result()?;
        if let Some(root_id) = root_id {
            transaction
                .execute_raw(statement(
                    "UPDATE source_roots SET is_primary = 1 WHERE root_id = ?1 AND enabled = 1",
                    vec![root_id.into()],
                ))
                .await
                .into_local_result()?;
        }
        transaction.commit().await.into_local_result()?;
        Ok(true)
    }

    /// 从 registry 恢复 adapter 内部路径，并按调用边界决定是否包含停用项。
    async fn load_root_paths(
        &self,
        include_disabled: bool,
    ) -> Result<Vec<RegisteredRoot>, LocalError> {
        let sql = if include_disabled {
            "SELECT root_id, access_path, alias, enabled FROM source_roots
             ORDER BY alias, root_id"
        } else {
            "SELECT root_id, access_path, alias, enabled FROM source_roots
             WHERE enabled = 1 AND activation_state IN ('ready', 'indexing')
             ORDER BY alias, root_id"
        };
        let rows = self
            .scan_connection()
            .query_all_raw(statement(sql, vec![]))
            .await
            .into_local_result()?;
        let mut roots = Vec::with_capacity(rows.len());
        for row in rows {
            let root_id: String = row.try_get_by_index(0).into_local_result()?;
            let encoded: Vec<u8> = row.try_get_by_index(1).into_local_result()?;
            let alias: String = row.try_get_by_index(2).into_local_result()?;
            let enabled: bool = row.try_get_by_index(3).into_local_result()?;
            let path = decode_path(&encoded).ok_or_else(|| {
                LocalError::new(LocalErrorKind::Database, "registered root path is invalid")
            })?;
            roots.push(RegisteredRoot {
                root_id: Some(root_id),
                path,
                alias,
                enabled,
            });
        }
        Ok(roots)
    }

    /// 把重新确认的验证失败根重新排入后台统计，已就绪根保持不变。
    pub async fn queue_confirmed_root_for_background_scan(
        &mut self,
        root_id: &str,
    ) -> Result<RootActivationState, LocalError> {
        if let Some(transaction) = &self.active_scan_transaction {
            queue_confirmed_root_on(transaction, root_id).await
        } else {
            queue_confirmed_root_on(&self.connection, root_id).await
        }
    }

    /// 原子认领本轮后台统计根，遗留 `indexing` 根也会在下轮重试。
    pub async fn claim_background_index_roots(&mut self) -> Result<Vec<String>, LocalError> {
        if let Some(transaction) = &self.active_scan_transaction {
            claim_background_index_roots_on(transaction).await
        } else {
            let transaction = self.connection.begin().await.into_local_result()?;
            let root_ids = claim_background_index_roots_on(&transaction).await?;
            transaction.commit().await.into_local_result()?;
            Ok(root_ids)
        }
    }

    /// 只认领本轮发现结果中的精确根；不会把扫描开始后由其他入口新增的根带入本轮。
    pub async fn claim_discovered_roots_for_current_scan(
        &mut self,
        root_ids: &[String],
    ) -> Result<Vec<String>, LocalError> {
        if let Some(transaction) = &self.active_scan_transaction {
            claim_discovered_roots_on(transaction, root_ids).await
        } else {
            let transaction = self.connection.begin().await.into_local_result()?;
            let claimed = claim_discovered_roots_on(&transaction, root_ids).await?;
            transaction.commit().await.into_local_result()?;
            Ok(claimed)
        }
    }

    /// 根据当前 parser 已完整提交的来源完成本轮后台验证。
    ///
    /// 合法来源可能在本次日期窗口内没有任何调用；这种情况仍是已完成扫描的
    /// `ReadyNoCalls`，不能把“零调用”误当成候选结构验证失败。
    pub async fn finish_background_index_roots(
        &mut self,
        root_ids: &[String],
    ) -> Result<(), LocalError> {
        if let Some(transaction) = &self.active_scan_transaction {
            finish_background_index_roots_on(transaction, root_ids, self.parser_version).await
        } else {
            let transaction = self.connection.begin().await.into_local_result()?;
            finish_background_index_roots_on(&transaction, root_ids, self.parser_version).await?;
            transaction.commit().await.into_local_result()
        }
    }

    /// 启用或禁用登记根；禁用不会删除原始数据文件。
    pub async fn set_root_enabled(
        &mut self,
        root_id: &str,
        enabled: bool,
    ) -> Result<bool, LocalError> {
        let transaction = self.connection.begin().await.into_local_result()?;
        let result = if enabled {
            transaction
                .execute_raw(statement(
                    "UPDATE source_roots SET enabled = 1 WHERE root_id = ?1",
                    vec![root_id.into()],
                ))
                .await
                .into_local_result()?
        } else {
            transaction
                .execute_raw(statement(
                    "UPDATE source_roots SET enabled = 0, is_primary = 0 WHERE root_id = ?1",
                    vec![root_id.into()],
                ))
                .await
                .into_local_result()?
        };
        transaction.commit().await.into_local_result()?;
        Ok(result.rows_affected() > 0)
    }
}

mod mutate;

#[cfg(test)]
mod tests;
