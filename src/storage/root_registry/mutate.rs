//! 数据根别名、移除、覆盖状态与会话来源对账。

use std::collections::HashSet;

use sea_orm::{ConnectionTrait, TransactionTrait};

use crate::CoverageState;

#[cfg(any(test, feature = "test-util"))]
use super::super::RootRecord;
use super::super::{IntoLocalResult, LocalError, LocalIndex, coverage_state_label};
use super::{in_placeholders, statement};

/// 在调用方选择的事务边界内施加已知根删除与 Partial 标记。
async fn reconcile_known_roots_on<C: ConnectionTrait + ?Sized>(
    connection: &C,
    remove_root_ids: &[String],
    partial_root_ids: &[String],
) -> Result<(), LocalError> {
    if !remove_root_ids.is_empty() {
        let placeholders = in_placeholders(1, remove_root_ids.len());
        let id_values: Vec<sea_orm::Value> = remove_root_ids.iter().map(Into::into).collect();
        connection
            .execute_raw(statement(
                &format!("DELETE FROM source_roots WHERE root_id IN ({placeholders})"),
                id_values,
            ))
            .await
            .into_local_result()?;
    }
    if !partial_root_ids.is_empty() {
        let placeholders = in_placeholders(2, partial_root_ids.len());
        let mut values: Vec<sea_orm::Value> =
            vec![coverage_state_label(CoverageState::Partial).into()];
        values.extend(partial_root_ids.iter().map(Into::into));
        connection
            .execute_raw(statement(
                &format!(
                    "UPDATE source_roots SET last_coverage_state = ?1 WHERE root_id IN ({placeholders})"
                ),
                values,
            ))
            .await
            .into_local_result()?;
    }
    Ok(())
}

impl LocalIndex {
    /// 更新已登记数据根的安全展示别名；访问路径和原始文件不变。
    pub async fn set_root_alias(&mut self, root_id: &str, alias: &str) -> Result<bool, LocalError> {
        let result = self
            .connection
            .execute_raw(statement(
                "UPDATE source_roots SET alias = ?2 WHERE root_id = ?1",
                vec![root_id.into(), alias.into()],
            ))
            .await
            .into_local_result()?;
        Ok(result.rows_affected() > 0)
    }

    /// 移除根及其本产品索引记录，外部数据始终保持只读不变。
    pub async fn remove_root(&mut self, root_id: &str) -> Result<bool, LocalError> {
        let transaction = self.connection.begin().await.into_local_result()?;
        let result = transaction
            .execute_raw(statement(
                "DELETE FROM source_roots WHERE root_id = ?1",
                vec![root_id.into()],
            ))
            .await
            .into_local_result()?;
        transaction.commit().await.into_local_result()?;
        Ok(result.rows_affected() > 0)
    }

    /// 更新一个数据根最近的覆盖状态，供来源页解释部分或取消扫描。
    pub async fn record_root_coverage(
        &mut self,
        root_id: &str,
        state: CoverageState,
    ) -> Result<(), LocalError> {
        self.scan_connection()
            .execute_raw(statement(
                "UPDATE source_roots SET last_coverage_state = ?2 WHERE root_id = ?1",
                vec![root_id.into(), coverage_state_label(state).into()],
            ))
            .await
            .into_local_result()?;
        Ok(())
    }

    /// 在单一事务内施加已知根删除与 Partial 标记；任一步失败都回滚全部变更。
    pub async fn reconcile_known_roots(
        &mut self,
        remove_root_ids: &[String],
        partial_root_ids: &[String],
    ) -> Result<(), LocalError> {
        if remove_root_ids.is_empty() && partial_root_ids.is_empty() {
            return Ok(());
        }
        if let Some(transaction) = &self.active_scan_transaction {
            reconcile_known_roots_on(transaction, remove_root_ids, partial_root_ids).await
        } else {
            let transaction = self.connection.begin().await.into_local_result()?;
            let reconciliation =
                reconcile_known_roots_on(&transaction, remove_root_ids, partial_root_ids).await;
            match reconciliation {
                Ok(()) => transaction.commit().await.into_local_result(),
                Err(error) => {
                    transaction.rollback().await.into_local_result()?;
                    Err(error)
                }
            }
        }
    }

    /// 对账一个会话区域：精确拒绝始终删除，仅在完整枚举时推断并删除缺失来源。
    pub async fn reconcile_root_sources(
        &mut self,
        root_id: &str,
        archived: bool,
        retained_relative_labels: &HashSet<String>,
        rejected_relative_labels: &HashSet<String>,
        enumeration_complete: bool,
    ) -> Result<u64, LocalError> {
        let rows = self
            .scan_connection()
            .query_all_raw(statement(
                "SELECT source_id, relative_label FROM source_files
                 WHERE root_id = ?1 AND archived = ?2",
                vec![root_id.into(), archived.into()],
            ))
            .await
            .into_local_result()?;
        let mut stale_source_ids = Vec::new();
        for row in rows {
            let source_id: String = row.try_get_by_index(0).into_local_result()?;
            let relative_label: String = row.try_get_by_index(1).into_local_result()?;
            if rejected_relative_labels.contains(&relative_label)
                || (enumeration_complete && !retained_relative_labels.contains(&relative_label))
            {
                stale_source_ids.push(source_id);
            }
        }
        if stale_source_ids.is_empty() {
            return Ok(0);
        }
        let placeholders = in_placeholders(2, stale_source_ids.len());
        let mut values: Vec<sea_orm::Value> = vec![root_id.into()];
        values.extend(stale_source_ids.iter().map(Into::into));
        if let Some(transaction) = &self.active_scan_transaction {
            let result = transaction
                .execute_raw(statement(
                    &format!(
                        "DELETE FROM source_files WHERE root_id = ?1 AND source_id IN ({placeholders})"
                    ),
                    values,
                ))
                .await
                .into_local_result()?;
            Ok(result.rows_affected())
        } else {
            let transaction = self.connection.begin().await.into_local_result()?;
            let result = transaction
                .execute_raw(statement(
                &format!(
                    "DELETE FROM source_files WHERE root_id = ?1 AND source_id IN ({placeholders})"
                ),
                values,
            ))
            .await
            .into_local_result()?;
            transaction.commit().await.into_local_result()?;
            Ok(result.rows_affected())
        }
    }

    /// 返回不含绝对路径的数据根 registry 展示记录，供 adapter 诊断与测试观察登记状态。
    #[cfg(any(test, feature = "test-util"))]
    pub async fn list_sources(&self) -> Result<Vec<RootRecord>, LocalError> {
        super::super::snapshot::load_root_records(&self.connection, self.parser_version).await
    }
}
