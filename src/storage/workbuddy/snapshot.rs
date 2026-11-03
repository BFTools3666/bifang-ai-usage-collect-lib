//! 仅从同一已发布 SQLite 快照恢复 WorkBuddy 计量，不重新读取来源文件。
use super::{
    SnapshotState, WorkbuddyIndex, database_error, schema::statement, source::source_error,
};
use crate::readers::workbuddy::WorkbuddyUsageInputs;
use crate::workbuddy_facts::record::ValidatedWorkbuddyUsageRecord;
use crate::{CancellationSignal, LocalError, WorkbuddyUsageEventRecord};
use sea_orm::{ConnectionTrait, TransactionTrait};

impl WorkbuddyIndex {
    /// 读取完整已发布快照；尚未建立索引返回 None，来源缺失由根集合表达。
    /// # Errors
    /// 损坏的状态、计量或数据库返回脱敏错误，不回退直接扫描。
    pub async fn load_inputs(&self) -> Result<Option<WorkbuddyUsageInputs>, LocalError> {
        self.load_inputs_with_cancellation(&crate::readers::CancellationToken::default())
            .await
    }
    /// 带调用方取消信号读取同一发布代，关闭期间不继续解码大量历史行。
    /// # Errors
    /// 取消、损坏的计量或数据库错误返回稳定脱敏类别。
    pub async fn load_inputs_with_cancellation<C: CancellationSignal + ?Sized>(
        &self,
        cancellation: &C,
    ) -> Result<Option<WorkbuddyUsageInputs>, LocalError> {
        if cancellation.is_cancelled() {
            return Err(source_error());
        }
        let transaction = self.connection.begin().await.map_err(database_error)?;
        let state = self.state_on(&transaction).await?;
        let Some(state) = state else {
            transaction.rollback().await.map_err(database_error)?;
            return Ok(None);
        };
        let records = Self::records_on(&transaction, cancellation).await?;
        let (_, quality) =
            crate::workbuddy_facts::record::prepare_workbuddy_usage_records(&records);
        if quality.invalid_record_count > 0 || quality.conflicting_duplicate_record_count > 0 {
            return Err(source_error());
        }
        transaction.rollback().await.map_err(database_error)?;
        Ok(Some(WorkbuddyUsageInputs {
            records,
            coverage: state.coverage,
            root_aliases: state.root_aliases,
        }))
    }
    /// 在调用者当前事务中读取唯一发布状态。
    pub(super) async fn state_on<C: ConnectionTrait>(
        &self,
        connection: &C,
    ) -> Result<Option<SnapshotState>, LocalError> {
        connection.query_one_raw(statement("SELECT CASE WHEN length(state_json)<=16384 THEN state_json END FROM workbuddy_state WHERE singleton=1",vec![])).await.map_err(database_error)?.map(|row| {
            let json:String=row.try_get_by_index(0).map_err(database_error)?;
            serde_json::from_str(&json).map_err(|_|source_error())
        }).transpose()
    }
    /// 恢复已经过规范化的安全观察，调用者仍按同一事实比较规则校验跨来源冲突。
    pub(super) async fn records_on<C: ConnectionTrait, S: CancellationSignal + ?Sized>(
        connection: &C,
        cancellation: &S,
    ) -> Result<Vec<WorkbuddyUsageEventRecord>, LocalError> {
        let mut records = Vec::new();
        let mut source_cursor = String::new();
        let mut sequence_cursor = 0_i64;
        loop {
            if cancellation.is_cancelled() {
                return Err(source_error());
            }
            let rows=connection.query_all_raw(statement("SELECT source_id, sequence, CASE WHEN length(record_json)<=16384 THEN record_json END FROM workbuddy_events WHERE source_id>?1 OR (source_id=?1 AND sequence>?2) ORDER BY source_id,sequence LIMIT 256",vec![source_cursor.clone().into(),sequence_cursor.into()])).await.map_err(database_error)?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                if cancellation.is_cancelled() || records.len() >= 1_000_000 {
                    return Err(source_error());
                }
                let source_id: String = row.try_get_by_index(0).map_err(database_error)?;
                let sequence: i64 = row.try_get_by_index(1).map_err(database_error)?;
                if sequence <= 0
                    || source_id < source_cursor
                    || (source_id == source_cursor && sequence <= sequence_cursor)
                {
                    return Err(source_error());
                }
                source_cursor = source_id.clone();
                sequence_cursor = sequence;
                let json: String = row.try_get_by_index(2).map_err(database_error)?;
                let record: ValidatedWorkbuddyUsageRecord =
                    serde_json::from_str(&json).map_err(|_| source_error())?;
                if record.source_ids.as_slice() != [source_id.as_str()] {
                    return Err(source_error());
                }
                let integer = |value: u64| i64::try_from(value).map_err(|_| source_error());
                records.push(WorkbuddyUsageEventRecord {
                    logical_call_id: record.logical_call_id,
                    root_id: record.root_id,
                    session_key: record.session_key,
                    source_id,
                    occurred_at_epoch_ms: record.occurred_at_epoch_ms,
                    model: record.model,
                    project_key: record.project_key,
                    project_label: record.project_label,
                    request_count: i64::from(record.request_count),
                    input_tokens: integer(record.usage.input_tokens.ok_or_else(source_error)?)?,
                    cached_input_tokens: integer(
                        record.usage.cached_input_tokens.ok_or_else(source_error)?,
                    )?,
                    output_tokens: integer(record.usage.output_tokens.ok_or_else(source_error)?)?,
                    total_tokens: integer(record.usage.total_tokens)?,
                    credit: record.credit,
                    origin: record.origin,
                });
            }
            tokio::task::yield_now().await;
        }
        Ok(records)
    }
}
