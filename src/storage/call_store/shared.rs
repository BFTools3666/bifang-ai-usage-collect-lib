//! 调用写入共享 SQL、语句构造与行编解码辅助。

use sea_orm::{ConnectionTrait, DatabaseTransaction};

use crate::{Confidence, SessionTokenSnapshot, SourceProvenance, TokenUsage, UsageCall};

use super::super::{IntoLocalResult, LocalError, LocalErrorKind};

pub(super) const DELETE_DERIVED_CALLS_SQL: &str = "DELETE FROM usage_calls
     WHERE source_id = ?1 AND generation = ?2
       AND thread_key = ?3 AND confidence = 'derived'";

// 两条写入语句对应两种不同的写入语义：
//   INSERT_USAGE_CALL_SQL 用 `ON CONFLICT DO NOTHING`——如果这个
//     (source_id, generation, logical_call_id) 组合已经存在，什么也不做，
//     用于“第一次见到就写入，见过就跳过”的普通场景；
//   UPSERT_USAGE_CALL_SQL 用 `ON CONFLICT DO UPDATE`——已存在时用新值
//     整体覆盖旧行，用于下面 Claude 批次里“确认新观察更完整才允许覆盖”
//     的场景。两条语句共享完全相同的列和参数顺序，方便按需切换。
pub(super) const INSERT_USAGE_CALL_SQL: &str = "INSERT INTO usage_calls
     (source_id, generation, logical_call_id, occurred_at_epoch_ms,
      model, reasoning_effort, project_key, thread_key,
      project_label, thread_label,
      input_tokens, cached_input_tokens, cache_write_input_tokens,
      output_tokens, reasoning_output_tokens, total_tokens,
      total_is_derived, confidence, input_available, cached_input_available,
      output_available, reasoning_output_available, adapter_consistency_key)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
             ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)
     ON CONFLICT(source_id, generation, logical_call_id) DO NOTHING";

pub(super) const UPSERT_USAGE_CALL_SQL: &str = "INSERT INTO usage_calls
     (source_id, generation, logical_call_id, occurred_at_epoch_ms,
      model, reasoning_effort, project_key, thread_key,
      project_label, thread_label,
      input_tokens, cached_input_tokens, cache_write_input_tokens,
      output_tokens, reasoning_output_tokens, total_tokens,
      total_is_derived, confidence, input_available, cached_input_available,
      output_available, reasoning_output_available, adapter_consistency_key)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
             ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)
     ON CONFLICT(source_id, generation, logical_call_id) DO UPDATE SET
       occurred_at_epoch_ms = excluded.occurred_at_epoch_ms,
       model = excluded.model,
       reasoning_effort = excluded.reasoning_effort,
       project_key = excluded.project_key,
       thread_key = excluded.thread_key,
       project_label = excluded.project_label,
       thread_label = excluded.thread_label,
       input_tokens = excluded.input_tokens,
       cached_input_tokens = excluded.cached_input_tokens,
       cache_write_input_tokens = excluded.cache_write_input_tokens,
       output_tokens = excluded.output_tokens,
       reasoning_output_tokens = excluded.reasoning_output_tokens,
       total_tokens = excluded.total_tokens,
       total_is_derived = excluded.total_is_derived,
       confidence = excluded.confidence,
       input_available = excluded.input_available,
       cached_input_available = excluded.cached_input_available,
       output_available = excluded.output_available,
       reasoning_output_available = excluded.reasoning_output_available,
       adapter_consistency_key = excluded.adapter_consistency_key";

pub(crate) use crate::storage::sql::statement;

pub(super) use crate::storage::sql::in_placeholders;

/// 写入一条规范化调用，返回受影响行数（`ON CONFLICT DO NOTHING` 冲突时为 0）。
pub(super) async fn execute_usage_call(
    transaction: &DatabaseTransaction,
    sql: &str,
    source_id: &str,
    generation: i64,
    call: &UsageCall,
) -> Result<u64, LocalError> {
    let cache_write = call
        .usage
        .cache_write_input_tokens
        .map(to_sql_u64)
        .transpose()?;
    let cached_input = call
        .usage
        .cached_input_tokens
        .map(to_sql_u64)
        .transpose()?
        .unwrap_or_default();
    let reasoning_output = call
        .usage
        .reasoning_output_tokens
        .map(to_sql_u64)
        .transpose()?
        .unwrap_or_default();
    let input = call
        .usage
        .input_tokens
        .map(to_sql_u64)
        .transpose()?
        .unwrap_or_default();
    let output = call
        .usage
        .output_tokens
        .map(to_sql_u64)
        .transpose()?
        .unwrap_or_default();
    let result = transaction
        .execute_raw(statement(
            sql,
            vec![
                source_id.into(),
                generation.into(),
                (&call.logical_call_id).into(),
                call.occurred_at_epoch_ms.into(),
                call.model.clone().into(),
                call.reasoning_effort.clone().into(),
                call.project_key.clone().into(),
                (&call.thread_key).into(),
                call.project_label.clone().into(),
                call.thread_label.clone().into(),
                input.into(),
                cached_input.into(),
                cache_write.into(),
                output.into(),
                reasoning_output.into(),
                to_sql_u64(call.usage.total_tokens)?.into(),
                call.usage.total_is_derived.into(),
                confidence_label(call.confidence).into(),
                call.usage.input_tokens.is_some().into(),
                call.usage.cached_input_tokens.is_some().into(),
                call.usage.output_tokens.is_some().into(),
                call.usage.reasoning_output_tokens.is_some().into(),
                call.adapter_consistency_key.clone().into(),
            ],
        ))
        .await
        .into_local_result()?;
    Ok(result.rows_affected())
}

/// 把当前 generation 的一行恢复为 core 调用；列顺序须与调用方查询保持一致。
pub(crate) fn row_to_usage_call(row: &sea_orm::QueryResult) -> Result<UsageCall, LocalError> {
    let total_is_derived: bool = row.try_get_by_index(14).into_local_result()?;
    let total_tokens = from_sql_u64(row.try_get_by_index(13).into_local_result()?)?;
    let input = from_sql_u64(row.try_get_by_index(8).into_local_result()?)?;
    let cached_input = from_sql_u64(row.try_get_by_index(9).into_local_result()?)?;
    let output = from_sql_u64(row.try_get_by_index(11).into_local_result()?)?;
    let reasoning_output = from_sql_u64(row.try_get_by_index(12).into_local_result()?)?;
    let input_available: bool = row.try_get_by_index(16).into_local_result()?;
    let cached_input_available: bool = row.try_get_by_index(17).into_local_result()?;
    let output_available: bool = row.try_get_by_index(18).into_local_result()?;
    let reasoning_output_available: bool = row.try_get_by_index(19).into_local_result()?;
    let mut usage = TokenUsage::new_with_component_availability(
        input_available.then_some(input),
        cached_input_available.then_some(cached_input),
        row.try_get_by_index::<Option<i64>>(10)
            .into_local_result()?
            .map(from_sql_u64)
            .transpose()?,
        output_available.then_some(output),
        reasoning_output_available.then_some(reasoning_output),
        Some(total_tokens),
    )
    .map_err(LocalError::from)?;
    // 可用性降级发生在总量推导之后时，持久化事实可能同时具有“推导总量”与
    // 缺失主分项；先按已存总量验证安全下界，再恢复来源标记，不能重新推导。
    usage.total_is_derived = total_is_derived;
    let confidence_label: String = row.try_get_by_index(15).into_local_result()?;
    let confidence = confidence_from_label(&confidence_label)
        .ok_or_else(|| LocalError::new(LocalErrorKind::Database, "stored confidence is invalid"))?;
    Ok(UsageCall {
        logical_call_id: row.try_get_by_index(0).into_local_result()?,
        occurred_at_epoch_ms: row.try_get_by_index(1).into_local_result()?,
        model: row.try_get_by_index(2).into_local_result()?,
        reasoning_effort: row.try_get_by_index(3).into_local_result()?,
        project_key: row.try_get_by_index(4).into_local_result()?,
        thread_key: row.try_get_by_index(5).into_local_result()?,
        project_label: row.try_get_by_index(6).into_local_result()?,
        thread_label: row.try_get_by_index(7).into_local_result()?,
        usage,
        adapter_consistency_key: row.try_get_by_index(20).into_local_result()?,
        confidence,
        provenance: vec![SourceProvenance {
            source_id: row.try_get_by_index(21).into_local_result()?,
            root_id: row.try_get_by_index(22).into_local_result()?,
            relative_label: row.try_get_by_index(23).into_local_result()?,
            archived: row.try_get_by_index(24).into_local_result()?,
        }],
    })
}

/// 返回置信度的当前 schema 标签。
const fn confidence_label(confidence: Confidence) -> &'static str {
    match confidence {
        Confidence::Exact => "exact",
        Confidence::Derived => "derived",
        Confidence::Suspected => "suspected",
    }
}

/// 从当前 schema 标签恢复 core 置信度。
fn confidence_from_label(value: &str) -> Option<Confidence> {
    match value {
        "exact" => Some(Confidence::Exact),
        "derived" => Some(Confidence::Derived),
        "suspected" => Some(Confidence::Suspected),
        _ => None,
    }
}

// SQLite 的 INTEGER 列本质是有符号 64 位整数，没有原生 u64 类型；
// core 里的 Token 计数用的是 u64。这一对函数在写入/读出时做安全转换，
// `i64::try_from`/`u64::try_from` 在数值超出对方可表示范围时返回错误
// 而不是静默截断或环绕——只要 Token 数量不超过 i64::MAX（约 92 亿亿，
// 实际不可能达到），转换永远成功；真正溢出时会被当成 Overflow 错误上抛，
// 而不是悄悄存出一个错误的负数。
/// 将无符号计数安全收窄为 SQLite 可表示的有符号整数。
pub(crate) fn to_sql_u64(value: u64) -> Result<i64, LocalError> {
    i64::try_from(value)
        .map_err(|_| LocalError::new(LocalErrorKind::Overflow, "index value overflowed"))
}

/// 从 SQLite 非负有符号整数恢复 u64。
pub(crate) fn from_sql_u64(value: i64) -> Result<u64, LocalError> {
    u64::try_from(value)
        .map_err(|_| LocalError::new(LocalErrorKind::Database, "stored counter is negative"))
}

pub(super) const UPSERT_TOKEN_SNAPSHOT_SQL: &str = "INSERT INTO usage_token_snapshots
     (source_id, generation, thread_key, occurred_at_epoch_ms, logical_call_id,
      total_tokens, model, reasoning_effort, project_key)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
     ON CONFLICT(source_id, generation, thread_key, occurred_at_epoch_ms, logical_call_id)
     DO UPDATE SET
       total_tokens = excluded.total_tokens,
       model = excluded.model,
       reasoning_effort = excluded.reasoning_effort,
       project_key = excluded.project_key";

/// 写入一条会话 cumulative 快照；同一事件的归档副本按主键覆盖。
pub(super) async fn execute_token_snapshot(
    transaction: &DatabaseTransaction,
    source_id: &str,
    generation: i64,
    snapshot: &SessionTokenSnapshot,
) -> Result<u64, LocalError> {
    let result = transaction
        .execute_raw(statement(
            UPSERT_TOKEN_SNAPSHOT_SQL,
            vec![
                source_id.into(),
                generation.into(),
                (&snapshot.thread_key).into(),
                snapshot.occurred_at_epoch_ms.into(),
                (&snapshot.logical_call_id).into(),
                to_sql_u64(snapshot.cumulative_total_tokens)?.into(),
                snapshot.model.clone().into(),
                snapshot.reasoning_effort.clone().into(),
                snapshot.project_key.clone().into(),
            ],
        ))
        .await
        .into_local_result()?;
    Ok(result.rows_affected())
}

/// 在当前事务中写入一批会话快照。
pub(crate) async fn insert_token_snapshots(
    transaction: &DatabaseTransaction,
    source_id: &str,
    generation: u64,
    snapshots: &mut Vec<SessionTokenSnapshot>,
) -> Result<u64, LocalError> {
    if snapshots.is_empty() {
        return Ok(0);
    }
    let generation = to_sql_u64(generation)?;
    let mut written = 0_u64;
    for snapshot in snapshots.drain(..) {
        execute_token_snapshot(transaction, source_id, generation, &snapshot)
            .await
            .into_local_result()?;
        written = written.saturating_add(1);
    }
    Ok(written)
}
