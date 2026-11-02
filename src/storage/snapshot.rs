//! 在本机索引的单一快照事务中读取 canonical 调用与安全数据根记录。

use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

use crate::{
    CanonicalUsageSet, LocalIndexState, RootActivationState, SessionTokenSnapshot,
    SourceProvenance, attach_session_snapshots, canonicalize_usage_calls,
};

use super::call_store::{from_sql_u64, row_to_usage_call};
use super::{
    DiscoveryMethod, IntoLocalResult, LocalError, LocalErrorKind, RootRecord, UsageSnapshot,
    coverage_state_from_label,
};

/// 为会话快照查询构造带绑定值的 SQLite 语句。
fn statement(sql: &str, values: Vec<sea_orm::Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, sql, values)
}

/// 在一个事务中读取调用与根记录，读取完成后只提交事务边界；三次查询共享
/// 同一数据库快照，避免并发写入让调用记录和数据根信息互相矛盾。
pub(crate) async fn load_usage_snapshot(
    connection: &sea_orm::DatabaseConnection,
    parser_version: u32,
) -> Result<UsageSnapshot, LocalError> {
    let transaction = connection.begin().await.into_local_result()?;
    let canonical = load_canonical_calls(&transaction, parser_version)
        .await
        .into_local_result()?;
    let index_state = load_local_index_state(&transaction, canonical.calls.len(), parser_version)
        .await
        .into_local_result()?;
    let roots = load_root_records(&transaction, parser_version)
        .await
        .into_local_result()?;
    transaction.commit().await.into_local_result()?;
    Ok(UsageSnapshot {
        canonical,
        index_state,
        roots,
    })
}

/// 只读取给定发生时间下界后的 canonical 调用，根和索引状态仍来自同一事务。
pub(crate) async fn load_usage_snapshot_since(
    connection: &sea_orm::DatabaseConnection,
    parser_version: u32,
    cutoff_epoch_ms: i64,
) -> Result<UsageSnapshot, LocalError> {
    let transaction = connection.begin().await.into_local_result()?;
    let canonical = load_canonical_calls_since(&transaction, parser_version, cutoff_epoch_ms)
        .await
        .into_local_result()?;
    let index_state = load_local_index_state(&transaction, canonical.calls.len(), parser_version)
        .await
        .into_local_result()?;
    let roots = load_root_records(&transaction, parser_version)
        .await
        .into_local_result()?;
    transaction.commit().await.into_local_result()?;
    Ok(UsageSnapshot {
        canonical,
        index_state,
        roots,
    })
}

/// 用同一只读事务取得调用与累计快照，避免两次查询之间混入另一代提交。
pub(super) async fn load_canonical_snapshot(
    connection: &sea_orm::DatabaseConnection,
    parser_version: u32,
) -> Result<CanonicalUsageSet, LocalError> {
    let transaction = connection.begin().await.into_local_result()?;
    let canonical = load_canonical_calls(&transaction, parser_version).await?;
    transaction.commit().await.into_local_result()?;
    Ok(canonical)
}

/// 在一个有界只读事务内取得索引状态，不装载完整调用集合。
pub(super) async fn load_index_state(
    connection: &sea_orm::DatabaseConnection,
    parser_version: u32,
) -> Result<LocalIndexState, LocalError> {
    let transaction = connection.begin().await.into_local_result()?;
    let has_usage = has_current_parser_usage(&transaction, parser_version).await?;
    #[cfg(test)]
    super::read_snapshot_tests::pause_after_index_usage_query().await;
    let state =
        load_local_index_state(&transaction, usize::from(has_usage), parser_version).await?;
    transaction.commit().await.into_local_result()?;
    Ok(state)
}

/// 从调用方当前连接或事务读取是否已有当前解析代次的可展示事实。
pub(super) async fn has_current_parser_usage<C: ConnectionTrait>(
    connection: &C,
    parser_version: u32,
) -> Result<bool, LocalError> {
    let row = connection
        .query_one_raw(statement(
            "SELECT
           EXISTS(SELECT 1 FROM usage_calls u
             JOIN source_files f ON f.source_id = u.source_id
             WHERE f.parser_version = ?1 AND f.ready = 1 AND f.generation = u.generation)
           OR EXISTS(SELECT 1 FROM usage_token_snapshots s
             JOIN source_files f ON f.source_id = s.source_id
             WHERE f.parser_version = ?1 AND f.ready = 1 AND f.generation = s.generation)",
            vec![i64::from(parser_version).into()],
        ))
        .await
        .into_local_result()?
        .ok_or_else(|| LocalError::new(LocalErrorKind::Database, "usage state is unreadable"))?;
    Ok(row.try_get_by_index::<i64>(0).into_local_result()? != 0)
}

/// 从当前数据库快照推导索引四态，旧 parser 来源优先暴露为需要重扫。
pub(super) async fn load_local_index_state<C: ConnectionTrait>(
    connection: &C,
    canonical_call_count: usize,
    parser_version: u32,
) -> Result<LocalIndexState, LocalError> {
    let row = connection
        .query_one_raw(statement(
            "SELECT
               (SELECT COUNT(*) FROM scan_runs WHERE status = 'completed'),
               (SELECT COUNT(*) FROM source_files f
                JOIN source_roots r ON r.root_id = f.root_id
                WHERE f.ready = 1 AND f.parser_version = ?1 AND r.enabled = 1),
               (SELECT COUNT(*) FROM source_files f
                JOIN source_roots r ON r.root_id = f.root_id
                WHERE f.ready = 1 AND f.parser_version <> ?1 AND r.enabled = 1
                  AND NOT EXISTS (
                    SELECT 1 FROM source_files c
                    WHERE c.root_id = f.root_id
                      AND c.ready = 1
                      AND c.parser_version = ?1
                  )),
               (SELECT COUNT(*) FROM source_roots r
                WHERE r.enabled = 1
                  AND (r.last_coverage_state IS NULL
                       OR r.last_coverage_state IN ('partial', 'cancelled', 'failed'))),
               (SELECT COUNT(*) FROM source_roots WHERE enabled = 1)",
            vec![i64::from(parser_version).into()],
        ))
        .await
        .into_local_result()?
        .ok_or_else(|| {
            LocalError::new(
                LocalErrorKind::Database,
                "index state summary is unreadable",
            )
        })?;
    let completed_scan_count = from_sql_u64(row.try_get_by_index(0).into_local_result()?)?;
    let current_source_count = from_sql_u64(row.try_get_by_index(1).into_local_result()?)?;
    let stale_source_count = from_sql_u64(row.try_get_by_index(2).into_local_result()?)?;
    let enabled_unscanned_root_count = from_sql_u64(row.try_get_by_index(3).into_local_result()?)?;
    let enabled_root_count = from_sql_u64(row.try_get_by_index(4).into_local_result()?)?;

    Ok(crate::local_index_state_from_facts(
        completed_scan_count,
        current_source_count,
        stale_source_count,
        enabled_unscanned_root_count,
        enabled_root_count,
        canonical_call_count,
    ))
}

/// 从指定数据库快照读取不含绝对路径的数据根记录。
pub(crate) async fn load_root_records<C: ConnectionTrait>(
    connection: &C,
    parser_version: u32,
) -> Result<Vec<RootRecord>, LocalError> {
    let rows = connection
        .query_all_raw(statement(
            "SELECT r.root_id, r.alias, r.enabled, r.is_primary, r.discovery_method,
                    r.activation_state, r.last_coverage_state,
                    (SELECT COUNT(*) FROM source_files f
                     WHERE f.root_id = r.root_id AND f.ready = 1
                       AND f.parser_version = ?1),
                    (SELECT COUNT(*) FROM usage_calls u
                     JOIN source_files f ON f.source_id = u.source_id
                     WHERE f.root_id = r.root_id AND f.ready = 1
                       AND f.parser_version = ?1
                       AND f.generation = u.generation)
             FROM source_roots r
             ORDER BY alias, root_id",
            vec![i64::from(parser_version).into()],
        ))
        .await
        .into_local_result()?;
    let mut roots = Vec::with_capacity(rows.len());
    for row in rows {
        let root_id: String = row.try_get_by_index(0).into_local_result()?;
        let alias: String = row.try_get_by_index(1).into_local_result()?;
        let enabled: bool = row.try_get_by_index(2).into_local_result()?;
        let is_primary: bool = row.try_get_by_index(3).into_local_result()?;
        let method_label: String = row.try_get_by_index(4).into_local_result()?;
        let activation_label: String = row.try_get_by_index(5).into_local_result()?;
        let coverage_label: Option<String> = row.try_get_by_index(6).into_local_result()?;
        let source_file_count: i64 = row.try_get_by_index(7).into_local_result()?;
        let call_observation_count: i64 = row.try_get_by_index(8).into_local_result()?;
        let discovery_method = DiscoveryMethod::from_label(&method_label).ok_or_else(|| {
            LocalError::new(LocalErrorKind::Database, "root discovery method is invalid")
        })?;
        let last_coverage = coverage_label
            .as_deref()
            .and_then(coverage_state_from_label);
        roots.push(RootRecord {
            root_id,
            alias,
            enabled,
            activation_state: RootActivationState::from_label(&activation_label).ok_or_else(
                || LocalError::new(LocalErrorKind::Database, "root activation state is invalid"),
            )?,
            is_primary,
            discovery_method,
            last_coverage,
            source_file_count: from_sql_u64(source_file_count)?,
            call_observation_count: from_sql_u64(call_observation_count)?,
        });
    }
    Ok(roots)
}

/// 从指定数据库快照读取当前 generation 调用并由 core 执行 canonical 去重。
pub(crate) async fn load_canonical_calls<C: ConnectionTrait>(
    connection: &C,
    parser_version: u32,
) -> Result<CanonicalUsageSet, LocalError> {
    load_canonical_calls_since(connection, parser_version, i64::MIN).await
}

/// 从指定时间下界读取当前 generation 调用，避免视图装载窗口外全历史。
pub(crate) async fn load_canonical_calls_since<C: ConnectionTrait>(
    connection: &C,
    parser_version: u32,
    cutoff_epoch_ms: i64,
) -> Result<CanonicalUsageSet, LocalError> {
    let rows = connection
        .query_all_raw(statement(
            "SELECT u.logical_call_id, u.occurred_at_epoch_ms, u.model,
                    u.reasoning_effort, u.project_key, u.thread_key,
                    u.project_label, u.thread_label,
                    u.input_tokens, u.cached_input_tokens, u.cache_write_input_tokens,
                    u.output_tokens, u.reasoning_output_tokens, u.total_tokens,
                    u.total_is_derived, u.confidence,
                    u.input_available, u.cached_input_available,
                    u.output_available, u.reasoning_output_available,
                    u.adapter_consistency_key,
                    f.source_id, f.root_id, f.relative_label, f.archived
             FROM usage_calls u
             JOIN source_files f ON f.source_id = u.source_id
             JOIN source_roots r ON r.root_id = f.root_id
             WHERE f.ready = 1 AND f.parser_version = ?1
               AND r.enabled = 1
               AND f.generation = u.generation
               AND u.occurred_at_epoch_ms >= ?2
             ORDER BY u.occurred_at_epoch_ms, u.logical_call_id, f.root_id, f.source_id",
            vec![i64::from(parser_version).into(), cutoff_epoch_ms.into()],
        ))
        .await
        .into_local_result()?;
    #[cfg(test)]
    super::read_snapshot_tests::pause_after_calls_query().await;
    let mut calls = Vec::with_capacity(rows.len());
    for row in &rows {
        calls.push(row_to_usage_call(row)?);
    }
    let snapshot_rows = connection
        .query_all_raw(statement(
            "SELECT s.thread_key, s.occurred_at_epoch_ms, s.total_tokens, s.logical_call_id,
                    s.model, s.reasoning_effort, s.project_key,
                    f.source_id, f.root_id, f.relative_label, f.archived
             FROM usage_token_snapshots s
             JOIN source_files f ON f.source_id = s.source_id
             JOIN source_roots r ON r.root_id = f.root_id
             WHERE f.ready = 1 AND f.parser_version = ?1
               AND r.enabled = 1
               AND f.generation = s.generation
               AND s.occurred_at_epoch_ms >= ?2
             ORDER BY s.occurred_at_epoch_ms, s.logical_call_id, s.thread_key",
            vec![i64::from(parser_version).into(), cutoff_epoch_ms.into()],
        ))
        .await
        .into_local_result()?;
    let mut snapshots = Vec::with_capacity(snapshot_rows.len());
    for row in &snapshot_rows {
        snapshots.push(row_to_session_snapshot(row)?);
    }
    Ok(attach_session_snapshots(
        canonicalize_usage_calls(calls),
        snapshots,
    ))
}

/// 将数据库行严格解码为会话 Token 快照，拒绝越界或损坏数值。
fn row_to_session_snapshot(row: &sea_orm::QueryResult) -> Result<SessionTokenSnapshot, LocalError> {
    Ok(SessionTokenSnapshot {
        thread_key: row.try_get_by_index(0).into_local_result()?,
        occurred_at_epoch_ms: row.try_get_by_index(1).into_local_result()?,
        cumulative_total_tokens: from_sql_u64(row.try_get_by_index(2).into_local_result()?)?,
        logical_call_id: row.try_get_by_index(3).into_local_result()?,
        model: row.try_get_by_index(4).into_local_result()?,
        reasoning_effort: row.try_get_by_index(5).into_local_result()?,
        project_key: row.try_get_by_index(6).into_local_result()?,
        provenance: vec![SourceProvenance {
            source_id: row.try_get_by_index(7).into_local_result()?,
            root_id: row.try_get_by_index(8).into_local_result()?,
            relative_label: row.try_get_by_index(9).into_local_result()?,
            archived: row.try_get_by_index(10).into_local_result()?,
        }],
    })
}
