//! 新产品统计数据库的唯一格式；仅初始化空库，不兼容或升级旧库。

use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, SqliteTransactionMode, Statement,
    TransactionOptions, TransactionTrait,
};

use super::{IntoLocalResult, LocalError, LocalErrorKind, binding};
use crate::SourceClientKind;

/// 统计数据库的固定格式身份，ASCII `AUS1`。
pub(super) const APPLICATION_ID: i64 = 0x4155_5331;
/// 当前统计格式仅接受版本一，其他版本必须重新扫描到新数据库。
pub(super) const SCHEMA_VERSION: i64 = 1;

/// 一次建立最终表、布尔约束、外键及统计索引，不创建历史迁移记账表。
pub(super) const CREATE_SCHEMA_SQL: &str = "
CREATE TABLE source_roots (
    root_id TEXT PRIMARY KEY NOT NULL,
    access_path BLOB NOT NULL,
    alias TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
    discovery_method TEXT NOT NULL,
    last_coverage_state TEXT,
    is_primary INTEGER NOT NULL DEFAULT 0
        CHECK(is_primary IN (0, 1) AND (is_primary = 0 OR enabled = 1)),
    activation_state TEXT NOT NULL DEFAULT 'confirmed_unindexed'
);
CREATE UNIQUE INDEX source_roots_single_primary_idx
    ON source_roots(is_primary) WHERE is_primary = 1;
CREATE TABLE source_files (
    source_id TEXT PRIMARY KEY NOT NULL,
    root_id TEXT NOT NULL REFERENCES source_roots(root_id) ON DELETE CASCADE,
    relative_label TEXT NOT NULL,
    file_identity TEXT NOT NULL,
    archived INTEGER NOT NULL CHECK(archived IN (0, 1)),
    observed_size INTEGER NOT NULL,
    modified_at_epoch_ms INTEGER NOT NULL,
    parsed_offset INTEGER NOT NULL,
    trailing_bytes INTEGER NOT NULL,
    oversized_tail INTEGER NOT NULL CHECK(oversized_tail IN (0, 1)),
    parser_version INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    ready INTEGER NOT NULL CHECK(ready IN (0, 1)),
    thread_key TEXT NOT NULL,
    project_key TEXT,
    model TEXT,
    reasoning_effort TEXT,
    call_sequence INTEGER NOT NULL,
    project_label TEXT,
    thread_label TEXT,
    token_snapshots_ready INTEGER NOT NULL DEFAULT 0 CHECK(token_snapshots_ready IN (0, 1)),
    adapter_state TEXT,
    warning_counts_json TEXT
);
CREATE TABLE usage_calls (
    source_id TEXT NOT NULL REFERENCES source_files(source_id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    logical_call_id TEXT NOT NULL,
    occurred_at_epoch_ms INTEGER NOT NULL,
    model TEXT,
    reasoning_effort TEXT,
    project_key TEXT,
    thread_key TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    cached_input_tokens INTEGER NOT NULL,
    cache_write_input_tokens INTEGER,
    output_tokens INTEGER NOT NULL,
    reasoning_output_tokens INTEGER NOT NULL,
    total_tokens INTEGER NOT NULL,
    total_is_derived INTEGER NOT NULL CHECK(total_is_derived IN (0, 1)),
    confidence TEXT NOT NULL,
    cached_input_available INTEGER NOT NULL DEFAULT 1 CHECK(cached_input_available IN (0, 1)),
    reasoning_output_available INTEGER NOT NULL DEFAULT 1 CHECK(reasoning_output_available IN (0, 1)),
    adapter_consistency_key TEXT,
    project_label TEXT,
    thread_label TEXT,
    input_available INTEGER NOT NULL DEFAULT 1 CHECK(input_available IN (0, 1)),
    output_available INTEGER NOT NULL DEFAULT 1 CHECK(output_available IN (0, 1)),
    PRIMARY KEY(source_id, generation, logical_call_id)
);
CREATE INDEX usage_calls_logical_call_idx ON usage_calls(logical_call_id);
CREATE INDEX usage_calls_occurred_idx ON usage_calls(occurred_at_epoch_ms);
CREATE TABLE usage_token_snapshots (
    source_id TEXT NOT NULL REFERENCES source_files(source_id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    thread_key TEXT NOT NULL,
    occurred_at_epoch_ms INTEGER NOT NULL,
    logical_call_id TEXT NOT NULL,
    total_tokens INTEGER NOT NULL,
    model TEXT,
    reasoning_effort TEXT,
    project_key TEXT,
    PRIMARY KEY(source_id, generation, thread_key, occurred_at_epoch_ms, logical_call_id)
);
CREATE INDEX usage_token_snapshots_thread_time_idx
    ON usage_token_snapshots(thread_key, occurred_at_epoch_ms);
CREATE TABLE scan_runs (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id TEXT UNIQUE,
    mode TEXT NOT NULL,
    started_at_epoch_ms INTEGER NOT NULL,
    finished_at_epoch_ms INTEGER,
    status TEXT NOT NULL,
    cancelled INTEGER NOT NULL CHECK(cancelled IN (0, 1)),
    files_scanned INTEGER NOT NULL,
    calls_added INTEGER NOT NULL,
    warning_count INTEGER NOT NULL
);
CREATE TABLE usage_agent (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    agent TEXT NOT NULL
);
CREATE TABLE discovery_candidates (
    candidate_id TEXT PRIMARY KEY NOT NULL,
    access_path BLOB NOT NULL,
    strategy TEXT NOT NULL,
    evidence TEXT NOT NULL
);
";

/// 检查统计库格式标记与封闭表集合，只有真正空库允许初始化。
async fn is_empty<C: ConnectionTrait>(connection: &C) -> Result<bool, LocalError> {
    let application_id = read_integer_pragma(connection, "PRAGMA application_id").await?;
    let version = read_integer_pragma(connection, "PRAGMA user_version").await?;
    let tables = connection
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
        ))
        .await
        .into_local_result()?;
    if tables.is_empty() && application_id == 0 && version == 0 {
        let objects = connection
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'",
            ))
            .await
            .into_local_result()?;
        if objects.is_empty() {
            return Ok(true);
        }
    }
    if application_id != APPLICATION_ID || version != SCHEMA_VERSION {
        return Err(unsupported());
    }
    let names = tables
        .into_iter()
        .map(|row| row.try_get_by_index::<String>(0).into_local_result())
        .collect::<Result<Vec<_>, _>>()?;
    let expected = [
        "discovery_candidates",
        "scan_runs",
        "source_files",
        "source_roots",
        "sqlite_sequence",
        "usage_agent",
        "usage_calls",
        "usage_token_snapshots",
    ];
    if names.iter().map(String::as_str).ne(expected) {
        return Err(unsupported());
    }
    Ok(false)
}

/// 读取 SQLite 的格式标记，不接收调用方拼接的 SQL。
async fn read_integer_pragma<C: ConnectionTrait>(
    connection: &C,
    sql: &'static str,
) -> Result<i64, LocalError> {
    connection
        .query_one_raw(Statement::from_string(DbBackend::Sqlite, sql))
        .await
        .into_local_result()?
        .ok_or_else(unsupported)?
        .try_get_by_index(0)
        .into_local_result()
}

/// 返回不泄漏文件位置的未知或历史格式错误。
fn unsupported() -> LocalError {
    LocalError::new(
        LocalErrorKind::UnsupportedSchema,
        "statistics database format is unsupported; rescan into a new database",
    )
}

/// 原子初始化空库或只读复核当前格式和 Agent，不执行任何兼容迁移。
pub(super) async fn ensure_schema(
    connection: &DatabaseConnection,
    client: Option<SourceClientKind>,
) -> Result<(), LocalError> {
    let check = connection.begin().await.into_local_result()?;
    let current = match is_empty(&check).await {
        Ok(false) => binding::validate(&check, client).await,
        Ok(true) => {
            check.rollback().await.into_local_result()?;
            return initialize_empty(connection, client).await;
        }
        Err(error) => Err(error),
    };
    check.rollback().await.into_local_result()?;
    current
}

/// 首次初始化先取得 writer，再次核对空库以消解并发首次打开。
async fn initialize_empty(
    connection: &DatabaseConnection,
    client: Option<SourceClientKind>,
) -> Result<(), LocalError> {
    let transaction = connection
        .begin_with_options(TransactionOptions {
            sqlite_transaction_mode: Some(SqliteTransactionMode::Immediate),
            ..TransactionOptions::default()
        })
        .await
        .into_local_result()?;
    let result = async {
        if is_empty(&transaction).await? {
            transaction.execute_unprepared(CREATE_SCHEMA_SQL).await.into_local_result()?;
            transaction.execute_unprepared(&format!("PRAGMA application_id = {APPLICATION_ID}; PRAGMA user_version = {SCHEMA_VERSION};")).await.into_local_result()?;
            binding::initialize(&transaction, client).await?;
        } else {
            binding::validate(&transaction, client).await?;
        }
        Ok(())
    }.await;
    match result {
        Ok(()) => transaction.commit().await.into_local_result(),
        Err(error) => {
            transaction.rollback().await.into_local_result()?;
            Err(error)
        }
    }
}
