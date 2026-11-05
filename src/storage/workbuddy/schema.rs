//! WorkBuddy 派生库的独立格式，拒绝接管来源库或其它 Agent 的库。
use super::{WorkbuddyIndex, database_error};
use crate::{LocalError, LocalErrorKind};
use sea_orm::{ConnectionTrait, TransactionTrait};

/// ASCII AWB1 标识，只用于本产品的 WorkBuddy 派生库。
const APPLICATION_ID: i64 = 0x4157_4231;
/// checkpoint 与安全事件格式的首个稳定代次。
const SCHEMA_VERSION: i64 = 1;

pub(super) use crate::storage::sql::statement;

/// 校验独立格式及列形状；空库初始化先取得 writer 并复核，避免并发首次打开竞争。
pub(super) async fn ensure_schema(
    connection: &sea_orm::DatabaseConnection,
) -> Result<(), LocalError> {
    let check = connection.begin().await.map_err(database_error)?;
    let empty = is_empty(&check).await?;
    check.rollback().await.map_err(database_error)?;
    if empty {
        let transaction = connection
            .begin_with_options(sea_orm::TransactionOptions {
                sqlite_transaction_mode: Some(sea_orm::SqliteTransactionMode::Immediate),
                ..Default::default()
            })
            .await
            .map_err(database_error)?;
        if is_empty(&transaction).await? {
            transaction.execute_raw(statement(
                "CREATE TABLE workbuddy_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), state_json TEXT NOT NULL);
                 CREATE TABLE workbuddy_files (source_id TEXT PRIMARY KEY, checkpoint_json TEXT NOT NULL);
                 CREATE TABLE workbuddy_events (source_id TEXT NOT NULL REFERENCES workbuddy_files(source_id) ON DELETE CASCADE,
                   sequence INTEGER NOT NULL, occurred_at INTEGER NOT NULL, record_json TEXT NOT NULL CHECK(length(record_json)<=16384),
                   PRIMARY KEY(source_id,sequence));
                 CREATE INDEX workbuddy_events_time ON workbuddy_events(occurred_at);
                 PRAGMA application_id=1096237617; PRAGMA user_version=1;",vec![])).await.map_err(database_error)?;
        }
        transaction.commit().await.map_err(database_error)?;
    }
    connection
        .query_all_raw(statement(
            "SELECT source_id, sequence, occurred_at, record_json FROM workbuddy_events LIMIT 0",
            vec![],
        ))
        .await
        .map_err(database_error)?;
    connection
        .query_all_raw(statement(
            "SELECT source_id, checkpoint_json FROM workbuddy_files LIMIT 0",
            vec![],
        ))
        .await
        .map_err(database_error)?;
    connection
        .query_all_raw(statement(
            "SELECT singleton, state_json FROM workbuddy_state LIMIT 0",
            vec![],
        ))
        .await
        .map_err(database_error)?;
    Ok(())
}
/// 只有无对象且标记全空才可初始化；拒绝未知库、额外表、view 和触发器。
async fn is_empty<C: ConnectionTrait>(connection: &C) -> Result<bool, LocalError> {
    let identity = integer(connection, "PRAGMA application_id").await?;
    let version = integer(connection, "PRAGMA user_version").await?;
    let objects=connection.query_all_raw(statement("SELECT type,name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' AND type IN ('table','view','trigger') ORDER BY name",vec![])).await.map_err(database_error)?;
    if identity == 0 && version == 0 && objects.is_empty() {
        return Ok(true);
    }
    let names = objects
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get_by_index::<String>(0).map_err(database_error)?,
                row.try_get_by_index::<String>(1).map_err(database_error)?,
            ))
        })
        .collect::<Result<Vec<_>, LocalError>>()?;
    let expected = ["workbuddy_events", "workbuddy_files", "workbuddy_state"];
    if identity != APPLICATION_ID
        || version != SCHEMA_VERSION
        || names
            .iter()
            .map(|(kind, name)| (kind.as_str(), name.as_str()))
            .ne(expected.into_iter().map(|name| ("table", name)))
    {
        return Err(unsupported());
    }
    Ok(false)
}

/// 只读取固定 PRAGMA 数字，不允许调用者提供语句。
async fn integer<C: ConnectionTrait>(connection: &C, sql: &'static str) -> Result<i64, LocalError> {
    connection
        .query_one_raw(statement(sql, vec![]))
        .await
        .map_err(database_error)?
        .ok_or_else(unsupported)?
        .try_get_by_index(0)
        .map_err(database_error)
}
/// 未知库不迁移、不覆盖，也不降级到直接读取来源。
fn unsupported() -> LocalError {
    LocalError::new(
        LocalErrorKind::UnsupportedSchema,
        "workbuddy statistics database format is unsupported",
    )
}

impl WorkbuddyIndex {
    /// 仅清理过期的派生事件，保留来源检查点，避免下一轮重复读取全部文件。
    pub async fn prune_before(&self, cutoff_epoch_ms: i64) -> Result<u64, LocalError> {
        Ok(self
            .connection
            .execute_raw(statement(
                "DELETE FROM workbuddy_events WHERE occurred_at < ?1",
                vec![cutoff_epoch_ms.into()],
            ))
            .await
            .map_err(database_error)?
            .rows_affected())
    }
}
