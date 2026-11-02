//! 持久绑定统计库的 Agent，避免不同解析器把事实混入同一物理库。

use super::{IntoLocalResult, LocalIndex};
use crate::{LocalError, LocalErrorKind, SourceClientKind};
use sea_orm::{ConnectionTrait, DbBackend, Statement};

/// 返回可持久化的物理 Agent 身份，WorkBuddy 保持只读。
pub(super) fn agent_label(client: SourceClientKind) -> Result<&'static str, LocalError> {
    match client {
        SourceClientKind::Codex => Ok("codex"),
        SourceClientKind::ClaudeCode => Ok("claude"),
        SourceClientKind::GrokBuildCli => Ok("grok"),
        SourceClientKind::WorkBuddy => Err(mismatch()),
    }
}

/// 返回固定错误，不暴露库路径或已有数据。
fn mismatch() -> LocalError {
    LocalError::new(
        LocalErrorKind::InvalidUsage,
        "statistics database agent does not match",
    )
}

/// 在迁移事务内核对历史根身份，并幂等保存唯一 Agent。
pub(super) async fn bind<C: ConnectionTrait>(
    connection: &C,
    client: SourceClientKind,
) -> Result<(), LocalError> {
    let label = agent_label(client)?;
    connection.execute_unprepared("CREATE TABLE IF NOT EXISTS usage_agent (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), agent TEXT NOT NULL)").await.into_local_result()?;
    connection.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO usage_agent(singleton, agent) VALUES (1, ?1) ON CONFLICT(singleton) DO NOTHING", [label.into()])).await.into_local_result()?;
    let row = connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT agent FROM usage_agent WHERE singleton = 1",
        ))
        .await
        .into_local_result()?
        .ok_or_else(mismatch)?;
    if row.try_get_by_index::<String>(0).into_local_result()? != label {
        return Err(mismatch());
    }
    let roots_exist = connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'source_roots'",
        ))
        .await
        .into_local_result()?
        .is_some();
    if roots_exist {
        for row in connection
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT root_id FROM source_roots",
            ))
            .await
            .into_local_result()?
        {
            crate::validate_source_root_id(
                client,
                &row.try_get_by_index::<String>(0).into_local_result()?,
            )
            .map_err(|_| mismatch())?;
        }
    }
    Ok(())
}

impl LocalIndex {
    /// 根登记前验证其属于连接绑定的 Agent；旧测试夹具保持原始输入。
    pub(super) fn validate_root_agent(&self, root_id: &str) -> Result<(), LocalError> {
        if let Some(client) = self.client {
            crate::validate_source_root_id(client, root_id).map_err(|_| mismatch())?;
        }
        Ok(())
    }
}

/// 在修改日志模式、权限或 schema 前拒绝 GUI 业务库和其他未知数据库。
pub(super) async fn validate_statistics_schema<C: ConnectionTrait>(
    connection: &C,
) -> Result<(), LocalError> {
    let rows = connection
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT name FROM sqlite_master WHERE type = 'table'",
        ))
        .await
        .into_local_result()?;
    let allowed = [
        "source_roots",
        "source_files",
        "usage_calls",
        "scan_runs",
        "provider_snapshots",
        "usage_token_snapshots",
        "seaql_migrations",
        "sqlite_sequence",
        "usage_agent",
    ];
    for row in rows {
        let name: String = row.try_get_by_index(0).into_local_result()?;
        if !allowed.contains(&name.as_str()) {
            return Err(LocalError::new(
                LocalErrorKind::UnsupportedSchema,
                "database is not an Agent statistics index",
            ));
        }
    }
    Ok(())
}
