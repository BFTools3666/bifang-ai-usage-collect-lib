//! 绑定当前格式统计库的 Agent，不接管旧库或其他业务数据库。

use sea_orm::{ConnectionTrait, DbBackend, Statement};

use super::{IntoLocalResult, LocalIndex};
use crate::{LocalError, LocalErrorKind, SourceClientKind};

/// 返回可持久化的物理 Agent 身份，WorkBuddy 保持只读。
pub(super) fn agent_label(client: SourceClientKind) -> Result<&'static str, LocalError> {
    match client {
        SourceClientKind::Codex => Ok("codex"),
        SourceClientKind::ClaudeCode => Ok("claude"),
        SourceClientKind::GrokBuildCli => Ok("grok"),
        SourceClientKind::Glm => Ok("glm"),
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

/// 测试便利入口才允许使用脱离物理 Agent 的夹具标签。
fn expected_label(client: Option<SourceClientKind>) -> Result<&'static str, LocalError> {
    match client {
        Some(client) => agent_label(client),
        #[cfg(any(test, feature = "test-util"))]
        None => Ok("test"),
        #[cfg(not(any(test, feature = "test-util")))]
        None => Err(mismatch()),
    }
}

/// 在新库建表事务内写入唯一 Agent 标签，不使用可接管既有数据的 upsert。
pub(super) async fn initialize<C: ConnectionTrait>(
    connection: &C,
    client: Option<SourceClientKind>,
) -> Result<(), LocalError> {
    let label = expected_label(client)?;
    connection
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO usage_agent(singleton, agent) VALUES (1, ?1)",
            [label.into()],
        ))
        .await
        .into_local_result()?;
    Ok(())
}

/// 在任何权限、WAL 或 schema 写入前验证当前格式库的唯一 Agent。
pub(super) async fn validate<C: ConnectionTrait>(
    connection: &C,
    client: Option<SourceClientKind>,
) -> Result<(), LocalError> {
    let row = connection.query_one_raw(Statement::from_string(DbBackend::Sqlite,
        "SELECT agent FROM usage_agent WHERE singleton = 1 AND (SELECT count(*) FROM usage_agent) = 1")).await.into_local_result()?.ok_or_else(mismatch)?;
    let stored: String = row.try_get_by_index(0).into_local_result()?;
    #[cfg(any(test, feature = "test-util"))]
    if client.is_none() || stored == "test" {
        return Ok(());
    }
    if stored != expected_label(client)? {
        return Err(mismatch());
    }
    Ok(())
}

impl LocalIndex {
    /// 根登记前验证其属于连接绑定的 Agent；仅测试便利入口允许夹具技术 ID。
    pub(super) fn validate_root_agent(&self, root_id: &str) -> Result<(), LocalError> {
        if let Some(client) = self.client {
            crate::validate_source_root_id(client, root_id).map_err(|_| mismatch())?;
        }
        Ok(())
    }
}
