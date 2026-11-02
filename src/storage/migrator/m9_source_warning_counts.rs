//! 为来源 checkpoint 增加脱敏、分类化的解析警告计数。

use sea_orm::{ConnectionTrait, Statement};
use sea_orm_migration::prelude::*;

const UP_SQL: &str = "ALTER TABLE source_files ADD COLUMN warning_counts_json TEXT;";
const DOWN_SQL: &str = "ALTER TABLE source_files DROP COLUMN warning_counts_json;";

/// 新增可空警告快照；旧行保持 `NULL`，下次扫描会保守地重建来源。
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    /// 保存各 adapter 的具名警告分类与计数，不保存原始记录内容。
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        if table_exists(connection, "source_files").await? {
            connection.execute_unprepared(UP_SQL).await?;
        }
        Ok(())
    }

    /// 回滚来源警告快照字段。
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        if table_exists(connection, "source_files").await? {
            connection.execute_unprepared(DOWN_SQL).await?;
        }
        Ok(())
    }
}

/// 探测桥接旧 schema 中可能不存在的来源表。
async fn table_exists(connection: &impl ConnectionTrait, name: &str) -> Result<bool, DbErr> {
    Ok(connection
        .query_one_raw(Statement::from_sql_and_values(
            connection.get_database_backend(),
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [name.into()],
        ))
        .await?
        .is_some())
}
