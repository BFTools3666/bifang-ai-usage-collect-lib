//! 删除没有任何生产读写方的 `provider_snapshots` 表。

use sea_orm_migration::prelude::*;

const UP_SQL: &str = "DROP TABLE IF EXISTS provider_snapshots;";

const DOWN_SQL: &str = "CREATE TABLE IF NOT EXISTS provider_snapshots (
   snapshot_kind TEXT PRIMARY KEY NOT NULL,
   observed_at_epoch_ms INTEGER NOT NULL,
   valid_until_epoch_ms INTEGER NOT NULL,
   normalized_json TEXT NOT NULL
 );";

/// 丢弃遗留快照表；其内容从未被生产路径读取，也不属于可重建的用量事实。
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    /// 删除表；已不存在时保持幂等。
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.get_connection().execute_unprepared(UP_SQL).await?;
        Ok(())
    }

    /// 回滚时按原结构重建空表。
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(DOWN_SQL)
            .await?;
        Ok(())
    }
}
