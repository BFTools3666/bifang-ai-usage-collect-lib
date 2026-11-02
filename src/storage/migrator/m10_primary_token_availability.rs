//! 为 `usage_calls` 补充输入与输出 Token 的可得性标记列。

use sea_orm_migration::prelude::*;

const UP_SQL: &str = "ALTER TABLE usage_calls
   ADD COLUMN input_available INTEGER NOT NULL DEFAULT 1
     CHECK(input_available IN (0, 1));
 ALTER TABLE usage_calls
   ADD COLUMN output_available INTEGER NOT NULL DEFAULT 1
     CHECK(output_available IN (0, 1));";

const DOWN_SQL: &str = "ALTER TABLE usage_calls DROP COLUMN output_available;
 ALTER TABLE usage_calls DROP COLUMN input_available;";

/// 增加 `input_available`/`output_available` 两列；旧行的真值只能由 parser
/// generation 重扫恢复，默认 1 仅用于保持旧 schema 行可读取。
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    /// 追加两个带布尔约束的可用性标记。
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.get_connection().execute_unprepared(UP_SQL).await?;
        Ok(())
    }

    /// 回滚本次追加列。
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(DOWN_SQL)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::{ConnectionTrait, Database};

    use super::*;
    use crate::storage::migrator::Migrator;

    /// 验证旧行取得兼容默认值，同时数据库约束拒绝非布尔可用性标记。
    #[tokio::test]
    async fn adds_true_defaults_to_existing_rows_and_enforces_boolean_flags() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("in-memory database opens");
        connection
            .execute_unprepared(
                "CREATE TABLE usage_calls (row_id INTEGER PRIMARY KEY);
                 INSERT INTO usage_calls (row_id) VALUES (1);",
            )
            .await
            .expect("legacy fixture is created");

        Migration
            .up(&SchemaManager::new(&connection))
            .await
            .expect("availability migration succeeds");
        let row = connection
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DbBackend::Sqlite,
                "SELECT input_available, output_available
                 FROM usage_calls WHERE row_id = 1",
            ))
            .await
            .expect("migrated row query succeeds")
            .expect("migrated row exists");
        assert!(row.try_get_by_index::<bool>(0).unwrap());
        assert!(row.try_get_by_index::<bool>(1).unwrap());

        assert!(
            connection
                .execute_unprepared("UPDATE usage_calls SET input_available = 2 WHERE row_id = 1")
                .await
                .is_err()
        );
    }

    /// 验证真实 m9 行经历 m10 回滚再重放后，Token 数值保持不变，已丢失的
    /// 可用性位按旧行兼容契约恢复为 true。
    #[tokio::test]
    async fn down_then_up_restores_m9_rows_with_compatibility_defaults() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("in-memory database opens");
        Migrator::up(&connection, Some(9))
            .await
            .expect("real m9 schema is created");
        connection
            .execute_unprepared(
                "INSERT INTO source_roots (
                   root_id, access_path, alias, enabled, discovery_method
                 ) VALUES ('root-1', X'2F746D70', 'fixture', 1, 'explicit');
                 INSERT INTO source_files (
                   source_id, root_id, relative_label, file_identity, archived,
                   observed_size, modified_at_epoch_ms, parsed_offset,
                   trailing_bytes, oversized_tail, parser_version, generation,
                   ready, thread_key, call_sequence
                 ) VALUES (
                   'source-1', 'root-1', 'fixture.jsonl', 'identity-1', 0,
                   1, 1, 1, 0, 0, 1, 1, 1, 'thread-1', 1
                 );
                 INSERT INTO usage_calls (
                   source_id, generation, logical_call_id, occurred_at_epoch_ms,
                   thread_key, input_tokens, cached_input_tokens,
                   cache_write_input_tokens, output_tokens,
                   reasoning_output_tokens, total_tokens, total_is_derived,
                   confidence
                 ) VALUES (
                   'source-1', 1, 'call-1', 1, 'thread-1', 60, 10, 5, 40,
                   20, 100, 0, 'exact'
                 );",
            )
            .await
            .expect("m9 fixture row is inserted");

        Migrator::up(&connection, Some(1))
            .await
            .expect("m10 migration succeeds");
        connection
            .execute_unprepared(
                "UPDATE usage_calls
                 SET input_available = 0, output_available = 0
                 WHERE logical_call_id = 'call-1'",
            )
            .await
            .expect("m10 availability bits can be persisted");

        Migrator::down(&connection, Some(1))
            .await
            .expect("m10 migration rolls back to m9");
        let m9_columns = connection
            .query_all_raw(sea_orm::Statement::from_string(
                sea_orm::DbBackend::Sqlite,
                "PRAGMA table_info(usage_calls)",
            ))
            .await
            .expect("m9 columns can be inspected");
        let m9_column_names = m9_columns
            .iter()
            .map(|row| row.try_get_by_index::<String>(1).unwrap())
            .collect::<Vec<_>>();
        assert!(!m9_column_names.iter().any(|name| name == "input_available"));
        assert!(
            !m9_column_names
                .iter()
                .any(|name| name == "output_available")
        );

        Migrator::up(&connection, Some(1))
            .await
            .expect("m10 migration can be replayed");
        let row = connection
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DbBackend::Sqlite,
                "SELECT input_tokens, output_tokens, total_tokens,
                        input_available, output_available
                 FROM usage_calls WHERE logical_call_id = 'call-1'",
            ))
            .await
            .expect("replayed m10 row query succeeds")
            .expect("replayed m10 row exists");
        assert_eq!(row.try_get_by_index::<i64>(0).unwrap(), 60);
        assert_eq!(row.try_get_by_index::<i64>(1).unwrap(), 40);
        assert_eq!(row.try_get_by_index::<i64>(2).unwrap(), 100);
        assert!(row.try_get_by_index::<bool>(3).unwrap());
        assert!(row.try_get_by_index::<bool>(4).unwrap());
    }
}
