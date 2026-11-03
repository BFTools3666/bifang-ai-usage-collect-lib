//! ZCode 桌面用量库的只读白名单投影；不查询消息、设置或认证表。

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Duration;

use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseTransaction, DbBackend, QueryResult,
    Statement, TransactionTrait,
};

use crate::readers::discovery::{metadata_is_link_like, walk_ancestors_for_link_component};
use crate::readers::file_source::same_opened_file;
use crate::{
    Confidence, LocalError, LocalErrorKind, SourceProvenance, TokenUsage, UsageCall, stable_id,
};

/// 单页最多保留的白名单记录数，避免一次加载整个来源数据库。
const PAGE_SIZE: usize = 256;

/// 持有一致的只读 SQLite 快照及原始普通文件身份；不提供任意 SQL 接口。
pub struct ZcodeUsageDatabase {
    transaction: DatabaseTransaction,
    file: File,
    path: PathBuf,
    cursor: String,
    since_epoch_ms: i64,
    provenance: SourceProvenance,
}

impl std::fmt::Debug for ZcodeUsageDatabase {
    /// 调试信息只显示来源散列，不暴露原始访问路径。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ZcodeUsageDatabase")
            .field("source_id", &self.provenance.source_id)
            .finish_non_exhaustive()
    }
}

impl ZcodeUsageDatabase {
    /// 打开已授权 db 根内唯一用量库，拒绝链接与缺失，不创建数据库。
    /// # Errors
    /// 路径不安全、来源不可读或 SQLite 不可用时返回脱敏错误。
    pub async fn open(root: &Path, root_id: &str, since_epoch_ms: i64) -> Result<Self, LocalError> {
        if !super::path_rules::is_glm_database_root(root)
            || !crate::is_safe_usage_filter_id(root_id)
        {
            return Err(invalid_path());
        }
        let root = crate::input::canonical_source_root(root)?;
        let path = root.join("db.sqlite");
        validate_path(&path)?;
        for name in ["db.sqlite-wal", "db.sqlite-shm"] {
            let sidecar = root.join(name);
            match fs::symlink_metadata(sidecar) {
                Ok(metadata) if !metadata.is_file() || metadata_is_link_like(&metadata) => {
                    return Err(invalid_path());
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(unavailable()),
            }
        }
        let file = File::open(&path).map_err(|_| unavailable())?;
        let mut options = ConnectOptions::new("sqlite://zcode-source");
        options
            .max_connections(1)
            .sqlx_logging(false)
            .connect_timeout(Duration::from_secs(5));
        let database_path = path.clone();
        options.map_sqlx_sqlite_opts(move |options| {
            options
                .filename(&database_path)
                .read_only(true)
                .create_if_missing(false)
                .busy_timeout(Duration::from_secs(5))
                .pragma("query_only", "ON")
                .pragma("trusted_schema", "OFF")
        });
        let connection = Database::connect(options)
            .await
            .map_err(|_| unavailable())?;
        let transaction = connection.begin().await.map_err(|_| unavailable())?;
        // 拒绝用同名 view 或虚拟表诱导读取来源库的其它表或外部资源。
        let table = transaction
            .query_one_raw(statement(
                "SELECT type, substr(sql, 1, 256) FROM sqlite_master WHERE name = 'model_usage'",
                vec![],
            ))
            .await
            .map_err(|_| unavailable())?
            .ok_or_else(unavailable)?;
        let table_type: String = table.try_get_by_index(0).map_err(|_| unavailable())?;
        let definition: String = table.try_get_by_index(1).map_err(|_| unavailable())?;
        if table_type != "table" || definition.to_ascii_uppercase().contains("VIRTUAL") {
            return Err(invalid_usage());
        }
        let reader = Self {
            transaction,
            file,
            path,
            cursor: String::new(),
            since_epoch_ms,
            provenance: SourceProvenance {
                source_id: stable_id("glm-database-source", root_id),
                root_id: root_id.to_owned(),
                relative_label: "db.sqlite:model_usage".to_owned(),
                archived: false,
            },
        };
        reader.validate_identity()?;
        Ok(reader)
    }

    /// 分页读取专用表的模型、时间与用量；空页表示一致快照已读完。
    /// # Errors
    /// 未知 schema、非法计数、身份冲突或来源被替换时失败，不返回不完整页面。
    pub async fn next_batch(&mut self) -> Result<Vec<UsageCall>, LocalError> {
        self.validate_identity()?;
        let rows = self.transaction.query_all_raw(statement(
            "SELECT CASE WHEN length(id) BETWEEN 1 AND 256 THEN id END,
                    CASE WHEN length(session_id) BETWEEN 1 AND 256 THEN session_id END,
                    CASE WHEN length(model_id) BETWEEN 1 AND 128 THEN model_id END,
                    started_at, CASE WHEN typeof(input_tokens) IN ('integer','null') THEN input_tokens ELSE -1 END, CASE WHEN typeof(cache_read_input_tokens) IN ('integer','null') THEN cache_read_input_tokens ELSE -1 END,
                    CASE WHEN typeof(cache_creation_input_tokens) IN ('integer','null') THEN cache_creation_input_tokens ELSE -1 END, CASE WHEN typeof(output_tokens) IN ('integer','null') THEN output_tokens ELSE -1 END, CASE WHEN typeof(computed_total_tokens) IN ('integer','null') THEN computed_total_tokens ELSE -1 END
             FROM model_usage WHERE started_at >= ?1 AND id COLLATE BINARY > ?2 ORDER BY id COLLATE BINARY LIMIT ?3",
            vec![self.since_epoch_ms.into(), self.cursor.clone().into(), (PAGE_SIZE as i64).into()]))
            .await.map_err(|_| unavailable())?;
        let mut calls = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get_by_index(0).map_err(|_| invalid_usage())?;
            if id <= self.cursor {
                return Err(invalid_usage());
            }
            let session: String = row.try_get_by_index(1).map_err(|_| invalid_usage())?;
            let model: String = row.try_get_by_index(2).map_err(|_| invalid_usage())?;
            if super::jsonl::safe_identity(&id).is_none()
                || super::jsonl::safe_identity(&session).is_none()
            {
                return Err(invalid_usage());
            }
            let model = super::jsonl::safe_model_label(&model).ok_or_else(invalid_usage)?;
            let occurred_at_epoch_ms: i64 = row.try_get_by_index(3).map_err(|_| invalid_usage())?;
            let usage = TokenUsage::new_with_component_availability(
                count(&row, 4)?,
                count(&row, 5)?,
                count(&row, 6)?,
                count(&row, 7)?,
                None,
                Some(count(&row, 8)?.ok_or_else(invalid_usage)?),
            )
            .map_err(|_| invalid_usage())?;
            self.cursor = id.clone();
            calls.push(UsageCall {
                logical_call_id: stable_id(
                    "glm-database-call",
                    &format!("{}\0{id}", self.provenance.root_id),
                ),
                occurred_at_epoch_ms,
                model: Some(model.to_owned()),
                reasoning_effort: None,
                project_key: None,
                project_label: None,
                thread_key: stable_id("glm-thread", &session),
                thread_label: None,
                usage,
                adapter_consistency_key: None,
                confidence: Confidence::Exact,
                provenance: vec![self.provenance.clone()],
            });
        }
        self.validate_identity()?;
        Ok(calls)
    }

    /// 确认读取期间数据库路径仍指向原来打开的普通文件。
    fn validate_identity(&self) -> Result<(), LocalError> {
        validate_path(&self.path)?;
        let opened = self.file.metadata().map_err(|_| unavailable())?;
        let current = fs::symlink_metadata(&self.path).map_err(|_| unavailable())?;
        if !same_opened_file(&self.file, &self.path, &opened, &current)
            .map_err(|_| unavailable())?
        {
            return Err(invalid_path());
        }
        Ok(())
    }
}

/// 校验普通文件及所有祖先组件，不跟随链接。
fn validate_path(path: &Path) -> Result<(), LocalError> {
    if walk_ancestors_for_link_component(path).map_err(|_| unavailable())? {
        return Err(invalid_path());
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| unavailable())?;
    if !metadata.is_file() || metadata_is_link_like(&metadata) {
        return Err(invalid_path());
    }
    Ok(())
}

/// 只接受 SQLite 的可空非负整数，拒绝浮点、溢出和负数。
fn count(row: &QueryResult, index: usize) -> Result<Option<u64>, LocalError> {
    let value: Option<i64> = row.try_get_by_index(index).map_err(|_| invalid_usage())?;
    value
        .map(|value| u64::try_from(value).map_err(|_| invalid_usage()))
        .transpose()
}

/// 构造固定参数化查询，不拼接任何外部输入。
fn statement(sql: &str, values: Vec<sea_orm::Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, sql, values)
}

/// 来源损坏或未知结构时只暴露稳定脱敏错误。
fn unavailable() -> LocalError {
    LocalError::new(
        LocalErrorKind::SourceUnavailable,
        "zcode usage database is unavailable",
    )
}
/// 不可信计量不能发布为完整统计。
fn invalid_usage() -> LocalError {
    LocalError::new(
        LocalErrorKind::InvalidUsage,
        "zcode database usage is invalid",
    )
}
/// 拒绝越界或链接型数据库来源。
fn invalid_path() -> LocalError {
    LocalError::new(
        LocalErrorKind::InvalidPath,
        "zcode database path is invalid",
    )
}

#[cfg(test)]
mod tests;
