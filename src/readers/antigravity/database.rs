//! 对每个会话库只读查询 steps.metadata 与 gen_metadata.data，不触碰正文列。

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Duration;

use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseTransaction, DbBackend, Statement, TransactionTrait};

use super::discovery::{is_conversation_file_name, is_conversation_root};
use super::metadata::{self, MAX_METADATA_BYTES};
use crate::{Confidence, LocalError, LocalErrorKind, SourceProvenance, UsageCall, stable_id};
use crate::readers::{metadata_is_link_like, same_opened_file};
use crate::readers::discovery::walk_ancestors_for_link_component;

/// 固定页大小限制一次从来源库加载的 blob 总量。
const PAGE_SIZE: i64 = 128;

/// 一致只读快照；路径和原始元数据不通过公开操作暴露。
pub struct AntigravityUsageDatabase {
    transaction: DatabaseTransaction,
    file: File,
    path: PathBuf,
    cursor: i64,
    exhausted: bool,
    model_cursor: i64,
    models: BTreeMap<u64, Option<String>>,
    session_id: String,
    since_epoch_ms: i64,
    provenance: SourceProvenance,
}

impl std::fmt::Debug for AntigravityUsageDatabase {
    /// 不把原始路径、会话内容或模型原始元数据带入日志。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AntigravityUsageDatabase")
            .field("source_id", &self.provenance.source_id)
            .finish_non_exhaustive()
    }
}

impl AntigravityUsageDatabase {
    /// 打开已授权 conversations 根内的一个 UUID.db，保留 WAL 中的已提交记录。
    /// # Errors
    /// 非本地普通文件、链接、非法 schema 或不可读来源返回稳定脱敏错误。
    pub async fn open(root: &Path, root_id: &str, name: &str, since_epoch_ms: i64) -> Result<Self, LocalError> {
        if !is_conversation_root(root) || !is_conversation_file_name(name) {
            return Err(invalid_path());
        }
        crate::validate_source_root_id(crate::SourceClientKind::Antigravity, root_id)
            .map_err(|_| invalid_path())?;
        let root = crate::input::canonical_source_root(root)?;
        let path = root.join(name);
        validate_path(&path)?;
        let file = File::open(&path).map_err(|_| unavailable())?;
        let mut options = ConnectOptions::new("sqlite://antigravity-source");
        options.max_connections(1).sqlx_logging(false).connect_timeout(Duration::from_secs(5));
        let database_path = path.clone();
        options.map_sqlx_sqlite_opts(move |options| {
            options.filename(&database_path).read_only(true).create_if_missing(false)
                .busy_timeout(Duration::from_secs(5)).pragma("query_only", "ON").pragma("trusted_schema", "OFF")
        });
        let connection = Database::connect(options).await.map_err(|_| unavailable())?;
        let transaction = connection.begin().await.map_err(|_| unavailable())?;
        for table in ["steps", "gen_metadata"] {
            let row = transaction.query_one_raw(statement(
                "SELECT type, substr(sql,1,256) FROM sqlite_master WHERE name = ?1", vec![table.into()],
            )).await.map_err(|_| unavailable())?.ok_or_else(invalid_usage)?;
            let kind: String = row.try_get_by_index(0).map_err(|_| invalid_usage())?;
            let sql: String = row.try_get_by_index(1).map_err(|_| invalid_usage())?;
            if kind != "table" || sql.to_ascii_uppercase().contains("VIRTUAL") { return Err(invalid_usage()) }
        }
        let session_id = name.trim_end_matches(".db").to_ascii_lowercase();
        let reader = Self {
            transaction, file, path, cursor: -1, exhausted: false, model_cursor: -1, models: BTreeMap::new(),
            session_id, since_epoch_ms,
            provenance: SourceProvenance {
                source_id: stable_id("antigravity-source", &format!("{root_id}\0{name}")),
                root_id: root_id.to_owned(), relative_label: name.to_owned(), archived: false,
            },
        };
        reader.validate_identity()?;
        Ok(reader)
    }

    /// 分页读取模型枚举名称；返回 false 后可开始读取用量，调用方在页间检查取消。
    /// # Errors
    /// 损坏或超预算的模型元数据不参与成功快照。
    pub async fn read_model_batch(&mut self) -> Result<bool, LocalError> {
        self.validate_identity()?;
        let rows = self.transaction.query_all_raw(statement(
            "SELECT idx, CASE WHEN typeof(data) = 'blob' AND length(data) <= ?1 THEN data END FROM gen_metadata WHERE idx > ?2 ORDER BY idx LIMIT ?3",
            vec![(MAX_METADATA_BYTES as i64).into(), self.model_cursor.into(), PAGE_SIZE.into()],
        )).await.map_err(|_| unavailable())?;
        if rows.is_empty() { return Ok(false) }
        for row in rows {
            self.model_cursor = row.try_get_by_index(0).map_err(|_| invalid_usage())?;
            // 完整 model I/O 可能含数 MiB 正文；只采用轻量 generation 标签快照。
            // 缺少可用标签不会丢失计量，步骤仍按真实模型枚举分别分组。
            let bytes: Option<Vec<u8>> = row.try_get_by_index(1).map_err(|_| invalid_usage())?;
            let Some(bytes) = bytes else { continue };
            if let Ok(Some((id, label))) = metadata::decode_model(&bytes) {
                if self.models.len() >= 256 && !self.models.contains_key(&id) { return Err(invalid_usage()) }
                self.models.entry(id).and_modify(|value| {
                    if value.as_ref() != Some(&label) { *value = None }
                }).or_insert(Some(label));
            }
        }
        self.validate_identity()?;
        Ok(true)
    }

    /// 读取一页步骤元数据；过滤后的空页须配合 `exhausted()` 判断实际 EOF。
    /// # Errors
    /// 结构漂移或计量非法时失败，调用方保留此前已发布的索引。
    pub async fn next_batch(&mut self) -> Result<Vec<UsageCall>, LocalError> {
        self.validate_identity()?;
        let rows = self.transaction.query_all_raw(statement(
            "SELECT idx, CASE WHEN typeof(metadata) = 'blob' AND length(metadata) <= ?1 THEN metadata END FROM steps WHERE idx > ?2 AND metadata IS NOT NULL ORDER BY idx LIMIT ?3",
            vec![(MAX_METADATA_BYTES as i64).into(), self.cursor.into(), PAGE_SIZE.into()],
        )).await.map_err(|_| unavailable())?;
        let mut calls = Vec::with_capacity(rows.len());
        for row in &rows {
            let index: i64 = row.try_get_by_index(0).map_err(|_| invalid_usage())?;
            if index <= self.cursor { return Err(invalid_usage()) }
            self.cursor = index;
            let bytes: Vec<u8> = row.try_get_by_index(1).map_err(|_| invalid_usage())?;
            let Some(step) = metadata::decode_step(&bytes)? else { continue };
            if step.occurred_at_epoch_ms < self.since_epoch_ms { continue }
            let model = self.models.get(&step.model_id).and_then(Clone::clone)
                .unwrap_or_else(|| format!("antigravity-model-{}", step.model_id));
            calls.push(UsageCall {
                logical_call_id: stable_id("antigravity-call", &format!("{}\0{index}", self.session_id)),
                occurred_at_epoch_ms: step.occurred_at_epoch_ms, model: Some(model), reasoning_effort: None,
                project_key: None, project_label: None,
                thread_key: stable_id("antigravity-thread", &self.session_id), thread_label: None,
                usage: step.usage, adapter_consistency_key: None, confidence: Confidence::Exact,
                provenance: vec![self.provenance.clone()],
            });
        }
        self.validate_identity()?;
        // 过滤后的空页不能被误认为来源 EOF；由单独的标记告知调用方是否继续。
        self.exhausted = rows.is_empty();
        Ok(calls)
    }

    /// 返回上次分页是否实际到达 SQLite 快照末尾。
    pub fn exhausted(&self) -> bool { self.exhausted }

    /// 在每页前后复核数据库及 WAL / SHM 均未被链接替换。
    fn validate_identity(&self) -> Result<(), LocalError> {
        validate_path(&self.path)?;
        let opened = self.file.metadata().map_err(|_| unavailable())?;
        let current = fs::symlink_metadata(&self.path).map_err(|_| unavailable())?;
        if !same_opened_file(&self.file, &self.path, &opened, &current).map_err(|_| unavailable())? {
            return Err(invalid_path());
        }
        Ok(())
    }
}

/// 拒绝数据库路径及其 sidecar 的链接和特殊对象，不创建不存在的文件。
fn validate_path(path: &Path) -> Result<(), LocalError> {
    if walk_ancestors_for_link_component(path).map_err(|_| unavailable())? { return Err(invalid_path()) }
    let metadata = fs::symlink_metadata(path).map_err(|_| unavailable())?;
    if !metadata.is_file() || metadata_is_link_like(&metadata) { return Err(invalid_path()) }
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_os_string(); sidecar.push(suffix);
        match fs::symlink_metadata(PathBuf::from(sidecar)) {
            Ok(metadata) if !metadata.is_file() || metadata_is_link_like(&metadata) => return Err(invalid_path()),
            Ok(_) => {},
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
            Err(_) => return Err(unavailable()),
        }
    }
    Ok(())
}

/// SQL 完全固定且所有输入均以参数绑定。
fn statement(sql: &str, values: Vec<sea_orm::Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Sqlite, sql, values)
}
/// 来源错误不泄漏路径与原文。
fn unavailable() -> LocalError { LocalError::new(LocalErrorKind::SourceUnavailable, "antigravity conversation database is unavailable") }
/// 结构和计量失败阻止发布不完整快照。
fn invalid_usage() -> LocalError { LocalError::new(LocalErrorKind::InvalidUsage, "antigravity conversation usage is invalid") }
/// 文件边界失败阻止访问其他目录。
fn invalid_path() -> LocalError { LocalError::new(LocalErrorKind::InvalidPath, "antigravity conversation path is invalid") }
