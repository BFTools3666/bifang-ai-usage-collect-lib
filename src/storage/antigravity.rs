//! 按会话原子替换 Antigravity 计量快照，主库与 WAL 共同决定增量检查点。

use std::fs;
use std::path::Path;
use std::time::UNIX_EPOCH;

use super::LocalIndex;
use crate::readers::antigravity::database::AntigravityUsageDatabase;
use crate::readers::antigravity::discovery::{AntigravityDiscoveredRoot, is_conversation_file_name};
use crate::{CancellationSignal, LocalError, LocalErrorKind, SourceClientKind, SourceParseCheckpoint, stable_id};

/// 单个会话的可观察索引结果；取消不发布新 generation。
#[derive(Debug, Clone, Copy, Default)]
pub struct AntigravityDatabaseIndexOutcome {
    /// 本轮新 generation 中写入的调用数量。
    pub calls_added: u64,
    /// 既有会话已生成替换快照。
    pub rebuilt: bool,
    /// 主文件和 WAL 均未变化，复用了已有检查点。
    pub unchanged: bool,
    /// 协作取消导致本轮结果未发布。
    pub cancelled: bool,
}

impl LocalIndex {
    /// 在宿主拥有的扫描事务中导入一个会话；同一 UUID + step 永远只有一次调用。
    /// # Errors
    /// 来源漂移、计量或写入失败时不推进检查点，已有成功快照保持可用。
    pub async fn index_antigravity_database(
        &mut self,
        root: &AntigravityDiscoveredRoot,
        name: &str,
        since_epoch_ms: i64,
        force_rebuild: bool,
        cancel: &dyn CancellationSignal,
    ) -> Result<AntigravityDatabaseIndexOutcome, LocalError> {
        if self.active_scan_transaction.is_none() || self.client != Some(SourceClientKind::Antigravity) {
            return Err(LocalError::new(LocalErrorKind::Database, "antigravity import requires its active scan"));
        }
        if !is_conversation_file_name(name) { return Err(invalid_source()) }
        let fingerprint = source_fingerprint(&root.path.join(name))?;
        let mut source = AntigravityUsageDatabase::open(&root.path, &root.root_id, name, since_epoch_ms).await?;
        let source_id = stable_id("antigravity-source", &format!("{}\0{name}", root.root_id));
        let previous = self.stored_source_file(&source_id).await?;
        let version = SourceClientKind::Antigravity.parser_version();
        if !force_rebuild && previous.as_ref().is_some_and(|previous| {
            previous.file_identity == fingerprint && previous.parser_version == version
        }) {
            return Ok(AntigravityDatabaseIndexOutcome { unchanged: true, ..Default::default() });
        }
        let generation = previous.as_ref().map_or(0, |value| value.generation)
            .checked_add(1).ok_or_else(|| LocalError::new(LocalErrorKind::Overflow, "antigravity generation overflowed"))?;
        let thread = stable_id("antigravity-thread", &name.trim_end_matches(".db").to_ascii_lowercase());
        self.begin_source_file_checkpoint(&source_id, &root.root_id, name, &fingerprint,
            false, version, &thread, None, None, None).await?;
        self.discard_calls_beyond_generation(&source_id, generation - 1).await?;
        let imported = self.import_antigravity_pages(&source_id, generation, &mut source, cancel).await;
        let added = match imported {
            Ok(Some(added)) => added,
            Ok(None) => {
                self.discard_generation(&source_id, generation).await?;
                return Ok(AntigravityDatabaseIndexOutcome { cancelled: true, ..Default::default() });
            }
            Err(error) => {
                self.discard_generation(&source_id, generation).await?;
                return Err(error);
            }
        };
        if cancel.is_cancelled() || source_fingerprint(&root.path.join(name))? != fingerprint {
            self.discard_generation(&source_id, generation).await?;
            if cancel.is_cancelled() {
                return Ok(AntigravityDatabaseIndexOutcome { cancelled: true, ..Default::default() });
            }
            return Err(invalid_source());
        }
        self.commit_source_file_checkpoint(&source_id, &root.root_id, name, &fingerprint,
            false, 0, 0, 0, 0, false, version, generation,
            &SourceParseCheckpoint { thread_key: thread, project_key: None, project_label: None,
                thread_label: None, model: None, reasoning_effort: None, call_sequence: added,
                adapter_state: None }, Some("{}")).await?;
        Ok(AntigravityDatabaseIndexOutcome { calls_added: added, rebuilt: previous.is_some(), ..Default::default() })
    }

    /// 页间检查取消，避免长会话把取消响应延迟到全量读取结束。
    async fn import_antigravity_pages(
        &mut self,
        source_id: &str,
        generation: u64,
        source: &mut AntigravityUsageDatabase,
        cancel: &dyn CancellationSignal,
    ) -> Result<Option<u64>, LocalError> {
        while !cancel.is_cancelled() && source.read_model_batch().await? {}
        let mut added = 0_u64;
        while !cancel.is_cancelled() {
            let mut batch = source.next_batch().await?;
            if source.exhausted() { return Ok(Some(added)) }
            added = added.saturating_add(self.insert_usage_batch(source_id, generation, &mut batch).await?);
        }
        Ok(None)
    }
}

/// 纳秒修改时间与长度同时覆盖主库和 WAL；WAL 缺失不能遗漏仅存在于 WAL 的新增步骤。
fn source_fingerprint(path: &Path) -> Result<String, LocalError> {
    let mut parts = String::new();
    for suffix in ["", "-wal"] {
        let mut name = path.as_os_str().to_os_string(); name.push(suffix);
        match fs::symlink_metadata(std::path::PathBuf::from(name)) {
            Ok(metadata) if metadata.is_file() && !crate::readers::metadata_is_link_like(&metadata) => {
                let modified = metadata.modified().ok().and_then(|value| value.duration_since(UNIX_EPOCH).ok()).ok_or_else(invalid_source)?;
                parts.push_str(&format!("{suffix}:{}:{};", metadata.len(), modified.as_nanos()));
            }
            Err(error) if !suffix.is_empty() && error.kind() == std::io::ErrorKind::NotFound => parts.push_str("wal:absent;"),
            _ => return Err(invalid_source()),
        }
    }
    Ok(stable_id("antigravity-file", &parts))
}

/// 来源在快照期间变化时保留上轮结果，交给下一轮重新读取。
fn invalid_source() -> LocalError {
    LocalError::new(LocalErrorKind::SourceUnavailable, "antigravity conversation changed or is unavailable")
}
