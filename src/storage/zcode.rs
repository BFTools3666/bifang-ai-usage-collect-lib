//! 在既有扫描发布事务中替换 ZCode 专用用量表快照，包含 WAL 中的新记录。

use super::LocalIndex;
use crate::readers::glm::database::ZcodeUsageDatabase;
use crate::readers::glm::discovery::GlmDiscoveredRoot;
use crate::{
    CancellationSignal, LocalError, LocalErrorKind, SourceClientKind, SourceParseCheckpoint,
    stable_id,
};

/// 数据库扫描的可观察计数；取消时不发布新 generation。
#[derive(Debug, Clone, Copy, Default)]
pub struct ZcodeDatabaseIndexOutcome {
    /// 当前保留窗口内写入的新快照调用数。
    pub calls_added: u64,
    /// 本来源此前是否已有完整快照。
    pub rebuilt: bool,
    /// 协作取消状态。
    pub cancelled: bool,
}

impl LocalIndex {
    /// 已登记桌面来源持续拥有同安装的统计权威，完整扫描或手动根不得重新叠加日志。
    /// # Errors
    /// 候选日志与已登记或同批数据库重叠时返回来源不可用，外层保留已发布快照。
    pub async fn validate_zcode_root_authorities(
        &self,
        roots: &[GlmDiscoveredRoot],
    ) -> Result<(), LocalError> {
        let mut paths = self
            .registered_root_paths()
            .await?
            .into_iter()
            .map(|(_, path)| path)
            .collect::<Vec<_>>();
        paths.extend(roots.iter().map(|root| root.path.clone()));
        for root in roots {
            if let Some(database) =
                crate::readers::glm::path_rules::database_for_glm_rollout(&root.path)
                && paths.iter().any(|path| path == &database)
            {
                return Err(LocalError::new(
                    LocalErrorKind::SourceUnavailable,
                    "zcode database remains the authoritative source",
                ));
            }
        }
        Ok(())
    }

    /// 以分页只读事务重建桌面用量来源，沿用宿主拥有的整轮原子发布事务。
    /// # Errors
    /// 来源、身份、计量或写入失败时返回错误，由外层扫描事务整体回滚。
    pub async fn index_zcode_database(
        &mut self,
        root: &GlmDiscoveredRoot,
        ingest_since_epoch_ms: i64,
        cancellation: &dyn CancellationSignal,
    ) -> Result<ZcodeDatabaseIndexOutcome, LocalError> {
        if self.active_scan_transaction.is_none() {
            return Err(LocalError::new(
                LocalErrorKind::Database,
                "zcode database requires an active scan",
            ));
        }
        let mut source =
            ZcodeUsageDatabase::open(&root.path, &root.root_id, ingest_since_epoch_ms).await?;
        let source_id = stable_id("glm-database-source", &root.root_id);
        let previous = self.stored_source_file(&source_id).await?;
        let generation = previous
            .as_ref()
            .map_or(0, |source| source.generation)
            .checked_add(1)
            .ok_or_else(|| {
                LocalError::new(
                    LocalErrorKind::Overflow,
                    "zcode source generation overflowed",
                )
            })?;
        let version = SourceClientKind::Glm.parser_version();
        self.begin_source_file_checkpoint(
            &source_id,
            &root.root_id,
            "db.sqlite:model_usage",
            "zcode-model-usage-v1",
            false,
            version,
            "glm-database",
            None,
            None,
            None,
        )
        .await?;
        self.discard_calls_beyond_generation(&source_id, generation - 1)
            .await?;
        let mut outcome = ZcodeDatabaseIndexOutcome {
            rebuilt: previous.is_some(),
            ..Default::default()
        };
        loop {
            if cancellation.is_cancelled() {
                outcome.cancelled = true;
                self.discard_generation(&source_id, generation).await?;
                return Ok(outcome);
            }
            let mut batch = source.next_batch().await?;
            if batch.is_empty() {
                break;
            }
            outcome.calls_added = outcome.calls_added.saturating_add(
                self.insert_usage_batch(&source_id, generation, &mut batch)
                    .await?,
            );
        }
        if cancellation.is_cancelled() {
            outcome.cancelled = true;
            self.discard_generation(&source_id, generation).await?;
            return Ok(outcome);
        }
        self.commit_source_file_checkpoint(
            &source_id,
            &root.root_id,
            "db.sqlite:model_usage",
            "zcode-model-usage-v1",
            false,
            0,
            0,
            0,
            0,
            false,
            version,
            generation,
            &SourceParseCheckpoint {
                thread_key: "glm-database".to_owned(),
                project_key: None,
                project_label: None,
                thread_label: None,
                model: None,
                reasoning_effort: None,
                call_sequence: outcome.calls_added,
                adapter_state: None,
            },
            Some("{}"),
        )
        .await?;
        Ok(outcome)
    }
}
