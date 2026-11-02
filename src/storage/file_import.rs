//! 单文件完整观测与统计事务导入；来源读取和数据库提交分离。

use super::LocalIndex;
use crate::input::{
    JsonlFormat, SourceFileRequest, UsageFileMetadata, UsageReadOptions, UsageReadReport,
};
use crate::parsing::UsageEmitter;
use crate::{
    CancellationSignal, CoverageState, DiscoveryMethod, LocalError, LocalErrorKind,
    SourceClientKind, SourceParseCheckpoint, UsageCall,
};
use std::path::PathBuf;

/// 已解析但尚未提交的单文件统计，丢弃此值不会改变数据库。
pub struct PreparedFileIndex {
    client: SourceClientKind,
    root_path: PathBuf,
    metadata: Option<UsageFileMetadata>,
    report: UsageReadReport,
    calls: Vec<UsageCall>,
}

impl std::fmt::Debug for PreparedFileIndex {
    /// 只输出安全报告和计数，不把来源路径或派生种子写入诊断。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedFileIndex")
            .field("client", &self.client)
            .field("report", &self.report)
            .field("call_count", &self.calls.len())
            .finish_non_exhaustive()
    }
}

/// 与现有索引写入相同的有限批次，避免大文件触及 SQLite 绑定变量上限。
const IMPORT_BATCH_SIZE: usize = 256;

impl PreparedFileIndex {
    /// 在宿主拥有的 blocking worker 中验证并只读解析一个来源文件。
    ///
    /// 事件在提交前交付，消费者只能在提交成功后视为已持久化。
    /// # Errors
    /// 路径、来源读取或回调失败时不产生可提交结果。
    pub fn read(
        format: JsonlFormat,
        mut request: SourceFileRequest,
        options: UsageReadOptions,
        cancel: &dyn CancellationSignal,
        emit: &mut UsageEmitter<'_>,
    ) -> Result<Self, LocalError> {
        let client = match format {
            JsonlFormat::Codex => SourceClientKind::Codex,
            JsonlFormat::Claude => SourceClientKind::ClaudeCode,
            JsonlFormat::Grok => SourceClientKind::GrokBuildCli,
        };
        crate::validate_source_root_id(client, &request.root_id).map_err(|_| {
            LocalError::new(
                LocalErrorKind::InvalidPath,
                "source root identifier is invalid",
            )
        })?;
        if !cancel.is_cancelled() {
            request.root_path = crate::input::canonical_source_root(&request.root_path)?;
        }
        let root_path = request.root_path.clone();
        let mut calls = Vec::new();
        let (report, metadata) = crate::input::read_usage_file_observed(
            format,
            request,
            options,
            cancel,
            &mut |call| {
                emit(call.clone())?;
                calls.push(call);
                Ok(())
            },
        )?;
        Ok(Self {
            client,
            root_path,
            metadata,
            report,
            calls,
        })
    }

    /// 返回不包含原生路径种子的来源读取报告。
    pub fn report(&self) -> &UsageReadReport {
        &self.report
    }
}

impl LocalIndex {
    /// 原子替换该文件的统计事实和 checkpoint，保留其他文件及其他 Agent 库。
    ///
    /// 完整重读后使用新 generation 替换旧结果，重复导入不会重复累计。
    /// 返回 `false` 表示取消且没有发布；失败自动回滚本次全部统计变化。
    /// # Errors
    /// Agent 不匹配、扫描忙、数值溢出或数据库失败时返回稳定错误。
    pub async fn commit_file(
        &mut self,
        prepared: PreparedFileIndex,
        cancel: &dyn CancellationSignal,
    ) -> Result<bool, LocalError> {
        if self.client != Some(prepared.client) {
            return Err(LocalError::new(
                LocalErrorKind::InvalidUsage,
                "statistics database agent does not match",
            ));
        }
        if cancel.is_cancelled() || prepared.report.cancelled {
            return Ok(false);
        }
        self.begin_scan_attempt().await?;
        let result = self.stage_file(prepared).await;
        match result {
            Ok(()) if !cancel.is_cancelled() => {
                self.publish_scan_attempt().await?;
                Ok(true)
            }
            Ok(()) => {
                self.abort_scan_attempt().await?;
                Ok(false)
            }
            Err(error) => {
                self.abort_scan_attempt().await?;
                Err(error)
            }
        }
    }

    /// 把单次来源的完整结果暂存到尚未发布的扫描事务。
    async fn stage_file(&mut self, mut prepared: PreparedFileIndex) -> Result<(), LocalError> {
        let info = prepared.metadata.ok_or_else(|| {
            LocalError::new(
                LocalErrorKind::SourceUnavailable,
                "source observation is unavailable",
            )
        })?;
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| {
                    LocalError::new(
                        LocalErrorKind::InvalidUsage,
                        "observation time is unavailable",
                    )
                })?
                .as_millis(),
        )
        .map_err(|_| LocalError::new(LocalErrorKind::Overflow, "observation time overflowed"))?;
        let scan_id = self.begin_scan("file-import", now).await?;
        self.register_root_fields(
            &info.root_id,
            &prepared.root_path,
            prepared.client.default_source_root_alias(),
            DiscoveryMethod::Registered,
        )
        .await?;
        self.claim_discovered_roots_for_current_scan(std::slice::from_ref(&info.root_id))
            .await?;
        let generation =
            self.stored_source_file(&info.source_id)
                .await?
                .map_or(Ok(0), |source| {
                    source.generation.checked_add(1).ok_or_else(|| {
                        LocalError::new(LocalErrorKind::Overflow, "source generation overflowed")
                    })
                })?;
        let label = format!("{}:{}", prepared.report.parser.id, info.source_id);
        // 这里只保存完整观测边界，不伪造来源专属增量上下文；缺失警告快照使 GUI 安全重建。
        let checkpoint = SourceParseCheckpoint {
            thread_key: crate::stable_id("thread", &info.thread_seed),
            project_key: None,
            project_label: None,
            thread_label: None,
            model: None,
            reasoning_effort: None,
            call_sequence: prepared.report.calls_emitted,
            adapter_state: None,
        };
        self.begin_source_file_checkpoint(
            &info.source_id,
            &info.root_id,
            &label,
            &info.fingerprint,
            info.archived,
            self.parser_version,
            &checkpoint.thread_key,
            None,
            None,
            None,
        )
        .await?;
        let mut calls_added = 0_u64;
        let mut warning_count = prepared.report.warning_count;
        let mut calls = prepared.calls.into_iter();
        loop {
            let mut batch: Vec<_> = calls.by_ref().take(IMPORT_BATCH_SIZE).collect();
            if batch.is_empty() {
                break;
            }
            let added = if prepared.client == SourceClientKind::ClaudeCode {
                let outcome = self
                    .insert_claude_usage_batch(&info.source_id, generation, &mut batch)
                    .await?;
                warning_count = warning_count
                    .checked_add(outcome.regressed)
                    .ok_or_else(|| {
                        LocalError::new(LocalErrorKind::Overflow, "source warning count overflowed")
                    })?;
                outcome.added
            } else {
                self.insert_usage_batch(&info.source_id, generation, &mut batch)
                    .await?
            };
            calls_added = calls_added.checked_add(added).ok_or_else(|| {
                LocalError::new(LocalErrorKind::Overflow, "source call count overflowed")
            })?;
        }
        self.insert_usage_and_snapshots(
            &info.source_id,
            generation,
            &mut Vec::new(),
            &mut prepared.report.session_snapshots,
        )
        .await?;
        self.commit_source_file_checkpoint(
            &info.source_id,
            &info.root_id,
            &label,
            &info.fingerprint,
            info.archived,
            info.observed_size,
            info.modified_at_epoch_ms,
            prepared.report.committed_bytes,
            prepared.report.trailing_bytes,
            prepared.report.discarding_oversized_line,
            self.parser_version,
            generation,
            &checkpoint,
            None,
        )
        .await?;
        // 单文件导入没有枚举整个根，不能清除其他来源的覆盖缺口。
        let coverage = CoverageState::Partial;
        self.record_root_coverage(&info.root_id, coverage).await?;
        self.finish_background_index_roots(&[info.root_id]).await?;
        self.finish_scan(
            &scan_id,
            now,
            "partial",
            false,
            1,
            calls_added,
            warning_count,
        )
        .await
    }
}
