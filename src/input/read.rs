//! 无产品上下文的单文件获取入口，便于普通应用和安全的 FFI 包装层使用。

use super::{
    OpenedUsageFile, SourceFileRequest, open_claude_source, open_grok_source, open_rollout_source,
};
use crate::parsing::{
    ClaudeParser, CodexParser, GrokParser, ParseRequest, ParserDescriptor, ReadRange, UsageEmitter,
    UsageParser, read_observed_prefix,
};
use crate::readers::file_source::{
    RolloutFileInspection, RolloutProbeBudget, inspect_rollout_file,
};
use crate::readers::{claude::jsonl as claude, grok::jsonl as grok, jsonl};
use crate::{
    CancellationSignal, LocalError, LocalErrorKind, SessionTokenSnapshot, SourceProvenance,
};
use serde::Serialize;

/// 库内置的单文件格式；外部来源通过 UsageParser 扩展，不需要增加此枚举。
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum JsonlFormat {
    /// Codex rollout，包括归档会话。
    Codex,
    /// Claude Code / Desktop Agent transcript。
    Claude,
    /// Grok 已完成轮次 updates。
    Grok,
}

impl JsonlFormat {
    /// 返回内置格式的稳定解析描述。
    pub fn descriptor(self) -> ParserDescriptor {
        match self {
            Self::Codex => CodexParser.descriptor(),
            Self::Claude => ClaudeParser.descriptor(),
            Self::Grok => GrokParser.descriptor(),
        }
    }
}

/// 简单文件读取的资源预算；不包含界面窗口、保留天数或持久化状态。
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageReadOptions {
    /// 普通 JSONL 行的内存预算。
    pub max_line_bytes: usize,
}

impl Default for UsageReadOptions {
    /// 沿用各来源已有的 1 MiB 单行预算。
    fn default() -> Self {
        Self {
            max_line_bytes: crate::readers::DEFAULT_MAX_JSONL_LINE_BYTES,
        }
    }
}

/// 单文件获取报告的 JSON schema 版本，由包装层与客户端显式识别。
pub const USAGE_READ_REPORT_SCHEMA_VERSION: u16 = 1;

/// 可序列化的读取报告；不携带绝对路径、原始项目种子或数据库状态。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageReadReport {
    /// 报告的版本化 wire schema。
    pub schema_version: u16,
    /// 实际解析器和语义版本。
    pub parser: ParserDescriptor,
    /// 成功交付给调用方的用量观察数量。
    pub calls_emitted: u64,
    /// 本轮实际消耗的输入字节数。
    pub bytes_consumed: u64,
    /// 从文件起点起可提交的完整行字节数。
    pub committed_bytes: u64,
    /// 尚不完整的尾部字节数。
    pub trailing_bytes: u64,
    /// 下一次续读仍需丢弃未闭合的超长行。
    pub discarding_oversized_line: bool,
    /// 格式或用量质量告警总数。
    pub warning_count: u64,
    /// 本次是否在取消边界提前结束。
    pub cancelled: bool,
    /// Codex 兼容累计快照，消费方不得丢弃或当作额外调用累加。
    pub session_snapshots: Vec<SessionTokenSnapshot>,
}

impl UsageReadReport {
    /// 建立在取消状态下未打开任何来源的报告。
    fn cancelled(parser: ParserDescriptor) -> Self {
        Self {
            schema_version: USAGE_READ_REPORT_SCHEMA_VERSION,
            parser,
            calls_emitted: 0,
            bytes_consumed: 0,
            committed_bytes: 0,
            trailing_bytes: 0,
            discarding_oversized_line: false,
            warning_count: 0,
            cancelled: true,
            session_snapshots: Vec::new(),
        }
    }
}

/// 打开并完整读取本次观测的单个文件前缀，逐调用交付结果。
///
/// 这是无需索引、数据库和解析上下文知识的便捷入口。需要增量续读时使用
/// OpenedUsageFile 与 read_observed_prefix，并由消费方保存成功返回的上下文。
/// Codex 的快照随报告保留；调用记录流式输出，报告本身不承诺常量内存。
/// 此函数不创建线程，异步宿主应在自身拥有和回收的 blocking worker 中调用。
///
/// # Errors
/// 路径/链接/来源布局不安全、I/O 或消费方回调失败时返回错误。失败时消费方必须
/// 放弃本轮状态；外部数据库错误应由消费方保管，不作为来源错误写入库内状态。
pub fn read_usage_file(
    format: JsonlFormat,
    request: SourceFileRequest,
    options: UsageReadOptions,
    cancellation: &dyn CancellationSignal,
    emit: &mut UsageEmitter<'_>,
) -> Result<UsageReadReport, LocalError> {
    read_usage_file_observed(format, request, options, cancellation, emit).map(|(report, _)| report)
}

/// 为统计存储保留同一次打开的来源观察，原生种子不跨公开协议。
#[cfg_attr(not(feature = "storage"), allow(dead_code))]
pub(crate) fn read_usage_file_observed(
    format: JsonlFormat,
    request: SourceFileRequest,
    options: UsageReadOptions,
    cancellation: &dyn CancellationSignal,
    emit: &mut UsageEmitter<'_>,
) -> Result<(UsageReadReport, Option<super::UsageFileMetadata>), LocalError> {
    let parser = format.descriptor();
    if cancellation.is_cancelled() {
        return Ok((UsageReadReport::cancelled(parser), None));
    }
    let request = super::normalize_request(request)?;
    let opened = match format {
        JsonlFormat::Codex => {
            super::validate_rollout_request(&request)?;
            let mut budget =
                RolloutProbeBudget::new(1, jsonl::ROLLOUT_SIGNATURE_MAX_BYTES.saturating_add(1));
            match inspect_rollout_file(&request.file_path, cancellation, &mut budget) {
                RolloutFileInspection::Matched(source) => open_rollout_source(source, request)?,
                RolloutFileInspection::Cancelled => {
                    return Ok((UsageReadReport::cancelled(parser), None));
                }
                RolloutFileInspection::Rejected => {
                    return Err(LocalError::new(
                        LocalErrorKind::InvalidPath,
                        "source is not a signed rollout",
                    ));
                }
                RolloutFileInspection::Indeterminate | RolloutFileInspection::BudgetExhausted => {
                    return Err(LocalError::new(
                        LocalErrorKind::SourceUnavailable,
                        "rollout signature is unavailable",
                    ));
                }
            }
        }
        JsonlFormat::Claude => open_claude_source(request)?,
        JsonlFormat::Grok => open_grok_source(request)?,
    };
    let metadata = opened.metadata().clone();
    read_opened(format, opened, options, cancellation, emit).map(|report| (report, Some(metadata)))
}

/// 读取已经校验的同一文件句柄，并将来源报告投影成通用 wire 值。
fn read_opened(
    format: JsonlFormat,
    opened: OpenedUsageFile,
    options: UsageReadOptions,
    cancellation: &dyn CancellationSignal,
    emit: &mut UsageEmitter<'_>,
) -> Result<UsageReadReport, LocalError> {
    let (file, info) = opened.into_parts();
    // 原生相对路径可能包含编码工作目录，只把匿名技术标签交付给便利入口的消费者。
    let relative_label = format!("{}:{}", format.descriptor().id, info.source_id);
    let provenance = SourceProvenance {
        source_id: info.source_id,
        root_id: info.root_id,
        archived: info.archived,
        relative_label,
    };
    let range = ReadRange::new(0, info.observed_size)?;
    let (
        calls_emitted,
        bytes_consumed,
        committed_bytes,
        trailing_bytes,
        discarding_oversized_line,
        warning_count,
        cancelled,
        session_snapshots,
    ) = match format {
        JsonlFormat::Codex => {
            let parsed = read_observed_prefix(
                &CodexParser,
                file,
                range,
                ParseRequest::new(
                    jsonl::JsonlParseContext::new(&info.thread_seed),
                    &provenance,
                    cancellation,
                )
                .max_line_bytes(options.max_line_bytes),
                emit,
            )?;
            let r = parsed.report;
            (
                r.calls_emitted,
                r.bytes_consumed,
                r.checkpoint.committed_bytes,
                r.checkpoint.trailing_bytes,
                r.checkpoint.discarding_oversized_line,
                r.warnings.total(),
                r.cancelled,
                r.snapshots,
            )
        }
        JsonlFormat::Claude => {
            let parsed = read_observed_prefix(
                &ClaudeParser,
                file,
                range,
                ParseRequest::new(
                    claude::ClaudeJsonlParseContext::new(
                        &info.thread_seed,
                        info.project_seed.as_deref(),
                    ),
                    &provenance,
                    cancellation,
                )
                .max_line_bytes(options.max_line_bytes),
                emit,
            )?;
            let r = parsed.report;
            (
                r.calls_emitted,
                r.bytes_consumed,
                r.checkpoint.committed_bytes,
                r.checkpoint.trailing_bytes,
                r.checkpoint.discarding_oversized_line,
                r.warnings.total(),
                r.cancelled,
                Vec::new(),
            )
        }
        JsonlFormat::Grok => {
            let parsed = read_observed_prefix(
                &GrokParser,
                file,
                range,
                ParseRequest::new(
                    grok::GrokJsonlParseContext::new(
                        &info.thread_seed,
                        info.project_seed.as_deref(),
                    ),
                    &provenance,
                    cancellation,
                )
                .max_line_bytes(options.max_line_bytes),
                emit,
            )?;
            let r = parsed.report;
            (
                r.calls_emitted,
                r.bytes_consumed,
                r.checkpoint.committed_bytes,
                r.checkpoint.trailing_bytes,
                r.checkpoint.discarding_oversized_line,
                r.warnings.total(),
                r.cancelled,
                Vec::new(),
            )
        }
    };
    Ok(UsageReadReport {
        schema_version: USAGE_READ_REPORT_SCHEMA_VERSION,
        parser: format.descriptor(),
        calls_emitted,
        bytes_consumed,
        committed_bytes,
        trailing_bytes,
        discarding_oversized_line,
        warning_count,
        cancelled,
        session_snapshots,
    })
}
