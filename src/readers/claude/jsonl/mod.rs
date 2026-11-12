//! Claude Code transcript 的有界流式解析器；只反序列化用量事实，不接触消息正文。

use std::io::Read;

use crate::{
    Confidence, SourceClientKind, SourceProvenance, TokenUsage, UsageCall,
    claude_project_display_label,
};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use super::super::CancellationToken;
use super::super::LocalError;
use super::super::discovery::stable_id;
use super::super::file_source::map_source_io_error;
use super::super::jsonl_lines::{JsonlRecord, LineFlow, read_jsonl_records};
use record::{LineOutcome, TranscriptRecord};

mod record;

/// Claude transcript 独立解析语义版本；与共享来源模型声明的解析代次一致。
pub const CLAUDE_PARSER_VERSION: u32 = SourceClientKind::ClaudeCode.parser_version();

/// 保存增量扫描可安全提交的完整行偏移，不保留半行字节。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClaudeJsonlCheckpoint {
    /// 已确认可提交的完整行字节数（不含尾部半行）。
    pub committed_bytes: u64,
    /// 当前缓冲中尚未凑成完整行的尾部字节数。
    pub trailing_bytes: u64,
    /// 标识尾部正在丢弃一条超长行的剩余内容。
    pub discarding_oversized_line: bool,
}

/// 仅包含格式与数据质量计数，不包含 transcript 内容。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaudeJsonlWarningCounts {
    /// 无法解析为合法 JSON 的行数。
    pub malformed_lines: u64,
    /// 超出单行字节上限而被丢弃的行数。
    pub oversized_lines: u64,
    /// 未识别事件类型的行数。
    pub unknown_events: u64,
    /// 缺少判定用量所需字段的行数。
    pub missing_usage: u64,
    /// 用量字段存在但数值不合法的行数。
    pub invalid_usage: u64,
    /// 时间戳缺失或无法解析的行数。
    pub invalid_timestamps: u64,
    /// 因数值回退被拒绝合并的观察次数。
    pub regressed_observations: u64,
}

impl ClaudeJsonlWarningCounts {
    /// 汇总全部质量告警计数，供覆盖状态判定使用。
    pub fn total(self) -> u64 {
        self.malformed_lines
            .saturating_add(self.oversized_lines)
            .saturating_add(self.unknown_events)
            .saturating_add(self.missing_usage)
            .saturating_add(self.invalid_usage)
            .saturating_add(self.invalid_timestamps)
            .saturating_add(self.regressed_observations)
    }

    /// 合并同一 transcript 追加片段与历史 generation 的质量计数。
    pub fn merge_append(self, appended: Self) -> Self {
        Self {
            malformed_lines: self
                .malformed_lines
                .saturating_add(appended.malformed_lines),
            oversized_lines: self
                .oversized_lines
                .saturating_add(appended.oversized_lines),
            unknown_events: self.unknown_events.saturating_add(appended.unknown_events),
            missing_usage: self.missing_usage.saturating_add(appended.missing_usage),
            invalid_usage: self.invalid_usage.saturating_add(appended.invalid_usage),
            invalid_timestamps: self
                .invalid_timestamps
                .saturating_add(appended.invalid_timestamps),
            regressed_observations: self
                .regressed_observations
                .saturating_add(appended.regressed_observations),
        }
    }
}

/// 跨增量片段持久化的安全上下文；所有原始 ID 与项目目录名只保留散列键。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeJsonlParseContext {
    /// 由原始线程种子派生出的稳定散列键。
    pub thread_key: String,
    /// 由原始项目种子派生出的稳定散列键；无项目关联时为空。
    pub project_key: Option<String>,
    /// 安全展示用的项目标签；不含原始项目路径。
    pub project_label: Option<String>,
}

impl ClaudeJsonlParseContext {
    /// 从原始线程/项目种子派生出散列键与安全展示标签。
    pub fn new(thread_seed: &str, project_seed: Option<&str>) -> Self {
        Self {
            thread_key: stable_id("claude-thread", thread_seed),
            project_key: project_seed.map(|value| stable_id("claude-project", value)),
            project_label: project_seed.and_then(claude_project_display_label),
        }
    }
}

/// 一次流式解析的安全结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeJsonlParseReport {
    /// 本次流式解析实际发出的调用条数。
    pub calls_emitted: u64,
    /// 本次调用读取的原始字节总数。
    pub bytes_consumed: u64,
    /// 可安全用于下次增量续读的行边界检查点。
    pub checkpoint: ClaudeJsonlCheckpoint,
    /// 本次解析累计的格式与数据质量告警。
    pub warnings: ClaudeJsonlWarningCounts,
    /// 标识解析是否因取消信号提前终止。
    pub cancelled: bool,
}

/// 流式读取 Claude transcript；相邻同响应的逐步增长观察只向 sink 发送最后一条。
///
/// 非相邻或跨增量片段的相同逻辑调用由索引层使用同一 `logical_call_id` 再执行
/// 单调 upsert，避免用文件级无界内存换取去重。
pub fn parse_claude_jsonl_stream<R, F>(
    reader: R,
    max_line_bytes: usize,
    context: &mut ClaudeJsonlParseContext,
    provenance: &SourceProvenance,
    cancellation: &dyn crate::CancellationSignal,
    mut on_call: F,
) -> Result<ClaudeJsonlParseReport, LocalError>
where
    R: Read,
    F: FnMut(UsageCall) -> Result<(), LocalError>,
{
    let mut warnings = ClaudeJsonlWarningCounts::default();
    let mut pending = None::<UsageCall>;
    let mut calls_emitted = 0_u64;

    let progress = read_jsonl_records(
        reader,
        max_line_bytes,
        || cancellation.is_cancelled(),
        map_source_io_error,
        |record| {
            match parse_line(record, context, provenance) {
                LineOutcome::Call(call) => {
                    merge_or_emit(
                        *call,
                        &mut pending,
                        &mut warnings,
                        &mut calls_emitted,
                        &mut on_call,
                    )?;
                }
                LineOutcome::Ignored => {}
                LineOutcome::Unknown => {
                    warnings.unknown_events = warnings.unknown_events.saturating_add(1)
                }
                LineOutcome::Malformed => {
                    warnings.malformed_lines = warnings.malformed_lines.saturating_add(1)
                }
                LineOutcome::Oversized => {
                    warnings.oversized_lines = warnings.oversized_lines.saturating_add(1)
                }
                LineOutcome::MissingUsage => {
                    warnings.missing_usage = warnings.missing_usage.saturating_add(1)
                }
                LineOutcome::InvalidUsage => {
                    warnings.invalid_usage = warnings.invalid_usage.saturating_add(1)
                }
                LineOutcome::InvalidTimestamp => {
                    warnings.invalid_timestamps = warnings.invalid_timestamps.saturating_add(1)
                }
            }
            Ok(LineFlow::Continue)
        },
    )?;

    if let Some(call) = pending {
        on_call(call)?;
        calls_emitted = calls_emitted.saturating_add(1);
    }
    Ok(ClaudeJsonlParseReport {
        calls_emitted,
        bytes_consumed: progress.bytes_consumed,
        checkpoint: ClaudeJsonlCheckpoint {
            committed_bytes: progress.committed_bytes,
            trailing_bytes: progress.trailing_bytes,
            discarding_oversized_line: progress.discarding_oversized_line,
        },
        warnings,
        cancelled: progress.cancelled,
    })
}

/// 合并相邻同一逻辑调用的连续观察，只在遇到不同调用时才真正 emit 前一条。
// `pending` 是一个只装“最多一条”的前瞻缓冲区，不是无界的去重 Map：
// Claude transcript 文件里，同一条 assistant 消息在流式生成过程中会
// 被连续写入多行、一行比一行更完整。merge_or_emit 每次只跟“刚缓存的
// 上一条”比较——如果新记录和上一条是同一个 logical_call_id，就地合并
// （保留其中数值更完整/单调的那份，不立即向下游发送）；一旦遇到不同
// 的 logical_call_id，说明连续重复观察已经结束，才把缓存的那条真正
// emit 出去。这样只用 O(1) 额外内存就能压掉“连续相邻重复”的绝大多数
// 冗余，真正跨行、跨文件的重复交给 call_store.rs 里的单调 upsert 兜底。
fn merge_or_emit<F>(
    call: UsageCall,
    pending: &mut Option<UsageCall>,
    warnings: &mut ClaudeJsonlWarningCounts,
    calls_emitted: &mut u64,
    on_call: &mut F,
) -> Result<(), LocalError>
where
    F: FnMut(UsageCall) -> Result<(), LocalError>,
{
    let Some(previous) = pending.take() else {
        *pending = Some(call);
        return Ok(());
    };
    if previous.logical_call_id != call.logical_call_id {
        on_call(previous)?;
        *calls_emitted = calls_emitted.saturating_add(1);
        *pending = Some(call);
        return Ok(());
    }
    if usage_is_monotonic(&previous, &call) {
        *pending = Some(call);
    } else {
        warnings.regressed_observations = warnings.regressed_observations.saturating_add(1);
        *pending = Some(previous);
    }
    Ok(())
}

/// 判定两次相邻观察在各 Token 计数上是否逐项非递减（即单调增长）。
// 任一侧缺少缓存读取或缓存写入时直接判定非单调；merge_or_emit 遇到
// 非单调结果会整体丢弃更新的观察（既不合并也不单独 emit），只保留仍在
// pending 中的上一条，靠 regressed_observations 计数暴露这种丢弃，
// 而不是把字段更少的观察当新调用写入。
fn usage_is_monotonic(previous: &UsageCall, next: &UsageCall) -> bool {
    let (Some(previous_cached), Some(next_cached)) = (
        previous.usage.cached_input_tokens,
        next.usage.cached_input_tokens,
    ) else {
        return false;
    };
    let (Some(previous_input), Some(next_input), Some(previous_output), Some(next_output)) = (
        previous.usage.input_tokens,
        next.usage.input_tokens,
        previous.usage.output_tokens,
        next.usage.output_tokens,
    ) else {
        return false;
    };
    let (Some(previous_cache_write), Some(next_cache_write)) = (
        previous.usage.cache_write_input_tokens,
        next.usage.cache_write_input_tokens,
    ) else {
        return false;
    };
    let previous_uncached = previous_input
        .checked_sub(previous_cached)
        .and_then(|value| value.checked_sub(previous_cache_write));
    let next_uncached = next_input
        .checked_sub(next_cached)
        .and_then(|value| value.checked_sub(next_cache_write));
    previous_uncached
        .zip(next_uncached)
        .is_some_and(|(previous, next)| next >= previous)
        && next_input >= previous_input
        && next_cached >= previous_cached
        && next_cache_write >= previous_cache_write
        && next_output >= previous_output
        && next.usage.total_tokens >= previous.usage.total_tokens
}

/// 解析单行 JSON 为分类结果，只提取判定用量所需的最小字段。
fn parse_line(
    record: JsonlRecord<TranscriptRecord>,
    context: &mut ClaudeJsonlParseContext,
    provenance: &SourceProvenance,
) -> LineOutcome {
    let record = match record {
        JsonlRecord::Parsed(record) => record,
        JsonlRecord::Empty => return LineOutcome::Unknown,
        JsonlRecord::Malformed => return LineOutcome::Malformed,
        JsonlRecord::OversizedMalformed => return LineOutcome::Oversized,
    };
    let kind = record.kind.as_deref();
    if kind != Some("assistant") {
        // `cost-state` 只是 session 级累计快照（与逐条 assistant usage 重复计数），
        // 其余是会话/权限/文件历史等元数据，都不携带需要单独入库的用量。
        return if matches!(
            kind,
            Some(
                "user"
                    | "system"
                    | "summary"
                    | "progress"
                    | "queue-operation"
                    | "file-history-snapshot"
                    | "file-history-delta"
                    | "attachment"
                    | "last-prompt"
                    | "mode"
                    | "permission-mode"
                    | "atis-latch"
                    | "bridge-session"
                    | "cost-state"
                    | "ai-title"
                    | "api-request"
                    | "api-request-blob"
                    | "api-request-shape"
                    | "tools-used"
            )
        ) {
            LineOutcome::Ignored
        } else {
            LineOutcome::Unknown
        };
    }
    let session_id = match record.session_id.as_deref().filter(|value| valid_id(value)) {
        Some(value) => value,
        None => return LineOutcome::MissingUsage,
    };
    let occurred_at_epoch_ms = match record
        .timestamp
        .as_deref()
        .and_then(|value| value.parse::<Timestamp>().ok())
        .map(|value| value.as_millisecond())
    {
        Some(value) => value,
        None => return LineOutcome::InvalidTimestamp,
    };
    let reasoning_effort = record.effort.as_deref().and_then(safe_effort_label);
    let message = match record.message {
        Some(message) => message,
        None => return LineOutcome::MissingUsage,
    };
    let message_id = match message.id.as_deref().filter(|value| valid_id(value)) {
        Some(value) => value,
        None => return LineOutcome::MissingUsage,
    };
    let usage = match message.usage {
        Some(usage) => usage,
        None => return LineOutcome::MissingUsage,
    };
    let Some(uncached_input) = usage.input_tokens else {
        return LineOutcome::MissingUsage;
    };
    let Some(output_tokens) = usage.output_tokens else {
        return LineOutcome::MissingUsage;
    };
    let Some(cached_input) = usage.cache_read_input_tokens else {
        return LineOutcome::MissingUsage;
    };
    let Some(cache_write) = usage.cache_creation_input_tokens else {
        return LineOutcome::MissingUsage;
    };
    let total_input = match uncached_input
        .checked_add(cached_input)
        .and_then(|value| value.checked_add(cache_write))
    {
        Some(value) => value,
        None => return LineOutcome::InvalidUsage,
    };
    let usage = match TokenUsage::new_with_availability(
        total_input,
        Some(cached_input),
        Some(cache_write),
        output_tokens,
        None,
        None,
    ) {
        Ok(usage) => usage,
        Err(_) => return LineOutcome::InvalidUsage,
    };
    let logical_call_id = stable_id(
        "claude-call",
        &format!("{}\u{0}{session_id}\u{0}{message_id}", context.thread_key),
    );
    LineOutcome::Call(Box::new(UsageCall {
        logical_call_id,
        occurred_at_epoch_ms,
        model: message.model.as_deref().and_then(safe_model_label),
        reasoning_effort,
        project_key: context.project_key.clone(),
        thread_key: context.thread_key.clone(),
        project_label: context.project_label.clone(),
        thread_label: None,
        usage,
        adapter_consistency_key: None,
        confidence: Confidence::Exact,
        provenance: vec![provenance.clone()],
    }))
}

/// 校验 session/message ID 非空、有界且不含控制字符。
fn valid_id(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

/// 校验模型名安全可展示后返回去除首尾空白的副本，否则返回 `None`。
fn safe_model_label(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()
        && trimmed.len() <= 128
        && !trimmed.contains(['/', '\\'])
        && !trimmed.chars().any(char::is_control))
    .then(|| trimmed.to_owned())
}

/// 只接受短小技术推理强度标签，拒绝路径、正文空白与异常长字符串。
fn safe_effort_label(value: &str) -> Option<String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
    {
        return None;
    }
    Some(value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造测试用的固定来源标注。
    fn provenance() -> SourceProvenance {
        SourceProvenance {
            source_id: "source-test".to_owned(),
            root_id: "root-test".to_owned(),
            relative_label: "project/session.jsonl".to_owned(),
            archived: false,
        }
    }

    /// 验证三条相邻单调观察被合并为一条调用，且缓存计入总输入 Token。
    #[test]
    fn coalesces_three_monotonic_observations_and_maps_cache_into_total_input() {
        let input = concat!(
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:00Z\",\"message\":{\"id\":\"msg-a\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":20,\"cache_creation_input_tokens\":30,\"output_tokens\":1},\"content\":\"private bait\"}}\n",
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:01Z\",\"message\":{\"id\":\"msg-a\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":20,\"cache_creation_input_tokens\":30,\"output_tokens\":2}}}\n",
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:02Z\",\"message\":{\"id\":\"msg-a\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":20,\"cache_creation_input_tokens\":30,\"output_tokens\":3}}}\n"
        );
        let mut context = ClaudeJsonlParseContext::new("fixture", Some("project-a"));
        let mut calls = Vec::new();
        let report = parse_claude_jsonl_stream(
            input.as_bytes(),
            1024 * 1024,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |call| {
                calls.push(call);
                Ok(())
            },
        )
        .expect("fixture parses");

        assert_eq!(report.calls_emitted, 1);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].usage.input_tokens, Some(60));
        assert_eq!(calls[0].usage.cached_input_tokens, Some(20));
        assert_eq!(calls[0].usage.cache_write_input_tokens, Some(30));
        assert_eq!(calls[0].usage.output_tokens, Some(3));
        assert_eq!(calls[0].usage.total_tokens, 63);
        assert_eq!(calls[0].usage.reasoning_output_tokens, None);
        assert_eq!(calls[0].reasoning_effort, None);
    }

    /// 验证数值回退的观察被拒绝合并，且原始消息正文从不被保留。
    #[test]
    fn rejects_regression_and_never_retains_message_content() {
        let input = concat!(
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:00Z\",\"message\":{\"id\":\"msg-a\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":0,\"cache_creation_input_tokens\":0,\"output_tokens\":8},\"content\":\"do-not-store-this\"}}\n",
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:01Z\",\"message\":{\"id\":\"msg-a\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":0,\"cache_creation_input_tokens\":0,\"output_tokens\":7}}}\n"
        );
        let mut context = ClaudeJsonlParseContext::new("fixture", None);
        let mut calls = Vec::new();
        let report = parse_claude_jsonl_stream(
            input.as_bytes(),
            1024,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |call| {
                calls.push(call);
                Ok(())
            },
        )
        .expect("fixture parses");

        assert_eq!(report.warnings.regressed_observations, 1);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].usage.output_tokens, Some(8));
        let debug = format!("{calls:?}");
        assert!(!debug.contains("do-not-store-this"));
    }

    /// 验证即使缓存增长掩盖了总量下降，原始输入 Token 的回退也会被识别。
    #[test]
    fn rejects_raw_input_regression_even_when_cache_growth_masks_the_total() {
        let input = concat!(
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:00Z\",\"message\":{\"id\":\"msg-a\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":20,\"cache_creation_input_tokens\":30,\"output_tokens\":1}}}\n",
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:01Z\",\"message\":{\"id\":\"msg-a\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":9,\"cache_read_input_tokens\":20,\"cache_creation_input_tokens\":31,\"output_tokens\":2}}}\n"
        );
        let mut context = ClaudeJsonlParseContext::new("fixture", None);
        let mut calls = Vec::new();
        let report = parse_claude_jsonl_stream(
            input.as_bytes(),
            1024,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |call| {
                calls.push(call);
                Ok(())
            },
        )
        .expect("fixture parses");

        assert_eq!(report.warnings.regressed_observations, 1);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].usage.output_tokens, Some(1));
        assert_eq!(calls[0].usage.cache_write_input_tokens, Some(30));
    }

    /// 验证畸形、超长、未完结与未知行都只计入告警，不产生任何调用。
    #[test]
    fn reports_malformed_oversized_partial_and_unknown_records_without_calls() {
        let input =
            b"{malformed}\n{\"type\":\"future-event\"}\n123456789\n{\"type\":\"assistant\"}";
        let mut context = ClaudeJsonlParseContext::new("fixture", None);
        let report = parse_claude_jsonl_stream(
            &input[..],
            8,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |_| Ok(()),
        )
        .expect("fixture is processed");

        assert_eq!(report.calls_emitted, 0);
        assert!(report.warnings.oversized_lines >= 2);
        assert!(report.checkpoint.trailing_bytes > 0);
    }

    /// 验证 Token 计数溢出被记为质量告警，而不是发生数值回绕。
    #[test]
    fn overflow_is_a_quality_warning_instead_of_wrapping() {
        let input = format!(
            "{{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:00Z\",\"message\":{{\"id\":\"msg-a\",\"model\":\"claude-test\",\"usage\":{{\"input_tokens\":{},\"cache_read_input_tokens\":1,\"cache_creation_input_tokens\":0,\"output_tokens\":1}}}}}}\n",
            u64::MAX
        );
        let mut context = ClaudeJsonlParseContext::new("fixture", None);
        let report = parse_claude_jsonl_stream(
            input.as_bytes(),
            1024,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |_| Ok(()),
        )
        .expect("overflow fixture is processed");
        assert_eq!(report.calls_emitted, 0);
        assert_eq!(report.warnings.invalid_usage, 1);
    }

    /// Claude 没有独立总量；缺少任一缓存输入组成部分时不得伪造零值，显式零则
    /// 仍是完整事实并可安全派生总量。
    #[test]
    fn distinguishes_missing_cache_components_from_explicit_zero() {
        let input = concat!(
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:00Z\",\"message\":{\"id\":\"missing-cache\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":0,\"output_tokens\":2}}}\n",
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:01Z\",\"message\":{\"id\":\"explicit-zero\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":0,\"cache_creation_input_tokens\":0,\"output_tokens\":2}}}\n"
        );
        let mut context = ClaudeJsonlParseContext::new("fixture", None);
        let mut calls = Vec::new();
        let report = parse_claude_jsonl_stream(
            input.as_bytes(),
            1024,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |call| {
                calls.push(call);
                Ok(())
            },
        )
        .expect("availability fixture parses");

        assert_eq!(report.warnings.missing_usage, 1);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].usage.input_tokens, Some(10));
        assert_eq!(calls[0].usage.cached_input_tokens, Some(0));
        assert_eq!(calls[0].usage.cache_write_input_tokens, Some(0));
        assert_eq!(calls[0].usage.output_tokens, Some(2));
        assert_eq!(calls[0].usage.total_tokens, 12);
        assert!(calls[0].usage.total_is_derived);
    }

    /// 验证只忽略已知的附件类事件，未来新事件类型会计入未知告警。
    #[test]
    fn ignores_only_known_attachment_events_and_warns_on_future_types() {
        let input = concat!(
            "{\"type\":\"attachment\",\"message\":{\"content\":\"private bait\"}}\n",
            "{\"type\":\"future-attachment-like\",\"message\":{\"content\":\"private bait\"}}\n"
        );
        let mut context = ClaudeJsonlParseContext::new("fixture", None);
        let report = parse_claude_jsonl_stream(
            input.as_bytes(),
            1024,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |_| Ok(()),
        )
        .expect("attachment fixture is processed");

        assert_eq!(report.calls_emitted, 0);
        assert_eq!(report.warnings.unknown_events, 1);
        assert_eq!(report.warnings.total(), 1);
    }

    /// 当前 Claude Code 写出的会话/权限/文件历史/累计成本元数据行都是已知非用量事件，
    /// 不得再计入未知告警把覆盖压成 Partial；`cost-state` 的累计快照也不得单独入库。
    #[test]
    fn ignores_current_claude_metadata_events_without_warnings_or_calls() {
        let input = concat!(
            "{\"type\":\"last-prompt\",\"sessionId\":\"s\",\"lastPrompt\":\"private bait\"}\n",
            "{\"type\":\"mode\",\"sessionId\":\"s\",\"mode\":\"default\"}\n",
            "{\"type\":\"permission-mode\",\"sessionId\":\"s\",\"permissionMode\":\"default\"}\n",
            "{\"type\":\"atis-latch\",\"sessionId\":\"s\",\"latched\":true}\n",
            "{\"type\":\"bridge-session\",\"sessionId\":\"s\",\"ownerAccountUuid\":\"private bait\"}\n",
            "{\"type\":\"file-history-delta\",\"sessionId\":\"s\",\"delta\":{}}\n",
            "{\"type\":\"cost-state\",\"sessionId\":\"s\",\"modelUsage\":{\"claude-test\":{\"inputTokens\":99,\"outputTokens\":99}}}\n"
        );
        let mut context = ClaudeJsonlParseContext::new("fixture", None);
        let report = parse_claude_jsonl_stream(
            input.as_bytes(),
            1024,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |_| Ok(()),
        )
        .expect("metadata fixture is processed");

        assert_eq!(report.calls_emitted, 0);
        assert_eq!(report.warnings.total(), 0);
    }

    /// 助手用量行上的 `effort` 必须进入 shipped `UsageCall.reasoning_effort`。
    #[test]
    fn persists_assistant_effort_label_on_the_call() {
        let input = concat!(
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:00Z\",",
            "\"effort\":\"high\",\"message\":{\"id\":\"msg-a\",\"model\":\"claude-sonnet-5\",",
            "\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":0,\"cache_creation_input_tokens\":0,\"output_tokens\":2}}}\n"
        );
        let mut context = ClaudeJsonlParseContext::new("fixture", None);
        let mut calls = Vec::new();
        parse_claude_jsonl_stream(
            input.as_bytes(),
            1024,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |call| {
                calls.push(call);
                Ok(())
            },
        )
        .expect("effort fixture parses");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].reasoning_effort.as_deref(), Some("high"));
        assert_eq!(calls[0].model.as_deref(), Some("claude-sonnet-5"));
    }

    /// 缺少 effort 的助手用量行必须保持推理强度为 none，不得发明值。
    #[test]
    fn omits_reasoning_effort_when_assistant_line_has_no_effort() {
        let input = concat!(
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:00Z\",",
            "\"message\":{\"id\":\"msg-a\",\"model\":\"claude-test\",",
            "\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":0,\"cache_creation_input_tokens\":0,\"output_tokens\":2}}}\n"
        );
        let mut context = ClaudeJsonlParseContext::new("fixture", None);
        let mut calls = Vec::new();
        parse_claude_jsonl_stream(
            input.as_bytes(),
            1024,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |call| {
                calls.push(call);
                Ok(())
            },
        )
        .expect("no-effort fixture parses");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].reasoning_effort, None);
    }

    /// 路径型 effort 与凭据对象里的邮箱都不得进入调用字段。
    #[test]
    fn rejects_path_like_effort_and_does_not_copy_credential_email() {
        let input = concat!(
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:00Z\",",
            "\"effort\":\"/Users/alice/secret\",\"auth\":{\"email\":\"alice@example.com\"},",
            "\"message\":{\"id\":\"msg-a\",\"model\":\"claude-test\",",
            "\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":0,\"cache_creation_input_tokens\":0,\"output_tokens\":2}}}\n"
        );
        let mut context = ClaudeJsonlParseContext::new("fixture", None);
        let mut calls = Vec::new();
        parse_claude_jsonl_stream(
            input.as_bytes(),
            1024,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |call| {
                calls.push(call);
                Ok(())
            },
        )
        .expect("unsafe effort still yields a usage call");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].reasoning_effort, None);
        assert_eq!(calls[0].model.as_deref(), Some("claude-test"));
        let debug = format!("{calls:?}");
        assert!(!debug.contains("alice@example.com"));
        assert!(!debug.contains("/Users/alice"));
    }

    /// 助手一次写入超大文件时用量与正文同行；超过行预算也必须计入，不能整行丢弃。
    #[test]
    fn counts_usage_on_lines_above_the_memory_budget() {
        let content = "x".repeat(2 * 1024 * 1024);
        let input = format!(
            "{{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-07-31T08:00:00Z\",\"message\":{{\"id\":\"msg-big\",\"model\":\"claude-test\",\"content\":[{{\"type\":\"tool_use\",\"input\":{{\"content\":\"{content}\"}}}}],\"usage\":{{\"input_tokens\":10,\"cache_read_input_tokens\":0,\"cache_creation_input_tokens\":0,\"output_tokens\":7}}}}}}\n"
        );
        let mut context = ClaudeJsonlParseContext::new("fixture", None);
        let mut calls = Vec::new();
        let report = parse_claude_jsonl_stream(
            input.as_bytes(),
            1024 * 1024,
            &mut context,
            &provenance(),
            &CancellationToken::new(),
            |call| {
                calls.push(call);
                Ok(())
            },
        )
        .expect("oversized fixture parses");

        assert_eq!(report.warnings.oversized_lines, 0);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].usage.output_tokens, Some(7));
    }
}
