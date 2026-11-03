//! GLM（Z Code CLI）`model-io-sess_*.jsonl` 的有界流式解析器。
//!
//! 只反序列化用量、模型与时间元数据；`request`/`response` 中的消息正文、
//! 工具调用与 headers 未在本类型声明，由 serde 跳过而不缓冲。

use std::collections::BTreeMap;
use std::io::Read;

use crate::{Confidence, SourceClientKind, SourceProvenance, TokenUsage, UsageCall};
use serde::{Deserialize, Serialize};

use super::super::LocalError;
use super::super::discovery::stable_id;
use super::super::file_source::map_source_io_error;
use super::super::jsonl_lines::{JsonlRecord, LineFlow, read_jsonl_records};
use jiff::Timestamp;

/// GLM 模型 I/O 用量独立解析语义版本；与共享来源模型声明的解析代次一致。
pub const GLM_PARSER_VERSION: u32 = SourceClientKind::Glm.parser_version();

/// 保存增量扫描可安全提交的完整行偏移。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GlmJsonlCheckpoint {
    /// 已确认可提交的完整行字节数。
    pub committed_bytes: u64,
    /// 当前缓冲中尚未凑成完整行的尾部字节数。
    pub trailing_bytes: u64,
    /// 标识尾部正在丢弃一条超长行。
    pub discarding_oversized_line: bool,
}

/// 仅包含格式与数据质量计数。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GlmJsonlWarningCounts {
    /// 无法解析为合法 JSON 的行数。
    pub malformed_lines: u64,
    /// 超出单行字节上限且无法解析的行数。
    pub oversized_lines: u64,
    /// 缺少事件类型标记的行数。
    pub unknown_events: u64,
    /// 声明了用量但缺少判定所需字段的行数。
    pub missing_usage: u64,
    /// 用量字段存在但数值不合法的行数。
    pub invalid_usage: u64,
    /// 开始与完成时间均缺失或无法解析的行数。
    pub invalid_timestamps: u64,
    /// 缺少可信会话、请求或重试身份的用量行数。
    pub invalid_identities: u64,
}

impl GlmJsonlWarningCounts {
    /// 汇总全部质量告警计数。
    pub fn total(self) -> u64 {
        self.malformed_lines
            .saturating_add(self.oversized_lines)
            .saturating_add(self.unknown_events)
            .saturating_add(self.missing_usage)
            .saturating_add(self.invalid_usage)
            .saturating_add(self.invalid_timestamps)
            .saturating_add(self.invalid_identities)
    }

    /// 合并同一来源追加片段与历史 generation 的质量计数。
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
            invalid_identities: self
                .invalid_identities
                .saturating_add(appended.invalid_identities),
        }
    }
}

/// 跨增量片段持久化的安全上下文。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlmJsonlParseContext {
    /// 由文件会话标识派生的稳定线程键。
    pub thread_key: String,
    /// 已发出事件数，供增量 checkpoint 保存进度。
    pub call_sequence: u64,
    /// 当前来源已观察到的安全事实，用于跨追加检测重复身份和冲突。
    #[serde(default)]
    observed_calls: BTreeMap<String, String>,
}

impl GlmJsonlParseContext {
    /// 从会话种子派生散列键。
    pub fn new(thread_seed: &str) -> Self {
        Self {
            thread_key: stable_id("glm-thread", thread_seed),
            call_sequence: 0,
            observed_calls: BTreeMap::new(),
        }
    }
}

/// 一次流式解析的安全结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlmJsonlParseReport {
    /// 本次实际发出的调用条数。
    pub calls_emitted: u64,
    /// 本次读取的原始字节总数。
    pub bytes_consumed: u64,
    /// 可安全用于下次增量续读的检查点。
    pub checkpoint: GlmJsonlCheckpoint,
    /// 本次解析累计的格式与数据质量告警。
    pub warnings: GlmJsonlWarningCounts,
    /// 标识解析是否因取消信号提前终止。
    pub cancelled: bool,
}

/// 模型引用层；只声明 `modelId`，其余键由 serde 跳过。
#[derive(Deserialize)]
struct ModelRef {
    /// 请求使用的模型标识。
    #[serde(rename = "modelId", default)]
    model_id: Option<String>,
}

/// 响应层；只声明用量与模型标识，正文、工具调用等不缓冲。
#[derive(Deserialize)]
struct ResponseLayer {
    /// 本层直接携带的用量计数。
    #[serde(default)]
    usage: Option<RawUsage>,
}

/// 只承载 GLM 单行事件解析所需的白名单字段。
#[derive(Deserialize)]
struct Envelope {
    /// 事件类型标记；只有 `model_io` 才会产生用量调用。
    #[serde(rename = "type", default)]
    kind: Option<String>,
    /// 顶层模型引用。
    #[serde(default)]
    model: Option<ModelRef>,
    /// 响应层，用量位于 `response.usage`。
    #[serde(default)]
    response: Option<ResponseLayer>,
    /// 请求开始时间，RFC 3339 文本。
    #[serde(rename = "startedAt", default)]
    started_at: Option<String>,
    /// 请求完成时间；开始时间未提供时作为可观察时间的回退。
    #[serde(rename = "completedAt", default)]
    completed_at: Option<String>,
    /// 上游会话身份，作为线程维度唯一来源。
    #[serde(rename = "sessionId", default)]
    session_id: Option<String>,
    /// 真实请求来源元数据；各来源均计入，不据此过滤。
    #[serde(rename = "querySource", default)]
    query_source: Option<String>,
    /// 请求 ID，用于生成稳定的调用身份。
    #[serde(rename = "requestId", default)]
    request_id: Option<String>,
    /// 轮次元数据；不替代缺失的请求 ID。
    #[serde(rename = "turnId", default)]
    turn_id: Option<String>,
    /// 同一请求的重试序号，参与身份防碰撞。
    #[serde(default)]
    attempt: Option<u64>,
}

/// 保存 GLM 用量对象中未经规范化的 Token 计数。
#[derive(Deserialize, Clone)]
struct RawUsage {
    /// 全部输入 token 数，已包含缓存读取与缓存写入。
    #[serde(rename = "inputTokens", default)]
    input_tokens: Option<u64>,
    /// 输出 token 数。
    #[serde(rename = "outputTokens", default)]
    output_tokens: Option<u64>,
    /// 总 token 数。
    #[serde(rename = "totalTokens", default)]
    total_tokens: Option<u64>,
    /// 缓存读取 token 数，是全部输入的子集。
    #[serde(rename = "cacheReadTokens", default)]
    cache_read_tokens: Option<u64>,
    /// 缓存写入 token 数，已位于全部输入内。
    #[serde(rename = "cacheWriteTokens", default)]
    cache_write_tokens: Option<u64>,
}

/// 区分一行被忽略、形成调用或计入告警的结果。
enum LineOutcome {
    /// 解析出一次调用。
    Calls(Vec<UsageCall>),
    /// 空行、非 `model_io` 事件或未声明用量的行，静默忽略。
    Ignored,
    /// 缺少事件类型标记，计入未知警告。
    Unknown,
    /// 不是合法 JSON，计入畸形警告。
    Malformed,
    /// 超过单行字节上限且无法解析的行。
    Oversized,
    /// 声明了用量但缺少判定所需字段。
    MissingUsage,
    /// 用量字段类型或取值非法。
    InvalidUsage,
    /// 开始时间缺失或无法解析。
    InvalidTimestamp,
    /// 缺少稳定请求或会话身份，不能生成 canonical 调用。
    InvalidIdentity,
}

/// 流式读取 GLM rollout 会话文件，只发出带可信用量的 `model_io` 事实。
///
/// `querySource` 为 main_turn、subagent、session_title 或 web_search_tool 的事件
/// 都是真实上游用量，本函数不做区分，全部计入。
/// # Errors
/// 来源 I/O、回调失败或同身份事实冲突时返回固定安全错误；调用方必须回滚未发布文件事务。
pub fn parse_glm_jsonl_stream<R, F>(
    reader: R,
    max_line_bytes: usize,
    context: &mut GlmJsonlParseContext,
    provenance: &SourceProvenance,
    cancellation: &dyn crate::CancellationSignal,
    mut on_call: F,
) -> Result<GlmJsonlParseReport, LocalError>
where
    R: Read,
    F: FnMut(UsageCall) -> Result<(), LocalError>,
{
    let mut warnings = GlmJsonlWarningCounts::default();
    let mut calls_emitted = 0_u64;

    let progress = read_jsonl_records(
        reader,
        max_line_bytes,
        || cancellation.is_cancelled(),
        map_source_io_error,
        |record| {
            match parse_line(record, context, provenance) {
                LineOutcome::Calls(calls) => {
                    for call in calls {
                        let observed = observation_fingerprint(&call)?;
                        if let Some(existing) = context.observed_calls.get(&call.logical_call_id) {
                            if existing != &observed {
                                return Err(LocalError::new(
                                    crate::LocalErrorKind::InvalidUsage,
                                    "GLM request identity has conflicting observations",
                                ));
                            }
                            continue;
                        }
                        context
                            .observed_calls
                            .insert(call.logical_call_id.clone(), observed);
                        context.call_sequence = context.call_sequence.saturating_add(1);
                        on_call(call)?;
                        calls_emitted = calls_emitted.saturating_add(1);
                    }
                }
                LineOutcome::Ignored => {}
                LineOutcome::Unknown => {
                    warnings.unknown_events = warnings.unknown_events.saturating_add(1);
                }
                LineOutcome::Malformed => {
                    warnings.malformed_lines = warnings.malformed_lines.saturating_add(1);
                }
                LineOutcome::Oversized => {
                    warnings.oversized_lines = warnings.oversized_lines.saturating_add(1);
                }
                LineOutcome::MissingUsage => {
                    warnings.missing_usage = warnings.missing_usage.saturating_add(1);
                }
                LineOutcome::InvalidUsage => {
                    warnings.invalid_usage = warnings.invalid_usage.saturating_add(1);
                }
                LineOutcome::InvalidTimestamp => {
                    warnings.invalid_timestamps = warnings.invalid_timestamps.saturating_add(1);
                }
                LineOutcome::InvalidIdentity => {
                    warnings.invalid_identities = warnings.invalid_identities.saturating_add(1);
                }
            }
            Ok(LineFlow::Continue)
        },
    )?;

    Ok(GlmJsonlParseReport {
        calls_emitted,
        bytes_consumed: progress.bytes_consumed,
        checkpoint: GlmJsonlCheckpoint {
            committed_bytes: progress.committed_bytes,
            trailing_bytes: progress.trailing_bytes,
            discarding_oversized_line: progress.discarding_oversized_line,
        },
        warnings,
        cancelled: progress.cancelled,
    })
}

/// 解析单行 GLM 事件并只接受具有可信用量的 `model_io` 轮次。
fn parse_line(
    record: JsonlRecord<Envelope>,
    context: &mut GlmJsonlParseContext,
    provenance: &SourceProvenance,
) -> LineOutcome {
    let record = match record {
        JsonlRecord::Parsed(record) => record,
        JsonlRecord::Empty => return LineOutcome::Ignored,
        JsonlRecord::Malformed => return LineOutcome::Malformed,
        JsonlRecord::OversizedMalformed => return LineOutcome::Oversized,
    };
    let kind = record.kind.as_deref().unwrap_or_default();
    if kind.is_empty() {
        return LineOutcome::Unknown;
    }
    if kind != "model_io" {
        return LineOutcome::Ignored;
    }
    // 无 response.usage 的合法 model_io 行（如未完成的请求）按非用量行忽略。
    let usage = match record
        .response
        .as_ref()
        .and_then(|response| response.usage.clone())
    {
        Some(usage) => usage,
        None => return LineOutcome::Ignored,
    };
    // GLM 只接受上游直接给出的总量，不能使用共享构造器的输入/输出推算回退。
    if usage.total_tokens.is_none() {
        return LineOutcome::MissingUsage;
    }
    let usage = match TokenUsage::new_with_component_availability(
        usage.input_tokens,
        usage.cache_read_tokens,
        usage.cache_write_tokens,
        usage.output_tokens,
        None,
        usage.total_tokens,
    ) {
        Ok(usage) => usage,
        Err(crate::TokenUsageError::MissingTotalTokens) => return LineOutcome::MissingUsage,
        Err(_) => return LineOutcome::InvalidUsage,
    };
    let occurred_at_epoch_ms = match parse_started_at(record.started_at.as_deref())
        .or_else(|| parse_started_at(record.completed_at.as_deref()))
    {
        Some(value) => value,
        None => return LineOutcome::InvalidTimestamp,
    };
    let (Some(session_id), Some(request_id), Some(attempt)) = (
        record.session_id.as_deref().and_then(safe_identity),
        record.request_id.as_deref().and_then(safe_identity),
        record.attempt,
    ) else {
        return LineOutcome::InvalidIdentity;
    };
    let model = record
        .model
        .as_ref()
        .and_then(|model| model.model_id.as_deref())
        .and_then(safe_model_label);
    // 这两个白名单元数据不改变请求计数，也不替代稳定身份。
    let _query_source = record.query_source.as_deref().and_then(safe_identity);
    let _turn_id = record.turn_id.as_deref().and_then(safe_identity);
    let thread_key = stable_id("glm-thread", session_id);
    context.thread_key.clone_from(&thread_key);
    let identity = format!(
        "{}\u{0}{}\u{0}{request_id}\u{0}{attempt}",
        provenance.root_id, provenance.source_id,
    );
    LineOutcome::Calls(vec![UsageCall {
        logical_call_id: stable_id("glm-call", &identity),
        occurred_at_epoch_ms,
        model: model.map(str::to_owned),
        reasoning_effort: None,
        project_key: None,
        project_label: None,
        thread_key,
        thread_label: None,
        usage,
        adapter_consistency_key: None,
        confidence: Confidence::Exact,
        provenance: vec![provenance.clone()],
    }])
}

/// 将 RFC 3339 开始时间转换为毫秒时间戳；缺失或非法返回 `None`。
fn parse_started_at(value: Option<&str>) -> Option<i64> {
    value
        .and_then(|text| text.parse::<Timestamp>().ok())
        .map(|timestamp| timestamp.as_millisecond())
}

/// 只接受有界且不含控制字符的请求或会话身份；原文只用于派生散列键。
fn safe_identity(value: &str) -> Option<&str> {
    (!value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control))
        .then_some(value)
}

/// 清理 GLM 模型标签并拒绝路径或控制字符。
fn safe_model_label(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()
        && trimmed.len() <= 128
        && !trimmed.contains(['/', '\\'])
        && !trimmed.chars().any(char::is_control))
    .then_some(trimmed)
}

/// 对安全计量字段生成稳定摘要，避免检查点重复保存整条调用或任何正文。
fn observation_fingerprint(call: &UsageCall) -> Result<String, LocalError> {
    let safe_fields = serde_json::to_string(&(
        call.occurred_at_epoch_ms,
        &call.model,
        &call.thread_key,
        &call.usage,
    ))
    .map_err(|_| {
        LocalError::new(
            crate::LocalErrorKind::InvalidUsage,
            "GLM request observation cannot be encoded",
        )
    })?;
    Ok(stable_id("glm-observation", &safe_fields))
}
