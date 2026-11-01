//! WorkBuddy project JSONL 的有界流式用量投影。

use std::fs::{self, File};
use std::io::{Read, Take};
use std::path::Path;

use crate::{
    CoverageReport, CoverageState, WorkbuddyUsageEventRecord, safe_path_basename, stable_id,
};
use jiff::Timestamp;
use serde::Deserialize;

use crate::readers::jsonl_lines::{JsonlRecord, LineFlow, cancelled_io_error, read_jsonl_records};
use crate::readers::{
    DEFAULT_MAX_JSONL_LINE_BYTES, metadata_is_link_like, same_opened_file,
    walk_ancestors_for_link_component,
};

use super::projects::{WorkbuddyProjectFile, discover_project_files};
use super::{WorkbuddyCancellation, WorkbuddyReadError};

/// 单次读取允许消耗的 project JSONL 总字节数。
const MAX_PROJECT_JSONL_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// 单次读取允许保留的 usage 事件数。
const MAX_USAGE_RECORDS: usize = 500_000;

/// 一次 project JSONL 读取的安全投影与覆盖结论。
pub(super) struct WorkbuddyProjectUsageRead {
    /// 已通过白名单投影的用量事件记录。
    pub(super) records: Vec<WorkbuddyUsageEventRecord>,
    /// 本次读取的覆盖结论；预算耗尽、权限拒绝或警告都会使其为部分覆盖。
    pub(super) coverage: CoverageReport,
}

/// 只反序列化顶层事件中统计必需的白名单字段。
#[derive(Deserialize)]
struct ProjectEvent {
    /// 事件时间戳（毫秒）；缺失或非法按警告计数。
    timestamp: Option<i64>,
    /// 会话 ID，用于归并同一会话的调用。
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
    /// 工作目录；仅用于派生项目标签，原值不进入统计输出。
    cwd: Option<String>,
    /// provider 层数据，含模型、调用 ID 与用量。
    #[serde(rename = "providerData")]
    provider_data: Option<ProviderData>,
}

/// WorkBuddy provider 层的实际模型、调用 ID 与直接 usage。
#[derive(Deserialize)]
struct ProviderData {
    /// provider 消息 ID，作为调用身份的一部分。
    #[serde(rename = "messageId")]
    message_id: Option<String>,
    /// 实际使用的模型名。
    model: Option<String>,
    /// 直接 provider usage，是 token 与请求数的唯一来源。
    usage: Option<ProviderUsage>,
    /// 原始 usage，仅用于读取逐请求积分。
    #[serde(rename = "rawUsage")]
    raw_usage: Option<RawUsage>,
}

/// 直接 provider usage 是 Token 与请求数的唯一来源。
#[derive(Deserialize)]
struct ProviderUsage {
    /// 输入 token 数。
    #[serde(rename = "inputTokens")]
    input_tokens: Option<i64>,
    /// 输入 token 明细，仅从中读取缓存命中数。
    #[serde(rename = "inputTokensDetails")]
    input_token_details: Option<Vec<InputTokenDetail>>,
    /// 输出 token 数。
    #[serde(rename = "outputTokens")]
    output_tokens: Option<i64>,
    /// 总 token 数。
    #[serde(rename = "totalTokens")]
    total_tokens: Option<i64>,
    /// 请求数。
    requests: Option<i64>,
}

/// 缓存输入只读取 `cached_tokens`，它是 input 的子集。
#[derive(Deserialize)]
struct InputTokenDetail {
    /// 缓存命中的输入 token 数，是输入 token 的子集。
    cached_tokens: Option<i64>,
}

/// raw usage 仅用于逐请求积分；其它 provider 私有字段全部忽略。
#[derive(Deserialize)]
struct RawUsage {
    /// 单次请求消耗的积分；非有限或为负时为 `None`。
    #[serde(default, deserialize_with = "deserialize_optional_f64")]
    credit: Option<f64>,
}

/// 把任意 JSON credit 值宽容投影为有限非负数；不可用时保持 `None`。
fn deserialize_optional_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(value
        .as_f64()
        .filter(|credit| credit.is_finite() && *credit >= 0.0))
}

/// 完整行投影后的稳定分类；普通消息与工具事件属于 `Ignored`。
enum LineOutcome {
    /// 解析出的一条用量事件；装箱以避免枚举体积过大。
    Usage(Box<WorkbuddyUsageEventRecord>),
    /// 普通消息、工具事件等与用量无关的行，静默忽略。
    Ignored,
    /// 格式、时间、身份或用量字段不合法；只计数，不携带文件名或记录正文。
    Warning,
}

/// 枚举并读取全部获批 project JSONL 布局；单文件失败时保留其它可用事实并标为部分覆盖。
pub(super) fn read_project_usage<C: WorkbuddyCancellation>(
    workbuddy_home: &Path,
    root_id: &str,
    cancellation: &C,
) -> Result<WorkbuddyProjectUsageRead, WorkbuddyReadError> {
    let files = discover_project_files(workbuddy_home, cancellation)?;
    let mut records = Vec::new();
    let mut warning_count = 0_u64;
    let mut io_skipped_count = 0_u64;
    let mut remaining_bytes = MAX_PROJECT_JSONL_BYTES;
    let mut budget_exhausted = files.budget_exhausted;

    for source in &files.files {
        if cancellation.is_cancelled() {
            return Err(WorkbuddyReadError::Read);
        }
        if remaining_bytes == 0 || records.len() >= MAX_USAGE_RECORDS {
            budget_exhausted = true;
            break;
        }
        let (reader, observed_len) = match open_fixed_prefix(&source.path) {
            Ok(value) => value,
            Err(_) => {
                io_skipped_count = io_skipped_count.saturating_add(1);
                continue;
            }
        };
        let allowed_len = observed_len.min(remaining_bytes);
        if allowed_len < observed_len {
            budget_exhausted = true;
        }
        remaining_bytes = remaining_bytes.saturating_sub(allowed_len);
        match parse_project_stream(
            reader.take(allowed_len),
            source,
            root_id,
            &mut records,
            cancellation,
        ) {
            Ok(report) => {
                warning_count = warning_count.saturating_add(report.warning_count);
                budget_exhausted |= report.record_budget_exhausted;
            }
            Err(_) => {
                if cancellation.is_cancelled() {
                    return Err(WorkbuddyReadError::Read);
                }
                io_skipped_count = io_skipped_count.saturating_add(1);
            }
        }
        if budget_exhausted && (remaining_bytes == 0 || records.len() >= MAX_USAGE_RECORDS) {
            break;
        }
    }

    let skipped_count = files
        .skipped_count
        .saturating_add(io_skipped_count)
        .saturating_add(u64::from(budget_exhausted));
    let state = if files.permission_denied_count > 0 || skipped_count > 0 || warning_count > 0 {
        CoverageState::Partial
    } else {
        CoverageState::Complete
    };
    Ok(WorkbuddyProjectUsageRead {
        records,
        coverage: CoverageReport {
            state,
            roots_scanned: 1,
            roots_discovered: 1,
            permission_denied_count: files.permission_denied_count,
            skipped_count,
            warning_count,
        },
    })
}

/// 打开普通文件、复核链接与句柄身份，并冻结本轮只读长度。
pub(super) fn open_fixed_prefix(path: &Path) -> std::io::Result<(File, u64)> {
    let file = File::open(path)?;
    let opened_metadata = file.metadata()?;
    if !opened_metadata.is_file() || walk_ancestors_for_link_component(path)? {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unsupported WorkBuddy source",
        ));
    }
    let current_metadata = fs::symlink_metadata(path)?;
    if metadata_is_link_like(&current_metadata)
        || !current_metadata.is_file()
        || !same_opened_file(&file, path, &opened_metadata, &current_metadata)?
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WorkBuddy source changed during inspection",
        ));
    }
    Ok((file, opened_metadata.len()))
}

/// 单文件解析报告。
struct ProjectStreamReport {
    /// 本文件解析中遇到的警告行数。
    warning_count: u64,
    /// 记录数是否触及上限而提前停止。
    record_budget_exhausted: bool,
}

/// 用共享有界切行器逐行反序列化白名单字段；超长行流式解析，末尾半行不提交。
fn parse_project_stream<C: WorkbuddyCancellation>(
    reader: Take<File>,
    source: &WorkbuddyProjectFile,
    root_id: &str,
    records: &mut Vec<WorkbuddyUsageEventRecord>,
    cancellation: &C,
) -> std::io::Result<ProjectStreamReport> {
    let mut warning_count = 0_u64;
    let mut record_budget_exhausted = false;
    let progress = read_jsonl_records(
        reader,
        DEFAULT_MAX_JSONL_LINE_BYTES,
        || cancellation.is_cancelled(),
        |error| error,
        |record| {
            let outcome = match record {
                JsonlRecord::Parsed(event) => classify_event(event, source, root_id),
                JsonlRecord::Empty => LineOutcome::Ignored,
                JsonlRecord::Malformed | JsonlRecord::OversizedMalformed => LineOutcome::Warning,
            };
            match outcome {
                LineOutcome::Usage(record) => records.push(*record),
                LineOutcome::Ignored => {}
                LineOutcome::Warning => warning_count = warning_count.saturating_add(1),
            }
            if records.len() >= MAX_USAGE_RECORDS {
                record_budget_exhausted = true;
                return Ok(LineFlow::Stop);
            }
            Ok(LineFlow::Continue)
        },
    )?;
    if progress.cancelled {
        return Err(cancelled_io_error());
    }
    Ok(ProjectStreamReport {
        warning_count,
        record_budget_exhausted,
    })
}

/// 从白名单字段提取直接 provider usage；嵌套 message usage 从未声明，因而不会计数。
fn classify_event(
    event: ProjectEvent,
    source: &WorkbuddyProjectFile,
    root_id: &str,
) -> LineOutcome {
    let Some(provider) = event.provider_data else {
        return LineOutcome::Ignored;
    };
    let Some(usage) = provider.usage else {
        return LineOutcome::Ignored;
    };
    let Some(timestamp) = event.timestamp else {
        return LineOutcome::Warning;
    };
    if Timestamp::from_millisecond(timestamp).is_err() {
        return LineOutcome::Warning;
    }
    let Some(session_id) = nonempty(event.session_id.as_deref()) else {
        return LineOutcome::Warning;
    };
    let Some(message_id) = nonempty(provider.message_id.as_deref()) else {
        return LineOutcome::Warning;
    };
    let Some((input_tokens, cached_input_tokens, output_tokens, total_tokens, requests)) =
        validated_usage(&usage)
    else {
        return LineOutcome::Warning;
    };

    let project_seed = nonempty(event.cwd.as_deref()).unwrap_or(&source.project_key_material);
    let project_label = event.cwd.as_deref().and_then(safe_path_basename);
    let session_key = stable_id("workbuddy-session", &format!("{root_id}\0{session_id}"));
    let logical_call_id = stable_id(
        "workbuddy-call",
        &format!("{root_id}\0{session_id}\0{message_id}\0{timestamp}"),
    );
    let source_id = stable_id(
        "workbuddy-source",
        &format!("{root_id}\0{}", source.relative_key),
    );
    LineOutcome::Usage(Box::new(WorkbuddyUsageEventRecord {
        logical_call_id,
        root_id: root_id.to_owned(),
        session_key,
        source_id,
        occurred_at_epoch_ms: timestamp,
        model: provider.model,
        project_key: Some(stable_id("workbuddy-project", project_seed)),
        project_label,
        request_count: requests,
        input_tokens,
        cached_input_tokens,
        output_tokens,
        total_tokens,
        credit: provider.raw_usage.and_then(|raw| raw.credit),
        origin: source.origin,
    }))
}

/// 校验 usage 的精确整数、不变量与缓存子集关系。
fn validated_usage(usage: &ProviderUsage) -> Option<(i64, i64, i64, i64, i64)> {
    let input_tokens = usage.input_tokens?;
    let output_tokens = usage.output_tokens?;
    let total_tokens = usage.total_tokens?;
    let requests = usage.requests?;
    let cached_input_tokens = usage
        .input_token_details
        .as_ref()?
        .iter()
        .try_fold(0_i64, |sum, detail| sum.checked_add(detail.cached_tokens?))?;
    if input_tokens < 0
        || output_tokens < 0
        || total_tokens < 0
        || cached_input_tokens < 0
        || cached_input_tokens > input_tokens
        || !(1..=10_000).contains(&requests)
        || input_tokens.checked_add(output_tokens)? != total_tokens
    {
        return None;
    }
    Some((
        input_tokens,
        cached_input_tokens,
        output_tokens,
        total_tokens,
        requests,
    ))
}

/// 只接受去除首尾空白后的非空身份字段。
fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}
