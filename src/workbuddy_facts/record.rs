//! WorkBuddy project JSONL 用量事件的校验、去重与安全归一化。

use std::collections::BTreeMap;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

use crate::{TokenUsage, safe_path_basename, safe_technical_label, safe_thread_title};

/// WorkBuddy 当前每条 usage 事件只描述一个上游请求；聚合计数必须失败关闭。
const REQUESTS_PER_EVENT: i64 = 1;

/// 区分 WorkBuddy 顶层会话与受限 `subagents` 布局中的请求。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkbuddyUsageOrigin {
    /// `projects/<project>/<session>.jsonl` 中的请求。
    TopLevel,
    /// `projects/<project>/<session>/subagents/<agent>.jsonl` 中的请求。
    Subagent,
}

/// adapter 从单条 JSONL 记录投影出的最小用量事实。
///
/// 读取器输出的身份字段已经过稳定哈希；项目标签只允许安全末段。该结构
/// 不实现 `Debug` 或序列化，避免未来日志误带尚未校验的模型文本。
#[derive(Clone, PartialEq)]
pub struct WorkbuddyUsageEventRecord {
    /// `(root, sessionId, providerData.messageId, timestamp)` 生成的内容无关稳定调用 ID。
    pub logical_call_id: String,
    /// 当前国内版或国际版固定根的稳定 ID，不含路径。
    pub root_id: String,
    /// 上游 session ID 生成的内容无关稳定线程 ID。
    pub session_key: String,
    /// 当前来源文件生成的稳定 ID，不含路径。
    pub source_id: String,
    /// 请求发生时刻的 Unix 毫秒时间戳。
    pub occurred_at_epoch_ms: i64,
    /// 当前事件实际执行的模型；规范化时执行安全 slug 校验。
    pub model: Option<String>,
    /// 内容无关的项目稳定 ID。
    pub project_key: Option<String>,
    /// 已截取的安全项目末段；规范化时再次校验。
    pub project_label: Option<String>,
    /// 上游 `providerData.usage.requests`；当前合法值固定为 1。
    pub request_count: i64,
    /// 全部输入 Token，包含缓存输入。
    pub input_tokens: i64,
    /// 输入 Token 中命中缓存的子集。
    pub cached_input_tokens: i64,
    /// 输出 Token。
    pub output_tokens: i64,
    /// 上游明确总量，必须严格等于输入加输出。
    pub total_tokens: i64,
    /// 上游逐请求积分；缺失时 Token 仍可用，但积分覆盖不完整。
    pub credit: Option<f64>,
    /// 顶层或 subagent 来源。
    pub origin: WorkbuddyUsageOrigin,
}

/// WorkBuddy usage 记录校验与去重后的质量计数。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkbuddyUsageQuality {
    /// 字段或 Token 不变量不合法、因而被排除的记录数。
    pub invalid_record_count: u64,
    /// 相同稳定调用 ID 携带冲突事实、因而整组被排除的记录数。
    pub conflicting_duplicate_record_count: u64,
    /// 完全一致的重复观察数量；只计一次用量。
    pub duplicate_record_count: u64,
}

/// 已验证并去重的逐请求事实，可由消费方聚合或投影到 wire 数据。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ValidatedWorkbuddyUsageRecord {
    /// 稳定逻辑调用 ID，去重与冲突判定的键。
    pub logical_call_id: String,
    /// 所属数据根 ID。
    pub root_id: String,
    /// 会话键，用于归并同一会话。
    pub session_key: String,
    /// 贡献该调用的来源文件 ID；重复观察会合并到同一调用。
    pub source_ids: Vec<String>,
    /// 调用发生时间（毫秒 Unix 时间戳）。
    pub occurred_at_epoch_ms: i64,
    /// 模型名；来源未提供为 `None`。
    pub model: Option<String>,
    /// 项目键，用于按项目归并；缺失为 `None`。
    pub project_key: Option<String>,
    /// 项目展示标签，已脱敏；缺失为 `None`。
    pub project_label: Option<String>,
    /// 该调用包含的请求数。
    pub request_count: u32,
    /// Token 用量，已通过不变量校验。
    pub usage: TokenUsage,
    /// 积分消耗；来源未提供或非法为 `None`。
    pub credit: Option<f64>,
    /// 调用的来源类别（如 project 日志）。
    pub origin: WorkbuddyUsageOrigin,
}

/// 校验、稳定归一化并按逻辑调用 ID 去重；冲突组整组失败关闭。
pub fn prepare_workbuddy_usage_records(
    records: &[WorkbuddyUsageEventRecord],
) -> (Vec<ValidatedWorkbuddyUsageRecord>, WorkbuddyUsageQuality) {
    let mut quality = WorkbuddyUsageQuality::default();
    let mut groups = BTreeMap::<&str, Vec<&WorkbuddyUsageEventRecord>>::new();
    for record in records {
        if record.logical_call_id.trim().is_empty() {
            quality.invalid_record_count = quality.invalid_record_count.saturating_add(1);
            continue;
        }
        groups
            .entry(record.logical_call_id.as_str())
            .or_default()
            .push(record);
    }

    let mut prepared = Vec::with_capacity(groups.len());
    for group in groups.into_values() {
        let mut validated = Vec::with_capacity(group.len());
        let mut group_invalid = false;
        for record in group {
            match validate_record(record) {
                Some(record) => {
                    validated.push(record);
                }
                None => {
                    quality.invalid_record_count = quality.invalid_record_count.saturating_add(1);
                    group_invalid = true;
                }
            }
        }
        if group_invalid || validated.is_empty() {
            continue;
        }

        let first = &validated[0];
        if validated
            .iter()
            .skip(1)
            .any(|candidate| !same_fact(first, candidate))
        {
            quality.conflicting_duplicate_record_count = quality
                .conflicting_duplicate_record_count
                .saturating_add(u64::try_from(validated.len()).unwrap_or(u64::MAX));
            continue;
        }

        quality.duplicate_record_count = quality
            .duplicate_record_count
            .saturating_add(u64::try_from(validated.len().saturating_sub(1)).unwrap_or(u64::MAX));
        let mut selected = validated.remove(0);
        for duplicate in validated {
            selected.source_ids.extend(duplicate.source_ids);
        }
        selected.source_ids.sort();
        selected.source_ids.dedup();
        prepared.push(selected);
    }
    prepared.sort_by(|left, right| {
        left.occurred_at_epoch_ms
            .cmp(&right.occurred_at_epoch_ms)
            .then_with(|| left.logical_call_id.cmp(&right.logical_call_id))
    });
    (prepared, quality)
}

/// 把一条 adapter 投影转换为强类型 TokenUsage；失败时不保留局部事实。
fn validate_record(record: &WorkbuddyUsageEventRecord) -> Option<ValidatedWorkbuddyUsageRecord> {
    if record.session_key.trim().is_empty()
        || record.source_id.trim().is_empty()
        || Timestamp::from_millisecond(record.occurred_at_epoch_ms).is_err()
        || record.request_count != REQUESTS_PER_EVENT
    {
        return None;
    }
    let input_tokens = u64::try_from(record.input_tokens).ok()?;
    let cached_input_tokens = u64::try_from(record.cached_input_tokens).ok()?;
    let output_tokens = u64::try_from(record.output_tokens).ok()?;
    let total_tokens = u64::try_from(record.total_tokens).ok()?;
    if input_tokens.checked_add(output_tokens)? != total_tokens {
        return None;
    }
    let usage = TokenUsage::new_with_availability(
        input_tokens,
        Some(cached_input_tokens),
        None,
        output_tokens,
        None,
        Some(total_tokens),
    )
    .ok()?;
    let credit = record
        .credit
        .filter(|value| value.is_finite() && *value >= 0.0);
    Some(ValidatedWorkbuddyUsageRecord {
        logical_call_id: record.logical_call_id.clone(),
        root_id: record.root_id.clone(),
        session_key: record.session_key.clone(),
        source_ids: vec![record.source_id.clone()],
        occurred_at_epoch_ms: record.occurred_at_epoch_ms,
        model: record.model.as_deref().and_then(safe_thread_title),
        project_key: record.project_key.as_deref().and_then(safe_technical_label),
        project_label: record.project_label.as_deref().and_then(safe_path_basename),
        request_count: u32::try_from(record.request_count).ok()?,
        usage,
        credit,
        origin: record.origin,
    })
}

/// 判断两个同 ID 观察是否描述同一事实；来源 ID 不参与比较，只用于合并追溯。
fn same_fact(left: &ValidatedWorkbuddyUsageRecord, right: &ValidatedWorkbuddyUsageRecord) -> bool {
    left.logical_call_id == right.logical_call_id
        && left.root_id == right.root_id
        && left.session_key == right.session_key
        && left.occurred_at_epoch_ms == right.occurred_at_epoch_ms
        && left.model == right.model
        && left.project_key == right.project_key
        && left.project_label == right.project_label
        && left.request_count == right.request_count
        && left.usage == right.usage
        && optional_float_eq(left.credit, right.credit)
        && left.origin == right.origin
}

/// 已经过 finite 校验的积分使用位级比较，避免近似比较掩盖重复冲突。
fn optional_float_eq(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.to_bits() == right.to_bits(),
        (None, None) => true,
        _ => false,
    }
}
