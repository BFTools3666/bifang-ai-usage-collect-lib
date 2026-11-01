//! 汇总 rollout JSONL 的脱敏解析警告与追加合并语义。

use serde::{Deserialize, Serialize};

/// 汇总格式漂移与受控回退计数，不包含任何原始记录内容。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlWarningCounts {
    /// 不能解析为 JSON 对象的完整行数量。
    pub malformed_lines: u64,
    /// 超过单行上限并被流式丢弃的完整行数量。
    pub oversized_lines: u64,
    /// 结构有效但不是当前支持事件的行数量。
    pub unknown_events: u64,
    /// Token 事件没有单次事实或可用累计回退的数量。
    pub missing_usage: u64,
    /// Token 字段违反 core 不变量的数量。
    pub invalid_usage: u64,
    /// Token 事件缺失或包含无法解析时间戳的数量。
    pub invalid_timestamps: u64,
    /// 因缺失 `last_token_usage` 而显式使用累计值的数量。
    pub cumulative_fallbacks: u64,
    /// fork/subagent 文件尚未找到可证明的自身开始边界。
    pub unresolved_owned_boundaries: u64,
}

impl JsonlWarningCounts {
    /// 返回会降低覆盖完整度的全部警告数量。
    pub fn total(self) -> u64 {
        self.malformed_lines
            .saturating_add(self.oversized_lines)
            .saturating_add(self.unknown_events)
            .saturating_add(self.missing_usage)
            .saturating_add(self.invalid_usage)
            .saturating_add(self.invalid_timestamps)
            .saturating_add(self.cumulative_fallbacks)
            .saturating_add(self.unresolved_owned_boundaries)
    }

    /// 合并同一来源后续追加片段的质量事实。所有历史行警告单调累加；
    /// 未解决 owner 边界是当前状态，采用追加解析结束时的新值。
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
            cumulative_fallbacks: self
                .cumulative_fallbacks
                .saturating_add(appended.cumulative_fallbacks),
            unresolved_owned_boundaries: appended.unresolved_owned_boundaries,
        }
    }
}
