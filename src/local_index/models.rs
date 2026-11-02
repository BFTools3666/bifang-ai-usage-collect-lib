//! 本机索引跨 adapter 使用的安全值对象。

use crate::{Confidence, TokenUsage};

/// Codex 新观察相对同线程既有精确事实的持久化动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexCallWriteDecision {
    /// 较弱推算值不得覆盖已经出现的精确事实。
    IgnoreDerived,
    /// 精确事实出现时先清理同线程旧推算值，再写入当前观察。
    ReplaceDerivedThenUpsert,
    /// 不需要额外清理，直接插入或更新当前观察。
    Upsert,
}

/// 按“精确事实永久优先”规则决定 Codex 单条观察的写入动作。
pub const fn codex_call_write_decision(
    confidence: Confidence,
    exact_thread_exists: bool,
) -> CodexCallWriteDecision {
    match (confidence, exact_thread_exists) {
        (Confidence::Derived, true) => CodexCallWriteDecision::IgnoreDerived,
        (Confidence::Exact, _) => CodexCallWriteDecision::ReplaceDerivedThenUpsert,
        _ => CodexCallWriteDecision::Upsert,
    }
}

/// 数据库既有 Claude 调用的安全 Token 计数投影。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedTokenUsage {
    /// 输入 Token 总量。
    pub input_tokens: Option<u64>,
    /// 缓存读取 Token；旧 schema 不可用时为 `None`。
    pub cached_input_tokens: Option<u64>,
    /// 缓存写入 Token；旧 schema 不可用时为 `None`。
    pub cache_write_input_tokens: Option<u64>,
    /// 输出 Token 总量。
    pub output_tokens: Option<u64>,
    /// 总 Token。
    pub total_tokens: u64,
}

impl From<&TokenUsage> for IndexedTokenUsage {
    /// 从已校验 Token 事实构造持久化比较投影。
    fn from(usage: &TokenUsage) -> Self {
        Self {
            input_tokens: usage.input_tokens,
            cached_input_tokens: usage.cached_input_tokens,
            cache_write_input_tokens: usage.cache_write_input_tokens,
            output_tokens: usage.output_tokens,
            total_tokens: usage.total_tokens,
        }
    }
}

/// 判断 Claude 后续观察是否逐字段单调不减，并拒绝不可安全拆分的缓存事实。
pub fn claude_usage_observation_is_monotonic(
    previous: IndexedTokenUsage,
    next: &TokenUsage,
) -> bool {
    let (Some(previous_input), Some(next_input), Some(previous_output), Some(next_output)) = (
        previous.input_tokens,
        next.input_tokens,
        previous.output_tokens,
        next.output_tokens,
    ) else {
        return false;
    };
    let (Some(previous_cached), Some(next_cached)) =
        (previous.cached_input_tokens, next.cached_input_tokens)
    else {
        return false;
    };
    let (Some(previous_cache_write), Some(next_cache_write)) = (
        previous.cache_write_input_tokens,
        next.cache_write_input_tokens,
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
        && next.total_tokens >= previous.total_tokens
}

/// 描述一个来源文件的安全 checkpoint，不包含绝对路径或半行正文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSourceFile {
    /// adapter 生成的稳定来源 ID。
    pub source_id: String,
    /// 所属数据根 ID。
    pub root_id: String,
    /// 数据根内部的安全相对标签。
    pub relative_label: String,
    /// 来源文件的内部身份指纹，只用于判断追加还是替换。
    pub file_identity: String,
    /// 标识来源属于归档区域。
    pub archived: bool,
    /// 上次快照读取到的文件大小。
    pub observed_size: u64,
    /// 上次快照的修改时间毫秒值。
    pub modified_at_epoch_ms: i64,
    /// 最后完整 JSONL 行之后的字节偏移。
    pub parsed_offset: u64,
    /// 未提交末行的字节数量，不保存其原始内容。
    pub trailing_bytes: u64,
    /// 当前来源采用的解析器版本。
    pub parser_version: u32,
    /// 截断或替换时递增的来源 generation。
    pub generation: u64,
    /// 本 generation 是否已写入兼容 cumulative 快照。
    pub token_snapshots_ready: bool,
}

/// 汇总 Claude 单调 upsert 的新增与被拒绝回退观察数量。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaudeBatchOutcome {
    /// 本次批次内首次插入的观察数量。
    pub added: u64,
    /// 本次批次内因回退而被拒绝、保留旧事实的观察数量。
    pub regressed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Codex 只在精确事实出现时清理推算行，并拒绝后续推算回退。
    #[test]
    fn decides_codex_write_action_from_confidence() {
        assert_eq!(
            codex_call_write_decision(Confidence::Derived, true),
            CodexCallWriteDecision::IgnoreDerived
        );
        assert_eq!(
            codex_call_write_decision(Confidence::Exact, false),
            CodexCallWriteDecision::ReplaceDerivedThenUpsert
        );
        assert_eq!(
            codex_call_write_decision(Confidence::Suspected, true),
            CodexCallWriteDecision::Upsert
        );
    }

    /// Claude 单调比较同时检查输入拆分、输出与总量。
    #[test]
    fn rejects_regressed_or_incomplete_claude_usage() {
        let previous = IndexedTokenUsage {
            input_tokens: Some(20),
            cached_input_tokens: Some(5),
            cache_write_input_tokens: Some(3),
            output_tokens: Some(4),
            total_tokens: 24,
        };
        let monotonic =
            TokenUsage::new(22, 6, Some(3), 5, 0, Some(27)).expect("monotonic fixture is valid");
        let regressed = TokenUsage::new(21, 4, Some(3), 5, 0, Some(26))
            .expect("regressed fixture is structurally valid");
        let incomplete = TokenUsage::new_with_availability(22, None, Some(3), 5, Some(0), Some(27))
            .expect("incomplete cache fixture is structurally valid");

        assert!(claude_usage_observation_is_monotonic(previous, &monotonic));
        assert!(!claude_usage_observation_is_monotonic(previous, &regressed));
        assert!(!claude_usage_observation_is_monotonic(
            previous,
            &incomplete
        ));
    }
}
