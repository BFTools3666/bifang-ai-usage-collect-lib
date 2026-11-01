//! 调用级去重与可见性衍生。

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    Confidence, SessionTokenSnapshot, SourceProvenance, TokenUsage, TotalTokenAccounting, UsageCall,
};

/// 描述 definite duplicate 合并时发现的质量问题。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CanonicalizationWarningKind {
    /// 同一逻辑调用指纹携带不同 Token 字段。
    ConflictingDuplicate,
}

/// 描述不含正文或绝对路径的 canonical 质量警告。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalizationWarning {
    /// 出现冲突的逻辑调用指纹。
    pub logical_call_id: String,
    /// 稳定警告类别。
    pub kind: CanonicalizationWarningKind,
}

/// 按逻辑调用指纹去重后的调用集合。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalUsageSet {
    /// 当前集合的 provider 级总 Token 聚合口径；筛选子集必须原样继承。
    pub total_token_accounting: TotalTokenAccounting,
    /// 空筛选或冲突整组丢弃后仍保留的 provider Token 分项可用性。
    pub empty_tokens: TokenUsage,
    /// 每个逻辑调用只保留一次的 canonical 事实。
    pub calls: Vec<UsageCall>,
    /// 被合并的重复来源总数。
    pub duplicate_source_count: u64,
    /// 按逻辑调用保存被合并观察数。
    pub duplicate_counts_by_call: BTreeMap<String, u64>,
    /// 由逻辑调用冲突生成的质量警告。
    pub warnings: Vec<CanonicalizationWarning>,
    /// Codex 历史兼容 cumulative 快照；不参与 ADR-104 生产聚合。
    pub snapshots: Vec<SessionTokenSnapshot>,
}

/// 统一去重结果并保持 provenance。
pub fn canonicalize_usage_calls(calls: Vec<UsageCall>) -> CanonicalUsageSet {
    let mut canonical = BTreeMap::<String, UsageCall>::new();
    let mut duplicate_source_count = 0_u64;
    let mut duplicate_counts_by_call = BTreeMap::<String, u64>::new();
    let mut selected_confidence_by_call = BTreeMap::<String, Confidence>::new();
    let mut conflicted_calls = BTreeSet::<String>::new();
    let mut warnings = Vec::new();

    for call in calls {
        let logical_call_id = call.logical_call_id.clone();
        let incoming_confidence = call.confidence;

        match canonical.get_mut(&logical_call_id) {
            None => {
                selected_confidence_by_call.insert(logical_call_id.clone(), incoming_confidence);
                canonical.insert(logical_call_id, call);
            }
            Some(existing) => {
                duplicate_source_count = duplicate_source_count.saturating_add(1);
                let call_duplicate_count = duplicate_counts_by_call
                    .entry(logical_call_id.clone())
                    .or_default();
                *call_duplicate_count = call_duplicate_count.saturating_add(1);

                let selected_confidence = selected_confidence_by_call
                    .get(&logical_call_id)
                    .copied()
                    .unwrap_or(existing.confidence);
                let merged_usage = merge_compatible_token_usage(&existing.usage, &call.usage);
                let conflict = merged_usage.is_none();
                if conflict {
                    conflicted_calls.insert(logical_call_id.clone());
                    warnings.push(CanonicalizationWarning {
                        logical_call_id: logical_call_id.clone(),
                        kind: CanonicalizationWarningKind::ConflictingDuplicate,
                    });
                    if confidence_rank(incoming_confidence) > confidence_rank(selected_confidence) {
                        let mut replacement = call.clone();
                        replacement.provenance = existing.provenance.clone();
                        *existing = replacement;
                        selected_confidence_by_call
                            .insert(logical_call_id.clone(), incoming_confidence);
                    }
                    existing.confidence = Confidence::Suspected;
                } else {
                    existing.usage = merged_usage.expect("compatible usage was checked");
                    if confidence_rank(incoming_confidence) > confidence_rank(selected_confidence) {
                        selected_confidence_by_call
                            .insert(logical_call_id.clone(), incoming_confidence);
                        if !conflicted_calls.contains(&logical_call_id) {
                            existing.confidence = incoming_confidence;
                        }
                    }
                }

                merge_provenance(&mut existing.provenance, call.provenance);
            }
        }
    }

    for call in canonical.values_mut() {
        call.provenance.sort_by(|left, right| {
            left.root_id
                .cmp(&right.root_id)
                .then_with(|| left.source_id.cmp(&right.source_id))
                .then_with(|| left.relative_label.cmp(&right.relative_label))
                .then_with(|| left.archived.cmp(&right.archived))
        });
    }

    let calls = canonical.into_values().collect::<Vec<_>>();
    let empty_tokens = empty_tokens_for_calls(&calls);
    CanonicalUsageSet {
        total_token_accounting: TotalTokenAccounting::Observed,
        empty_tokens,
        calls,
        duplicate_source_count,
        duplicate_counts_by_call,
        warnings,
        snapshots: Vec::new(),
    }
}

/// 合并同一调用的互补可用分项；双方都提供且数值不同时才视为冲突。
fn merge_compatible_token_usage(left: &TokenUsage, right: &TokenUsage) -> Option<TokenUsage> {
    if left.total_tokens != right.total_tokens {
        return None;
    }
    /// 合并同一分项的互补可用值，并在两个已知值不一致时拒绝融合。
    fn merge_component(left: Option<u64>, right: Option<u64>) -> Option<Option<u64>> {
        match (left, right) {
            (Some(left), Some(right)) if left != right => None,
            (Some(value), _) | (_, Some(value)) => Some(Some(value)),
            (None, None) => Some(None),
        }
    }
    let mut merged = TokenUsage::new_with_component_availability(
        merge_component(left.input_tokens, right.input_tokens)?,
        merge_component(left.cached_input_tokens, right.cached_input_tokens)?,
        merge_component(
            left.cache_write_input_tokens,
            right.cache_write_input_tokens,
        )?,
        merge_component(left.output_tokens, right.output_tokens)?,
        merge_component(left.reasoning_output_tokens, right.reasoning_output_tokens)?,
        Some(left.total_tokens),
    )
    .ok()?;
    merged.total_is_derived = left.total_is_derived && right.total_is_derived;
    Some(merged)
}

impl CanonicalUsageSet {
    /// 把单一 provider 的已批准总量口径附到 canonical 集合并返回自身。
    pub fn with_total_token_accounting(mut self, accounting: TotalTokenAccounting) -> Self {
        self.total_token_accounting = accounting;
        self
    }
}

/// 按 thread + 时间 + logical_call_id 去重会话快照，合并跨文件 provenance。
///
/// 同一 `session_id` 出现在活动文件与归档副本时不得把 latest 计两次。
pub fn canonicalize_session_snapshots(
    snapshots: Vec<SessionTokenSnapshot>,
) -> Vec<SessionTokenSnapshot> {
    let mut canonical =
        std::collections::BTreeMap::<(String, i64, String), SessionTokenSnapshot>::new();
    for snapshot in snapshots {
        let key = (
            snapshot.thread_key.clone(),
            snapshot.occurred_at_epoch_ms,
            snapshot.logical_call_id.clone(),
        );
        match canonical.get_mut(&key) {
            None => {
                canonical.insert(key, snapshot);
            }
            Some(existing) => {
                merge_provenance(&mut existing.provenance, snapshot.provenance);
            }
        }
    }
    let mut snapshots = canonical.into_values().collect::<Vec<_>>();
    for snapshot in &mut snapshots {
        snapshot.provenance.sort_by(|left, right| {
            left.root_id
                .cmp(&right.root_id)
                .then_with(|| left.source_id.cmp(&right.source_id))
                .then_with(|| left.relative_label.cmp(&right.relative_label))
                .then_with(|| left.archived.cmp(&right.archived))
        });
    }
    snapshots
}

/// 把已去重快照附到 canonical 集合；调用去重结果保持不变。
pub fn attach_session_snapshots(
    mut canonical: CanonicalUsageSet,
    snapshots: Vec<SessionTokenSnapshot>,
) -> CanonicalUsageSet {
    canonical.snapshots = canonicalize_session_snapshots(snapshots);
    canonical
}

/// 计算空集合时可见字段可用性，避免把未知误标记为 0。
pub(crate) fn empty_tokens_for_calls(calls: &[UsageCall]) -> TokenUsage {
    let Some(_) = calls.first() else {
        return TokenUsage::zero();
    };

    TokenUsage::zero_with_all_component_availability(
        calls.iter().all(|call| call.usage.input_tokens.is_some()),
        calls
            .iter()
            .all(|call| call.usage.cached_input_tokens.is_some()),
        calls
            .iter()
            .all(|call| call.usage.cache_write_input_tokens.is_some()),
        calls.iter().all(|call| call.usage.output_tokens.is_some()),
        calls
            .iter()
            .all(|call| call.usage.reasoning_output_tokens.is_some()),
    )
}

/// 合并重复调用的来源证据，并保持每个物理来源只出现一次。
fn merge_provenance(existing: &mut Vec<SourceProvenance>, incoming: Vec<SourceProvenance>) {
    for provenance in incoming {
        if !existing.contains(&provenance) {
            existing.push(provenance);
        }
    }
}

/// 把置信度映射为可比较等级，供冲突事实选择使用。
const fn confidence_rank(confidence: Confidence) -> u8 {
    match confidence {
        Confidence::Suspected => 0,
        Confidence::Derived => 1,
        Confidence::Exact => 2,
    }
}

/// 返回两份事实中较保守的置信度，避免聚合后夸大数据质量。
pub(crate) fn lower_confidence(left: Confidence, right: Confidence) -> Confidence {
    if confidence_rank(left) <= confidence_rank(right) {
        left
    } else {
        right
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::usage::TokenUsage;

    /// 构造 canonical 去重测试使用的同形调用。
    fn call(
        logical_call_id: &str,
        source_id: &str,
        usage: TokenUsage,
        confidence: Confidence,
    ) -> UsageCall {
        UsageCall {
            logical_call_id: logical_call_id.to_owned(),
            occurred_at_epoch_ms: 1,
            model: Some("gpt".to_owned()),
            reasoning_effort: None,
            project_key: None,
            thread_key: "thread".to_owned(),
            project_label: None,
            thread_label: None,
            usage,
            adapter_consistency_key: None,
            confidence,
            provenance: vec![SourceProvenance {
                source_id: source_id.to_owned(),
                root_id: "root".to_owned(),
                relative_label: "session".to_owned(),
                archived: false,
            }],
        }
    }

    /// 验证只有源数据实际提供的分项才会被标记为“可用（零值）”，
    /// 缺失的分项保持 `None` 而不是被误判为已提供的零。
    #[test]
    fn empty_tokens_for_calls_keep_partial_components_available() {
        let call = UsageCall {
            logical_call_id: "a".to_owned(),
            occurred_at_epoch_ms: 1,
            model: Some("gpt".to_owned()),
            reasoning_effort: None,
            project_key: None,
            thread_key: "t".to_owned(),
            project_label: None,
            thread_label: None,
            usage: TokenUsage::new_with_availability(2, Some(1), None, 3, None, Some(5))
                .expect("fixture is valid"),
            adapter_consistency_key: None,
            confidence: Confidence::Exact,
            provenance: vec![],
        };
        let empty = empty_tokens_for_calls(&[call]);
        assert_eq!(empty.cached_input_tokens, Some(0));
        assert!(empty.cache_write_input_tokens.is_none());
        assert!(empty.reasoning_output_tokens.is_none());
    }

    /// 验证同一逻辑调用的互补观察会安全补全 `None`，包括显式零，且不会生成冲突。
    #[test]
    fn canonical_duplicate_enriches_missing_components_without_conflict() {
        let sparse =
            TokenUsage::new_with_component_availability(None, None, None, None, None, Some(10))
                .expect("sparse usage is valid");
        let complete =
            TokenUsage::new(6, 0, Some(0), 4, 0, None).expect("complete derived usage is valid");
        let canonical = canonicalize_usage_calls(vec![
            call("same", "sparse", sparse, Confidence::Derived),
            call("same", "complete", complete, Confidence::Exact),
        ]);

        assert_eq!(canonical.calls.len(), 1);
        assert_eq!(canonical.duplicate_source_count, 1);
        assert!(canonical.warnings.is_empty());
        let merged = &canonical.calls[0];
        assert_eq!(merged.usage.input_tokens, Some(6));
        assert_eq!(merged.usage.cached_input_tokens, Some(0));
        assert_eq!(merged.usage.cache_write_input_tokens, Some(0));
        assert_eq!(merged.usage.output_tokens, Some(4));
        assert_eq!(merged.usage.reasoning_output_tokens, Some(0));
        assert_eq!(merged.usage.total_tokens, 10);
        assert!(!merged.usage.total_is_derived);
        assert_eq!(merged.confidence, Confidence::Exact);
        assert_eq!(merged.provenance.len(), 2);
        assert_eq!(canonical.empty_tokens.input_tokens, Some(0));
        assert_eq!(canonical.empty_tokens.output_tokens, Some(0));
    }

    /// 验证双方都提供但数值不同的分项仍按 definite duplicate 冲突处理。
    #[test]
    fn canonical_duplicate_rejects_conflicting_known_components() {
        let first = TokenUsage::new(6, 1, Some(0), 4, 0, Some(10)).expect("first usage is valid");
        let conflicting = TokenUsage::new(7, 1, Some(0), 3, 0, Some(10))
            .expect("conflicting usage is independently valid");
        let canonical = canonicalize_usage_calls(vec![
            call("same", "first", first.clone(), Confidence::Exact),
            call("same", "second", conflicting, Confidence::Exact),
        ]);

        assert_eq!(canonical.calls.len(), 1);
        assert_eq!(canonical.calls[0].usage, first);
        assert_eq!(canonical.calls[0].confidence, Confidence::Suspected);
        assert_eq!(canonical.calls[0].provenance.len(), 2);
        assert_eq!(canonical.duplicate_source_count, 1);
        assert_eq!(canonical.warnings.len(), 1);
        assert_eq!(
            canonical.warnings[0],
            CanonicalizationWarning {
                logical_call_id: "same".to_owned(),
                kind: CanonicalizationWarningKind::ConflictingDuplicate,
            }
        );
    }
}
