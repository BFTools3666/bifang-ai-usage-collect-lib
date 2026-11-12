use super::*;
use crate::{SourceProvenance, canonicalize_usage_calls};

/// 构造不含正文和主机路径的统计测试调用。
fn call(
    id: &str,
    model: Option<&str>,
    root: &str,
    total_tokens: u64,
    cached_tokens: u64,
) -> UsageCall {
    let input_tokens = total_tokens.saturating_sub(10);
    UsageCall {
        logical_call_id: id.to_owned(),
        occurred_at_epoch_ms: 1,
        model: model.map(ToOwned::to_owned),
        reasoning_effort: Some("medium".to_owned()),
        project_key: Some("project-a".to_owned()),
        thread_key: format!("thread-{id}"),
        project_label: None,
        thread_label: None,
        usage: TokenUsage::new(
            input_tokens,
            cached_tokens,
            Some(input_tokens.min(3)),
            10,
            4,
            Some(total_tokens),
        )
        .expect("synthetic usage is valid"),
        adapter_consistency_key: None,
        confidence: Confidence::Exact,
        provenance: vec![SourceProvenance {
            source_id: format!("source-{root}-{id}"),
            root_id: root.to_owned(),
            relative_label: format!("sessions/{id}.jsonl"),
            archived: false,
        }],
    }
}

/// 验证 Top-N、其余项与总计对全部可加字段严格对账。
#[test]
fn stable_group_id_golden_values() {
    let cases = [
        (UsageDimension::Model, None, "e6104504144237c6"),
        (UsageDimension::Model, Some(""), "bb020d3315a8b28e"),
        (UsageDimension::Model, Some("gpt-5"), "4909fdc1cf0c1389"),
        (UsageDimension::Model, Some("项目"), "27cb79ae9d01c4b5"),
    ];
    for (dimension, key, hash) in cases {
        assert_eq!(
            stable_group_id(dimension, key),
            format!("statistics-{hash}")
        );
    }
}

#[test]
fn bounded_groups_reconcile_with_total_and_sort_deterministically() {
    let canonical = canonicalize_usage_calls(vec![
        call("a", Some("model-b"), "root-a", 110, 20),
        call("b", Some("model-a"), "root-a", 110, 10),
        call("c", None, "root-b", 50, 0),
    ]);

    let result = group_usage(&canonical, UsageDimension::Model, 2).expect("grouping must succeed");
    let reconciled = result.groups[0]
        .measure
        .checked_add(&result.groups[1].measure)
        .and_then(|measure| {
            measure.checked_add(result.remainder.as_ref().expect("one group is bounded"))
        })
        .expect("reconciliation must not overflow");

    assert_eq!(result.groups[0].key.as_deref(), Some("model-a"));
    assert_eq!(result.groups[1].key.as_deref(), Some("model-b"));
    assert_eq!(result.remainder.as_ref().map(|row| row.call_count), Some(1));
    assert_eq!(reconciled, result.total);
    assert_eq!(result.total.tokens.reasoning_output_tokens, Some(12));
    assert_eq!(result.total.tokens.cache_write_input_tokens, Some(9));
}

/// 验证跨根副本只归属稳定排序后的一个 canonical 数据根。
#[test]
fn root_group_uses_one_deterministic_owner_for_duplicate_sources() {
    let original = call("same", Some("model-a"), "root-z", 100, 20);
    let mut duplicate = original.clone();
    duplicate.provenance[0].root_id = "root-a".to_owned();
    duplicate.provenance[0].source_id = "source-a".to_owned();
    let canonical = canonicalize_usage_calls(vec![original, duplicate]);

    let result =
        group_usage(&canonical, UsageDimension::Root, 10).expect("root grouping must succeed");

    assert_eq!(result.groups.len(), 1);
    assert_eq!(result.groups[0].key.as_deref(), Some("root-a"));
    assert_eq!(result.groups[0].measure.call_count, 1);
    assert_eq!(result.groups[0].measure.duplicate_source_count, 1);
    assert_eq!(result.total, result.groups[0].measure);
}

/// 验证空白与缺失字段进入明确未知组，零输入缓存占比保持不适用。
#[test]
fn unknown_group_and_zero_input_do_not_invent_values() {
    let mut unknown = call("unknown", Some("  "), "root-a", 10, 0);
    unknown.usage =
        TokenUsage::new(0, 0, None, 10, 0, Some(10)).expect("zero-input usage is valid");
    let canonical = canonicalize_usage_calls(vec![unknown]);

    let result =
        group_usage(&canonical, UsageDimension::Model, 10).expect("unknown grouping must succeed");

    assert_eq!(result.groups[0].key, None);
    assert_eq!(result.groups[0].measure.cache_read_basis_points, None);
    assert!(result.remainder.is_none());
}
