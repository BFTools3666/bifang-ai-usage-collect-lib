//! Grok 完成事件重放身份与数字时间边界回归，仅使用合成元数据。

use serde_json::{Value, json};

use crate::readers::CancellationToken;
use crate::{SourceProvenance, UsageCall, canonicalize_usage_calls};

use super::jsonl::{GrokJsonlParseContext, GrokJsonlParseReport, parse_grok_jsonl_stream};

/// 生成只含用量元数据的已完成事件；省略 ID 时复用真实生产匿名轮次能力。
fn completed_event(turn_id: Option<&str>, model: &str, timestamp: Value) -> String {
    let mut value = json!({"sessionUpdate":"turn_completed","timestamp":timestamp,"model":model,
        "usage":{"inputTokens":12,"outputTokens":8,"totalTokens":20}});
    if let Some(id) = turn_id {
        value["turnId"] = json!(id);
    }
    format!("{value}\n")
}

/// 从同一个安全解析上下文读取片段，模拟多轮追加而不访问任何真实文件。
fn parse_fragment(
    input: &str,
    context: &mut GrokJsonlParseContext,
) -> (Vec<UsageCall>, GrokJsonlParseReport) {
    let provenance = SourceProvenance {
        source_id: "grok-identity-fixture".into(),
        root_id: "grok-fixture-root".into(),
        relative_label: "grok-fixture".into(),
        archived: false,
    };
    let mut calls = Vec::new();
    let report = parse_grok_jsonl_stream(
        input.as_bytes(),
        1024 * 1024,
        context,
        &provenance,
        &CancellationToken::new(),
        |call| {
            calls.push(call);
            Ok(())
        },
    )
    .expect("synthetic Grok fragment parses");
    (calls, report)
}

/// 同一明确轮次和模型的重放必须规范成一次用量，不因行序号变化重复计量。
#[test]
fn repeated_explicit_turn_and_model_have_one_canonical_call() {
    let event = completed_event(Some("turn-1"), "grok-model", json!(1786891792));
    let mut context = GrokJsonlParseContext::new("session-1", None);
    let (calls, _) = parse_fragment(&format!("{event}{event}"), &mut context);
    assert_eq!(
        calls.len(),
        2,
        "parsing preserves observations for canonicalization"
    );
    assert_eq!(calls[0].logical_call_id, calls[1].logical_call_id);
    let canonical = canonicalize_usage_calls(calls);
    assert_eq!(canonical.calls.len(), 1);
    assert_eq!(canonical.calls[0].usage.total_tokens, 20);
    assert_eq!(canonical.duplicate_source_count, 1);
}

/// 跨增量片段及插入其他轮次后，明确轮次身份仍然不变。
#[test]
fn explicit_turn_replay_is_stable_across_incremental_fragments() {
    let event = completed_event(Some("turn-1"), "grok-model", json!(1786891792));
    let other = completed_event(Some("turn-2"), "grok-model", json!(1786891793));
    let mut context = GrokJsonlParseContext::new("session-1", None);
    let (mut calls, _) = parse_fragment(&event, &mut context);
    let first_id = calls[0].logical_call_id.clone();
    calls.extend(parse_fragment(&other, &mut context).0);
    let replay = parse_fragment(&event, &mut context).0;
    assert_eq!(first_id, replay[0].logical_call_id);
    calls.extend(replay);
    assert_eq!(canonicalize_usage_calls(calls).calls.len(), 2);

    let mut rebuilt = GrokJsonlParseContext::new("session-1", None);
    parse_fragment(&other, &mut rebuilt);
    assert_eq!(
        first_id,
        parse_fragment(&event, &mut rebuilt).0[0].logical_call_id
    );
}

/// 相同轮次的不同模型和不同会话不能误合并，保留各自独立成本。
#[test]
fn explicit_turn_identity_keeps_model_and_session_boundaries() {
    let mut session = GrokJsonlParseContext::new("session-1", None);
    let first = completed_event(Some("turn-1"), "grok-a", json!(1786891792));
    let second = completed_event(Some("turn-1"), "grok-b", json!(1786891792));
    let (mut calls, _) = parse_fragment(&format!("{first}{second}"), &mut session);
    let mut other_session = GrokJsonlParseContext::new("session-2", None);
    calls.extend(parse_fragment(&first, &mut other_session).0);
    assert_eq!(canonicalize_usage_calls(calls).calls.len(), 3);
}

/// 无轮次 ID 或空 ID 时仍按安全序号区分完成事件，不能丢弃真实追加。
#[test]
fn anonymous_and_empty_id_turns_remain_distinct_across_append() {
    let anonymous = completed_event(None, "grok-model", json!(1786891792));
    let empty = completed_event(Some(""), "grok-model", json!(1786891792));
    let mut context = GrokJsonlParseContext::new("session-1", None);
    let (mut calls, _) = parse_fragment(&format!("{anonymous}{empty}"), &mut context);
    calls.extend(parse_fragment(&format!("{anonymous}{empty}"), &mut context).0);
    assert_eq!(canonicalize_usage_calls(calls).calls.len(), 4);
}

/// 既有事件 ID 作为轮次 ID 的兼容回退时，同样承担稳定身份。
#[test]
fn fallback_event_id_replay_has_stable_identity() {
    let mut value: Value =
        serde_json::from_str(&completed_event(None, "grok-model", json!(1786891792)))
            .expect("event object");
    value["id"] = json!("event-1");
    let event = format!("{value}\n");
    let mut context = GrokJsonlParseContext::new("session-1", None);
    let (calls, _) = parse_fragment(&format!("{event}{event}"), &mut context);
    assert_eq!(canonicalize_usage_calls(calls).calls.len(), 1);
}

/// 极端数字不能 panic、夹紧或进入日期聚合，而应产生稳定时间戳告警。
#[test]
fn extreme_numeric_timestamps_are_rejected_without_panicking() {
    for timestamp in [
        json!(i64::MIN),
        json!(i64::MAX),
        json!(u64::MAX),
        json!(1e100),
        json!(-1e100),
    ] {
        let event = completed_event(Some("turn-1"), "grok-model", timestamp.clone());
        let mut context = GrokJsonlParseContext::new("session-1", None);
        let (calls, report) = parse_fragment(&event, &mut context);
        assert!(calls.is_empty(), "timestamp {timestamp}");
        assert_eq!(
            report.warnings.invalid_timestamps, 1,
            "timestamp {timestamp}"
        );
        assert_eq!(context.call_sequence, 0);
    }
}

/// 有效秒、毫秒、RFC3339及有限小数继续采用既有取整语义。
#[test]
fn valid_timestamp_formats_and_fractional_compatibility_are_preserved() {
    for (timestamp, expected) in [
        (json!(1786891792), 1786891792000_i64),
        (json!(1786891792000_i64), 1786891792000),
        (json!("2026-08-16T14:49:52Z"), 1786891792000),
        (json!(1786891792.75), 1786891792000),
        (json!(1.5), 1000),
        (json!(-1.5), -1000),
        (json!(-1786891792000_i64), -1786891792000),
    ] {
        let event = completed_event(Some("turn-1"), "grok-model", timestamp.clone());
        let mut context = GrokJsonlParseContext::new("session-1", None);
        let (calls, report) = parse_fragment(&event, &mut context);
        assert_eq!(calls.len(), 1, "timestamp {timestamp}");
        assert_eq!(
            calls[0].occurred_at_epoch_ms, expected,
            "timestamp {timestamp}"
        );
        assert_eq!(report.warnings.invalid_timestamps, 0);
    }
}
