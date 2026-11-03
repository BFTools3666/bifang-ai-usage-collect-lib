//! GLM 请求身份、线程来源、增量检查点和正文隔离的回归。

use std::io::Cursor;

use serde_json::{Value, json};

use super::jsonl::{GlmJsonlParseContext, GlmJsonlParseReport, parse_glm_jsonl_stream};
use crate::readers::CancellationToken;
use crate::{LocalError, LocalErrorKind, SourceProvenance, UsageCall};

/// 生成只含必要元数据的可信模型 I/O 事件。
fn event() -> Value {
    json!({
        "type": "model_io", "sessionId": "upstream-session", "requestId": "request-1",
        "attempt": 1, "querySource": "main_turn", "turnId": "turn-1",
        "startedAt": "2026-11-02T17:30:12.147Z", "model": {"modelId": "glm-test"},
        "response": {"usage": {
            "inputTokens": 20, "cacheReadTokens": 12, "cacheWriteTokens": 1,
            "outputTokens": 5, "totalTokens": 25
        }}
    })
}

/// 构造含根和文件 namespace 的安全来源证据。
fn provenance(root: &str, source: &str) -> SourceProvenance {
    SourceProvenance {
        root_id: root.to_owned(),
        source_id: source.to_owned(),
        relative_label: "model-io-sess_file.jsonl".to_owned(),
        archived: false,
    }
}

/// 对同一个上下文追加完整事件，并把失败前观察与成功报告分别返回。
fn append(
    value: &Value,
    context: &mut GlmJsonlParseContext,
    source: &SourceProvenance,
) -> Result<(Vec<UsageCall>, GlmJsonlParseReport), LocalError> {
    let text = format!("{value}\n");
    let mut calls = Vec::new();
    let report = parse_glm_jsonl_stream(
        Cursor::new(text.as_bytes()),
        32,
        context,
        source,
        &CancellationToken::default(),
        |call| {
            calls.push(call);
            Ok(())
        },
    )?;
    Ok((calls, report))
}

/// 请求身份包含根与文件 namespace，同一上游请求在不同来源中互不吞并。
#[test]
fn request_identity_is_namespaced_by_root_and_file() {
    let mut ids = std::collections::BTreeSet::new();
    for (root, source) in [
        ("root-a", "file-a"),
        ("root-b", "file-a"),
        ("root-a", "file-b"),
    ] {
        let (calls, _) = append(
            &event(),
            &mut GlmJsonlParseContext::new("same-file"),
            &provenance(root, source),
        )
        .expect("distinct source parses");
        ids.insert(calls[0].logical_call_id.clone());
    }
    assert_eq!(ids.len(), 3);
}

/// 线程分组只使用上游 sessionId，不把文件名伪装成会话身份。
#[test]
fn thread_identity_uses_upstream_session_id() {
    let (first, _) = append(
        &event(),
        &mut GlmJsonlParseContext::new("file-a"),
        &provenance("root", "a"),
    )
    .expect("first source parses");
    let (second, _) = append(
        &event(),
        &mut GlmJsonlParseContext::new("file-b"),
        &provenance("root", "b"),
    )
    .expect("second source parses");
    assert_eq!(first[0].thread_key, second[0].thread_key);
    assert_eq!(
        first[0].thread_key,
        crate::stable_id("glm-thread", "upstream-session")
    );
}

/// 精确重复跨增量和序列化恢复只计一次，模型或计数变化均拒绝发布。
#[test]
fn restored_context_rejects_conflicting_request_observations() {
    let source = provenance("root", "source");
    let mut context = GlmJsonlParseContext::new("file");
    append(&event(), &mut context, &source).expect("first observation parses");
    let encoded = serde_json::to_string(&context).expect("checkpoint encodes");
    let mut restored: GlmJsonlParseContext =
        serde_json::from_str(&encoded).expect("checkpoint restores");
    let (calls, report) = append(&event(), &mut restored, &source).expect("duplicate is harmless");
    assert!(calls.is_empty());
    assert_eq!(report.calls_emitted, 0);
    for changed_field in ["model", "usage"] {
        let mut changed = event();
        if changed_field == "model" {
            changed["model"]["modelId"] = json!("different-model");
        } else {
            changed["response"]["usage"]["totalTokens"] = json!(26);
        }
        let error = append(&changed, &mut restored.clone(), &source)
            .expect_err("conflicting identity is rejected");
        assert_eq!(error.kind(), LocalErrorKind::InvalidUsage);
        assert!(!error.to_string().contains("request-1"));
    }
}

/// 缺开始时间时采用可信完成时间，且模型只由顶层 model.modelId 提供。
#[test]
fn completed_timestamp_fallback_does_not_guess_model() {
    let mut value = event();
    value
        .as_object_mut()
        .expect("event object")
        .remove("startedAt");
    value.as_object_mut().expect("event object").remove("model");
    value["completedAt"] = json!("2026-11-02T17:30:26.793Z");
    value["response"]["modelId"] = json!("response-model-is-not-authoritative");
    let (calls, report) = append(
        &value,
        &mut GlmJsonlParseContext::new("file"),
        &provenance("root", "a"),
    )
    .expect("completed timestamp is usable");
    assert_eq!(report.calls_emitted, 1);
    assert_eq!(calls[0].occurred_at_epoch_ms, 1_793_640_626_793);
    assert_eq!(calls[0].model, None);
}

/// 未完成无用量行不降低覆盖，缺稳定请求身份的已计费行保守报缺口。
#[test]
fn unfinished_event_is_ignored_and_missing_identity_is_reported() {
    let mut pending = event();
    pending["response"] = json!({"finishReason": "pending"});
    let (calls, report) = append(
        &pending,
        &mut GlmJsonlParseContext::new("file"),
        &provenance("root", "a"),
    )
    .expect("pending event is valid JSON");
    assert!(calls.is_empty());
    assert_eq!(report.warnings.total(), 0);
    let mut anonymous = event();
    anonymous
        .as_object_mut()
        .expect("event object")
        .remove("requestId");
    let (calls, report) = append(
        &anonymous,
        &mut GlmJsonlParseContext::new("file"),
        &provenance("root", "a"),
    )
    .expect("missing identity is a quality warning");
    assert!(calls.is_empty());
    assert_eq!(report.warnings.invalid_identities, 1);
}

/// 超长正文不会丢掉行尾用量，也不会写入可持久化的解析上下文。
#[test]
fn oversized_body_preserves_usage_without_retaining_content() {
    let mut value = event();
    let private_body = "private-body-marker".repeat(100_000);
    value["request"] = json!({"body": private_body, "headers": {"authorization": "secret-marker"}});
    value["response"]["body"] = json!({"messages": ["private-response-marker"]});
    let mut context = GlmJsonlParseContext::new("file");
    let (calls, report) = append(&value, &mut context, &provenance("root", "a"))
        .expect("oversized JSON is parsed through the shared stream");
    assert_eq!(calls[0].usage.total_tokens, 25);
    assert_eq!(report.warnings.total(), 0);
    let checkpoint = serde_json::to_string(&context).expect("safe checkpoint encodes");
    for private in [
        "private-body-marker",
        "private-response-marker",
        "secret-marker",
        "request-1",
    ] {
        assert!(!checkpoint.contains(private));
    }
}
