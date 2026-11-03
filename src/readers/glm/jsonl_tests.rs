//! GLM 模型 I/O 解析器的行为回归；夹具结构来自本机真实 rollout 样本。

use std::io::Cursor;

use super::jsonl::{GlmJsonlParseContext, parse_glm_jsonl_stream};
use crate::{CancellationSignal, SourceProvenance, TokenUsage, UsageCall};

/// 合成夹具：按真实 rollout 结构覆盖各 querySource、缺用量、畸形与身份场景。
pub const SYNTHETIC_GLM_MODEL_IO_JSONL: &str =
    include_str!("../../../tests/fixtures/synthetic_glm_model_io_jsonl.jsonl");

/// 测试用空取消信号。
struct NeverCancelled;
impl CancellationSignal for NeverCancelled {
    /// 测试过程中永不取消。
    fn is_cancelled(&self) -> bool {
        false
    }
}

/// 读取夹具并收集全部调用。
fn parse_fixture(jsonl: &str) -> (Vec<UsageCall>, super::jsonl::GlmJsonlParseReport) {
    let mut context = GlmJsonlParseContext::new("sess-main-file");
    let provenance = SourceProvenance {
        source_id: "source".to_owned(),
        root_id: "root".to_owned(),
        relative_label: "label".to_owned(),
        archived: false,
    };
    let mut calls = Vec::new();
    let report = parse_glm_jsonl_stream(
        Cursor::new(jsonl.as_bytes()),
        crate::readers::DEFAULT_MAX_JSONL_LINE_BYTES,
        &mut context,
        &provenance,
        &NeverCancelled,
        |call| {
            calls.push(call);
            Ok(())
        },
    )
    .expect("fixture parses");
    (calls, report)
}

/// 各 querySource 的可信用量事件都计入，缺用量的合法行按非用量行忽略。
#[test]
fn emits_calls_for_every_credible_model_io_event() {
    let (calls, report) = parse_fixture(SYNTHETIC_GLM_MODEL_IO_JSONL);
    assert_eq!(calls.len(), 5);
    assert_eq!(report.calls_emitted, 5);
    let models = calls
        .iter()
        .map(|call| call.model.as_deref())
        .collect::<Vec<_>>();
    assert!(models.iter().all(|model| model.is_some()));
    assert!(models.contains(&Some("GLM-5.3-Flash")));
    assert!(models.contains(&Some("GLM-5.3")));
}

/// 行内 usage 与解析出的调用逐字段一致；total-only 行只保留总量，分项保持未提供。
#[test]
fn parsed_usage_matches_inline_counters() {
    let (calls, _) = parse_fixture(SYNTHETIC_GLM_MODEL_IO_JSONL);
    // 2026-11-02T17:30:12.147Z 的毫秒时间戳。
    let first = calls
        .iter()
        .find(|call| call.occurred_at_epoch_ms == 1_793_640_612_147)
        .expect("first main_turn call exists");
    assert_eq!(first.usage.input_tokens, Some(331_398));
    assert_eq!(first.usage.cached_input_tokens, Some(331_008));
    assert_eq!(first.usage.cache_write_input_tokens, Some(0));
    assert_eq!(first.usage.output_tokens, Some(459));
    assert_eq!(first.usage.total_tokens, 331_857);
    assert!(!first.usage.total_is_derived);
    // 2026-11-02T18:10:00.000Z 的 total-only 行。
    let total_only = calls
        .iter()
        .find(|call| call.occurred_at_epoch_ms == 1_793_643_000_000)
        .expect("web_search_tool total-only call exists");
    assert_eq!(total_only.usage.total_tokens, 999);
    assert_eq!(total_only.usage.input_tokens, None);
    assert_eq!(total_only.usage.output_tokens, None);
}

/// 同一 requestId 不同 attempt 的两次响应是独立上游事实，身份不得碰撞。
#[test]
fn retries_with_same_request_id_get_distinct_identities() {
    let (calls, _) = parse_fixture(SYNTHETIC_GLM_MODEL_IO_JSONL);
    // attempt 1 于 17:30:12.147Z，attempt 2 于 20:00:00.000Z。
    let attempt_one = calls
        .iter()
        .find(|call| call.occurred_at_epoch_ms == 1_793_640_612_147)
        .expect("attempt 1 exists");
    let attempt_two = calls
        .iter()
        .find(|call| call.occurred_at_epoch_ms == 1_793_649_600_000)
        .expect("attempt 2 exists");
    assert_ne!(
        attempt_one.logical_call_id, attempt_two.logical_call_id,
        "attempt 必须参与稳定身份"
    );
}

/// 畸形行、缺类型行与缺开始时间行计入对应告警，不产生调用。
#[test]
fn malformed_unknown_and_missing_timestamp_lines_count_warnings() {
    let (calls, report) = parse_fixture(SYNTHETIC_GLM_MODEL_IO_JSONL);
    assert_eq!(calls.len(), 5);
    assert_eq!(report.warnings.malformed_lines, 1);
    assert_eq!(report.warnings.unknown_events, 1);
    assert_eq!(report.warnings.invalid_timestamps, 1);
    assert_eq!(report.warnings.missing_usage, 0);
    assert_eq!(report.warnings.invalid_usage, 0);
    assert_eq!(report.warnings.invalid_identities, 1);
}

/// 声明了 usage 但缺少可信总量的行计入缺用量告警。
#[test]
fn usage_without_total_counts_missing_usage() {
    let jsonl = "{\"type\":\"model_io\",\"requestId\":\"r\",\"startedAt\":\"2026-11-02T17:30:12.147Z\",\"response\":{\"usage\":{\"inputTokens\":5}}}\n";
    let (calls, report) = parse_fixture(jsonl);
    assert!(calls.is_empty());
    assert_eq!(report.warnings.missing_usage, 1);
}

/// 即使输入与输出齐全，也不能推算缺失的上游总量或生成调用。
#[test]
fn input_and_output_without_total_do_not_emit_derived_usage() {
    let jsonl = concat!(
        "{\"type\":\"model_io\",\"sessionId\":\"s\",\"requestId\":\"r\",\"attempt\":1,",
        "\"startedAt\":\"2026-11-02T17:30:12.147Z\",",
        "\"response\":{\"usage\":{\"inputTokens\":10,\"outputTokens\":5}}}\n"
    );
    let (calls, report) = parse_fixture(jsonl);
    assert!(calls.is_empty(), "缺少上游总量时不能生成派生调用");
    assert_eq!(report.warnings.missing_usage, 1);
}

/// 检查点提交到全部完整行的换行处，尾部未换行的半行不入库。
#[test]
fn checkpoint_commits_only_complete_lines() {
    let jsonl = "abc-partial-without-newline";
    let (_, report) = parse_fixture(jsonl);
    assert_eq!(report.checkpoint.committed_bytes, 0);
    assert_eq!(
        report.checkpoint.trailing_bytes,
        "abc-partial-without-newline".len() as u64
    );
    assert_eq!(report.calls_emitted, 0);
}

/// 夹具内全部调用与独立求和的总量逐项对账。
#[test]
fn fixture_total_is_deterministic() {
    let (calls, _) = parse_fixture(SYNTHETIC_GLM_MODEL_IO_JSONL);
    let total = calls.iter().fold(TokenUsage::zero(), |acc, call| {
        acc.checked_add(&call.usage).expect("fixture sum fits")
    });
    assert_eq!(total.total_tokens, 334_261);
}
