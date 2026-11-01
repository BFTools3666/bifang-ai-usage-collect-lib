//! 独立应用实现新格式，不依赖产品来源枚举、运行时或数据库。

use ai_usage_collect_scan::parsing::{
    ParseRequest, ParsedUsage, ParserDescriptor, ReadRange, UsageEmitter, UsageParser,
    read_observed_prefix,
};
use ai_usage_collect_scan::{
    Confidence, LocalError, LocalErrorKind, SourceProvenance, TokenUsage, UsageCall,
    aggregate_canonical_usage, canonicalize_usage_calls,
};
use std::io::{BufRead, BufReader, Cursor, Read};
use std::sync::atomic::{AtomicBool, Ordering};

/// 外部应用自己的格式，逐行把整数解释为一次已观测的输入 Token。
struct ExternalParser;

impl UsageParser for ExternalParser {
    /// 外部客户端自行定义的已读记录游标。
    type Context = u64;
    /// 外部格式自行定义的本轮输出数量。
    type Report = u64;
    /// 外部来源命名不需要加入任何产品闭集。
    fn descriptor(&self) -> ParserDescriptor {
        ParserDescriptor {
            id: "external-test-format",
            version: 1,
        }
    }
    /// 直接接入标准 Read、宿主取消检查和统一记录回调。
    fn parse(
        &self,
        reader: &mut dyn Read,
        request: ParseRequest<'_, u64>,
        emit: &mut UsageEmitter<'_>,
    ) -> Result<ParsedUsage<u64, u64>, LocalError> {
        let ParseRequest {
            mut context,
            provenance,
            cancellation,
            ..
        } = request;
        let mut emitted = 0;
        for line in BufReader::new(reader).lines() {
            if cancellation.is_cancelled() {
                break;
            }
            let line = line.map_err(|_| {
                LocalError::new(
                    LocalErrorKind::SourceUnavailable,
                    "external input could not be read",
                )
            })?;
            let amount = line.parse::<u64>().map_err(|_| {
                LocalError::new(LocalErrorKind::InvalidUsage, "external input is invalid")
            })?;
            context += 1;
            let call = UsageCall {
                logical_call_id: format!("external-{context}"),
                occurred_at_epoch_ms: 1_780_000_000_000,
                model: Some("external-model".into()),
                reasoning_effort: None,
                project_key: None,
                project_label: None,
                thread_key: "external-thread".into(),
                thread_label: None,
                usage: TokenUsage::new(amount, 0, Some(0), 0, 0, Some(amount))?,
                adapter_consistency_key: None,
                confidence: Confidence::Exact,
                provenance: vec![provenance.clone()],
            };
            emit(call)?;
            emitted += 1;
        }
        Ok(ParsedUsage {
            context,
            report: emitted,
        })
    }
}

/// 从外部动态解析器取得标准记录并复用库内去重和统计，观测前缀之外的追加不可见。
#[test]
fn external_dynamic_parser_reuses_acquisition_and_statistics() {
    let parser: &dyn UsageParser<Context = u64, Report = u64> = &ExternalParser;
    let flag = AtomicBool::new(false);
    let cancellation = || flag.load(Ordering::Acquire);
    let provenance = SourceProvenance {
        source_id: "external-source".into(),
        root_id: "external-root".into(),
        relative_label: "usage.txt".into(),
        archived: false,
    };
    let mut records = Vec::new();
    let parsed = read_observed_prefix(
        parser,
        Cursor::new(b"3\n4\n999\n"),
        ReadRange::new(0, 4).expect("observed prefix"),
        ParseRequest::new(0, &provenance, &cancellation),
        &mut |call| {
            records.push(call);
            Ok(())
        },
    )
    .expect("external parser works");
    assert_eq!(parser.descriptor().id, "external-test-format");
    assert_eq!(parsed.context, 2);
    assert_eq!(parsed.report, 2);
    let aggregate =
        aggregate_canonical_usage(&canonicalize_usage_calls(records)).expect("standard statistics");
    assert_eq!(aggregate.tokens.total_tokens, 7);
    assert_eq!(aggregate.call_count, 2);
    flag.store(true, Ordering::Release);
    let cancelled = read_observed_prefix(
        parser,
        Cursor::new(b"5\n"),
        ReadRange::new(0, 2).expect("range"),
        ParseRequest::new(parsed.context, &provenance, &cancellation),
        &mut |_| panic!("host cancellation must prevent output"),
    )
    .expect("cancelled result");
    assert_eq!(cancelled.context, 2);
    assert_eq!(cancelled.report, 0);
}

/// 来源错误与消费者错误保持区分，失败时不会交出可提交的解析状态。
#[test]
fn callback_failure_and_invalid_ranges_are_visible() {
    let provenance = SourceProvenance {
        source_id: "external".into(),
        root_id: "external".into(),
        relative_label: "fixture".into(),
        archived: false,
    };
    let error = read_observed_prefix(
        &ExternalParser,
        Cursor::new(b"3\n4\n"),
        ReadRange::new(0, 4).expect("range"),
        ParseRequest::new(0, &provenance, &|| false),
        &mut |_| {
            Err(LocalError::new(
                LocalErrorKind::SourceUnavailable,
                "consumer rejected observation",
            ))
        },
    )
    .expect_err("no successful state after sink failure");
    assert_eq!(error.message(), "consumer rejected observation");
    assert!(ReadRange::new(5, 4).is_err());
}
