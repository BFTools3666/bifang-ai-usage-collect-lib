//! 外部消费者仅依赖扫描包完成发现、枚举、解析和统计，结果由调用方保管。

use std::collections::HashMap;
use std::fs::{self, File};

use ai_usage_collect_scan::readers::grok::enumerate::{
    EnumerateWindow, ScanCounters, enumerate_root_updates,
};
use ai_usage_collect_scan::readers::grok::jsonl::{GrokJsonlParseContext, parse_grok_jsonl_stream};
use ai_usage_collect_scan::readers::grok::{GrokDiscoveryInputs, discover_grok_quick};
use ai_usage_collect_scan::{
    LocalError, LocalErrorKind, ScanCancellation, SourceProvenance, aggregate_canonical_usage,
    canonicalize_usage_calls,
};

/// 外部客户端用自己的内存 sink 保存扫描结果，来源文件及目录不被扫描包写入。
#[test]
fn discovers_and_reads_usage_without_creating_storage() {
    let home = tempfile::tempdir().expect("isolated source root");
    let session = home
        .path()
        .join("sessions/%2Ftmp%2Fapp/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    fs::create_dir_all(&session).expect("fixture session");
    let source = session.join("updates.jsonl");
    let original = include_str!("fixtures/synthetic_grok_updates_jsonl.jsonl");
    fs::write(&source, original).expect("synthetic metadata fixture");
    let cancellation = ScanCancellation::new();
    let discovered = discover_grok_quick(
        &GrokDiscoveryInputs {
            home_dir: None,
            grok_home: Some(home.path().to_path_buf()),
            registered_roots: Vec::new(),
        },
        &cancellation,
    );
    assert_eq!(discovered.roots.len(), 1);
    let root = &discovered.roots[0];
    let enumerated = enumerate_root_updates(
        root,
        ScanCounters {
            remaining_directories: u64::MAX,
            remaining_entries: u64::MAX,
            ..ScanCounters::default()
        },
        &EnumerateWindow {
            scan_since_epoch_ms: i64::MIN,
            known_sources: HashMap::new(),
        },
        &cancellation,
    );
    assert!(enumerated.enumeration_complete);
    assert_eq!(enumerated.paths.len(), 1);
    let mut context = GrokJsonlParseContext::new("external-client", None);
    let provenance = SourceProvenance {
        root_id: root.root_id.clone(),
        source_id: "external-source".into(),
        relative_label: "sessions/fixture/updates.jsonl".into(),
        archived: false,
    };
    let mut calls = Vec::new();
    let report = parse_grok_jsonl_stream(
        File::open(&enumerated.paths[0]).expect("read-only source"),
        1024 * 1024,
        &mut context,
        &provenance,
        &cancellation,
        |call| {
            calls.push(call);
            Ok(())
        },
    )
    .expect("metadata parsed");
    assert_eq!(report.calls_emitted, 3);
    let aggregate =
        aggregate_canonical_usage(&canonicalize_usage_calls(calls)).expect("exact aggregate");
    assert_eq!(aggregate.tokens.total_tokens, 260);
    assert_eq!(
        fs::read_to_string(&source).expect("original source"),
        original
    );
    assert_eq!(fs::read_dir(home.path()).expect("root entries").count(), 1);
    assert_eq!(fs::read_dir(&session).expect("session entries").count(), 1);
}

/// 取消在读取边界阻止输出，调用方 sink 的失败原样传播且不被库吞掉。
#[test]
fn cancellation_and_consumer_errors_stop_reading() {
    let original = include_str!("fixtures/synthetic_grok_updates_jsonl.jsonl");
    let provenance = SourceProvenance {
        source_id: "source".into(),
        root_id: "root".into(),
        relative_label: "fixture".into(),
        archived: false,
    };
    let cancelled = ScanCancellation::new();
    cancelled.cancel();
    let report = parse_grok_jsonl_stream(
        original.as_bytes(),
        1024 * 1024,
        &mut GrokJsonlParseContext::new("fixture", None),
        &provenance,
        &cancelled,
        |_| panic!("cancelled scan must not emit a record"),
    )
    .expect("cancelled report");
    assert!(report.cancelled);
    assert_eq!(report.calls_emitted, 0);
    let error = parse_grok_jsonl_stream(
        original.as_bytes(),
        1024 * 1024,
        &mut GrokJsonlParseContext::new("fixture", None),
        &provenance,
        &ScanCancellation::new(),
        |_| {
            Err(LocalError::new(
                LocalErrorKind::SourceUnavailable,
                "consumer rejected record",
            ))
        },
    )
    .expect_err("sink failure must stop parsing");
    assert_eq!(error.to_string(), "consumer rejected record");
}

/// 独立客户端从两个 WorkBuddy 版本读取顶层及子代理用量，正文与取消状态保持隔离。
#[test]
fn reads_workbuddy_editions_without_runtime_or_database() {
    use ai_usage_collect_scan::readers::workbuddy::{
        WorkbuddyReadError, discover_workbuddy_sources_from_user_home, read_workbuddy_usage_inputs,
    };

    let home = tempfile::tempdir().expect("isolated user directory");
    for edition in [".workbuddy", ".workbuddy-ai"] {
        let project = home.path().join(edition).join("projects/project-a");
        fs::create_dir_all(project.join("session-a/subagents")).expect("fixture layout");
        for (relative, message_id) in [
            ("session-a.jsonl", "parent"),
            ("session-a/subagents/agent-a.jsonl", "child"),
        ] {
            let event = serde_json::json!({
                "timestamp": 1_783_252_800_000_i64,
                "sessionId": "session-a",
                "message": "private-body-bait",
                "providerData": {
                    "messageId": message_id,
                    "model": "fixture-model",
                    "usage": {
                        "inputTokens": 10, "outputTokens": 5, "totalTokens": 15,
                        "inputTokensDetails": [{"cached_tokens": 2}], "requests": 1
                    },
                    "rawUsage": {"credit": 0.5}
                }
            });
            fs::write(project.join(relative), format!("{event}\n")).expect("synthetic usage");
        }
    }
    let sources = discover_workbuddy_sources_from_user_home(home.path()).expect("fixed roots");
    assert_eq!(sources.len(), 2);
    let cancellation = ScanCancellation::new();
    let inputs = read_workbuddy_usage_inputs(&sources, &cancellation).expect("read-only metadata");
    assert_eq!(inputs.records.len(), 4);
    assert_eq!(inputs.root_aliases.len(), 2);
    assert_eq!(
        inputs
            .records
            .iter()
            .map(|record| record.total_tokens)
            .sum::<i64>(),
        60
    );
    assert!(
        inputs
            .records
            .iter()
            .all(|record| record.model.as_deref() == Some("fixture-model"))
    );
    cancellation.cancel();
    assert!(matches!(
        read_workbuddy_usage_inputs(&sources, &cancellation),
        Err(WorkbuddyReadError::Read)
    ));
}
