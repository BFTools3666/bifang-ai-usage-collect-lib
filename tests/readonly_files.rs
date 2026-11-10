//! 外部程序使用文件获取 façade 和增量解析协议，不装配看板、设置或数据库。

use ai_usage_collect_scan::input::{
    JsonlFormat, SourceFileRequest, UsageReadOptions, open_grok_source, read_usage_file,
};
use ai_usage_collect_scan::parsing::{GrokParser, ParseRequest, ReadRange, read_observed_prefix};
use ai_usage_collect_scan::readers::grok::jsonl::GrokJsonlParseContext;
use ai_usage_collect_scan::{
    LocalError, LocalErrorKind, SourceProvenance, aggregate_canonical_usage,
    canonicalize_usage_calls,
};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

/// 合成 Codex 所有权元数据与用量事件，正文诱饵不属于输出合同。
const CODEX: &str = concat!(
    "{\"timestamp\":\"2026-08-15T12:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"session-fixture\",\"prompt\":\"private-body-bait\"}}\n",
    "{\"timestamp\":\"2026-08-15T12:01:00Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"call_id\":\"call-fixture\",\"model\":\"fixture-model\",\"info\":{\"last_token_usage\":{\"input_tokens\":10,\"cached_input_tokens\":2,\"output_tokens\":5,\"reasoning_output_tokens\":0,\"total_tokens\":15},\"total_token_usage\":{\"input_tokens\":100,\"cached_input_tokens\":20,\"output_tokens\":50,\"reasoning_output_tokens\":0,\"total_tokens\":150}}}}\n"
);
/// 合成 Claude 单次消息，缓存输入与输出共同组成真实总量。
const CLAUDE: &str = "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-08-15T12:00:00Z\",\"message\":{\"id\":\"msg-a\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":2,\"cache_creation_input_tokens\":3,\"output_tokens\":5},\"content\":\"private-body-bait\"}}\n";
/// Grok 完成与进行中轮次的共享合成记录。
const GROK: &str = include_str!("fixtures/synthetic_grok_updates_jsonl.jsonl");

/// 为指定内置格式建立完整、隔离的已批准布局。
fn source(format: JsonlFormat) -> (tempfile::TempDir, SourceFileRequest, u64) {
    let temp = tempfile::tempdir().expect("isolated source");
    let root = fs::canonicalize(temp.path()).expect("canonical root");
    let (relative, content, total) = match format {
        JsonlFormat::Codex => ("sessions/2026/rollout-fixture.jsonl", CODEX, 15),
        JsonlFormat::Claude => (
            "projects/-Users-private-home-repository/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl",
            CLAUDE,
            20,
        ),
        JsonlFormat::Grok => (
            "sessions/%2FUsers%2Fprivate-home%2Frepository/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/updates.jsonl",
            GROK,
            260,
        ),
        _ => panic!("test only constructs supported builtins"),
    };
    let file = root.join(relative);
    fs::create_dir_all(file.parent().expect("source parent")).expect("fixture folders");
    fs::write(&file, content).expect("synthetic source");
    (
        temp,
        SourceFileRequest {
            root_path: root,
            file_path: file,
            root_id: "root-fixture".into(),
        },
        total,
    )
}

/// 通过同一获取入口读取三个来源并序列化安全 wire 值，保留 Codex 快照且不创建存储。
#[test]
fn common_file_facade_preserves_usage_snapshots_and_wire_contract() {
    for format in [JsonlFormat::Codex, JsonlFormat::Claude, JsonlFormat::Grok] {
        let (_temp, request, total) = source(format);
        let original = fs::read(&request.file_path).expect("source bytes");
        let mut calls = Vec::new();
        let report = read_usage_file(
            format,
            request.clone(),
            UsageReadOptions::default(),
            &|| false,
            &mut |call| {
                calls.push(call);
                Ok(())
            },
        )
        .expect("standalone read");
        assert!(!report.cancelled);
        assert_eq!(report.calls_emitted, calls.len() as u64);
        assert_eq!(report.committed_bytes, original.len() as u64);
        assert_eq!(
            aggregate_canonical_usage(&canonicalize_usage_calls(calls.clone()))
                .expect("aggregate")
                .tokens
                .total_tokens,
            total
        );
        if format == JsonlFormat::Codex {
            assert!(
                !report.session_snapshots.is_empty(),
                "compatibility snapshots must remain observable"
            );
        }
        let bytes = serde_json::to_vec(&(calls, report))
            .expect("usage records and reports serialize to UTF-8");
        let wire: serde_json::Value =
            serde_json::from_slice(&bytes).expect("wire consumer parses owned data");
        assert_eq!(wire[1]["schemaVersion"], 1);
        let text = String::from_utf8(bytes).expect("UTF-8 JSON");
        assert!(!text.contains("private-body-bait"));
        for seed in [
            "-Users-private-home",
            "%2FUsers%2Fprivate-home",
            "/Users/private-home",
        ] {
            assert!(
                !text.contains(seed),
                "encoded project path must stay native: {seed}"
            );
        }
        assert!(!text.contains(request.root_path.to_str().expect("ASCII test root")));
        assert_eq!(
            fs::read(&request.file_path).expect("source unchanged"),
            original
        );
        assert_eq!(
            fs::read_dir(&request.root_path)
                .expect("root entries")
                .count(),
            1
        );
    }
}

/// 文件追加只进入下次观测；通用 Trait 保留原生上下文与完整行偏移供客户端续读。
#[test]
fn observed_prefix_and_resume_preserve_the_existing_source_protocol() {
    let (_temp, request, _) = source(JsonlFormat::Grok);
    let (file, info) = open_grok_source(request.clone())
        .expect("validated file")
        .into_parts();
    let extra = "{\"sessionUpdate\":\"turn_completed\",\"timestamp\":\"2026-08-15T12:02:00Z\",\"model\":\"grok-extra\",\"usage\":{\"inputTokens\":1,\"outputTokens\":2,\"totalTokens\":3}}\n";
    OpenOptions::new()
        .append(true)
        .open(&request.file_path)
        .expect("append fixture")
        .write_all(extra.as_bytes())
        .expect("append after observation");
    let provenance = SourceProvenance {
        source_id: info.source_id,
        root_id: info.root_id,
        relative_label: info.relative_label,
        archived: info.archived,
    };
    let context = GrokJsonlParseContext::new(&info.thread_seed, info.project_seed.as_deref());
    let mut calls = Vec::new();
    let parsed = read_observed_prefix(
        &GrokParser,
        file,
        ReadRange::new(0, info.observed_size).expect("prefix"),
        ParseRequest::new(context, &provenance, &|| false),
        &mut |call| {
            calls.push(call);
            Ok(())
        },
    )
    .expect("original prefix");
    assert_eq!(parsed.report.calls_emitted, 3);
    assert_eq!(
        aggregate_canonical_usage(&canonicalize_usage_calls(calls))
            .expect("prefix totals")
            .tokens
            .total_tokens,
        260
    );
    let (file, next) = open_grok_source(request)
        .expect("next observation")
        .into_parts();
    let mut appended = Vec::new();
    let resumed = read_observed_prefix(
        &GrokParser,
        file,
        ReadRange::new(parsed.report.checkpoint.committed_bytes, next.observed_size)
            .expect("append range"),
        ParseRequest::new(parsed.context, &provenance, &|| false),
        &mut |call| {
            appended.push(call);
            Ok(())
        },
    )
    .expect("resumed prefix");
    assert_eq!(resumed.report.calls_emitted, 1);
    assert_eq!(appended[0].usage.total_tokens, 3);
}

/// 原生句柄在路径被替换后仍指向原文件，解析不会重新打开被替换的路径。
#[cfg(unix)]
#[test]
fn validated_handle_survives_path_replacement() {
    let (_temp, request, _) = source(JsonlFormat::Grok);
    let (file, info) = open_grok_source(request.clone())
        .expect("same handle")
        .into_parts();
    fs::rename(&request.file_path, request.file_path.with_extension("old"))
        .expect("retain old source");
    fs::write(&request.file_path, b"replacement is not usage\n").expect("replace path");
    let provenance = SourceProvenance {
        source_id: info.source_id,
        root_id: info.root_id,
        relative_label: info.relative_label,
        archived: false,
    };
    let parsed = read_observed_prefix(
        &GrokParser,
        file,
        ReadRange::new(0, info.observed_size).expect("range"),
        ParseRequest::new(
            GrokJsonlParseContext::new("session", None),
            &provenance,
            &|| false,
        ),
        &mut |_| Ok(()),
    )
    .expect("old handle read");
    assert_eq!(parsed.report.calls_emitted, 3);
}

/// 拒绝父目录跳转及凭据文件名，不能在已批准根之外打开签名文件。
#[test]
fn rejects_parent_traversal_and_credential_files_before_reading() {
    let parent = tempfile::tempdir().expect("isolated tree");
    let parent = fs::canonicalize(parent.path()).expect("canonical tree");
    let root = parent.join("allowed");
    fs::create_dir_all(root.join("sessions")).expect("session area");
    fs::create_dir_all(root.join("projects")).expect("project area");
    let outside = parent.join("outside");
    fs::create_dir(&outside).expect("outside root");
    fs::write(outside.join("rollout-valid.jsonl"), CODEX).expect("signed outside fixture");
    for (format, relative) in [
        (
            JsonlFormat::Codex,
            "sessions/../../outside/rollout-valid.jsonl",
        ),
        (
            JsonlFormat::Claude,
            "projects/../../outside/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl",
        ),
        (JsonlFormat::Grok, "sessions/../../outside/updates.jsonl"),
        (JsonlFormat::Codex, "auth.json"),
    ] {
        let result = read_usage_file(
            format,
            SourceFileRequest {
                root_path: root.clone(),
                file_path: root.join(relative),
                root_id: "allowed".into(),
            },
            UsageReadOptions::default(),
            &|| false,
            &mut |_| panic!("unsafe input must not emit"),
        );
        assert_eq!(
            result.expect_err("unsafe path must be rejected").kind(),
            LocalErrorKind::InvalidPath
        );
    }
}

/// 协作取消可以借用宿主状态，预取消时不会访问不存在的根；sink 失败保持可见。
#[test]
fn host_cancellation_and_sink_failure_are_explicit() {
    let missing = SourceFileRequest {
        root_path: PathBuf::from("/nonexistent-usage-source"),
        file_path: PathBuf::from("/nonexistent-usage-source/sessions/rollout-a.jsonl"),
        root_id: "cancelled".into(),
    };
    let cancelled = read_usage_file(
        JsonlFormat::Codex,
        missing,
        UsageReadOptions::default(),
        &|| true,
        &mut |_| panic!("cancelled request cannot emit"),
    )
    .expect("cancelled before opening");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.bytes_consumed, 0);
    let (_temp, request, _) = source(JsonlFormat::Grok);
    let failed = read_usage_file(
        JsonlFormat::Grok,
        request,
        UsageReadOptions::default(),
        &|| false,
        &mut |_| {
            Err(LocalError::new(
                LocalErrorKind::SourceUnavailable,
                "consumer storage failed",
            ))
        },
    )
    .expect_err("sink failure cannot become success");
    assert_eq!(failed.message(), "consumer storage failed");
}

/// 非 UTF-8 归档路径仍从原生组件确定来源区域，不从编码字符串反推。
#[cfg(target_os = "linux")]
#[test]
fn non_utf8_archive_paths_keep_archived_provenance() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    let temp = tempfile::tempdir().expect("isolated archive");
    let root = fs::canonicalize(temp.path()).expect("canonical root");
    let directory = root
        .join("archived_sessions")
        .join(OsString::from_vec(vec![b'p', 0xff]));
    fs::create_dir_all(&directory).expect("native archive path");
    let file = directory.join("rollout-archive.jsonl");
    fs::write(&file, CODEX).expect("archive fixture");
    let mut calls = Vec::new();
    let report = read_usage_file(
        JsonlFormat::Codex,
        SourceFileRequest {
            root_path: root,
            file_path: file,
            root_id: "archive".into(),
        },
        UsageReadOptions::default(),
        &|| false,
        &mut |call| {
            calls.push(call);
            Ok(())
        },
    )
    .expect("native path is readable");
    assert_eq!(report.calls_emitted, 1);
    assert!(
        calls
            .iter()
            .flat_map(|call| &call.provenance)
            .all(|source| source.archived)
    );
    assert!(
        report
            .session_snapshots
            .iter()
            .flat_map(|snapshot| &snapshot.provenance)
            .all(|source| source.archived)
    );
}

/// 读取器拒绝来源叶子链接，避免在路径校验后跟随到根外文件。
#[cfg(unix)]
#[test]
fn file_facade_rejects_linked_sources() {
    use std::os::unix::fs::symlink;
    let (_temp, request, _) = source(JsonlFormat::Claude);
    let external = tempfile::NamedTempFile::new().expect("outside file");
    fs::remove_file(&request.file_path).expect("replace fixture leaf");
    symlink(external.path(), &request.file_path).expect("fixture link");
    assert!(
        read_usage_file(
            JsonlFormat::Claude,
            request,
            UsageReadOptions::default(),
            &|| false,
            &mut |_| Ok(())
        )
        .is_err()
    );
}

/// 在当前宿主真实文件系统上验证归档标记随调用与快照一起返回。
#[test]
fn archive_flags_are_preserved_by_the_file_facade() {
    let (_temp, mut request, _) = source(JsonlFormat::Codex);
    fs::rename(
        request.root_path.join("sessions"),
        request.root_path.join("archived_sessions"),
    )
    .expect("archive fixture");
    request.file_path = request
        .root_path
        .join("archived_sessions/2026/rollout-fixture.jsonl");
    let mut calls = Vec::new();
    let report = read_usage_file(
        JsonlFormat::Codex,
        request,
        UsageReadOptions::default(),
        &|| false,
        &mut |call| {
            calls.push(call);
            Ok(())
        },
    )
    .expect("archive read");
    assert_eq!(report.calls_emitted, 1);
    assert!(
        calls
            .iter()
            .flat_map(|call| &call.provenance)
            .all(|source| source.archived)
    );
    assert!(
        report
            .session_snapshots
            .iter()
            .flat_map(|snapshot| &snapshot.provenance)
            .all(|source| source.archived)
    );
}

/// 规范根与宿主原始临时路径表示可以组合使用，不把平台根别名误判为根外路径。
#[test]
fn platform_path_forms_preserve_file_acquisition() {
    for format in [JsonlFormat::Codex, JsonlFormat::Claude, JsonlFormat::Grok] {
        let (temp, mut request, _) = source(format);
        let relative = request
            .file_path
            .strip_prefix(&request.root_path)
            .expect("fixture relative")
            .to_path_buf();
        request.file_path = temp.path().join(relative);
        let mut count = 0;
        let report = read_usage_file(
            format,
            request,
            UsageReadOptions::default(),
            &|| false,
            &mut |_| {
                count += 1;
                Ok(())
            },
        )
        .expect("platform path alias is approved");
        assert_eq!(report.calls_emitted, count);
        assert!(count > 0);
    }
}

/// 默认大小写不敏感的 Windows 根别名按目录身份确认；大小写敏感卷不强行折叠。
#[cfg(windows)]
#[test]
fn windows_case_aliases_require_the_same_directory_identity() {
    let (_temp, mut request, _) = source(JsonlFormat::Grok);
    let relative = request
        .file_path
        .strip_prefix(&request.root_path)
        .expect("fixture relative")
        .to_path_buf();
    let alias = PathBuf::from(
        request
            .root_path
            .to_str()
            .expect("Unicode temporary root")
            .to_uppercase(),
    );
    request.file_path = alias.join(relative);
    let equivalent = same_file::is_same_file(&request.root_path, &alias).unwrap_or(false);
    let result = read_usage_file(
        JsonlFormat::Grok,
        request,
        UsageReadOptions::default(),
        &|| false,
        &mut |_| Ok(()),
    );
    assert_eq!(
        result.is_ok(),
        equivalent,
        "case-sensitive roots cannot be aliased"
    );
}

/// 普通 Win32 后缀不能因转为 verbatim 根而读取尾随点或空格的另一个目录。
#[cfg(windows)]
#[test]
fn windows_ambiguous_suffixes_are_rejected_before_opening() {
    let (_temp, mut request, _) = source(JsonlFormat::Grok);
    request.file_path = request
        .root_path
        .join("sessions./project/session/updates.jsonl");
    // 去掉 canonicalize 产生的 verbatim 前缀，构造普通 Win32 输入。
    let ordinary = request
        .file_path
        .to_str()
        .expect("Unicode fixture")
        .strip_prefix(r"\\?\")
        .expect("canonical verbatim prefix");
    request.file_path = PathBuf::from(ordinary);
    let error = read_usage_file(
        JsonlFormat::Grok,
        request,
        UsageReadOptions::default(),
        &|| false,
        &mut |_| panic!("ambiguous path cannot emit"),
    )
    .expect_err("ambiguous suffix must fail");
    assert_eq!(error.kind(), LocalErrorKind::InvalidPath);
}
