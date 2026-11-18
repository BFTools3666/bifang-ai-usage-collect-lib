//! 用普通临时目录模拟本地来源内嵌其他卷，不在测试中创建平台挂载。

use super::jsonl::open_fixed_prefix_with_boundary;
use super::projects::discover_project_files_with_boundary;
use super::source_boundary::WorkbuddySourceBoundary;
use super::{WorkbuddyReadError, read_traces_with_boundary};
use crate::readers::LocalPathStatus;
use std::fs;
use std::path::Path;

/// 按夹具路径模拟底层卷，普通目录与本地子挂载均获准。
fn fixture_volume(path: &Path) -> LocalPathStatus {
    if path.ancestors().any(|part| {
        matches!(
            part.file_name().and_then(|name| name.to_str()),
            Some("network-project" | "network-pid" | "network.jsonl")
        )
    }) {
        return LocalPathStatus::RejectedNetwork;
    }
    if path.file_name().and_then(|name| name.to_str()) == Some("unknown.json") {
        return LocalPathStatus::Indeterminate;
    }
    LocalPathStatus::ConfirmedLocal
}

/// 构造日志夹具的父目录与测试文件。
fn write_fixture(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().expect("fixture file parent")).expect("fixture directory");
    fs::write(path, contents).expect("fixture transcript");
}

/// 本地 project 根的子挂载需要单独鉴别，拒绝文件不会计入候选或被打开。
#[test]
fn workbuddy_project_enumeration_and_open_reject_unapproved_child_volume() {
    let directory = tempfile::tempdir().expect("isolated WorkBuddy fixture");
    let root = fs::canonicalize(directory.path()).expect("plain fixture root");
    let normal = root.join("projects/local-project/local.jsonl");
    let unapproved = root.join("projects/local-project/network.jsonl");
    let nested_local = root.join("projects/local-project/session/subagents/local-child.jsonl");
    for path in [&normal, &unapproved, &nested_local] {
        write_fixture(path, "{}");
    }
    let boundary = WorkbuddySourceBoundary(fixture_volume);
    let result =
        discover_project_files_with_boundary(&root, &|| false, boundary).expect("layout discovery");
    assert_eq!(result.file_count(), 2);
    assert_eq!(result.skipped_count, 1);
    assert!(result.files.iter().all(|file| file.path != unapproved));
    assert!(open_fixed_prefix_with_boundary(&nested_local, boundary).is_ok());
    assert_eq!(
        open_fixed_prefix_with_boundary(&unapproved, boundary)
            .expect_err("do not open a remote transcript")
            .kind(),
        std::io::ErrorKind::InvalidInput
    );

    write_fixture(&root.join("projects/network-project/file.jsonl"), "{}");
    assert!(matches!(
        discover_project_files_with_boundary(&root, &|| false, boundary),
        Err(WorkbuddyReadError::Read)
    ));
}

/// Trace 的文件夹级和文件级子挂载均拒绝，只解析已确认本地的诊断。
#[test]
fn workbuddy_trace_reader_rejects_nested_network_and_unknown_volumes() {
    let directory = tempfile::tempdir().expect("isolated WorkBuddy trace fixture");
    let root = fs::canonicalize(directory.path()).expect("plain fixture root");
    let valid_trace =
        r#"{"trace":{"startedAt":"2026-11-18T08:00:00Z","duration":70,"status":"ok"}}"#;
    for relative in [
        "traces/local-pid/local.json",
        "traces/network-pid/remote.json",
        "traces/local-pid/unknown.json",
    ] {
        write_fixture(&root.join(relative), valid_trace);
    }
    let records =
        read_traces_with_boundary(&root, &|| false, WorkbuddySourceBoundary(fixture_volume));
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].duration_ms, 70);
}
