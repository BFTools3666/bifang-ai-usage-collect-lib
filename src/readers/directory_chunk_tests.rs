//! 合成目录验证分片完整性、单个打开文件以及取消和同句柄读取边界。

use super::{EntryAction, VisitWindow, visit_directory_chunk};
use crate::readers::CancellationToken;
use crate::readers::directory_cursor::DirectoryCursor;
use crate::readers::file_source::RolloutProbeBudget;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

/// 不排除任何合成文件，签名探测仍采用生产的同句柄验证。
fn window() -> VisitWindow {
    VisitWindow {
        scan_since_epoch_ms: i64::MIN,
        known_sources: Arc::new(HashMap::new()),
    }
}

/// 每片最多保留一个匹配文件，同时续读完整目录且不按总文件数截断。
#[test]
fn directory_chunks_deliver_every_file_with_one_open_source_per_chunk() {
    let temp = tempfile::tempdir().expect("isolated source root");
    let root = std::fs::canonicalize(temp.path()).expect("canonical fixture root");
    let directory = root.join("sessions");
    std::fs::create_dir(&directory).expect("fixture directory");
    for index in 0..173 {
        std::fs::write(directory.join(format!("rollout-{index}.jsonl")), format!("{{\"timestamp\":\"2026-11-03T00:00:00Z\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"fixture-{index}\"}}}}\n"))
            .expect("fixture source");
    }
    let cancellation = CancellationToken::new();
    let mut cursor = DirectoryCursor::new(directory);
    let mut budget = RolloutProbeBudget::new(u64::MAX, u64::MAX);
    let mut remaining = u64::MAX;
    let mut sources = std::collections::HashSet::new();
    loop {
        let chunk = visit_directory_chunk(
            cursor,
            root.clone(),
            remaining,
            &window(),
            &cancellation,
            budget,
        )
        .expect("fixture chunk");
        assert!(chunk.outcome.enumeration_complete);
        remaining = chunk.outcome.remaining_entries;
        budget = chunk.outcome.probe_budget;
        assert!(chunk.outcome.actions.len() <= 128);
        let matched = chunk
            .outcome
            .actions
            .iter()
            .filter(|action| matches!(action, EntryAction::IndexFile { .. }))
            .count();
        assert!(
            matched <= 1,
            "one chunk must not retain many source handles"
        );
        for action in chunk.outcome.actions {
            if let EntryAction::IndexFile {
                relative_label,
                source,
            } = action
            {
                assert!(sources.insert(relative_label));
                drop(source);
            }
        }
        let Some(next) = chunk.continuation else {
            break;
        };
        cursor = next;
    }
    assert_eq!(sources.len(), 173);
    assert_eq!(u64::MAX - remaining, 173);
}

/// 取消续读立即结束并放弃游标，不重新枚举或交付后续来源。
#[test]
fn cancelled_directory_chunk_drops_the_continuation() {
    let temp = tempfile::tempdir().expect("isolated source root");
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let chunk = visit_directory_chunk(
        DirectoryCursor::new(temp.path().into()),
        temp.path().into(),
        u64::MAX,
        &window(),
        &cancellation,
        RolloutProbeBudget::new(u64::MAX, u64::MAX),
    )
    .expect("cancelled chunk");
    assert!(chunk.outcome.cancelled);
    assert!(!chunk.outcome.enumeration_complete);
    assert!(chunk.outcome.actions.is_empty());
    assert!(chunk.continuation.is_none());
}

/// 分片交付后即使路径被替换，消费方仍从原先验证的同一文件句柄读取。
#[test]
fn chunk_keeps_the_validated_source_handle_when_path_is_replaced() {
    let temp = tempfile::tempdir().expect("isolated source root");
    let root = std::fs::canonicalize(temp.path()).expect("canonical fixture root");
    let directory = root.join("sessions");
    std::fs::create_dir(&directory).expect("fixture directory");
    let path = directory.join("rollout-one.jsonl");
    let original = b"{\"timestamp\":\"2026-11-03T00:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"original-fixture\"}}\n";
    std::fs::write(&path, original).expect("fixture source");
    let chunk = visit_directory_chunk(
        DirectoryCursor::new(directory),
        root.clone(),
        u64::MAX,
        &window(),
        &CancellationToken::new(),
        RolloutProbeBudget::new(u64::MAX, u64::MAX),
    )
    .expect("source chunk");
    let source = chunk
        .outcome
        .actions
        .into_iter()
        .find_map(|action| match action {
            EntryAction::IndexFile { source, .. } => Some(source),
            _ => None,
        })
        .expect("validated source is delivered");
    std::fs::rename(&path, root.join("old-source.jsonl")).expect("fixture path moves");
    std::fs::write(&path, b"replacement must not be read").expect("replacement fixture");
    let mut file = source.into_file();
    file.seek(SeekFrom::Start(0))
        .expect("original handle rewinds");
    let mut actual = Vec::new();
    file.read_to_end(&mut actual)
        .expect("original fixture reads");
    assert_eq!(actual, original);
}
