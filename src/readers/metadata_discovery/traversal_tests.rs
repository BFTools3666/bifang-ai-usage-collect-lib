//! 分片和深度优先调度只限制工作集，不截断完整发现范围。

use super::{DirectoryCursor, MetadataWorkerPool, VolumeQueue, take_fair_jobs};
use crate::{RootDiscoveryAgentScope, RootDiscoveryCoordinator};
use std::collections::{HashSet, VecDeque};

/// 单个大宽度目录逐片返回，所有后续目录仍被完整交付。
#[test]
fn wide_directory_resumes_without_collecting_all_children() {
    let temp = tempfile::tempdir().expect("isolated metadata fixture");
    for index in 0..257 {
        std::fs::create_dir(temp.path().join(format!("child-{index}"))).expect("child fixture");
    }
    let coordinator = RootDiscoveryCoordinator::default();
    let pool = MetadataWorkerPool::new(1, RootDiscoveryAgentScope::all_physical(), coordinator);
    let mut cursor = DirectoryCursor::new(temp.path().into());
    let mut found = HashSet::new();
    let mut chunks = 0;
    loop {
        let mut results = pool.inspect(vec![(0, cursor)]);
        let (_, result) = results.pop().expect("one worker result");
        let result = result.expect("metadata worker returns normally");
        assert!(result.children.len() <= 128);
        for child in result.children {
            assert!(found.insert(child.path().to_path_buf()));
        }
        chunks += 1;
        let Some(next) = result.continuation else {
            break;
        };
        cursor = next;
    }
    assert_eq!(found.len(), 257);
    assert_eq!(chunks, 3);
}

/// 一轮内已派发深层目录时，不提前续读其祖先而积累整树兄弟目录。
#[test]
fn fair_jobs_do_not_resume_ancestors_while_deeper_work_is_dispatched() {
    let parent = DirectoryCursor::new("root".into());
    let child = parent.child("root/child".into());
    let mut queues = vec![VolumeQueue {
        pending: VecDeque::from([parent, child]),
        completed: false,
        traversal_root: "root".into(),
    }];
    let jobs = take_fair_jobs(&mut queues, 2, &mut 0);
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].1.depth(), 1);
    assert_eq!(queues[0].pending.len(), 1);
}

/// 遍历每个搜索根完成时各发出一次带计数的诊断事件，最后发出阶段结束事件，且不含完整路径。
#[test]
fn traversal_reports_each_root_once_and_the_phase_end() {
    use crate::{
        RootDiscoveryDiagnostic, RootDiscoveryPlatform, RootDiscoveryScope,
        RootDiscoveryStrategy,
    };
    use crate::readers::LocalVolumeRoots;
    use std::sync::{Arc, Mutex};

    let temp = tempfile::tempdir().expect("isolated metadata fixture");
    let root = temp.path().join("alpha");
    std::fs::create_dir_all(root.join("nested")).expect("nested fixture");
    std::fs::write(root.join("note.txt"), b"x").expect("file fixture");
    let coordinator = RootDiscoveryCoordinator::default();
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        1,
    ));
    let events = Arc::new(Mutex::new(Vec::new()));
    let sink_events = Arc::clone(&events);
    coordinator.set_diagnostic_sink(Arc::new(move |event| {
        sink_events.lock().expect("event lock").push(event);
    }));
    super::super::discover_metadata_roots_in_with_callback(
        &coordinator,
        LocalVolumeRoots {
            search_roots: vec![root],
            ..LocalVolumeRoots::default()
        },
        Vec::new(),
        RootDiscoveryAgentScope::all_physical(),
        &|_| {},
    );
    let events = events.lock().expect("event lock");
    let roots = events
        .iter()
        .filter_map(|event| match event {
            RootDiscoveryDiagnostic::TraversalRootFinished {
                root,
                ordinal,
                directories,
                file_names,
                ..
            } => Some((root.clone(), *ordinal, *directories, *file_names)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(roots, vec![("alpha".to_owned(), 0, 2, 1)]);
    assert!(matches!(
        events.last(),
        Some(RootDiscoveryDiagnostic::TraversalFinished { .. })
    ));
}
