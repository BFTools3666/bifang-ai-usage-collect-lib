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
