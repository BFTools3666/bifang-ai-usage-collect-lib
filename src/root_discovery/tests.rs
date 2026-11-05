//! 验证数据根发现进度、诊断、候选选择和终态协调。

use super::*;

/// 系统索引阶段的分页计数立即体现在进度里，且只在 Running 时累加。
#[test]
fn index_file_names_advance_progress_only_while_running() {
    let coordinator = RootDiscoveryCoordinator::default();
    coordinator.add_index_file_names(5);
    assert_eq!(coordinator.snapshot().progress.file_names_checked, 0);
    assert!(coordinator.start(
        RootDiscoveryStrategy::WindowsSearch,
        RootDiscoveryPlatform::Windows,
        RootDiscoveryScope::UserPriority,
        1
    ));
    coordinator.add_index_file_names(512);
    coordinator.add_index_file_names(3);
    assert_eq!(coordinator.snapshot().progress.file_names_checked, 515);
}

/// 诊断事件只交给已安装的接收端；未安装时静默丢弃。
#[test]
fn diagnostics_reach_only_an_installed_sink() {
    let coordinator = RootDiscoveryCoordinator::default();
    coordinator.emit_diagnostic(RootDiscoveryDiagnostic::TraversalFinished { elapsed_ms: 1 });
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink_seen = Arc::clone(&seen);
    coordinator.set_diagnostic_sink(Arc::new(move |event| {
        sink_seen.lock().expect("seen lock").push(event);
    }));
    coordinator.emit_diagnostic(RootDiscoveryDiagnostic::TraversalFinished { elapsed_ms: 2 });
    assert_eq!(
        *seen.lock().expect("seen lock"),
        vec![RootDiscoveryDiagnostic::TraversalFinished { elapsed_ms: 2 }]
    );
}

/// 验证权限缺口保留计数，并把终态保守降级为部分完成。
#[test]
fn permission_gap_finishes_with_warning_counts() {
    let coordinator = RootDiscoveryCoordinator::default();
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        2
    ));
    coordinator.update_progress(RootDiscoveryProgress {
        volumes_completed: 2,
        volumes_total: 2,
        permission_denied: 1,
        ..RootDiscoveryProgress::default()
    });
    coordinator.finish(false, true);
    assert_eq!(
        coordinator.snapshot().lifecycle,
        RootDiscoveryLifecycle::Partial
    );
}

/// 验证取消不会被结束逻辑覆盖成成功。
#[test]
fn cancellation_is_terminal() {
    let coordinator = RootDiscoveryCoordinator::default();
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        1
    ));
    assert!(coordinator.request_cancel());
    coordinator.finish(false, true);
    assert_eq!(
        coordinator.snapshot().lifecycle,
        RootDiscoveryLifecycle::Cancelled
    );
}

/// 验证链接、reparse、网络或未知卷跳过后以部分覆盖结束。
#[test]
fn policy_skip_finishes_with_warning_counts() {
    let coordinator = RootDiscoveryCoordinator::default();
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        1
    ));
    coordinator.update_progress(RootDiscoveryProgress {
        volumes_completed: 1,
        volumes_total: 1,
        skipped: 1,
        ..RootDiscoveryProgress::default()
    });
    coordinator.finish(false, true);
    assert_eq!(
        coordinator.snapshot().lifecycle,
        RootDiscoveryLifecycle::Partial
    );
}

/// 每次真正启动都生成新运行编号，运行中的拒绝不会消耗编号。
#[test]
fn successful_starts_advance_the_run_id() {
    let coordinator = RootDiscoveryCoordinator::default();
    assert_eq!(coordinator.snapshot().run_id, 0);
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        1,
    ));
    assert_eq!(coordinator.snapshot().run_id, 1);
    assert!(!coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        1,
    ));
    assert_eq!(coordinator.snapshot().run_id, 1);
    coordinator.finish(false, false);
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        1,
    ));
    assert_eq!(coordinator.snapshot().run_id, 2);
}

/// adapter 的覆盖状态可明确保留预算缺口与取消终态。
#[test]
fn explicit_coverage_controls_the_terminal_state() {
    let coordinator = RootDiscoveryCoordinator::default();
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::ManualSubtree,
        1,
    ));
    coordinator.finish_with_coverage(CoverageState::Partial, false, true);
    assert_eq!(
        coordinator.snapshot().lifecycle,
        RootDiscoveryLifecycle::Partial
    );

    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::ManualSubtree,
        1,
    ));
    coordinator.finish_with_coverage(CoverageState::Cancelled, false, true);
    assert_eq!(
        coordinator.snapshot().lifecycle,
        RootDiscoveryLifecycle::Cancelled
    );

    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::ManualSubtree,
        1,
    ));
    coordinator.finish_with_coverage(CoverageState::Failed, false, true);
    let failed = coordinator.snapshot();
    assert_eq!(failed.lifecycle, RootDiscoveryLifecycle::Failed);
    assert_eq!(
        failed.error_code.as_deref(),
        Some(ROOT_DISCOVERY_COVERAGE_FAILED_CODE)
    );
}

/// 验证轮询快照与生命周期状态原子携带本轮 Agent 归属。
#[test]
fn snapshot_preserves_the_frozen_agent_scope() {
    let coordinator = RootDiscoveryCoordinator::default();
    let claude_scope = RootDiscoveryAgentScope::for_agent(SourceClientKind::ClaudeCode)
        .expect("Claude Code is a physical discovery Agent");
    assert!(coordinator.start_with_agent_scope(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::FullLocalVolumes,
        1,
        claude_scope,
    ));

    let status = coordinator.snapshot();
    assert_eq!(status.agent_scope, claude_scope);
    assert!(!status.agent_scope.contains(SourceClientKind::Codex));
    assert!(status.agent_scope.contains(SourceClientKind::ClaudeCode));
}

/// 验证候选确认去重、未知 ID 拒绝，且未确认候选不会跨任务持久化。
#[test]
fn candidate_selection_is_runtime_only_and_strict() {
    let coordinator = RootDiscoveryCoordinator::default();
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        1
    ));
    let candidate = RootCandidate {
        id: "candidate-a".to_owned(),
        client: SourceClientKind::Codex,
        absolute_path: "C:/runtime-only".into(),
        strategy: RootDiscoveryStrategy::MetadataTraversal,
        evidence: RootCandidateEvidence::CodexRollout,
    };
    assert!(coordinator.submit_candidate(candidate.clone()));
    assert!(!coordinator.submit_candidate(candidate.clone()));
    let selected = coordinator
        .select_candidates(&["candidate-a".to_owned(), "candidate-a".to_owned()])
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0], candidate);
    assert_eq!(
        coordinator.select_candidates(&["missing".to_owned()]),
        Err(RootCandidateSelectionError::UnknownCandidate)
    );

    coordinator.finish(false, true);
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        1
    ));
    assert!(coordinator.candidates().is_empty());
    assert!(coordinator.submit_candidate(candidate.clone()));
    assert_eq!(coordinator.candidates(), [candidate]);
}
/// 发现结束后仍等宿主持久化，期间取消和存储失败不会被成功终态覆盖。
#[test]
fn terminal_waits_for_host_publication_and_keeps_cancellation_or_failure() {
    let coordinator = RootDiscoveryCoordinator::default();
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        0
    ));
    coordinator.defer_completion();
    coordinator.finish(false, false);
    assert_eq!(
        coordinator.snapshot().lifecycle,
        RootDiscoveryLifecycle::Running
    );
    assert!(coordinator.request_cancel());
    coordinator.publish_completion();
    assert_eq!(
        coordinator.snapshot().lifecycle,
        RootDiscoveryLifecycle::Cancelled
    );
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        0
    ));
    coordinator.defer_completion();
    coordinator.finish(false, false);
    coordinator.fail("candidate_storage_failed");
    coordinator.finish(false, false);
    coordinator.finish_with_coverage(CoverageState::Cancelled, false, true);
    coordinator.publish_completion();
    assert_eq!(
        coordinator.snapshot().lifecycle,
        RootDiscoveryLifecycle::Failed
    );
    assert_eq!(
        coordinator.snapshot().error_code.as_deref(),
        Some("candidate_storage_failed")
    );
}
