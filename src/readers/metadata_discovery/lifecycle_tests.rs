use super::*;
use tempfile::tempdir;

/// 返回普通 Sources 发现使用的全物理 Agent 闭集。
fn all_agents() -> RootDiscoveryAgentScope {
    RootDiscoveryAgentScope::all_physical()
}

/// macOS 快速扫描不得先枚举未限定目录的全局 Spotlight 结果。
#[test]
fn user_priority_uses_only_a_scope_bounded_platform_index() {
    #[cfg(target_os = "macos")]
    {
        assert!(!platform_index::should_query(
            RootDiscoveryScope::UserPriority
        ));
        assert!(platform_index::should_query(
            RootDiscoveryScope::FullLocalVolumes
        ));
    }
    #[cfg(target_os = "windows")]
    {
        assert!(platform_index::should_query(
            RootDiscoveryScope::UserPriority
        ));
        assert!(platform_index::should_query(
            RootDiscoveryScope::FullLocalVolumes
        ));
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        assert!(!platform_index::should_query(
            RootDiscoveryScope::UserPriority
        ));
    }
}

/// 取消令牌必须传入目录 worker，并把任务收敛到明确 Cancelled 终态。
#[test]
fn traversal_worker_observes_cancellation_and_finishes_terminally() {
    let temp = tempdir().unwrap();
    let session = temp.path().join("root/sessions/2026");
    fs::create_dir_all(&session).unwrap();
    fs::write(session.join("rollout-cancelled.jsonl"), b"unopened").unwrap();
    let coordinator = RootDiscoveryCoordinator::default();
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        1,
    ));
    assert!(coordinator.request_cancel());

    let worker_pool = MetadataWorkerPool::new(1, all_agents(), coordinator.clone());
    let mut inspected = worker_pool.inspect(vec![(0, temp.path().into())]);
    let (_, inspected) = inspected.pop().expect("worker returns its cancelled job");
    let inspected = inspected.expect("cancelled inspection does not panic");
    assert!(inspected.children.is_empty());
    assert!(inspected.candidates.is_empty());
    assert_eq!(inspected.file_names_checked, 0);

    discover_metadata_roots_in_with_callback(
        &coordinator,
        LocalVolumeRoots {
            search_roots: vec![temp.path().into()],
            ..LocalVolumeRoots::default()
        },
        Vec::new(),
        all_agents(),
        &|_| {},
    );

    assert_eq!(
        coordinator.snapshot().lifecycle,
        crate::RootDiscoveryLifecycle::Cancelled
    );
    assert!(coordinator.candidates().is_empty());
}

/// Linux 默认根无需环境变量即可发现，且不会遍历用户主目录的其他子树。
#[cfg(target_os = "linux")]
#[test]
fn linux_priority_discovers_default_clients_without_scanning_sibling_directories() {
    let home = tempdir().unwrap();
    let app_data = home.path().join("custom-config");
    let desktop = "Claude/local-agent-mode-sessions/install/org/local_00000000-0000-0000-0000-000000000002/.claude";
    for relative in [
        ".codex/sessions/2026/rollout-default.jsonl",
        ".claude/projects/synthetic/00000000-0000-0000-0000-000000000001.jsonl",
        "Documents/other/sessions/2026/rollout-outside.jsonl",
    ] {
        let path = home.path().join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, b"metadata-only fixture; contents must not be parsed").unwrap();
    }
    for root in [
        app_data.join(desktop),
        app_data.join("Claude/local-agent-mode-sessions/install/org/local_invalid/.claude"),
        app_data.join("Other/.claude"),
    ] {
        let project = root.join("projects/synthetic");
        fs::create_dir_all(&project).unwrap();
        fs::write(
            project.join("00000000-0000-0000-0000-000000000003.jsonl"),
            b"metadata-only fixture; contents must not be parsed",
        )
        .unwrap();
    }
    let expected = [
        (home.path().join(".codex"), SourceClientKind::Codex),
        (home.path().join(".claude"), SourceClientKind::ClaudeCode),
        (app_data.join(desktop), SourceClientKind::ClaudeCode),
    ];
    for selected in [
        vec![SourceClientKind::Codex, SourceClientKind::ClaudeCode],
        vec![SourceClientKind::Codex],
        vec![SourceClientKind::ClaudeCode],
    ] {
        let agents = crate::resolve_root_discovery_agent_scope(
            RootDiscoveryScope::UserPriority,
            Some(&selected),
        )
        .unwrap();
        let roots = scope::linux_priority_roots(home.path().to_path_buf(), Some(&app_data), agents);
        let volumes = scope::local_priority_scope(roots.clone());
        let coordinator = RootDiscoveryCoordinator::default();
        discover_metadata_roots_in_with_callback(&coordinator, volumes, roots, agents, &|_| {});

        let candidates = coordinator.candidates();
        let expected = expected
            .iter()
            .filter(|(_, client)| selected.contains(client));
        assert_eq!(candidates.len(), expected.clone().count());
        for (path, client) in expected {
            assert!(candidates.iter().any(|candidate| {
                candidate.client == *client && Path::new(&candidate.absolute_path) == path
            }));
        }
    }
}
