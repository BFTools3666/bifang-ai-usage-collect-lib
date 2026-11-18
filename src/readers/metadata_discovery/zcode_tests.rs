//! GUI 元数据发现支持没有 CLI 日志的 ZCode 桌面安装。

use super::*;

/// 仅有桌面专用库的安装也能通过 UserPriority 环境根签名并提交正确 Agent。
#[test]
fn user_priority_finds_desktop_only_zcode_home() {
    let temp = tempfile::tempdir().expect("isolated ZCode home");
    let home = fs::canonicalize(temp.path()).expect("canonical home");
    let database = home.join(".zcode/cli/db");
    fs::create_dir_all(&database).expect("desktop root");
    fs::write(database.join("db.sqlite"), b"SQLite format 3\0").expect("desktop signature");
    let agents = crate::resolve_root_discovery_agent_scope(
        RootDiscoveryScope::UserPriority,
        Some(&[SourceClientKind::Glm]),
    )
    .expect("selected ZCode scope");
    let coordinator = RootDiscoveryCoordinator::default();
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        0,
    ));
    let inspection =
        explicit_environment_candidates(&coordinator, None, None, None, Some(home), agents);
    assert_eq!(inspection.candidates.len(), 1);
    assert_eq!(inspection.candidates[0].0, database);
    assert_eq!(
        inspection.candidates[0].1,
        RootCandidateEvidence::GlmModelIoUpdates
    );
    assert_eq!(inspection.skipped, 0);
    submit_candidate(
        &coordinator,
        inspection.candidates[0].0.clone(),
        inspection.candidates[0].1,
        RootDiscoveryStrategy::MetadataTraversal,
        agents,
        &|_| {},
    );
    let candidates = coordinator.candidates();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].client, SourceClientKind::Glm);
    assert_eq!(candidates[0].absolute_path, database);
}

/// 全盘发现只凭固定布局识别 ZCode 桌面候选，并排除无关 SQLite 文件。
#[test]
fn full_traversal_finds_only_fixed_zcode_database_layout() {
    let temp = tempfile::tempdir().expect("isolated ZCode volume");
    let desktop_root = temp.path().join("custom-home/.zcode/cli/db");
    for path in [
        desktop_root.join("db.sqlite"),
        desktop_root.join("other.sqlite"),
        temp.path().join("unrelated/cli/db/db.sqlite"),
    ] {
        fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture directory");
        fs::write(path, b"metadata only").expect("fixture file");
    }
    let volumes = LocalVolumeRoots {
        search_roots: vec![temp.path().into()],
        ..LocalVolumeRoots::default()
    };
    let zcode = crate::resolve_root_discovery_agent_scope(
        RootDiscoveryScope::FullLocalVolumes,
        Some(&[SourceClientKind::Glm]),
    )
    .expect("ZCode scope");
    let coordinator = RootDiscoveryCoordinator::default();
    discover_metadata_roots_in_with_callback(&coordinator, volumes, Vec::new(), zcode, &|_| {});

    let candidates = coordinator.candidates();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].client, SourceClientKind::Glm);
    assert_eq!(candidates[0].absolute_path, desktop_root);
    assert_eq!(
        candidates[0].evidence,
        RootCandidateEvidence::GlmModelIoUpdates
    );
    assert!(
        candidate_from_file_name(
            &desktop_root.join("db.sqlite"),
            crate::resolve_root_discovery_agent_scope(
                RootDiscoveryScope::FullLocalVolumes,
                Some(&[SourceClientKind::Codex]),
            )
            .expect("Codex scope")
        )
        .is_none()
    );
}
