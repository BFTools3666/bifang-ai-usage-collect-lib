//! GUI 优先发现显式使用 ZCode 桌面来源策略，不要求同安装已有 CLI 日志。

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
