//! Antigravity 的默认位置、全盘布局发现与 Agent 闭集保持一致。

use super::*;

/// 初始化的 Antigravity 单选发现只提交对应本地会话根。
#[test]
fn user_priority_finds_antigravity_and_respects_agent_filter() {
    let temp = tempfile::tempdir().expect("isolated home");
    let home = fs::canonicalize(temp.path()).expect("canonical home");
    let root = home.join(".gemini/antigravity/conversations");
    fs::create_dir_all(&root).expect("conversation root");
    fs::write(root.join("00000000-0000-0000-0000-000000000001.db"), b"SQLite format 3\0").expect("signature");
    let agents = crate::RootDiscoveryAgentScope::for_agent(SourceClientKind::Antigravity).expect("physical Agent");
    let coordinator = RootDiscoveryCoordinator::default();
    assert!(coordinator.start(RootDiscoveryStrategy::MetadataTraversal, RootDiscoveryPlatform::Other, RootDiscoveryScope::UserPriority, 0));
    let inspection = explicit_environment_candidates(&coordinator, None, None, None, Some(home.clone()), agents);
    assert_eq!(inspection.candidates, [(root.clone(), RootCandidateEvidence::AntigravityConversationDb)]);
    submit_candidate(&coordinator, root, RootCandidateEvidence::AntigravityConversationDb,
        RootDiscoveryStrategy::MetadataTraversal, agents, &|_| {});
    assert_eq!(coordinator.candidates()[0].client, SourceClientKind::Antigravity);
    let codex = crate::RootDiscoveryAgentScope::for_agent(SourceClientKind::Codex).expect("Codex scope");
    let inspection = explicit_environment_candidates(&coordinator, None, None, None, Some(home), codex);
    assert!(inspection.candidates.is_empty());
}

/// 仅固定布局的 UUID 数据库产生候选，不把任意 SQLite 或旧 pb 误识别为已支持来源。
#[test]
fn antigravity_file_candidates_require_fixed_layout_and_selected_scope() {
    let agents = crate::RootDiscoveryAgentScope::for_agent(SourceClientKind::Antigravity).expect("physical Agent");
    let root = PathBuf::from("/user/.gemini/antigravity/conversations");
    assert_eq!(candidate_from_file_name(&root.join("00000000-0000-0000-0000-000000000001.db"), agents),
        Some((root.clone(), RootCandidateEvidence::AntigravityConversationDb)));
    for name in ["conversation_summaries.db", "00000000-0000-0000-0000-000000000001.pb"] {
        assert!(candidate_from_file_name(&root.join(name), agents).is_none());
    }
    assert!(candidate_from_file_name(&PathBuf::from("/user/other/conversations/00000000-0000-0000-0000-000000000001.db"), agents).is_none());
    let codex = crate::RootDiscoveryAgentScope::for_agent(SourceClientKind::Codex).expect("Codex scope");
    assert!(candidate_from_file_name(&root.join("00000000-0000-0000-0000-000000000001.db"), codex).is_none());
}
