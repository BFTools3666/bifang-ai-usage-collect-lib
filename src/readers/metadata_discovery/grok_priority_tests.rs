use super::*;
use tempfile::tempdir;

/// 在隔离主目录写入带签名的默认 `.grok`，不设 `GROK_HOME`。
fn write_signed_default_grok_home(home_dir: &Path) -> PathBuf {
    let grok_home = home_dir.join(".grok");
    let session = grok_home
        .join("sessions")
        .join("%2Ftmp%2Fapp")
        .join("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    fs::create_dir_all(&session).expect("session directory");
    fs::write(
        session.join("updates.jsonl"),
        crate::readers::SYNTHETIC_GROK_UPDATES_JSONL,
    )
    .expect("fixture written");
    grok_home
}

/// UserPriority 显式核对在未设 `GROK_HOME` 时仍必须发现默认 `~/.grok`。
#[test]
fn user_priority_finds_default_grok_home_without_grok_home_env() {
    let isolated_home = tempdir().unwrap();
    let grok_home = write_signed_default_grok_home(isolated_home.path());
    let agents = crate::resolve_root_discovery_agent_scope(
        RootDiscoveryScope::UserPriority,
        Some(&[SourceClientKind::GrokBuildCli]),
    )
    .unwrap();
    let coordinator = RootDiscoveryCoordinator::default();
    assert!(coordinator.start(
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryPlatform::Other,
        RootDiscoveryScope::UserPriority,
        0,
    ));
    let inspection = explicit_environment_candidates(
        &coordinator,
        None,
        None,
        None,
        Some(isolated_home.path().to_path_buf()),
        agents,
    );
    assert_eq!(inspection.candidates.len(), 1);
    assert_eq!(
        inspection.candidates[0].1,
        RootCandidateEvidence::GrokSessionUpdates
    );
    assert_ne!(
        inspection.candidates[0].1,
        RootCandidateEvidence::CodexRollout
    );
    assert_ne!(
        inspection.candidates[0].1,
        RootCandidateEvidence::ClaudeTranscript
    );
    assert_eq!(
        inspection.candidates[0].0,
        fs::canonicalize(&grok_home).unwrap()
    );
    submit_candidate(
        &coordinator,
        inspection.candidates[0].0.clone(),
        inspection.candidates[0].1,
        RootDiscoveryStrategy::MetadataTraversal,
        agents,
        &|_| {},
    );
    let submitted = coordinator.candidates();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].client, SourceClientKind::GrokBuildCli);
    eprintln!(
        "gating default grok client={:?} evidence={:?}",
        submitted[0].client, inspection.candidates[0].1
    );
}

/// 默认 `.grok` 缺失、空 sessions、或只有未完成轮次时不得伪造 Grok 根。
#[test]
fn user_priority_does_not_invent_grok_root_without_signed_updates() {
    let missing = tempdir().unwrap();
    let unsigned = tempdir().unwrap();
    fs::create_dir_all(unsigned.path().join(".grok/sessions")).unwrap();
    let started_only = tempdir().unwrap();
    let started_session = started_only
        .path()
        .join(".grok/sessions/%2Ftmp%2Fapp/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    fs::create_dir_all(&started_session).unwrap();
    fs::write(
        started_session.join("updates.jsonl"),
        r#"{"timestamp":1786891791,"method":"session/update","params":{"sessionId":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","update":{"sessionUpdate":"turn_started"}}}"#,
    )
    .unwrap();
    let coordinator = RootDiscoveryCoordinator::default();
    for home in [missing.path(), unsigned.path(), started_only.path()] {
        let inspection = explicit_environment_candidates(
            &coordinator,
            None,
            None,
            None,
            Some(home.to_path_buf()),
            RootDiscoveryAgentScope::all_physical(),
        );
        assert!(
            inspection
                .candidates
                .iter()
                .all(|(_, evidence)| *evidence != RootCandidateEvidence::GrokSessionUpdates),
            "unsigned or missing .grok must not yield a Grok root"
        );
        eprintln!(
            "gating unsigned home candidates={} grok_hits=0",
            inspection.candidates.len()
        );
    }
}

/// 显式非空 `GROK_HOME` 指向另一合法夹具时仍发现该目录。
#[test]
fn user_priority_still_finds_explicit_grok_home() {
    let isolated_home = tempdir().unwrap();
    let grok_home = tempdir().unwrap();
    let session = grok_home
        .path()
        .join("sessions")
        .join("%2Ftmp%2Fapp")
        .join("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    fs::create_dir_all(&session).unwrap();
    fs::write(
        session.join("updates.jsonl"),
        crate::readers::SYNTHETIC_GROK_UPDATES_JSONL,
    )
    .unwrap();
    let coordinator = RootDiscoveryCoordinator::default();
    let inspection = explicit_environment_candidates(
        &coordinator,
        None,
        None,
        Some(grok_home.path().to_path_buf()),
        Some(isolated_home.path().to_path_buf()),
        RootDiscoveryAgentScope::all_physical(),
    );
    assert_eq!(inspection.candidates.len(), 1);
    assert_eq!(
        inspection.candidates[0].1,
        RootCandidateEvidence::GrokSessionUpdates
    );
    assert_eq!(
        inspection.candidates[0].0,
        fs::canonicalize(grok_home.path()).unwrap()
    );
    eprintln!(
        "gating explicit GROK_HOME evidence={:?} path_is_isolated_home={}",
        inspection.candidates[0].1,
        inspection.candidates[0].0.starts_with(isolated_home.path())
    );
}

/// 生产 `params.update` 信封在未设 `GROK_HOME` 时仍必须被默认 `.grok` 识别为 Grok。
#[test]
fn user_priority_finds_default_grok_home_from_production_envelope() {
    let isolated_home = tempdir().unwrap();
    let grok_home = isolated_home.path().join(".grok");
    let session = grok_home
        .join("sessions")
        .join("%2Ftmp%2Fapp")
        .join("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    fs::create_dir_all(&session).unwrap();
    fs::write(
        session.join("updates.jsonl"),
        crate::readers::PRODUCTION_GROK_SESSION_ENVELOPE_JSONL,
    )
    .unwrap();
    let coordinator = RootDiscoveryCoordinator::default();
    let inspection = explicit_environment_candidates(
        &coordinator,
        None,
        None,
        None,
        Some(isolated_home.path().to_path_buf()),
        RootDiscoveryAgentScope::all_physical(),
    );
    assert_eq!(inspection.candidates.len(), 1);
    assert_eq!(
        inspection.candidates[0].1,
        RootCandidateEvidence::GrokSessionUpdates
    );
    assert_eq!(
        inspection.candidates[0].0,
        fs::canonicalize(&grok_home).unwrap()
    );
    submit_candidate(
        &coordinator,
        inspection.candidates[0].0.clone(),
        inspection.candidates[0].1,
        RootDiscoveryStrategy::MetadataTraversal,
        RootDiscoveryAgentScope::all_physical(),
        &|_| {},
    );
    let submitted = coordinator.candidates();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].client, SourceClientKind::GrokBuildCli);
    eprintln!(
        "gating production envelope evidence={:?} client={:?}",
        inspection.candidates[0].1, submitted[0].client
    );
}
