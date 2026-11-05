//! 新产品发现位置和统计共同保存在各 Agent 库，确认转换必须原子发布。
#![cfg(feature = "storage")]

use ai_usage_collect_scan::storage::LocalIndex;
use ai_usage_collect_scan::{
    LocalErrorKind, RootActivationState, RootCandidate, RootCandidateEvidence,
    RootDiscoveryStrategy, SourceClientKind, stable_id,
};
#[cfg(feature = "test-util")]
use sea_orm::ConnectionTrait;
use std::path::Path;

/// 构造不同 Agent 的规范候选身份，不读取原始会话文件。
fn candidate(base: &Path, client: SourceClientKind) -> RootCandidate {
    let absolute_path = base.join(client.root_id_namespace());
    RootCandidate {
        id: stable_id(
            client.candidate_namespace(),
            &ai_usage_collect_scan::path_key(&absolute_path),
        ),
        client,
        absolute_path,
        strategy: RootDiscoveryStrategy::MetadataTraversal,
        evidence: match client {
            SourceClientKind::Codex => RootCandidateEvidence::CodexRollout,
            SourceClientKind::ClaudeCode => RootCandidateEvidence::ClaudeTranscript,
            SourceClientKind::GrokBuildCli => RootCandidateEvidence::GrokSessionUpdates,
            _ => unreachable!("physical fixtures only"),
        },
    }
}

/// 清空候选只删除未确认候选，不影响已登记根，且重复清空返回零。
#[tokio::test]
async fn clearing_candidates_keeps_confirmed_roots() {
    let base = tempfile::tempdir().expect("database base");
    let client = SourceClientKind::Codex;
    let mut index = LocalIndex::open(base.path(), Path::new("codex.db"), client)
        .await
        .expect("new agent database");
    let confirmed = candidate(base.path(), client);
    index
        .save_discovery_candidate(&confirmed)
        .await
        .expect("candidate saved");
    index
        .confirm_discovery_candidate(&confirmed, "safe alias")
        .await
        .expect("confirmation commits");
    let pending_path = base.path().join("pending-root");
    let pending = RootCandidate {
        id: stable_id(
            client.candidate_namespace(),
            &ai_usage_collect_scan::path_key(&pending_path),
        ),
        absolute_path: pending_path,
        ..confirmed.clone()
    };
    assert!(
        index
            .save_discovery_candidate(&pending)
            .await
            .expect("pending candidate saved"),
        "pending candidate must be newly stored"
    );

    assert_eq!(
        index.clear_discovery_candidates().await.expect("cleared"),
        1
    );
    assert_eq!(
        index
            .clear_discovery_candidates()
            .await
            .expect("idempotent"),
        0
    );
    drop(index);
    let reopened = LocalIndex::open(base.path(), Path::new("codex.db"), client)
        .await
        .expect("reopen");
    assert!(
        reopened
            .discovery_candidates()
            .await
            .expect("none left")
            .is_empty()
    );
    assert_eq!(reopened.all_roots().await.expect("roots kept").len(), 1);
}

/// 候选位置跨连接存在，分库隔离、去重且确认前不激活任何来源。
#[tokio::test]
async fn discovery_positions_survive_reopen_and_confirmation_is_atomic() {
    let base = tempfile::tempdir().expect("database base");
    for (client, name) in [
        (SourceClientKind::Codex, "codex.db"),
        (SourceClientKind::ClaudeCode, "claude.db"),
        (SourceClientKind::GrokBuildCli, "grok.db"),
    ] {
        let found = candidate(base.path(), client);
        let mut index = LocalIndex::open(base.path(), Path::new(name), client)
            .await
            .expect("new agent database");
        assert!(
            index
                .save_discovery_candidate(&found)
                .await
                .expect("candidate saved")
        );
        assert!(
            index
                .save_discovery_candidate(&found)
                .await
                .expect("candidate deduplicates")
        );
        assert!(index.known_roots().await.expect("no roots yet").is_empty());
        drop(index);
        let mut reopened = LocalIndex::open(base.path(), Path::new(name), client)
            .await
            .expect("reopen");
        assert_eq!(
            reopened
                .discovery_candidates()
                .await
                .expect("restored locations"),
            vec![found.clone()]
        );
        let confirmed = reopened
            .confirm_discovery_candidate(&found, "safe alias")
            .await
            .expect("confirmation commits");
        assert!(confirmed.added);
        assert_eq!(
            confirmed.activation_state,
            RootActivationState::ConfirmedUnindexed
        );
        assert!(
            reopened
                .discovery_candidates()
                .await
                .expect("candidate removed")
                .is_empty()
        );
        assert!(
            reopened
                .known_roots()
                .await
                .expect("confirmed roots do not scan before activation")
                .is_empty()
        );
        let roots = reopened.all_roots().await.expect("root location persists");
        assert_eq!(
            roots[0].root_id.as_deref(),
            Some(confirmed.root_id.as_str())
        );
        assert_eq!(roots[0].path, Path::new(&found.absolute_path));
        assert!(
            !reopened
                .save_discovery_candidate(&found)
                .await
                .expect("registered root is not reintroduced")
        );
        assert!(
            reopened
                .discovery_candidates()
                .await
                .expect("still no pending candidate")
                .is_empty()
        );
    }
}

/// 不同 Agent、伪造身份或上跳路径不能写入候选表。
#[tokio::test]
async fn rejects_foreign_or_invalid_positions_without_partial_rows() {
    let base = tempfile::tempdir().expect("database base");
    let mut index = LocalIndex::open(base.path(), Path::new("codex.db"), SourceClientKind::Codex)
        .await
        .expect("database");
    let mut found = candidate(base.path(), SourceClientKind::GrokBuildCli);
    assert_eq!(
        index
            .save_discovery_candidate(&found)
            .await
            .expect_err("foreign agent")
            .kind(),
        LocalErrorKind::InvalidUsage
    );
    found = candidate(base.path(), SourceClientKind::Codex);
    found.id.push('x');
    assert!(index.save_discovery_candidate(&found).await.is_err());
    found.absolute_path = base.path().join("../escape");
    found.id = stable_id(
        SourceClientKind::Codex.candidate_namespace(),
        &ai_usage_collect_scan::path_key(&found.absolute_path),
    );
    assert!(index.save_discovery_candidate(&found).await.is_err());
    assert!(
        index
            .discovery_candidates()
            .await
            .expect("no partial rows")
            .is_empty()
    );
}

/// 确认失败或陈旧选择保留候选和未登记状态，下一次可以安全重试。
#[cfg(feature = "test-util")]
#[tokio::test]
async fn failed_or_changed_confirmation_preserves_the_saved_candidate() {
    let base = tempfile::tempdir().expect("database base");
    let mut index = LocalIndex::open(base.path(), Path::new("codex.db"), SourceClientKind::Codex)
        .await
        .expect("database");
    let found = candidate(base.path(), SourceClientKind::Codex);
    index
        .save_discovery_candidate(&found)
        .await
        .expect("saved position");
    let mut changed = found.clone();
    changed.strategy = RootDiscoveryStrategy::MacOsSpotlight;
    assert!(
        index
            .confirm_discovery_candidate(&changed, "safe alias")
            .await
            .is_err()
    );
    index.test_connection().execute_unprepared("CREATE TRIGGER fail_candidate_confirmation BEFORE DELETE ON discovery_candidates BEGIN SELECT RAISE(ABORT, 'injected confirmation failure'); END").await.expect("inject failure");
    assert!(
        index
            .confirm_discovery_candidate(&found, "safe alias")
            .await
            .is_err()
    );
    assert!(
        index
            .known_roots()
            .await
            .expect("registration rolled back")
            .is_empty()
    );
    assert_eq!(
        index.discovery_candidates().await.expect("position kept"),
        vec![found.clone()]
    );
    index
        .test_connection()
        .execute_unprepared("DROP TRIGGER fail_candidate_confirmation")
        .await
        .expect("remove failure");
    assert!(
        index
            .confirm_discovery_candidate(&found, "safe alias")
            .await
            .expect("retry")
            .added
    );
}

/// 同 Agent 并发首次打开都成功，异 Agent 竞争不能接管已确定的身份。
#[tokio::test]
async fn concurrent_initialization_keeps_one_format_and_one_agent() {
    let base = tempfile::tempdir().expect("database base");
    let open = || LocalIndex::open(base.path(), Path::new("codex.db"), SourceClientKind::Codex);
    let (left, right) = tokio::join!(open(), open());
    assert!(
        left.is_ok() && right.is_ok(),
        "both current Agent connections open"
    );
    let (codex, grok) = tokio::join!(
        LocalIndex::open(
            base.path(),
            Path::new("competing.db"),
            SourceClientKind::Codex
        ),
        LocalIndex::open(
            base.path(),
            Path::new("competing.db"),
            SourceClientKind::GrokBuildCli
        )
    );
    assert_ne!(
        codex.is_ok(),
        grok.is_ok(),
        "only one Agent can own the same physical file"
    );
    let error = if let Err(error) = codex {
        error
    } else {
        grok.expect_err("Grok must lose the binding race")
    };
    assert_eq!(error.kind(), LocalErrorKind::InvalidUsage);
}

/// 扫描持有 writer 时新连接仍可打开当前格式并读取上次发布的位置。
#[tokio::test]
async fn current_database_can_open_while_scan_transaction_is_active() {
    let base = tempfile::tempdir().expect("database base");
    let mut writer = LocalIndex::open(base.path(), Path::new("codex.db"), SourceClientKind::Codex)
        .await
        .expect("writer opens");
    let found = candidate(base.path(), SourceClientKind::Codex);
    writer
        .save_discovery_candidate(&found)
        .await
        .expect("committed candidate");
    let scan_id = writer
        .begin_scan("incremental", 1_000)
        .await
        .expect("scan transaction starts");
    let reader = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        LocalIndex::open(base.path(), Path::new("codex.db"), SourceClientKind::Codex),
    )
    .await
    .expect("read-only format check does not wait for writer")
    .expect("current reader opens");
    assert_eq!(
        reader
            .discovery_candidates()
            .await
            .expect("published positions"),
        vec![found]
    );
    writer
        .finish_scan(&scan_id, 1_001, "cancelled", true, 0, 0, 0)
        .await
        .expect("scan rolls back");
}

/// Unix 原生非 UTF-8 路径跨落盘恢复保持字节与稳定身份，不经过有损展示文本。
#[cfg(unix)]
#[tokio::test]
async fn discovery_positions_preserve_native_non_utf8_paths() {
    use std::os::unix::ffi::OsStringExt;
    let base = tempfile::tempdir().expect("database base");
    let mut found = candidate(base.path(), SourceClientKind::Codex);
    found.absolute_path = base
        .path()
        .join(std::ffi::OsString::from_vec(b"private-\xff-root".to_vec()));
    found.id = stable_id(
        found.client.candidate_namespace(),
        &ai_usage_collect_scan::path_key(&found.absolute_path),
    );
    let mut index = LocalIndex::open(base.path(), Path::new("codex.db"), found.client)
        .await
        .expect("current database");
    index
        .save_discovery_candidate(&found)
        .await
        .expect("native path saved");
    drop(index);
    let index = LocalIndex::open(base.path(), Path::new("codex.db"), found.client)
        .await
        .expect("reopen");
    assert_eq!(
        index
            .discovery_candidates()
            .await
            .expect("native path restored"),
        vec![found.clone()]
    );
    assert!(!format!("{found:?}").contains("private-"));
}

/// 原生手动登记同一位置时也在事务内移除候选，重复登记不复活待确认项。
#[tokio::test]
async fn direct_manual_registration_atomically_consumes_saved_position() {
    let base = tempfile::tempdir().expect("database base");
    let found = candidate(base.path(), SourceClientKind::Codex);
    let mut index = LocalIndex::open(base.path(), Path::new("codex.db"), found.client)
        .await
        .expect("database");
    index
        .save_discovery_candidate(&found)
        .await
        .expect("saved candidate");
    let root_id = stable_id(
        found.client.root_id_namespace(),
        &ai_usage_collect_scan::path_key(&found.absolute_path),
    );
    assert!(
        index
            .register_confirmed_root_fields(
                &root_id,
                &found.absolute_path,
                "manual root",
                ai_usage_collect_scan::DiscoveryMethod::Registered
            )
            .await
            .expect("manual root registered")
    );
    assert!(
        index
            .discovery_candidates()
            .await
            .expect("pending position consumed")
            .is_empty()
    );
    assert_eq!(index.all_roots().await.expect("confirmed root").len(), 1);
}

/// 本应用早先版本（版本号更旧）创建的统计库被删除后按当前格式重建，旧数据不保留；
/// 重建后的库带有按根查询索引。
#[tokio::test]
async fn older_own_database_is_removed_and_rebuilt() {
    let base = tempfile::tempdir().expect("database base");
    let client = SourceClientKind::Codex;
    let mut index = LocalIndex::open(base.path(), Path::new("codex.db"), client)
        .await
        .expect("new agent database");
    index
        .save_discovery_candidate(&candidate(base.path(), client))
        .await
        .expect("old data exists");
    index
        .test_connection()
        .execute_unprepared("DROP INDEX source_files_root_idx; PRAGMA user_version = 1;")
        .await
        .expect("simulate a database created by the previous format version");
    drop(index);

    let rebuilt = LocalIndex::open(base.path(), Path::new("codex.db"), client)
        .await
        .expect("older own database is rebuilt instead of rejected");
    let backups = std::fs::read_dir(base.path())
        .expect("database directory lists")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("codex.db.v1-") && name.ends_with(".zip"))
        .collect::<Vec<_>>();
    assert_eq!(backups.len(), 1, "old database is zipped beside itself");
    let archive_file =
        std::fs::File::open(base.path().join(&backups[0])).expect("backup archive opens");
    let mut archive = zip::ZipArchive::new(archive_file).expect("backup is a valid zip");
    let entry = archive
        .by_name("codex.db")
        .expect("main database is archived");
    assert!(entry.size() > 0, "archived database is not empty");
    assert!(
        rebuilt
            .discovery_candidates()
            .await
            .expect("candidates readable")
            .is_empty(),
        "old data is intentionally not preserved"
    );
    let has_index = rebuilt
        .test_connection()
        .query_one_raw(sea_orm::Statement::from_string(
            sea_orm::DbBackend::Sqlite,
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'source_files_root_idx'",
        ))
        .await
        .expect("index lookup")
        .is_some();
    assert!(has_index);
}

/// 多个打开者同时发现同一个旧库时，只备份一次、全部成功，且没有谁删掉别人刚重建的新库。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_opens_rebuild_an_older_database_exactly_once() {
    let base = tempfile::tempdir().expect("database base");
    let client = SourceClientKind::Codex;
    let index = LocalIndex::open(base.path(), Path::new("codex.db"), client)
        .await
        .expect("new agent database");
    index
        .test_connection()
        .execute_unprepared("PRAGMA user_version = 1;")
        .await
        .expect("simulate the previous format version");
    drop(index);

    let opens = (0..6).map(|_| {
        let dir = base.path().to_path_buf();
        tokio::spawn(async move {
            LocalIndex::open(&dir, Path::new("codex.db"), client)
                .await
                .map(|_| ())
        })
    });
    for outcome in futures_join_all(opens).await {
        outcome
            .expect("open task joins")
            .expect("every concurrent open succeeds");
    }
    let backups = std::fs::read_dir(base.path())
        .expect("directory lists")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".zip"))
        .count();
    assert_eq!(backups, 1, "the older database is backed up exactly once");
    let reopened = LocalIndex::open(base.path(), Path::new("codex.db"), client)
        .await
        .expect("rebuilt database stays usable");
    assert!(
        reopened
            .discovery_candidates()
            .await
            .expect("readable")
            .is_empty()
    );
}

/// 依次等待一组已派生的任务；避免为测试引入额外依赖。
async fn futures_join_all<T>(
    handles: impl Iterator<Item = tokio::task::JoinHandle<T>>,
) -> Vec<Result<T, tokio::task::JoinError>> {
    let mut results = Vec::new();
    for handle in handles.collect::<Vec<_>>() {
        results.push(handle.await);
    }
    results
}

/// 别人的 SQLite 文件（应用标识不同）绝不会被当作旧库删除。
#[tokio::test]
async fn foreign_database_is_never_removed() {
    let base = tempfile::tempdir().expect("database base");
    let path = base.path().join("codex.db");
    let foreign = sea_orm::Database::connect(format!("sqlite://{}?mode=rwc", path.display()))
        .await
        .expect("foreign database");
    sea_orm::ConnectionTrait::execute_unprepared(
        &foreign,
        "PRAGMA application_id = 12345; PRAGMA user_version = 1; CREATE TABLE keep(v TEXT); INSERT INTO keep VALUES('mine');",
    )
    .await
    .expect("foreign fixture");
    foreign.close().await.expect("close foreign");
    let original = std::fs::read(&path).expect("foreign bytes");

    let error = LocalIndex::open(base.path(), Path::new("codex.db"), SourceClientKind::Codex)
        .await
        .expect_err("foreign database is rejected");
    assert_eq!(error.kind(), LocalErrorKind::UnsupportedSchema);
    assert_eq!(std::fs::read(&path).expect("still there"), original);
}
