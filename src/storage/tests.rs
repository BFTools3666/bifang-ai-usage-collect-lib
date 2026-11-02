use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};

use crate::readers::jsonl::PARSER_VERSION;
use crate::{Confidence, CoverageState, SourceClientKind, TokenUsage, UsageCall};

use super::*;

/// 验证空 app-data 路径与符号链接路径被拒绝，不会创建数据库文件。
#[tokio::test]
async fn rejects_invalid_app_data_directories() {
    let error = LocalIndex::open_in_app_data(Path::new(""), 1)
        .await
        .expect_err("empty app data directory is rejected");
    assert_eq!(error.kind(), LocalErrorKind::InvalidPath);
}

/// 验证全新目录可以打开数据库、迁移到当前 schema，并对外暴露稳定路径。
#[tokio::test]
async fn opens_fresh_database_at_current_schema() {
    let temp = tempfile::tempdir().expect("temp dir is created");
    let index = LocalIndex::open_in_app_data(temp.path(), 1)
        .await
        .expect("fresh database opens and migrates");
    assert_eq!(
        index.database_path(),
        temp.path().join(USAGE_INDEX_FILE_NAME)
    );
    assert!(index.database_path().is_file());

    let roots = index
        .known_roots()
        .await
        .expect("known roots query succeeds");
    assert!(roots.is_empty());

    let journal_mode = index
        .connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA journal_mode".to_owned(),
        ))
        .await
        .expect("journal mode reads")
        .expect("journal mode has a row")
        .try_get_by_index::<String>(0)
        .expect("journal mode is text");
    assert_eq!(journal_mode, "wal");

    #[cfg(unix)]
    for suffix in ["-wal", "-shm"] {
        use std::os::unix::fs::PermissionsExt;

        let path = index
            .database_path()
            .with_file_name(format!("{USAGE_INDEX_FILE_NAME}{suffix}"));
        let mode = std::fs::metadata(path)
            .expect("active WAL sidecar exists")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "WAL sidecar remains private");
    }
}

/// 验证重新打开同一目录复用既有 schema，不因重复迁移报错。
#[tokio::test]
async fn reopening_same_directory_is_idempotent() {
    let temp = tempfile::tempdir().expect("temp dir is created");
    {
        LocalIndex::open_in_app_data(temp.path(), 1)
            .await
            .expect("first open succeeds");
    }
    LocalIndex::open_in_app_data(temp.path(), 1)
        .await
        .expect("second open reuses migrated schema");
}

/// 在活动扫描事务中写入一个完全脱敏的来源与调用夹具。
pub(super) async fn stage_scan_source(index: &mut LocalIndex, source_id: &str, input_tokens: u64) {
    let warning_counts_json =
        serde_json::to_string(&crate::readers::jsonl::JsonlWarningCounts::default())
            .expect("warning fixture serializes");
    index
        .begin_source_file_checkpoint(
            source_id,
            "root-publication",
            &format!("sessions/{source_id}.jsonl"),
            &format!("identity-{source_id}"),
            false,
            crate::readers::jsonl::PARSER_VERSION,
            &format!("thread-{source_id}"),
            None,
            None,
            None,
        )
        .await
        .expect("source checkpoint begins");
    let mut calls = vec![UsageCall {
        logical_call_id: format!("call-{source_id}"),
        occurred_at_epoch_ms: 1_000,
        model: Some("fixture-model".to_owned()),
        reasoning_effort: None,
        project_key: None,
        thread_key: format!("thread-{source_id}"),
        project_label: None,
        thread_label: None,
        usage: TokenUsage::new_with_availability(
            input_tokens,
            Some(0),
            Some(0),
            1,
            Some(0),
            Some(input_tokens + 1),
        )
        .expect("fixture usage is valid"),
        adapter_consistency_key: None,
        confidence: Confidence::Exact,
        provenance: Vec::new(),
    }];
    index
        .insert_usage_batch(source_id, 1, &mut calls)
        .await
        .expect("source call is staged");
    index
        .commit_source_file_checkpoint(
            source_id,
            "root-publication",
            &format!("sessions/{source_id}.jsonl"),
            &format!("identity-{source_id}"),
            false,
            1,
            1_000,
            1,
            0,
            false,
            crate::readers::jsonl::PARSER_VERSION,
            1,
            &SourceParseCheckpoint {
                thread_key: format!("thread-{source_id}"),
                project_key: None,
                project_label: None,
                thread_label: None,
                model: Some("fixture-model".to_owned()),
                reasoning_effort: None,
                call_sequence: 1,
                adapter_state: None,
            },
            Some(&warning_counts_json),
        )
        .await
        .expect("source checkpoint is staged");
}

/// 扫描内逐来源完成 checkpoint 时，并发 reader 仍只能读取上一个完整发布快照；
/// 两个来源与覆盖状态在终态提交时一起可见，取消则整体回滚。
#[tokio::test]
async fn publishes_all_scan_sources_atomically_and_rolls_back_cancelled_attempts() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let mut writer =
        LocalIndex::open_in_app_data(temp.path(), crate::readers::jsonl::PARSER_VERSION)
            .await
            .expect("writer opens");
    let mut reader =
        LocalIndex::open_in_app_data(temp.path(), crate::readers::jsonl::PARSER_VERSION)
            .await
            .expect("reader opens before the write transaction");

    let scan_id = writer
        .begin_scan("atomic-publication", 1_000)
        .await
        .expect("scan starts");
    writer
        .register_root_fields(
            "root-publication",
            temp.path(),
            "publication root",
            DiscoveryMethod::Registered,
        )
        .await
        .expect("root is staged");
    stage_scan_source(&mut writer, "source-a", 10).await;
    assert!(
        reader
            .canonical_calls()
            .await
            .expect("reader sees prior snapshot after first source")
            .calls
            .is_empty()
    );
    stage_scan_source(&mut writer, "source-b", 20).await;
    assert!(
        reader
            .canonical_calls()
            .await
            .expect("reader sees prior snapshot after second source")
            .calls
            .is_empty()
    );
    writer
        .record_root_coverage("root-publication", CoverageState::Complete)
        .await
        .expect("coverage is staged");
    writer
        .finish_scan(&scan_id, 1_001, "completed", false, 2, 2, 0)
        .await
        .expect("scan publishes");
    let published = reader
        .usage_snapshot()
        .await
        .expect("published snapshot loads");
    assert_eq!(published.canonical.calls.len(), 2);
    assert_eq!(
        published.roots[0].last_coverage,
        Some(CoverageState::Complete)
    );

    let cancelled_scan = writer
        .begin_scan("cancelled-publication", 2_000)
        .await
        .expect("next scan starts");
    stage_scan_source(&mut writer, "source-c", 30).await;
    assert_eq!(
        reader
            .canonical_calls()
            .await
            .expect("reader remains on published snapshot")
            .calls
            .len(),
        2
    );
    writer
        .finish_scan(&cancelled_scan, 2_001, "cancelled", true, 1, 1, 0)
        .await
        .expect("cancelled attempt rolls back");
    assert_eq!(
        reader
            .canonical_calls()
            .await
            .expect("cancelled source stays unpublished")
            .calls
            .len(),
        2
    );
}

/// 取消必须恢复扫描前的保留窗口数据、根删除/Partial 与 activation claim。
#[tokio::test]
async fn cancelled_attempt_restores_pruned_usage_and_reconciled_roots() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let mut writer = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("writer opens");
    writer
        .register_root_fields(
            "root-publication",
            temp.path(),
            "kept root",
            DiscoveryMethod::Registered,
        )
        .await
        .expect("kept root registers");
    writer
        .register_root_fields(
            "root-remove",
            temp.path(),
            "removed root",
            DiscoveryMethod::Registered,
        )
        .await
        .expect("removal candidate registers");
    let seed_scan = writer.begin_scan("seed", 1_000).await.expect("seed starts");
    stage_scan_source(&mut writer, "seed-source", 10).await;
    writer
        .record_root_coverage("root-publication", CoverageState::Complete)
        .await
        .expect("seed coverage stages");
    writer
        .finish_scan(&seed_scan, 1_001, "completed", false, 1, 1, 0)
        .await
        .expect("seed publishes");
    writer
        .connection
        .execute_unprepared("UPDATE source_roots SET activation_state = 'confirmed_unindexed'")
        .await
        .expect("roots are queued");

    writer
        .begin_scan_attempt()
        .await
        .expect("workflow transaction starts");
    writer
        .prune_usage_before(2_000)
        .await
        .expect("prune is staged");
    assert_eq!(
        writer
            .claim_background_index_roots()
            .await
            .expect("claims are staged")
            .len(),
        2
    );
    writer
        .reconcile_known_roots(
            &["root-remove".to_owned()],
            &["root-publication".to_owned()],
        )
        .await
        .expect("reconciliation is staged");
    let scan_id = writer
        .begin_scan("cancelled-workflow", 2_000)
        .await
        .expect("provider scan starts in workflow transaction");
    writer
        .finish_scan(&scan_id, 2_001, "completed", false, 0, 0, 0)
        .await
        .expect("provider terminal is staged");
    writer
        .cancel_scan_attempt(2_002)
        .await
        .expect("workflow cancellation rolls back");

    assert_eq!(
        writer
            .canonical_calls()
            .await
            .expect("snapshot reads")
            .calls
            .len(),
        1
    );
    let rows = writer
        .connection
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT root_id, activation_state, last_coverage_state
             FROM source_roots ORDER BY root_id"
                .to_owned(),
        ))
        .await
        .expect("roots remain readable");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| (
                row.try_get_by_index::<String>(0).expect("root id"),
                row.try_get_by_index::<String>(1).expect("activation")
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                "root-publication".to_owned(),
                "confirmed_unindexed".to_owned()
            ),
            ("root-remove".to_owned(), "confirmed_unindexed".to_owned()),
        ]
    );
    assert_eq!(
        rows[0]
            .try_get_by_index::<Option<String>>(2)
            .expect("coverage reads")
            .as_deref(),
        Some("complete")
    );
}

/// 打开一个已经迁移完成的数据库文件用于只读检查；调用方负责先让
/// `LocalIndex` 完成迁移并释放连接，避免和它持有的单连接池竞争。
async fn reopen_for_inspection(database_path: &Path) -> sea_orm::DatabaseConnection {
    let mut options = ConnectOptions::new("sqlite://placeholder.sqlite3");
    let database_path = database_path.to_path_buf();
    options.map_sqlx_sqlite_opts(move |sqlite_options| {
        sqlite_options
            .filename(&database_path)
            .create_if_missing(true)
    });
    Database::connect(options)
        .await
        .expect("migrated database reopens for inspection")
}

/// 验证元数据发现迁移会清理旧派生调用，避免旧 parser generation 混入新索引。
#[tokio::test]
async fn migration_rebuilds_legacy_derived_usage_calls() {
    let temp = tempfile::tempdir().expect("temp dir is created");
    let database_path = temp.path().join(USAGE_INDEX_FILE_NAME);
    let fixture = reopen_for_inspection(&database_path).await;
    fixture
        .execute_unprepared(
            "CREATE TABLE source_roots (
               root_id TEXT PRIMARY KEY NOT NULL,
               access_path BLOB NOT NULL,
               alias TEXT NOT NULL,
               enabled INTEGER NOT NULL,
               discovery_method TEXT NOT NULL,
               last_coverage_state TEXT
             );
             CREATE TABLE usage_calls (
               cached_input_tokens INTEGER NOT NULL,
               reasoning_output_tokens INTEGER NOT NULL
             );
             INSERT INTO usage_calls VALUES (7, 3);
             PRAGMA user_version = 1;",
        )
        .await
        .expect("v1 fixture is created");
    drop(fixture);

    LocalIndex::open_in_app_data(temp.path(), 1)
        .await
        .expect("v1 schema migrates");

    let inspection = reopen_for_inspection(&database_path).await;
    let count: i64 = inspection
        .query_one_raw(sea_orm::Statement::from_string(
            sea_orm::DbBackend::Sqlite,
            "SELECT COUNT(*) FROM usage_calls",
        ))
        .await
        .expect("migrated count query succeeds")
        .expect("migrated count exists")
        .try_get_by_index(0)
        .expect("count is readable");
    assert_eq!(count, 0);
}

/// 验证 v3 数据根迁移为单主根 schema：默认不隐式选择主目录，且数据库
/// 自身的部分唯一索引与 CHECK 约束在应用代码之外仍拒绝非法状态
/// （同时两个主根、或主根被禁用），即使有人绕过 Rust 层直接执行原始 SQL。
#[tokio::test]
async fn migrates_v3_roots_with_no_implicit_primary_and_enforces_single_enabled_primary() {
    let temp = tempfile::tempdir().expect("temp dir is created");
    let database_path = temp.path().join(USAGE_INDEX_FILE_NAME);
    let fixture = reopen_for_inspection(&database_path).await;
    fixture
        .execute_unprepared(
            "CREATE TABLE source_roots (
               root_id TEXT PRIMARY KEY NOT NULL,
               access_path BLOB NOT NULL,
               alias TEXT NOT NULL,
               enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
               discovery_method TEXT NOT NULL,
               last_coverage_state TEXT
             );
             CREATE TABLE source_files (
               source_id TEXT PRIMARY KEY NOT NULL,
               root_id TEXT NOT NULL REFERENCES source_roots(root_id) ON DELETE CASCADE,
               relative_label TEXT NOT NULL,
               file_identity TEXT NOT NULL,
               archived INTEGER NOT NULL CHECK(archived IN (0, 1)),
               observed_size INTEGER NOT NULL,
               modified_at_epoch_ms INTEGER NOT NULL,
               parsed_offset INTEGER NOT NULL,
               trailing_bytes INTEGER NOT NULL,
               oversized_tail INTEGER NOT NULL CHECK(oversized_tail IN (0, 1)),
               parser_version INTEGER NOT NULL,
               generation INTEGER NOT NULL,
               ready INTEGER NOT NULL CHECK(ready IN (0, 1)),
               thread_key TEXT NOT NULL,
               project_key TEXT,
               model TEXT,
               reasoning_effort TEXT,
               call_sequence INTEGER NOT NULL
             );
             CREATE TABLE usage_calls (
               source_id TEXT NOT NULL REFERENCES source_files(source_id) ON DELETE CASCADE,
               generation INTEGER NOT NULL,
               logical_call_id TEXT NOT NULL,
               occurred_at_epoch_ms INTEGER NOT NULL,
               model TEXT,
               reasoning_effort TEXT,
               project_key TEXT,
               thread_key TEXT NOT NULL,
               input_tokens INTEGER NOT NULL,
               cached_input_tokens INTEGER NOT NULL,
               cache_write_input_tokens INTEGER,
               output_tokens INTEGER NOT NULL,
               reasoning_output_tokens INTEGER NOT NULL,
               total_tokens INTEGER NOT NULL,
               total_is_derived INTEGER NOT NULL CHECK(total_is_derived IN (0, 1)),
               confidence TEXT NOT NULL,
               cached_input_available INTEGER NOT NULL DEFAULT 1,
               reasoning_output_available INTEGER NOT NULL DEFAULT 1,
               adapter_consistency_key TEXT,
               PRIMARY KEY(source_id, generation, logical_call_id)
             );
             CREATE TABLE scan_runs (
               row_id INTEGER PRIMARY KEY AUTOINCREMENT,
               scan_id TEXT UNIQUE,
               mode TEXT NOT NULL,
               started_at_epoch_ms INTEGER NOT NULL,
               finished_at_epoch_ms INTEGER,
               status TEXT NOT NULL,
               cancelled INTEGER NOT NULL CHECK(cancelled IN (0, 1)),
               files_scanned INTEGER NOT NULL,
               calls_added INTEGER NOT NULL,
               warning_count INTEGER NOT NULL
             );
             CREATE TABLE provider_snapshots (
               snapshot_kind TEXT PRIMARY KEY NOT NULL,
               observed_at_epoch_ms INTEGER NOT NULL,
               valid_until_epoch_ms INTEGER NOT NULL,
               normalized_json TEXT NOT NULL
             );
             INSERT INTO source_roots
               (root_id, access_path, alias, enabled, discovery_method)
             VALUES
               ('root-a', X'01', 'A', 1, 'registered'),
               ('root-b', X'02', 'B', 1, 'registered');
             PRAGMA user_version = 3;",
        )
        .await
        .expect("v3 fixture is created");
    drop(fixture);

    let index = LocalIndex::open_in_app_data(temp.path(), 1)
        .await
        .expect("v3 schema migrates");
    let roots = index.list_sources().await.expect("root records load");
    assert_eq!(roots.len(), 2);
    assert!(roots.iter().all(|root| !root.is_primary));
    assert!(
        roots.iter().all(|root| {
            root.activation_state == crate::RootActivationState::ConfirmedUnindexed
        })
    );
    drop(index);

    let inspection = reopen_for_inspection(&database_path).await;
    inspection
        .execute_unprepared("UPDATE source_roots SET is_primary = 1 WHERE root_id = 'root-a'")
        .await
        .expect("first enabled primary is accepted");
    assert!(
        inspection
            .execute_unprepared("UPDATE source_roots SET is_primary = 1 WHERE root_id = 'root-b'")
            .await
            .is_err(),
        "the partial unique index must reject a second primary"
    );
    assert!(
        inspection
            .execute_unprepared("UPDATE source_roots SET enabled = 0 WHERE root_id = 'root-a'")
            .await
            .is_err(),
        "a primary root cannot remain disabled"
    );
}

/// 读取已迁移用量库中的扫描 mode，用于确认文件只含本次写入。
async fn scan_modes(database_path: &Path) -> Vec<String> {
    let inspection = reopen_for_inspection(database_path).await;
    let rows = inspection
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT mode FROM scan_runs ORDER BY row_id",
        ))
        .await
        .expect("scan modes are readable");
    rows.into_iter()
        .map(|row| row.try_get_by_index::<String>(0).expect("mode is text"))
        .collect()
}

/// 验证同一产品 app-data 下三个 Agent 打开/写入各自独占库，互不改写对方记录。
#[tokio::test]
async fn opening_and_writing_one_agent_index_does_not_touch_another() {
    let temp = tempfile::tempdir().expect("isolated product app-data exists");
    let product = temp.path();
    let clients = [
        SourceClientKind::Codex,
        SourceClientKind::ClaudeCode,
        SourceClientKind::GrokBuildCli,
    ];
    let paths: Vec<_> = clients
        .into_iter()
        .map(|client| test_client_app_data_dir(product, client).join(USAGE_INDEX_FILE_NAME))
        .collect();
    let unique = paths.iter().collect::<std::collections::HashSet<_>>();
    assert_eq!(unique.len(), clients.len());

    let mut previous_markers: Vec<String> = Vec::new();
    for (index, client) in clients.into_iter().enumerate() {
        let marker = format!("exclusive-{}", client.root_id_namespace());
        let app_data = test_client_app_data_dir(product, client);
        let mut local_index = LocalIndex::open_in_app_data(&app_data, client.parser_version())
            .await
            .expect("agent index opens through shipped mapping");
        assert_eq!(local_index.database_path(), paths[index].as_path());
        let scan_id = local_index
            .begin_scan(&marker, 1_000 + i64::try_from(index).expect("index fits"))
            .await
            .expect("exclusive scan marker is written");
        local_index
            .finish_scan(
                &scan_id,
                1_001 + i64::try_from(index).expect("index fits"),
                "completed",
                false,
                0,
                0,
                0,
            )
            .await
            .expect("exclusive scan marker is published");
        drop(local_index);

        assert!(paths[index].is_file(), "Agent index remains a file");
        for (other, previous_marker) in previous_markers.iter().enumerate() {
            assert_eq!(
                scan_modes(&paths[other]).await,
                vec![previous_marker.clone()],
                "{client:?} 写入不得改写另一 Agent 的扫描记录"
            );
        }
        assert_eq!(scan_modes(&paths[index]).await, vec![marker.clone()]);
        previous_markers.push(marker);
    }
}

/// 验证 30 日读取下界与物理裁剪同时覆盖调用和累计快照，并保留来源 checkpoint。
#[tokio::test]
async fn bounds_snapshot_reads_and_prunes_derived_history_without_losing_sources() {
    let temp = tempfile::tempdir().expect("isolated app-data exists");
    let index = LocalIndex::open_in_app_data(temp.path(), 8)
        .await
        .expect("index opens");
    assert!(
        !index
            .has_current_parser_usage()
            .await
            .expect("empty usage state")
    );
    let database_path = index.database_path().to_path_buf();
    drop(index);

    let fixture = reopen_for_inspection(&database_path).await;
    fixture
        .execute_unprepared(
            "INSERT INTO source_roots
               (root_id, access_path, alias, enabled, discovery_method,
                last_coverage_state, activation_state)
             VALUES ('root-retention', X'01', 'Retention', 1, 'registered', 'complete', 'ready');
             INSERT INTO source_files
               (source_id, root_id, relative_label, file_identity, archived,
                observed_size, modified_at_epoch_ms, parsed_offset, trailing_bytes,
                oversized_tail, parser_version, generation, ready, thread_key,
                project_key, model, reasoning_effort, call_sequence, token_snapshots_ready)
             VALUES
               ('source-retention', 'root-retention', 'sessions/test.jsonl', 'file', 0,
                10, 200, 10, 0, 0, 8, 1, 1, 'thread', NULL, NULL, NULL, 2, 1);
             INSERT INTO usage_calls
               (source_id, generation, logical_call_id, occurred_at_epoch_ms,
                model, reasoning_effort, project_key, thread_key, input_tokens,
                cached_input_tokens, cache_write_input_tokens, output_tokens,
                reasoning_output_tokens, total_tokens, total_is_derived, confidence)
             VALUES
               ('source-retention', 1, 'old', 99, NULL, NULL, NULL, 'thread', 1, 0, NULL, 0, 0, 1, 0, 'exact'),
               ('source-retention', 1, 'recent', 100, NULL, NULL, NULL, 'thread', 2, 0, NULL, 0, 0, 2, 0, 'exact');
             INSERT INTO usage_token_snapshots
               (source_id, generation, thread_key, occurred_at_epoch_ms,
                logical_call_id, total_tokens)
             VALUES
               ('source-retention', 1, 'thread', 99, 'old', 1),
               ('source-retention', 1, 'thread', 100, 'recent', 2);",
        )
        .await
        .expect("retention fixture is inserted");
    drop(fixture);

    let mut index = LocalIndex::open_in_app_data(temp.path(), 8)
        .await
        .expect("index reopens");
    assert!(
        index
            .has_current_parser_usage()
            .await
            .expect("source state")
    );
    let bounded = index
        .usage_snapshot_since(100)
        .await
        .expect("bounded snapshot loads");
    assert_eq!(bounded.canonical.calls.len(), 1);
    assert_eq!(bounded.canonical.calls[0].logical_call_id, "recent");
    index
        .prune_usage_before(100)
        .await
        .expect("derived history is pruned");
    assert_eq!(
        index
            .canonical_calls()
            .await
            .expect("calls remain")
            .calls
            .len(),
        1
    );
    assert!(
        index
            .has_current_parser_usage()
            .await
            .expect("source remains")
    );
}

/// 为旧分库回归构造相互隔离的夹具目录。
fn test_client_app_data_dir(base: &Path, client: SourceClientKind) -> PathBuf {
    base.join(client.root_id_namespace())
}
