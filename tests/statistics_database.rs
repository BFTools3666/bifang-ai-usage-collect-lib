//! 外部调用者通过相对路径保存各 Agent 的统计，验证隔离、回滚及来源只读。
#![cfg(feature = "storage")]

use ai_usage_collect_scan::input::{JsonlFormat, SourceFileRequest, UsageReadOptions};
use ai_usage_collect_scan::storage::{LocalIndex, PreparedFileIndex};
use ai_usage_collect_scan::{
    LocalErrorKind, SourceClientKind, aggregate_canonical_usage, path_key, stable_id,
};
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

/// 建立单个合规来源文件并返回 Agent 与预期总量。
fn fixture(format: JsonlFormat) -> (tempfile::TempDir, SourceFileRequest, SourceClientKind, u64) {
    let temp = tempfile::tempdir().expect("source directory");
    let root = std::fs::canonicalize(temp.path()).expect("canonical source root");
    let (relative, content, client, total) = match format {
        JsonlFormat::Codex => (
            "sessions/2026/rollout-fixture.jsonl",
            concat!(
                "{\"timestamp\":\"2026-08-15T12:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"session-fixture\"}}\n",
                "{\"timestamp\":\"2026-08-15T12:01:00Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"token_count\",\"call_id\":\"call-fixture\",\"info\":{\"last_token_usage\":{\"input_tokens\":10,\"cached_input_tokens\":2,\"output_tokens\":5,\"reasoning_output_tokens\":0,\"total_tokens\":15}}}}\n"
            ),
            SourceClientKind::Codex,
            15,
        ),
        JsonlFormat::Claude => (
            "projects/-Users-private-project/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jsonl",
            "{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-08-15T12:00:00Z\",\"message\":{\"id\":\"msg-a\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":2,\"cache_creation_input_tokens\":3,\"output_tokens\":5}}}\n",
            SourceClientKind::ClaudeCode,
            20,
        ),
        JsonlFormat::Grok => (
            "sessions/%2FUsers%2Fprivate-project/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/updates.jsonl",
            include_str!("fixtures/synthetic_grok_updates_jsonl.jsonl"),
            SourceClientKind::GrokBuildCli,
            260,
        ),
        _ => unreachable!("supported fixtures only"),
    };
    let file_path = root.join(relative);
    std::fs::create_dir_all(file_path.parent().expect("file parent")).expect("source folders");
    std::fs::write(&file_path, content).expect("source bytes");
    let root_id = stable_id(client.root_id_namespace(), &path_key(&root));
    (
        temp,
        SourceFileRequest {
            root_path: root,
            file_path,
            root_id,
        },
        client,
        total,
    )
}

/// 三种 Agent 可按调用方名称分库，重复导入与重新打开均保留精确统计且不改源文件。
#[tokio::test]
async fn persists_each_agent_at_explicit_relative_path_without_duplicate_totals() {
    let databases = tempfile::tempdir().expect("database base");
    for (format, name) in [
        (JsonlFormat::Codex, "codex.db"),
        (JsonlFormat::Claude, "claude.db"),
        (JsonlFormat::Grok, "nested/grok.db"),
    ] {
        let (_source, request, client, total) = fixture(format);
        let original = std::fs::read(&request.file_path).expect("original source");
        let mut index = LocalIndex::open(databases.path(), Path::new(name), client)
            .await
            .expect("agent opens");
        for _ in 0..2 {
            let prepared = PreparedFileIndex::read(
                format,
                request.clone(),
                UsageReadOptions::default(),
                &|| false,
                &mut |_| Ok(()),
            )
            .expect("source parsed");
            assert!(
                index
                    .commit_file(prepared, &|| false)
                    .await
                    .expect("file committed")
            );
        }
        drop(index);
        let reopened = LocalIndex::open(databases.path(), Path::new(name), client)
            .await
            .expect("agent reopens");
        let canonical = reopened.canonical_calls().await.expect("stored facts");
        assert_eq!(
            aggregate_canonical_usage(&canonical)
                .expect("stored aggregate")
                .tokens
                .total_tokens,
            total,
            "{name}"
        );
        assert_eq!(
            std::fs::read(&request.file_path).expect("unchanged source"),
            original
        );
        let wrong = if client == SourceClientKind::Codex {
            SourceClientKind::GrokBuildCli
        } else {
            SourceClientKind::Codex
        };
        assert_eq!(
            LocalIndex::open(databases.path(), Path::new(name), wrong)
                .await
                .expect_err("wrong agent refused")
                .kind(),
            LocalErrorKind::InvalidUsage
        );
    }
}

/// 事务写入中取消或 SQL 失败均保留前次已发布统计。
#[tokio::test]
async fn cancellation_and_write_failure_preserve_last_published_usage() {
    let databases = tempfile::tempdir().expect("database base");
    let (_source, request, client, total) = fixture(JsonlFormat::Claude);
    let mut index = LocalIndex::open(databases.path(), Path::new("claude.db"), client)
        .await
        .expect("database");
    let prepare = || {
        PreparedFileIndex::read(
            JsonlFormat::Claude,
            request.clone(),
            UsageReadOptions::default(),
            &|| false,
            &mut |_| Ok(()),
        )
        .expect("source parsed")
    };
    assert!(
        index
            .commit_file(prepare(), &|| false)
            .await
            .expect("initial commit")
    );
    let checks = AtomicUsize::new(0);
    assert!(
        !index
            .commit_file(prepare(), &|| checks.fetch_add(1, Ordering::SeqCst) > 0)
            .await
            .expect("cancelled rollback")
    );
    assert_eq!(
        aggregate_canonical_usage(&index.canonical_calls().await.expect("old facts"))
            .expect("aggregate")
            .tokens
            .total_tokens,
        total
    );
    #[cfg(feature = "test-util")]
    {
        index.test_connection().execute_unprepared("CREATE TRIGGER fail_import BEFORE INSERT ON usage_calls BEGIN SELECT RAISE(ABORT, 'test failure'); END").await.expect("inject failure");
        assert!(index.commit_file(prepare(), &|| false).await.is_err());
        assert!(!index.test_scan_transaction_active());
        assert_eq!(
            aggregate_canonical_usage(&index.canonical_calls().await.expect("old facts"))
                .expect("aggregate")
                .tokens
                .total_tokens,
            total
        );
    }
}

/// 业务数据库不得被统计库接管，内容和日志模式保持不变。
#[tokio::test]
async fn rejects_gui_business_database_without_changing_its_contents() {
    let base = tempfile::tempdir().expect("database base");
    let path = base.path().join("gui.db");
    let db = Database::connect(format!("sqlite://{}?mode=rwc", path.display()))
        .await
        .expect("GUI business database");
    db.execute_unprepared(
        "CREATE TABLE settings (value TEXT); INSERT INTO settings VALUES ('keep-gui-settings')",
    )
    .await
    .expect("GUI settings");
    db.close().await.expect("close GUI database");
    let original = std::fs::read(&path).expect("business bytes");
    assert_eq!(
        LocalIndex::open(base.path(), Path::new("gui.db"), SourceClientKind::Codex)
            .await
            .expect_err("business database rejected")
            .kind(),
        LocalErrorKind::UnsupportedSchema
    );
    assert_eq!(std::fs::read(&path).expect("business preserved"), original);
    let db = Database::connect(format!("sqlite://{}", path.display()))
        .await
        .expect("GUI still opens");
    assert_eq!(
        db.query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT value FROM settings"
        ))
        .await
        .expect("settings readable")
        .expect("settings row")
        .try_get_by_index::<String>(0)
        .expect("setting text"),
        "keep-gui-settings"
    );
}

/// 相对路径拒绝逃逸和特殊语法，WorkBuddy 不会获得数据库。
#[tokio::test]
async fn rejects_invalid_database_paths_and_nonphysical_agent() {
    let base = tempfile::tempdir().expect("database base");
    for path in [
        "",
        "../outside.db",
        "/absolute.db",
        "C:\\outside.db",
        "folder/../../outside.db",
    ] {
        assert_eq!(
            LocalIndex::open(base.path(), Path::new(path), SourceClientKind::Codex)
                .await
                .expect_err("invalid relative path")
                .kind(),
            LocalErrorKind::InvalidPath,
            "{path}"
        );
    }
    assert_eq!(
        LocalIndex::open(
            base.path(),
            Path::new("workbuddy.db"),
            SourceClientKind::WorkBuddy
        )
        .await
        .expect_err("WorkBuddy stays read only")
        .kind(),
        LocalErrorKind::InvalidUsage
    );
    assert!(!base.path().join("workbuddy.db").exists());
    #[cfg(unix)]
    {
        let outside = tempfile::tempdir().expect("outside base");
        std::os::unix::fs::symlink(outside.path(), base.path().join("linked")).expect("link");
        assert_eq!(
            LocalIndex::open(
                base.path(),
                Path::new("linked/codex.db"),
                SourceClientKind::Codex
            )
            .await
            .expect_err("link rejected")
            .kind(),
            LocalErrorKind::InvalidPath
        );
        assert!(!outside.path().join("codex.db").exists());
        let target = outside.path().join("keep.bin");
        std::fs::write(&target, b"keep-sidecar-target").expect("outside target");
        std::os::unix::fs::symlink(&target, base.path().join("codex.db-wal"))
            .expect("sidecar link");
        assert_eq!(
            LocalIndex::open(base.path(), Path::new("codex.db"), SourceClientKind::Codex)
                .await
                .expect_err("sidecar link rejected")
                .kind(),
            LocalErrorKind::InvalidPath
        );
        assert_eq!(
            std::fs::read(&target).expect("target unchanged"),
            b"keep-sidecar-target"
        );
    }
}

/// 公开 Debug 不包含来源或数据库绝对路径，错 Agent 根不能写入已绑定库。
#[tokio::test]
async fn hides_private_paths_and_rejects_foreign_roots_before_writing() {
    let base = tempfile::tempdir().expect("database base");
    let (_source, request, client, _) = fixture(JsonlFormat::Claude);
    let prepared = PreparedFileIndex::read(
        JsonlFormat::Claude,
        request.clone(),
        UsageReadOptions::default(),
        &|| false,
        &mut |_| Ok(()),
    )
    .expect("prepared source");
    assert!(!format!("{prepared:?}").contains(request.root_path.to_str().expect("fixture UTF-8")));
    let mut index = LocalIndex::open(base.path(), Path::new("claude.db"), client)
        .await
        .expect("database");
    assert!(!format!("{index:?}").contains(base.path().to_str().expect("fixture UTF-8")));
    let foreign_root = stable_id(
        SourceClientKind::GrokBuildCli.root_id_namespace(),
        "foreign",
    );
    assert_eq!(
        index
            .register_root_fields(
                &foreign_root,
                &request.root_path,
                "foreign",
                ai_usage_collect_scan::DiscoveryMethod::Registered
            )
            .await
            .expect_err("wrong root refused")
            .kind(),
        LocalErrorKind::InvalidUsage
    );
    assert_eq!(
        index
            .register_confirmed_root_fields(
                &foreign_root,
                &request.root_path,
                "foreign",
                ai_usage_collect_scan::DiscoveryMethod::Registered
            )
            .await
            .expect_err("wrong confirmed root refused")
            .kind(),
        LocalErrorKind::InvalidUsage
    );
    assert!(
        index
            .known_roots()
            .await
            .expect("roots unchanged")
            .is_empty()
    );
}

/// 大文件按有限批次提交，非相邻 Claude 回退记录质量告警而不覆盖可信值。
#[tokio::test]
async fn imports_large_transcript_and_preserves_regression_quality() {
    let base = tempfile::tempdir().expect("database base");
    let (_source, request, client, _) = fixture(JsonlFormat::Claude);
    let mut content = String::new();
    for i in 0..33_000 {
        content.push_str(&format!("{{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-08-15T12:00:00Z\",\"message\":{{\"id\":\"msg-{i}\",\"model\":\"claude-test\",\"usage\":{{\"input_tokens\":10,\"cache_read_input_tokens\":2,\"cache_creation_input_tokens\":3,\"output_tokens\":5}}}}}}\n"));
    }
    content.push_str("{\"type\":\"assistant\",\"sessionId\":\"session-a\",\"timestamp\":\"2026-08-15T12:00:00Z\",\"message\":{\"id\":\"msg-0\",\"model\":\"claude-test\",\"usage\":{\"input_tokens\":5,\"cache_read_input_tokens\":1,\"cache_creation_input_tokens\":1,\"output_tokens\":1}}}\n");
    std::fs::write(&request.file_path, content).expect("large transcript");
    let prepared = PreparedFileIndex::read(
        JsonlFormat::Claude,
        request,
        UsageReadOptions::default(),
        &|| false,
        &mut |_| Ok(()),
    )
    .expect("large transcript parsed");
    let mut index = LocalIndex::open(base.path(), Path::new("claude.db"), client)
        .await
        .expect("database");
    assert!(
        index
            .commit_file(prepared, &|| false)
            .await
            .expect("large transcript committed")
    );
    let aggregate = aggregate_canonical_usage(&index.canonical_calls().await.expect("calls"))
        .expect("aggregate");
    assert_eq!(aggregate.call_count, 33_000);
    assert_eq!(aggregate.tokens.total_tokens, 660_000);
    let snapshot = index.usage_snapshot().await.expect("snapshot");
    assert_eq!(
        snapshot.roots[0].last_coverage,
        Some(ai_usage_collect_scan::CoverageState::Partial),
        "single file never claims the whole root was enumerated"
    );
    #[cfg(feature = "test-util")]
    {
        let row = index
            .test_connection()
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT warning_count FROM scan_runs ORDER BY row_id DESC LIMIT 1",
            ))
            .await
            .expect("scan quality")
            .expect("scan row");
        assert_eq!(row.try_get_by_index::<i64>(0).expect("warning count"), 1);
    }
}
