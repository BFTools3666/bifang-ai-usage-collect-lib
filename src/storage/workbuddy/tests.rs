//! WorkBuddy 独立库与增量读取的隔离回归。
use super::*;
use crate::workbuddy_scan_source_from_path;
use std::{fs, io::Write};
/// 测试取消上下文不访问应用生命周期。
#[derive(Clone, Copy)]
struct NeverCancelled;
impl CancellationSignal for NeverCancelled {
    /// 正常扫描始终允许继续。
    fn is_cancelled(&self) -> bool {
        false
    }
}

/// 合成含用量白名单与正文诱饵的完整行。
fn event(id: &str, time: i64, input: i64) -> String {
    format!(
        "{}\n",
        serde_json::json!({"timestamp":time,"sessionId":"private-session", "cwd":"/private/project", "body":"private-body-bait", "providerData":{"messageId":id,"model":"model-a","usage":{"inputTokens":input,"inputTokensDetails":[{"cached_tokens":2}],"outputTokens":3,"totalTokens":input+3,"requests":1},"rawUsage":{"credit":0.5},"message":{"text":"private-provider-bait"}}})
    )
}
/// 冷启动、重复读取零解析、追加半行补齐、重启与截断替换保持精确累计。
#[tokio::test]
async fn workbuddy_incremental_checkpoint_survives_reopen_and_handles_partial_lines() {
    let temp = tempfile::tempdir().expect("isolated source");
    let home = fs::canonicalize(temp.path())
        .expect("canonical source")
        .join(".workbuddy");
    let project = home.join("projects/project");
    fs::create_dir_all(&project).expect("source tree");
    let path = project.join("session.jsonl");
    fs::write(&path, event("first", 1000, 10)).expect("first event");
    let app = tempfile::tempdir().expect("isolated index");
    let sources = vec![workbuddy_scan_source_from_path(home)];
    let mut index = WorkbuddyIndex::open(app.path(), Path::new("workbuddy.db"))
        .await
        .expect("independent index");
    let first = index
        .refresh(
            &sources,
            0,
            WorkbuddyRefreshMode::Incremental,
            NeverCancelled,
        )
        .await
        .expect("first scan");
    assert!(first.bytes_read > 0);
    assert_eq!(
        index
            .load_inputs()
            .await
            .expect("snapshot")
            .expect("published")
            .records
            .len(),
        1
    );
    assert_eq!(
        index
            .refresh(
                &sources,
                0,
                WorkbuddyRefreshMode::Incremental,
                NeverCancelled
            )
            .await
            .expect("unchanged")
            .bytes_read,
        0
    );
    let second = event("second", 2000, 20);
    let split = second.len() / 2;
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("append source")
        .write_all(&second.as_bytes()[..split])
        .expect("half line");
    index
        .refresh(
            &sources,
            0,
            WorkbuddyRefreshMode::Incremental,
            NeverCancelled,
        )
        .await
        .expect("half line waits");
    assert_eq!(
        index
            .load_inputs()
            .await
            .expect("snapshot")
            .expect("published")
            .records
            .len(),
        1
    );
    drop(index);
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("append source")
        .write_all(&second.as_bytes()[split..])
        .expect("finish line");
    let mut index = WorkbuddyIndex::open(app.path(), Path::new("workbuddy.db"))
        .await
        .expect("reopen index");
    index
        .refresh(
            &sources,
            0,
            WorkbuddyRefreshMode::Incremental,
            NeverCancelled,
        )
        .await
        .expect("resume");
    let snapshot = index
        .load_inputs()
        .await
        .expect("snapshot")
        .expect("published");
    assert_eq!(snapshot.records.len(), 2);
    assert_eq!(
        snapshot.records.iter().map(|r| r.total_tokens).sum::<i64>(),
        36
    );
    fs::write(&path, event("replacement", 3000, 30)).expect("replace source");
    index
        .refresh(
            &sources,
            0,
            WorkbuddyRefreshMode::Incremental,
            NeverCancelled,
        )
        .await
        .expect("rebuild");
    assert_eq!(
        index
            .load_inputs()
            .await
            .expect("snapshot")
            .expect("published")
            .records
            .len(),
        1
    );
    let contents = fs::read(app.path().join("workbuddy.db")).expect("database bytes");
    assert!(!String::from_utf8_lossy(&contents).contains("private-body-bait"));
    assert!(!String::from_utf8_lossy(&contents).contains("private-session"));
}

/// 两个视图并发首次打开时只初始化一次，均能读取同一独立格式。
#[tokio::test]
async fn workbuddy_concurrent_first_open_and_foreign_database_are_safe() {
    let app = tempfile::tempdir().expect("isolated database root");
    let (left, right) = tokio::join!(
        WorkbuddyIndex::open(app.path(), Path::new("workbuddy.db")),
        WorkbuddyIndex::open(app.path(), Path::new("workbuddy.db"))
    );
    assert!(left.is_ok());
    assert!(right.is_ok());
    assert!(
        left.expect("left connection")
            .load_inputs()
            .await
            .expect("uninitialized snapshot")
            .is_none()
    );
    let foreign = app.path().join("foreign.db");
    let mut options = sea_orm::ConnectOptions::new("sqlite://foreign");
    let filename = foreign.clone();
    options
        .map_sqlx_sqlite_opts(move |options| options.filename(&filename).create_if_missing(true));
    let connection = sea_orm::Database::connect(options)
        .await
        .expect("foreign database");
    connection
        .execute_raw(statement(
            "CREATE TABLE business(value TEXT); INSERT INTO business VALUES('original data')",
            vec![],
        ))
        .await
        .expect("foreign schema");
    connection.close().await.expect("close fixture");
    let before = fs::read(&foreign).expect("foreign bytes");
    assert!(
        WorkbuddyIndex::open(app.path(), Path::new("foreign.db"))
            .await
            .is_err()
    );
    assert_eq!(fs::read(&foreign).expect("foreign preserved"), before);
}

/// 国内、国际、顶层与子代理共用一次发布，积分与模型保留且查询不访问源文件。
#[tokio::test]
async fn workbuddy_dual_root_snapshot_preserves_metadata_and_reads_without_sources() {
    let temp = tempfile::tempdir().expect("isolated source");
    let home = fs::canonicalize(temp.path()).expect("canonical source");
    let domestic = home.join(".workbuddy");
    let international = home.join(".workbuddy-ai");
    fs::create_dir_all(domestic.join("projects/project/session/subagents")).expect("domestic tree");
    fs::create_dir_all(international.join("projects/project")).expect("international tree");
    fs::write(
        domestic.join("projects/project/session.jsonl"),
        event("same", 1000, 10),
    )
    .expect("domestic usage");
    fs::write(
        domestic.join("projects/project/session/subagents/agent.jsonl"),
        event("child", 2000, 20).replace("model-a", "model-b"),
    )
    .expect("subagent usage");
    fs::write(
        international.join("projects/project/session.jsonl"),
        event("same", 1000, 30),
    )
    .expect("separate namespace");
    let app = tempfile::tempdir().expect("isolated index");
    let sources = vec![
        workbuddy_scan_source_from_path(domestic.clone()),
        workbuddy_scan_source_from_path(international.clone()),
    ];
    let mut index = WorkbuddyIndex::open(app.path(), Path::new("workbuddy.db"))
        .await
        .expect("index");
    index
        .refresh(
            &sources,
            0,
            WorkbuddyRefreshMode::Incremental,
            NeverCancelled,
        )
        .await
        .expect("publish both roots");
    let inputs = index
        .load_inputs()
        .await
        .expect("snapshot")
        .expect("published");
    assert_eq!(inputs.records.len(), 3);
    assert_eq!(inputs.root_aliases.len(), 2);
    assert_eq!(
        inputs.records.iter().map(|r| r.total_tokens).sum::<i64>(),
        69
    );
    assert_eq!(
        inputs
            .records
            .iter()
            .filter(|r| r.origin == crate::WorkbuddyUsageOrigin::Subagent)
            .count(),
        1
    );
    assert_eq!(
        inputs.records.iter().filter_map(|r| r.credit).sum::<f64>(),
        1.5
    );
    let rows = index
        .connection
        .query_all_raw(statement(
            "SELECT record_json FROM workbuddy_events",
            vec![],
        ))
        .await
        .expect("stored safe fields");
    for row in rows {
        let json: String = row.try_get_by_index(0).expect("metadata JSON");
        assert!(!json.contains("private-session"));
        assert!(!json.contains("private-body-bait"));
        assert!(!json.contains("/private/project"));
    }
    fs::remove_dir_all(domestic).expect("remove synthetic raw source");
    fs::remove_dir_all(international).expect("remove synthetic raw source");
    assert_eq!(
        index
            .load_inputs()
            .await
            .expect("database-only query")
            .expect("published")
            .records
            .len(),
        3
    );
    index.prune_before(1500).await.expect("retention cleanup");
    let remaining = index
        .load_inputs()
        .await
        .expect("pruned snapshot")
        .expect("published");
    assert_eq!(remaining.records.len(), 1);
}

/// 跨追加身份冲突及写入失败不得推进检查点或发布半份记录，后续修复可继续续读。
#[tokio::test]
async fn workbuddy_conflicts_and_write_failure_preserve_the_previous_publication() {
    let temp = tempfile::tempdir().expect("isolated source");
    let home = fs::canonicalize(temp.path())
        .expect("canonical source")
        .join(".workbuddy");
    fs::create_dir_all(home.join("projects/project")).expect("source tree");
    let path = home.join("projects/project/session.jsonl");
    let first = event("same", 1000, 10);
    fs::write(&path, &first).expect("first source");
    let app = tempfile::tempdir().expect("isolated index");
    let sources = vec![workbuddy_scan_source_from_path(home)];
    let mut index = WorkbuddyIndex::open(app.path(), Path::new("workbuddy.db"))
        .await
        .expect("index");
    index
        .refresh(
            &sources,
            0,
            WorkbuddyRefreshMode::Incremental,
            NeverCancelled,
        )
        .await
        .expect("initial publication");
    let checkpoint = index
        .checkpoints()
        .await
        .expect("initial checkpoint")
        .into_values()
        .next()
        .expect("source checkpoint")
        .parsed_offset;
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("append")
        .write_all(event("same", 1000, 99).as_bytes())
        .expect("conflict");
    assert!(
        index
            .refresh(
                &sources,
                0,
                WorkbuddyRefreshMode::Incremental,
                NeverCancelled
            )
            .await
            .is_err()
    );
    let stale = index
        .load_inputs()
        .await
        .expect("previous snapshot")
        .expect("published");
    assert_eq!(stale.coverage.state, CoverageState::Partial);
    assert_eq!(stale.records.len(), 1);
    assert_eq!(stale.records[0].total_tokens, 13);
    assert_eq!(
        index
            .checkpoints()
            .await
            .expect("checkpoint preserved")
            .into_values()
            .next()
            .expect("source")
            .parsed_offset,
        checkpoint
    );
    fs::write(&path, &first).expect("remove conflict");
    index
        .refresh(&sources, 0, WorkbuddyRefreshMode::Rebuild, NeverCancelled)
        .await
        .expect("explicit repair");
    index.connection.execute_raw(statement("CREATE TRIGGER fail_write BEFORE INSERT ON workbuddy_events BEGIN SELECT RAISE(ABORT,'synthetic failure'); END",vec![])).await.expect("write failure injection");
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("append")
        .write_all(event("new", 2000, 20).as_bytes())
        .expect("new request");
    assert!(
        index
            .refresh(
                &sources,
                0,
                WorkbuddyRefreshMode::Incremental,
                NeverCancelled
            )
            .await
            .is_err()
    );
    assert_eq!(
        index
            .load_inputs()
            .await
            .expect("old snapshot")
            .expect("published")
            .records
            .len(),
        1
    );
    index
        .connection
        .execute_raw(statement("DROP TRIGGER fail_write", vec![]))
        .await
        .expect("remove injection");
    index
        .refresh(
            &sources,
            0,
            WorkbuddyRefreshMode::Incremental,
            NeverCancelled,
        )
        .await
        .expect("retry uses old checkpoint");
    assert_eq!(
        index
            .load_inputs()
            .await
            .expect("recovered snapshot")
            .expect("published")
            .records
            .len(),
        2
    );
}

/// 已取消扫描不推进来源检查点；取消读取也不能解码整库或回退直接读取来源。
#[tokio::test]
async fn workbuddy_cancelled_refresh_keeps_the_previous_records_and_checkpoint() {
    let temp = tempfile::tempdir().expect("isolated source");
    let home = fs::canonicalize(temp.path())
        .expect("canonical source")
        .join(".workbuddy");
    fs::create_dir_all(home.join("projects/project")).expect("source tree");
    let path = home.join("projects/project/session.jsonl");
    fs::write(&path, event("first", 1000, 10)).expect("initial usage");
    let sources = vec![workbuddy_scan_source_from_path(home)];
    let app = tempfile::tempdir().expect("isolated database");
    let mut index = WorkbuddyIndex::open(app.path(), Path::new("workbuddy.db"))
        .await
        .expect("index");
    index
        .refresh(
            &sources,
            0,
            WorkbuddyRefreshMode::Incremental,
            NeverCancelled,
        )
        .await
        .expect("initial publication");
    let before = index
        .checkpoints()
        .await
        .expect("checkpoint")
        .into_values()
        .next()
        .expect("source")
        .parsed_offset;
    fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("append")
        .write_all(event("second", 2000, 20).as_bytes())
        .expect("new usage");
    let cancel = crate::readers::CancellationToken::default();
    cancel.cancel();
    assert!(
        index
            .refresh(
                &sources,
                0,
                WorkbuddyRefreshMode::Incremental,
                cancel.clone()
            )
            .await
            .is_err()
    );
    assert!(index.load_inputs_with_cancellation(&cancel).await.is_err());
    assert_eq!(
        index
            .load_inputs()
            .await
            .expect("last snapshot")
            .expect("published")
            .records
            .len(),
        1
    );
    assert_eq!(
        index
            .checkpoints()
            .await
            .expect("unchanged checkpoint")
            .into_values()
            .next()
            .expect("source")
            .parsed_offset,
        before
    );
}
