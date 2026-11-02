use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use std::time::Duration;

use crate::{CoverageState, DiscoveryMethod};

use super::snapshot_ops::resolve_scan_commit_error;
use super::{ActiveScanRecord, LocalError, LocalErrorKind, LocalIndex};
use crate::readers::jsonl::PARSER_VERSION;

/// 扫描先读取策略时，其他连接的根更新不能使随后写入落到过期 WAL 快照。
#[tokio::test]
async fn scan_attempt_reserves_writer_before_reading_policy() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let mut writer = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("writer opens");
    writer
        .register_root_fields(
            "root-lock",
            temp.path(),
            "before",
            DiscoveryMethod::Registered,
        )
        .await
        .expect("root registers");
    let mut competing = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("competing writer opens");
    writer.begin_scan_attempt().await.expect("attempt starts");
    writer
        .has_current_parser_usage()
        .await
        .expect("policy reads");

    let mut update =
        tokio::spawn(async move { competing.set_root_alias("root-lock", "after").await });
    let early_update = tokio::time::timeout(Duration::from_millis(200), &mut update).await;
    let prune = writer.prune_usage_before(0).await;
    writer
        .abort_scan_attempt()
        .await
        .expect("attempt rolls back");
    let (finished_early, update_result) = match early_update {
        Ok(result) => (true, result),
        Err(_) => (false, update.await),
    };
    update_result
        .expect("competing task joins")
        .expect("queued root update succeeds");
    assert!(
        prune.is_ok(),
        "scan policy read must remain writable: {prune:?}"
    );
    assert!(
        !finished_early,
        "competing write waits for the scan transaction"
    );
}

/// 活动扫描写事务期间新建的概览读连接应立即取得上一次发布的 Token 快照。
#[tokio::test]
async fn newly_opened_reader_loads_published_tokens_during_scan_transaction() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let mut writer = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("writer opens");
    writer
        .register_root_fields(
            "root-publication",
            temp.path(),
            "publication root",
            DiscoveryMethod::Registered,
        )
        .await
        .expect("root registers");
    let scan_id = writer
        .begin_scan("published", 1_000)
        .await
        .expect("scan starts");
    super::tests::stage_scan_source(&mut writer, "published", 10).await;
    writer
        .finish_scan(&scan_id, 1_001, "completed", false, 1, 1, 0)
        .await
        .expect("seed scan publishes");

    writer
        .begin_scan_attempt()
        .await
        .expect("scan transaction starts");
    super::tests::stage_scan_source(&mut writer, "pending", 20).await;
    let snapshot = tokio::time::timeout(Duration::from_secs(1), async {
        let mut reader = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION).await?;
        reader.usage_snapshot().await
    })
    .await
    .expect("overview read must not wait for the active scan")
    .expect("published snapshot remains readable");
    assert_eq!(snapshot.canonical.calls.len(), 1);
    assert_eq!(snapshot.canonical.calls[0].usage.total_tokens, 11);
    writer
        .abort_scan_attempt()
        .await
        .expect("scan transaction rolls back");
}

/// 长读事务挡住扫描提交时，新的概览连接仍应及时读到已发布 Token。
#[tokio::test]
async fn new_overview_reader_remains_available_while_scan_publication_waits_for_old_reader() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let mut writer = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("writer opens");
    writer
        .register_root_fields(
            "root-publication",
            temp.path(),
            "publication root",
            DiscoveryMethod::Registered,
        )
        .await
        .expect("root registers");
    let seed_scan = writer
        .begin_scan("published", 1_000)
        .await
        .expect("scan starts");
    super::tests::stage_scan_source(&mut writer, "published", 10).await;
    writer
        .finish_scan(&seed_scan, 1_001, "completed", false, 1, 1, 0)
        .await
        .expect("seed scan publishes");

    let old_reader = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("old reader opens");
    let old_read_transaction = old_reader
        .connection
        .begin()
        .await
        .expect("read transaction opens");
    old_read_transaction
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT COUNT(*) FROM usage_calls".to_owned(),
        ))
        .await
        .expect("read transaction acquires its snapshot");

    writer
        .begin_scan_attempt()
        .await
        .expect("scan transaction starts");
    let pending_scan = writer
        .begin_scan("pending", 2_000)
        .await
        .expect("scan starts");
    super::tests::stage_scan_source(&mut writer, "pending", 20).await;
    writer
        .finish_scan(&pending_scan, 2_001, "completed", false, 1, 1, 0)
        .await
        .expect("scan terminal stages");
    let publication = tokio::spawn(async move { writer.publish_scan_attempt().await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let snapshot = tokio::time::timeout(Duration::from_secs(1), async {
        let mut reader = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION).await?;
        reader.usage_snapshot().await
    })
    .await
    .expect("overview read must not wait for an old reader to release its snapshot")
    .expect("published tokens remain readable");
    assert!(
        snapshot
            .canonical
            .calls
            .iter()
            .any(|call| call.usage.total_tokens == 11)
    );

    old_read_transaction
        .rollback()
        .await
        .expect("old reader releases its snapshot");
    publication
        .await
        .expect("publication task joins")
        .expect("scan publishes");
}

/// 读取单条扫描的脱敏状态。
async fn stored_scan_status(index: &LocalIndex, scan_id: &str) -> Option<String> {
    index
        .connection
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT status FROM scan_runs WHERE scan_id = ?1",
            vec![scan_id.into()],
        ))
        .await
        .expect("scan status query succeeds")
        .map(|row| row.try_get_by_index(0).expect("scan status is text"))
}

/// commit 报错后只要同一 scan id 已经可见，就不能把未知的已提交终态
/// 单独改写为 Failed，否则会留下 Failed 记录配已发布数据的矛盾快照。
#[tokio::test]
async fn visible_unexpected_terminal_is_preserved_after_uncertain_commit() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let index = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("index opens");
    let record = ActiveScanRecord {
        scan_id: "visible-uncertain".to_owned(),
        mode: "quick".to_owned(),
        started_at_epoch_ms: 1_000,
    };
    index
        .connection
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO scan_runs
             (scan_id, mode, started_at_epoch_ms, finished_at_epoch_ms, status, cancelled,
              files_scanned, calls_added, warning_count)
             VALUES (?1, ?2, ?3, 1001, 'partial', 0, 4, 3, 2)",
            vec![
                record.scan_id.clone().into(),
                record.mode.clone().into(),
                record.started_at_epoch_ms.into(),
            ],
        ))
        .await
        .expect("visible terminal fixture is inserted");

    let error = resolve_scan_commit_error(
        &index.connection,
        &record,
        1_002,
        "completed",
        LocalError::new(LocalErrorKind::Database, "synthetic commit failure"),
    )
    .await
    .expect_err("unexpected visible terminal stays uncertain");

    assert_eq!(
        error.to_string(),
        "scan publication commit outcome is uncertain"
    );
    assert_eq!(
        stored_scan_status(&index, &record.scan_id).await.as_deref(),
        Some("partial")
    );
}

/// 发现侧 Partial 必须在提交前把 provider Completed 终态降为 Partial。
#[tokio::test]
async fn merged_partial_coverage_is_published_as_partial_terminal() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let mut index = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("index opens");
    index.begin_scan_attempt().await.expect("attempt starts");
    let scan_id = index
        .begin_scan("merged-partial", 2_500)
        .await
        .expect("scan starts");
    index
        .finish_scan(&scan_id, 2_501, "completed", false, 0, 0, 0)
        .await
        .expect("provider terminal stages");
    index
        .publish_scan_attempt_for_coverage(CoverageState::Partial)
        .await
        .expect("merged coverage publishes");
    assert_eq!(
        stored_scan_status(&index, &scan_id).await.as_deref(),
        Some("partial")
    );
}

/// 后台根终态任一步失败都回滚整轮，并允许同一个 LocalIndex 重试。
#[tokio::test]
async fn root_activation_failure_rolls_back_all_roots_and_allows_retry() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let mut index = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("index opens");
    for root_id in ["root-a", "root-b"] {
        index
            .register_root_fields(root_id, temp.path(), root_id, DiscoveryMethod::Registered)
            .await
            .expect("root registers");
    }
    index
        .connection
        .execute_unprepared(
            "UPDATE source_roots SET activation_state = 'confirmed_unindexed';
             CREATE TRIGGER fail_second_root_activation
             BEFORE UPDATE OF activation_state ON source_roots
             WHEN OLD.root_id = 'root-b' AND NEW.activation_state = 'validation_failed'
             BEGIN SELECT RAISE(FAIL, 'fixture activation failure'); END;",
        )
        .await
        .expect("activation failure trigger installs");

    index.begin_scan_attempt().await.expect("attempt starts");
    let claimed = index
        .claim_background_index_roots()
        .await
        .expect("claims are staged");
    let scan_id = index
        .begin_scan("activation-failure", 3_000)
        .await
        .expect("scan starts");
    index
        .finish_scan(&scan_id, 3_001, "completed", false, 0, 0, 0)
        .await
        .expect("terminal is staged");
    assert!(index.finish_background_index_roots(&claimed).await.is_err());
    index
        .fail_scan_attempt(3_002)
        .await
        .expect("failed workflow rolls back");
    let states = index
        .connection
        .query_all_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT activation_state FROM source_roots ORDER BY root_id".to_owned(),
        ))
        .await
        .expect("activation states read")
        .into_iter()
        .map(|row| row.try_get_by_index::<String>(0).expect("activation state"))
        .collect::<Vec<_>>();
    assert_eq!(states, vec!["confirmed_unindexed", "confirmed_unindexed"]);
    index
        .connection
        .execute_unprepared("DROP TRIGGER fail_second_root_activation")
        .await
        .expect("trigger drops");

    index.begin_scan_attempt().await.expect("retry starts");
    let claimed = index
        .claim_background_index_roots()
        .await
        .expect("retry claims roots");
    let scan_id = index
        .begin_scan("activation-retry", 4_000)
        .await
        .expect("retry scan starts");
    index
        .finish_scan(&scan_id, 4_001, "completed", false, 0, 0, 0)
        .await
        .expect("retry terminal stages");
    index
        .finish_background_index_roots(&claimed)
        .await
        .expect("retry activation finishes");
    index
        .publish_scan_attempt()
        .await
        .expect("retry publishes atomically");
}

/// scan terminal UPDATE 失败必须转成 Failed，不能遗留 Running。
#[tokio::test]
async fn terminal_update_failure_records_failed_and_releases_transaction() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let mut index = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("index opens");
    index
        .connection
        .execute_unprepared(
            "CREATE TRIGGER fail_scan_terminal_update
             BEFORE UPDATE OF status ON scan_runs
             WHEN NEW.status IN ('completed', 'partial')
             BEGIN SELECT RAISE(FAIL, 'fixture terminal failure'); END;",
        )
        .await
        .expect("terminal trigger installs");
    let scan_id = index
        .begin_scan("finish-failure", 5_000)
        .await
        .expect("scan starts");
    assert!(
        index
            .finish_scan(&scan_id, 5_001, "completed", false, 0, 0, 0)
            .await
            .is_err()
    );
    assert!(index.active_scan_transaction.is_none());
    assert_eq!(
        stored_scan_status(&index, &scan_id).await.as_deref(),
        Some("failed")
    );
}

/// 延迟外键让 COMMIT 本身失败，扫描仍须收口 Failed 且同实例可重试。
#[tokio::test]
async fn commit_failure_records_failed_and_allows_same_instance_retry() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let mut index = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("index opens");
    index
        .connection
        .execute_unprepared(
            "CREATE TABLE commit_failure_fixture (
               id INTEGER PRIMARY KEY,
               root_id TEXT NOT NULL,
               FOREIGN KEY(root_id) REFERENCES source_roots(root_id)
                 DEFERRABLE INITIALLY DEFERRED
             )",
        )
        .await
        .expect("deferred constraint fixture installs");
    let scan_id = index
        .begin_scan("commit-failure", 6_000)
        .await
        .expect("scan starts");
    index
        .scan_connection()
        .execute_raw(Statement::from_string(
            DbBackend::Sqlite,
            "INSERT INTO commit_failure_fixture(id, root_id) VALUES (1, 'missing-root')",
        ))
        .await
        .expect("deferred violation is staged");
    assert!(
        index
            .finish_scan(&scan_id, 6_001, "completed", false, 0, 0, 0)
            .await
            .is_err()
    );
    assert!(index.active_scan_transaction.is_none());
    assert_eq!(
        stored_scan_status(&index, &scan_id).await.as_deref(),
        Some("failed")
    );
    let retry = index
        .begin_scan("commit-retry", 7_000)
        .await
        .expect("retry starts");
    index
        .finish_scan(&retry, 7_001, "completed", false, 0, 0, 0)
        .await
        .expect("same instance retries after commit failure");
}

/// begin 的运行记录 INSERT 失败会整体回滚并允许同实例重试。
#[tokio::test]
async fn begin_record_failure_has_no_running_orphan_and_allows_retry() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let mut index = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("index opens");
    index
        .connection
        .execute_unprepared(
            "CREATE TRIGGER fail_running_scan_insert
             BEFORE INSERT ON scan_runs WHEN NEW.status = 'running'
             BEGIN SELECT RAISE(FAIL, 'fixture begin failure'); END;",
        )
        .await
        .expect("begin trigger installs");
    assert!(index.begin_scan("begin-failure", 8_000).await.is_err());
    assert!(index.active_scan_transaction.is_none());
    let running: i64 = index
        .connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT COUNT(*) FROM scan_runs WHERE status = 'running'".to_owned(),
        ))
        .await
        .expect("running count reads")
        .expect("count row exists")
        .try_get_by_index(0)
        .expect("count is integer");
    assert_eq!(running, 0);
    index
        .connection
        .execute_unprepared("DROP TRIGGER fail_running_scan_insert")
        .await
        .expect("begin trigger drops");
    let retry = index
        .begin_scan("begin-retry", 9_000)
        .await
        .expect("retry starts");
    index
        .finish_scan(&retry, 9_001, "completed", false, 0, 0, 0)
        .await
        .expect("same instance retries after begin failure");
}

/// 事务本身无法开始时，不可能先留下外部 Running 行。
#[tokio::test]
async fn transaction_begin_failure_cannot_orphan_running_scan() {
    let temp = tempfile::tempdir().expect("isolated app data is available");
    let mut index = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("index opens");
    index
        .connection
        .clone()
        .close()
        .await
        .expect("fixture closes pool");
    assert!(index.begin_scan("closed-pool", 10_000).await.is_err());
    let reopened = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("database reopens");
    let running: i64 = reopened
        .connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT COUNT(*) FROM scan_runs WHERE status = 'running'".to_owned(),
        ))
        .await
        .expect("running count reads")
        .expect("count row exists")
        .try_get_by_index(0)
        .expect("count is integer");
    assert_eq!(running, 0);
}
