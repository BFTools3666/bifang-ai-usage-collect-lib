//! 桌面库只读、逐模型统计、WAL 与坏来源的隔离回归。

use super::*;
use sea_orm::DatabaseConnection;
use tempfile::{TempDir, tempdir};

/// 建立只含获批用量表与正文诱饵的临时桌面库。
async fn fixture() -> (TempDir, PathBuf, DatabaseConnection) {
    let temp = tempdir().expect("temporary source home");
    let root = temp.path().join(".zcode/cli/db");
    fs::create_dir_all(&root).expect("source directory");
    let path = root.join("db.sqlite");
    let mut options = ConnectOptions::new("sqlite://test-source");
    options.max_connections(1).sqlx_logging(false);
    options.map_sqlx_sqlite_opts(move |options| options.filename(&path).create_if_missing(true));
    let connection = Database::connect(options).await.expect("source database");
    connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA journal_mode=WAL".to_owned(),
        ))
        .await
        .expect("WAL mode");
    connection
        .execute_raw(Statement::from_string(
            DbBackend::Sqlite,
            "CREATE TABLE model_usage (id TEXT PRIMARY KEY, session_id TEXT, model_id TEXT,
         started_at INTEGER, input_tokens INTEGER, cache_read_input_tokens INTEGER,
         cache_creation_input_tokens INTEGER, output_tokens INTEGER, computed_total_tokens INTEGER,
         error_message TEXT, raw_usage_json TEXT, provider_metadata_json TEXT)"
                .to_owned(),
        ))
        .await
        .expect("usage schema");
    connection
        .execute_raw(Statement::from_string(
            DbBackend::Sqlite,
            "CREATE TABLE message (body TEXT); INSERT INTO message VALUES ('private-body-bait')"
                .to_owned(),
        ))
        .await
        .expect("unread body bait");
    (temp, root, connection)
}

/// 写入已脱敏的逐模型计量，包含禁止投影的额外诱饵列。
async fn insert(connection: &DatabaseConnection, id: &str, model: &str, time: i64, input: i64) {
    connection
        .execute_raw(statement(
            "INSERT INTO model_usage VALUES (?1, 'private-session-id', ?2, ?3, ?4, 0, 0, 2, ?5,
         'private-error-bait', 'private-raw-bait', 'private-provider-bait')",
            vec![
                id.into(),
                model.into(),
                time.into(),
                input.into(),
                (input + 2).into(),
            ],
        ))
        .await
        .expect("synthetic usage");
}

/// 昨日两个模型原样分组、边界按毫秒保留，全部输出不含正文或原始身份。
#[tokio::test]
async fn reads_both_models_and_only_whitelisted_metadata() {
    let (_temp, root, connection) = fixture().await;
    let yesterday = 1_793_577_600_000;
    insert(&connection, "paid", "GLM-5.3", yesterday, 100).await;
    insert(&connection, "free", "GLM-5.3-Flash", yesterday + 1, 200).await;
    insert(&connection, "old", "GLM-5.3", yesterday - 1, 300).await;
    let before = fs::read(root.join("db.sqlite")).expect("source bytes");
    let mut reader = ZcodeUsageDatabase::open(&root, "glm-root-test", yesterday)
        .await
        .expect("read only source");
    let calls = reader.next_batch().await.expect("usage page");
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls
            .iter()
            .map(|call| call.usage.total_tokens)
            .sum::<u64>(),
        304
    );
    assert_eq!(
        calls
            .iter()
            .map(|call| call.model.as_deref().expect("model"))
            .collect::<Vec<_>>(),
        ["GLM-5.3-Flash", "GLM-5.3"]
    );
    assert!(reader.next_batch().await.expect("end of source").is_empty());
    let encoded = serde_json::to_string(&calls).expect("safe calls");
    assert!(!encoded.contains("private-"));
    assert_eq!(
        fs::read(root.join("db.sqlite")).expect("source after read"),
        before
    );
}

/// 只读快照保持一致；新读者必须看到 WAL 内更新，不能以主文件未变跳过。
#[tokio::test]
async fn reads_wal_updates_without_changing_an_open_snapshot() {
    let (_temp, root, connection) = fixture().await;
    insert(&connection, "a", "GLM-5.3", 100, 10).await;
    let mut first = ZcodeUsageDatabase::open(&root, "glm-root-test", 0)
        .await
        .expect("first reader");
    let main = fs::read(root.join("db.sqlite")).expect("main database");
    insert(&connection, "b", "GLM-5.3-Flash", 200, 20).await;
    assert_eq!(
        fs::read(root.join("db.sqlite")).expect("main unchanged"),
        main
    );
    assert_eq!(
        first
            .next_batch()
            .await
            .expect("consistent old snapshot")
            .len(),
        1
    );
    let mut next = ZcodeUsageDatabase::open(&root, "glm-root-test", 0)
        .await
        .expect("new reader");
    assert_eq!(next.next_batch().await.expect("new WAL snapshot").len(), 2);
}

/// 非法计量阻断整页；缺失来源不得由 SQLite 自动创建。
#[tokio::test]
async fn rejects_negative_counts_and_does_not_create_missing_database() {
    let (_temp, root, connection) = fixture().await;
    insert(&connection, "bad", "GLM-5.3", 100, -1).await;
    let mut reader = ZcodeUsageDatabase::open(&root, "glm-root-test", 0)
        .await
        .expect("open source");
    assert_eq!(
        reader
            .next_batch()
            .await
            .expect_err("negative usage")
            .kind(),
        LocalErrorKind::InvalidUsage
    );
    let missing = root.parent().expect("cli").join("other/db");
    fs::create_dir_all(&missing).expect("empty source root");
    assert!(
        ZcodeUsageDatabase::open(&missing, "glm-root-missing", 0)
            .await
            .is_err()
    );
    assert!(!missing.join("db.sqlite").exists());
}

/// SQLite view 不得伪装成用量表去读取其它来源。
#[tokio::test]
async fn rejects_views_in_place_of_usage_table() {
    let (_temp, root, connection) = fixture().await;
    connection
        .execute_raw(Statement::from_string(
            DbBackend::Sqlite,
            "DROP TABLE model_usage; CREATE VIEW model_usage AS SELECT body FROM message"
                .to_owned(),
        ))
        .await
        .expect("decoy view");
    assert!(
        ZcodeUsageDatabase::open(&root, "glm-root-test", 0)
            .await
            .is_err()
    );
}

/// 链接型主库和 WAL 在打开前被拒绝，外部目标不被访问。
#[cfg(unix)]
#[tokio::test]
async fn rejects_linked_database_and_wal() {
    let (_temp, root, connection) = fixture().await;
    connection.close().await.expect("close writer");
    let database = root.join("db.sqlite");
    let real = root.join("outside.sqlite");
    fs::rename(&database, &real).expect("move test source");
    std::os::unix::fs::symlink(&real, &database).expect("decoy database link");
    assert!(
        ZcodeUsageDatabase::open(&root, "glm-root-test", 0)
            .await
            .is_err()
    );
    fs::remove_file(&database).expect("remove test link");
    fs::rename(&real, &database).expect("restore source");
    std::os::unix::fs::symlink(&database, root.join("db.sqlite-wal")).expect("decoy WAL link");
    assert!(
        ZcodeUsageDatabase::open(&root, "glm-root-test", 0)
            .await
            .is_err()
    );
}
