//! 真实 SQLite 锁控制 journal 初始化时序，验证有限等待、复读与共享调用者。

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement,
    TransactionTrait,
};
use tokio::sync::Barrier;

use super::private_sqlite::SQLITE_BUSY_TIMEOUT;
use super::workbuddy::WorkbuddyIndex;
use super::{LocalErrorKind, LocalIndex, enable_concurrent_snapshot_reads};
use crate::SourceClientKind;

/// 测试仅使用静态无参数 SQL，避免耦合其它测试模块的私有辅助函数。
fn statement(sql: &str) -> Statement {
    Statement::from_string(DbBackend::Sqlite, sql.to_owned())
}

/// 当前受测任务记录真实锁错误，可在第一次错误后交给测试驱动切换 journal。
struct WalGate {
    /// 已观察到的真实 SQLite BUSY/LOCKED 次数。
    failures: Arc<AtomicUsize>,
    /// 首次错误后的到达和恢复屏障；不设置时只统计。
    barriers: Option<(Arc<Barrier>, Arc<Barrier>)>,
}

tokio::task_local! {
    /// 钩子只属于当前测试任务，不会阻塞其它同时运行的测试。
    static WAL_GATE: WalGate;
}

/// 仅测试任务在第一个真实锁错误后暂停，生产代码不包含屏障或日志。
pub(super) async fn pause_after_lock_contention() {
    let barriers = WAL_GATE
        .try_with(|gate| {
            (gate.failures.fetch_add(1, Ordering::SeqCst) == 0)
                .then(|| gate.barriers.clone())
                .flatten()
        })
        .ok()
        .flatten();
    if let Some((reached, resume)) = barriers {
        reached.wait().await;
        resume.wait().await;
    }
}

/// 打开测试自造的普通 SQLite，保持默认 DELETE journal，不读取任何产品数据。
async fn fixture_connection(path: &Path, read_only: bool) -> DatabaseConnection {
    let mut options = ConnectOptions::new("sqlite://journal-fixture");
    let filename = path.to_path_buf();
    options.sqlx_logging(false).max_connections(1);
    options.map_sqlx_sqlite_opts(move |options| {
        options
            .filename(&filename)
            .create_if_missing(!read_only)
            .read_only(read_only)
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
    });
    Database::connect(options).await.expect("fixture opens")
}

/// 读取真实连接当前 journal 模式，不能把一次请求被受理当作切换成功。
async fn mode<C: ConnectionTrait>(connection: &C) -> String {
    connection
        .query_one_raw(statement("PRAGMA journal_mode"))
        .await
        .expect("journal reads")
        .expect("journal row")
        .try_get_by_index(0)
        .expect("journal mode decodes")
}

/// 读锁强制首次切换失败，另一连接完成 WAL 后失败方必须复读实际模式而成功。
#[tokio::test]
async fn journal_contention_accepts_only_verified_wal_after_another_connection_switches() {
    let temp = tempfile::tempdir().expect("isolated database");
    let path = temp.path().join("journal.db");
    let holder = fixture_connection(&path, false).await;
    holder
        .execute_unprepared("CREATE TABLE fixture(value INTEGER); INSERT INTO fixture VALUES(1)")
        .await
        .expect("fixture schema");
    let contender = fixture_connection(&path, false).await;
    // 仅测试连接关闭 SQLite 自带等待，稳定触发真实 BUSY 后由应用预算接管。
    contender
        .execute_unprepared("PRAGMA busy_timeout = 0")
        .await
        .expect("fixture busy timeout");
    let read_lock = holder.begin().await.expect("reader starts");
    read_lock
        .query_all_raw(statement("SELECT value FROM fixture"))
        .await
        .expect("shared read lock is held");
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let failures = Arc::new(AtomicUsize::new(0));
    let gate = WalGate {
        failures: Arc::clone(&failures),
        barriers: Some((Arc::clone(&reached), Arc::clone(&resume))),
    };
    let pending = tokio::spawn(WAL_GATE.scope(gate, async move {
        let result = enable_concurrent_snapshot_reads(&contender).await;
        (result, contender)
    }));
    tokio::time::timeout(Duration::from_secs(2), reached.wait())
        .await
        .expect("real journal contention reaches the barrier");
    assert_eq!(mode(&read_lock).await, "delete");
    read_lock.rollback().await.expect("read lock releases");
    holder
        .execute_unprepared("PRAGMA journal_mode = WAL")
        .await
        .expect("other connection configures WAL");
    resume.wait().await;
    let (result, contender) = pending.await.expect("contender joins");
    result.expect("actual WAL readback resolves contention");
    assert_eq!(failures.load(Ordering::SeqCst), 1);
    assert_eq!(mode(&contender).await, "wal");
}

/// 持续外部读锁不能伪装为已配置 WAL，有限忙等待预算到期后明确失败。
#[tokio::test]
async fn persistent_reader_keeps_delete_mode_and_fails_within_the_busy_budget() {
    let temp = tempfile::tempdir().expect("isolated database");
    let path = temp.path().join("journal.db");
    let holder = fixture_connection(&path, false).await;
    holder
        .execute_unprepared("CREATE TABLE fixture(value INTEGER)")
        .await
        .expect("fixture schema");
    let contender = fixture_connection(&path, false).await;
    contender
        .execute_unprepared("PRAGMA busy_timeout = 0")
        .await
        .expect("fixture busy timeout");
    let read_lock = holder.begin().await.expect("reader starts");
    read_lock
        .query_all_raw(statement("SELECT value FROM fixture"))
        .await
        .expect("shared read lock is held");
    let failures = Arc::new(AtomicUsize::new(0));
    let gate = WalGate {
        failures: Arc::clone(&failures),
        barriers: None,
    };
    let error = tokio::time::timeout(
        SQLITE_BUSY_TIMEOUT + Duration::from_secs(1),
        WAL_GATE.scope(gate, enable_concurrent_snapshot_reads(&contender)),
    )
    .await
    .expect("journal wait is bounded")
    .expect_err("reader prevents WAL switching");
    assert_eq!(error.kind(), LocalErrorKind::Database);
    assert!(
        failures.load(Ordering::SeqCst) > 1,
        "real contention causes bounded async retries"
    );
    assert_eq!(mode(&read_lock).await, "delete");
    read_lock.rollback().await.expect("read lock releases");
}

/// 非锁竞争的只读写入错误立即传播，不进入锁等待或把 DELETE 模式当作可用 WAL。
#[tokio::test]
async fn read_only_journal_error_does_not_enter_the_contention_retry() {
    let temp = tempfile::tempdir().expect("isolated database");
    let path = temp.path().join("journal.db");
    let holder = fixture_connection(&path, false).await;
    holder
        .execute_unprepared("CREATE TABLE fixture(value INTEGER)")
        .await
        .expect("fixture schema");
    let reader = fixture_connection(&path, true).await;
    let failures = Arc::new(AtomicUsize::new(0));
    let gate = WalGate {
        failures: Arc::clone(&failures),
        barriers: None,
    };
    let error = WAL_GATE
        .scope(gate, enable_concurrent_snapshot_reads(&reader))
        .await
        .expect_err("read-only DELETE database cannot switch journal");
    assert_eq!(error.kind(), LocalErrorKind::Database);
    assert_eq!(failures.load(Ordering::SeqCst), 0);
    assert_eq!(mode(&holder).await, "delete");
}

/// 两种公开开库入口共享首次并发协议，普通与嵌套相对路径均不得随机失败。
#[tokio::test]
async fn concurrent_first_open_supports_both_index_callers_and_nested_paths() {
    for relative in ["index.db", "nested/index.db"] {
        for attempt in 0..32 {
            let app = tempfile::tempdir().expect("isolated LocalIndex database");
            let (left, right) = tokio::join!(
                LocalIndex::open(app.path(), Path::new(relative), SourceClientKind::Codex),
                LocalIndex::open(app.path(), Path::new(relative), SourceClientKind::Codex)
            );
            assert!(
                left.is_ok(),
                "LocalIndex {relative} attempt {attempt}: {left:?}"
            );
            assert!(
                right.is_ok(),
                "LocalIndex {relative} attempt {attempt}: {right:?}"
            );
            let app = tempfile::tempdir().expect("isolated WorkBuddy database");
            let (left, right) = tokio::join!(
                WorkbuddyIndex::open(app.path(), Path::new(relative)),
                WorkbuddyIndex::open(app.path(), Path::new(relative))
            );
            assert!(
                left.is_ok(),
                "WorkBuddy {relative} attempt {attempt}: {left:?}"
            );
            assert!(
                right.is_ok(),
                "WorkBuddy {relative} attempt {attempt}: {right:?}"
            );
        }
    }
}
