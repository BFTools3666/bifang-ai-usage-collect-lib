//! 用任务隔离的双屏障控制查询间提交，验证公有读取的快照与扫描事务边界。

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use sea_orm::{ConnectionTrait, DbBackend, Statement};
use tokio::sync::Barrier;

use super::{LocalError, LocalErrorKind, LocalIndex};
use crate::readers::jsonl::PARSER_VERSION;
use crate::{
    CanonicalUsageSet, CoverageState, DiscoveryMethod, LocalIndexState, ProviderKind,
    SessionTokenSnapshot,
};

/// 只暂停当前受测查询阶段，不影响同时运行的其他单元测试。
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadStage {
    /// 调用已读取，但累计快照尚未读取。
    Calls,
    /// 当前代次存在性已读取，但索引状态计数尚未读取。
    IndexUsage,
}

/// 当前测试任务专用的到达与放行屏障。
struct ReadGate {
    /// 仅该阶段允许暂停。
    stage: ReadStage,
    /// 通知测试驱动：首个真实 SQL 已读取完毕。
    reached: Arc<Barrier>,
    /// 等待测试驱动完成并发提交后继续读取。
    resume: Arc<Barrier>,
}

tokio::task_local! {
    /// 仅当前测试 future 可见的屏障，不增加生产字段或运行时钩子。
    static READ_GATE: ReadGate;
}

/// 在真实调用查询之后暂停当前受测读取。
pub(super) async fn pause_after_calls_query() {
    pause(ReadStage::Calls).await;
}

/// 在真实索引存在性查询之后暂停当前受测读取。
pub(super) async fn pause_after_index_usage_query() {
    pause(ReadStage::IndexUsage).await;
}

/// 仅在测试驱动明确安装相同阶段的任务屏障时等待。
async fn pause(stage: ReadStage) {
    let barriers = READ_GATE
        .try_with(|gate| {
            (gate.stage == stage).then(|| (Arc::clone(&gate.reached), Arc::clone(&gate.resume)))
        })
        .ok()
        .flatten();
    if let Some((reached, resume)) = barriers {
        reached.wait().await;
        resume.wait().await;
    }
}

/// 发布同一来源的新 generation，调用与累计快照具有可区分的同代数值。
async fn publish_version(
    index: &mut LocalIndex,
    root: &std::path::Path,
    generation: i64,
    input: u64,
) {
    let scan_id = index
        .begin_scan("snapshot-regression", generation * 1_000)
        .await
        .expect("scan starts");
    index
        .register_root_fields(
            "root-publication",
            root,
            "snapshot fixture",
            DiscoveryMethod::Registered,
        )
        .await
        .expect("root registers");
    super::tests::stage_scan_source(index, "generation", input).await;
    for table in ["source_files", "usage_calls"] {
        index
            .scan_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                format!("UPDATE {table} SET generation = ?1 WHERE source_id = 'generation'"),
                [generation.into()],
            ))
            .await
            .expect("fixture generation advances");
    }
    index
        .scan_connection()
        .execute_raw(Statement::from_string(
            DbBackend::Sqlite,
            "DELETE FROM usage_token_snapshots WHERE source_id = 'generation'".to_owned(),
        ))
        .await
        .expect("old snapshot is replaced inside transaction");
    index
        .insert_usage_and_snapshots(
            "generation",
            generation as u64,
            &mut Vec::new(),
            &mut vec![SessionTokenSnapshot {
                thread_key: "thread-generation".into(),
                occurred_at_epoch_ms: 1_000,
                cumulative_total_tokens: (input + 1) * 10,
                logical_call_id: "call-generation".into(),
                model: None,
                reasoning_effort: None,
                project_key: None,
                provenance: Vec::new(),
            }],
        )
        .await
        .expect("same-generation snapshot is staged");
    index
        .record_root_coverage("root-publication", CoverageState::Complete)
        .await
        .expect("coverage stages");
    index
        .finish_scan(
            &scan_id,
            generation * 1_000 + 1,
            "completed",
            false,
            1,
            1,
            0,
        )
        .await
        .expect("version publishes atomically");
}

/// 三个公有调用读取入口都必须使用同一事务覆盖调用与累计快照。
#[derive(Clone, Copy, Debug)]
enum ReadKind {
    /// 仅规范调用与快照。
    Canonical,
    /// 完整视图快照。
    Full,
    /// 有时间下界的视图快照。
    Since,
}

/// 按公有入口读取，不直接调用待修复的内部 helper。
async fn read(index: &mut LocalIndex, kind: ReadKind) -> Result<CanonicalUsageSet, LocalError> {
    match kind {
        ReadKind::Canonical => index.canonical_calls().await,
        ReadKind::Full => index
            .usage_snapshot()
            .await
            .map(|snapshot| snapshot.canonical),
        ReadKind::Since => index
            .usage_snapshot_since(0)
            .await
            .map(|snapshot| snapshot.canonical),
    }
}

/// 精确在两次 SELECT 之间发布新 generation，所有公有读取仍应返回完整旧快照。
#[tokio::test]
async fn public_reads_do_not_mix_calls_and_snapshots_across_publication() {
    for kind in [ReadKind::Canonical, ReadKind::Full, ReadKind::Since] {
        let temp = tempfile::tempdir().expect("isolated database");
        let mut writer = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
            .await
            .expect("writer opens");
        publish_version(&mut writer, temp.path(), 1, 10).await;
        let mut reader = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
            .await
            .expect("reader opens");
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let gate = ReadGate {
            stage: ReadStage::Calls,
            reached: Arc::clone(&reached),
            resume: Arc::clone(&resume),
        };
        let pending = tokio::spawn(READ_GATE.scope(gate, async move {
            let result = read(&mut reader, kind).await;
            (reader, result)
        }));
        tokio::time::timeout(Duration::from_secs(2), reached.wait())
            .await
            .expect("first SELECT reaches the barrier");
        publish_version(&mut writer, temp.path(), 2, 20).await;
        resume.wait().await;
        let (reader, old) = tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .expect("reader finishes")
            .expect("reader task joins");
        let old = old.expect("old snapshot reads");
        assert_eq!(old.calls.len(), 1, "{kind:?}");
        assert_eq!(old.snapshots.len(), 1, "{kind:?}");
        assert_eq!(old.calls[0].usage.total_tokens, 11, "{kind:?}");
        assert_eq!(old.snapshots[0].cumulative_total_tokens, 110, "{kind:?}");
        let current = reader
            .canonical_calls()
            .await
            .expect("next read sees new publication");
        assert_eq!(current.calls[0].usage.total_tokens, 21, "{kind:?}");
        assert_eq!(
            current.snapshots[0].cumulative_total_tokens, 210,
            "{kind:?}"
        );
    }
}

/// 索引 EXISTS 和状态计数之间并发回补时，状态不得变成从未发布的 ReadyNoCalls。
#[tokio::test]
async fn index_state_counts_share_one_published_snapshot() {
    let temp = tempfile::tempdir().expect("isolated database");
    let mut writer = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("writer opens");
    publish_version(&mut writer, temp.path(), 1, 10).await;
    writer
        .connection
        .execute_unprepared(&format!(
            "UPDATE source_files SET parser_version = {}",
            PARSER_VERSION - 1
        ))
        .await
        .expect("stale parser fixture");
    let reader = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("reader opens");
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let gate = ReadGate {
        stage: ReadStage::IndexUsage,
        reached: Arc::clone(&reached),
        resume: Arc::clone(&resume),
    };
    let pending =
        tokio::spawn(READ_GATE.scope(gate, async move { (reader.index_state().await, reader) }));
    tokio::time::timeout(Duration::from_secs(2), reached.wait())
        .await
        .expect("usage query reaches barrier");
    publish_version(&mut writer, temp.path(), 2, 20).await;
    resume.wait().await;
    let (old, reader) = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("state query finishes")
        .expect("reader joins");
    assert_eq!(
        old.expect("prior state reads"),
        LocalIndexState::NeedsRescan
    );
    assert_eq!(
        reader.index_state().await.expect("new state reads"),
        LocalIndexState::Ready
    );
}

/// 同实例公开已发布读取必须在一百毫秒内返回稳定忙错误。
async fn assert_busy<T: std::fmt::Debug>(future: impl Future<Output = Result<T, LocalError>>) {
    let error = tokio::time::timeout(Duration::from_millis(100), future)
        .await
        .expect("published read must not wait for its own pool")
        .expect_err("active published read is busy");
    assert_eq!(error.kind(), LocalErrorKind::ScanBusy);
}

/// 公开已发布视图立即拒绝同实例 active 扫描，专用 active 聚合仍能读取暂存事实。
#[tokio::test]
async fn active_scan_does_not_deadlock_public_reads_or_its_own_aggregate() {
    let temp = tempfile::tempdir().expect("isolated database");
    let mut writer = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("writer opens");
    publish_version(&mut writer, temp.path(), 1, 10).await;
    writer
        .begin_scan_attempt()
        .await
        .expect("unpublished scan starts");
    super::tests::stage_scan_source(&mut writer, "pending", 20).await;
    assert_busy(writer.canonical_calls()).await;
    assert_busy(writer.index_state()).await;
    assert_busy(writer.usage_snapshot()).await;
    assert_busy(writer.usage_snapshot_since(0)).await;
    assert_busy(writer.aggregate()).await;
    assert_busy(writer.aggregate_for_provider(ProviderKind::RolloutJsonl)).await;
    assert!(
        writer
            .has_current_parser_usage()
            .await
            .expect("scan policy still uses active transaction")
    );
    let staged = tokio::time::timeout(
        Duration::from_secs(2),
        writer.aggregate_active_scan_for_provider(ProviderKind::RolloutJsonl),
    )
    .await
    .expect("active aggregate uses owned transaction")
    .expect("active aggregate succeeds");
    assert_eq!(staged.tokens.total_tokens, 32);
    writer
        .abort_scan_attempt()
        .await
        .expect("unpublished changes roll back");
    assert_eq!(
        writer
            .aggregate_active_scan_for_provider(ProviderKind::RolloutJsonl)
            .await
            .expect("non-active aggregate uses published snapshot")
            .tokens
            .total_tokens,
        11
    );
    assert_eq!(
        writer
            .aggregate_for_provider(ProviderKind::RolloutJsonl)
            .await
            .expect("published aggregate resumes")
            .tokens
            .total_tokens,
        11
    );
}
