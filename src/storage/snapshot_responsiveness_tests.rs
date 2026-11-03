//! 以隔离索引验证分页不截断、协作取消与 current-thread 执行器的 CPU 所有权。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use sea_orm::{DbBackend, Statement};

use super::{LocalError, LocalErrorKind, LocalIndex};
use crate::readers::jsonl::PARSER_VERSION;
use crate::{CoverageState, DiscoveryMethod, ProviderKind, ScanCancellation};

/// 区分规范去重与最终摘要两个必须离开异步执行器的 CPU 阶段。
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CpuStage {
    /// 调用与累计快照的规范化阶段。
    Canonical,
    /// 最终摘要聚合及规范集合回收阶段。
    Aggregate,
}

/// 仅测试持有的阻塞屏障，超时可自行解除以避免错误实现挂死整个测试进程。
#[derive(Clone)]
pub(super) struct CpuGate {
    /// 通知异步测试当前 CPU 阶段已经开始。
    reached: Arc<tokio::sync::Notify>,
    /// 异步 heartbeat 放行，条件变量只在受测阻塞任务中等待。
    resume: Arc<(Mutex<bool>, Condvar)>,
}

impl CpuGate {
    /// 通知并等待 heartbeat；错误地在 current-thread 运行时调用时会有界失败。
    pub(super) fn pause(&self) {
        self.reached.notify_one();
        let (lock, condition) = &*self.resume;
        let resumed = lock.lock().expect("CPU gate lock is healthy");
        let (resumed, _) = condition
            .wait_timeout_while(resumed, Duration::from_secs(2), |resumed| !*resumed)
            .expect("CPU gate wait is healthy");
        assert!(
            *resumed,
            "async heartbeat must run while CPU work is paused"
        );
    }

    /// 由运行时任务放行受控 CPU 阶段。
    fn resume(&self) {
        let (lock, condition) = &*self.resume;
        *lock.lock().expect("CPU gate lock is healthy") = true;
        condition.notify_one();
    }
}

/// 当前 future 专用控制，不影响同进程并行执行的其他真实存储回归。
struct ReadControl {
    /// 已完成的真实 SQL 调用页数。
    pages: Arc<AtomicUsize>,
    /// 达到指定页后请求取消。
    cancel_after_page: Option<usize>,
    /// 与受测摘要共用的取消令牌。
    cancellation: ScanCancellation,
    /// 可选 CPU 阶段屏障。
    cpu: Option<(CpuStage, CpuGate)>,
}

tokio::task_local! {
    /// 只有明确安装测试控制的 future 才会观察到页计数和 CPU 屏障。
    static READ_CONTROL: ReadControl;
}

/// 在一页真实调用解码完成后记录进度，可精确请求页级取消。
pub(super) fn after_calls_page() {
    let _ = READ_CONTROL.try_with(|control| {
        let pages = control.pages.fetch_add(1, Ordering::SeqCst) + 1;
        if control.cancel_after_page == Some(pages) {
            control.cancellation.cancel();
        }
    });
}

/// 在进入阻塞任务前复制当前 future 的 CPU 测试屏障。
pub(super) fn cpu_gate(stage: CpuStage) -> Option<CpuGate> {
    READ_CONTROL
        .try_with(|control| {
            control
                .cpu
                .as_ref()
                .and_then(|(selected, gate)| (*selected == stage).then(|| gate.clone()))
        })
        .ok()
        .flatten()
}

/// 使用单个真实来源批量填入超过两页的调用，不读取任何宿主来源。
async fn fixture() -> (tempfile::TempDir, LocalIndex) {
    let temp = tempfile::tempdir().expect("isolated index directory");
    let mut index = LocalIndex::open_in_app_data(temp.path(), PARSER_VERSION)
        .await
        .expect("isolated index opens");
    let scan = index.begin_scan("fixture", 1).await.expect("scan begins");
    index
        .register_root_fields(
            "root-publication",
            temp.path(),
            "fixture",
            DiscoveryMethod::Registered,
        )
        .await
        .expect("fixture root registers");
    super::tests::stage_scan_source(&mut index, "source-a", 10).await;
    super::tests::stage_scan_source(&mut index, "source-b", 20).await;
    let statements =
        "WITH RECURSIVE numbers(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM numbers WHERE n<519)
         INSERT INTO usage_calls SELECT source_id,generation,printf('bulk-%04d',n),occurred_at_epoch_ms,
         model,reasoning_effort,project_key,thread_key,input_tokens,cached_input_tokens,
         cache_write_input_tokens,output_tokens,reasoning_output_tokens,total_tokens,total_is_derived,
         confidence,cached_input_available,reasoning_output_available,adapter_consistency_key,
         project_label,thread_label,input_available,output_available
         FROM usage_calls,numbers WHERE source_id='source-a';
         UPDATE usage_calls SET logical_call_id='bulk-0255',confidence='derived' WHERE source_id='source-b';
         INSERT INTO usage_token_snapshots(source_id,generation,thread_key,occurred_at_epoch_ms,
           logical_call_id,total_tokens) VALUES
           ('source-a',1,'snapshot-thread',1000,'snapshot-call',123),
           ('source-b',1,'snapshot-thread',1000,'snapshot-call',999);";
    for sql in statements.split(';').filter(|sql| !sql.trim().is_empty()) {
        index
            .scan_connection()
            .execute_raw(Statement::from_string(DbBackend::Sqlite, sql.to_owned()))
            .await
            .expect("multi-page fixture is staged");
    }
    index
        .record_root_coverage("root-publication", CoverageState::Complete)
        .await
        .expect("coverage stages");
    index
        .finish_scan(&scan, 2, "completed", false, 2, 522, 0)
        .await
        .expect("fixture publishes");
    (temp, index)
}

/// 分页跨越同时间、同逻辑调用的跨源冲突，且不改变累计快照原先到赢家。
#[tokio::test(flavor = "current_thread")]
async fn paged_snapshot_preserves_all_calls_conflicts_and_snapshot_order() {
    let (_temp, mut index) = fixture().await;
    let pages = Arc::new(AtomicUsize::new(0));
    let cancellation = ScanCancellation::new();
    let control = ReadControl {
        pages: Arc::clone(&pages),
        cancel_after_page: None,
        cancellation: cancellation.clone(),
        cpu: None,
    };
    let snapshot = READ_CONTROL
        .scope(control, index.usage_snapshot())
        .await
        .expect("paged snapshot");
    assert_eq!(
        pages.load(Ordering::SeqCst),
        3,
        "all 522 physical rows are read"
    );
    assert_eq!(snapshot.canonical.calls.len(), 521);
    assert_eq!(snapshot.canonical.duplicate_source_count, 1);
    assert_eq!(snapshot.canonical.warnings.len(), 1);
    let duplicate = snapshot
        .canonical
        .calls
        .iter()
        .find(|call| call.logical_call_id == "bulk-0255")
        .expect("duplicate remains");
    assert_eq!(
        duplicate.usage.total_tokens, 11,
        "higher-confidence original wins"
    );
    assert_eq!(duplicate.provenance.len(), 2);
    assert_eq!(snapshot.canonical.snapshots.len(), 1);
    assert_eq!(snapshot.canonical.snapshots[0].cumulative_total_tokens, 123);
    assert_eq!(snapshot.canonical.snapshots[0].provenance.len(), 2);
    let expected = crate::aggregate_canonical_usage(&snapshot.canonical).expect("pure aggregate");
    assert_eq!(
        index
            .aggregate_for_provider(ProviderKind::RolloutJsonl)
            .await
            .expect("legacy aggregate"),
        expected
    );
    assert_eq!(
        index
            .aggregate_for_provider_with_cancellation(ProviderKind::RolloutJsonl, &cancellation)
            .await
            .expect("cancellable aggregate"),
        expected
    );
    let recent = index
        .usage_snapshot_since(1001)
        .await
        .expect("empty cutoff snapshot");
    assert!(recent.canonical.calls.is_empty());
    assert!(recent.canonical.snapshots.is_empty());
}

/// 当前任务页级取消停止下一页，返回错误后旧已发布代仍完整可读。
#[tokio::test(flavor = "current_thread")]
async fn summary_cancellation_stops_at_page_boundary_without_publishing_partial_results() {
    let (_temp, index) = fixture().await;
    let cancellation = ScanCancellation::new();
    let pages = Arc::new(AtomicUsize::new(0));
    let control = ReadControl {
        pages: Arc::clone(&pages),
        cancel_after_page: Some(1),
        cancellation: cancellation.clone(),
        cpu: None,
    };
    let result = READ_CONTROL
        .scope(
            control,
            index.aggregate_for_provider_with_cancellation(
                ProviderKind::RolloutJsonl,
                &cancellation,
            ),
        )
        .await;
    assert_eq!(
        result.expect_err("read is cancelled").kind(),
        LocalErrorKind::SourceUnavailable
    );
    assert_eq!(pages.load(Ordering::SeqCst), 1);
    assert_eq!(
        index
            .canonical_calls()
            .await
            .expect("published snapshot remains intact")
            .calls
            .len(),
        521
    );
    let cancelled = ScanCancellation::new();
    cancelled.cancel();
    assert!(
        index
            .aggregate_active_scan_for_provider_with_cancellation(
                ProviderKind::RolloutJsonl,
                &cancelled
            )
            .await
            .is_err()
    );
}

/// 规范化和最终聚合在单线程执行器上都允许 heartbeat 运行并立即请求取消。
#[tokio::test(flavor = "current_thread")]
async fn summary_cpu_work_keeps_current_thread_heartbeat_and_cancellation_responsive() {
    let (_temp, index) = fixture().await;
    for stage in [CpuStage::Canonical, CpuStage::Aggregate] {
        let cancellation = ScanCancellation::new();
        let gate = CpuGate {
            reached: Arc::new(tokio::sync::Notify::new()),
            resume: Arc::new((Mutex::new(false), Condvar::new())),
        };
        let control = ReadControl {
            pages: Arc::new(AtomicUsize::new(0)),
            cancel_after_page: None,
            cancellation: cancellation.clone(),
            cpu: Some((stage, gate.clone())),
        };
        let read = READ_CONTROL.scope(
            control,
            index.aggregate_for_provider_with_cancellation(
                ProviderKind::RolloutJsonl,
                &cancellation,
            ),
        );
        let heartbeat = async {
            tokio::time::timeout(Duration::from_secs(1), gate.reached.notified())
                .await
                .expect("CPU worker starts");
            tokio::task::yield_now().await;
            cancellation.cancel();
            gate.resume();
        };
        let (result, ()) = tokio::join!(read, heartbeat);
        assert_eq!(
            result.expect_err("worker observes cancellation").kind(),
            LocalErrorKind::SourceUnavailable
        );
    }
}

/// 纯业务取消回调在调用及快照循环内部终止，未取消路径与旧入口完全一致。
#[test]
fn pure_canonical_and_aggregate_checks_stop_inside_cpu_loops() {
    use crate::aggregate::{
        aggregate_canonical_usage_with_check, attach_session_snapshots_with_check,
        canonicalize_usage_calls_with_check,
    };
    use crate::{Confidence, SourceProvenance, TokenUsage, UsageCall};
    let calls = (0..32)
        .map(|number| UsageCall {
            logical_call_id: format!("call-{number}"),
            occurred_at_epoch_ms: 1,
            model: None,
            reasoning_effort: None,
            project_key: None,
            thread_key: "thread".into(),
            project_label: None,
            thread_label: None,
            usage: TokenUsage::new(1, 0, None, 1, 0, Some(2)).expect("valid fixture"),
            adapter_consistency_key: None,
            confidence: Confidence::Exact,
            provenance: vec![SourceProvenance {
                source_id: format!("source-{number}"),
                root_id: "root".into(),
                relative_label: "fixture".into(),
                archived: false,
            }],
        })
        .collect::<Vec<_>>();
    let mut checks = 0;
    let cancel = || {
        checks += 1;
        if checks == 8 {
            Err(LocalError::new(
                LocalErrorKind::SourceUnavailable,
                "fixture cancelled",
            ))
        } else {
            Ok(())
        }
    };
    assert!(canonicalize_usage_calls_with_check(calls.clone(), cancel).is_err());
    assert_eq!(checks, 8);
    let canonical = crate::canonicalize_usage_calls(calls.clone());
    assert_eq!(
        canonicalize_usage_calls_with_check(calls, || Ok::<(), LocalError>(()))
            .expect("uncancelled"),
        canonical
    );
    let mut checks = 0;
    assert!(
        aggregate_canonical_usage_with_check(&canonical, || {
            checks += 1;
            if checks == 8 {
                Err(LocalError::new(
                    LocalErrorKind::SourceUnavailable,
                    "fixture cancelled",
                ))
            } else {
                Ok(())
            }
        })
        .is_err()
    );
    assert_eq!(checks, 8);
    let snapshots = (0..32)
        .map(|number| crate::SessionTokenSnapshot {
            thread_key: format!("thread-{number}"),
            occurred_at_epoch_ms: 1,
            cumulative_total_tokens: 10,
            logical_call_id: "snapshot".into(),
            model: None,
            reasoning_effort: None,
            project_key: None,
            provenance: Vec::new(),
        })
        .collect();
    let mut checks = 0;
    assert!(
        attach_session_snapshots_with_check(canonical, snapshots, || {
            checks += 1;
            if checks == 8 {
                Err(LocalError::new(
                    LocalErrorKind::SourceUnavailable,
                    "fixture cancelled",
                ))
            } else {
                Ok(())
            }
        })
        .is_err()
    );
    assert_eq!(checks, 8);
}
