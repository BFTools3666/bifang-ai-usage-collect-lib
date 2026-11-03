//! 把规范去重与摘要 CPU 工作交给持有输入的阻塞任务，并协作处理取消。

use crate::aggregate::{
    aggregate_canonical_usage_with_check, attach_session_snapshots_with_check,
    canonicalize_usage_calls_with_check,
};
use crate::{
    CanonicalUsageSet, LocalError, LocalErrorKind, LocalUsageAggregate, ProviderKind,
    ScanCancellation, UsageCall,
};

/// 取消只终止当前读取或摘要，不把部分结果交给扫描发布。
pub(super) fn check_cancelled(cancellation: &ScanCancellation) -> Result<(), LocalError> {
    if cancellation.is_cancelled() {
        Err(LocalError::new(
            LocalErrorKind::SourceUnavailable,
            "local index read was cancelled",
        ))
    } else {
        Ok(())
    }
}

/// 在拥有调用与快照的阻塞任务中执行原始规范合并，等待任务完整回收。
pub(super) async fn finish_canonical_read(
    calls: Vec<UsageCall>,
    snapshot_rows: Vec<sea_orm::QueryResult>,
    cancellation: &ScanCancellation,
) -> Result<CanonicalUsageSet, LocalError> {
    let cancellation = cancellation.clone();
    #[cfg(test)]
    let gate = super::snapshot_responsiveness_tests::cpu_gate(
        super::snapshot_responsiveness_tests::CpuStage::Canonical,
    );
    tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        if let Some(gate) = gate {
            gate.pause();
        }
        let check = || check_cancelled(&cancellation);
        check()?;
        let mut snapshots = Vec::with_capacity(snapshot_rows.len());
        for row in snapshot_rows {
            check()?;
            snapshots.push(super::snapshot::row_to_session_snapshot(&row)?);
        }
        let canonical = canonicalize_usage_calls_with_check(calls, check)?;
        attach_session_snapshots_with_check(canonical, snapshots, check)
    })
    .await
    .map_err(|_| worker_error())?
}

/// 由阻塞任务拥有完整规范集合，聚合与集合析构都不占用异步执行器。
pub(super) async fn aggregate_owned_canonical(
    canonical: CanonicalUsageSet,
    provider: ProviderKind,
    cancellation: &ScanCancellation,
) -> Result<LocalUsageAggregate, LocalError> {
    let cancellation = cancellation.clone();
    #[cfg(test)]
    let gate = super::snapshot_responsiveness_tests::cpu_gate(
        super::snapshot_responsiveness_tests::CpuStage::Aggregate,
    );
    tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        if let Some(gate) = gate {
            gate.pause();
        }
        let canonical =
            canonical.with_total_token_accounting(provider.local_total_token_accounting());
        aggregate_canonical_usage_with_check(&canonical, || check_cancelled(&cancellation))
    })
    .await
    .map_err(|_| worker_error())?
}

/// 阻塞任务异常不能泄露数据库位置或原始解析数据。
fn worker_error() -> LocalError {
    LocalError::new(LocalErrorKind::Database, "local index computation failed")
}

/// 页读取失败时也在拥有数据的阻塞任务中释放已积累调用，避免退出路径同步析构大集合。
pub(super) async fn discard_calls(calls: Vec<UsageCall>) {
    let _ = tokio::task::spawn_blocking(move || drop(calls)).await;
}
