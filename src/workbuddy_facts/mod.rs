//! WorkBuddy 用量记录、安全规范化与来源覆盖事实。
pub mod record;
use crate::{CoverageReport, CoverageState};
pub use record::{WorkbuddyUsageEventRecord, WorkbuddyUsageOrigin, WorkbuddyUsageQuality};
use serde::{Deserialize, Serialize};
/// Trace 整体状态；只作为独立健康诊断，不参与 Token 或模型统计。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkbuddyTraceStatus {
    /// trace 内全部 span 正常完成。
    Ok,
    /// trace 内出现未捕获错误。
    Error,
    /// trace 被取消。
    Cancelled,
}

/// 单条 trace 的最小诊断字段。
#[derive(Debug, Clone, PartialEq)]
pub struct WorkbuddyTraceRecord {
    /// trace 起始时刻的 Unix 毫秒时间戳。
    pub started_at_epoch_ms: i64,
    /// trace 总耗时，单位毫秒。
    pub duration_ms: i64,
    /// trace 整体状态。
    pub status: WorkbuddyTraceStatus,
}

/// 合并并列的国内版与国际版覆盖报告；状态取最保守值，所有计数按根累加。
pub fn merge_workbuddy_coverage_reports(reports: &[CoverageReport]) -> Option<CoverageReport> {
    let first = reports.first()?.clone();
    Some(reports.iter().skip(1).fold(first, |mut merged, report| {
        merged.state = match (merged.state, report.state) {
            (CoverageState::Failed, _) | (_, CoverageState::Failed) => CoverageState::Failed,
            (CoverageState::Cancelled, _) | (_, CoverageState::Cancelled) => {
                CoverageState::Cancelled
            }
            (CoverageState::Partial, _) | (_, CoverageState::Partial) => CoverageState::Partial,
            (CoverageState::Complete, CoverageState::Complete) => CoverageState::Complete,
        };
        merged.roots_scanned = merged.roots_scanned.saturating_add(report.roots_scanned);
        merged.roots_discovered = merged
            .roots_discovered
            .saturating_add(report.roots_discovered);
        merged.permission_denied_count = merged
            .permission_denied_count
            .saturating_add(report.permission_denied_count);
        merged.skipped_count = merged.skipped_count.saturating_add(report.skipped_count);
        merged.warning_count = merged.warning_count.saturating_add(report.warning_count);
        merged
    }))
}
