//! 把 WorkBuddy project JSONL 逐请求事实转成 canonical 统计页。

use std::collections::BTreeMap;

use jiff::tz::TimeZone;

use super::record::ValidatedWorkbuddyUsageRecord;
use crate::{
    Confidence, CoverageReport, LocalIndexState, LocalUsageWindow, ProviderKind, SourceProvenance,
    TimeStandard, UsageCall, UsageDimension, UsageStatisticsPage,
    build_usage_statistics_with_standard, canonicalize_usage_calls,
};

/// 把已校验、去重的 WorkBuddy usage 事件转成 canonical 调用。
/// 校验层已保证每个事件只含一个请求，因此容量只随记录条数增长。
fn workbuddy_usage_calls(
    records: &[ValidatedWorkbuddyUsageRecord],
    root_aliases: &BTreeMap<String, String>,
) -> Result<Vec<UsageCall>, String> {
    let mut calls = Vec::with_capacity(records.len());
    for record in records {
        let root_alias = root_aliases
            .get(&record.root_id)
            .ok_or_else(|| "WorkBuddy 记录缺少对应固定根身份".to_owned())?;
        let relative_label = format!("{root_alias}/projects JSONL");
        calls.push(UsageCall {
            logical_call_id: record.logical_call_id.clone(),
            occurred_at_epoch_ms: record.occurred_at_epoch_ms,
            model: record.model.clone(),
            reasoning_effort: None,
            project_key: record.project_key.clone(),
            project_label: record.project_label.clone(),
            thread_key: record.session_key.clone(),
            thread_label: None,
            usage: record.usage.clone(),
            adapter_consistency_key: Some(record.logical_call_id.clone()),
            confidence: Confidence::Exact,
            provenance: record
                .source_ids
                .iter()
                .map(|source_id| SourceProvenance {
                    source_id: source_id.clone(),
                    root_id: record.root_id.clone(),
                    relative_label: relative_label.clone(),
                    archived: false,
                })
                .collect(),
        });
    }
    Ok(calls)
}

/// 按窗口与维度装配 WorkBuddy 用量统计页，复用本机 Agent 的日桶与 Top-N 分组。
#[allow(clippy::too_many_arguments)]
pub(super) fn build_workbuddy_usage_statistics(
    records: &[ValidatedWorkbuddyUsageRecord],
    coverage: &CoverageReport,
    root_aliases: &BTreeMap<String, String>,
    window: LocalUsageWindow,
    dimension: UsageDimension,
    observed_at_epoch_ms: i64,
    time_standard: &TimeStandard,
    device_tz: &TimeZone,
) -> Result<UsageStatisticsPage, String> {
    let calls = workbuddy_usage_calls(records, root_aliases)?;
    let index_state = if calls.is_empty() {
        LocalIndexState::ReadyNoCalls
    } else {
        LocalIndexState::Ready
    };
    let canonical = canonicalize_usage_calls(calls);
    let source_label = ProviderKind::WorkbuddyProjectJsonl
        .parser_source_label(crate::SourceClientKind::WorkBuddy.parser_version());
    build_usage_statistics_with_standard(
        &canonical,
        root_aliases,
        index_state,
        coverage,
        window,
        dimension,
        observed_at_epoch_ms,
        ProviderKind::WorkbuddyProjectJsonl,
        Some(source_label.as_str()),
        time_standard.clone(),
        device_tz,
    )
}
