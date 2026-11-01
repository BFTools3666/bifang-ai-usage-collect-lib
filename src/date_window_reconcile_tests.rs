//! 同一 canonical 快照下概览与统计日桶按查看时间标准对账。

use super::*;
use crate::{
    Confidence, CoverageReport, CoverageState, LocalIndexState, ProviderKind, SourceProvenance,
    TimeStandard, TokenUsage, UsageCall, UsageDimension, build_local_windows,
    build_local_windows_with_standard, build_usage_statistics,
    build_usage_statistics_with_standard, canonicalize_usage_calls,
};
use jiff::civil::Date;
use jiff::tz::TimeZone;

/// 构造系统本地时区下某一民用日期的代表时刻。
fn local_civil_ms(date: Date, hour: i8, minute: i8, second: i8) -> i64 {
    date.at(hour, minute, second, 0)
        .to_zoned(TimeZone::system())
        .expect("civil instant is representable")
        .timestamp()
        .as_millisecond()
}

/// 构造可进入三个装配入口的单一调用。
fn call(id: &str, occurred_at_epoch_ms: i64, total_tokens: u64) -> UsageCall {
    UsageCall {
        logical_call_id: id.to_owned(),
        occurred_at_epoch_ms,
        model: Some("model-a".to_owned()),
        reasoning_effort: Some("high".to_owned()),
        project_key: Some(format!("project-{id}")),
        project_label: None,
        thread_key: format!("thread-{id}"),
        thread_label: None,
        usage: TokenUsage::new(6, 2, Some(1), 4, 1, Some(total_tokens))
            .expect("fixture usage is valid"),
        adapter_consistency_key: None,
        confidence: Confidence::Exact,
        provenance: vec![SourceProvenance {
            source_id: format!("source-{id}"),
            root_id: "root".to_owned(),
            relative_label: "session".to_owned(),
            archived: false,
        }],
    }
}

/// 完整覆盖报告，避免质量字段干扰合计对账。
fn coverage() -> CoverageReport {
    CoverageReport {
        state: CoverageState::Complete,
        roots_scanned: 1,
        roots_discovered: 1,
        permission_denied_count: 0,
        skipped_count: 0,
        warning_count: 0,
    }
}

/// 从已交付统计页取出指定本地日桶合计。
fn bucket_total_tokens(page: &crate::UsageStatisticsPage, date: Date) -> u64 {
    page.daily_buckets
        .iter()
        .find(|bucket| bucket.local_date == date.to_string())
        .map(|bucket| bucket.measure.tokens.total_tokens)
        .unwrap_or(0)
}

/// 用不落在周一/月首的锚点覆盖全部六个日历窗口，确认概览按单次遍历缓存的
/// 民用日分类每个窗口都恰好收进该窗口应含的调用，且与各窗口独立走统计
/// 入口得到的日桶之和完全一致；月外调用必须被全部六个窗口拒之门外。
#[test]
fn overview_windows_reconcile_with_statistics_across_all_six_calendar_windows() {
    let today = Date::new(2026, 8, 19).expect("anchor date is a Wednesday");
    let observed = local_civil_ms(today, 15, 0, 0);

    let yesterday = Date::new(2026, 8, 18).expect("date is valid");
    let monday_this_week = Date::new(2026, 8, 17).expect("date is valid");
    let last_sunday = Date::new(2026, 8, 16).expect("date is valid");
    let last_monday = Date::new(2026, 8, 10).expect("date is valid");
    let month_start = Date::new(2026, 8, 1).expect("date is valid");
    let last_month_last = Date::new(2026, 7, 31).expect("date is valid");
    let last_month_first = Date::new(2026, 7, 1).expect("date is valid");
    let two_months_ago = Date::new(2026, 6, 15).expect("date is valid");

    let canonical = canonicalize_usage_calls(vec![
        call("today", local_civil_ms(today, 10, 0, 0), 101),
        call("yesterday", local_civil_ms(yesterday, 10, 0, 0), 202),
        call(
            "monday-this-week",
            local_civil_ms(monday_this_week, 10, 0, 0),
            303,
        ),
        call("last-sunday", local_civil_ms(last_sunday, 10, 0, 0), 404),
        call("last-monday", local_civil_ms(last_monday, 10, 0, 0), 505),
        call("month-start", local_civil_ms(month_start, 10, 0, 0), 606),
        call(
            "last-month-last",
            local_civil_ms(last_month_last, 10, 0, 0),
            707,
        ),
        call(
            "last-month-first",
            local_civil_ms(last_month_first, 10, 0, 0),
            808,
        ),
        call(
            "two-months-ago",
            local_civil_ms(two_months_ago, 10, 0, 0),
            909,
        ),
    ]);

    let overview = build_local_windows(
        &canonical,
        &coverage(),
        LocalIndexState::Ready,
        observed,
        ProviderKind::RolloutJsonl,
        None,
    )
    .expect("overview windows build");

    let window_total = |window: LocalUsageWindow| -> u64 {
        overview
            .windows
            .iter()
            .find(|candidate| candidate.window == window)
            .expect("window exists")
            .fact
            .value
            .tokens
            .total_tokens
    };

    assert_eq!(window_total(LocalUsageWindow::Today), 101);
    assert_eq!(window_total(LocalUsageWindow::Yesterday), 202);
    assert_eq!(window_total(LocalUsageWindow::ThisWeek), 101 + 202 + 303);
    assert_eq!(window_total(LocalUsageWindow::LastWeek), 404 + 505);
    assert_eq!(
        window_total(LocalUsageWindow::ThisMonth),
        101 + 202 + 303 + 404 + 505 + 606
    );
    assert_eq!(window_total(LocalUsageWindow::LastMonth), 707 + 808);

    for window in LocalUsageWindow::OVERVIEW_WINDOWS {
        let stats = build_usage_statistics(
            &canonical,
            &Default::default(),
            LocalIndexState::Ready,
            &coverage(),
            window,
            UsageDimension::Model,
            observed,
            ProviderKind::RolloutJsonl,
            None,
        )
        .expect("statistics build");
        let bucket_sum: u64 = stats
            .daily_buckets
            .iter()
            .map(|bucket| bucket.measure.tokens.total_tokens)
            .sum();
        assert_eq!(
            window_total(window),
            bucket_sum,
            "window {window:?} overview/statistics totals diverge"
        );
    }
}

/// 观测 15:00 之后的当地 23:59:59 仍进当天概览与用量日桶；UTC 23:59:59 只进 UTC 当天。
#[test]
fn civil_end_after_observation_counts_in_overview_and_statistics() {
    let device_tz = TimeZone::fixed(jiff::tz::offset(8));
    let today = Date::new(2026, 8, 19).expect("anchor date is valid");
    let observed = today
        .at(15, 0, 0, 0)
        .to_zoned(device_tz.clone())
        .expect("observed instant is representable")
        .timestamp()
        .as_millisecond();
    let local_end = today
        .at(23, 59, 59, 0)
        .to_zoned(device_tz.clone())
        .expect("local civil end is representable")
        .timestamp()
        .as_millisecond();
    let utc_end = today
        .at(23, 59, 59, 0)
        .to_zoned(TimeZone::UTC)
        .expect("UTC civil end is representable")
        .timestamp()
        .as_millisecond();
    assert!(local_end > observed);

    let local_canonical = canonicalize_usage_calls(vec![call("local-end", local_end, 41)]);
    let utc_canonical = canonicalize_usage_calls(vec![call("utc-end", utc_end, 53)]);

    let local_overview = build_local_windows_with_standard(
        &local_canonical,
        &coverage(),
        LocalIndexState::Ready,
        observed,
        ProviderKind::RolloutJsonl,
        None,
        TimeStandard::Local,
        &device_tz,
    )
    .expect("local overview builds");
    let local_today_tokens = local_overview
        .windows
        .iter()
        .find(|window| window.window == LocalUsageWindow::Today)
        .expect("local today exists")
        .fact
        .value
        .tokens
        .total_tokens;
    assert_eq!(local_today_tokens, 41);
    let local_stats = build_usage_statistics_with_standard(
        &local_canonical,
        &Default::default(),
        LocalIndexState::Ready,
        &coverage(),
        LocalUsageWindow::Today,
        UsageDimension::Model,
        observed,
        ProviderKind::RolloutJsonl,
        None,
        TimeStandard::Local,
        &device_tz,
    )
    .expect("local today statistics build");
    assert_eq!(bucket_total_tokens(&local_stats, today), 41);
    assert_eq!(local_stats.fact.value.tokens.total_tokens, 41);

    let utc_overview = build_local_windows_with_standard(
        &utc_canonical,
        &coverage(),
        LocalIndexState::Ready,
        observed,
        ProviderKind::RolloutJsonl,
        None,
        TimeStandard::utc(),
        &device_tz,
    )
    .expect("utc overview builds");
    let utc_today_tokens = utc_overview
        .windows
        .iter()
        .find(|window| window.window == LocalUsageWindow::Today)
        .expect("utc today exists")
        .fact
        .value
        .tokens
        .total_tokens;
    assert_eq!(utc_today_tokens, 53);
    let local_view_of_utc_end = build_local_windows_with_standard(
        &utc_canonical,
        &coverage(),
        LocalIndexState::Ready,
        observed,
        ProviderKind::RolloutJsonl,
        None,
        TimeStandard::Local,
        &device_tz,
    )
    .expect("local view of UTC end builds");
    let local_view_today = local_view_of_utc_end
        .windows
        .iter()
        .find(|window| window.window == LocalUsageWindow::Today)
        .expect("local today exists")
        .fact
        .value
        .tokens
        .total_tokens;
    assert_eq!(local_view_today, 0);
    let utc_stats = build_usage_statistics_with_standard(
        &utc_canonical,
        &Default::default(),
        LocalIndexState::Ready,
        &coverage(),
        LocalUsageWindow::Today,
        UsageDimension::Model,
        observed,
        ProviderKind::RolloutJsonl,
        None,
        TimeStandard::utc(),
        &device_tz,
    )
    .expect("utc today statistics build");
    assert_eq!(bucket_total_tokens(&utc_stats, today), 53);
    assert_eq!(utc_stats.fact.value.tokens.total_tokens, 53);
}
