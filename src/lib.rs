//! AI 用量的只读扫描、解析、规范化与统计；持久化由调用客户端负责。
#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

mod aggregate; // 用量数据的规范化、去重与聚合逻辑
pub use aggregate::{
    CanonicalUsageSet, CanonicalizationWarning, CanonicalizationWarningKind, MetricFusionError,
    aggregate_canonical_usage, attach_session_snapshots, canonicalize_session_snapshots,
    canonicalize_usage_calls, filter_canonical_usage, partition_canonical_usage_two,
    prefer_metric_within_scope,
};
mod calls_view; // 调用列表装配：过滤、排序、分页、游标
pub use calls_view::{
    BuildUsageCallRowsError, USAGE_CALL_PAGE_SIZE, UsageAvailableFilters, UsageCallFilters,
    UsageCallItem, UsageCallRow, UsageCallSortDirection, UsageCallSortField, UsageCallsPage,
    UsageCallsQuery, UsageFilterOption, build_usage_calls_page, cursor_error, cursor_start,
    decode_cursor, encode_cursor, fingerprint_query, fingerprint_snapshot,
};
mod chart_view; // 图表页的 24 小时/多日时间桶与固定维度分布
pub use chart_view::{
    UsageChartBucket, UsageChartDimension, UsageChartGranularity, UsageChartPage,
    build_usage_chart_with_standard,
};
mod combined_view; // 已开启 Agent 的跨库只读概览与调用联合视图
pub use combined_view::{
    AgentUsageSnapshot, CombinedUsageSnapshot, PendingAgentMember, UsageViewError, UsageViewKind,
    build_combined_local_windows_with_standard, build_combined_usage_calls_page,
    build_combined_usage_chart_with_standard, combine_agent_usage_snapshots,
    resolve_usage_view_members,
};
mod discovery_agent_scope; // 初始化发现允许触达的物理 Agent 闭集
pub use discovery_agent_scope::{
    RootDiscoveryAgentScope, RootDiscoveryAgentScopeError, resolve_root_discovery_agent_scope,
};
mod display_label; // 跨端显示标签语义码
pub use display_label::DisplayLabelCode;
mod display_name; // 受控项目末段与短线程标题
pub use display_name::{
    SAFE_DISPLAY_NAME_MAX_CHARS, claude_project_display_label, grok_project_display_label,
    safe_path_basename, safe_thread_title,
};
mod local_view; // 本机窗口/来源摘要的纯业务装配
pub use local_view::{
    LocalRecordsSummary, SourceDiscoveryCode, SourceDiscoveryMethod, SourceRootInput,
    SourceRootSummary, WindowUsage, build_empty_local_windows, build_local_windows,
    build_local_windows_with_standard, build_source_roots,
};
mod provider; // 本机 provider、scope 与带质量元数据的事实信封
pub use provider::{
    Completeness, Confidence, Freshness, MetricFact, MetricScope, ProviderKind,
    empty_token_usage_for_provider,
};
mod root_discovery; // 元数据优先的数据根发现任务、候选与激活状态
pub use root_discovery::{
    RootActivationState, RootCandidate, RootCandidateEvidence, RootCandidateSelectionError,
    RootDiscoveryCoordinator, RootDiscoveryLifecycle, RootDiscoveryPlatform, RootDiscoveryProgress,
    RootDiscoveryScope, RootDiscoveryStatus, RootDiscoveryStrategy,
};
mod root_discovery_priority; // 快速发现的平台优先相对目录表
pub use root_discovery_priority::root_discovery_priority_relative_roots;
mod source; // 本机数据源、覆盖、Call 模型与聚合
pub use source::{
    CoverageReport, CoverageState, LocalIndexState, LocalUsageAggregate, SessionTokenSnapshot,
    SourceProvenance, UsageCall, empty_local_usage_aggregate_for_provider,
    immediate_reindex_required, local_fact_quality, local_index_state_from_facts,
};
mod statistics; // 按维度分组统计用量的纯函数与结果结构
pub use statistics::{
    BoundedUsageGroups, STATISTICS_GROUP_LIMIT, UsageDimension, UsageGroup, UsageMeasure,
    group_usage, stable_group_id, summarize_usage,
};
mod statistics_view; // 统计页聚合、对账与可见标签映射
pub use statistics_view::{
    UsageDailyBucket, UsageGroupDisplay, UsageStatisticsPage, build_usage_statistics,
    build_usage_statistics_with_standard,
};
mod time_standard; // 查看用当地时间/UTC 自然日标准
pub use time_standard::{
    TimeStandard, TimeStandardMode, device_time_zone_name, is_known_iana_time_zone,
};
mod timeline; // 本机窗口边界与时间戳窗口辅助
pub use timeline::{
    LocalUsageWindow, TimelineError, WindowBoundaries, belongs_to_local_window, belongs_to_window,
    civil_at, civil_date_for_timestamp, dates_for_day_count, dates_for_window, day_start_epoch_ms,
    filter_canonical_usage_for_date, filter_canonical_usage_for_dates,
    filter_canonical_usage_for_local_date, filter_canonical_usage_for_local_dates,
    filter_canonical_usage_for_local_window, filter_canonical_usage_for_window,
    first_representable_in_date, first_representable_local_instant, inclusive_calendar_range,
    local_date_for_timestamp, local_dates_for_day_count, local_dates_for_window,
    local_day_start_epoch_ms, occurred_on_dates, occurred_on_local_dates,
    retention_cutoff_epoch_ms,
};
mod usage; // 单次调用 Token 用量的模型与校验
pub use usage::{
    IncrementalTokenUsageDecision, SelectedTokenUsage, TokenUsage, TokenUsageError,
    TotalTokenAccounting, select_incremental_call_usage, select_single_call_usage,
};
mod workbuddy_stats; // WorkBuddy 本地用量统计的纯聚合：逐请求 JSONL 事件 -> 快照
pub use workbuddy_stats::{
    WorkbuddyDailyBucket, WorkbuddyHourlyBucket, WorkbuddyHourlyTrend, WorkbuddyModelUsageGroup,
    WorkbuddyModelUsageWindow, WorkbuddyStatisticsSnapshot, WorkbuddyTraceRecord,
    WorkbuddyTraceStatus, WorkbuddyUsageEventRecord, WorkbuddyUsageOrigin, WorkbuddyUsageQuality,
    WorkbuddyWindowAggregate, build_workbuddy_only_local_windows, build_workbuddy_usage_details,
    complete_workbuddy_coverage, compute_workbuddy_statistics_with_standard,
    mark_workbuddy_unavailable_in_local_windows, merge_workbuddy_coverage_reports,
    merge_workbuddy_into_local_windows, workbuddy_combined_usage_unavailable_message,
    workbuddy_daily_token_totals,
};
mod enabled_agents;
mod retention_days;
pub use enabled_agents::{
    EnabledAgents, EnabledAgentsError, agent_wire_label, enabled_agents_error_message,
    parse_single_agent_label,
};
pub use retention_days::{
    DEFAULT_RETENTION_DAYS, MAX_RETENTION_DAYS, MIN_RETENTION_DAYS, RetentionDays,
    RetentionDaysError, resolve_retention_days,
};
mod policy;
pub use policy::{
    CLAUDE_DESKTOP_AGENT_HOME_DIR_NAME, CLAUDE_DESKTOP_AGENT_SESSION_DIR_PREFIX,
    CLAUDE_DESKTOP_AGENT_SESSIONS_DIR_NAME, CLAUDE_DESKTOP_APP_DIR_NAME,
    LOCAL_DISCOVERY_WORKER_LIMIT, SourceClientKind, SourceFileObservation,
    SourceRootIdValidationError, UsageFilterIdValidationError, WORKBUDDY_HOME_DIR_NAME,
    WORKBUDDY_INTERNATIONAL_HOME_DIR_NAME, WORKBUDDY_PROJECTS_DIR_NAME, WorkbuddyScanSource,
    claude_desktop_agent_home_alias, claude_desktop_agent_home_alias_from_path,
    claude_desktop_agent_home_dir, claude_desktop_agent_sessions_dir, coverage_completeness,
    discover_workbuddy_scan_source, discover_workbuddy_scan_sources, empty_coverage,
    initial_coverage, is_claude_desktop_agent_session_dir_name, is_safe_usage_filter_id,
    merge_coverage_reports, retain_ingestable_calls, safe_root_label, safe_short_value,
    safe_technical_label, source_file_needs_visit, source_root_alias_from_path,
    validate_source_root_id, validate_usage_filter_id, workbuddy_home_from_user_home,
    workbuddy_homes_from_user_home, workbuddy_scan_source_from_path,
};
mod cancellation;
pub use cancellation::ScanCancellation;
mod local_index;
pub use local_index::{
    DiscoveryMethod, LocalError, LocalErrorKind, RegisteredRoot, RootRecord, SourceParseCheckpoint,
    UsageSnapshot, coverage_state_from_label, coverage_state_label, path_key, stable_id,
};

/// 本机文件的只读发现、枚举和解析接口。
pub mod readers;

mod discovery_policy;
pub use discovery_policy::{
    append_platform_discovery_excludes, append_platform_discovery_excludes_for_volumes,
    is_discovery_path_excluded, is_full_discovery_skip_directory_name,
    path_has_full_discovery_skip_directory_name,
};
