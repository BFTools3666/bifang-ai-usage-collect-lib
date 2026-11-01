//! AI 用量的只读扫描、解析、规范化与统计；持久化由调用客户端负责。
#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

mod aggregate; // 用量数据的规范化、去重与聚合逻辑
pub use aggregate::{
    CanonicalUsageSet, CanonicalizationWarning, CanonicalizationWarningKind, MetricFusionError,
    aggregate_canonical_usage, attach_session_snapshots, canonicalize_session_snapshots,
    canonicalize_usage_calls, copy_matching_snapshots, filter_canonical_usage,
    partition_canonical_usage_two, prefer_metric_within_scope,
};
mod discovery_agent_scope; // 初始化发现允许触达的物理 Agent 闭集
pub use discovery_agent_scope::{
    RootDiscoveryAgentScope, RootDiscoveryAgentScopeError, resolve_root_discovery_agent_scope,
};
mod display_name; // 受控项目末段与短线程标题
pub use display_name::{
    SAFE_DISPLAY_NAME_MAX_CHARS, claude_project_display_label, grok_project_display_label,
    safe_path_basename, safe_thread_title,
};
mod provider; // 本机 provider、scope 与带质量元数据的事实信封
pub use provider::{
    Completeness, Confidence, Freshness, MetricFact, MetricScope, ProviderKind,
    empty_token_usage_for_provider,
};
mod root_discovery; // 元数据优先的数据根发现任务、候选与激活状态
pub use root_discovery::{
    RootCandidate, RootCandidateEvidence, RootCandidateSelectionError, RootDiscoveryCoordinator,
    RootDiscoveryLifecycle, RootDiscoveryPlatform, RootDiscoveryProgress, RootDiscoveryScope,
    RootDiscoveryStatus, RootDiscoveryStrategy,
};
mod root_discovery_priority; // 快速发现的平台优先相对目录表
pub use root_discovery_priority::root_discovery_priority_relative_roots;
mod source; // 本机数据源、覆盖、Call 模型与聚合
pub use source::{
    CoverageReport, CoverageState, LocalUsageAggregate, SessionTokenSnapshot, SourceProvenance,
    UsageCall, empty_local_usage_aggregate_for_provider,
};
mod statistics; // 按维度分组统计用量的纯函数与结果结构
pub use statistics::{
    BoundedUsageGroups, UsageDimension, UsageGroup, UsageMeasure, group_usage, measure_calls,
    stable_group_id, summarize_usage,
};
mod time_standard; // 查看用当地时间/UTC 自然日标准
pub use time_standard::{
    TimeStandard, TimeStandardMode, device_time_zone_name, is_known_iana_time_zone,
};
mod timeline; // 本机窗口边界与时间戳窗口辅助
pub use timeline::{
    LocalUsageWindow, TimelineError, WindowBoundaries, belongs_to_local_window, belongs_to_window,
    calendar_cutoff_epoch_ms, civil_at, civil_date_for_timestamp, dates_for_day_count,
    dates_for_window, day_start_epoch_ms, filter_canonical_usage_for_date,
    filter_canonical_usage_for_dates, filter_canonical_usage_for_local_date,
    filter_canonical_usage_for_local_dates, filter_canonical_usage_for_local_window,
    filter_canonical_usage_for_window, first_representable_in_date,
    first_representable_local_instant, inclusive_calendar_range, local_date_for_timestamp,
    local_dates_for_day_count, local_dates_for_window, local_day_start_epoch_ms, occurred_on_dates,
    occurred_on_local_dates,
};
mod usage; // 单次调用 Token 用量的模型与校验
pub use usage::{
    IncrementalTokenUsageDecision, SelectedTokenUsage, TokenUsage, TokenUsageError,
    TotalTokenAccounting, select_incremental_call_usage, select_single_call_usage,
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
    merge_coverage_reports, safe_root_label, safe_short_value, safe_technical_label,
    source_file_needs_visit, source_root_alias_from_path, validate_source_root_id,
    validate_usage_filter_id, workbuddy_home_from_user_home, workbuddy_homes_from_user_home,
    workbuddy_scan_source_from_path,
};
mod cancellation;
pub use cancellation::{CancellationSignal, ScanCancellation};
mod local_index;
pub use local_index::{
    DiscoveryMethod, LocalError, LocalErrorKind, RegisteredRoot, SourceParseCheckpoint, path_key,
    stable_id,
};

/// 本机文件的只读发现、枚举和解析接口。
pub mod readers;

mod discovery_policy;
pub use discovery_policy::{
    append_platform_discovery_excludes, append_platform_discovery_excludes_for_volumes,
    is_discovery_path_excluded, is_full_discovery_skip_directory_name,
    path_has_full_discovery_skip_directory_name,
};

/// WorkBuddy 的来源记录与通用校验。
pub mod workbuddy_facts;
pub use workbuddy_facts::{
    WorkbuddyTraceRecord, WorkbuddyTraceStatus, WorkbuddyUsageEventRecord, WorkbuddyUsageOrigin,
    WorkbuddyUsageQuality, merge_workbuddy_coverage_reports,
};

/// 可扩展的来源解析与观测前缀读取协议。
pub mod parsing;

/// 经路径验证的只读文件打开与来源观察。
pub mod input;
