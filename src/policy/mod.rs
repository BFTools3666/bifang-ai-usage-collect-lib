//! 扫描来源、只读标签与覆盖的共享规则。
use crate::{Completeness, CoverageReport, CoverageState};
use thiserror::Error;
/// 数据源发现任务固定最多使用两个目录 I/O worker。
pub const LOCAL_DISCOVERY_WORKER_LIMIT: usize = 2;

/// 表示源根来源类型，用于稳定的标识前缀校验。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SourceClientKind {
    /// Codex 稳定标识前缀 `root-`。
    Codex,
    /// Claude Code 稳定标识前缀 `claude-root-`。
    ClaudeCode,
    /// Grok Build CLI 稳定标识前缀 `grok-root-`。
    GrokBuildCli,
    /// WorkBuddy 稳定标识前缀 `workbuddy-root-`。
    WorkBuddy,
}

impl SourceClientKind {
    /// 返回来源根 ID 的稳定前缀。
    fn stable_root_id_prefix(self) -> &'static str {
        match self {
            Self::Codex => "root-",
            Self::ClaudeCode => "claude-root-",
            Self::GrokBuildCli => "grok-root-",
            Self::WorkBuddy => "workbuddy-root-",
        }
    }

    /// 返回登记稳定根 ID 时交给 `stable_id` 的命名空间。
    pub const fn root_id_namespace(self) -> &'static str {
        match self {
            Self::Codex => "root",
            Self::ClaudeCode => "claude-root",
            Self::GrokBuildCli => "grok-root",
            Self::WorkBuddy => "workbuddy-root",
        }
    }

    /// 返回未确认候选根 `stable_id` 使用的命名空间。
    pub const fn candidate_namespace(self) -> &'static str {
        match self {
            Self::Codex => "candidate-codex",
            Self::ClaudeCode => "candidate-claude",
            Self::GrokBuildCli => "candidate-grok",
            Self::WorkBuddy => "candidate-workbuddy",
        }
    }

    /// 返回默认来源根展示名称。
    pub const fn default_source_root_alias(self) -> &'static str {
        match self {
            Self::Codex => "Codex 数据根",
            Self::ClaudeCode => "Claude Code 数据根",
            Self::GrokBuildCli => "Grok 数据根",
            Self::WorkBuddy => "WorkBuddy 数据根",
        }
    }

    /// 返回在来源根列表中展示的环境标签。
    pub const fn source_environment_label(self) -> &'static str {
        match self {
            Self::Codex => "当前 CODEX_HOME",
            Self::ClaudeCode => "当前 CLAUDE_CONFIG_DIR",
            Self::GrokBuildCli => "当前 GROK_HOME",
            Self::WorkBuddy => "当前 WORKBUDDY_HOME",
        }
    }

    /// 返回客户端当前 parser generation（用于跨 CLI/MCP 复用一致的解析边界）。
    ///
    /// 这是读写唯一来源：GUI JSONL writer 的 generation 常量必须等于本值，
    /// 概览/统计/调用/Collect 打开索引也必须用本值。writer 单独加一代而
    /// 此处未加，会导致重建后调用已入库、读取仍按旧代当成空索引。
    pub const fn parser_version(self) -> u32 {
        match self {
            Self::Codex => 10,
            Self::ClaudeCode => 6,
            Self::GrokBuildCli => 5,
            Self::WorkBuddy => 1,
        }
    }
}

/// 表示源根 ID 规则校验失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SourceRootIdValidationError {
    /// 不是预期前缀或长度/字符规则的稳定标识。
    #[error("invalid source root id")]
    Invalid,
}

/// 校验来源根标识：必须是对应客户端固定前缀 + 16 位十六进制。
pub fn validate_source_root_id(
    client: SourceClientKind,
    root_id: &str,
) -> Result<(), SourceRootIdValidationError> {
    let suffix = root_id
        .strip_prefix(client.stable_root_id_prefix())
        .unwrap_or_default();
    if suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(SourceRootIdValidationError::Invalid)
    }
}

/// 表示筛选字段 ID 校验失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum UsageFilterIdValidationError {
    /// 值为空、过长或包含非法字符。
    #[error("invalid usage filter id")]
    Invalid,
}

/// 校验固定格式的调用筛选 ID（ASCII 技术标签或内部匿名 ID）。
pub fn is_safe_usage_filter_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 160
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
}

/// 校验调用筛选 ID，返回稳定错误类型供适配层做文案映射。
pub fn validate_usage_filter_id(value: &str) -> Result<(), UsageFilterIdValidationError> {
    if is_safe_usage_filter_id(value) {
        Ok(())
    } else {
        Err(UsageFilterIdValidationError::Invalid)
    }
}

/// 在展示路径中允许直接透传的技术标签。
pub fn safe_technical_label(value: &str) -> Option<String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
    {
        return None;
    }
    Some(value.to_owned())
}

/// 在展示路径中允许直接透传的数据根别名。
pub fn safe_root_label(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty()
        || value.chars().count() > 64
        || value.chars().any(char::is_control)
        || value.contains('/')
        || value.contains('\\')
    {
        None
    } else {
        Some(value.to_owned())
    }
}

/// 在分页或列表显示上保留内部标识短尾，避免完整 ID 外泄。
pub fn safe_short_value(key: &str) -> String {
    key.chars()
        .rev()
        .take(8)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

/// 将覆盖覆盖状态映射为展示完整度：仅完整扫描为 Complete，其余为 Partial。
pub const fn coverage_completeness(state: CoverageState) -> Completeness {
    match state {
        CoverageState::Complete => Completeness::Complete,
        CoverageState::Partial | CoverageState::Cancelled | CoverageState::Failed => {
            Completeness::Partial
        }
    }
}

/// 合并已确认/全设备发现覆盖报告与扫描覆盖报告。
pub fn merge_coverage_reports(discovery: CoverageReport, scan: &CoverageReport) -> CoverageReport {
    let state = if discovery.state == CoverageState::Cancelled
        || scan.state == CoverageState::Cancelled
    {
        CoverageState::Cancelled
    } else if discovery.state == CoverageState::Failed || scan.state == CoverageState::Failed {
        CoverageState::Failed
    } else if discovery.state == CoverageState::Partial || scan.state == CoverageState::Partial {
        CoverageState::Partial
    } else {
        CoverageState::Complete
    };
    CoverageReport {
        state,
        roots_scanned: discovery.roots_scanned.max(scan.roots_scanned),
        roots_discovered: discovery.roots_discovered,
        permission_denied_count: discovery
            .permission_denied_count
            .saturating_add(scan.permission_denied_count),
        skipped_count: discovery.skipped_count.saturating_add(scan.skipped_count),
        warning_count: discovery.warning_count.saturating_add(scan.warning_count),
    }
}

/// 返回清空索引后不声称已完整覆盖的默认覆盖报告。
pub fn empty_coverage() -> CoverageReport {
    CoverageReport {
        state: CoverageState::Partial,
        roots_scanned: 0,
        roots_discovered: 0,
        permission_denied_count: 0,
        skipped_count: 0,
        warning_count: 0,
    }
}

/// 返回启动时可复用的初始覆盖报告。
pub fn initial_coverage() -> CoverageReport {
    empty_coverage()
}

mod claude_desktop_agent_home;
pub use claude_desktop_agent_home::{
    CLAUDE_DESKTOP_AGENT_HOME_DIR_NAME, CLAUDE_DESKTOP_AGENT_SESSION_DIR_PREFIX,
    CLAUDE_DESKTOP_AGENT_SESSIONS_DIR_NAME, CLAUDE_DESKTOP_APP_DIR_NAME,
    claude_desktop_agent_home_alias, claude_desktop_agent_home_alias_from_path,
    claude_desktop_agent_home_dir, claude_desktop_agent_sessions_dir,
    is_claude_desktop_agent_session_dir_name,
};
mod source_root_alias;
pub use source_root_alias::source_root_alias_from_path;
mod source_visit;
pub use source_visit::{SourceFileObservation, retain_ingestable_calls, source_file_needs_visit};
mod workbuddy_scan_source;
pub use workbuddy_scan_source::{
    WORKBUDDY_HOME_DIR_NAME, WORKBUDDY_INTERNATIONAL_HOME_DIR_NAME, WORKBUDDY_PROJECTS_DIR_NAME,
    WorkbuddyScanSource, discover_workbuddy_scan_source, discover_workbuddy_scan_sources,
    workbuddy_home_from_user_home, workbuddy_homes_from_user_home, workbuddy_scan_source_from_path,
};
