//! 产品登记根、索引激活状态和一致快照。
use crate::{CanonicalUsageSet, CoverageState, DiscoveryMethod, LocalIndexState};
/// 数据根首次索引的激活状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootActivationState {
    /// 用户已确认登记，但尚未读取内容。
    ConfirmedUnindexed,
    /// 用户显式启动的首次或更新索引正在执行。
    Indexing,
    /// 已成功读到至少一条合法用量记录，可参与周期更新。
    Ready,
    /// 索引未读到合法记录或结构验证失败。
    ValidationFailed,
}

impl RootActivationState {
    /// 返回数据库使用的稳定标签。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConfirmedUnindexed => "confirmed_unindexed",
            Self::Indexing => "indexing",
            Self::Ready => "ready",
            Self::ValidationFailed => "validation_failed",
        }
    }

    /// 从数据库标签恢复状态，未知标签保守拒绝。
    pub fn from_label(value: &str) -> Option<Self> {
        match value {
            "confirmed_unindexed" => Some(Self::ConfirmedUnindexed),
            "indexing" => Some(Self::Indexing),
            "ready" => Some(Self::Ready),
            "validation_failed" => Some(Self::ValidationFailed),
            _ => None,
        }
    }
}

/// 表示 registry 中一个可展示的数据根，不返回其绝对访问路径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootRecord {
    /// adapter 根据访问位置生成的稳定根 ID。
    pub root_id: String,
    /// 用户别名或安全默认名称。
    pub alias: String,
    /// 标识该根当前是否参与扫描。
    pub enabled: bool,
    /// 首次确认、索引中、可周期更新或验证失败的独立状态。
    pub activation_state: RootActivationState,
    /// 标识该根是否为 Codex 唯一主数据目录。
    pub is_primary: bool,
    /// 根的发现或登记方式。
    pub discovery_method: DiscoveryMethod,
    /// 最近一次扫描对该根的覆盖结论。
    pub last_coverage: Option<CoverageState>,
    /// 当前索引仍保留 checkpoint 的来源文件数量。
    pub source_file_count: u64,
    /// 当前 generation 中可追溯的调用观察数量，canonical 去重在 core 另行完成。
    pub call_observation_count: u64,
}

/// 把 core 覆盖状态转换为数据库稳定标签。
pub const fn coverage_state_label(state: CoverageState) -> &'static str {
    match state {
        CoverageState::Complete => "complete",
        CoverageState::Partial => "partial",
        CoverageState::Cancelled => "cancelled",
        CoverageState::Failed => "failed",
    }
}

/// 从当前 schema 的稳定标签恢复 core 覆盖状态。
pub fn coverage_state_from_label(value: &str) -> Option<CoverageState> {
    match value {
        "complete" => Some(CoverageState::Complete),
        "partial" => Some(CoverageState::Partial),
        "cancelled" => Some(CoverageState::Cancelled),
        "failed" => Some(CoverageState::Failed),
        _ => None,
    }
}
/// 保存同一个只读快照中取得的 canonical 调用与安全数据根记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageSnapshot {
    /// 当前 generation 的调用观察经 core 去重后的集合。
    pub canonical: CanonicalUsageSet,
    /// 从扫描与 parser 来源事实推导出的本机索引状态。
    pub index_state: LocalIndexState,
    /// 与调用读取处于同一存储快照的数据根安全记录。
    pub roots: Vec<RootRecord>,
}
