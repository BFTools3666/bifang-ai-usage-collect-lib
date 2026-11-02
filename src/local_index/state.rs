//! 从持久统计事实推导索引状态。
use serde::{Deserialize, Serialize};
/// 区分本机索引尚未建立、需要重扫、已扫描空结果与可用调用，避免把采集缺失误作真实零值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LocalIndexState {
    /// 本产品没有任何扫描运行或当前来源，不能把空聚合解释为真实零用量。
    NotScanned,
    /// 已有来源只属于旧 parser generation，必须重建后才能进入当前统计。
    NeedsRescan,
    /// 至少执行过一次扫描，但当前 parser generation 没有可用 canonical 调用。
    ReadyNoCalls,
    /// 当前 parser generation 已包含可参与统计的 canonical 调用。
    Ready,
}

/// 从持久层读取的安全计数事实推导本机索引四态。
pub const fn local_index_state_from_facts(
    completed_scan_count: u64,
    current_source_count: u64,
    stale_source_count: u64,
    enabled_unscanned_root_count: u64,
    enabled_root_count: u64,
    canonical_call_count: usize,
) -> LocalIndexState {
    if enabled_root_count == 0 {
        LocalIndexState::NotScanned
    } else if stale_source_count > 0 {
        LocalIndexState::NeedsRescan
    } else if canonical_call_count > 0 {
        LocalIndexState::Ready
    } else if enabled_unscanned_root_count > 0
        || (completed_scan_count == 0 && current_source_count == 0)
    {
        LocalIndexState::NotScanned
    } else {
        LocalIndexState::ReadyNoCalls
    }
}

#[cfg(test)]
mod local_index_state_tests {
    use super::*;

    /// 旧 parser、可信调用、未扫根和真实空结果按保守优先级解析。
    #[test]
    fn derives_local_index_state_from_storage_facts() {
        assert_eq!(
            local_index_state_from_facts(1, 1, 1, 0, 1, 2),
            LocalIndexState::NeedsRescan
        );
        assert_eq!(
            local_index_state_from_facts(1, 1, 0, 0, 1, 2),
            LocalIndexState::Ready
        );
        assert_eq!(
            local_index_state_from_facts(1, 1, 0, 1, 1, 0),
            LocalIndexState::NotScanned
        );
        assert_eq!(
            local_index_state_from_facts(1, 1, 0, 0, 1, 0),
            LocalIndexState::ReadyNoCalls
        );
        assert_eq!(
            local_index_state_from_facts(1, 1, 0, 0, 0, 9),
            LocalIndexState::NotScanned
        );
    }
}
