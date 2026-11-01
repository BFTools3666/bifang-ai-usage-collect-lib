//! 定义初始化发现可见的物理 Agent 闭集与失败关闭校验。

use thiserror::Error;

use crate::{RootDiscoveryScope, SourceClientKind};

const CODEX_MASK: u8 = 1 << 0;
const CLAUDE_CODE_MASK: u8 = 1 << 1;
const GROK_BUILD_CLI_MASK: u8 = 1 << 2;
const ALL_PHYSICAL_AGENTS_MASK: u8 = CODEX_MASK | CLAUDE_CODE_MASK | GROK_BUILD_CLI_MASK;

/// 一次数据根发现允许触达并产出候选的物理 Agent 闭集。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootDiscoveryAgentScope {
    /// 按位表示允许发现的物理 Agent；位定义见本文件顶部掩码常量。
    mask: u8,
}

impl Default for RootDiscoveryAgentScope {
    /// 默认兼容普通数据源页的全部物理 Agent 闭集。
    fn default() -> Self {
        Self::all_physical()
    }
}

impl RootDiscoveryAgentScope {
    /// 返回包含全部物理扫描 Agent 的兼容发现闭集。
    pub const fn all_physical() -> Self {
        Self {
            mask: ALL_PHYSICAL_AGENTS_MASK,
        }
    }

    /// 构造只允许一个物理 Agent 的闭集；WorkBuddy 不属于发现客户端。
    pub const fn for_agent(client: SourceClientKind) -> Option<Self> {
        let mask = match client {
            SourceClientKind::Codex => CODEX_MASK,
            SourceClientKind::ClaudeCode => CLAUDE_CODE_MASK,
            SourceClientKind::GrokBuildCli => GROK_BUILD_CLI_MASK,
            SourceClientKind::WorkBuddy => return None,
        };
        Some(Self { mask })
    }

    /// 判断当前闭集是否允许指定物理 Agent；WorkBuddy 永远返回 false。
    pub const fn contains(self, client: SourceClientKind) -> bool {
        let mask = match client {
            SourceClientKind::Codex => CODEX_MASK,
            SourceClientKind::ClaudeCode => CLAUDE_CODE_MASK,
            SourceClientKind::GrokBuildCli => GROK_BUILD_CLI_MASK,
            SourceClientKind::WorkBuddy => return false,
        };
        self.mask & mask != 0
    }
}

/// 发现 Agent 过滤请求违反闭集边界时返回的稳定错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RootDiscoveryAgentScopeError {
    /// 调用方显式传入了空的初始化 Agent 集合。
    #[error("root discovery agent filter must not be empty")]
    EmptyFilter,
    /// 手动子树入口已经固定单个 Agent，不接受批量过滤。
    #[error("manual-subtree discovery does not accept an agent filter")]
    FilterNotAllowedForManualSubtree,
    /// WorkBuddy 不是物理扫描与数据根登记客户端。
    #[error("WorkBuddy is not a physical root-discovery agent")]
    WorkbuddyNotPhysical,
}

/// 把可选 IPC 过滤解析为闭集；省略过滤时兼容普通 Sources 的全客户端发现。
pub fn resolve_root_discovery_agent_scope(
    discovery_scope: RootDiscoveryScope,
    requested_agents: Option<&[SourceClientKind]>,
) -> Result<RootDiscoveryAgentScope, RootDiscoveryAgentScopeError> {
    let Some(requested_agents) = requested_agents else {
        return Ok(RootDiscoveryAgentScope::all_physical());
    };
    if requested_agents.is_empty() {
        return Err(RootDiscoveryAgentScopeError::EmptyFilter);
    }
    if discovery_scope == RootDiscoveryScope::ManualSubtree {
        return Err(RootDiscoveryAgentScopeError::FilterNotAllowedForManualSubtree);
    }
    let mut mask = 0_u8;
    for client in requested_agents {
        mask |= match client {
            SourceClientKind::Codex => CODEX_MASK,
            SourceClientKind::ClaudeCode => CLAUDE_CODE_MASK,
            SourceClientKind::GrokBuildCli => GROK_BUILD_CLI_MASK,
            SourceClientKind::WorkBuddy => {
                return Err(RootDiscoveryAgentScopeError::WorkbuddyNotPhysical);
            }
        };
    }
    Ok(RootDiscoveryAgentScope { mask })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 省略过滤保持旧 Sources 调用的全物理客户端行为。
    #[test]
    fn omitted_filter_resolves_all_physical_agents() {
        let agents = resolve_root_discovery_agent_scope(RootDiscoveryScope::UserPriority, None)
            .expect("omitted filter remains compatible");
        assert!(agents.contains(SourceClientKind::Codex));
        assert!(agents.contains(SourceClientKind::ClaudeCode));
        assert!(agents.contains(SourceClientKind::GrokBuildCli));
        assert!(!agents.contains(SourceClientKind::WorkBuddy));
    }

    /// 显式空集必须失败关闭，不能退化为扫描全部 Agent。
    #[test]
    fn explicit_empty_filter_is_rejected() {
        assert_eq!(
            resolve_root_discovery_agent_scope(RootDiscoveryScope::UserPriority, Some(&[])),
            Err(RootDiscoveryAgentScopeError::EmptyFilter)
        );
    }

    /// 手动子树由独立入口固定单个 Agent，不接受批量过滤参数。
    #[test]
    fn filtered_manual_subtree_discovery_is_rejected() {
        assert_eq!(
            resolve_root_discovery_agent_scope(
                RootDiscoveryScope::ManualSubtree,
                Some(&[SourceClientKind::Codex]),
            ),
            Err(RootDiscoveryAgentScopeError::FilterNotAllowedForManualSubtree)
        );
    }

    /// 全本地卷仍须服从当前页面选择的物理 Agent 闭集。
    #[test]
    fn full_local_volume_filter_remains_closed() {
        let agents = resolve_root_discovery_agent_scope(
            RootDiscoveryScope::FullLocalVolumes,
            Some(&[SourceClientKind::GrokBuildCli]),
        )
        .expect("full-volume discovery accepts a physical Agent filter");

        assert!(!agents.contains(SourceClientKind::Codex));
        assert!(!agents.contains(SourceClientKind::ClaudeCode));
        assert!(agents.contains(SourceClientKind::GrokBuildCli));
    }

    /// WorkBuddy 不得进入物理 Agent 发现闭集。
    #[test]
    fn workbuddy_filter_is_rejected() {
        assert_eq!(
            resolve_root_discovery_agent_scope(
                RootDiscoveryScope::UserPriority,
                Some(&[SourceClientKind::WorkBuddy]),
            ),
            Err(RootDiscoveryAgentScopeError::WorkbuddyNotPhysical)
        );
    }

    /// 单选过滤只开放请求的 Agent，重复项不扩大闭集。
    #[test]
    fn single_agent_filter_remains_closed() {
        let agents = resolve_root_discovery_agent_scope(
            RootDiscoveryScope::UserPriority,
            Some(&[SourceClientKind::ClaudeCode, SourceClientKind::ClaudeCode]),
        )
        .expect("single physical agent is valid");
        assert!(!agents.contains(SourceClientKind::Codex));
        assert!(agents.contains(SourceClientKind::ClaudeCode));
        assert!(!agents.contains(SourceClientKind::GrokBuildCli));
    }

    /// 混选闭集只包含显式去重后的物理 Agent。
    #[test]
    fn mixed_filter_does_not_expand_to_unselected_agents() {
        let agents = resolve_root_discovery_agent_scope(
            RootDiscoveryScope::UserPriority,
            Some(&[SourceClientKind::Codex, SourceClientKind::GrokBuildCli]),
        )
        .expect("mixed physical agents are valid");
        assert!(agents.contains(SourceClientKind::Codex));
        assert!(!agents.contains(SourceClientKind::ClaudeCode));
        assert!(agents.contains(SourceClientKind::GrokBuildCli));
    }
}
