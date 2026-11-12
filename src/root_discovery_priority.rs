//! 快速发现的平台优先相对目录表；纯策略，可在任意宿主上验证全部平台。

use crate::{RootDiscoveryAgentScope, SourceClientKind};

/// 一条优先目录登记：相对 OS 用户根的路径，以及它专属的 Agent。
///
/// `None` 表示 Codex 与 Claude Code 共用的通用应用数据目录，只要两者之一已开放就加入。
type PriorityEntry = (&'static str, Option<SourceClientKind>);

/// Windows 用户根下的优先目录，顺序即入队顺序。
const WINDOWS_PRIORITY_ROOTS: &[PriorityEntry] = &[
    ("AppData/Roaming/AIManagerData", None),
    (".claude", Some(SourceClientKind::ClaudeCode)),
    (".codex", Some(SourceClientKind::Codex)),
    ("AppData/Roaming", None),
    ("AppData/Local", None),
];

/// macOS 用户根下的优先目录，顺序即入队顺序。
///
/// 不登记整个 `~/Library`：其下 Developer、Caches、Containers、iCloud 等子树常达
/// 数十万目录却不存放已知会话数据，快速扫描遍历它会持续数分钟，初始化看似卡住。
const MACOS_PRIORITY_ROOTS: &[PriorityEntry] = &[
    ("Library/Application Support/AIManagerData", None),
    (".claude", Some(SourceClientKind::ClaudeCode)),
    (".codex", Some(SourceClientKind::Codex)),
    ("Library/Application Support", None),
];

/// Linux 只登记默认 CLI 根；Desktop 应用数据根由 adapter 按 XDG 目录另行枚举。
const LINUX_PRIORITY_ROOTS: &[PriorityEntry] = &[
    (".claude", Some(SourceClientKind::ClaudeCode)),
    (".codex", Some(SourceClientKind::Codex)),
];

/// 按 `std::env::consts::OS` 取值返回已开放 Agent 对应的优先相对目录，保持登记顺序。
///
/// 未登记的平台返回空列表，快速扫描因此不触盘；Grok 不在表内，只走显式直达根。
pub fn root_discovery_priority_relative_roots(
    os: &str,
    agents: RootDiscoveryAgentScope,
) -> Vec<&'static str> {
    let table = match os {
        "windows" => WINDOWS_PRIORITY_ROOTS,
        "macos" => MACOS_PRIORITY_ROOTS,
        "linux" => LINUX_PRIORITY_ROOTS,
        _ => &[],
    };
    let shared_selected =
        agents.contains(SourceClientKind::Codex) || agents.contains(SourceClientKind::ClaudeCode);
    table
        .iter()
        .filter(|(_, client)| client.map_or(shared_selected, |client| agents.contains(client)))
        .map(|(relative, _)| *relative)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RootDiscoveryScope, resolve_root_discovery_agent_scope};

    /// 返回只开放单个 Agent 的快速发现闭集。
    fn only(client: SourceClientKind) -> RootDiscoveryAgentScope {
        resolve_root_discovery_agent_scope(RootDiscoveryScope::UserPriority, Some(&[client]))
            .expect("physical discovery agent")
    }

    /// 全客户端优先目录在三个平台上都保持既有产品顺序，未登记平台为空。
    #[test]
    fn all_agents_keep_the_product_order_per_platform() {
        let all = RootDiscoveryAgentScope::all_physical();
        assert_eq!(
            root_discovery_priority_relative_roots("windows", all),
            [
                "AppData/Roaming/AIManagerData",
                ".claude",
                ".codex",
                "AppData/Roaming",
                "AppData/Local",
            ]
        );
        assert_eq!(
            root_discovery_priority_relative_roots("macos", all),
            [
                "Library/Application Support/AIManagerData",
                ".claude",
                ".codex",
                "Library/Application Support",
            ]
        );
        assert_eq!(
            root_discovery_priority_relative_roots("linux", all),
            [".claude", ".codex"]
        );
        assert!(root_discovery_priority_relative_roots("freebsd", all).is_empty());
    }

    /// 单选 Claude 保留共享目录（含 AIManagerData 托管实例）但排除 Codex 专用根；单选 Grok 在任何平台都不触盘。
    #[test]
    fn selected_agents_exclude_unselected_dedicated_roots() {
        for os in ["windows", "macos", "linux"] {
            let claude =
                root_discovery_priority_relative_roots(os, only(SourceClientKind::ClaudeCode));
            assert!(claude.contains(&".claude"), "{os}");
            assert!(!claude.contains(&".codex"), "{os}");
            assert_eq!(
                claude.iter().any(|path| path.ends_with("AIManagerData")),
                os != "linux",
                "{os}"
            );
            assert!(
                root_discovery_priority_relative_roots(os, only(SourceClientKind::GrokBuildCli))
                    .is_empty(),
                "{os}"
            );
        }
        assert_eq!(
            root_discovery_priority_relative_roots("windows", only(SourceClientKind::ClaudeCode)),
            [
                "AppData/Roaming/AIManagerData",
                ".claude",
                "AppData/Roaming",
                "AppData/Local"
            ]
        );
    }
}
