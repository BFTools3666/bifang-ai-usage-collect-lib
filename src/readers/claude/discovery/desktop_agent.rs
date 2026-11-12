//! Claude Desktop 本地 Agent 会话自带数据根的有界枚举。
//!
//! Claude Desktop 为每个本地 Agent（Cowork）会话单独创建一份完整的 Claude Code
//! 数据根，快速发现无法像 `~/.claude` 那样用一条固定路径命中，必须按 core 固定的
//! 三层布局读取目录项。本模块只负责目录枚举这一宿主机制：布局、命名与展示别名
//! 都取自 core，结构签名仍由 Claude 通用候选探测负责。

use std::fs;
use std::path::{Path, PathBuf};

use crate::{
    claude_desktop_agent_home_alias, claude_desktop_agent_home_dir,
    claude_desktop_agent_sessions_dir, is_claude_desktop_agent_session_dir_name,
};

use super::super::super::discovery::{metadata_is_link_like, validate_local_plain_directory};

/// 一个目录结构已确认存在的 Claude Desktop 本地 Agent 数据根候选。
///
/// 只表示“路径存在且命名合规”，是否真的含有 transcript 用量仍由结构签名判定。
pub struct ClaudeDesktopAgentHome {
    /// adapter 内只读访问路径。
    pub path: PathBuf,
    /// 不含完整路径的安全展示别名。
    pub alias: String,
}

/// 在 core 固定布局内枚举当前用户的 Claude Desktop 本地 Agent 数据根。
///
/// 依次读取安装标识、组织标识与会话三层目录，跳过链接状目录项和命名不合规的会话
/// 目录，只返回 `.claude` 真实存在的候选。层级固定为三层且不递归，因此不设遍历
/// 数量上限；候选随后与其他 Claude 候选共享同一份有界结构签名预算。目录缺失、无
/// 权限或读取失败都按“Desktop 未使用本地 Agent”返回空结果：这些候选是可选默认根，
/// 不应污染整体发现覆盖状态。
pub fn claude_desktop_agent_homes(app_data_dir: &Path) -> Vec<ClaudeDesktopAgentHome> {
    let sessions_dir = claude_desktop_agent_sessions_dir(app_data_dir);
    let mut homes = Vec::new();
    for installation in child_directories(&sessions_dir) {
        for organization in child_directories(&installation) {
            for session in child_directories(&organization) {
                let Some(alias) = session
                    .file_name()
                    .and_then(|name| name.to_str())
                    .filter(|name| is_claude_desktop_agent_session_dir_name(name))
                    .and_then(claude_desktop_agent_home_alias)
                else {
                    continue;
                };
                let path = claude_desktop_agent_home_dir(&session);
                if is_plain_directory(&path) {
                    homes.push(ClaudeDesktopAgentHome { path, alias });
                }
            }
        }
    }
    homes
}

/// 在固定两层布局内枚举 AIManagerData 托管的 Claude 实例数据根。
///
/// 托管实例通过 `CLAUDE_CONFIG_DIR` 指向 `AIManagerData/claude/instances/<实例>`，
/// 从 Finder 启动的 GUI 读不到该环境变量，只能按固定相对路径读取目录项。层级
/// 固定、不递归；是否含有 transcript 用量仍由结构签名判定。目录缺失、无权限或
/// 读取失败按空结果处理，不污染整体发现覆盖状态。
pub fn claude_managed_instance_homes(app_data_dir: &Path) -> Vec<PathBuf> {
    child_directories(
        &app_data_dir
            .join("AIManagerData")
            .join("claude")
            .join("instances"),
    )
}

/// 按名称升序返回目录下的全部非链接子目录；读取失败按空目录处理。
///
/// 固定排序保证同一台设备上的候选顺序稳定，让共享签名预算的消费顺序可复现。
fn child_directories(dir: &Path) -> Vec<PathBuf> {
    // 每一层枚举前都重新确认完整祖先链与本地卷身份，避免先跟随 Claude、
    // sessions 或中间层链接读出外部目录，再到最终候选签名阶段才拒绝。
    if validate_local_plain_directory(dir).is_err() {
        return Vec::new();
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut children = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| is_plain_directory(path))
        .collect::<Vec<_>>();
    children.sort();
    children
}

/// 判定路径是否为真实存在且非链接状的普通目录。
fn is_plain_directory(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .is_ok_and(|metadata| metadata.is_dir() && !metadata_is_link_like(&metadata))
}
