//! Claude Desktop 本地 Agent 会话数据根的固定布局与安全别名。
//! 本模块不探测文件系统：平台应用数据目录解析与目录枚举属于 GUI adapter，
//! 这里只保留“什么样的路径才是一个 Claude Desktop 本地 Agent 数据根”这条业务规则。
//!
//! 布局固定为三层且不递归，因此不需要额外遍历数量上限：候选仍与其他 Claude
//! 候选共享同一份有界结构签名预算。
//!
//! Claude Desktop 的本地 Agent（Cowork）模式为每个会话单独创建一个完整的
//! Claude Code 数据根，布局固定为
//! `<应用数据目录>/Claude/local-agent-mode-sessions/<安装标识>/<组织标识>/local_<会话 UUID>/.claude`。
//! 该根内部同样是 `projects/<project>/<UUID>.jsonl`，因此可以直接复用
//! Claude Code 的 transcript 结构签名、解析与索引，不构成新的客户端。

use std::path::{Path, PathBuf};

/// Claude Desktop 在各平台用户应用数据目录下的固定应用目录名。
pub const CLAUDE_DESKTOP_APP_DIR_NAME: &str = "Claude";
/// Claude Desktop 本地 Agent 模式会话集合的固定目录名。
pub const CLAUDE_DESKTOP_AGENT_SESSIONS_DIR_NAME: &str = "local-agent-mode-sessions";
/// 单个本地 Agent 会话目录的固定前缀，其后紧跟会话 UUID。
pub const CLAUDE_DESKTOP_AGENT_SESSION_DIR_PREFIX: &str = "local_";
/// 每个本地 Agent 会话自带的 Claude Code 数据根目录名。
pub const CLAUDE_DESKTOP_AGENT_HOME_DIR_NAME: &str = ".claude";

/// 展示别名中保留的会话 UUID 短前缀长度，既能区分会话又不暴露完整标识。
const AGENT_SESSION_SHORT_ID_LEN: usize = 8;

/// 由平台用户应用数据目录推导 Claude Desktop 本地 Agent 会话集合目录；不探测存在性。
pub fn claude_desktop_agent_sessions_dir(app_data_dir: &Path) -> PathBuf {
    app_data_dir
        .join(CLAUDE_DESKTOP_APP_DIR_NAME)
        .join(CLAUDE_DESKTOP_AGENT_SESSIONS_DIR_NAME)
}

/// 由单个本地 Agent 会话目录推导其自带的 Claude Code 数据根路径；不探测存在性。
pub fn claude_desktop_agent_home_dir(session_dir: &Path) -> PathBuf {
    session_dir.join(CLAUDE_DESKTOP_AGENT_HOME_DIR_NAME)
}

/// 判定目录名是否为 `local_<UUID>` 形式的本地 Agent 会话目录。
///
/// 会话集合目录下还存在 `skills-plugin` 等非会话目录，因此必须按完整命名
/// 规则判定，不能只看前缀之外的任意字符串。
pub fn is_claude_desktop_agent_session_dir_name(name: &str) -> bool {
    name.strip_prefix(CLAUDE_DESKTOP_AGENT_SESSION_DIR_PREFIX)
        .is_some_and(is_hyphenated_uuid)
}

/// 为本地 Agent 会话目录名生成不含路径的安全展示别名；目录名不合规则返回 `None`。
pub fn claude_desktop_agent_home_alias(session_dir_name: &str) -> Option<String> {
    let session_id = session_dir_name.strip_prefix(CLAUDE_DESKTOP_AGENT_SESSION_DIR_PREFIX)?;
    if !is_hyphenated_uuid(session_id) {
        return None;
    }
    let short_id = &session_id[..AGENT_SESSION_SHORT_ID_LEN];
    Some(format!("Claude Desktop 本地 Agent {short_id}"))
}

/// 判定数据根路径是否落在 Claude Desktop 本地 Agent 固定布局内，并返回其安全别名。
///
/// 必须逐级核对 `.claude`、`local_<UUID>` 与再上三级的
/// `local-agent-mode-sessions`，避免任何同名 `.claude` 目录被误标成 Desktop 会话根。
pub fn claude_desktop_agent_home_alias_from_path(home_dir: &Path) -> Option<String> {
    if path_component(home_dir)? != CLAUDE_DESKTOP_AGENT_HOME_DIR_NAME {
        return None;
    }
    let session_dir = home_dir.parent()?;
    let sessions_dir = session_dir.parent()?.parent()?.parent()?;
    if path_component(sessions_dir)? != CLAUDE_DESKTOP_AGENT_SESSIONS_DIR_NAME {
        return None;
    }
    claude_desktop_agent_home_alias(path_component(session_dir)?)
}

/// 取路径末段的 UTF-8 目录名；末段缺失或不可解码时返回 `None`。
fn path_component(path: &Path) -> Option<&str> {
    path.file_name().and_then(|value| value.to_str())
}

/// 判定字符串是否符合标准 UUID（含连字符）格式。
fn is_hyphenated_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 固定会话 UUID，短前缀在别名断言中直接可读。
    const SESSION_ID: &str = "048c4e6d-c215-47ca-9c77-75d7f016803a";

    /// 会话集合目录与会话数据根都只由路径字节推导，不要求路径真实存在。
    #[test]
    fn agent_paths_are_derived_without_touching_filesystem() {
        let app_data = Path::new("definitely-missing-app-data");
        let sessions = claude_desktop_agent_sessions_dir(app_data);
        assert_eq!(
            sessions,
            app_data.join("Claude").join("local-agent-mode-sessions")
        );
        assert_eq!(
            claude_desktop_agent_home_dir(&sessions.join("install/org/local_x")),
            sessions.join("install/org/local_x/.claude")
        );
    }

    /// 只有 `local_<UUID>` 才是会话目录；插件等同级目录必须被排除。
    #[test]
    fn only_uuid_suffixed_session_directories_are_accepted() {
        assert!(is_claude_desktop_agent_session_dir_name(&format!(
            "local_{SESSION_ID}"
        )));
        assert!(!is_claude_desktop_agent_session_dir_name("skills-plugin"));
        assert!(!is_claude_desktop_agent_session_dir_name("local_"));
        assert!(!is_claude_desktop_agent_session_dir_name(
            "local_not-a-uuid"
        ));
        assert!(!is_claude_desktop_agent_session_dir_name(SESSION_ID));
    }

    /// 合规布局产出短标识别名，且别名不包含任何路径片段。
    #[test]
    fn agent_home_alias_uses_short_session_id_without_path() {
        let home = Path::new("/any/base")
            .join("Claude")
            .join("local-agent-mode-sessions")
            .join("install-id")
            .join("org-id")
            .join(format!("local_{SESSION_ID}"))
            .join(".claude");
        let alias = claude_desktop_agent_home_alias_from_path(&home)
            .expect("layout is a desktop agent home");
        assert_eq!(alias, "Claude Desktop 本地 Agent 048c4e6d");
        assert!(!alias.contains('/'));
        assert!(!alias.contains("install-id"));
    }

    /// 普通 `~/.claude` 与层级不完整的同名目录都不得被认成 Desktop 会话根。
    #[test]
    fn unrelated_claude_home_layouts_are_rejected() {
        assert_eq!(
            claude_desktop_agent_home_alias_from_path(Path::new("/Users/tester/.claude")),
            None
        );
        let missing_sessions_level = Path::new("/base")
            .join("install-id")
            .join("org-id")
            .join(format!("local_{SESSION_ID}"))
            .join(".claude");
        assert_eq!(
            claude_desktop_agent_home_alias_from_path(&missing_sessions_level),
            None
        );
        let wrong_leaf = Path::new("/base")
            .join("local-agent-mode-sessions")
            .join("install-id")
            .join("org-id")
            .join(format!("local_{SESSION_ID}"))
            .join("projects");
        assert_eq!(claude_desktop_agent_home_alias_from_path(&wrong_leaf), None);
    }
}
