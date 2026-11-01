use std::path::Path;

use super::SourceClientKind;
use super::claude_desktop_agent_home::claude_desktop_agent_home_alias_from_path;

/// 仅用于展示的来源根别名：Claude Desktop 本地 Agent 会话根优先使用其专属短标识
/// 别名，其余情况取目录名，目录名缺失时回退客户端默认别名。
///
/// Claude Desktop 为每个本地 Agent 会话单独创建一个 `.claude` 数据根，若按目录名
/// 命名会出现多个无法区分的 `.claude`，因此这里必须先按固定布局识别会话根。
pub fn source_root_alias_from_path(path: &Path, source_client: SourceClientKind) -> String {
    if source_client == SourceClientKind::ClaudeCode
        && let Some(alias) = claude_desktop_agent_home_alias_from_path(path)
    {
        return alias;
    }
    path.file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| source_client.default_source_root_alias().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claude Desktop 会话根在 Claude Code 客户端下使用可区分的专属别名。
    #[test]
    fn claude_desktop_agent_home_uses_dedicated_alias() {
        let home = Path::new("/base/Claude/local-agent-mode-sessions/install/org")
            .join("local_048c4e6d-c215-47ca-9c77-75d7f016803a")
            .join(".claude");
        assert_eq!(
            source_root_alias_from_path(&home, SourceClientKind::ClaudeCode),
            "Claude Desktop 本地 Agent 048c4e6d"
        );
    }

    /// 普通 Claude Code 数据根与其他客户端继续使用目录名别名。
    #[test]
    fn ordinary_roots_keep_directory_name_alias() {
        assert_eq!(
            source_root_alias_from_path(
                Path::new("/Users/tester/.claude"),
                SourceClientKind::ClaudeCode
            ),
            ".claude"
        );
        assert_eq!(
            source_root_alias_from_path(Path::new("/Users/tester/.codex"), SourceClientKind::Codex),
            ".codex"
        );
    }
}
