//! 定义当前开发版的扫描范围与高优先级入队顺序。

use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};

use crate::{
    RootCandidateEvidence, RootDiscoveryAgentScope, RootDiscoveryPlatform, RootDiscoveryScope,
    SourceClientKind, WORKBUDDY_HOME_DIR_NAME, WORKBUDDY_INTERNATIONAL_HOME_DIR_NAME,
    root_discovery_priority_relative_roots,
};

use super::super::{
    LocalPathStatus, LocalVolumeRoots, classify_local_path, current_user_home,
    enumerate_local_volume_roots, metadata_is_link_like, validate_local_plain_directory,
};

/// 返回当前编译目标对应的数据源发现平台。
pub(super) const fn current_platform() -> RootDiscoveryPlatform {
    if cfg!(target_os = "windows") {
        RootDiscoveryPlatform::Windows
    } else if cfg!(target_os = "macos") {
        RootDiscoveryPlatform::MacOs
    } else {
        RootDiscoveryPlatform::Other
    }
}

/// 根据用户选择返回快速优先目录或全部已确认本地卷。
///
/// 快速扫描（`UserPriority`）的搜索根仅为调用方已解析的平台优先目录，不包含用户主目录本身，
/// 因此不会遍历 Desktop、Downloads 等优先列表之外的路径。
pub(super) fn discovery_scope(
    scope: RootDiscoveryScope,
    priority_roots: &[PathBuf],
) -> LocalVolumeRoots {
    if scope == RootDiscoveryScope::FullLocalVolumes {
        return enumerate_local_volume_roots();
    }
    local_priority_scope(priority_roots.to_vec())
}

/// 在读取任何目录项前，把用户优先根限制为已确认本地、非链接的现存目录。
/// 缺失的可选优先目录是普通状态；网络、未知卷或链接则计入保守跳过。
pub(super) fn local_priority_scope(roots: Vec<PathBuf>) -> LocalVolumeRoots {
    let mut result = LocalVolumeRoots::default();
    for root in roots {
        match classify_local_path(&root) {
            LocalPathStatus::ConfirmedLocal => match validate_local_plain_directory(&root) {
                Ok(()) => result.search_roots.push(root),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    result.other_skipped_count = result.other_skipped_count.saturating_add(1);
                    result.excluded_roots.push(root);
                }
            },
            LocalPathStatus::RejectedNetwork => {
                result.network_skipped_count = result.network_skipped_count.saturating_add(1);
                result.excluded_roots.push(root);
            }
            LocalPathStatus::Missing => {}
            LocalPathStatus::Indeterminate => {
                result.other_skipped_count = result.other_skipped_count.saturating_add(1);
                result.excluded_roots.push(root);
            }
        }
    }
    result
}

/// 返回相对当前用户根的平台优先目录；快速扫描只以这些路径为搜索根。
pub(super) fn platform_priority_roots(agents: RootDiscoveryAgentScope) -> Vec<PathBuf> {
    let Some(root) = current_user_home() else {
        return Vec::new();
    };
    #[cfg(target_os = "linux")]
    {
        linux_priority_roots(
            root,
            super::super::current_user_app_data_dir().as_deref(),
            agents,
        )
    }
    #[cfg(not(target_os = "linux"))]
    platform_default_roots(root, agents)
}

/// Linux 只加入默认 CLI 根和固定三层枚举出的 Desktop 根，遵循 XDG 应用目录。
#[cfg(target_os = "linux")]
pub(super) fn linux_priority_roots(
    home: PathBuf,
    app_data_dir: Option<&Path>,
    agents: RootDiscoveryAgentScope,
) -> Vec<PathBuf> {
    let mut roots = platform_default_roots(home, agents);
    if agents.contains(SourceClientKind::ClaudeCode)
        && let Some(app_data_dir) = app_data_dir
    {
        roots.extend(super::super::claude::claude_desktop_agent_priority_roots(
            app_data_dir,
        ));
    }
    roots
}

/// 把 core 的当前平台优先相对目录表拼接到 OS 用户根下。
pub(super) fn platform_default_roots(
    root: PathBuf,
    agents: RootDiscoveryAgentScope,
) -> Vec<PathBuf> {
    root_discovery_priority_relative_roots(std::env::consts::OS, agents)
        .into_iter()
        .map(|relative| root.join(relative))
        .collect()
}

/// 判断目录是否属于当前闭集未选择的专用 Agent 根；命中时不得进入其子树。
pub(super) fn is_unselected_agent_directory(path: &Path, agents: RootDiscoveryAgentScope) -> bool {
    let Some(name) = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_ascii_lowercase)
    else {
        return false;
    };
    // 国内版与国际版两个固定 WorkBuddy 根都必须在发现层被跳过：它们的
    // `projects/<project>/<uuid>.jsonl` 会命中下面的 Claude transcript 规则，
    // 漏掉任一根都会把该根误登记成 Claude Code 数据根。根名只取 core 的单点事实，
    // adapter 不得二次编码，否则新增版本根时会再次静默漏掉。
    let client = if name.eq_ignore_ascii_case(WORKBUDDY_HOME_DIR_NAME)
        || name.eq_ignore_ascii_case(WORKBUDDY_INTERNATIONAL_HOME_DIR_NAME)
    {
        SourceClientKind::WorkBuddy
    } else {
        match name.as_str() {
            ".codex" | "aimanagerdata" => SourceClientKind::Codex,
            ".claude" => SourceClientKind::ClaudeCode,
            ".grok" => SourceClientKind::GrokBuildCli,
            ".zcode" => SourceClientKind::Glm,
            _ => return false,
        }
    };
    !agents.contains(client)
}

/// 路径是否位于闭集未选择的专用 Agent 根之下。
///
/// 遍历分支在目录入口就跳过未选中的 Agent 根，子树永远不会被读到；系统索引却直接给出文件
/// 路径，绕过了那个入口。若不复核祖先，Spotlight 返回的
/// `~/.workbuddy-ai/projects/<project>/<uuid>.jsonl` 会按 Claude transcript 规则把该
/// WorkBuddy 根登记成 Claude Code 数据根。
pub(super) fn path_under_unselected_agent_directory(
    path: &Path,
    agents: RootDiscoveryAgentScope,
) -> bool {
    path.ancestors()
        .any(|ancestor| is_unselected_agent_directory(ancestor, agents))
}

/// 只为闭集内 Agent 按文件名与父目录结构推导候选根。
pub(super) fn candidate_from_file_name(
    path: &Path,
    agents: RootDiscoveryAgentScope,
) -> Option<(PathBuf, RootCandidateEvidence)> {
    let parent = path.parent()?;
    if !crate::readers::glm::path_rules::is_allowed_glm_discovery_directory(parent) {
        return None;
    }
    let name = path.file_name()?.to_str()?;
    if agents.contains(SourceClientKind::Codex)
        && name.starts_with("rollout-")
        && name.ends_with(".jsonl")
    {
        for ancestor in path.ancestors().skip(1) {
            let component = ancestor.file_name()?.to_str()?;
            if matches!(component, "sessions" | "archived_sessions") {
                let root = ancestor.parent()?.to_path_buf();
                return allowed_root(&root).then_some((root, RootCandidateEvidence::CodexRollout));
            }
        }
    }
    if !name.ends_with(".jsonl") {
        return None;
    }
    let stem = name.strip_suffix(".jsonl")?;
    let parent = path.parent()?;
    if agents.contains(SourceClientKind::ClaudeCode)
        && is_uuid_name(stem)
        && parent.parent()?.file_name()?.to_str()? == "projects"
    {
        let root = parent.parent()?.parent()?.to_path_buf();
        return allowed_root(&root).then_some((root, RootCandidateEvidence::ClaudeTranscript));
    }
    if agents.contains(SourceClientKind::ClaudeCode)
        && is_subagent_name(stem)
        && parent.file_name()?.to_str()? == "subagents"
    {
        let project_root = parent.parent()?.parent()?;
        if project_root.parent()?.file_name()?.to_str()? == "projects" {
            let root = project_root.parent()?.parent()?.to_path_buf();
            return allowed_root(&root).then_some((root, RootCandidateEvidence::ClaudeSubagent));
        }
    }
    if agents.contains(SourceClientKind::GrokBuildCli) && name == "updates.jsonl" {
        let session_dir = parent;
        let cwd_dir = session_dir.parent()?;
        let sessions = cwd_dir.parent()?;
        if sessions.file_name()?.to_str()? == "sessions" {
            let root = sessions.parent()?.to_path_buf();
            return allowed_root(&root)
                .then_some((root, RootCandidateEvidence::GrokSessionUpdates));
        }
    }
    if agents.contains(SourceClientKind::Glm)
        && stem.starts_with("model-io-sess_")
        && !stem["model-io-sess_".len()..].is_empty()
        && parent.file_name()?.to_str()? == "rollout"
        && parent.parent()?.file_name()?.to_str()? == "cli"
    {
        let root = parent.to_path_buf();
        return (allowed_root(&root)
            && crate::readers::glm::path_rules::is_allowed_glm_root(&root))
        .then_some((root, RootCandidateEvidence::GlmModelIoUpdates));
    }
    None
}

/// 拒绝不得误作数据源的辅助目录名称。
fn allowed_root(path: &Path) -> bool {
    !matches!(
        path.file_name()
            .and_then(|name| name.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("logs" | "log" | "cache" | "caches" | "test" | "tests")
    )
}

/// 验证标准连字符 UUID 文件名，不解析正文。
fn is_uuid_name(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 8 | 13 | 18 | 23) && byte == b'-'
                || !matches!(index, 8 | 13 | 18 | 23) && byte.is_ascii_hexdigit()
        })
}

/// 验证 Claude subagent 的稳定文件名前缀。
fn is_subagent_name(value: &str) -> bool {
    value.strip_prefix("agent-").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

/// 优先目录预先标记为已入队，后续宽遍历不会重复扫描它们。
pub(super) fn priority_queue(root: &Path, priority_roots: &[PathBuf]) -> VecDeque<PathBuf> {
    priority_roots
        .iter()
        .filter(|path| path.starts_with(root))
        .filter(|path| path.as_path() != root)
        .filter(|path| {
            fs::symlink_metadata(path)
                .is_ok_and(|metadata| metadata.is_dir() && !metadata_is_link_like(&metadata))
        })
        .cloned()
        .chain(std::iter::once(root.to_path_buf()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        WORKBUDDY_HOME_DIR_NAME, WORKBUDDY_INTERNATIONAL_HOME_DIR_NAME,
        resolve_root_discovery_agent_scope,
    };

    /// 快速扫描只保留夹具中已确认本地的现存目录，不读取真实用户数据。
    #[test]
    fn user_priority_scope_is_only_platform_priority_roots() {
        let home = tempfile::tempdir().unwrap();
        let directory = home.path().join(".claude");
        let regular_file = home.path().join(".codex");
        let missing = home.path().join("missing");
        fs::create_dir(&directory).unwrap();
        fs::write(&regular_file, b"not a directory").unwrap();
        let scope = local_priority_scope(vec![directory.clone(), regular_file, missing]);
        assert_eq!(scope.search_roots, vec![directory]);
        assert!(scope.search_roots.iter().all(|path| {
            classify_local_path(path) == LocalPathStatus::ConfirmedLocal
                && validate_local_plain_directory(path).is_ok()
        }));
        assert!(!scope.search_roots.contains(&home.path().to_path_buf()));
    }

    /// 用户根拼接保持 core 登记顺序，不在 adapter 重排或过滤。
    #[test]
    fn platform_default_roots_join_core_order_under_the_user_root() {
        let home = PathBuf::from("/synthetic-home");
        let agents = RootDiscoveryAgentScope::all_physical();
        let expected = root_discovery_priority_relative_roots(std::env::consts::OS, agents)
            .into_iter()
            .map(|relative| home.join(relative))
            .collect::<Vec<_>>();
        assert_eq!(platform_default_roots(home, agents), expected);
    }

    /// 单选 Claude 不把 Codex 专用根加入触盘种子，单选 Grok 只走显式直达根。
    #[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
    #[test]
    fn selected_priority_roots_exclude_unselected_agent_directories() {
        let profile = PathBuf::from(if cfg!(target_os = "windows") {
            "C:/Users/synthetic"
        } else if cfg!(target_os = "macos") {
            "/Users/synthetic"
        } else {
            "/home/synthetic"
        });
        let claude = resolve_root_discovery_agent_scope(
            RootDiscoveryScope::UserPriority,
            Some(&[SourceClientKind::ClaudeCode]),
        )
        .expect("Claude is a physical discovery agent");
        let roots = platform_default_roots(profile.clone(), claude);
        assert!(roots.contains(&profile.join(".claude")));
        assert!(!roots.contains(&profile.join(".codex")));
        assert!(
            roots.iter().all(
                |path| path.file_name().and_then(|name| name.to_str()) != Some("AIManagerData")
            )
        );

        let grok = resolve_root_discovery_agent_scope(
            RootDiscoveryScope::UserPriority,
            Some(&[SourceClientKind::GrokBuildCli]),
        )
        .expect("Grok is a physical discovery agent");
        assert!(platform_default_roots(profile, grok).is_empty());
    }

    /// 国内版与国际版两个固定 WorkBuddy 根都不属于物理发现客户端，必须在发现层被跳过。
    /// 漏掉国际版 `~/.workbuddy-ai` 时，全盘发现会下钻该根，并把其中的
    /// `projects/<project>/<uuid>.jsonl` 按 Claude transcript 规则登记成 Claude Code 数据根。
    /// 系统索引直接给出文件路径、不经过目录入口，因此必须沿祖先复核同一闭集。
    #[test]
    fn workbuddy_roots_stay_unreachable_for_traversal_and_system_index() {
        let claude_only = resolve_root_discovery_agent_scope(
            RootDiscoveryScope::FullLocalVolumes,
            Some(&[SourceClientKind::ClaudeCode]),
        )
        .expect("Claude is a physical discovery agent");
        let leaf = "projects/project-a/0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0.jsonl";

        let selected = PathBuf::from("/Users/synthetic/.claude").join(leaf);
        assert!(
            !path_under_unselected_agent_directory(&selected, claude_only),
            "已选中 Agent 的专用根不得被跳过"
        );

        for agents in [RootDiscoveryAgentScope::all_physical(), claude_only] {
            for name in [
                WORKBUDDY_HOME_DIR_NAME,
                WORKBUDDY_INTERNATIONAL_HOME_DIR_NAME,
            ] {
                let root = PathBuf::from("/Users/synthetic").join(name);
                assert!(
                    is_unselected_agent_directory(&root, agents),
                    "WorkBuddy 固定根 {name} 必须被发现层跳过"
                );
                assert!(
                    path_under_unselected_agent_directory(&root.join(leaf), agents),
                    "系统索引返回的 {name} 会话文件必须被拒绝"
                );
            }
        }
    }
    /// 未开启 GLM 不产出其候选；开启后仍拒绝私有子树中的各种伪装布局。
    #[test]
    fn glm_metadata_candidates_respect_selection_and_private_subtrees() {
        let home = PathBuf::from("/synthetic-home");
        let approved = home.join(".zcode/cli/rollout/model-io-sess_valid.jsonl");
        let glm =
            RootDiscoveryAgentScope::for_agent(SourceClientKind::Glm).expect("GLM is physical");
        let codex =
            RootDiscoveryAgentScope::for_agent(SourceClientKind::Codex).expect("Codex is physical");
        assert!(candidate_from_file_name(&approved, glm).is_some());
        assert!(candidate_from_file_name(&approved, codex).is_none());
        assert!(is_unselected_agent_directory(&home.join(".zcode"), codex));
        for relative in [
            ".zcode/logs/model-io-sess_decoy.jsonl",
            ".zcode/workspace/project/cli/rollout/model-io-sess_decoy.jsonl",
            ".zcode/workspace/projects/project/0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0.jsonl",
        ] {
            assert!(
                candidate_from_file_name(
                    &home.join(relative),
                    RootDiscoveryAgentScope::all_physical()
                )
                .is_none()
            );
        }
    }
}
