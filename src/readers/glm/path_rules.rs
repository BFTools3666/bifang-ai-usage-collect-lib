//! GLM（Z Code CLI）rollout 目录内 `model-io-sess_*.jsonl` 的路径硬规则。

use std::path::{Component, Path};

/// rollout 根下可索引文件名的固定前缀。
pub const GLM_MODEL_IO_FILE_PREFIX: &str = "model-io-sess_";
/// rollout 根下可索引文件名的固定后缀。
pub const GLM_MODEL_IO_FILE_SUFFIX: &str = ".jsonl";

/// 判定相对路径是否为可索引的 GLM 模型 I/O 会话文件。
///
/// Z Code CLI 的 rollout 根是平铺目录：合法来源只能是单个普通文件名，
/// 即 `model-io-sess_<会话标识>.jsonl`（subagent 会话文件同样带该前缀）。
pub fn is_glm_model_io_path(relative: &Path) -> bool {
    let mut components = relative.components();
    let Some(Component::Normal(name)) = components.next() else {
        return false;
    };
    if components.next().is_some() {
        return false;
    }
    let Some(name) = name.to_str() else {
        return false;
    };
    is_model_io_file_name(name)
}

/// 判定单个文件名是否符合 `model-io-sess_*.jsonl` 且会话标识非空有界。
pub fn is_model_io_file_name(name: &str) -> bool {
    let Some(stripped) = name
        .strip_prefix(GLM_MODEL_IO_FILE_PREFIX)
        .and_then(|value| value.strip_suffix(GLM_MODEL_IO_FILE_SUFFIX))
    else {
        return false;
    };
    !stripped.is_empty() && stripped.len() <= 128 && !stripped.contains(['/', '\\'])
}

/// 从相对路径取出会话标识（文件名去掉固定前后缀），供线程键使用。
pub fn glm_session_seed(relative: &Path) -> Option<String> {
    let name = relative
        .components()
        .next()
        .and_then(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })?;
    name.strip_prefix(GLM_MODEL_IO_FILE_PREFIX)
        .and_then(|value| value.strip_suffix(GLM_MODEL_IO_FILE_SUFFIX))
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// 返回路径相对最外层 `.zcode` 私有目录的位置，不跟随链接或读取文件。
fn zcode_relative_path(path: &Path) -> Option<&Path> {
    path.ancestors()
        .filter(|ancestor| {
            ancestor
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case(".zcode"))
        })
        .last()
        .and_then(|ancestor| path.strip_prefix(ancestor).ok())
}

/// `.zcode` 内只批准固定 `cli/rollout` 根；外部显式授权的同布局根仍可用。
pub fn is_allowed_glm_root(path: &Path) -> bool {
    zcode_relative_path(path).is_none_or(|relative| relative == Path::new("cli").join("rollout"))
}

/// 发现可以经过 `.zcode` 与 `cli`，但不能进入其余私有子树或 rollout 子目录。
pub fn is_allowed_glm_discovery_directory(path: &Path) -> bool {
    zcode_relative_path(path).is_none_or(|relative| {
        relative.as_os_str().is_empty()
            || relative == Path::new("cli")
            || relative == Path::new("cli").join("rollout")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// 只接受平铺的 `model-io-sess_*.jsonl`，拒绝子目录、前缀不符与空会话标识。
    #[test]
    fn accepts_only_flat_model_io_files() {
        assert!(is_glm_model_io_path(Path::new(
            "model-io-sess_09a04db8-7e58-48e7-9c8f-d5a9afc20996.jsonl"
        )));
        assert!(is_glm_model_io_path(Path::new(
            "model-io-sess_subagent_agent_9fb3da14-7132-4bf9-b8f5-617529142d2d.jsonl"
        )));
        assert!(!is_glm_model_io_path(Path::new("sessions/updates.jsonl")));
        assert!(!is_glm_model_io_path(Path::new("model-io-sess_.jsonl")));
        assert!(!is_glm_model_io_path(Path::new("model-io-sess_plain.txt")));
        assert!(!is_glm_model_io_path(Path::new(
            "nested/model-io-sess_x.jsonl"
        )));
        assert_eq!(
            glm_session_seed(Path::new("model-io-sess_subagent_agent_1.jsonl")).as_deref(),
            Some("subagent_agent_1")
        );
    }
    /// 私有目录边界按完整组件判断，外部显式根与固定 rollout 分别放行。
    #[test]
    fn zcode_private_boundary_allows_only_the_fixed_rollout() {
        let home = Path::new("/synthetic-home");
        assert!(is_allowed_glm_root(&home.join(".zcode/cli/rollout")));
        assert!(is_allowed_glm_root(&home.join("external-rollout")));
        for relative in [
            ".zcode",
            ".zcode/cli",
            ".zcode/credentials",
            ".zcode/workspace/cli/rollout",
            ".zcode/cli/rollout/nested",
        ] {
            assert!(!is_allowed_glm_root(&home.join(relative)));
        }
        assert!(is_allowed_glm_discovery_directory(&home.join(".zcode")));
        assert!(is_allowed_glm_discovery_directory(&home.join(".zcode/cli")));
        assert!(!is_allowed_glm_discovery_directory(
            &home.join(".zcode/logs")
        ));
        assert!(!is_allowed_glm_discovery_directory(
            &home.join(".zcode/cli/rollout/nested")
        ));
    }
}
