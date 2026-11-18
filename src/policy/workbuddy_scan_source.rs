//! WorkBuddy 只读本机来源策略：版本、默认路径与稳定身份推导。

use std::path::{Path, PathBuf};

use crate::{SourceClientKind, path_key, source_root_alias_from_path, stable_id};
use serde::{Deserialize, Serialize};

/// WorkBuddy 固定安装目录名，位于当前用户主目录下。
pub const WORKBUDDY_HOME_DIR_NAME: &str = ".workbuddy";
/// WorkBuddy 国际版固定安装目录名，位于当前用户主目录下。
pub const WORKBUDDY_INTERNATIONAL_HOME_DIR_NAME: &str = ".workbuddy-ai";
/// WorkBuddy 逐请求项目记录固定目录名，用作可统计结构证据。
pub const WORKBUDDY_PROJECTS_DIR_NAME: &str = "projects";

/// 独立授权、发现和统计的 WorkBuddy 版本。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkbuddyEdition {
    /// 国内版。
    Domestic,
    /// 国际版。
    International,
}

impl WorkbuddyEdition {
    /// 稳定显示和扫描顺序。
    pub const ALL: [Self; 2] = [Self::Domestic, Self::International];

    /// 识别数据布局的安装目录名。
    pub const fn directory_name(self) -> &'static str {
        match self {
            Self::Domestic => WORKBUDDY_HOME_DIR_NAME,
            Self::International => WORKBUDDY_INTERNATIONAL_HOME_DIR_NAME,
        }
    }

    /// 本产品在其私有目录下使用的独立派生库文件名。
    pub const fn database_file_name(self) -> &'static str {
        match self {
            Self::Domestic => "workbuddy-domestic.db",
            Self::International => "workbuddy-international.db",
        }
    }

    /// 只按完整目录名识别版本，兼容 Windows 与常见 macOS 大小写规则。
    pub(crate) fn matches_directory(self, path: &Path) -> bool {
        matches_workbuddy_directory_name(path, self.directory_name())
    }
}

/// 按目标平台的目录匹配规则核对 WorkBuddy 的固定布局组件。
pub(crate) fn matches_workbuddy_directory_name(path: &Path, expected: &str) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            if cfg!(any(target_os = "windows", target_os = "macos")) {
                name.eq_ignore_ascii_case(expected)
            } else {
                name == expected
            }
        })
}

/// GUI adapter 已确认存在的 WorkBuddy 只读本机来源。
///
/// 路径只供原生进程打开来源，显示层应使用别名与稳定标识。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkbuddyScanSource {
    /// 稳定根 ID，前缀为 `workbuddy-root-`。
    pub root_id: String,
    /// 安全展示别名，不含绝对路径。
    pub alias: String,
    /// adapter 内部只读访问路径。
    pub path: PathBuf,
}

/// 由用户主目录推导 WorkBuddy 本机根，不探测目录是否存在。
pub fn workbuddy_home_from_user_home(user_home: &Path) -> PathBuf {
    user_home.join(WORKBUDDY_HOME_DIR_NAME)
}

/// 由用户主目录按稳定顺序推导国内版与国际版两个固定候选根，不探测文件系统。
pub fn workbuddy_homes_from_user_home(user_home: &Path) -> [PathBuf; 2] {
    [
        workbuddy_home_from_user_home(user_home),
        user_home.join(WORKBUDDY_INTERNATIONAL_HOME_DIR_NAME),
    ]
}

/// 从用户主目录纯推导 WorkBuddy 只读来源身份；是否存在由 GUI adapter 判断。
///
/// 函数名为兼容既有调用边界保留，不执行“发现”、目录读取或登记。
pub fn discover_workbuddy_scan_source(user_home: &Path) -> WorkbuddyScanSource {
    workbuddy_scan_source_from_path(workbuddy_home_from_user_home(user_home))
}

/// 从用户主目录纯推导国内版与国际版两个只读来源身份；存在性仍由 GUI adapter 判断。
pub fn discover_workbuddy_scan_sources(user_home: &Path) -> Vec<WorkbuddyScanSource> {
    workbuddy_homes_from_user_home(user_home)
        .into_iter()
        .map(workbuddy_scan_source_from_path)
        .collect()
}

/// 按稳定命名空间从路径生成 WorkBuddy 只读来源身份。
pub fn workbuddy_scan_source_from_path(path: PathBuf) -> WorkbuddyScanSource {
    let client = SourceClientKind::WorkBuddy;
    WorkbuddyScanSource {
        root_id: stable_id(client.root_id_namespace(), &path_key(&path)),
        alias: source_root_alias_from_path(&path, client),
        path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 稳定身份推导只消费路径字节，不要求路径真实存在或触碰文件系统。
    #[test]
    fn workbuddy_identity_derivation_is_pure_and_stable() {
        let user_home = Path::new("definitely-missing-user-home");
        let first = discover_workbuddy_scan_source(user_home);
        let second = discover_workbuddy_scan_source(user_home);
        assert_eq!(first, second);
        assert!(first.root_id.starts_with("workbuddy-root-"));
        assert_eq!(first.alias, WORKBUDDY_HOME_DIR_NAME);
        assert_eq!(first.path, workbuddy_home_from_user_home(user_home));
    }

    /// 国内版与国际版固定根必须保持稳定顺序，并生成互不冲突的来源身份。
    #[test]
    fn workbuddy_editions_derive_two_distinct_stable_sources() {
        let user_home = Path::new("definitely-missing-user-home");
        let sources = discover_workbuddy_scan_sources(user_home);
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].alias, WORKBUDDY_HOME_DIR_NAME);
        assert_eq!(sources[1].alias, WORKBUDDY_INTERNATIONAL_HOME_DIR_NAME);
        assert_eq!(sources[0].path, user_home.join(WORKBUDDY_HOME_DIR_NAME));
        assert_eq!(
            sources[1].path,
            user_home.join(WORKBUDDY_INTERNATIONAL_HOME_DIR_NAME)
        );
        assert_ne!(sources[0].root_id, sources[1].root_id);
    }
}
