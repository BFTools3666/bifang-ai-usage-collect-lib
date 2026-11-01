//! 定义调用方登记的数据根输入与发现方式，不实现登记存储。

use std::path::PathBuf;

/// 标识数据根是由哪个受控入口发现或登记的。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryMethod {
    /// 当前用户主目录下的默认 `.codex`。
    DefaultHome,
    /// 当前进程 `CODEX_HOME` 指向的数据根。
    Environment,
    /// 用户明确登记并可随时禁用的数据根。
    Registered,
    /// 用户主动发起全设备发现得到的候选根。
    FullDevice,
    /// 元数据发现经用户确认得到的候选根。
    MetadataDiscovery,
}

// 稳定文本标签供消费方序列化或持久化；未知标签返回 None，不推断新语义。
impl DiscoveryMethod {
    /// 返回与消费客户端兼容的稳定短标签。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DefaultHome => "default_home",
            Self::Environment => "environment",
            Self::Registered => "registered",
            Self::FullDevice => "full_device",
            Self::MetadataDiscovery => "metadata_discovery",
        }
    }

    /// 从已知稳定标签恢复发现方式。
    pub fn from_label(value: &str) -> Option<Self> {
        match value {
            "default_home" => Some(Self::DefaultHome),
            "environment" => Some(Self::Environment),
            "registered" => Some(Self::Registered),
            "full_device" => Some(Self::FullDevice),
            "metadata_discovery" => Some(Self::MetadataDiscovery),
            _ => None,
        }
    }

    /// 重复物理目录只允许更高信任入口覆盖展示身份。
    pub const fn priority(self) -> u8 {
        match self {
            Self::Registered => 3,
            Self::Environment => 2,
            Self::DefaultHome => 1,
            Self::FullDevice | Self::MetadataDiscovery => 0,
        }
    }

    /// 缺省 `.codex`/`.claude` 目录本就是可选状态；只有非默认目录方式，
    /// 或该候选对应一个已登记根需要重新校验，才计入覆盖缺口。
    pub const fn counts_as_coverage_gap(self, has_existing_root_id: bool) -> bool {
        !matches!(self, Self::DefaultHome) || has_existing_root_id
    }
}

/// 表示用户明确维护的自定义数据根输入。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredRoot {
    /// 已持久化根的精确稳定 ID；尚未登记的新候选为空。
    pub root_id: Option<String>,
    /// 只在 adapter 内用于只读访问和 registry 持久化的绝对或调用方解析路径。
    pub path: PathBuf,
    /// 默认向 GUI 展示的用户别名，不要求暴露完整路径。
    pub alias: String,
    /// 禁用的数据根保留登记信息但不参与快速扫描。
    pub enabled: bool,
}
