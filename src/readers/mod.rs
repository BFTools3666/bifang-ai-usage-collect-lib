//! 本机来源的同步只读访问；异步调度和结果存储由调用方管理。
pub mod claude;
pub mod directory;
pub mod directory_cursor;
pub mod discovery;
pub(crate) mod enumerate_common;
pub mod file_source;
pub mod glm;
pub mod grok;
pub mod jsonl;
pub mod jsonl_lines;
pub mod user_paths;
pub mod volume_roots;
pub use crate::ScanCancellation as CancellationToken;
pub use crate::{DiscoveryMethod, LocalError, LocalErrorKind, RegisteredRoot, path_key, stable_id};
pub use discovery::{
    DiscoveredRoot, DiscoveryInputs, DiscoveryProgress, DiscoveryResult, FullDiscoveryOptions,
    discover_full_device_with_progress, discover_quick, walk_ancestors_for_link_component,
};
pub use file_source::same_opened_file;
pub use jsonl::DEFAULT_MAX_JSONL_LINE_BYTES;
pub use user_paths::{current_user_app_data_dir, current_user_home};
pub use volume_roots::{
    LocalPathStatus, LocalVolumeRoots, classify_local_path, enumerate_local_volume_roots,
    is_obviously_network_path,
};

/// WorkBuddy 的固定布局来源读取与安全投影。
pub mod workbuddy;
pub use discovery::metadata_is_link_like;

/// 只读取平台元数据与固定目录签名的来源发现。
pub mod metadata_discovery;
pub use claude::{ClaudeDiscoveryInputs, discover_claude_quick};
pub use discovery::validate_local_plain_directory;
pub use glm::{GlmDiscoveryInputs, discover_glm_quick};
#[cfg(test)]
pub(crate) use grok::jsonl::{
    PRODUCTION_GROK_SESSION_ENVELOPE_JSONL, SYNTHETIC_GROK_UPDATES_JSONL,
};
pub use grok::{GrokDiscoveryInputs, discover_grok_quick};
