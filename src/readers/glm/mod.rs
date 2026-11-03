//! ZCode 桌面与 CLI 的只读发现、用量白名单与独立索引。

#[cfg(feature = "storage")]
pub mod database;
pub mod discovery;
pub mod jsonl;
pub mod path_rules;
pub use discovery::{
    GlmDiscoveredRoot, GlmDiscoveryInputs, GlmDiscoveryResult,
    discover_glm_full_device_with_progress, discover_glm_quick,
};
pub mod enumerate;
#[cfg(test)]
mod jsonl_tests;

#[cfg(test)]
mod identity_tests;
