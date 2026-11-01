//! Grok 的只读发现、文件结构校验与流式解析。
pub mod discovery;
pub mod jsonl;
pub mod path_rules;
pub use discovery::{
    GrokDiscoveredRoot, GrokDiscoveryInputs, GrokDiscoveryResult,
    discover_grok_full_device_with_progress, discover_grok_quick,
};
pub mod enumerate;
#[cfg(test)]
mod jsonl_tests;
