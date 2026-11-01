//! Claude Code 的只读发现、文件结构校验与流式解析。
pub mod discovery;
pub mod jsonl;
pub mod path_rules;
pub use discovery::{
    ClaudeDiscoveredRoot, ClaudeDiscoveryInputs, ClaudeDiscoveryResult,
    discover_claude_full_device_with_progress, discover_claude_quick,
};
pub mod enumerate;

#[cfg(target_os = "linux")]
pub(crate) use discovery::claude_desktop_agent_priority_roots;
