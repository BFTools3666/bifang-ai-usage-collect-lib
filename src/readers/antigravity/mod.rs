//! Antigravity App / CLI 本机会话数据库的发现与计量读取。

pub mod discovery;
pub mod metadata;
#[cfg(feature = "storage")]
pub mod database;

pub use discovery::{
    AntigravityDiscoveredRoot, AntigravityDiscoveryInputs, AntigravityDiscoveryResult,
    discover_antigravity_full_device_with_progress, discover_antigravity_quick,
};

#[cfg(test)]
mod tests;
