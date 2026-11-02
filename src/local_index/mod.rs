//! 扫描错误、匿名来源标识和内存快照；不执行数据库操作。
mod error;
mod id_codec;
mod snapshot;
mod source_registry;
pub use error::{LocalError, LocalErrorKind};
pub use id_codec::{path_key, stable_id};
pub use snapshot::SourceParseCheckpoint;
pub use source_registry::{DiscoveryMethod, RegisteredRoot};

mod information;
mod models;
mod state;
pub use information::{
    RootActivationState, RootRecord, UsageSnapshot, coverage_state_from_label, coverage_state_label,
};
pub use models::{
    ClaudeBatchOutcome, CodexCallWriteDecision, IndexedTokenUsage, StoredSourceFile,
    claude_usage_observation_is_monotonic, codex_call_write_decision,
};
pub use state::{LocalIndexState, local_index_state_from_facts};
