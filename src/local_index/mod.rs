//! 扫描错误、匿名来源标识和内存快照；不执行数据库操作。
mod error;
mod id_codec;
mod snapshot;
mod source_registry;
pub use error::{LocalError, LocalErrorKind};
pub use id_codec::{path_key, stable_id};
pub use snapshot::SourceParseCheckpoint;
pub use source_registry::{DiscoveryMethod, RegisteredRoot};
