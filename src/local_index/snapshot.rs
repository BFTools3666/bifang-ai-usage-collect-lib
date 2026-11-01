//! 客户端可自行存储的解析上下文与内存快照。
use crate::{CanonicalUsageSet, LocalIndexState};
/// 描述增量解析恢复所需的安全上下文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceParseCheckpoint {
    /// 恢复解析时使用的线程标识。
    pub thread_key: String,
    /// 恢复解析时使用的项目键。
    pub project_key: Option<String>,
    /// 受控项目末段。
    pub project_label: Option<String>,
    /// 可选短线程标题。
    pub thread_label: Option<String>,
    /// 恢复解析时使用的模型名。
    pub model: Option<String>,
    /// 恢复解析时使用的推理强度。
    pub reasoning_effort: Option<String>,
    /// 文件内下一个调用序号。
    pub call_sequence: u64,
    /// 内容无关的版本化解析状态，由消费客户端自行保存。
    pub adapter_state: Option<String>,
}
/// 保存同一个只读快照中取得的 canonical 调用与安全数据根记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageSnapshot {
    /// 当前 generation 的调用观察经 core 去重后的集合。
    pub canonical: CanonicalUsageSet,
    /// 从扫描与 parser 来源事实推导出的本机索引状态。
    pub index_state: LocalIndexState,
    /// 与调用读取处于同一存储快照的数据根安全记录。
    pub roots: Vec<super::RootRecord>,
}
