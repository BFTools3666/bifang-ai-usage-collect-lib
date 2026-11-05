//! 按 Agent 划分的根内路径枚举共用的计数、标签、窗口与结果结构。

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::SourceFileObservation;

/// 汇总一次扫描内跨目录、跨文件累积的可变计数与剩余预算。
#[derive(Default, Clone)]
pub struct ScanCounters {
    /// 剩余允许遍历的目录数预算。
    pub remaining_directories: u64,
    /// 剩余允许处理的目录项数预算。
    pub remaining_entries: u64,
    /// 本次扫描已处理的文件数。
    pub files_scanned: u64,
    /// 未发生变化、跳过重新索引的文件数。
    pub unchanged_files: u64,
    /// 因截断/替换等原因整份重建的文件数。
    pub rebuilt_files: u64,
    /// 本次扫描新增写入的调用条数。
    pub calls_added: u64,
    /// 权限拒绝计数。
    pub permission_denied_count: u64,
    /// 因链接、非本地卷等原因跳过的计数。
    pub skipped_count: u64,
    /// 格式与数据质量告警计数。
    pub warning_count: u64,
    /// 标识预算已耗尽，本次枚举未必完整。
    pub budget_exhausted: bool,
}

/// 一个根内按相对路径累积的保留与拒绝来源标签。
#[derive(Default)]
pub struct RootSourceLabels {
    /// 已确认应保留索引的来源相对标签集合。
    pub retained: HashSet<String>,
    /// 已确认应拒绝索引的来源相对标签集合。
    pub rejected: HashSet<String>,
}

/// 枚举结果：更新后的计数、标签、完整性与待索引路径；计数类型由各 Agent 决定。
pub struct EnumerateOutcome<C = ScanCounters> {
    /// 更新后的扫描计数。
    pub counters: C,
    /// 更新后的保留/拒绝来源标签。
    pub labels: RootSourceLabels,
    /// 标识本次枚举已完整覆盖该根，未因预算或错误提前中止。
    pub enumeration_complete: bool,
    /// 标识本次枚举因取消信号提前终止。
    pub cancelled: bool,
    /// 本次枚举确认的待索引文件路径。
    pub paths: Vec<PathBuf>,
}

/// 枚举一个根所需的窗口下界与上次 checkpoint 观察值。
pub struct EnumerateWindow {
    /// 从未索引过的文件只在修改时间不早于该下界时进入解析。
    pub scan_since_epoch_ms: i64,
    /// 该根下已索引来源上次记录的大小与修改时间，按相对标签索引。
    pub known_sources: HashMap<String, SourceFileObservation>,
}
