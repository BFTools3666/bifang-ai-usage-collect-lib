//! 根据调用方传入的观察值和时间下界，决定来源是否需要重新读取。

/// 上次成功索引某来源时记录下来的文件事实，用于判断文件是否已变化。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceFileObservation {
    /// 上次 checkpoint 记录的文件字节数。
    pub observed_size: u64,
    /// 上次 checkpoint 记录的修改时间毫秒值。
    pub modified_at_epoch_ms: i64,
}

/// 决定枚举阶段是否要打开并解析一个候选来源文件。
///
/// - 修改时间落在扫描窗口内的文件始终进入解析（增量或重建由 adapter 决定）。
/// - 修改时间早于窗口、且从未索引过的文件不进入本轮，这是控制首次/周期成本的窄口。
/// - 修改时间早于窗口、但大小或修改时间与上次 checkpoint 不一致的文件必须进入
///   解析：它在两次扫描之间被追加或替换过，只按窗口跳过会让这些字节永远丢失。
pub fn source_file_needs_visit(
    modified_at_epoch_ms: i64,
    observed_size: u64,
    scan_since_epoch_ms: i64,
    checkpoint: Option<SourceFileObservation>,
) -> bool {
    if modified_at_epoch_ms >= scan_since_epoch_ms {
        return true;
    }
    match checkpoint {
        None => false,
        Some(checkpoint) => {
            checkpoint.observed_size != observed_size
                || checkpoint.modified_at_epoch_ms != modified_at_epoch_ms
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 窗口内文件不论是否已索引都必须进入解析。
    #[test]
    fn files_modified_inside_the_window_are_always_visited() {
        assert!(source_file_needs_visit(1_000, 10, 1_000, None));
        assert!(source_file_needs_visit(
            1_500,
            10,
            1_000,
            Some(SourceFileObservation {
                observed_size: 10,
                modified_at_epoch_ms: 1_500,
            })
        ));
    }

    /// 窗口外且从未索引的文件是唯一允许跳过的“未知”来源。
    #[test]
    fn unknown_files_outside_the_window_are_skipped() {
        assert!(!source_file_needs_visit(999, 10, 1_000, None));
    }

    /// 窗口外但检查点一致的文件不重复打开。
    #[test]
    fn unchanged_indexed_files_outside_the_window_are_skipped() {
        assert!(!source_file_needs_visit(
            500,
            10,
            1_000,
            Some(SourceFileObservation {
                observed_size: 10,
                modified_at_epoch_ms: 500,
            })
        ));
    }

    /// 窗口外但大小或修改时间与检查点不一致的文件必须重新解析。
    #[test]
    fn changed_indexed_files_outside_the_window_are_visited() {
        let checkpoint = Some(SourceFileObservation {
            observed_size: 10,
            modified_at_epoch_ms: 400,
        });
        assert!(source_file_needs_visit(500, 10, 1_000, checkpoint));
        assert!(source_file_needs_visit(400, 12, 1_000, checkpoint));
    }
}
