//! GLM rollout 根内 `model-io-sess_*.jsonl` 的同步枚举岛。

use std::fs;

use crate::source_file_needs_visit;

use crate::readers::CancellationToken;
use crate::readers::discovery::metadata_is_link_like;
use crate::readers::discovery::path_key;
use crate::readers::file_source::modified_epoch_ms;

use super::discovery::GlmDiscoveredRoot;
use super::path_rules::is_glm_model_io_path;

/// 汇总一次扫描内跨目录、跨文件累积的可变计数与剩余预算。
#[derive(Default, Clone)]
pub struct GlmScanCounters {
    /// 剩余允许检查的目录项预算。
    pub remaining_entries: u64,
    /// 已扫描的文件数。
    pub files_scanned: u64,
    /// 内容未变化、无需重新索引的文件数。
    pub unchanged_files: u64,
    /// 因版本或身份变化而整文件重建的文件数。
    pub rebuilt_files: u64,
    /// 本次新增入库的调用数。
    pub calls_added: u64,
    /// 因权限不足被跳过的位置数。
    pub permission_denied_count: u64,
    /// 因规则或错误被跳过的位置数。
    pub skipped_count: u64,
    /// 解析过程中累计的警告数。
    pub warning_count: u64,
    /// 预算是否已耗尽；耗尽后覆盖结论只能是部分。
    pub budget_exhausted: bool,
}

pub use crate::readers::enumerate_common::{EnumerateOutcome, EnumerateWindow, RootSourceLabels};

/// ZCode 的来源标签与共用结构同形，保留原有公开名称。
pub type GlmRootSourceLabels = RootSourceLabels;

/// ZCode 的枚举窗口与共用结构同形，保留原有公开名称。
pub type GlmEnumerateWindow = EnumerateWindow;

/// ZCode 的枚举结果使用自己的计数类型。
pub type GlmEnumerateOutcome = EnumerateOutcome<GlmScanCounters>;

/// 在 blocking 岛内枚举一个 GLM rollout 根下全部可索引会话文件。
///
/// rollout 根是平铺目录，只有一层普通文件；子目录一律拒绝，不递归。
pub fn enumerate_root_model_io(
    root: &GlmDiscoveredRoot,
    mut counters: GlmScanCounters,
    window: &GlmEnumerateWindow,
    cancellation: &CancellationToken,
) -> GlmEnumerateOutcome {
    let mut labels = GlmRootSourceLabels::default();
    let mut enumeration_complete = true;
    let mut cancelled = false;
    let mut paths = Vec::new();
    let entries = match fs::read_dir(&root.path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            counters.permission_denied_count = counters.permission_denied_count.saturating_add(1);
            enumeration_complete = false;
            return GlmEnumerateOutcome {
                counters,
                labels,
                enumeration_complete,
                cancelled,
                paths,
            };
        }
        Err(_) => {
            counters.skipped_count = counters.skipped_count.saturating_add(1);
            enumeration_complete = false;
            return GlmEnumerateOutcome {
                counters,
                labels,
                enumeration_complete,
                cancelled,
                paths,
            };
        }
    };
    for entry in entries {
        if cancellation.is_cancelled() {
            cancelled = true;
            enumeration_complete = false;
            break;
        }
        if counters.remaining_entries == 0 {
            counters.budget_exhausted = true;
            enumeration_complete = false;
            break;
        }
        counters.remaining_entries = counters.remaining_entries.saturating_sub(1);
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                counters.skipped_count = counters.skipped_count.saturating_add(1);
                enumeration_complete = false;
                continue;
            }
        };
        let metadata = match fs::symlink_metadata(entry.path()) {
            Ok(metadata) => metadata,
            Err(_) => {
                counters.skipped_count = counters.skipped_count.saturating_add(1);
                enumeration_complete = false;
                continue;
            }
        };
        if metadata_is_link_like(&metadata) {
            continue;
        }
        if !metadata.is_file() {
            labels
                .rejected
                .insert(entry.path().strip_prefix(&root.path).map_or_else(
                    |_| entry.file_name().to_string_lossy().into_owned(),
                    |relative| relative.to_string_lossy().into_owned(),
                ));
            continue;
        }
        let entry_path = entry.path();
        let Ok(relative) = entry_path.strip_prefix(&root.path) else {
            continue;
        };
        if is_glm_model_io_path(relative) {
            let relative_label = path_key(relative);
            counters.files_scanned = counters.files_scanned.saturating_add(1);
            // 窗口外但被追加/替换过的已索引文件必须重新解析，否则新增字节会永久丢失。
            if source_file_needs_visit(
                modified_epoch_ms(&metadata),
                metadata.len(),
                window.scan_since_epoch_ms,
                window.known_sources.get(&relative_label).copied(),
            ) {
                paths.push(entry_path);
            } else {
                labels.retained.insert(relative_label);
            }
        } else {
            labels
                .rejected
                .insert(relative.to_string_lossy().into_owned());
        }
    }
    GlmEnumerateOutcome {
        counters,
        labels,
        enumeration_complete,
        cancelled,
        paths,
    }
}
