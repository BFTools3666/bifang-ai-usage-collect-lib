//! 同步目录枚举与签名探测；生产调用按可续读分片消费，不同时保留整目录文件句柄。
//! 整目录入口只作兼容壳，不按每条 DirEntry 创建 blocking 任务。

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use crate::{SourceFileObservation, source_file_needs_visit};

use crate::readers::CancellationToken;
use crate::readers::LocalError;
use crate::readers::directory_cursor::DirectoryCursor;
use crate::readers::discovery::{metadata_is_link_like, path_key};
use crate::readers::file_source::{
    RolloutFileInspection, RolloutProbeBudget, ValidatedRolloutFile, inspect_rollout_file,
    is_rollout_jsonl, modified_epoch_ms,
};

#[cfg(test)]
#[path = "directory_chunk_tests.rs"]
mod chunk_tests;

/// 目录访问后需要由 async 扫描壳继续处理的动作。
pub enum EntryAction {
    /// 继续深入子目录。
    PushDirectory(PathBuf),
    /// 仅保留相对标签，不索引。
    RetainLabel(String),
    /// 确认拒绝的相对标签。
    RejectLabel(String),
    /// 已通过签名探测，交给 async 侧串行索引。
    IndexFile {
        /// 根内相对标签，可作为调用方的来源键。
        relative_label: String,
        /// 已通过签名与同一文件句柄检查的只读来源。
        source: ValidatedRolloutFile,
    },
}

/// 一次目录级 blocking 访问的汇总结果。
pub struct DirectoryVisitOutcome {
    /// 访问后剩余允许处理的目录项数预算。
    pub remaining_entries: u64,
    /// 访问后剩余的签名探测预算。
    pub probe_budget: RolloutProbeBudget,
    /// 权限拒绝计数。
    pub permission_denied: u64,
    /// 因链接、非本地卷等原因跳过的计数。
    pub skipped: u64,
    /// 格式与数据质量告警计数。
    pub warnings: u64,
    /// 需要由 async 侧继续处理的动作列表。
    pub actions: Vec<EntryAction>,
    /// 标识本目录已完整枚举，未因预算或错误提前中止。
    pub enumeration_complete: bool,
    /// 标识本次访问因取消信号提前终止。
    pub cancelled: bool,
    /// 标识预算已耗尽，本次访问未必完整。
    pub budget_exhausted: bool,
}

/// 一个根区域内枚举所需的窗口下界与上次 checkpoint 观察值；跨目录岛共享。
pub struct VisitWindow {
    /// 从未索引过的文件只在修改时间不早于该下界时进入解析。
    pub scan_since_epoch_ms: i64,
    /// 该根区域下已索引来源上次记录的大小与修改时间，按相对标签索引。
    pub known_sources: Arc<HashMap<String, SourceFileObservation>>,
}

/// 单个分片限制工作集，不限制目录总项数或总批次数。
const DIRECTORY_CHUNK_ENTRIES: usize = 128;

/// 一个可续读目录分片；必须消费 continuation 才能证明整目录枚举完成。
/// outcome 的 enumeration_complete 只表示本片未因错误、取消或预算中断。
pub struct DirectoryVisitChunk {
    /// 本片的脱敏计数及最多一个持有原始只读句柄的匹配文件。
    pub outcome: DirectoryVisitOutcome,
    /// 下一片继续使用的同一目录句柄；None 表示目录结束或本次访问已中断。
    pub continuation: Option<DirectoryCursor>,
}

/// 显式整目录兼容入口；产品和包装层应使用分片入口以限制同时持有的文件句柄。
pub fn visit_directory(
    directory: PathBuf,
    root_path: PathBuf,
    mut remaining_entries: u64,
    window: &VisitWindow,
    cancellation: &CancellationToken,
    mut probe_budget: RolloutProbeBudget,
) -> Result<DirectoryVisitOutcome, LocalError> {
    let mut cursor = DirectoryCursor::new(directory);
    let mut result = DirectoryVisitOutcome {
        remaining_entries,
        probe_budget,
        permission_denied: 0,
        skipped: 0,
        warnings: 0,
        actions: Vec::new(),
        enumeration_complete: true,
        cancelled: false,
        budget_exhausted: false,
    };
    loop {
        let chunk = visit_directory_chunk(
            cursor,
            root_path.clone(),
            remaining_entries,
            window,
            cancellation,
            probe_budget,
        )?;
        let outcome = chunk.outcome;
        remaining_entries = outcome.remaining_entries;
        probe_budget = outcome.probe_budget;
        result.remaining_entries = remaining_entries;
        result.probe_budget = probe_budget;
        result.permission_denied = result
            .permission_denied
            .saturating_add(outcome.permission_denied);
        result.skipped = result.skipped.saturating_add(outcome.skipped);
        result.warnings = result.warnings.saturating_add(outcome.warnings);
        result.actions.extend(outcome.actions);
        result.enumeration_complete &= outcome.enumeration_complete;
        result.cancelled |= outcome.cancelled;
        result.budget_exhausted |= outcome.budget_exhausted;
        let Some(next) = chunk.continuation else {
            return Ok(result);
        };
        cursor = next;
    }
}

/// 读取并预检一个目录；预算与取消在岛内边界检查。
pub fn visit_directory_chunk(
    mut cursor: DirectoryCursor,
    root_path: PathBuf,
    mut remaining_entries: u64,
    window: &VisitWindow,
    cancellation: &CancellationToken,
    mut probe_budget: RolloutProbeBudget,
) -> Result<DirectoryVisitChunk, LocalError> {
    let mut permission_denied = 0_u64;
    let mut skipped = 0_u64;
    let mut warnings = 0_u64;
    let mut actions = Vec::new();
    let mut enumeration_complete = true;
    let mut cancelled = false;
    let mut budget_exhausted = false;
    let mut continuation = None;
    let mut processed = 0;
    loop {
        if cancellation.is_cancelled() {
            cancelled = true;
            enumeration_complete = false;
            break;
        }
        if processed == DIRECTORY_CHUNK_ENTRIES {
            continuation = Some(cursor);
            break;
        }
        let unopened = !cursor.started();
        let entry = match cursor.next_entry() {
            Ok(Some(entry)) => Ok(entry),
            Ok(None) => break,
            Err(error) if unopened => {
                return Err(crate::readers::file_source::map_source_io_error(error));
            }
            Err(error) => Err(error),
        };
        processed += 1;
        if remaining_entries == 0 {
            enumeration_complete = false;
            budget_exhausted = true;
            skipped = skipped.saturating_add(1);
            warnings = warnings.saturating_add(1);
            break;
        }
        remaining_entries -= 1;
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                enumeration_complete = false;
                if error.kind() == std::io::ErrorKind::PermissionDenied {
                    permission_denied = permission_denied.saturating_add(1);
                } else {
                    skipped = skipped.saturating_add(1);
                }
                warnings = warnings.saturating_add(1);
                continue;
            }
        };
        let path = entry.path();
        let relative_label = is_rollout_jsonl(&path)
            .then(|| path.strip_prefix(&root_path).ok())
            .flatten()
            .map(path_key);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(_) => {
                if let Some(relative_label) = relative_label {
                    actions.push(EntryAction::RetainLabel(relative_label));
                }
                enumeration_complete = false;
                skipped = skipped.saturating_add(1);
                warnings = warnings.saturating_add(1);
                continue;
            }
        };
        if metadata_is_link_like(&metadata) {
            if let Some(relative_label) = relative_label {
                actions.push(EntryAction::RejectLabel(relative_label));
            } else {
                skipped = skipped.saturating_add(1);
                warnings = warnings.saturating_add(1);
            }
            continue;
        }
        if metadata.is_dir() {
            if let Some(relative_label) = relative_label {
                actions.push(EntryAction::RejectLabel(relative_label));
            }
            actions.push(EntryAction::PushDirectory(path));
            continue;
        }
        let Some(relative_label) = relative_label else {
            continue;
        };
        if !metadata.is_file() {
            actions.push(EntryAction::RejectLabel(relative_label));
            continue;
        }
        // 窗口外但被追加/替换过的已索引文件必须重新解析，否则新增字节会永久丢失。
        if !source_file_needs_visit(
            modified_epoch_ms(&metadata),
            metadata.len(),
            window.scan_since_epoch_ms,
            window.known_sources.get(&relative_label).copied(),
        ) {
            actions.push(EntryAction::RetainLabel(relative_label));
            continue;
        }
        match inspect_rollout_file(&path, cancellation, &mut probe_budget) {
            RolloutFileInspection::Matched(source) => {
                actions.push(EntryAction::IndexFile {
                    relative_label,
                    source,
                });
                continuation = Some(cursor);
                break;
            }
            RolloutFileInspection::Rejected => {
                actions.push(EntryAction::RejectLabel(relative_label));
            }
            RolloutFileInspection::Indeterminate => {
                actions.push(EntryAction::RetainLabel(relative_label));
                skipped = skipped.saturating_add(1);
                warnings = warnings.saturating_add(1);
            }
            RolloutFileInspection::Cancelled => {
                cancelled = true;
                enumeration_complete = false;
                break;
            }
            RolloutFileInspection::BudgetExhausted => {
                skipped = skipped.saturating_add(1);
                warnings = warnings.saturating_add(1);
                enumeration_complete = false;
                budget_exhausted = true;
                break;
            }
        }
    }
    Ok(DirectoryVisitChunk {
        outcome: DirectoryVisitOutcome {
            remaining_entries,
            probe_budget,
            permission_denied,
            skipped,
            warnings,
            actions,
            enumeration_complete,
            cancelled,
            budget_exhausted,
        },
        continuation,
    })
}
