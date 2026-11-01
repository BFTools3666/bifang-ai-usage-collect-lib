//! WorkBuddy 固定本机布局的只读用量与 Trace 投影。
mod jsonl;
mod projects;
use self::jsonl::{open_fixed_prefix, read_project_usage};
use self::projects::{WorkbuddyProjectFiles, discover_project_files};
/// 与其他来源共用宿主取消协议，保留原路径作为兼容名称。
pub use crate::CancellationSignal as WorkbuddyCancellation;
use crate::readers::jsonl_lines::{CancellableRead, cancelled_io_error};
use crate::{
    WORKBUDDY_PROJECTS_DIR_NAME, WorkbuddyScanSource, WorkbuddyTraceRecord, WorkbuddyTraceStatus,
    discover_workbuddy_scan_sources, merge_workbuddy_coverage_reports,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufReader, Read};
use std::path::Path;

/// WorkBuddy trace 记录子目录固定名称。
const WORKBUDDY_TRACES_DIR_NAME: &str = "traces";
/// 单次读取最多扫描的 trace 文件数。
const MAX_TRACE_FILES: usize = 5_000;
/// 单次 Trace 诊断最多观察的目录项数，限制大量无效文件或目录造成的资源消耗。
const MAX_TRACE_DIRECTORY_ENTRIES: usize = 25_000;
/// 单个 trace 允许的最大字节数；超限只影响独立诊断，不影响用量。
const MAX_TRACE_FILE_BYTES: u64 = 64 * 1024 * 1024;
/// 单次 trace 诊断允许读取的总字节数。
const MAX_TRACE_TOTAL_BYTES: u64 = 512 * 1024 * 1024;

/// 标识 WorkBuddy 本地统计读取失败的稳定类别，不携带路径细节。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WorkbuddyReadError {
    /// 未定位安装目录，或获批 `projects` 根普通缺失。
    #[error("workbuddy source is unavailable")]
    SourceUnavailable,
    /// 固定根、`projects` 或文件在读取过程中不可安全读取。
    #[error("workbuddy source could not be read")]
    Read,
}

/// 数据源页可展示的 project JSONL 结构证据；不包含任何路径。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkbuddyProjectSourceEvidence {
    /// 顶层与 subagent transcript 文件总数。
    pub file_count: u64,
    /// 顶层 transcript 文件数。
    pub top_level_file_count: u64,
    /// subagent transcript 文件数。
    pub subagent_file_count: u64,
    /// 枚举时权限拒绝数量。
    pub permission_denied_count: u64,
    /// 链接、I/O 或不安全候选跳过数量。
    pub skipped_count: u64,
    /// 是否触发有界枚举预算。
    pub budget_exhausted: bool,
}

impl WorkbuddyProjectSourceEvidence {
    /// 从内部枚举结果复制无路径计数。
    fn from_files(files: &WorkbuddyProjectFiles) -> Self {
        Self {
            file_count: files.file_count(),
            top_level_file_count: files.top_level_file_count,
            subagent_file_count: files.subagent_file_count,
            permission_denied_count: files.permission_denied_count,
            skipped_count: files.skipped_count,
            budget_exhausted: files.budget_exhausted,
        }
    }
}
/// 在 GUI adapter 内分类两个固定根与其 `projects` 子目录；core 只负责路径与稳定身份推导。
///
/// 普通缺失表示该版本没有可读本机记录；链接、非目录和其它 I/O 失败都必须显式失败，
/// 避免统计、用量、数据源状态与 Collect 对同一个不安全根得出不同结论。
pub fn discover_workbuddy_sources_from_user_home(
    user_home: &Path,
) -> Result<Vec<WorkbuddyScanSource>, WorkbuddyReadError> {
    let mut discovered = Vec::new();
    for source in discover_workbuddy_scan_sources(user_home) {
        if !is_ordinary_workbuddy_directory(&source.path)? {
            continue;
        }
        if !is_ordinary_workbuddy_directory(&source.path.join(WORKBUDDY_PROJECTS_DIR_NAME))? {
            continue;
        }
        discovered.push(source);
    }
    Ok(discovered)
}

/// 把普通缺失与不安全/不可读路径稳定区分，且不会跟随链接读取元数据。
fn is_ordinary_workbuddy_directory(path: &Path) -> Result<bool, WorkbuddyReadError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(WorkbuddyReadError::Read),
    };
    if crate::readers::metadata_is_link_like(&metadata) || !metadata.is_dir() {
        return Err(WorkbuddyReadError::Read);
    }
    Ok(true)
}

/// 检查获批 project JSONL 布局并返回无路径证据；不会解析记录正文。
pub fn inspect_workbuddy_project_source_with_cancellation<C>(
    workbuddy_home: &Path,
    cancellation: &C,
) -> Result<WorkbuddyProjectSourceEvidence, WorkbuddyReadError>
where
    C: WorkbuddyCancellation + ?Sized,
{
    discover_project_files(workbuddy_home, cancellation)
        .map(|files| WorkbuddyProjectSourceEvidence::from_files(&files))
}

/// adapter 内一次读取的全部安全输入。
pub struct WorkbuddyUsageInputs {
    /// 已校验去重的逐请求用量记录。
    pub records: Vec<crate::WorkbuddyUsageEventRecord>,
    /// 本次读取的覆盖结论。
    pub coverage: crate::CoverageReport,
    /// 数据根 ID 到展示别名的映射。
    pub root_aliases: BTreeMap<String, String>,
}
/// 在固定两个候选根内顺序读取用量；缺失根可忽略，读取失败或部分覆盖保留失败关闭语义。
pub fn read_workbuddy_usage_inputs<C>(
    sources: &[WorkbuddyScanSource],
    cancellation: &C,
) -> Result<WorkbuddyUsageInputs, WorkbuddyReadError>
where
    C: WorkbuddyCancellation + ?Sized,
{
    let mut records = Vec::new();
    let mut coverages = Vec::new();
    let mut root_aliases = BTreeMap::new();
    for source in sources {
        if cancellation.is_cancelled() {
            return Err(WorkbuddyReadError::Read);
        }
        let usage = match read_project_usage(&source.path, &source.root_id, cancellation) {
            Ok(usage) => usage,
            Err(WorkbuddyReadError::SourceUnavailable) => continue,
            Err(WorkbuddyReadError::Read) => return Err(WorkbuddyReadError::Read),
        };
        records.extend(usage.records);
        coverages.push(usage.coverage);
        root_aliases.insert(source.root_id.clone(), source.alias.clone());
    }
    let coverage = merge_workbuddy_coverage_reports(&coverages)
        .ok_or(WorkbuddyReadError::SourceUnavailable)?;
    Ok(WorkbuddyUsageInputs {
        records,
        coverage,
        root_aliases,
    })
}

/// trace JSON 文件的最小反序列化形状；未声明的 spans/正文由 serde 丢弃。
#[derive(Deserialize)]
struct TraceFileEnvelope {
    /// trace 的元信息；spans 与正文由 serde 丢弃。
    trace: TraceMeta,
}

/// Trace 仅保留开始时间、耗时和整体状态。
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TraceMeta {
    /// trace 开始时间文本。
    started_at: String,
    /// trace 耗时。
    duration: i64,
    /// trace 整体状态文本。
    status: String,
}

/// 从全部固定 WorkBuddy 根合并独立 Trace 诊断；单根诊断失败仍沿用既有忽略语义。
pub fn read_traces_from_sources_with_cancellation<C>(
    sources: &[WorkbuddyScanSource],
    cancellation: &C,
) -> Vec<WorkbuddyTraceRecord>
where
    C: WorkbuddyCancellation + ?Sized,
{
    let mut records = Vec::new();
    for source in sources {
        if cancellation.is_cancelled() {
            break;
        }
        records.extend(read_traces_with_cancellation(&source.path, cancellation));
    }
    records
}

/// 只遍历 `traces/<pid>/*.json` 两层普通目录/文件，并应用文件、字节与取消边界。
pub fn read_traces_with_cancellation<C>(
    workbuddy_home: &Path,
    cancellation: &C,
) -> Vec<WorkbuddyTraceRecord>
where
    C: WorkbuddyCancellation + ?Sized,
{
    if cancellation.is_cancelled() {
        return Vec::new();
    }
    let traces_dir = workbuddy_home.join(WORKBUDDY_TRACES_DIR_NAME);
    let Ok(metadata) = fs::symlink_metadata(&traces_dir) else {
        return Vec::new();
    };
    if crate::readers::metadata_is_link_like(&metadata) || !metadata.is_dir() {
        return Vec::new();
    }
    let mut remaining_entries = MAX_TRACE_DIRECTORY_ENTRIES;
    let Ok(mut pid_entries) =
        sorted_entries_with_cancellation(&traces_dir, &mut remaining_entries, cancellation)
    else {
        return Vec::new();
    };
    let mut records = Vec::new();
    let mut remaining_bytes = MAX_TRACE_TOTAL_BYTES;
    for pid_entry in pid_entries.drain(..) {
        if cancellation.is_cancelled() {
            return records;
        }
        let pid_path = pid_entry.path();
        let Ok(pid_metadata) = fs::symlink_metadata(&pid_path) else {
            continue;
        };
        if crate::readers::metadata_is_link_like(&pid_metadata) || !pid_metadata.is_dir() {
            continue;
        }
        if remaining_entries == 0 {
            return records;
        }
        let Ok(file_entries) =
            sorted_entries_with_cancellation(&pid_path, &mut remaining_entries, cancellation)
        else {
            continue;
        };
        for file_entry in file_entries {
            if cancellation.is_cancelled()
                || records.len() >= MAX_TRACE_FILES
                || remaining_bytes == 0
            {
                return records;
            }
            let path = file_entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let Some((record, consumed)) =
                read_trace_file_with_cancellation(&path, remaining_bytes, cancellation)
            else {
                continue;
            };
            remaining_bytes = remaining_bytes.saturating_sub(consumed);
            records.push(record);
        }
    }
    records
}

/// 确定性读取一个目录的直接子项。
pub fn sorted_entries_with_cancellation<C>(
    directory: &Path,
    remaining_entries: &mut usize,
    cancellation: &C,
) -> std::io::Result<Vec<fs::DirEntry>>
where
    C: WorkbuddyCancellation + ?Sized,
{
    if cancellation.is_cancelled() {
        return Err(cancelled_io_error());
    }
    let mut entries = Vec::new();
    for entry in fs::read_dir(directory)? {
        if cancellation.is_cancelled() {
            return Err(cancelled_io_error());
        }
        if *remaining_entries == 0 {
            break;
        }
        *remaining_entries -= 1;
        entries.push(entry?);
    }
    entries.sort_by_key(fs::DirEntry::file_name);
    Ok(entries)
}

/// 从固定文件前缀流式解析一个完整 Trace；活动半写文件会自然被忽略。
pub fn read_trace_file_with_cancellation<C>(
    path: &Path,
    remaining_bytes: u64,
    cancellation: &C,
) -> Option<(WorkbuddyTraceRecord, u64)>
where
    C: WorkbuddyCancellation + ?Sized,
{
    if cancellation.is_cancelled() {
        return None;
    }
    let (file, observed_len) = open_fixed_prefix(path).ok()?;
    if observed_len == 0 || observed_len > MAX_TRACE_FILE_BYTES || observed_len > remaining_bytes {
        return None;
    }
    let is_cancelled = || cancellation.is_cancelled();
    let reader = CancellableRead {
        inner: BufReader::new(file.take(observed_len)),
        is_cancelled: &is_cancelled,
    };
    let envelope: TraceFileEnvelope = serde_json::from_reader(reader).ok()?;
    let status = match envelope.trace.status.as_str() {
        "ok" => WorkbuddyTraceStatus::Ok,
        "error" => WorkbuddyTraceStatus::Error,
        "cancelled" | "canceled" => WorkbuddyTraceStatus::Cancelled,
        _ => return None,
    };
    let started_at_epoch_ms = envelope
        .trace
        .started_at
        .parse::<jiff::Timestamp>()
        .ok()?
        .as_millisecond();
    Some((
        WorkbuddyTraceRecord {
            started_at_epoch_ms,
            duration_ms: envelope.trace.duration.max(0),
            status,
        },
        observed_len,
    ))
}
