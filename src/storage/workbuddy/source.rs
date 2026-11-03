//! WorkBuddy 固定布局内的增量文件读取；只返回安全计量与完整行检查点。
use crate::readers::file_source::file_identity;
use crate::readers::workbuddy::{
    jsonl::{MAX_PROJECT_JSONL_BYTES, MAX_USAGE_RECORDS, open_fixed_prefix, parse_project_stream},
    projects::discover_project_files,
};
use crate::{
    CancellationSignal, LocalError, LocalErrorKind, WorkbuddyScanSource, WorkbuddyUsageEventRecord,
    stable_id,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Seek, SeekFrom},
    time::UNIX_EPOCH,
};

/// 来源只保存散列身份、文件观察和完整行进度，不保存原始会话身份或绝对路径。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Checkpoint {
    pub source_id: String,
    pub root_id: String,
    pub fingerprint: String,
    pub observed_size: u64,
    pub modified: String,
    pub parsed_offset: u64,
    pub sequence: u64,
    pub parser_version: u32,
}
/// 一份已读取或判定未变的文件；重建时先替换其旧事件集合。
pub(super) struct PreparedFile {
    pub checkpoint: Checkpoint,
    pub records: Vec<WorkbuddyUsageEventRecord>,
    pub rebuilt: bool,
    pub unchanged: bool,
}
/// 一轮固定根的完整枚举和安全增量结果，失败不会提交半份来源。
pub(super) struct PreparedUpdate {
    pub files: Vec<PreparedFile>,
    pub root_aliases: BTreeMap<String, String>,
    pub bytes_read: u64,
}

/// 在宿主受控 blocking 岛内枚举两根并续读完整行，所有 source 错误均失败关闭。
pub(super) fn prepare_update<C: CancellationSignal + ?Sized>(
    sources: &[WorkbuddyScanSource],
    known: &BTreeMap<String, Checkpoint>,
    force_rebuild: bool,
    cancellation: &C,
) -> Result<PreparedUpdate, LocalError> {
    if sources.len() > 2 {
        return Err(source_error());
    }
    let mut prepared = PreparedUpdate {
        files: Vec::new(),
        root_aliases: BTreeMap::new(),
        bytes_read: 0,
    };
    for source in sources {
        check_cancelled(cancellation)?;
        let files =
            discover_project_files(&source.path, cancellation).map_err(|_| source_error())?;
        if files.budget_exhausted || files.skipped_count > 0 || files.permission_denied_count > 0 {
            return Err(source_error());
        }
        prepared
            .root_aliases
            .insert(source.root_id.clone(), source.alias.clone());
        let mut remaining = MAX_PROJECT_JSONL_BYTES;
        let mut records_read = 0;
        for file in files.files {
            check_cancelled(cancellation)?;
            let source_id = stable_id(
                "workbuddy-source",
                &format!("{}\0{}", source.root_id, file.relative_key),
            );
            let (mut reader, size) = open_fixed_prefix(&file.path).map_err(|_| source_error())?;
            let metadata = reader.metadata().map_err(|_| source_error())?;
            let modified = metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
                .map(|value| value.as_nanos().to_string())
                .unwrap_or_default();
            let prefix = file_identity(&mut reader)?;
            let fingerprint = physical_identity(&metadata, &prefix);
            let previous = known.get(&source_id);
            let compatible = previous.is_some_and(|value| {
                value.parser_version == 1
                    && value.fingerprint == fingerprint
                    && value.parsed_offset <= size
            });
            let unchanged = !force_rebuild
                && compatible
                && previous
                    .is_some_and(|value| value.observed_size == size && value.modified == modified);
            if unchanged {
                prepared.files.push(PreparedFile {
                    checkpoint: previous.expect("unchanged requires known source").clone(),
                    records: Vec::new(),
                    rebuilt: false,
                    unchanged: true,
                });
                continue;
            }
            let append = !force_rebuild
                && compatible
                && previous.is_some_and(|value| size > value.observed_size);
            let start = if append {
                previous
                    .expect("append requires known source")
                    .parsed_offset
            } else {
                0
            };
            let length = size.checked_sub(start).ok_or_else(source_error)?;
            if length > remaining {
                return Err(source_error());
            }
            remaining -= length;
            reader
                .seek(SeekFrom::Start(start))
                .map_err(|_| source_error())?;
            let mut records = Vec::new();
            let report = parse_project_stream(
                reader.take(length),
                &file,
                &source.root_id,
                &mut records,
                cancellation,
            )
            .map_err(|_| source_error())?;
            if report.warning_count > 0 || report.record_budget_exhausted {
                return Err(source_error());
            }
            records_read += records.len();
            if records_read >= MAX_USAGE_RECORDS {
                return Err(source_error());
            }
            prepared.bytes_read = prepared.bytes_read.saturating_add(length);
            let parsed_offset = start
                .checked_add(report.committed_bytes)
                .ok_or_else(source_error)?;
            if parsed_offset.checked_add(report.trailing_bytes) != Some(size) {
                return Err(source_error());
            }
            prepared.files.push(PreparedFile {
                checkpoint: Checkpoint {
                    source_id,
                    root_id: source.root_id.clone(),
                    fingerprint,
                    observed_size: size,
                    modified,
                    parsed_offset,
                    sequence: if append {
                        previous.expect("append checkpoint").sequence
                    } else {
                        0
                    },
                    parser_version: 1,
                },
                records,
                rebuilt: !append,
                unchanged: false,
            });
        }
    }
    check_cancelled(cancellation)?;
    Ok(prepared)
}

/// Unix 持久化 inode 身份；其它平台以创建时间和前缀区分替换，缺少身份时保守重建。
fn physical_identity(metadata: &fs::Metadata, prefix: &str) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        format!("{}:{}:{prefix}", metadata.dev(), metadata.ino())
    }
    #[cfg(not(unix))]
    {
        let created = metadata
            .created()
            .ok()
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|value| value.as_nanos().to_string());
        created.map_or_else(
            || format!("unknown-{}-{prefix}", metadata.len()),
            |created| format!("{created}:{prefix}"),
        )
    }
}
/// 取消只停止本轮，不推进任何来源检查点。
fn check_cancelled<C: CancellationSignal + ?Sized>(cancellation: &C) -> Result<(), LocalError> {
    if cancellation.is_cancelled() {
        Err(source_error())
    } else {
        Ok(())
    }
}
/// 文件、格式和预算失败不携带路径或记录正文。
pub(super) fn source_error() -> LocalError {
    LocalError::new(
        LocalErrorKind::SourceUnavailable,
        "workbuddy source could not be indexed",
    )
}
