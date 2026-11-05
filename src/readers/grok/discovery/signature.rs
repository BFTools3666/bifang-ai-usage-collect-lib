//! Grok home 结构签名探测：只承认 `sessions/**/updates.jsonl`。

pub use super::super::super::discovery::agent_probe::{
    Inspection, SignatureBudget, coverage_state, crosses_search_root, is_excluded,
    reject_link_components,
};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;

use crate::RootCandidateEvidence;
use serde::Deserialize;

use super::super::super::discovery::metadata_is_link_like;
use super::super::path_rules::is_grok_updates_path;

const MAX_SIGNATURE_LINE_BYTES: u64 = 1024 * 1024;
const MAX_SIGNATURE_RECORDS_PER_FILE: u64 = 64;

#[derive(Deserialize)]
/// 仅反序列化 Grok 更新签名所需的外层事件字段。
struct SignatureEnvelope {
    /// 事件类型标记，只用于识别已完成轮次。
    #[serde(rename = "sessionUpdate")]
    session_update: Option<String>,
    /// 嵌套的 update 层。
    update: Option<Box<SignatureEnvelope>>,
    /// 嵌套的 params 层。
    params: Option<Box<SignatureEnvelope>>,
    /// 本层携带的用量计数。
    usage: Option<SignatureUsage>,
    /// 按模型拆分的用量；签名只关心其是否存在，保持原始 JSON。
    #[serde(rename = "modelUsage")]
    model_usage: Option<serde_json::Value>,
}

#[derive(Deserialize)]
/// 仅反序列化判断已完成用量所需的计数字段。
struct SignatureUsage {
    /// 输入 token 数。
    #[serde(rename = "inputTokens", alias = "input_tokens")]
    input_tokens: Option<u64>,
    /// 输出 token 数。
    #[serde(rename = "outputTokens", alias = "output_tokens")]
    output_tokens: Option<u64>,
    /// 总 token 数。
    #[serde(rename = "totalTokens", alias = "total_tokens")]
    total_tokens: Option<u64>,
}

/// 探测候选路径下的 `sessions` 子目录是否具备 Grok 会话结构。
pub fn inspect_root(path: &Path, budget: &mut SignatureBudget<'_>) -> Inspection {
    let sessions = path.join("sessions");
    let metadata = match fs::symlink_metadata(&sessions) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Inspection::NotRoot,
        Err(_) => return Inspection::Indeterminate,
    };
    if metadata_is_link_like(&metadata) || !metadata.is_dir() {
        return Inspection::Rejected;
    }
    inspect_sessions(path, &sessions, budget)
}

/// 在严格 sessions 目录内检查是否存在有效 Grok 更新文件。
fn inspect_sessions(root: &Path, sessions: &Path, budget: &mut SignatureBudget<'_>) -> Inspection {
    walk_for_updates(root, sessions, budget, 0)
}

/// 在共享预算内遍历会话子树并寻找 `updates.jsonl`。
fn walk_for_updates(
    root: &Path,
    directory: &Path,
    budget: &mut SignatureBudget<'_>,
    depth: u32,
) -> Inspection {
    if budget.cancellation.is_cancelled() {
        return Inspection::Cancelled;
    }
    if budget.remaining_directories == 0 {
        return Inspection::BudgetExhausted;
    }
    budget.remaining_directories -= 1;
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(_) => return Inspection::Indeterminate,
    };
    let mut indeterminate = false;
    for entry in entries {
        if budget.cancellation.is_cancelled() {
            return Inspection::Cancelled;
        }
        if budget.remaining_entries == 0 {
            return Inspection::BudgetExhausted;
        }
        budget.remaining_entries -= 1;
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                indeterminate = true;
                continue;
            }
        };
        let metadata = match fs::symlink_metadata(entry.path()) {
            Ok(metadata) => metadata,
            Err(_) => {
                indeterminate = true;
                continue;
            }
        };
        if metadata_is_link_like(&metadata) {
            continue;
        }
        if metadata.is_dir() {
            if depth >= 3 {
                continue;
            }
            match walk_for_updates(root, &entry.path(), budget, depth + 1) {
                Inspection::Found(evidence) => return Inspection::Found(evidence),
                Inspection::Cancelled => return Inspection::Cancelled,
                Inspection::BudgetExhausted => return Inspection::BudgetExhausted,
                Inspection::Indeterminate => indeterminate = true,
                Inspection::NotRoot | Inspection::Rejected => {}
            }
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        let entry_path = entry.path();
        let Ok(relative) = entry_path.strip_prefix(root) else {
            continue;
        };
        if !is_grok_updates_path(relative) {
            continue;
        }
        match inspect_updates_file(&entry.path(), budget) {
            Inspection::Found(evidence) => return Inspection::Found(evidence),
            Inspection::Cancelled => return Inspection::Cancelled,
            Inspection::BudgetExhausted => return Inspection::BudgetExhausted,
            Inspection::Indeterminate => indeterminate = true,
            Inspection::NotRoot | Inspection::Rejected => {}
        }
    }
    if indeterminate {
        Inspection::Indeterminate
    } else {
        Inspection::Rejected
    }
}

/// 读取单个有界更新文件并确认至少一个已完成用量事件。
fn inspect_updates_file(path: &Path, budget: &mut SignatureBudget<'_>) -> Inspection {
    if budget.remaining_files == 0 {
        return Inspection::BudgetExhausted;
    }
    budget.remaining_files -= 1;
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Inspection::Indeterminate,
    };
    let mut reader = BufReader::new(file);
    let mut records = 0_u64;
    let mut line = Vec::new();
    loop {
        if budget.cancellation.is_cancelled() {
            return Inspection::Cancelled;
        }
        if records >= MAX_SIGNATURE_RECORDS_PER_FILE {
            return Inspection::Rejected;
        }
        let (read, oversize) = match read_bounded_line(
            &mut reader,
            &mut line,
            budget.remaining_bytes,
            budget.cancellation,
        ) {
            Ok(LineRead::Eof) => return Inspection::Rejected,
            Ok(LineRead::Cancelled) => return Inspection::Cancelled,
            Ok(LineRead::Line(read)) => (read, false),
            Ok(LineRead::Oversize(read)) => (read, true),
            Err(_) => return Inspection::Indeterminate,
        };
        if budget.remaining_bytes < read {
            return Inspection::BudgetExhausted;
        }
        budget.remaining_bytes -= read;
        if oversize {
            continue;
        }
        records += 1;
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        if line_has_completed_usage(&line) {
            return Inspection::Found(RootCandidateEvidence::GrokSessionUpdates);
        }
    }
}

/// 一次有界读行的结果；超长行不会进入缓冲，只记录已消耗的字节数。
#[derive(Debug, PartialEq, Eq)]
enum LineRead {
    /// 在读取块之间收到取消，调用方不得把未完成读取判为否定签名。
    Cancelled,
    /// 文件结束，没有更多内容。
    Eof,
    /// 不超过上限的一行（含换行符）已读入缓冲，值为消耗的字节数。
    Line(u64),
    /// 超过上限的一行：已丢弃其余内容，值为消耗的字节数（可能超过 `byte_allowance`）。
    Oversize(u64),
}

/// 读取一行，缓冲最多保留 `MAX_SIGNATURE_LINE_BYTES + 1` 字节。
///
/// 超长行的剩余部分只被消耗、不被缓冲；累计消耗超过 `byte_allowance` 即停止并返回，
/// 由调用方按预算耗尽处理，避免一个没有换行的巨大文件被整行读入内存。
fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
    byte_allowance: u64,
    cancellation: &impl crate::CancellationSignal,
) -> std::io::Result<LineRead> {
    // 即使调用方的 BufRead 一次暴露整文件，也只消费一个固定块后重新检查取消。
    const READ_CHUNK_BYTES: usize = 8 * 1024;
    line.clear();
    let mut consumed = 0_u64;
    let mut oversize = false;
    loop {
        if cancellation.is_cancelled() {
            line.clear();
            return Ok(LineRead::Cancelled);
        }
        let buffered = reader.fill_buf()?;
        if buffered.is_empty() {
            return Ok(if consumed == 0 {
                LineRead::Eof
            } else if oversize {
                LineRead::Oversize(consumed)
            } else {
                LineRead::Line(consumed)
            });
        }
        let chunk = &buffered[..buffered.len().min(READ_CHUNK_BYTES)];
        let newline = chunk.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(chunk.len(), |position| position + 1);
        consumed = consumed.saturating_add(take as u64);
        if !oversize && consumed <= MAX_SIGNATURE_LINE_BYTES {
            line.extend_from_slice(&chunk[..take]);
        } else {
            oversize = true;
            line.clear();
        }
        reader.consume(take);
        if newline.is_some() || (oversize && consumed > byte_allowance) {
            return Ok(if oversize {
                LineRead::Oversize(consumed)
            } else {
                LineRead::Line(consumed)
            });
        }
    }
}

/// 判断一行 JSON 是否携带可计入索引的已完成总量。
fn line_has_completed_usage(line: &[u8]) -> bool {
    let Ok(record) = serde_json::from_slice::<SignatureEnvelope>(line) else {
        return false;
    };
    let payload = signature_usage_payload(&record);
    let session_update = payload.session_update.as_deref().unwrap_or_default();
    if session_update != "turn_completed" {
        return false;
    }
    let has_model_usage = payload
        .model_usage
        .as_ref()
        .is_some_and(|value| value.as_object().is_some_and(|map| !map.is_empty()));
    has_model_usage
        || payload.usage.as_ref().is_some_and(|item| {
            item.total_tokens.is_some()
                || (item.input_tokens.is_some() && item.output_tokens.is_some())
        })
}

/// 生产 `updates.jsonl` 把 turn 放在 `params.update`；旧夹具则在顶层或 `update`。
fn signature_usage_payload(record: &SignatureEnvelope) -> &SignatureEnvelope {
    record
        .params
        .as_ref()
        .and_then(|params| params.update.as_ref())
        .or(record.update.as_ref())
        .map_or(record, Box::as_ref)
}

/// 取路径末段作为安全展示别名。
pub fn safe_alias(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("Grok 数据根")
        .to_owned()
}

#[cfg(test)]
mod bounded_line_tests {
    use super::*;
    use std::io::Cursor;

    /// 已超过单行上限后仍逐块响应取消，不必读完整个没有换行的文件。
    #[test]
    fn cancellation_during_oversize_discard_stops_before_eof() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let total = (MAX_SIGNATURE_LINE_BYTES * 3) as usize;
        let mut reader = Cursor::new(vec![b'x'; total]);
        let checks = AtomicUsize::new(0);
        let cancellation = || checks.fetch_add(1, Ordering::SeqCst) > 130;
        let mut line = Vec::new();
        assert_eq!(
            read_bounded_line(&mut reader, &mut line, u64::MAX, &cancellation)
                .expect("cancellation is a normal outcome"),
            LineRead::Cancelled
        );
        assert!(reader.position() > MAX_SIGNATURE_LINE_BYTES);
        assert!(reader.position() < total as u64);
        assert!(line.is_empty());
    }

    /// 普通行按原样读入，含换行符，字节数与读入长度一致。
    #[test]
    fn short_lines_are_read_whole() {
        let mut reader = Cursor::new(b"{\"a\":1}\nnext\n".to_vec());
        let mut line = Vec::new();
        assert_eq!(
            read_bounded_line(
                &mut reader,
                &mut line,
                u64::MAX,
                &crate::ScanCancellation::new()
            )
            .expect("read"),
            LineRead::Line(8)
        );
        assert_eq!(line, b"{\"a\":1}\n");
        assert_eq!(
            read_bounded_line(
                &mut reader,
                &mut line,
                u64::MAX,
                &crate::ScanCancellation::new()
            )
            .expect("read"),
            LineRead::Line(5)
        );
    }

    /// 没有换行的超大内容不会整体进入缓冲：缓冲被清空，消耗字节数仍如实返回。
    #[test]
    fn oversize_line_without_newline_is_not_buffered() {
        let total = MAX_SIGNATURE_LINE_BYTES * 3;
        let mut reader = Cursor::new(vec![b'x'; total as usize]);
        let mut line = Vec::new();
        let result = read_bounded_line(
            &mut reader,
            &mut line,
            u64::MAX,
            &crate::ScanCancellation::new(),
        )
        .expect("read");
        assert_eq!(result, LineRead::Oversize(total));
        assert!(line.is_empty());
        assert!(line.capacity() <= (MAX_SIGNATURE_LINE_BYTES as usize) * 2);
    }

    /// 超长行之后的下一行仍可正常读取，且不被超长行吞掉。
    #[test]
    fn the_line_after_an_oversize_line_is_intact() {
        let mut data = vec![b'x'; (MAX_SIGNATURE_LINE_BYTES + 10) as usize];
        data.push(b'\n');
        data.extend_from_slice(b"ok\n");
        let mut reader = Cursor::new(data);
        let mut line = Vec::new();
        assert!(matches!(
            read_bounded_line(
                &mut reader,
                &mut line,
                u64::MAX,
                &crate::ScanCancellation::new()
            )
            .expect("read"),
            LineRead::Oversize(_)
        ));
        assert_eq!(
            read_bounded_line(
                &mut reader,
                &mut line,
                u64::MAX,
                &crate::ScanCancellation::new()
            )
            .expect("read"),
            LineRead::Line(3)
        );
        assert_eq!(line, b"ok\n");
    }

    /// 超长行的恰好上限+1 字节且以换行结尾时，不会多吞下一行。
    #[test]
    fn oversize_line_ending_exactly_at_the_read_limit_does_not_swallow_the_next_line() {
        let mut data = vec![b'x'; MAX_SIGNATURE_LINE_BYTES as usize];
        data.push(b'\n');
        data.extend_from_slice(b"ok\n");
        let mut reader = Cursor::new(data);
        let mut line = Vec::new();
        assert_eq!(
            read_bounded_line(
                &mut reader,
                &mut line,
                u64::MAX,
                &crate::ScanCancellation::new()
            )
            .expect("read"),
            LineRead::Oversize(MAX_SIGNATURE_LINE_BYTES + 1)
        );
        assert_eq!(
            read_bounded_line(
                &mut reader,
                &mut line,
                u64::MAX,
                &crate::ScanCancellation::new()
            )
            .expect("read"),
            LineRead::Line(3)
        );
    }

    /// 超出字节预算时提前停止丢弃，返回值大于预算由调用方判定耗尽，不读完整个流。
    #[test]
    fn discarding_stops_once_the_byte_allowance_is_exceeded() {
        let total = MAX_SIGNATURE_LINE_BYTES * 8;
        let mut reader = Cursor::new(vec![b'x'; total as usize]);
        let mut line = Vec::new();
        let allowance = MAX_SIGNATURE_LINE_BYTES;
        match read_bounded_line(
            &mut reader,
            &mut line,
            allowance,
            &crate::ScanCancellation::new(),
        )
        .expect("read")
        {
            LineRead::Oversize(consumed) => {
                assert!(consumed > allowance);
                assert!(consumed < total);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
