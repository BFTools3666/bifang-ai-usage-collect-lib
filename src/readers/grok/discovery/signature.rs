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
        line.clear();
        let read = match reader.read_until(b'\n', &mut line) {
            Ok(0) => return Inspection::Rejected,
            Ok(read) => read as u64,
            Err(_) => return Inspection::Indeterminate,
        };
        if budget.remaining_bytes < read {
            return Inspection::BudgetExhausted;
        }
        budget.remaining_bytes -= read;
        if read > MAX_SIGNATURE_LINE_BYTES {
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
