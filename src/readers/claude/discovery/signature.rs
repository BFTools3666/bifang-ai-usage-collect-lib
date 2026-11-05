//! Claude transcript 结构签名探测与有界预算。

pub use super::super::super::discovery::agent_probe::{
    Inspection, SignatureBudget, coverage_state, crosses_search_root, is_excluded,
    reject_link_components,
};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use crate::{RootCandidateEvidence, SourceClientKind};
use jiff::Timestamp;
use serde::Deserialize;

use super::super::super::discovery::metadata_is_link_like;
use super::super::path_rules::{is_subagent_jsonl_name, is_uuid, is_uuid_jsonl_name};

/// 单条结构签名记录允许的最大字节数，超出即视为拒绝证据。
const MAX_SIGNATURE_LINE_BYTES: u64 = 1024 * 1024;
/// 每个 transcript 文件最多尝试读取的记录条数上限。
const MAX_SIGNATURE_RECORDS_PER_FILE: u64 = 64;

/// 结构签名探测只反序列化的最小字段集合，缺失字段一律容忍为 `None`。
#[derive(Deserialize)]
struct SignatureRecord {
    /// 事件类型标签；只有 `"assistant"` 才可能是用量证据。
    #[serde(rename = "type")]
    kind: Option<String>,
    /// 会话 ID。
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
    /// 记录时间戳原始字符串。
    timestamp: Option<String>,
    /// 承载用量证据的消息片段。
    message: Option<SignatureMessage>,
}

/// 记录中承载用量证据的消息片段。
#[derive(Deserialize)]
struct SignatureMessage {
    /// 消息 ID。
    id: Option<String>,
    /// 模型名。
    model: Option<String>,
    /// Token 用量字段。
    usage: Option<SignatureUsage>,
}

/// 判定是否为真实用量记录所需的 Token 计数字段。
#[derive(Deserialize)]
struct SignatureUsage {
    /// 输入 Token。
    input_tokens: Option<u64>,
    /// 输出 Token。
    output_tokens: Option<u64>,
    /// 缓存读取的输入 Token。
    #[serde(default)]
    cache_read_input_tokens: Option<u64>,
    /// 缓存写入的输入 Token。
    #[serde(default)]
    cache_creation_input_tokens: Option<u64>,
}

/// 探测候选路径下的 `projects` 子目录，判定其是否具备 Claude transcript 结构特征。
pub fn inspect_root(path: &Path, budget: &mut SignatureBudget<'_>) -> Inspection {
    let projects = path.join("projects");
    let metadata = match fs::symlink_metadata(&projects) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Inspection::NotRoot,
        Err(_) => return Inspection::Indeterminate,
    };
    if metadata_is_link_like(&metadata) || !metadata.is_dir() {
        return Inspection::Rejected;
    }
    inspect_projects(&projects, budget)
}

/// 遍历 `projects` 目录下的各项目子目录，任一项目命中即视为确认。
fn inspect_projects(projects: &Path, budget: &mut SignatureBudget<'_>) -> Inspection {
    if budget.remaining_directories == 0 {
        return Inspection::BudgetExhausted;
    }
    budget.remaining_directories -= 1;
    let entries = match fs::read_dir(projects) {
        Ok(entries) => entries,
        Err(_) => return Inspection::Indeterminate,
    };
    let mut indeterminate = false;
    for project in entries {
        if budget.cancellation.is_cancelled() {
            return Inspection::Cancelled;
        }
        if budget.remaining_entries == 0 {
            return Inspection::BudgetExhausted;
        }
        budget.remaining_entries -= 1;
        let project = match project {
            Ok(entry) => entry,
            Err(_) => {
                indeterminate = true;
                continue;
            }
        };
        let metadata = match fs::symlink_metadata(project.path()) {
            Ok(metadata) => metadata,
            Err(_) => {
                indeterminate = true;
                continue;
            }
        };
        if metadata_is_link_like(&metadata) || !metadata.is_dir() {
            continue;
        }
        match inspect_project_directory(&project.path(), budget) {
            Inspection::Found(evidence) => return Inspection::Found(evidence),
            Inspection::Indeterminate => indeterminate = true,
            Inspection::Cancelled => return Inspection::Cancelled,
            Inspection::BudgetExhausted => return Inspection::BudgetExhausted,
            Inspection::NotRoot | Inspection::Rejected => {}
        }
    }
    if indeterminate {
        Inspection::Indeterminate
    } else {
        Inspection::Rejected
    }
}

/// 在单个项目目录内查找 UUID 命名的 transcript 文件或 subagents 子目录。
fn inspect_project_directory(project: &Path, budget: &mut SignatureBudget<'_>) -> Inspection {
    if budget.remaining_directories == 0 {
        return Inspection::BudgetExhausted;
    }
    budget.remaining_directories -= 1;
    let entries = match fs::read_dir(project) {
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
        let path = entry.path();
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(_) => {
                indeterminate = true;
                continue;
            }
        };
        if metadata_is_link_like(&metadata) {
            continue;
        }
        let is_uuid_jsonl = path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(is_uuid_jsonl_name);
        if metadata.is_file() && is_uuid_jsonl {
            match inspect_transcript(&path, budget, RootCandidateEvidence::ClaudeTranscript) {
                Inspection::Found(evidence) => return Inspection::Found(evidence),
                Inspection::Indeterminate => indeterminate = true,
                Inspection::Cancelled => return Inspection::Cancelled,
                Inspection::BudgetExhausted => return Inspection::BudgetExhausted,
                Inspection::NotRoot | Inspection::Rejected => {}
            }
        } else if metadata.is_dir()
            && path
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(is_uuid)
        {
            match inspect_subagents_directory(&path.join("subagents"), budget) {
                Inspection::Found(evidence) => return Inspection::Found(evidence),
                Inspection::Indeterminate => indeterminate = true,
                Inspection::Cancelled => return Inspection::Cancelled,
                Inspection::BudgetExhausted => return Inspection::BudgetExhausted,
                Inspection::NotRoot | Inspection::Rejected => {}
            }
        }
    }
    if indeterminate {
        Inspection::Indeterminate
    } else {
        Inspection::Rejected
    }
}

/// 检查 `subagents` 子目录内是否存在符合命名规则的 transcript 文件。
fn inspect_subagents_directory(path: &Path, budget: &mut SignatureBudget<'_>) -> Inspection {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Inspection::NotRoot,
        Err(_) => return Inspection::Indeterminate,
    };
    if metadata_is_link_like(&metadata) || !metadata.is_dir() {
        return Inspection::Rejected;
    }
    if budget.remaining_directories == 0 {
        return Inspection::BudgetExhausted;
    }
    budget.remaining_directories -= 1;
    let entries = match fs::read_dir(path) {
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
        let path = entry.path();
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(_) => {
                indeterminate = true;
                continue;
            }
        };
        if metadata_is_link_like(&metadata)
            || !metadata.is_file()
            || !path
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(is_subagent_jsonl_name)
        {
            continue;
        }
        match inspect_transcript(&path, budget, RootCandidateEvidence::ClaudeSubagent) {
            Inspection::Found(evidence) => return Inspection::Found(evidence),
            Inspection::Indeterminate => indeterminate = true,
            Inspection::Cancelled => return Inspection::Cancelled,
            Inspection::BudgetExhausted => return Inspection::BudgetExhausted,
            Inspection::NotRoot | Inspection::Rejected => {}
        }
    }
    if indeterminate {
        Inspection::Indeterminate
    } else {
        Inspection::Rejected
    }
}

/// 逐行读取单个 transcript 文件，寻找符合用量特征的记录作为结构证据。
fn inspect_transcript(
    path: &Path,
    budget: &mut SignatureBudget<'_>,
    evidence: RootCandidateEvidence,
) -> Inspection {
    if budget.remaining_files == 0 || budget.remaining_bytes == 0 {
        return Inspection::BudgetExhausted;
    }
    budget.remaining_files -= 1;
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Inspection::Indeterminate,
    };
    let mut reader = BufReader::new(file);
    for _ in 0..MAX_SIGNATURE_RECORDS_PER_FILE {
        if budget.cancellation.is_cancelled() {
            return Inspection::Cancelled;
        }
        if budget.remaining_bytes == 0 {
            return Inspection::BudgetExhausted;
        }
        let line_limit = MAX_SIGNATURE_LINE_BYTES.min(budget.remaining_bytes);
        let mut line = Vec::new();
        let read = match reader
            .by_ref()
            .take(line_limit.saturating_add(1))
            .read_until(b'\n', &mut line)
        {
            Ok(read) => read,
            Err(_) => return Inspection::Indeterminate,
        };
        if read == 0 {
            return Inspection::Rejected;
        }
        let read_u64 = u64::try_from(read).unwrap_or(u64::MAX);
        budget.remaining_bytes = budget.remaining_bytes.saturating_sub(read_u64);
        if read_u64 > line_limit || !line.ends_with(b"\n") {
            return if budget.remaining_bytes == 0 {
                Inspection::BudgetExhausted
            } else {
                Inspection::Rejected
            };
        }
        if let Ok(record) = serde_json::from_slice::<SignatureRecord>(&line)
            && is_usage_signature(&record)
        {
            return Inspection::Found(evidence);
        }
    }
    Inspection::Rejected
}

/// 判定单条记录是否具备真实用量记录的最小字段特征。
fn is_usage_signature(record: &SignatureRecord) -> bool {
    if record.kind.as_deref() != Some("assistant")
        || !nonempty(record.session_id.as_deref())
        || record
            .timestamp
            .as_deref()
            .is_none_or(|value| value.parse::<Timestamp>().is_err())
    {
        return false;
    }
    let Some(message) = &record.message else {
        return false;
    };
    let Some(usage) = &message.usage else {
        return false;
    };
    nonempty(message.id.as_deref())
        && nonempty(message.model.as_deref())
        && usage.input_tokens.is_some()
        && usage.output_tokens.is_some()
        && usage
            .input_tokens
            .unwrap_or_default()
            .checked_add(usage.cache_read_input_tokens.unwrap_or_default())
            .and_then(|value| {
                value.checked_add(usage.cache_creation_input_tokens.unwrap_or_default())
            })
            .is_some()
}

/// 判定可选字符串在去除首尾空白后是否非空。
fn nonempty(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

/// 按 core 的 Claude Code 别名规则生成安全展示别名，不在 adapter 内二次编码。
///
/// Claude Desktop 本地 Agent 会话根的专属短标识别名同样由该规则给出，保证快速
/// 发现、全设备发现与手动添加三条入口对同一路径产生一致展示身份。
pub fn safe_alias(path: &Path) -> String {
    crate::source_root_alias_from_path(path, SourceClientKind::ClaudeCode)
}
