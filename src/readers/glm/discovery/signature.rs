//! GLM（Z Code CLI）rollout 根结构签名探测：只承认平铺 `model-io-sess_*.jsonl`。

use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::{CoverageState, RootCandidateEvidence};
use serde::Deserialize;

use super::super::super::discovery::{metadata_is_link_like, walk_ancestors_for_link_component};
use super::super::super::file_source::same_opened_file;
use super::super::super::jsonl_lines::{JsonlRecord, LineFlow, read_jsonl_records};
use super::super::super::{CancellationToken, FullDiscoveryOptions};
use super::super::path_rules::{is_allowed_glm_root, is_model_io_file_name};

/// 签名探测最多缓存一 MiB，超出部分由共用切行器流式跳过正文。
const MAX_SIGNATURE_LINE_BYTES: usize = 1024 * 1024;
/// 单文件最多探测的完整记录数，避免扫描无限追加来源。
const MAX_SIGNATURE_RECORDS_PER_FILE: u64 = 64;

/// 结构签名探测过程中共享的有界预算。
pub struct SignatureBudget<'a> {
    /// 协作取消令牌，探测循环中定期检查。
    cancellation: &'a CancellationToken,
    /// 剩余允许遍历的目录数。
    remaining_directories: u64,
    /// 剩余允许检查的目录项数。
    remaining_entries: u64,
    /// 剩余允许打开的文件数。
    remaining_files: u64,
    /// 剩余允许读取的字节数。
    remaining_bytes: u64,
}

impl<'a> SignatureBudget<'a> {
    /// 从全设备发现选项初始化各项预算上限。
    pub fn new(options: &FullDiscoveryOptions, cancellation: &'a CancellationToken) -> Self {
        Self {
            cancellation,
            remaining_directories: options.max_signature_directories,
            remaining_entries: options.max_signature_entries,
            remaining_files: options.max_signature_files,
            remaining_bytes: options.max_signature_bytes,
        }
    }
}

/// 单次结构签名探测的结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inspection {
    /// 确认是 GLM rollout 根，并保留命中的结构证据。
    Found(RootCandidateEvidence),
    /// 目录不存在。
    NotRoot,
    /// 确认结构不符合 GLM rollout 特征。
    Rejected,
    /// 未能得出确定结论，须保留旧登记。
    Indeterminate,
    /// 探测过程中收到取消信号。
    Cancelled,
    /// 预算耗尽提前中止。
    BudgetExhausted,
}

/// 仅反序列化 GLM 签名所需的白名单字段。
#[derive(Deserialize)]
struct SignatureEnvelope {
    /// 事件类型标记，只用于识别模型 I/O 事件。
    #[serde(rename = "type", default)]
    kind: Option<String>,
    /// 响应层，签名只关心用量是否存在。
    #[serde(default)]
    response: Option<SignatureResponse>,
}

/// 响应层签名；只声明用量计数。
#[derive(Deserialize)]
struct SignatureResponse {
    /// 用量计数；只判断判定字段是否存在。
    #[serde(default)]
    usage: Option<SignatureUsage>,
}

/// 仅反序列化判断可信用量所需的计数字段。
#[derive(Deserialize)]
struct SignatureUsage {
    /// 全部输入 token 数。
    #[serde(rename = "inputTokens", default)]
    input_tokens: Option<u64>,
    /// 输出 token 数。
    #[serde(rename = "outputTokens", default)]
    output_tokens: Option<u64>,
    /// 总 token 数。
    #[serde(rename = "totalTokens", default)]
    total_tokens: Option<u64>,
    /// 已知缓存读取必须满足输入子集约束。
    #[serde(rename = "cacheReadTokens", default)]
    cache_read_tokens: Option<u64>,
    /// 已知缓存写入必须满足输入子集约束。
    #[serde(rename = "cacheWriteTokens", default)]
    cache_write_tokens: Option<u64>,
}

/// 探测候选路径是否为含可信用量文件的 GLM rollout 根。
pub fn inspect_root(path: &Path, budget: &mut SignatureBudget<'_>) -> Inspection {
    if !is_allowed_glm_root(path) {
        return Inspection::Rejected;
    }
    match reject_link_components(path) {
        Ok(true) => return Inspection::Rejected,
        Err(_) => return Inspection::Indeterminate,
        Ok(false) => {}
    }
    if budget.cancellation.is_cancelled() {
        return Inspection::Cancelled;
    }
    if budget.remaining_directories == 0 {
        return Inspection::BudgetExhausted;
    }
    budget.remaining_directories -= 1;
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Inspection::NotRoot,
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
        if metadata_is_link_like(&metadata) || !metadata.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !is_model_io_file_name(name) {
            continue;
        }
        match inspect_model_io_file(&entry.path(), budget) {
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

/// 读取单个有界会话文件并确认至少一个可信用量事件。
fn inspect_model_io_file(path: &Path, budget: &mut SignatureBudget<'_>) -> Inspection {
    if budget.remaining_files == 0 {
        return Inspection::BudgetExhausted;
    }
    budget.remaining_files -= 1;
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Inspection::Indeterminate,
    };
    // 内容读取前复核当前路径仍指向已打开的普通文件，拒绝签名探测期间的链接替换。
    let metadata = match file.metadata() {
        Ok(value) => value,
        Err(_) => return Inspection::Indeterminate,
    };
    let current_metadata = match fs::symlink_metadata(path) {
        Ok(value) => value,
        Err(_) => return Inspection::Indeterminate,
    };
    if !metadata.is_file()
        || !current_metadata.is_file()
        || metadata_is_link_like(&current_metadata)
        || !matches!(reject_link_components(path), Ok(false))
        || !matches!(
            same_opened_file(&file, path, &metadata, &current_metadata),
            Ok(true)
        )
    {
        return Inspection::Rejected;
    }
    let byte_limit = budget.remaining_bytes;
    if byte_limit == 0 {
        return Inspection::BudgetExhausted;
    }
    let mut reader = file.take(byte_limit);
    let mut records = 0_u64;
    let mut found = false;
    let parsed = read_jsonl_records::<SignatureEnvelope, _, std::io::Error>(
        &mut reader,
        MAX_SIGNATURE_LINE_BYTES,
        || budget.cancellation.is_cancelled(),
        |error| error,
        |record| {
            records = records.saturating_add(1);
            if let JsonlRecord::Parsed(record) = record
                && has_model_io_usage(record)
            {
                found = true;
                return Ok(LineFlow::Stop);
            }
            Ok(if records >= MAX_SIGNATURE_RECORDS_PER_FILE {
                LineFlow::Stop
            } else {
                LineFlow::Continue
            })
        },
    );
    // Take 在实际读取层执行总预算，计费包含 BufReader 已预读的字节。
    budget.remaining_bytes = budget
        .remaining_bytes
        .saturating_sub(byte_limit - reader.limit());
    match parsed {
        Ok(progress) if progress.cancelled => Inspection::Cancelled,
        Ok(_) if found => Inspection::Found(RootCandidateEvidence::GlmModelIoUpdates),
        Ok(_) if reader.limit() == 0 => Inspection::BudgetExhausted,
        Ok(progress)
            if records >= MAX_SIGNATURE_RECORDS_PER_FILE || progress.trailing_bytes > 0 =>
        {
            Inspection::Indeterminate
        }
        Ok(_) => Inspection::Rejected,
        Err(_) => Inspection::Indeterminate,
    }
}

/// 按共享 Token 不变量判断白名单记录是否含可信模型 I/O 用量。
fn has_model_io_usage(record: SignatureEnvelope) -> bool {
    if record.kind.as_deref() != Some("model_io") {
        return false;
    }
    record.response.is_some_and(|response| {
        response.usage.is_some_and(|usage| {
            if usage.total_tokens.is_none() {
                return false;
            }
            crate::TokenUsage::new_with_component_availability(
                usage.input_tokens,
                usage.cache_read_tokens,
                usage.cache_write_tokens,
                usage.output_tokens,
                None,
                usage.total_tokens,
            )
            .is_ok()
        })
    })
}

/// 沿路径祖先检查链接组件。
pub fn reject_link_components(path: &Path) -> std::io::Result<bool> {
    match walk_ancestors_for_link_component(path) {
        Ok(has_link) => Ok(has_link),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// 取路径末段作为安全展示别名；rollout 目录展示为 `rollout`。
pub fn safe_alias(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("GLM 数据根")
        .to_owned()
}

/// 按取消、预算与跳过计数折算覆盖状态。
pub fn coverage_state(
    cancelled: bool,
    budget_exhausted: bool,
    permission_denied_count: u64,
    skipped_count: u64,
) -> CoverageState {
    if cancelled {
        CoverageState::Cancelled
    } else if budget_exhausted || permission_denied_count > 0 || skipped_count > 0 {
        CoverageState::Partial
    } else {
        CoverageState::Complete
    }
}

/// 判定路径是否落在排除根集合内。
pub fn is_excluded(path: &Path, traversal_root: &Path, excluded_roots: &[PathBuf]) -> bool {
    crate::is_discovery_path_excluded(path, traversal_root, excluded_roots)
}

/// 判定路径是否越过了另一个独立搜索根。
pub fn crosses_search_root(path: &Path, traversal_root: &Path, search_roots: &[PathBuf]) -> bool {
    search_roots.iter().any(|other| {
        other != traversal_root && other.starts_with(traversal_root) && path.starts_with(other)
    })
}

/// 默认 GLM rollout 根在用户主目录下的固定相对路径段。
pub const GLM_ROLLOUT_HOME_RELATIVE: &str = ".zcode/cli/rollout";

/// 拼接用户主目录与固定 rollout 相对路径。
pub fn default_rollout_dir(home_dir: &Path) -> PathBuf {
    home_dir.join(GLM_ROLLOUT_HOME_RELATIVE)
}
