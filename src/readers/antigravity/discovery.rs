//! 只检查 Antigravity 固定会话布局与 SQLite 文件头，不读取会话正文。

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::{CoverageReport, CoverageState, DiscoveryMethod, RegisteredRoot, RootCandidateEvidence, path_key, stable_id};
use crate::readers::{CancellationToken, DiscoveryProgress, FullDiscoveryOptions};
use crate::readers::discovery::{metadata_is_link_like, validate_local_plain_directory};

/// 已检查布局与数据库文件头的会话目录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AntigravityDiscoveredRoot {
    /// 后端独占的原生目录路径。
    pub path: PathBuf,
    /// 规范化路径对应的稳定标识。
    pub root_id: String,
    /// 不含绝对路径的来源标签。
    pub alias: String,
    /// 根的发现入口。
    pub discovery_method: DiscoveryMethod,
    /// 命中的固定布局证据。
    pub evidence: RootCandidateEvidence,
}

/// 快速发现输入；仅构造数据，不在构造时访问磁盘。
#[derive(Debug, Clone, Default)]
pub struct AntigravityDiscoveryInputs {
    /// 可选用户根；为空时只检查显式登记根。
    pub home_dir: Option<PathBuf>,
    /// 当前客户端已登记根。
    pub registered_roots: Vec<RegisteredRoot>,
}

impl AntigravityDiscoveryInputs {
    /// 使用宿主用户根，不读取环境凭据或账号文件。
    pub fn from_process_environment(registered_roots: Vec<RegisteredRoot>) -> Self {
        Self { home_dir: crate::readers::current_user_home(), registered_roots }
    }
}

/// 发现结果与完整覆盖事实；无法确认的已登记根保留原索引。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AntigravityDiscoveryResult {
    /// 已确认根。
    pub roots: Vec<AntigravityDiscoveredRoot>,
    /// 明确无效的登记标识。
    pub confirmed_invalid_root_ids: Vec<String>,
    /// 本轮未能确认的登记标识。
    pub unconfirmed_root_ids: Vec<String>,
    /// 访问和预算缺口。
    pub coverage: CoverageReport,
    /// 实际检查的目录数量。
    pub directories_scanned: u64,
    /// 拒绝的链接数量。
    pub symlink_skipped_count: u64,
    /// 拒绝的网络路径数量。
    pub network_skipped_count: u64,
}

/// 只接受标准 UUID 文件名，避免把摘要库或任意其他数据库作为会话读取。
pub fn is_conversation_file_name(name: &str) -> bool {
    let Some(id) = name.strip_suffix(".db") else { return false };
    id.len() == 36 && id.bytes().enumerate().all(|(index, byte)| {
        if matches!(index, 8 | 13 | 18 | 23) { byte == b'-' } else { byte.is_ascii_hexdigit() }
    })
}

/// 识别 App 与 CLI 的固定会话根，拒绝其他 `.gemini` 数据。
pub fn is_conversation_root(path: &Path) -> bool {
    path.file_name().is_some_and(|name| name == "conversations")
        && path.parent().and_then(Path::file_name)
            .is_some_and(|name| name == "antigravity" || name == "antigravity-cli")
}

/// 接受用户选择的 App 根或其 conversations 子目录。
pub fn conversation_root(path: &Path) -> PathBuf {
    if path.file_name().is_some_and(|name| name == "antigravity" || name == "antigravity-cli") {
        path.join("conversations")
    } else { path.to_path_buf() }
}

/// 在有界条目预算内核对普通本地目录及 SQLite 文件头。
/// # Errors
/// 权限、链接、网络卷和 I/O 不确定性原样交给宿主归类；不创建文件。
pub fn inspect_root(path: &Path, options: &FullDiscoveryOptions, cancel: &CancellationToken) -> std::io::Result<bool> {
    inspect_with_budget(path, &mut SignatureBudget::new(options), cancel)
}

/// 多个候选共享实际消耗的签名预算，不能在每个根重置。
struct SignatureBudget {
    entries: u64,
    files: u64,
    bytes: u64,
}

impl SignatureBudget {
    /// 只继承调用方本轮已授权的额度。
    fn new(options: &FullDiscoveryOptions) -> Self {
        Self {
            entries: options.max_signature_entries,
            files: options.max_signature_files,
            bytes: options.max_signature_bytes,
        }
    }
}

/// 探测过程中扣减预算；耗尽与用户取消都不伪装为“没有来源”。
fn inspect_with_budget(path: &Path, budget: &mut SignatureBudget, cancel: &CancellationToken) -> std::io::Result<bool> {
    if !is_conversation_root(path) { return Ok(false) }
    validate_local_plain_directory(path)?;
    for entry in fs::read_dir(path)? {
        if cancel.is_cancelled() { return Err(std::io::ErrorKind::Interrupted.into()) }
        if budget.entries == 0 {
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
        budget.entries -= 1;
        let entry = entry?;
        if !entry.file_name().to_str().is_some_and(is_conversation_file_name) { continue }
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.is_file() || metadata_is_link_like(&metadata) { continue }
        if budget.files == 0 || budget.bytes < 16 {
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
        budget.files -= 1;
        budget.bytes -= 16;
        let mut header = [0_u8; 16];
        File::open(entry.path())?.read_exact(&mut header)?;
        if header == *b"SQLite format 3\0" { return Ok(true) }
    }
    Ok(false)
}

/// 快速发现只检查 App / CLI 的已知会话目录与用户登记目录。
pub fn discover_antigravity_quick(inputs: &AntigravityDiscoveryInputs, cancel: &CancellationToken) -> AntigravityDiscoveryResult {
    let mut result = empty_result();
    let mut candidates = inputs.registered_roots.iter().filter(|root| root.enabled)
        .map(|root| (root.path.clone(), root.root_id.clone(), root.alias.clone(), DiscoveryMethod::Registered))
        .collect::<Vec<_>>();
    if let Some(home) = &inputs.home_dir {
        for name in ["antigravity", "antigravity-cli"] {
            candidates.push((home.join(".gemini").join(name).join("conversations"), None,
                if name == "antigravity" { "Antigravity" } else { "Antigravity CLI" }.to_owned(), DiscoveryMethod::DefaultHome));
        }
    }
    let mut roots = BTreeMap::new();
    let mut budget = SignatureBudget::new(&FullDiscoveryOptions::default());
    for (path, id, alias, method) in candidates {
        if cancel.is_cancelled() { result.coverage.state = CoverageState::Cancelled; break }
        let path = conversation_root(&path);
        match inspect_with_budget(&path, &mut budget, cancel) {
            Ok(true) => {
                if let Ok(path) = fs::canonicalize(&path) {
                    let root_id = id.unwrap_or_else(|| stable_id("antigravity-root", &path_key(&path)));
                    roots.entry(path.clone()).or_insert(AntigravityDiscoveredRoot {
                        path, root_id, alias, discovery_method: method,
                        evidence: RootCandidateEvidence::AntigravityConversationDb,
                    });
                } else { result.coverage.skipped_count += 1; if let Some(id) = id { result.unconfirmed_root_ids.push(id) } }
            }
            Ok(false) | Err(_) if id.is_some() => {
                result.unconfirmed_root_ids.extend(id);
                result.coverage.skipped_count += 1;
            }
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => result.coverage.skipped_count += 1,
            _ => {}
        }
    }
    result.roots = roots.into_values().collect();
    finish_result(&mut result);
    result
}

/// 用户显式授权的有界设备或子树发现；只打开已识别布局中的 SQLite 文件头。
pub fn discover_antigravity_full_device_with_progress<F>(options: &FullDiscoveryOptions, cancel: &CancellationToken, mut progress: F) -> AntigravityDiscoveryResult
where F: FnMut(DiscoveryProgress) {
    let mut result = empty_result();
    result.coverage.skipped_count = options.preflight_network_skipped_count
        .saturating_add(options.preflight_other_skipped_count);
    result.network_skipped_count = options.preflight_network_skipped_count;
    let mut signature_budget = SignatureBudget::new(options);
    let mut stack = options.search_roots.clone();
    let mut visited = HashSet::new();
    let mut entries_left = options.max_entries;
    while let Some(path) = stack.pop() {
        if cancel.is_cancelled() { result.coverage.state = CoverageState::Cancelled; break }
        if result.directories_scanned >= options.max_directories || entries_left == 0 {
            result.coverage.skipped_count += 1; break;
        }
        if options.excluded_roots.iter().any(|excluded| path.starts_with(excluded)) { continue }
        if validate_local_plain_directory(&path).is_err() { result.coverage.skipped_count += 1; continue }
        let Ok(path) = fs::canonicalize(path) else { result.coverage.skipped_count += 1; continue };
        if !visited.insert(path.clone()) { continue }
        result.directories_scanned += 1;
        if is_conversation_root(&path) {
            match inspect_with_budget(&path, &mut signature_budget, cancel) {
                Ok(true) => {
                    result.roots.push(AntigravityDiscoveredRoot {
                        root_id: stable_id("antigravity-root", &path_key(&path)),
                        alias: "Antigravity".to_owned(), path,
                        discovery_method: DiscoveryMethod::FullDevice,
                        evidence: RootCandidateEvidence::AntigravityConversationDb,
                    });
                    continue;
                }
                Ok(false) => continue,
                Err(_) => { result.coverage.skipped_count += 1; continue }
            }
        }
        let Ok(entries) = fs::read_dir(&path) else { result.coverage.skipped_count += 1; continue };
        for entry in entries {
            if entries_left == 0 || cancel.is_cancelled() { break }
            entries_left -= 1;
            match entry {
                Ok(entry) if entry.file_type().is_ok_and(|kind| kind.is_dir() && !kind.is_symlink()) => stack.push(entry.path()),
                Err(_) => result.coverage.skipped_count += 1,
                _ => {}
            }
        }
        progress(DiscoveryProgress { directories_scanned: result.directories_scanned, roots_discovered: result.roots.len() as u64 });
    }
    finish_result(&mut result);
    progress(DiscoveryProgress { directories_scanned: result.directories_scanned, roots_discovered: result.roots.len() as u64 });
    result
}

/// 空发现结果不把未访问的内容记作错误。
fn empty_result() -> AntigravityDiscoveryResult {
    AntigravityDiscoveryResult {
        roots: Vec::new(), confirmed_invalid_root_ids: Vec::new(), unconfirmed_root_ids: Vec::new(),
        coverage: CoverageReport { state: CoverageState::Complete, roots_scanned: 0, roots_discovered: 0, permission_denied_count: 0, skipped_count: 0, warning_count: 0 },
        directories_scanned: 0, symlink_skipped_count: 0, network_skipped_count: 0,
    }
}

/// 收口覆盖数量；取消和访问缺口保持可观察。
fn finish_result(result: &mut AntigravityDiscoveryResult) {
    result.coverage.roots_discovered = result.roots.len() as u64;
    result.coverage.roots_scanned = result.roots.len() as u64;
    if result.coverage.state != CoverageState::Cancelled && result.coverage.skipped_count > 0 {
        result.coverage.state = CoverageState::Partial;
    }
}
