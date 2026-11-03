//! 平台范围只凭卷、目录项与文件名发现候选；显式环境根另走有界严格签名。

use std::collections::{HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use crate::{
    CoverageReport, RootCandidate, RootCandidateEvidence, RootDiscoveryAgentScope,
    RootDiscoveryCoordinator, RootDiscoveryPlatform, RootDiscoveryProgress, RootDiscoveryScope,
    RootDiscoveryStrategy, SourceClientKind, append_platform_discovery_excludes_for_volumes,
    is_discovery_path_excluded, path_has_full_discovery_skip_directory_name, stable_id,
};

use super::current_user_home;
use super::discovery::metadata_is_link_like;
use super::{
    CancellationToken, ClaudeDiscoveryInputs, DiscoveryInputs, GlmDiscoveryInputs,
    GrokDiscoveryInputs, LocalVolumeRoots, discover_claude_quick, discover_glm_quick,
    discover_grok_quick, discover_quick, is_obviously_network_path, validate_local_plain_directory,
};

mod platform_index;
mod scope;

use scope::{
    candidate_from_file_name, current_platform, discovery_scope, is_unselected_agent_directory,
    path_under_unselected_agent_directory, platform_priority_roots, priority_queue,
};

/// 执行结果只包含非路径计数；候选直接提交到运行时协调器。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataDiscoverySummary {
    /// 系统索引是否可用。
    pub system_index_available: bool,
    /// 是否执行了普通元数据兜底。
    pub fallback_performed: bool,
}

/// 显式环境根的严格签名结果与必须向 UI 保留的覆盖缺口。
#[derive(Debug, Default)]
struct ExplicitEnvironmentInspection {
    /// 通过严格签名确认的候选路径及其证据类型。
    candidates: Vec<(PathBuf, RootCandidateEvidence)>,
    /// 权限拒绝计数。
    permission_denied: u64,
    /// 因链接、非本地卷等原因跳过的计数。
    skipped: u64,
}

/// 单个卷内待遍历目录的广度优先队列及其完成标记。
#[derive(Debug)]
struct VolumeQueue {
    /// 待处理的目录队列。
    pending: VecDeque<PathBuf>,
    /// 标识本卷是否已遍历完成。
    completed: bool,
    /// 该卷发现遍历的根路径。
    traversal_root: PathBuf,
}

/// 单个目录一次遍历产出的子目录、命中候选与错误/跳过计数。
#[derive(Debug, Default)]
struct DirectoryResult {
    /// 待继续入队的子目录。
    children: Vec<PathBuf>,
    /// 本次遍历命中的候选路径及其证据类型。
    candidates: Vec<(PathBuf, RootCandidateEvidence)>,
    /// 本次检查过的文件名数量。
    file_names_checked: u64,
    /// 权限拒绝计数。
    permission_denied: u64,
    /// I/O 错误计数。
    io_errors: u64,
    /// 因链接、非本地卷等原因跳过的计数。
    skipped: u64,
}

/// 覆盖整次发现生命周期的固定目录 worker 池，避免每轮反复创建原生线程。
struct MetadataWorkerPool {
    /// 任务发送端；关闭后 worker 退出。
    sender: Option<mpsc::SyncSender<(usize, PathBuf)>>,
    /// 按任务序号返回目录结果；失败只保留空错误。
    results: mpsc::Receiver<(usize, Result<DirectoryResult, ()>)>,
    /// worker 线程句柄，池销毁时回收。
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl MetadataWorkerPool {
    /// 创建固定大小的元数据探测工作池并启动受控 worker。
    fn new(
        worker_count: usize,
        agents: RootDiscoveryAgentScope,
        coordinator: RootDiscoveryCoordinator,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<(usize, PathBuf)>(worker_count * 2);
        let receiver = Arc::new(Mutex::new(receiver));
        let (result_sender, results) = mpsc::channel();
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let result_sender = result_sender.clone();
            let coordinator = coordinator.clone();
            workers.push(std::thread::spawn(move || {
                loop {
                    let job = receiver
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .recv();
                    let Ok((volume, path)) = job else {
                        break;
                    };
                    let result = std::panic::catch_unwind(|| {
                        inspect_directory_metadata(&path, agents, &coordinator)
                    })
                    .map_err(|_| ());
                    if result_sender.send((volume, result)).is_err() {
                        break;
                    }
                }
            }));
        }
        drop(result_sender);
        Self {
            sender: Some(sender),
            results,
            workers,
        }
    }

    /// 提交一批目录探测任务，并按原序收集每项结果。
    fn inspect(&self, jobs: Vec<(usize, PathBuf)>) -> Vec<(usize, Result<DirectoryResult, ()>)> {
        let Some(sender) = self.sender.as_ref() else {
            return Vec::new();
        };
        let mut expected = 0_usize;
        for job in jobs {
            if sender.send(job).is_err() {
                break;
            }
            expected += 1;
        }
        (0..expected)
            .filter_map(|_| self.results.recv().ok())
            .collect()
    }
}

impl Drop for MetadataWorkerPool {
    /// 关闭任务通道并回收全部元数据探测 worker。
    fn drop(&mut self) {
        self.sender.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

/// 执行发现并在候选首次插入时立即通知调用方。
pub fn discover_metadata_roots_with_callback(
    coordinator: &RootDiscoveryCoordinator,
    discovery_kind: RootDiscoveryScope,
    agents: RootDiscoveryAgentScope,
    on_candidate: impl Fn(RootCandidate),
) -> MetadataDiscoverySummary {
    // 优先目录在 Linux 上需要额外遍历 Desktop 应用数据，只解析一次供搜索范围与回退扫描共用。
    let priority_roots = platform_priority_roots(agents);
    let mut volumes = discovery_scope(discovery_kind, &priority_roots);
    if discovery_kind == RootDiscoveryScope::FullLocalVolumes {
        volumes.excluded_roots = append_platform_discovery_excludes_for_volumes(
            std::mem::take(&mut volumes.excluded_roots),
            &volumes.search_roots,
        );
    }
    let total = u64::try_from(volumes.search_roots.len()).unwrap_or(u64::MAX);
    let strategy = if cfg!(target_os = "windows") {
        RootDiscoveryStrategy::WindowsSearch
    } else if cfg!(target_os = "macos") {
        RootDiscoveryStrategy::MacOsSpotlight
    } else {
        RootDiscoveryStrategy::MetadataTraversal
    };
    if coordinator.snapshot().lifecycle != crate::RootDiscoveryLifecycle::Running
        && !coordinator.start(strategy, current_platform(), discovery_kind, total)
    {
        return MetadataDiscoverySummary::default();
    }
    if discovery_kind == RootDiscoveryScope::UserPriority {
        let inspection = explicit_environment_candidates(
            coordinator,
            std::env::var_os("CODEX_HOME").map(PathBuf::from),
            std::env::var_os("CLAUDE_CONFIG_DIR").map(PathBuf::from),
            std::env::var_os("GROK_HOME")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            current_user_home(),
            agents,
        );
        for (path, evidence) in inspection.candidates.iter().cloned() {
            submit_candidate(
                coordinator,
                path,
                evidence,
                RootDiscoveryStrategy::MetadataTraversal,
                agents,
                &on_candidate,
            );
        }
        record_explicit_environment_gaps(coordinator, &inspection);
    }
    if total == 0 {
        record_volume_scope_gaps(coordinator, &volumes, total);
        if discovery_kind == RootDiscoveryScope::FullLocalVolumes
            && coordinator.candidates().is_empty()
        {
            coordinator.fail("no_local_volumes");
        } else {
            coordinator.finish(false, false);
        }
        return MetadataDiscoverySummary::default();
    }
    let (system_index_available, indexed_paths) = if platform_index::should_query(discovery_kind) {
        platform_index::query(&volumes, coordinator, agents)
    } else {
        (false, Vec::new())
    };
    for path in indexed_paths {
        if coordinator.is_cancel_requested() {
            break;
        }
        if path_blocked_by_discovery_policy(&path, &volumes)
            || !path_is_in_search_scope(&path, &volumes)
            || path_under_unselected_agent_directory(&path, agents)
        {
            continue;
        }
        if let Some((candidate, evidence)) = candidate_from_file_name(&path, agents)
            && validate_local_plain_directory(&candidate).is_ok()
        {
            submit_candidate(
                coordinator,
                candidate,
                evidence,
                strategy,
                agents,
                &on_candidate,
            );
        }
    }
    coordinator.begin_fallback(system_index_available);
    let mut summary = discover_metadata_roots_in_with_callback(
        coordinator,
        volumes,
        priority_roots,
        agents,
        &on_candidate,
    );
    coordinator.finish(system_index_available, true);
    summary.system_index_available = system_index_available;
    summary
}

/// 仅对闭集内 Agent 有界核对环境根与 Grok 的环境根或默认目录。
fn explicit_environment_candidates(
    coordinator: &RootDiscoveryCoordinator,
    codex_home: Option<PathBuf>,
    claude_config_dir: Option<PathBuf>,
    grok_home: Option<PathBuf>,
    home_dir: Option<PathBuf>,
    agents: RootDiscoveryAgentScope,
) -> ExplicitEnvironmentInspection {
    if codex_home.is_none()
        && claude_config_dir.is_none()
        && grok_home.is_none()
        && home_dir.is_none()
    {
        return ExplicitEnvironmentInspection::default();
    }
    let cancellation = CancellationToken::new();
    if coordinator.is_cancel_requested() {
        cancellation.cancel();
    }
    let finished = Arc::new(AtomicBool::new(false));
    std::thread::scope(|scope| {
        let cancellation_for_monitor = cancellation.clone();
        let finished_for_monitor = Arc::clone(&finished);
        let monitor = scope.spawn(move || {
            while !finished_for_monitor.load(Ordering::Acquire) {
                if coordinator.is_cancel_requested() {
                    cancellation_for_monitor.cancel();
                    break;
                }
                std::thread::park_timeout(Duration::from_millis(10));
            }
        });
        let candidates = inspect_explicit_environment_candidates(
            codex_home,
            claude_config_dir,
            grok_home,
            home_dir,
            &cancellation,
            agents,
        );
        finished.store(true, Ordering::Release);
        monitor.thread().unpark();
        let _ = monitor.join();
        candidates
    })
}

/// 使用现有 Quick 签名预算，仅核对闭集内 Agent 的环境根。
/// Codex 与 Claude 互不依赖，并发核对以缩短有界扫描的墙钟时间。
fn inspect_explicit_environment_candidates(
    codex_home: Option<PathBuf>,
    claude_config_dir: Option<PathBuf>,
    grok_home: Option<PathBuf>,
    home_dir: Option<PathBuf>,
    cancellation: &CancellationToken,
    agents: RootDiscoveryAgentScope,
) -> ExplicitEnvironmentInspection {
    let codex_home = agents
        .contains(SourceClientKind::Codex)
        .then_some(codex_home)
        .flatten()
        .filter(|path| path.is_absolute());
    let claude_config_dir = agents
        .contains(SourceClientKind::ClaudeCode)
        .then_some(claude_config_dir)
        .flatten()
        .filter(|path| path.is_absolute());
    let grok_home = agents
        .contains(SourceClientKind::GrokBuildCli)
        .then_some(grok_home)
        .flatten()
        .filter(|path| path.is_absolute());
    let glm_home_dir = agents
        .contains(SourceClientKind::Glm)
        .then(|| home_dir.clone())
        .flatten()
        .filter(|path| path.is_absolute());
    let home_dir = agents
        .contains(SourceClientKind::GrokBuildCli)
        .then_some(home_dir)
        .flatten()
        .filter(|path| path.is_absolute());
    let (codex_result, claude_result, grok_result, glm_result) = std::thread::scope(|scope| {
        let codex_handle = codex_home.map(|codex_home| {
            let cancellation = cancellation.clone();
            scope.spawn(move || {
                discover_quick(
                    &DiscoveryInputs {
                        codex_home: Some(codex_home),
                        ..DiscoveryInputs::default()
                    },
                    &cancellation,
                )
            })
        });
        let claude_handle = claude_config_dir.map(|claude_config_dir| {
            let cancellation = cancellation.clone();
            scope.spawn(move || {
                discover_claude_quick(
                    &ClaudeDiscoveryInputs {
                        claude_config_dir: Some(claude_config_dir),
                        ..ClaudeDiscoveryInputs::default()
                    },
                    &cancellation,
                )
            })
        });
        // 快速签名同样保持最多两个并发读取者，先回收首批再启动其余客户端。
        let codex_result =
            codex_handle.map(|handle| handle.join().expect("codex quick discovery panicked"));
        let claude_result =
            claude_handle.map(|handle| handle.join().expect("claude quick discovery panicked"));
        let grok_handle = (grok_home.is_some() || home_dir.is_some()).then(|| {
            let cancellation = cancellation.clone();
            scope.spawn(move || {
                discover_grok_quick(
                    &GrokDiscoveryInputs {
                        grok_home,
                        home_dir,
                        registered_roots: Vec::new(),
                    },
                    &cancellation,
                )
            })
        });
        let glm_handle = glm_home_dir.map(|home_dir| {
            let cancellation = cancellation.clone();
            scope.spawn(move || {
                discover_glm_quick(
                    &GlmDiscoveryInputs {
                        home_dir: Some(home_dir),
                        registered_roots: Vec::new(),
                    },
                    &cancellation,
                )
            })
        });
        (
            codex_result,
            claude_result,
            grok_handle.map(|handle| handle.join().expect("grok quick discovery panicked")),
            glm_handle.map(|handle| handle.join().expect("glm quick discovery panicked")),
        )
    });

    let mut inspection = ExplicitEnvironmentInspection::default();
    if let Some(result) = codex_result {
        merge_coverage(&mut inspection, &result.coverage);
        inspection.candidates.extend(
            result
                .roots
                .into_iter()
                .map(|root| (root.path, RootCandidateEvidence::CodexRollout)),
        );
    }
    if let Some(result) = claude_result {
        merge_coverage(&mut inspection, &result.coverage);
        inspection.candidates.extend(
            result
                .roots
                .into_iter()
                .map(|root| (root.path, root.evidence)),
        );
    }
    if let Some(result) = grok_result {
        merge_coverage(&mut inspection, &result.coverage);
        inspection.candidates.extend(
            result
                .roots
                .into_iter()
                .map(|root| (root.path, root.evidence)),
        );
    }
    if let Some(result) = glm_result {
        merge_coverage(&mut inspection, &result.coverage);
        inspection.candidates.extend(
            result
                .roots
                .into_iter()
                .map(|root| (root.path, root.evidence)),
        );
    }
    inspection
}

/// 把一次 Quick 发现的权限拒绝与跳过计数并入显式环境根的核对结果。
fn merge_coverage(inspection: &mut ExplicitEnvironmentInspection, coverage: &CoverageReport) {
    inspection.permission_denied = inspection
        .permission_denied
        .saturating_add(coverage.permission_denied_count);
    inspection.skipped = inspection.skipped.saturating_add(coverage.skipped_count);
}

/// 把环境精确根的权限、网络、链接或预算缺口并入同一个发现任务。
fn record_explicit_environment_gaps(
    coordinator: &RootDiscoveryCoordinator,
    inspection: &ExplicitEnvironmentInspection,
) {
    let mut progress = coordinator.snapshot().progress;
    progress.permission_denied = progress
        .permission_denied
        .saturating_add(inspection.permission_denied);
    progress.skipped = progress.skipped.saturating_add(inspection.skipped);
    coordinator.update_progress(progress);
}

/// 把卷预检在遍历前拒绝的网络、未知或链接根保留在任务进度中。
fn record_volume_scope_gaps(
    coordinator: &RootDiscoveryCoordinator,
    volumes: &LocalVolumeRoots,
    total: u64,
) -> RootDiscoveryProgress {
    let mut progress = coordinator.snapshot().progress;
    progress.volumes_total = total;
    progress.skipped = progress.skipped.saturating_add(
        volumes
            .network_skipped_count
            .saturating_add(volumes.other_skipped_count),
    );
    coordinator.update_progress(progress);
    progress
}

/// 使用显式卷边界执行发现，供多卷公平性测试复用。
#[cfg(test)]
pub fn discover_metadata_roots_in(
    coordinator: &RootDiscoveryCoordinator,
    volumes: LocalVolumeRoots,
) -> MetadataDiscoverySummary {
    discover_metadata_roots_in_with_callback(
        coordinator,
        volumes,
        Vec::new(),
        RootDiscoveryAgentScope::all_physical(),
        &|_| {},
    )
}

/// 内部实现：仅在闭集内跨多卷公平轮询遍历目录，命中即实时上报候选。
fn discover_metadata_roots_in_with_callback(
    coordinator: &RootDiscoveryCoordinator,
    volumes: LocalVolumeRoots,
    priority_roots: Vec<PathBuf>,
    agents: RootDiscoveryAgentScope,
    on_candidate: &impl Fn(RootCandidate),
) -> MetadataDiscoverySummary {
    let total = u64::try_from(volumes.search_roots.len()).unwrap_or(u64::MAX);
    let excluded_roots = volumes.excluded_roots.clone();
    if coordinator.snapshot().lifecycle != crate::RootDiscoveryLifecycle::Running
        && !coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::FullLocalVolumes,
            total,
        )
    {
        return MetadataDiscoverySummary::default();
    }
    let mut progress = record_volume_scope_gaps(coordinator, &volumes, total);
    let mut queues = volumes
        .search_roots
        .into_iter()
        .map(|root| VolumeQueue {
            pending: priority_queue(&root, &priority_roots)
                .into_iter()
                .filter(|path| !is_discovery_path_excluded(path, &root, &excluded_roots))
                .collect(),
            completed: false,
            traversal_root: root,
        })
        .collect::<Vec<_>>();
    let mut visited = queues
        .iter()
        .flat_map(|queue| queue.pending.iter().cloned())
        .collect::<HashSet<_>>();
    let mut next_volume = 0_usize;
    let worker_pool = MetadataWorkerPool::new(adaptive_worker_count(), agents, coordinator.clone());

    while queues.iter().any(|queue| !queue.completed) {
        if coordinator.is_cancel_requested() {
            break;
        }
        let jobs = take_fair_jobs(&mut queues, adaptive_worker_count(), &mut next_volume);
        if jobs.is_empty() {
            mark_completed_queues(&mut queues, &mut progress);
            continue;
        }
        let results = worker_pool.inspect(jobs);
        for (volume, joined) in results {
            if coordinator.is_cancel_requested() {
                break;
            }
            progress.directories_checked = progress.directories_checked.saturating_add(1);
            let Ok(result) = joined else {
                progress.io_errors = progress.io_errors.saturating_add(1);
                continue;
            };
            progress.file_names_checked = progress
                .file_names_checked
                .saturating_add(result.file_names_checked);
            progress.permission_denied = progress
                .permission_denied
                .saturating_add(result.permission_denied);
            progress.io_errors = progress.io_errors.saturating_add(result.io_errors);
            progress.skipped = progress.skipped.saturating_add(result.skipped);
            let traversal_root = queues[volume].traversal_root.clone();
            for child in result.children {
                if is_discovery_path_excluded(&child, &traversal_root, &excluded_roots) {
                    progress.skipped = progress.skipped.saturating_add(1);
                    continue;
                }
                if visited.insert(child.clone()) {
                    queues[volume].pending.push_back(child);
                }
            }
            for (path, evidence) in result.candidates {
                if path_has_full_discovery_skip_directory_name(&path)
                    || excluded_roots
                        .iter()
                        .any(|excluded| path.starts_with(excluded))
                {
                    progress.skipped = progress.skipped.saturating_add(1);
                    continue;
                }
                submit_candidate(
                    coordinator,
                    path,
                    evidence,
                    RootDiscoveryStrategy::MetadataTraversal,
                    agents,
                    on_candidate,
                );
            }
        }
        mark_completed_queues(&mut queues, &mut progress);
        coordinator.update_progress(progress);
    }
    coordinator.update_progress(progress);
    coordinator.finish(false, true);
    MetadataDiscoverySummary {
        system_index_available: false,
        fallback_performed: true,
    }
}

/// Spotlight 等系统索引结果必须服从当前用户目录或本地卷边界。
fn path_is_in_search_scope(path: &Path, volumes: &LocalVolumeRoots) -> bool {
    volumes
        .search_roots
        .iter()
        .any(|root| path.starts_with(root))
}

/// 全盘策略：排除名单前缀或任意祖先目录名命中跳过名单。
fn path_blocked_by_discovery_policy(path: &Path, volumes: &LocalVolumeRoots) -> bool {
    if volumes
        .excluded_roots
        .iter()
        .any(|excluded| path.starts_with(excluded))
    {
        return true;
    }
    path.ancestors()
        .any(path_has_full_discovery_skip_directory_name)
}

/// 每轮从不同卷各取一个目录，确保较大前序卷不会饿死后续卷。
fn take_fair_jobs(
    queues: &mut [VolumeQueue],
    workers: usize,
    next_volume: &mut usize,
) -> Vec<(usize, PathBuf)> {
    let mut jobs = Vec::new();
    if queues.is_empty() {
        return jobs;
    }
    let mut consecutive_empty = 0_usize;
    while jobs.len() < workers && consecutive_empty < queues.len() {
        let index = *next_volume % queues.len();
        *next_volume = (index + 1) % queues.len();
        if let Some(path) = queues[index].pending.pop_front() {
            jobs.push((index, path));
            consecutive_empty = 0;
        } else {
            consecutive_empty += 1;
        }
    }
    jobs
}

/// 根据逻辑 CPU 自适应选择一到两个只读 worker。
fn adaptive_worker_count() -> usize {
    std::thread::available_parallelism()
        .map_or(1, usize::from)
        .clamp(1, crate::LOCAL_DISCOVERY_WORKER_LIMIT)
}

/// 把队列耗尽的卷计入完成数。
fn mark_completed_queues(queues: &mut [VolumeQueue], progress: &mut RootDiscoveryProgress) {
    for queue in queues {
        if !queue.completed && queue.pending.is_empty() {
            queue.completed = true;
            progress.volumes_completed = progress.volumes_completed.saturating_add(1);
        }
    }
}

/// 读取一个目录的目录项元数据；此函数没有任何打开普通文件的入口。
fn inspect_directory_metadata(
    path: &Path,
    agents: RootDiscoveryAgentScope,
    coordinator: &RootDiscoveryCoordinator,
) -> DirectoryResult {
    let mut result = DirectoryResult::default();
    if !crate::readers::glm::path_rules::is_allowed_glm_discovery_directory(path) {
        return result;
    }
    if coordinator.is_cancel_requested() {
        return result;
    }
    if is_obviously_network_path(path) {
        result.skipped = 1;
        return result;
    }
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            result.permission_denied = 1;
            return result;
        }
        Err(_) => {
            result.io_errors = 1;
            return result;
        }
    };
    for entry in entries {
        if coordinator.is_cancel_requested() {
            break;
        }
        let Ok(entry) = entry else {
            result.io_errors = result.io_errors.saturating_add(1);
            continue;
        };
        let entry_path = entry.path();
        if is_unselected_agent_directory(&entry_path, agents) {
            result.skipped = result.skipped.saturating_add(1);
            continue;
        }
        let Ok(metadata) = fs::symlink_metadata(&entry_path) else {
            result.io_errors = result.io_errors.saturating_add(1);
            continue;
        };
        if metadata_is_link_like(&metadata) {
            result.skipped = result.skipped.saturating_add(1);
            continue;
        }
        if metadata.is_dir() {
            result.children.push(entry_path);
        } else if metadata.is_file() {
            result.file_names_checked = result.file_names_checked.saturating_add(1);
            if let Some(candidate) = candidate_from_file_name(&entry_path, agents) {
                result.candidates.push(candidate);
            }
        }
    }
    result
}

/// 仅为闭集内 Agent 按规范路径去重并提交候选，日志不得输出路径。
fn submit_candidate(
    coordinator: &RootDiscoveryCoordinator,
    path: PathBuf,
    evidence: RootCandidateEvidence,
    strategy: RootDiscoveryStrategy,
    agents: RootDiscoveryAgentScope,
    on_candidate: &impl Fn(RootCandidate),
) {
    let client = match evidence {
        RootCandidateEvidence::CodexRollout => SourceClientKind::Codex,
        RootCandidateEvidence::ClaudeTranscript | RootCandidateEvidence::ClaudeSubagent => {
            SourceClientKind::ClaudeCode
        }
        RootCandidateEvidence::GrokSessionUpdates => SourceClientKind::GrokBuildCli,
        RootCandidateEvidence::GlmModelIoUpdates => SourceClientKind::Glm,
    };
    if !agents.contains(client) {
        return;
    }
    let key = crate::path_key(&path);
    let candidate = RootCandidate {
        id: stable_id(client.candidate_namespace(), &key),
        client,
        absolute_path: path,
        strategy,
        evidence,
    };
    if coordinator.submit_candidate(candidate.clone()) {
        on_candidate(candidate);
    }
}

#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod tests;
