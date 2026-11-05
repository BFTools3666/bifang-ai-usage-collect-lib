//! 分片深度优先的元数据发现；工作集随目录深度而非整盘目录数增长。
use super::scope::{candidate_from_file_name, is_unselected_agent_directory, priority_queue};
use super::{MetadataDiscoverySummary, record_volume_scope_gaps, submit_candidate};
use crate::readers::directory_cursor::DirectoryCursor;
use crate::readers::discovery::metadata_is_link_like;
use crate::readers::{LocalVolumeRoots, is_obviously_network_path};
use crate::{
    RootCandidate, RootCandidateEvidence, RootDiscoveryAgentScope, RootDiscoveryCoordinator,
    RootDiscoveryPlatform, RootDiscoveryProgress, RootDiscoveryScope, RootDiscoveryStrategy,
    is_discovery_path_excluded, path_has_full_discovery_skip_directory_name,
};
use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};

/// 单个片段最多保留的目录项；只限制工作集，不限制总遍历范围或批次数。
const DIRECTORY_CHUNK_ENTRIES: usize = 128;

#[cfg(test)]
#[path = "traversal_tests.rs"]
mod tests;

/// 单个卷的深度优先游标栈及其完成标记。
#[derive(Debug)]
pub(super) struct VolumeQueue {
    /// 待处理的目录队列。
    pub(super) pending: VecDeque<DirectoryCursor>,
    /// 标识本卷是否已遍历完成。
    pub(super) completed: bool,
    /// 该卷发现遍历的根路径。
    pub(super) traversal_root: PathBuf,
}

/// 单个目录一次遍历产出的子目录、命中候选与错误/跳过计数。
#[derive(Debug, Default)]
pub(super) struct DirectoryResult {
    /// 待继续入队的子目录。
    pub(super) children: Vec<DirectoryCursor>,
    /// 本次遍历命中的候选路径及其证据类型。
    pub(super) candidates: Vec<(PathBuf, RootCandidateEvidence)>,
    /// 当前目录尚未处理的原枚举游标，片段消费后继续使用同一句柄。
    continuation: Option<DirectoryCursor>,
    /// 本片是否首次访问目录。
    started_now: bool,
    /// 本次检查过的文件名数量。
    pub(super) file_names_checked: u64,
    /// 权限拒绝计数。
    permission_denied: u64,
    /// I/O 错误计数。
    io_errors: u64,
    /// 因链接、非本地卷等原因跳过的计数。
    skipped: u64,
}

/// 单个搜索根的遍历统计，仅用于诊断事件。
#[derive(Debug)]
struct RootTraversalStats {
    /// 该根下已读取的目录数。
    directories: u64,
    /// 该根下已检查的文件名数。
    file_names: u64,
    /// 是否已发出完成事件。
    reported: bool,
}

/// 覆盖整次发现生命周期的固定目录 worker 池，避免每轮反复创建原生线程。
pub(super) struct MetadataWorkerPool {
    /// 任务发送端；关闭后 worker 退出。
    sender: Option<mpsc::SyncSender<(usize, DirectoryCursor)>>,
    /// 按任务序号返回目录结果；失败只保留空错误。
    results: mpsc::Receiver<(usize, Result<DirectoryResult, ()>)>,
    /// worker 线程句柄，池销毁时回收。
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl MetadataWorkerPool {
    /// 创建固定大小的元数据探测工作池并启动受控 worker。
    pub(super) fn new(
        worker_count: usize,
        agents: RootDiscoveryAgentScope,
        coordinator: RootDiscoveryCoordinator,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<(usize, DirectoryCursor)>(worker_count * 2);
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
                        inspect_directory_metadata(path, agents, &coordinator)
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

    /// 提交并收齐本轮有界目录片段，不跨轮缓存 worker 结果。
    pub(super) fn inspect(
        &self,
        jobs: Vec<(usize, DirectoryCursor)>,
    ) -> Vec<(usize, Result<DirectoryResult, ()>)> {
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

/// 内部实现：仅在闭集内跨多卷公平轮询遍历目录，命中即实时上报候选。
pub(super) fn discover_metadata_roots_in_with_callback(
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
                .map(DirectoryCursor::new)
                .collect(),
            completed: false,
            traversal_root: root,
        })
        .collect::<Vec<_>>();
    let seed_roots = queues
        .iter()
        .flat_map(|queue| {
            queue
                .pending
                .iter()
                .map(|cursor| cursor.path().to_path_buf())
        })
        .collect::<HashSet<_>>();
    let mut next_volume = 0_usize;
    let traversal_started = std::time::Instant::now();
    let mut stats = queues
        .iter()
        .map(|_| RootTraversalStats {
            directories: 0,
            file_names: 0,
            reported: false,
        })
        .collect::<Vec<_>>();
    let worker_pool = MetadataWorkerPool::new(adaptive_worker_count(), agents, coordinator.clone());

    while queues.iter().any(|queue| !queue.completed) {
        if coordinator.is_cancel_requested() {
            break;
        }
        let jobs = take_fair_jobs(&mut queues, adaptive_worker_count(), &mut next_volume);
        if jobs.is_empty() {
            mark_completed_queues(&mut queues, &mut progress);
            report_completed_roots(coordinator, &queues, &mut stats, traversal_started);
            continue;
        }
        let results = worker_pool.inspect(jobs);
        for (volume, joined) in results {
            if coordinator.is_cancel_requested() {
                break;
            }
            let Ok(result) = joined else {
                progress.io_errors = progress.io_errors.saturating_add(1);
                continue;
            };
            stats[volume].directories = stats[volume]
                .directories
                .saturating_add(u64::from(result.started_now));
            stats[volume].file_names = stats[volume]
                .file_names
                .saturating_add(result.file_names_checked);
            progress.directories_checked = progress
                .directories_checked
                .saturating_add(u64::from(result.started_now));
            progress.file_names_checked = progress
                .file_names_checked
                .saturating_add(result.file_names_checked);
            progress.permission_denied = progress
                .permission_denied
                .saturating_add(result.permission_denied);
            progress.io_errors = progress.io_errors.saturating_add(result.io_errors);
            progress.skipped = progress.skipped.saturating_add(result.skipped);
            let traversal_root = queues[volume].traversal_root.clone();
            if let Some(cursor) = result.continuation {
                queues[volume].pending.push_front(cursor);
            }
            for child in result.children.into_iter().rev() {
                if is_discovery_path_excluded(child.path(), &traversal_root, &excluded_roots) {
                    progress.skipped = progress.skipped.saturating_add(1);
                    continue;
                }
                if !seed_roots.contains(child.path()) {
                    queues[volume].pending.push_front(child);
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
        report_completed_roots(coordinator, &queues, &mut stats, traversal_started);
        coordinator.update_progress(progress);
    }
    coordinator.update_progress(progress);
    coordinator.emit_diagnostic(crate::RootDiscoveryDiagnostic::TraversalFinished {
        elapsed_ms: u64::try_from(traversal_started.elapsed().as_millis()).unwrap_or(u64::MAX),
    });
    coordinator.finish(false, true);
    MetadataDiscoverySummary {
        system_index_available: false,
        fallback_performed: true,
    }
}

/// 公平轮转各卷并优先最深游标，祖先续读不能越过已派发的深层目录片段。
pub(super) fn take_fair_jobs(
    queues: &mut [VolumeQueue],
    workers: usize,
    next_volume: &mut usize,
) -> Vec<(usize, DirectoryCursor)> {
    let mut jobs = Vec::new();
    if queues.is_empty() {
        return jobs;
    }
    let mut selected_depth = vec![None; queues.len()];
    let mut consecutive_empty = 0_usize;
    while jobs.len() < workers && consecutive_empty < queues.len() {
        let index = *next_volume % queues.len();
        *next_volume = (index + 1) % queues.len();
        let deepest = queues[index]
            .pending
            .iter()
            .map(DirectoryCursor::depth)
            .max();
        let eligible = deepest
            .is_some_and(|deepest| selected_depth[index].is_none_or(|depth| deepest >= depth));
        if eligible {
            let position = queues[index]
                .pending
                .iter()
                .position(|cursor| Some(cursor.depth()) == deepest)
                .expect("eligible cursor exists");
            let path = queues[index]
                .pending
                .remove(position)
                .expect("selected cursor exists");
            selected_depth[index] = Some(path.depth());
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

/// 为刚刚遍历完的搜索根各发出一次诊断事件；耗时从遍历阶段开始计，因为各根交错执行。
fn report_completed_roots(
    coordinator: &RootDiscoveryCoordinator,
    queues: &[VolumeQueue],
    stats: &mut [RootTraversalStats],
    traversal_started: std::time::Instant,
) {
    for (ordinal, (queue, stat)) in queues.iter().zip(stats.iter_mut()).enumerate() {
        if !queue.completed || stat.reported {
            continue;
        }
        stat.reported = true;
        coordinator.emit_diagnostic(crate::RootDiscoveryDiagnostic::TraversalRootFinished {
            root: queue
                .traversal_root
                .file_name()
                .map_or_else(String::new, |name| name.to_string_lossy().into_owned()),
            ordinal: u64::try_from(ordinal).unwrap_or(u64::MAX),
            directories: stat.directories,
            file_names: stat.file_names,
            elapsed_ms: u64::try_from(traversal_started.elapsed().as_millis())
                .unwrap_or(u64::MAX),
        });
    }
}

/// 读取一个目录的目录项元数据；此函数没有任何打开普通文件的入口。
fn inspect_directory_metadata(
    mut cursor: DirectoryCursor,
    agents: RootDiscoveryAgentScope,
    coordinator: &RootDiscoveryCoordinator,
) -> DirectoryResult {
    let mut result = DirectoryResult {
        started_now: !cursor.started(),
        ..Default::default()
    };
    if !crate::readers::glm::path_rules::is_allowed_glm_discovery_directory(cursor.path()) {
        return result;
    }
    if coordinator.is_cancel_requested() {
        return result;
    }
    if is_obviously_network_path(cursor.path()) {
        result.skipped = 1;
        return result;
    }
    for _ in 0..DIRECTORY_CHUNK_ENTRIES {
        if coordinator.is_cancel_requested() {
            return result;
        }
        let entry = match cursor.next_entry() {
            Ok(Some(entry)) => entry,
            Ok(None) => return result,
            Err(error) => {
                if error.kind() == std::io::ErrorKind::PermissionDenied {
                    result.permission_denied += 1;
                } else {
                    result.io_errors += 1;
                }
                if !cursor.started() {
                    return result;
                }
                continue;
            }
        };
        let path = entry.path();
        if is_unselected_agent_directory(&path, agents) {
            result.skipped += 1;
            continue;
        }
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            result.io_errors += 1;
            continue;
        };
        if metadata_is_link_like(&metadata) {
            result.skipped += 1;
            continue;
        }
        if metadata.is_dir() {
            result.children.push(cursor.child(path));
        } else if metadata.is_file() {
            result.file_names_checked += 1;
            if let Some(candidate) = candidate_from_file_name(&path, agents) {
                result.candidates.push(candidate);
            }
        }
    }
    result.continuation = Some(cursor);
    result
}
