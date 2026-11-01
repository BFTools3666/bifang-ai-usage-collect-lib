//! 定义不读取会话正文的数据根发现任务、候选与状态机。

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use crate::{CoverageState, RootDiscoveryAgentScope, SourceClientKind};

/// adapter 只能给出失败覆盖而没有更具体原因时使用的稳定发现错误码。
pub const ROOT_DISCOVERY_COVERAGE_FAILED_CODE: &str = "coverage_failed";

/// 数据根发现任务的稳定生命周期。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootDiscoveryLifecycle {
    /// 尚未启动任务。
    Idle,
    /// 正在查询平台索引或遍历目录项。
    Running,
    /// 所有可访问本地卷均已遍历完成。
    Complete,
    /// 已完成遍历，但存在权限或 I/O 缺口。
    Partial,
    /// 用户已请求取消。
    Cancelled,
    /// 任务无法继续执行。
    Failed,
}

/// 发现候选所使用的平台策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootDiscoveryStrategy {
    /// Windows Search 元数据索引。
    WindowsSearch,
    /// macOS Spotlight 元数据索引。
    MacOsSpotlight,
    /// 普通文件系统元数据遍历。
    MetadataTraversal,
}

/// 数据源发现使用的平台路径策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootDiscoveryPlatform {
    /// Windows 用户目录与本地卷规则。
    Windows,
    /// macOS 用户目录与本地卷规则。
    MacOs,
    /// 没有专属系统索引与盘符规则的平台（含 Linux）；优先目录表见 `root_discovery_priority_relative_roots`。
    Other,
}

/// 数据源发现的用户选择范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootDiscoveryScope {
    /// 仅扫描当前平台的优先用户目录。
    UserPriority,
    /// 扫描所有可确认的本地卷，仍优先处理用户目录。
    FullLocalVolumes,
    /// 用户手动选择的单个本地子树深搜。
    ManualSubtree,
}

/// 候选根命中的结构证据。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootCandidateEvidence {
    /// `sessions` 或 `archived_sessions` 下的 rollout 文件名。
    CodexRollout,
    /// `projects/<project>/<UUID>.jsonl` 主 transcript 文件名。
    ClaudeTranscript,
    /// 合法 subagent 层级中的 transcript 文件名。
    ClaudeSubagent,
    /// `sessions/<cwd>/<session>/updates.jsonl` 完成轮次用量文件。
    GrokSessionUpdates,
}

/// 发现阶段产生且只在当前进程内存活的候选。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootCandidate {
    /// 当前发现任务内稳定的临时 ID。
    pub id: String,
    /// 候选所属客户端。
    pub client: SourceClientKind,
    /// 仅供 Rust 内部验证和登记使用的完整绝对路径；不得进入 DTO 或 WebView。
    pub absolute_path: String,
    /// 首次发现该候选的平台策略。
    pub strategy: RootDiscoveryStrategy,
    /// 文件名与父目录结构证据。
    pub evidence: RootCandidateEvidence,
}

/// 候选确认请求的稳定领域错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootCandidateSelectionError {
    /// 请求包含当前发现任务不存在的临时候选 ID。
    UnknownCandidate,
}

/// 不依赖虚假百分比的发现进度计数。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RootDiscoveryProgress {
    /// 已完成遍历的本地卷数。
    pub volumes_completed: u64,
    /// 本次任务识别到的本地卷总数。
    pub volumes_total: u64,
    /// 已读取目录项列表的目录数。
    pub directories_checked: u64,
    /// 已检查的普通文件名数量。
    pub file_names_checked: u64,
    /// 当前去重后的候选数。
    pub candidates_found: u64,
    /// 权限拒绝数量。
    pub permission_denied: u64,
    /// 其他 I/O 错误数量。
    pub io_errors: u64,
    /// 因链接、reparse、网络或未知卷跳过的数量。
    pub skipped: u64,
}

/// 可轮询的数据根发现状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootDiscoveryStatus {
    /// 当前进程内单调递增的发现运行编号；零仅表示尚未启动。
    pub run_id: u64,
    /// 当前生命周期。
    pub lifecycle: RootDiscoveryLifecycle,
    /// 当前阶段使用的平台策略。
    pub strategy: RootDiscoveryStrategy,
    /// 当前运行平台。
    pub platform: RootDiscoveryPlatform,
    /// 当前用户选择的发现范围。
    pub scope: RootDiscoveryScope,
    /// 当前任务冻结的物理 Agent 闭集。
    pub agent_scope: RootDiscoveryAgentScope,
    /// 系统索引是否可用。
    pub system_index_available: bool,
    /// 是否已经执行普通元数据兜底。
    pub fallback_performed: bool,
    /// 结构化进度计数。
    pub progress: RootDiscoveryProgress,
    /// 仅包含稳定错误码，不包含路径。
    pub error_code: Option<String>,
}

impl Default for RootDiscoveryStatus {
    /// 构造尚未启动的数据源发现状态。
    fn default() -> Self {
        Self {
            run_id: 0,
            lifecycle: RootDiscoveryLifecycle::Idle,
            strategy: RootDiscoveryStrategy::MetadataTraversal,
            platform: RootDiscoveryPlatform::Other,
            scope: RootDiscoveryScope::UserPriority,
            agent_scope: RootDiscoveryAgentScope::all_physical(),
            system_index_available: false,
            fallback_performed: false,
            progress: RootDiscoveryProgress::default(),
            error_code: None,
        }
    }
}

/// 线程安全地保存单个客户端当前发现任务和内存候选。
#[derive(Debug, Clone, Default)]
pub struct RootDiscoveryCoordinator {
    /// 受互斥锁保护的发现状态，供后台任务与界面轮询共享。
    inner: Arc<Mutex<RootDiscoveryState>>,
}

#[derive(Debug, Default)]
/// 保存测试期间发现任务的阶段、候选与取消状态。
struct RootDiscoveryState {
    /// 发现任务当前对外状态。
    status: RootDiscoveryStatus,
    /// 按候选 ID 保存的待确认候选。
    candidates: BTreeMap<String, RootCandidate>,
    /// 候选的首次出现顺序，保证列表稳定。
    candidate_order: Vec<String>,
    /// 是否已请求取消；任务据此尽快停止。
    cancel_requested: bool,
}

impl RootDiscoveryCoordinator {
    /// 启动新任务并清除上次未确认候选；已有任务运行时返回 `false`。
    pub fn start(
        &self,
        strategy: RootDiscoveryStrategy,
        platform: RootDiscoveryPlatform,
        scope: RootDiscoveryScope,
        volumes_total: u64,
    ) -> bool {
        self.start_with_agent_scope(
            strategy,
            platform,
            scope,
            volumes_total,
            RootDiscoveryAgentScope::all_physical(),
        )
    }

    /// 启动带物理 Agent 闭集的新任务，并清除上次未确认候选。
    pub fn start_with_agent_scope(
        &self,
        strategy: RootDiscoveryStrategy,
        platform: RootDiscoveryPlatform,
        scope: RootDiscoveryScope,
        volumes_total: u64,
        agent_scope: RootDiscoveryAgentScope,
    ) -> bool {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if state.status.lifecycle == RootDiscoveryLifecycle::Running {
            return false;
        }
        let run_id = state.status.run_id.saturating_add(1);
        state.status = RootDiscoveryStatus {
            run_id,
            lifecycle: RootDiscoveryLifecycle::Running,
            strategy,
            platform,
            scope,
            agent_scope,
            progress: RootDiscoveryProgress {
                volumes_total,
                ..RootDiscoveryProgress::default()
            },
            ..RootDiscoveryStatus::default()
        };
        state.candidates.clear();
        state.candidate_order.clear();
        state.cancel_requested = false;
        true
    }

    /// 返回当前发现任务冻结的物理 Agent 闭集。
    pub fn agent_scope(&self) -> RootDiscoveryAgentScope {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .status
            .agent_scope
    }

    /// 返回当前状态快照。
    pub fn snapshot(&self) -> RootDiscoveryStatus {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .status
            .clone()
    }

    /// 返回当前任务全部候选快照。
    pub fn candidates(&self) -> Vec<RootCandidate> {
        let state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state
            .candidate_order
            .iter()
            .filter_map(|id| state.candidates.get(id).cloned())
            .collect()
    }

    /// 验证并返回待确认候选；重复 ID 只返回一次，未知 ID 整体拒绝。
    pub fn select_candidates(
        &self,
        candidate_ids: &[String],
    ) -> Result<Vec<RootCandidate>, RootCandidateSelectionError> {
        let state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let unique_ids = candidate_ids.iter().collect::<BTreeSet<_>>();
        unique_ids
            .into_iter()
            .map(|id| {
                state
                    .candidates
                    .get(id)
                    .cloned()
                    .ok_or(RootCandidateSelectionError::UnknownCandidate)
            })
            .collect()
    }

    /// 在全部登记成功后释放已确认候选，不持久化其临时状态。
    pub fn remove_candidates(&self, candidate_ids: &[String]) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        for id in candidate_ids {
            state.candidates.remove(id);
            state
                .candidate_order
                .retain(|candidate_id| candidate_id != id);
        }
        state.status.progress.candidates_found = state.candidates.len() as u64;
    }

    /// 追加候选并按临时 ID 去重；首次插入返回 `true`。
    pub fn submit_candidate(&self, candidate: RootCandidate) -> bool {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let inserted = !state.candidates.contains_key(&candidate.id);
        if inserted {
            state.candidate_order.push(candidate.id.clone());
            state.candidates.insert(candidate.id.clone(), candidate);
        }
        state.status.progress.candidates_found = state.candidates.len() as u64;
        inserted
    }

    /// 覆盖当前结构化进度。
    pub fn update_progress(&self, progress: RootDiscoveryProgress) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.status.progress = progress;
        state.status.progress.candidates_found = state.candidates.len() as u64;
    }

    /// 切换到普通遍历阶段并记录平台索引可用性。
    pub fn begin_fallback(&self, system_index_available: bool) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.status.strategy = RootDiscoveryStrategy::MetadataTraversal;
        state.status.system_index_available = system_index_available;
        state.status.fallback_performed = true;
    }

    /// 请求任务在下一个目录项边界取消。
    pub fn request_cancel(&self) -> bool {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if state.status.lifecycle != RootDiscoveryLifecycle::Running {
            return false;
        }
        state.cancel_requested = true;
        true
    }

    /// 返回是否已经收到取消请求。
    pub fn is_cancel_requested(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cancel_requested
    }

    /// 按已累计的安全计数结束任务；任何明确覆盖缺口都降级为部分完成。
    pub fn finish(&self, system_index_available: bool, fallback_performed: bool) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.status.system_index_available = system_index_available;
        state.status.fallback_performed = fallback_performed;
        state.status.lifecycle = if state.cancel_requested {
            RootDiscoveryLifecycle::Cancelled
        } else if state.status.progress.permission_denied > 0
            || state.status.progress.io_errors > 0
            || state.status.progress.skipped > 0
        {
            RootDiscoveryLifecycle::Partial
        } else {
            RootDiscoveryLifecycle::Complete
        };
    }

    /// 按 adapter 的完整覆盖结论结束任务，同时以协调器计数作保守降级。
    pub fn finish_with_coverage(
        &self,
        coverage: CoverageState,
        system_index_available: bool,
        fallback_performed: bool,
    ) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.status.system_index_available = system_index_available;
        state.status.fallback_performed = fallback_performed;
        let has_gaps = state.status.progress.permission_denied > 0
            || state.status.progress.io_errors > 0
            || state.status.progress.skipped > 0;
        state.status.lifecycle = if state.cancel_requested || coverage == CoverageState::Cancelled {
            RootDiscoveryLifecycle::Cancelled
        } else {
            match coverage {
                CoverageState::Complete if !has_gaps => RootDiscoveryLifecycle::Complete,
                CoverageState::Complete | CoverageState::Partial => RootDiscoveryLifecycle::Partial,
                CoverageState::Cancelled => RootDiscoveryLifecycle::Cancelled,
                CoverageState::Failed => RootDiscoveryLifecycle::Failed,
            }
        };
        if state.status.lifecycle == RootDiscoveryLifecycle::Failed
            && state.status.error_code.is_none()
        {
            state.status.error_code = Some(ROOT_DISCOVERY_COVERAGE_FAILED_CODE.to_owned());
        }
    }

    /// 以稳定错误码结束无法继续的任务。
    pub fn fail(&self, error_code: impl Into<String>) {
        let mut state = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        state.status.lifecycle = RootDiscoveryLifecycle::Failed;
        state.status.error_code = Some(error_code.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证权限缺口保留计数，并把终态保守降级为部分完成。
    #[test]
    fn permission_gap_finishes_with_warning_counts() {
        let coordinator = RootDiscoveryCoordinator::default();
        assert!(coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::UserPriority,
            2
        ));
        coordinator.update_progress(RootDiscoveryProgress {
            volumes_completed: 2,
            volumes_total: 2,
            permission_denied: 1,
            ..RootDiscoveryProgress::default()
        });
        coordinator.finish(false, true);
        assert_eq!(
            coordinator.snapshot().lifecycle,
            RootDiscoveryLifecycle::Partial
        );
    }

    /// 验证取消不会被结束逻辑覆盖成成功。
    #[test]
    fn cancellation_is_terminal() {
        let coordinator = RootDiscoveryCoordinator::default();
        assert!(coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::UserPriority,
            1
        ));
        assert!(coordinator.request_cancel());
        coordinator.finish(false, true);
        assert_eq!(
            coordinator.snapshot().lifecycle,
            RootDiscoveryLifecycle::Cancelled
        );
    }

    /// 验证链接、reparse、网络或未知卷跳过后以部分覆盖结束。
    #[test]
    fn policy_skip_finishes_with_warning_counts() {
        let coordinator = RootDiscoveryCoordinator::default();
        assert!(coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::UserPriority,
            1
        ));
        coordinator.update_progress(RootDiscoveryProgress {
            volumes_completed: 1,
            volumes_total: 1,
            skipped: 1,
            ..RootDiscoveryProgress::default()
        });
        coordinator.finish(false, true);
        assert_eq!(
            coordinator.snapshot().lifecycle,
            RootDiscoveryLifecycle::Partial
        );
    }

    /// 每次真正启动都生成新运行编号，运行中的拒绝不会消耗编号。
    #[test]
    fn successful_starts_advance_the_run_id() {
        let coordinator = RootDiscoveryCoordinator::default();
        assert_eq!(coordinator.snapshot().run_id, 0);
        assert!(coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::UserPriority,
            1,
        ));
        assert_eq!(coordinator.snapshot().run_id, 1);
        assert!(!coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::UserPriority,
            1,
        ));
        assert_eq!(coordinator.snapshot().run_id, 1);
        coordinator.finish(false, false);
        assert!(coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::UserPriority,
            1,
        ));
        assert_eq!(coordinator.snapshot().run_id, 2);
    }

    /// adapter 的覆盖状态可明确保留预算缺口与取消终态。
    #[test]
    fn explicit_coverage_controls_the_terminal_state() {
        let coordinator = RootDiscoveryCoordinator::default();
        assert!(coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::ManualSubtree,
            1,
        ));
        coordinator.finish_with_coverage(CoverageState::Partial, false, true);
        assert_eq!(
            coordinator.snapshot().lifecycle,
            RootDiscoveryLifecycle::Partial
        );

        assert!(coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::ManualSubtree,
            1,
        ));
        coordinator.finish_with_coverage(CoverageState::Cancelled, false, true);
        assert_eq!(
            coordinator.snapshot().lifecycle,
            RootDiscoveryLifecycle::Cancelled
        );

        assert!(coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::ManualSubtree,
            1,
        ));
        coordinator.finish_with_coverage(CoverageState::Failed, false, true);
        let failed = coordinator.snapshot();
        assert_eq!(failed.lifecycle, RootDiscoveryLifecycle::Failed);
        assert_eq!(
            failed.error_code.as_deref(),
            Some(ROOT_DISCOVERY_COVERAGE_FAILED_CODE)
        );
    }

    /// 验证轮询快照与生命周期状态原子携带本轮 Agent 归属。
    #[test]
    fn snapshot_preserves_the_frozen_agent_scope() {
        let coordinator = RootDiscoveryCoordinator::default();
        let claude_scope = RootDiscoveryAgentScope::for_agent(SourceClientKind::ClaudeCode)
            .expect("Claude Code is a physical discovery Agent");
        assert!(coordinator.start_with_agent_scope(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::FullLocalVolumes,
            1,
            claude_scope,
        ));

        let status = coordinator.snapshot();
        assert_eq!(status.agent_scope, claude_scope);
        assert!(!status.agent_scope.contains(SourceClientKind::Codex));
        assert!(status.agent_scope.contains(SourceClientKind::ClaudeCode));
    }

    /// 验证候选确认去重、未知 ID 拒绝，且未确认候选不会跨任务持久化。
    #[test]
    fn candidate_selection_is_runtime_only_and_strict() {
        let coordinator = RootDiscoveryCoordinator::default();
        assert!(coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::UserPriority,
            1
        ));
        let candidate = RootCandidate {
            id: "candidate-a".to_owned(),
            client: SourceClientKind::Codex,
            absolute_path: "C:/runtime-only".to_owned(),
            strategy: RootDiscoveryStrategy::MetadataTraversal,
            evidence: RootCandidateEvidence::CodexRollout,
        };
        assert!(coordinator.submit_candidate(candidate.clone()));
        assert!(!coordinator.submit_candidate(candidate.clone()));
        let selected = coordinator
            .select_candidates(&["candidate-a".to_owned(), "candidate-a".to_owned()])
            .unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0], candidate);
        assert_eq!(
            coordinator.select_candidates(&["missing".to_owned()]),
            Err(RootCandidateSelectionError::UnknownCandidate)
        );

        coordinator.finish(false, true);
        assert!(coordinator.start(
            RootDiscoveryStrategy::MetadataTraversal,
            RootDiscoveryPlatform::Other,
            RootDiscoveryScope::UserPriority,
            1
        ));
        assert!(coordinator.candidates().is_empty());
        assert!(coordinator.submit_candidate(candidate.clone()));
        assert_eq!(coordinator.candidates(), [candidate]);
    }
}
