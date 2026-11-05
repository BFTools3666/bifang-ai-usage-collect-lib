//! 平台范围只凭卷、目录项与文件名发现候选；显式环境根另走有界严格签名。

#[cfg(test)]
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::{
    CoverageReport, RootCandidate, RootCandidateEvidence, RootDiscoveryAgentScope,
    RootDiscoveryCoordinator, RootDiscoveryProgress, RootDiscoveryScope, RootDiscoveryStrategy,
    SourceClientKind, append_platform_discovery_excludes_for_volumes,
    path_has_full_discovery_skip_directory_name, stable_id,
};

use super::current_user_home;
use super::glm::discovery::discover_zcode_quick;
use super::{
    CancellationToken, ClaudeDiscoveryInputs, DiscoveryInputs, GlmDiscoveryInputs,
    GrokDiscoveryInputs, LocalVolumeRoots, discover_claude_quick, discover_grok_quick,
    discover_quick, validate_local_plain_directory,
};

#[cfg(test)]
use crate::RootDiscoveryPlatform;

mod platform_index;
mod scope;
mod traversal;
use traversal::discover_metadata_roots_in_with_callback;
#[cfg(test)]
use traversal::{MetadataWorkerPool, VolumeQueue, take_fair_jobs};

use scope::{
    candidate_from_file_name, current_platform, discovery_scope,
    path_under_unselected_agent_directory, platform_priority_roots,
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
    let on_indexed_path = |path: PathBuf| {
        if coordinator.is_cancel_requested() {
            return;
        }
        if path_blocked_by_discovery_policy(&path, &volumes)
            || !path_is_in_search_scope(&path, &volumes)
            || path_under_unselected_agent_directory(&path, agents)
        {
            return;
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
    };
    let index_started = std::time::Instant::now();
    let index_names_before = coordinator.snapshot().progress.file_names_checked;
    let system_index_available = platform_index::should_query(discovery_kind)
        && platform_index::query(&volumes, coordinator, agents, on_indexed_path);
    coordinator.emit_diagnostic(crate::RootDiscoveryDiagnostic::SystemIndexFinished {
        available: system_index_available,
        paths: coordinator
            .snapshot()
            .progress
            .file_names_checked
            .saturating_sub(index_names_before),
        elapsed_ms: u64::try_from(index_started.elapsed().as_millis()).unwrap_or(u64::MAX),
    });
    if coordinator.is_cancel_requested() {
        coordinator.finish(system_index_available, false);
        return MetadataDiscoverySummary {
            system_index_available,
            fallback_performed: false,
        };
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
                discover_zcode_quick(
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
use scope::priority_queue;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod zcode_tests;
