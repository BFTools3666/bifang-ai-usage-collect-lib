//! GLM（Z Code CLI）数据根发现：只承认平铺 `model-io-sess_*.jsonl` 结构签名。

mod full;
mod signature;

#[cfg(test)]
mod tests;

pub use full::discover_glm_full_device_with_progress;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use crate::{CoverageReport, RootCandidateEvidence};

use super::super::discovery::{path_key, stable_id};
use super::super::{
    CancellationToken, DiscoveryMethod, FullDiscoveryOptions, LocalPathStatus, RegisteredRoot,
    classify_local_path, is_obviously_network_path,
};
use signature::{
    Inspection, SignatureBudget, coverage_state, default_rollout_dir, inspect_root,
    reject_link_components, safe_alias,
};

pub use signature::{
    Inspection as GlmRootInspection, SignatureBudget as GlmSignatureBudget,
    inspect_root as inspect_glm_root,
};

/// 汇总 GLM 快速发现的显式候选，不在构造阶段访问文件系统。
#[derive(Debug, Clone, Default)]
pub struct GlmDiscoveryInputs {
    /// 当前用户主目录；存在时只检查其 `.zcode/cli/rollout` 直接候选。
    pub home_dir: Option<PathBuf>,
    /// 仅属于 GLM 客户端的已登记根。
    pub registered_roots: Vec<RegisteredRoot>,
}

impl GlmDiscoveryInputs {
    /// 从进程环境生成候选；不会读取凭据或 `~/.zcode` 下的其他文件。
    pub fn from_process_environment(registered_roots: Vec<RegisteredRoot>) -> Self {
        Self {
            home_dir: super::super::current_user_home(),
            registered_roots,
        }
    }
}

/// 一个通过 GLM rollout 签名确认的数据根。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlmDiscoveredRoot {
    /// 仅 backend 使用的规范化访问路径。
    pub path: PathBuf,
    /// 按规范化访问位置生成或从 registry 恢复的稳定根 ID。
    pub root_id: String,
    /// 不包含完整绝对路径的展示别名。
    pub alias: String,
    /// 根进入发现流程的受控入口。
    pub discovery_method: DiscoveryMethod,
    /// 通过严格签名时实际命中的结构类型。
    pub evidence: RootCandidateEvidence,
}

/// GLM 发现结果；字段与 Codex/Claude/Grok 覆盖模型一致，但根类型不可混用。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlmDiscoveryResult {
    /// 本次确认的发现根。
    pub roots: Vec<GlmDiscoveredRoot>,
    /// 已确认不再合格、应从 registry 移除的根 ID。
    pub confirmed_invalid_root_ids: Vec<String>,
    /// 未能得出确定结论、应保留旧登记的根 ID。
    pub unconfirmed_root_ids: Vec<String>,
    /// 本次发现的覆盖结论。
    pub coverage: CoverageReport,
    /// 本次发现遍历的目录数。
    pub directories_scanned: u64,
    /// 因符号链接跳过的计数。
    pub symlink_skipped_count: u64,
    /// 因网络路径跳过的计数。
    pub network_skipped_count: u64,
}

/// 表示待核对的 GLM 本机根候选及其发现来源。
struct Candidate {
    /// 已登记数据根的 ID；尚未登记为 `None`。
    existing_root_id: Option<String>,
    /// 被同安装数据库替代的旧日志根；仅在新来源确认后删除派生观察。
    replaced_root_id: Option<String>,
    /// 候选根的真实路径，不进入对外结果。
    path: PathBuf,
    /// 候选展示别名。
    alias: String,
    /// 发现该候选所用的方法。
    method: DiscoveryMethod,
}

/// 只快速检查默认本机目录与已启用登记根。
pub fn discover_glm_quick(
    inputs: &GlmDiscoveryInputs,
    cancellation: &CancellationToken,
) -> GlmDiscoveryResult {
    inspect_candidates(
        quick_candidates(inputs),
        cancellation,
        &FullDiscoveryOptions::default(),
    )
}

/// 从默认本机目录与已启用登记根生成有界快速扫描候选。
fn quick_candidates(inputs: &GlmDiscoveryInputs) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    for root in &inputs.registered_roots {
        if root.enabled {
            let preferred = super::path_rules::preferred_glm_root(&root.path);
            let replacing = preferred != root.path;
            candidates.push(Candidate {
                existing_root_id: if replacing {
                    None
                } else {
                    root.root_id.clone()
                },
                replaced_root_id: if replacing {
                    root.root_id.clone()
                } else {
                    None
                },
                path: preferred,
                alias: root.alias.clone(),
                method: DiscoveryMethod::Registered,
            });
        }
    }
    if let Some(home_dir) = &inputs.home_dir {
        let rollout = default_rollout_dir(home_dir);
        let database = rollout
            .parent()
            .expect("fixed rollout has parent")
            .join("db");
        let path = if inputs
            .registered_roots
            .iter()
            .any(|root| root.enabled && root.path == database)
        {
            database
        } else {
            super::path_rules::preferred_glm_root(&rollout)
        };
        candidates.push(Candidate {
            existing_root_id: None,
            replaced_root_id: None,
            alias: safe_alias(&path),
            path,
            method: DiscoveryMethod::DefaultHome,
        });
    }
    candidates
}

/// 逐个核对候选的严格 GLM 签名并汇总安全发现结果。
fn inspect_candidates(
    candidates: Vec<Candidate>,
    cancellation: &CancellationToken,
    options: &FullDiscoveryOptions,
) -> GlmDiscoveryResult {
    let mut roots = BTreeMap::<PathBuf, GlmDiscoveredRoot>::new();
    let mut confirmed_invalid_ids = BTreeSet::new();
    let mut unconfirmed_ids = BTreeSet::new();
    let mut inspected_paths = BTreeSet::new();
    let mut permission_denied_count = 0_u64;
    let mut skipped_count = 0_u64;
    let mut symlink_skipped_count = 0_u64;
    let mut network_skipped_count = 0_u64;
    let mut cancelled = false;
    let mut budget_exhausted = false;
    let mut budget = SignatureBudget::new(options, cancellation);

    for candidate in candidates {
        let database_authority = super::path_rules::is_glm_database_root(&candidate.path);
        if cancellation.is_cancelled() {
            cancelled = true;
            break;
        }
        if is_obviously_network_path(&candidate.path) {
            network_skipped_count = network_skipped_count.saturating_add(1);
            skipped_count = skipped_count.saturating_add(1);
            if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                if database_authority {
                    unconfirmed_ids.insert(id);
                } else {
                    confirmed_invalid_ids.insert(id);
                }
            }
            continue;
        }
        match reject_link_components(&candidate.path) {
            Ok(true) => {
                symlink_skipped_count = symlink_skipped_count.saturating_add(1);
                skipped_count = skipped_count.saturating_add(1);
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    if database_authority {
                        unconfirmed_ids.insert(id);
                    } else {
                        confirmed_invalid_ids.insert(id);
                    }
                }
                continue;
            }
            Ok(false) => {}
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                permission_denied_count = permission_denied_count.saturating_add(1);
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    unconfirmed_ids.insert(id);
                }
                continue;
            }
            Err(_) => {
                skipped_count = skipped_count.saturating_add(1);
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    unconfirmed_ids.insert(id);
                }
                continue;
            }
        }
        match classify_local_path(&candidate.path) {
            LocalPathStatus::ConfirmedLocal => {}
            LocalPathStatus::RejectedNetwork => {
                network_skipped_count = network_skipped_count.saturating_add(1);
                skipped_count = skipped_count.saturating_add(1);
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    if database_authority {
                        unconfirmed_ids.insert(id);
                    } else {
                        confirmed_invalid_ids.insert(id);
                    }
                }
                continue;
            }
            LocalPathStatus::Missing => {
                if database_authority
                    || candidate
                        .method
                        .counts_as_coverage_gap(candidate.existing_root_id.is_some())
                {
                    skipped_count = skipped_count.saturating_add(1);
                }
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    if database_authority {
                        unconfirmed_ids.insert(id);
                    } else {
                        confirmed_invalid_ids.insert(id);
                    }
                }
                continue;
            }
            LocalPathStatus::Indeterminate => {
                skipped_count = skipped_count.saturating_add(1);
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    unconfirmed_ids.insert(id);
                }
                continue;
            }
        }
        let normalized = match fs::canonicalize(&candidate.path) {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    if database_authority {
                        unconfirmed_ids.insert(id);
                    } else {
                        confirmed_invalid_ids.insert(id);
                    }
                }
                continue;
            }
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                permission_denied_count = permission_denied_count.saturating_add(1);
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    unconfirmed_ids.insert(id);
                }
                continue;
            }
            Err(_) => {
                skipped_count = skipped_count.saturating_add(1);
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    unconfirmed_ids.insert(id);
                }
                continue;
            }
        };
        if !inspected_paths.insert(normalized.clone()) {
            if let Some(root) = roots.get_mut(&normalized) {
                if let Some(id) = candidate.replaced_root_id {
                    confirmed_invalid_ids.insert(id);
                }
                if let Some(id) = candidate.existing_root_id {
                    // 已登记数据库身份优先于从旧日志候选派生的新身份；与 core 的路径恢复一致。
                    root.root_id = id;
                    root.alias = candidate.alias;
                    root.discovery_method = candidate.method;
                }
            }
            continue;
        }
        match inspect_root(&normalized, &mut budget) {
            Inspection::Found(evidence) => {
                if let Some(id) = candidate.replaced_root_id {
                    confirmed_invalid_ids.insert(id);
                }
                let root_id = candidate
                    .existing_root_id
                    .unwrap_or_else(|| stable_id("glm-root", &path_key(&normalized)));
                roots.insert(
                    normalized.clone(),
                    GlmDiscoveredRoot {
                        path: normalized,
                        root_id,
                        alias: candidate.alias,
                        discovery_method: candidate.method,
                        evidence,
                    },
                );
            }
            Inspection::NotRoot | Inspection::Rejected => {
                if database_authority {
                    skipped_count = skipped_count.saturating_add(1);
                }
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    if database_authority {
                        unconfirmed_ids.insert(id);
                    } else {
                        confirmed_invalid_ids.insert(id);
                    }
                }
            }
            Inspection::Indeterminate => {
                skipped_count = skipped_count.saturating_add(1);
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    unconfirmed_ids.insert(id);
                }
            }
            Inspection::Cancelled => {
                cancelled = true;
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    unconfirmed_ids.insert(id);
                }
                break;
            }
            Inspection::BudgetExhausted => {
                budget_exhausted = true;
                if let Some(id) = candidate.existing_root_id.or(candidate.replaced_root_id) {
                    unconfirmed_ids.insert(id);
                }
                break;
            }
        }
    }

    GlmDiscoveryResult {
        coverage: CoverageReport {
            state: coverage_state(
                cancelled,
                budget_exhausted,
                permission_denied_count,
                skipped_count,
            ),
            roots_scanned: u64::try_from(roots.len()).unwrap_or(u64::MAX),
            roots_discovered: u64::try_from(roots.len()).unwrap_or(u64::MAX),
            permission_denied_count,
            skipped_count,
            warning_count: 0,
        },
        roots: roots.into_values().collect(),
        confirmed_invalid_root_ids: confirmed_invalid_ids.into_iter().collect(),
        unconfirmed_root_ids: unconfirmed_ids.into_iter().collect(),
        directories_scanned: 0,
        symlink_skipped_count,
        network_skipped_count,
    }
}
