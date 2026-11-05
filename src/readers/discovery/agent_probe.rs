//! 三个按 Agent 划分的结构签名探测（Claude / Grok / ZCode）共用的预算、结论枚举与
//! 覆盖、排除、链接辅助函数；各 Agent 只保留自己的结构判定与展示别名。

use std::path::{Path, PathBuf};

use crate::{CoverageState, RootCandidateEvidence};

use super::super::{CancellationToken, FullDiscoveryOptions};
use super::walk_ancestors_for_link_component;

/// 一次全设备发现内所有候选共享的结构签名探测预算，候选之间不会重新计数。
pub struct SignatureBudget<'a> {
    /// 用于中途响应取消请求的令牌。
    pub(crate) cancellation: &'a CancellationToken,
    /// 剩余允许遍历的目录数预算。
    pub(crate) remaining_directories: u64,
    /// 剩余允许处理的目录项数预算。
    pub(crate) remaining_entries: u64,
    /// 剩余允许打开的文件数预算。
    pub(crate) remaining_files: u64,
    /// 剩余允许读取的字节数预算。
    pub(crate) remaining_bytes: u64,
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
// 区分“确认不是”（Rejected/NotRoot）与“无法确认”（Indeterminate/
// Cancelled/BudgetExhausted）：前者可放心当作否定结论丢弃候选根，后者
// 必须保留旧登记，不能因权限突变、路径被移动等瞬时问题误删用户数据源。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inspection {
    /// 确认是该 Agent 的数据根，并保留命中的结构证据类型。
    Found(RootCandidateEvidence),
    /// 确认不是候选根（例如目录不存在）。
    NotRoot,
    /// 确认结构不符合该 Agent 的特征，视同否定结论。
    Rejected,
    /// 未能得出确定结论（如临时性 I/O 错误）；调用方须原样保留旧登记。
    Indeterminate,
    /// 探测过程中收到取消信号，提前中止。
    Cancelled,
    /// 预算耗尽提前中止，尚未得出结论。
    BudgetExhausted,
}

/// 沿路径的每一级祖先检查是否存在链接组件，供候选路径安全性校验复用。
pub fn reject_link_components(path: &Path) -> std::io::Result<bool> {
    match walk_ancestors_for_link_component(path) {
        Ok(has_link) => Ok(has_link),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// 按取消、预算耗尽与权限/跳过计数折算出本次发现的覆盖状态。
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

/// 判定路径是否落在本次遍历需要排除的固定根集合内。
pub fn is_excluded(path: &Path, traversal_root: &Path, excluded_roots: &[PathBuf]) -> bool {
    crate::is_discovery_path_excluded(path, traversal_root, excluded_roots)
}

/// 判定路径是否越过了另一个独立搜索根，避免同一子树被重复遍历。
pub fn crosses_search_root(path: &Path, traversal_root: &Path, search_roots: &[PathBuf]) -> bool {
    search_roots.iter().any(|other| {
        other != traversal_root && other.starts_with(traversal_root) && path.starts_with(other)
    })
}
