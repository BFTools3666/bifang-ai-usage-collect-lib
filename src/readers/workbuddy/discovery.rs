//! WorkBuddy 分版本发现；快速核验内置根，完整发现复用本地卷遍历。

use crate::readers::{
    LocalPathStatus, LocalVolumeRoots, classify_local_path, enumerate_local_volume_roots,
    metadata_discovery::{DirectoryProbe, traverse_metadata_roots},
    validate_local_plain_directory,
};
use crate::{
    CancellationSignal, LocalError, LocalErrorKind, RootDiscoveryCoordinator,
    RootDiscoveryLifecycle, RootDiscoveryScope, WORKBUDDY_PROJECTS_DIR_NAME, WorkbuddyEdition,
    WorkbuddyScanSource, append_platform_discovery_excludes_for_volumes, safe_short_value,
    workbuddy_scan_source_from_path,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

/// 在已确认的本地范围发现指定版本。快速检查指定用户目录下的内置布局，全盘遍历本地持久卷。
/// # Errors
/// 无法安全获取快速根或卷范围、取消或传入手动子树选项时返回脱敏错误。
pub fn discover_edition_sources(
    edition: WorkbuddyEdition,
    scope: RootDiscoveryScope,
    user_home: Option<&Path>,
    cancellation: Arc<dyn CancellationSignal>,
) -> Result<Vec<WorkbuddyScanSource>, LocalError> {
    if cancellation.is_cancelled() {
        return Err(discovery_failed());
    }
    let default_path = user_home.map(|path| path.join(edition.directory_name()));
    match scope {
        RootDiscoveryScope::UserPriority => {
            let path = default_path.ok_or_else(discovery_failed)?;
            let source = resolve_edition_source(&path, edition, true)?;
            Ok(source.into_iter().collect())
        }
        RootDiscoveryScope::FullLocalVolumes => {
            let mut volumes = enumerate_local_volume_roots();
            volumes.excluded_roots = append_platform_discovery_excludes_for_volumes(
                volumes.excluded_roots,
                &volumes.search_roots,
            );
            let default = default_path
                .and_then(|path| resolve_edition_source(&path, edition, true).ok().flatten());
            discover_edition_sources_on_volumes(edition, volumes, default.as_ref(), cancellation)
        }
        RootDiscoveryScope::ManualSubtree => Err(LocalError::new(
            LocalErrorKind::InvalidPath,
            "workbuddy discovery only supports quick and full local scopes",
        )),
    }
}

/// 公共遍历按本轮卷与排除边界逐一发现符合布局的根，未知卷和失败不会回退到任意主目录。
pub(crate) fn discover_edition_sources_on_volumes(
    edition: WorkbuddyEdition,
    volumes: LocalVolumeRoots,
    built_in: Option<&WorkbuddyScanSource>,
    cancellation: Arc<dyn CancellationSignal>,
) -> Result<Vec<WorkbuddyScanSource>, LocalError> {
    if volumes.search_roots.is_empty() || cancellation.is_cancelled() {
        return Err(discovery_failed());
    }
    let coordinator = RootDiscoveryCoordinator::default();
    let mut sources = BTreeMap::new();
    let priority = built_in
        .map(|source| source.path.clone())
        .into_iter()
        .collect();
    traverse_metadata_roots(
        &coordinator,
        volumes,
        priority,
        DirectoryProbe::Workbuddy(edition),
        Some(cancellation.clone()),
        |hit| {
            if let Ok(Some(mut source)) = resolve_edition_source(&hit.path, edition, false) {
                if let Some(default) = built_in.filter(|default| default.root_id == source.root_id)
                {
                    source.alias.clone_from(&default.alias);
                }
                sources.entry(source.root_id.clone()).or_insert(source);
            }
        },
    );
    if cancellation.is_cancelled()
        || matches!(
            coordinator.snapshot().lifecycle,
            RootDiscoveryLifecycle::Cancelled | RootDiscoveryLifecycle::Failed
        )
    {
        return Err(discovery_failed());
    }
    Ok(sources.into_values().collect())
}

/// 核验对应版本及其 projects 位于可读本地卷；普通缺失与挂载/权限失败分开处理。
pub(crate) fn resolve_edition_source(
    path: &Path,
    edition: WorkbuddyEdition,
    built_in: bool,
) -> Result<Option<WorkbuddyScanSource>, LocalError> {
    if !path.is_absolute() || !edition.matches_directory(path) {
        return Err(discovery_failed());
    }
    let projects = path.join(WORKBUDDY_PROJECTS_DIR_NAME);
    for candidate in [path, projects.as_path()] {
        if matches!(
            classify_local_path(candidate),
            LocalPathStatus::RejectedNetwork
        ) {
            return Err(discovery_failed());
        }
        match validate_local_plain_directory(candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(discovery_failed()),
        }
    }
    let canonical = std::fs::canonicalize(path).map_err(|_| discovery_failed())?;
    Ok(Some(edition_source(canonical, built_in)))
}

/// 只用标准末段和根 ID 短尾区分非默认数据根，保留原生路径供内部访问。
pub(crate) fn edition_source(path: PathBuf, built_in: bool) -> WorkbuddyScanSource {
    let mut source = workbuddy_scan_source_from_path(path);
    if !built_in {
        source.alias = format!("{} {}", source.alias, safe_short_value(&source.root_id));
    }
    source
}

/// 固定安全错误类别，目录路径只留在调用者内存中。
fn discovery_failed() -> LocalError {
    LocalError::new(
        LocalErrorKind::SourceUnavailable,
        "workbuddy root discovery failed",
    )
}
