//! 使用系统元数据索引提前返回 JSONL 文件名结果，不承担完整覆盖证明。

use std::path::PathBuf;

use crate::{RootDiscoveryAgentScope, RootDiscoveryCoordinator, RootDiscoveryScope};

use super::super::LocalVolumeRoots;

#[cfg(any(target_os = "windows", test))]
mod wait;

/// 仅在平台索引能严格服从当前范围时使用加速查询。
pub(super) fn should_query(scope: RootDiscoveryScope) -> bool {
    #[cfg(target_os = "windows")]
    {
        let _ = scope;
        true
    }
    #[cfg(target_os = "macos")]
    {
        // objc2 当前的 NSMetadataQuery 搜索范围 setter 是 unsafe API；在 workspace
        // 禁止 unsafe 的前提下，快速扫描不能先枚举全机 Spotlight 结果再过滤。
        scope == RootDiscoveryScope::FullLocalVolumes
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        let _ = scope;
        false
    }
}

/// 记录一个搜索根的系统索引查询；无论从哪个出口离开，析构时都会发出带耗时的诊断事件。
#[cfg(target_os = "windows")]
struct RootQueryTrace<'a> {
    /// 接收诊断事件的发现协调器。
    coordinator: &'a RootDiscoveryCoordinator,
    /// 搜索根目录名末段，不含完整路径。
    root: String,
    /// 搜索根序号。
    ordinal: u64,
    /// 开始查询的时刻。
    started: std::time::Instant,
    /// 已取回的分页数。
    pages: u64,
    /// 系统索引返回的文件数。
    paths: u64,
}

#[cfg(target_os = "windows")]
impl Drop for RootQueryTrace<'_> {
    /// 统一出口：超时、取消、错误和正常结束都留下同样的耗时记录。
    fn drop(&mut self) {
        self.coordinator
            .emit_diagnostic(crate::RootDiscoveryDiagnostic::SystemIndexRootFinished {
                root: std::mem::take(&mut self.root),
                ordinal: self.ordinal,
                pages: self.pages,
                paths: self.paths,
                elapsed_ms: crate::duration_millis(self.started.elapsed()),
            });
    }
}

#[cfg(target_os = "windows")]
impl RootQueryTrace<'_> {
    /// 熔断：任一系统索引请求超时即放弃整个系统索引阶段，由调用方随后 `break` 出根循环。
    ///
    /// 索引器对该请求是“挂住”而不是快速报错，继续逐根重试只会让每个根都白等一个超时；
    /// 系统索引只是加速，目录遍历随后仍会完整覆盖。
    fn abandon(&self) {
        self.coordinator
            .emit_diagnostic(crate::RootDiscoveryDiagnostic::SystemIndexAbandoned {
                root: self.root.clone(),
                ordinal: self.ordinal,
                waited_ms: crate::duration_millis(self.started.elapsed()),
            });
    }
}

/// Windows 通过进程内 WinRT Search API 分页查询索引中的 JSONL 路径。
#[cfg(target_os = "windows")]
pub(super) fn query(
    volumes: &LocalVolumeRoots,
    coordinator: &RootDiscoveryCoordinator,
    agents: RootDiscoveryAgentScope,
    mut on_path: impl FnMut(PathBuf),
) -> bool {
    use std::future::IntoFuture;
    use wait::{INDEX_OPERATION_TIMEOUT, WaitOutcome, wait_for_operation};
    use windows::Storage::Search::{FolderDepth, IndexerOption, QueryOptions};
    use windows::Storage::StorageFolder;
    use windows::core::HSTRING;

    let options = match QueryOptions::new() {
        Ok(options) => options,
        Err(_) => return false,
    };
    if options.SetFolderDepth(FolderDepth::Deep).is_err()
        || options
            .SetIndexerOption(IndexerOption::OnlyUseIndexer)
            .is_err()
        || options
            .FileTypeFilter()
            .and_then(|filter| filter.Append(&HSTRING::from(".jsonl")))
            .is_err()
    {
        return false;
    }
    let mut any_query_succeeded = false;
    'roots: for (ordinal, root) in volumes.search_roots.iter().enumerate() {
        if coordinator.is_cancel_requested() {
            break;
        }
        let mut trace = RootQueryTrace {
            coordinator,
            root: super::root_label(root),
            ordinal: u64::try_from(ordinal).unwrap_or(u64::MAX),
            started: std::time::Instant::now(),
            pages: 0,
            paths: 0,
        };
        let Ok(folder_operation) =
            StorageFolder::GetFolderFromPathAsync(&HSTRING::from(root.to_string_lossy().as_ref()))
        else {
            continue;
        };
        let folder = match wait_for_operation(
            folder_operation.clone().into_future(),
            coordinator,
            INDEX_OPERATION_TIMEOUT,
            || {
                let _ = folder_operation.Cancel();
            },
        ) {
            WaitOutcome::Ready(folder) => folder,
            WaitOutcome::TimedOut => {
                trace.abandon();
                break 'roots;
            }
            WaitOutcome::Stopped => continue,
        };
        let Ok(query) = folder.CreateFileQueryWithOptions(&options) else {
            continue;
        };
        let mut offset = 0_u32;
        loop {
            if coordinator.is_cancel_requested() {
                break;
            }
            let Ok(operation) = query.GetFilesAsync(offset, 512) else {
                break;
            };
            let files = match wait_for_operation(
                operation.clone().into_future(),
                coordinator,
                INDEX_OPERATION_TIMEOUT,
                || {
                    let _ = operation.Cancel();
                },
            ) {
                WaitOutcome::Ready(files) => files,
                WaitOutcome::TimedOut => {
                    trace.abandon();
                    break 'roots;
                }
                WaitOutcome::Stopped => break,
            };
            any_query_succeeded = true;
            let Ok(size) = files.Size() else {
                break;
            };
            trace.pages = trace.pages.saturating_add(1);
            trace.paths = trace.paths.saturating_add(u64::from(size));
            // 每取回一页就推进一次计数，使系统索引阶段也有可见的进度变化。
            coordinator.add_index_file_names(u64::from(size));
            for index in 0..size {
                if coordinator.is_cancel_requested() {
                    return any_query_succeeded;
                }
                let Ok(file) = files.GetAt(index) else {
                    continue;
                };
                if let Ok(path) = file.Path() {
                    let path = PathBuf::from(path.to_string());
                    if super::scope::candidate_from_file_name(&path, agents).is_some() {
                        on_path(path);
                    }
                }
            }
            if size < 512 {
                break;
            }
            let Some(next_offset) = offset.checked_add(size) else {
                break;
            };
            offset = next_offset;
        }
    }
    any_query_succeeded
}

/// macOS 通过进程内 `NSMetadataQuery` 查询 Spotlight，结果仍逐项执行本地卷策略。
#[cfg(target_os = "macos")]
pub(super) fn query(
    _volumes: &LocalVolumeRoots,
    coordinator: &RootDiscoveryCoordinator,
    agents: RootDiscoveryAgentScope,
    mut on_path: impl FnMut(PathBuf),
) -> bool {
    use std::time::{Duration, Instant};

    use objc2_foundation::{NSDate, NSMetadataQuery, NSPredicate, NSRunLoop, NSString};

    use super::super::{LocalPathStatus, classify_local_path};

    let Some(predicate) = NSPredicate::predicateFromMetadataQueryString(&NSString::from_str(
        "kMDItemFSName == '*.jsonl'cd",
    )) else {
        return false;
    };
    let query = NSMetadataQuery::new();
    query.setPredicate(Some(&predicate));
    if !query.startQuery() {
        return false;
    }

    let run_loop = NSRunLoop::currentRunLoop();
    let deadline = Instant::now() + Duration::from_secs(5);
    while query.isGathering() && Instant::now() < deadline {
        if coordinator.is_cancel_requested() {
            query.stopQuery();
            return true;
        }
        run_loop.runUntilDate(&NSDate::dateWithTimeIntervalSinceNow(0.05));
    }
    query.disableUpdates();
    // Apple 文档中 `NSMetadataItemPathKey` 的字面值是 `kMDItemPath`；用字面量避免
    // 读取 objc2 的 extern static（本 crate 禁止 unsafe-code）。
    let path_attribute = NSString::from_str("kMDItemPath");
    for index in 0..query.resultCount() {
        if coordinator.is_cancel_requested() {
            query.stopQuery();
            return true;
        }
        let Some(path) = query
            .valueOfAttribute_forResultAtIndex(&path_attribute, index)
            .and_then(|value| value.downcast::<NSString>().ok())
            .map(|path| PathBuf::from(path.to_string()))
        else {
            continue;
        };
        if super::scope::candidate_from_file_name(&path, agents).is_some()
            && matches!(classify_local_path(&path), LocalPathStatus::ConfirmedLocal)
        {
            on_path(path);
        }
    }
    query.stopQuery();
    true
}

/// 其他平台没有批准的系统索引加速器；调用方仍执行普通元数据兜底。
#[cfg(not(any(target_os = "windows", target_os = "macos")))]
pub(super) fn query(
    _volumes: &LocalVolumeRoots,
    _coordinator: &RootDiscoveryCoordinator,
    _agents: RootDiscoveryAgentScope,
    _on_path: impl FnMut(PathBuf),
) -> bool {
    false
}
