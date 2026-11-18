//! WorkBuddy 内容读取前共用的本地卷与链接边界。

use crate::readers::{
    LocalPathStatus, classify_local_path, is_obviously_network_path,
    walk_ancestors_for_link_component,
};
use std::io::{self, ErrorKind};
use std::path::Path;

/// 生产入口使用平台卷分类，测试可注入确切的挂载边界。
#[derive(Clone, Copy)]
pub(super) struct WorkbuddySourceBoundary(pub(super) fn(&Path) -> LocalPathStatus);

/// 使用当前平台元数据判断可读的 WorkBuddy 来源。
pub(super) const HOST_SOURCE_BOUNDARY: WorkbuddySourceBoundary =
    WorkbuddySourceBoundary(classify_local_path);

impl WorkbuddySourceBoundary {
    /// 在打开文件或枚举目录前核验精确路径，允许处于已确认本地的子挂载。
    pub(super) fn check(self, path: &Path) -> io::Result<()> {
        if is_obviously_network_path(path) || walk_ancestors_for_link_component(path)? {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "linked or remote WorkBuddy path is not eligible",
            ));
        }
        match (self.0)(path) {
            LocalPathStatus::ConfirmedLocal => Ok(()),
            LocalPathStatus::Missing => Err(io::Error::new(
                ErrorKind::NotFound,
                "WorkBuddy source disappeared",
            )),
            LocalPathStatus::RejectedNetwork | LocalPathStatus::Indeterminate => {
                Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "WorkBuddy path volume is not confirmed local",
                ))
            }
        }
    }
}
