//! 把统计存储机制错误脱敏映射为共享的稳定本机索引错误。

use std::io::ErrorKind;

use crate::{LocalError, LocalErrorKind};
use sea_orm::DbErr;

/// 把文件系统错误归一化为不泄露主机路径的稳定类别。
pub(super) fn map_io_error(error: std::io::Error) -> LocalError {
    match error.kind() {
        ErrorKind::PermissionDenied => {
            LocalError::new(LocalErrorKind::PermissionDenied, "local access was denied")
        }
        ErrorKind::NotFound => LocalError::new(
            LocalErrorKind::SourceUnavailable,
            "local source is unavailable",
        ),
        _ => LocalError::new(
            LocalErrorKind::SourceUnavailable,
            "local source operation failed",
        ),
    }
}

/// 把 SeaORM/SQLite 细节收敛为固定文案，不保存 SQL 或 app-data 路径。
pub(super) fn map_database_error(_error: DbErr) -> LocalError {
    LocalError::new(LocalErrorKind::Database, "local index operation failed")
}

/// 为 storage 内部结果提供显式、统一的脱敏转换入口。
pub(super) trait IntoLocalResult<T> {
    /// 丢弃底层错误细节并返回 core 稳定错误。
    fn into_local_result(self) -> Result<T, LocalError>;
}

impl<T> IntoLocalResult<T> for Result<T, DbErr> {
    /// 将数据库结果转换为不包含 SQL 或路径的稳定错误。
    fn into_local_result(self) -> Result<T, LocalError> {
        self.map_err(map_database_error)
    }
}

impl<T> IntoLocalResult<T> for Result<T, std::io::Error> {
    /// 将文件系统结果转换为不包含宿主细节的稳定错误。
    fn into_local_result(self) -> Result<T, LocalError> {
        self.map_err(map_io_error)
    }
}

impl<T> IntoLocalResult<T> for Result<T, LocalError> {
    /// 保留已经完成稳定化的本机索引错误。
    fn into_local_result(self) -> Result<T, LocalError> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 底层数据库错误文本不得穿过稳定错误边界。
    #[test]
    fn database_mapping_discards_sensitive_details() {
        let error = map_database_error(DbErr::Custom(
            "private SQL and /Users/example path".to_owned(),
        ));
        assert_eq!(error.kind(), LocalErrorKind::Database);
        assert_eq!(error.to_string(), "local index operation failed");
        assert!(!error.to_string().contains("/Users/example"));
    }

    /// 文件系统错误只保留稳定类别与固定文案。
    #[test]
    fn io_mapping_discards_sensitive_details() {
        let error = map_io_error(std::io::Error::new(
            ErrorKind::PermissionDenied,
            "/Users/example/private.jsonl",
        ));
        assert_eq!(error.kind(), LocalErrorKind::PermissionDenied);
        assert_eq!(error.to_string(), "local access was denied");
    }
}
