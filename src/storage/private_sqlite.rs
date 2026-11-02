//! 为统计 SQLite 提供统一的路径、连接和 Unix 权限边界。

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sea_orm::{ConnectOptions, Database, DatabaseConnection};

/// 标识私有 SQLite 打开失败的稳定类别，不携带路径或 SQL。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrivateSqliteError {
    /// 基准目录 或数据库文件不符合本地普通文件边界。
    InvalidPath,
    /// 当前用户无权创建、读取或收紧数据库权限。
    PermissionDenied,
    /// 文件系统操作失败。
    StorageUnavailable,
    /// SQLite 连接无法建立。
    Database,
}

/// 在显式 基准目录 目录中安全打开指定私有 SQLite，并返回真实文件路径。
pub(crate) async fn open_private_sqlite(
    app_data_dir: &Path,
    relative_path: &Path,
) -> Result<(DatabaseConnection, PathBuf), PrivateSqliteError> {
    if app_data_dir.as_os_str().is_empty()
        || !app_data_dir.is_absolute()
        || relative_path.as_os_str().is_empty()
        || relative_path
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
        || relative_path
            .to_str()
            .is_some_and(|value| (!cfg!(windows) && value.contains('\\')) || value.contains(':'))
    {
        return Err(PrivateSqliteError::InvalidPath);
    }
    if let Ok(metadata) = tokio::fs::symlink_metadata(app_data_dir).await
        && crate::readers::discovery::metadata_is_link_like(&metadata)
    {
        return Err(PrivateSqliteError::InvalidPath);
    }
    tokio::fs::create_dir_all(app_data_dir)
        .await
        .map_err(map_io_error)?;
    let mut database_path = app_data_dir.to_path_buf();
    let parts: Vec<_> = relative_path.components().collect();
    for (index, part) in parts.iter().enumerate() {
        database_path.push(part.as_os_str());
        match tokio::fs::symlink_metadata(&database_path).await {
            Ok(metadata)
                if crate::readers::discovery::metadata_is_link_like(&metadata)
                    || (index + 1 < parts.len() && !metadata.is_dir()) =>
            {
                return Err(PrivateSqliteError::InvalidPath);
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                if index + 1 < parts.len() {
                    tokio::fs::create_dir(&database_path)
                        .await
                        .map_err(map_io_error)?;
                }
            }
            Err(error) => return Err(map_io_error(error)),
        }
    }
    if let Ok(metadata) = tokio::fs::symlink_metadata(&database_path).await
        && (crate::readers::discovery::metadata_is_link_like(&metadata) || !metadata.is_file())
    {
        return Err(PrivateSqliteError::InvalidPath);
    }

    // SQLite 自己打开的旁文件同样不得通过链接越过调用方的保存范围。
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = database_path.as_os_str().to_os_string();
        sidecar.push(suffix);
        match tokio::fs::symlink_metadata(Path::new(&sidecar)).await {
            Ok(metadata)
                if crate::readers::discovery::metadata_is_link_like(&metadata)
                    || !metadata.is_file() =>
            {
                return Err(PrivateSqliteError::InvalidPath);
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(map_io_error(error)),
        }
    }

    let mut options = ConnectOptions::new("sqlite://placeholder.sqlite3");
    options.sqlx_logging(false);
    {
        let database_path = database_path.clone();
        options.map_sqlx_sqlite_opts(move |sqlite_options| {
            sqlite_options
                .filename(&database_path)
                .create_if_missing(true)
                .foreign_keys(true)
                .busy_timeout(Duration::from_millis(5_000))
                .pragma("trusted_schema", "OFF")
        });
    }
    options.max_connections(1);
    let connection = Database::connect(options)
        .await
        .map_err(|_| PrivateSqliteError::Database)?;
    Ok((connection, database_path))
}

/// 在 Unix 上把数据库限制为当前用户读写；其他平台沿用 基准目录 ACL。
pub(super) async fn set_private_database_permissions(
    _database_path: &Path,
) -> Result<(), PrivateSqliteError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = tokio::fs::metadata(_database_path)
            .await
            .map_err(map_io_error)?
            .permissions();
        permissions.set_mode(0o600);
        tokio::fs::set_permissions(_database_path, permissions)
            .await
            .map_err(map_io_error)?;
    }
    Ok(())
}

/// 把文件系统错误收敛为稳定类别。
fn map_io_error(error: std::io::Error) -> PrivateSqliteError {
    if error.kind() == ErrorKind::PermissionDenied {
        PrivateSqliteError::PermissionDenied
    } else {
        PrivateSqliteError::StorageUnavailable
    }
}
