//! 为统计 SQLite 提供统一的路径、连接和 Unix 权限边界。

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sea_orm::{ConnectOptions, Database, DatabaseConnection};

/// 普通 SQLite 忙等待与 journal 初始化竞争共用同一有限预算。
pub(super) const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_millis(5_000);

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
                    create_private_subdirectory(&database_path).await?;
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
                .busy_timeout(SQLITE_BUSY_TIMEOUT)
                .pragma("trusted_schema", "OFF")
        });
    }
    options.max_connections(1);
    let connection = Database::connect(options)
        .await
        .map_err(|_| PrivateSqliteError::Database)?;
    Ok((connection, database_path))
}

/// 并发创建同一父目录时只接纳复读确认的普通目录，链接、文件及其他错误仍拒绝。
async fn create_private_subdirectory(path: &Path) -> Result<(), PrivateSqliteError> {
    match tokio::fs::create_dir(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            let metadata = tokio::fs::symlink_metadata(path)
                .await
                .map_err(map_io_error)?;
            if crate::readers::discovery::metadata_is_link_like(&metadata) || !metadata.is_dir() {
                return Err(PrivateSqliteError::InvalidPath);
            }
            Ok(())
        }
        Err(error) => Err(map_io_error(error)),
    }
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

#[cfg(test)]
mod tests {
    use super::{PrivateSqliteError, create_private_subdirectory};

    /// 目录已经被另一创建者建立时允许继续，普通文件同名碰撞仍拒绝。
    #[tokio::test]
    async fn concurrent_directory_creation_accepts_only_an_existing_directory() {
        let temp = tempfile::tempdir().expect("isolated path fixture");
        let directory = temp.path().join("nested");
        std::fs::create_dir(&directory).expect("other creator establishes directory");
        create_private_subdirectory(&directory)
            .await
            .expect("existing directory is revalidated");
        let file = temp.path().join("file");
        std::fs::write(&file, b"original").expect("file collision fixture");
        assert_eq!(
            create_private_subdirectory(&file).await,
            Err(PrivateSqliteError::InvalidPath)
        );
        assert_eq!(
            std::fs::read(file).expect("collision remains unchanged"),
            b"original"
        );
    }

    /// 并发出现的链接不能被 AlreadyExists 分支当作普通目录接纳。
    #[cfg(unix)]
    #[tokio::test]
    async fn concurrent_directory_creation_rejects_a_link_collision() {
        let temp = tempfile::tempdir().expect("isolated path fixture");
        let target = temp.path().join("outside");
        std::fs::create_dir(&target).expect("link target fixture");
        let link = temp.path().join("nested");
        std::os::unix::fs::symlink(&target, &link).expect("link collision fixture");
        assert_eq!(
            create_private_subdirectory(&link).await,
            Err(PrivateSqliteError::InvalidPath)
        );
        assert!(
            std::fs::read_dir(target)
                .expect("target remains readable")
                .next()
                .is_none()
        );
    }
}
