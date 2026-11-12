//! 旧统计库的私有备份、删除与失败恢复；只处理已核对身份且已关闭的库。

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// 以新建语义建立私有文件，Unix 权限在首次写入前即已限制。
fn create_private_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

/// 把主库及存在的 WAL/共享内存旁文件原样打包；备份完成并刷新后才允许删除。
pub(super) fn backup_stale_database(
    database_path: &Path,
    stale_version: i64,
) -> io::Result<PathBuf> {
    let name = database_path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| io::Error::other("database file name is not valid UTF-8"))?;
    let stamp = super::clock::unix_now().map_or(0, |elapsed| elapsed.as_secs());
    let backup = database_path.with_file_name(format!("{name}.v{stale_version}-{stamp}.zip"));
    let mut archive = zip::ZipWriter::new(create_private_file(&backup)?);
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    for suffix in ["", "-wal", "-shm"] {
        let entry = format!("{name}{suffix}");
        let mut input = match File::open(database_path.with_file_name(&entry)) {
            Ok(input) => input,
            Err(error) if !suffix.is_empty() && error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        archive
            .start_file(entry, options)
            .map_err(io::Error::other)?;
        io::copy(&mut input, &mut archive)?;
    }
    archive.finish().map_err(io::Error::other)?.sync_all()?;
    Ok(backup)
}

/// 删除旧库；阻塞压缩与有限重试由调用方等待的 blocking 任务拥有。
pub(super) async fn retire_stale_database(
    database_path: &Path,
    stale_version: i64,
    guard: tokio::sync::OwnedMutexGuard<()>,
) -> io::Result<tokio::sync::OwnedMutexGuard<()>> {
    let database_path = database_path.to_path_buf();
    run_locked_retirement(guard, move || {
        retire_with_remover(&database_path, stale_version, remove_file_with_retry)
    })
    .await
}

/// 不可中断的阻塞文件操作持有打开锁，外层 future 取消也不能放入下一打开者。
pub(super) async fn run_locked_retirement(
    guard: tokio::sync::OwnedMutexGuard<()>,
    operation: impl FnOnce() -> io::Result<()> + Send + 'static,
) -> io::Result<tokio::sync::OwnedMutexGuard<()>> {
    tokio::task::spawn_blocking(move || {
        operation()?;
        Ok(guard)
    })
    .await
    .map_err(|_| io::Error::other("database retirement task failed"))?
}

/// 仅把删除函数作为本边界的故障注入点；任一步失败从已完成的备份恢复已删除文件。
pub(super) fn retire_with_remover(
    database_path: &Path,
    stale_version: i64,
    mut remove: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let backup = backup_stale_database(database_path, stale_version)?;
    let name = database_path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| io::Error::other("database file name is not valid UTF-8"))?;
    let mut removed = Vec::new();
    for suffix in ["", "-wal", "-shm"] {
        let entry = format!("{name}{suffix}");
        let path = database_path.with_file_name(&entry);
        let existed = match path
            .try_exists()
            .and_then(|existed| remove(&path).map(|()| existed))
        {
            Ok(existed) => existed,
            Err(error) => {
                restore_removed_files(&backup, database_path, &removed)?;
                return Err(error);
            }
        };
        if existed {
            removed.push(entry);
        }
    }
    tracing::warn!(
        kind = "statistics_database_rebuilt",
        stale_version,
        current_version = super::schema::SCHEMA_VERSION,
        backup = backup
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or(""),
        "an older statistics database was backed up beside itself and removed; the next scan rebuilds it"
    );
    Ok(())
}

/// 只恢复本次确实删除的固定文件名，不覆盖仍存在的原件或提取任意 ZIP 路径。
fn restore_removed_files(
    backup: &Path,
    database_path: &Path,
    removed: &[String],
) -> io::Result<()> {
    if removed.is_empty() {
        return Ok(());
    }
    let mut archive = zip::ZipArchive::new(File::open(backup)?).map_err(io::Error::other)?;
    let mut failed = false;
    for entry in removed {
        let restored = (|| {
            let mut input = archive.by_name(entry).map_err(io::Error::other)?;
            let mut output = create_private_file(&database_path.with_file_name(entry))?;
            io::copy(&mut input, &mut output)?;
            output.sync_all()
        })();
        failed |= restored.is_err();
    }
    if failed {
        tracing::error!(
            kind = "statistics_database_restore_failed",
            "statistics database restoration failed; the completed backup remains available"
        );
        return Err(io::Error::other("statistics database restoration failed"));
    }
    Ok(())
}

/// 文件已不存在视为成功，其他错误只做固定次数的短退避重试。
fn remove_file_with_retry(path: &Path) -> io::Result<()> {
    const MAX_ATTEMPTS: u32 = 40;
    for attempt in 0..MAX_ATTEMPTS {
        match std::fs::remove_file(path) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) if attempt + 1 == MAX_ATTEMPTS => return Err(error),
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    }
    unreachable!("the bounded removal loop always returns")
}
