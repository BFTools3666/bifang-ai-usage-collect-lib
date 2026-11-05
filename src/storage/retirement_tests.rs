//! 旧格式备份的私有权限和失败数据保全回归。

/// 外层打开任务取消后，不可取消的文件操作仍持锁到结束，后续打开者只能等待。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_waiter_keeps_the_open_lock_until_retirement_finishes() {
    let lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
    let guard = lock.clone().lock_owned().await;
    let (entered, started) = tokio::sync::oneshot::channel();
    let (finish, blocked) = std::sync::mpsc::channel();
    let task = tokio::spawn(super::retirement::run_locked_retirement(guard, move || {
        entered.send(()).expect("operation starts");
        blocked.recv().expect("controlled operation finishes");
        Ok(())
    }));
    started.await.expect("blocking operation entered");
    task.abort();
    assert!(
        task.await
            .expect_err("outer task is cancelled")
            .is_cancelled()
    );
    assert!(lock.try_lock().is_err(), "next open must remain blocked");
    finish.send(()).expect("release blocking operation");
    let _guard = tokio::time::timeout(std::time::Duration::from_secs(2), lock.lock())
        .await
        .expect("lock is released after operation completes");
}

/// 完成压缩时文件已经私有，不依赖随后调用者再收紧权限。
#[cfg(unix)]
#[test]
fn backup_is_private_before_retirement_can_delete_originals() {
    use std::os::unix::fs::PermissionsExt;
    let base = tempfile::tempdir().expect("isolated backup fixture");
    let database = base.path().join("codex.db");
    std::fs::write(&database, b"original statistics").expect("original fixture");
    let backup = super::retirement::backup_stale_database(&database, 1).expect("backup completes");
    assert_eq!(
        std::fs::metadata(backup)
            .expect("backup metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::read(database).expect("original remains"),
        b"original statistics"
    );
}

/// 任一文件删除失败时，主库及尚未 checkpoint 的旁文件都按原字节恢复。
#[test]
fn each_removal_failure_restores_the_complete_original_file_set() {
    use std::io::Read;
    for fail_at in 0..3 {
        let base = tempfile::tempdir().expect("isolated rollback fixture");
        let database = base.path().join("codex.db");
        let originals = [
            ("codex.db", b"original database".as_slice()),
            ("codex.db-wal", b"uncheckpointed WAL bytes".as_slice()),
            ("codex.db-shm", b"shared memory bytes".as_slice()),
        ];
        for (name, bytes) in originals {
            std::fs::write(base.path().join(name), bytes).expect("original fixture");
        }
        let mut position = 0;
        let result = super::retirement::retire_with_remover(&database, 1, |path| {
            let current = position;
            position += 1;
            if current == fail_at {
                Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
            } else {
                std::fs::remove_file(path)
            }
        });
        assert_eq!(
            result.expect_err("injected deletion fails").kind(),
            std::io::ErrorKind::PermissionDenied
        );
        let backup = std::fs::read_dir(base.path())
            .expect("directory lists")
            .filter_map(Result::ok)
            .find(|entry| entry.path().extension().is_some_and(|ext| ext == "zip"))
            .expect("completed recovery archive remains")
            .path();
        let mut archive = zip::ZipArchive::new(std::fs::File::open(backup).expect("archive opens"))
            .expect("valid recovery archive");
        for (name, bytes) in originals {
            assert_eq!(
                std::fs::read(base.path().join(name)).expect("original restored"),
                bytes
            );
            let mut archived = Vec::new();
            archive
                .by_name(name)
                .expect("original is archived")
                .read_to_end(&mut archived)
                .expect("archive entry is complete");
            assert_eq!(archived, bytes);
        }
    }
}

/// 压缩失败不能调用删除边界，已产生的部分备份也始终为私有。
#[cfg(unix)]
#[test]
fn backup_failure_keeps_originals_and_a_private_partial_archive() {
    use std::os::unix::fs::PermissionsExt;
    let base = tempfile::tempdir().expect("isolated failed backup");
    let database = base.path().join("codex.db");
    std::fs::write(&database, b"original statistics").expect("original fixture");
    std::fs::create_dir(base.path().join("codex.db-wal")).expect("unreadable sidecar fixture");
    let result = super::retirement::retire_with_remover(&database, 1, |_| {
        panic!("backup failure cannot delete")
    });
    assert!(result.is_err());
    assert_eq!(
        std::fs::read(database).expect("original remains"),
        b"original statistics"
    );
    let backup = std::fs::read_dir(base.path())
        .expect("directory lists")
        .filter_map(Result::ok)
        .find(|entry| entry.path().extension().is_some_and(|ext| ext == "zip"))
        .expect("partial archive");
    assert_eq!(
        backup
            .metadata()
            .expect("archive metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}
