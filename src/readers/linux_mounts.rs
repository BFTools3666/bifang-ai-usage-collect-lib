//! 解析 Linux 内核挂载表，包括依赖卷列表省略的目录和网络/虚拟挂载边界。

use super::{VolumeClass, classify_linux_file_system};
use std::io::{Error, ErrorKind, Result};

/// 解析当前命名空间 mountinfo，按每一条挂载点的文件系统类型分类。
pub(super) fn parse_mountinfo(input: &[u8]) -> Result<Vec<(Vec<u8>, VolumeClass)>> {
    input
        .split(|&byte| byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let mut fields = line.split(|&byte| byte == b' ');
            let mount = fields.nth(4).ok_or_else(invalid_mountinfo)?;
            fields
                .find(|&field| field == b"-")
                .ok_or_else(invalid_mountinfo)?;
            let file_system = fields
                .next()
                .filter(|item| !item.is_empty())
                .ok_or_else(invalid_mountinfo)?;
            let class =
                classify_linux_file_system(std::str::from_utf8(file_system).unwrap_or_default());
            Ok((decode_mount_path(mount)?, class))
        })
        .collect()
}

/// 内核 mountinfo 用固定八进制转义表示空格、制表符、换行和反斜线。
fn decode_mount_path(input: &[u8]) -> Result<Vec<u8>> {
    if input.first() != Some(&b'/') {
        return Err(invalid_mountinfo());
    }
    let mut path = Vec::with_capacity(input.len());
    let mut cursor = input;
    while let Some((&first, tail)) = cursor.split_first() {
        if first != b'\\' {
            path.push(first);
            cursor = tail;
            continue;
        }
        let escape = tail.get(..3).ok_or_else(invalid_mountinfo)?;
        path.push(match escape {
            b"040" => b' ',
            b"011" => b'\t',
            b"012" => b'\n',
            b"134" => b'\\',
            _ => return Err(invalid_mountinfo()),
        });
        cursor = &tail[3..];
    }
    Ok(path)
}

/// 内核范围无法确认时完整发现失败关闭，不暴露挂载路径。
fn invalid_mountinfo() -> Error {
    Error::new(ErrorKind::InvalidData, "invalid local mount metadata")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 前缀相似的真实盘继续扫描，同时保留 tmpfs 和网络子挂载的独立排除边界。
    #[test]
    fn linux_mountinfo_preserves_each_real_mountpoint() {
        let mounts = parse_mountinfo(
            concat!(
                "31 29 8:2 / /procdata rw - ext4 /dev/sda2 rw\n",
                "32 31 0:15 / /procdata/cache rw - tmpfs tmpfs rw\n",
                "33 29 8:3 / /sys-backup rw - ext4 /dev/sdb1 rw\n",
                "34 29 8:5 / /run1/share\\040local rw shared:1 - btrfs /dev/sdc1 rw\n",
                "35 34 0:17 / /run1/share\\040local/remote rw - nfs server:/remote rw\n",
            )
            .as_bytes(),
        )
        .expect("supported mountinfo");
        assert_eq!(
            mounts,
            [
                (b"/procdata".to_vec(), VolumeClass::Local),
                (b"/procdata/cache".to_vec(), VolumeClass::Unknown),
                (b"/sys-backup".to_vec(), VolumeClass::Local),
                (b"/run1/share local".to_vec(), VolumeClass::Local),
                (b"/run1/share local/remote".to_vec(), VolumeClass::Network),
            ]
        );
    }

    /// 非 UTF-8 挂载点保持路径字节，非法转义不会制造不同路径。
    #[test]
    fn linux_mountinfo_keeps_native_path_bytes() {
        let mounts =
            parse_mountinfo(b"31 29 8:1 / /mnt/\xff\\134backslash rw - ext4 /dev/sda1 rw\n")
                .expect("byte path");
        assert_eq!(mounts[0].0, b"/mnt/\xff\\backslash");
        assert!(parse_mountinfo(b"31 29 8:1 / /mnt/\\001bad rw - ext4 /dev/sda1 rw").is_err());
    }
}
