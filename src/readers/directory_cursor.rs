//! 保留同一目录枚举句柄的可续读游标，调用方逐片消费而不收集整棵目录树。

use std::fs::{self, DirEntry, ReadDir};
use std::path::{Path, PathBuf};

/// 一次目录枚举的拥有者；移动游标保留原始 ReadDir，丢弃时立即关闭目录句柄。
pub struct DirectoryCursor {
    path: PathBuf,
    entries: Option<ReadDir>,
    started: bool,
    finished: bool,
    depth: usize,
}

impl std::fmt::Debug for DirectoryCursor {
    /// 调试只显示生命周期，不输出原始目录路径。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectoryCursor")
            .field("started", &self.started)
            .field("finished", &self.finished)
            .finish()
    }
}

impl DirectoryCursor {
    /// 建立尚未打开目录的游标，不执行文件系统访问。
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            entries: None,
            started: false,
            finished: false,
            depth: 0,
        }
    }

    /// 返回仅供原生枚举策略使用的目录位置，不应进入日志或界面。
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 区分首次打开与续读，避免把分片重复计作新目录。
    pub fn started(&self) -> bool {
        self.started
    }

    /// 返回调度深度，祖先续读必须等待已派生的深层目录片段。
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// 为当前目录的一条子目录建立未打开游标，不增加活动目录句柄。
    pub fn child(&self, path: PathBuf) -> Self {
        let mut child = Self::new(path);
        child.depth = self.depth.saturating_add(1);
        child
    }

    /// 从同一个目录句柄取得下一项，EOF 后不会重新枚举；错误由调用方脱敏映射。
    /// # Errors
    /// 目录不可读或当前目录项访问失败时返回原生 I/O 错误。
    pub fn next_entry(&mut self) -> std::io::Result<Option<DirEntry>> {
        if self.finished {
            return Ok(None);
        }
        if !self.started {
            self.started = true;
            match fs::read_dir(&self.path) {
                Ok(entries) => self.entries = Some(entries),
                Err(error) => {
                    self.finished = true;
                    return Err(error);
                }
            }
        }
        match self.entries.as_mut().and_then(Iterator::next) {
            Some(entry) => entry.map(Some),
            None => {
                self.entries = None;
                self.finished = true;
                Ok(None)
            }
        }
    }
}

impl From<PathBuf> for DirectoryCursor {
    /// 从拥有的原生路径建立惰性目录游标。
    fn from(path: PathBuf) -> Self {
        Self::new(path)
    }
}

impl From<&Path> for DirectoryCursor {
    /// 从借用的原生路径建立惰性目录游标。
    fn from(path: &Path) -> Self {
        Self::new(path.to_path_buf())
    }
}
