//! 扫描读取的协作取消信号。
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
/// 表示可取消的扫描取消语义，不包含实现细节。
#[derive(Debug, Clone, Default)]
pub struct ScanCancellation {
    /// 共享取消标志；置位后各扫描阶段协作停止。
    cancelled: Arc<AtomicBool>,
}

impl ScanCancellation {
    /// 构造未取消的信号句柄。
    pub fn new() -> Self {
        Self::default()
    }

    /// 请求扫描在下一个安全边界退出。
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// 返回当前是否已请求取消。
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}
