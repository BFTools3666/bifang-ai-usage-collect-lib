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

/// 宿主可实现的协作取消检查；不绑定令牌、运行时或线程所有权。
pub trait CancellationSignal: Send + Sync {
    /// 返回当前读取是否应在下一个安全边界结束。
    fn is_cancelled(&self) -> bool;
}

impl CancellationSignal for ScanCancellation {
    /// 复用普通扫描令牌的原子取消状态。
    fn is_cancelled(&self) -> bool {
        ScanCancellation::is_cancelled(self)
    }
}

impl<F: Fn() -> bool + Send + Sync> CancellationSignal for F {
    /// 允许调用方以闭包借用自身取消标记，无需复制库内令牌。
    fn is_cancelled(&self) -> bool {
        self()
    }
}
