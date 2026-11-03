//! 系统索引加速器的可取消等待，仅在 blocking 读取岛内短周期轮询热启动 future。

use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::RootDiscoveryCoordinator;

/// 单个系统索引请求超过五秒即放弃加速，完整目录遍历仍负责覆盖证明。
pub(super) const INDEX_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
/// 取消最多等待一段二十五毫秒轮询间隔，不在 WinRT join 内无限阻塞。
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// 未获得系统终态时请求取消；取消的是系统索引请求，不结束其它进程。
struct OperationCancellation<C: FnOnce()> {
    /// 确认完成后取走回调，避免取消已经成功的操作。
    cancel: Option<C>,
}

impl<C: FnOnce()> Drop for OperationCancellation<C> {
    /// 超时、用户取消和等待者提前退出共用同一资源释放路径。
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel();
        }
    }
}

/// 等待已经启动的异步系统请求；完成错误、截止或用户取消均放弃本次加速。
pub(super) fn wait_for_operation<T, E>(
    future: impl Future<Output = Result<T, E>>,
    coordinator: &RootDiscoveryCoordinator,
    timeout: Duration,
    cancel: impl FnOnce(),
) -> Option<T> {
    let deadline = Instant::now() + timeout;
    let mut cancellation = OperationCancellation {
        cancel: Some(cancel),
    };
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if coordinator.is_cancel_requested() || Instant::now() >= deadline {
            return None;
        }
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            if let Ok(value) = result {
                cancellation.cancel.take();
                return Some(value);
            }
            return None;
        }
        std::thread::sleep(POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RootDiscoveryPlatform, RootDiscoveryScope, RootDiscoveryStrategy};
    use std::cell::Cell;
    use std::future::{pending, poll_fn, ready};

    /// 只构造内存发现状态，不读取开发机文件或平台索引。
    fn coordinator() -> RootDiscoveryCoordinator {
        let coordinator = RootDiscoveryCoordinator::default();
        assert!(coordinator.start(
            RootDiscoveryStrategy::WindowsSearch,
            RootDiscoveryPlatform::Windows,
            RootDiscoveryScope::FullLocalVolumes,
            1
        ));
        coordinator
    }

    /// 系统明确完成的成功结果不发送取消，错误则请求释放可能仍在途的系统操作。
    #[test]
    fn completed_operations_preserve_results_without_cancellation() {
        for result in [Ok(7), Err(())] {
            let cancelled = Cell::new(false);
            let actual = wait_for_operation(
                ready(result),
                &coordinator(),
                INDEX_OPERATION_TIMEOUT,
                || cancelled.set(true),
            );
            assert_eq!(actual, result.ok());
            assert_eq!(cancelled.get(), result.is_err());
        }
    }

    /// 永不完成的加速器只等待本次截止，并恰好发送一次系统取消。
    #[test]
    fn pending_operation_times_out_and_requests_cancellation_once() {
        let cancellations = Cell::new(0);
        let started = Instant::now();
        assert_eq!(
            wait_for_operation(
                pending::<Result<(), ()>>(),
                &coordinator(),
                Duration::from_millis(10),
                || cancellations.set(cancellations.get() + 1)
            ),
            None
        );
        assert_eq!(cancellations.get(), 1);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    /// 请求发出后取消也能离开等待；不必等到五秒截止或系统完成。
    #[test]
    fn cancellation_during_pending_operation_stops_before_the_deadline() {
        let coordinator = coordinator();
        let cancellations = Cell::new(0);
        let started = Instant::now();
        let future = poll_fn(|_| {
            assert!(coordinator.request_cancel());
            Poll::Pending::<Result<(), ()>>
        });
        assert_eq!(
            wait_for_operation(future, &coordinator, INDEX_OPERATION_TIMEOUT, || {
                cancellations.set(cancellations.get() + 1)
            }),
            None
        );
        assert_eq!(cancellations.get(), 1);
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
