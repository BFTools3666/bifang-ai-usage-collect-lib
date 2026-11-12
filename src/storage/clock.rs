//! 存储层共用的系统时钟读取。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// 返回自 Unix 纪元起的时长；系统时钟早于纪元时返回 `None`，由调用方决定回退。
pub(super) fn unix_now() -> Option<Duration> {
    SystemTime::now().duration_since(UNIX_EPOCH).ok()
}
