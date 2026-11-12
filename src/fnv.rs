//! 持久化 ID 共用的 FNV-1a 64 位哈希；输出被写入索引，不得改动算法或常量。

const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const PRIME: u64 = 0x100000001b3;

/// 对字节序列计算非加密的 FNV-1a 64 位哈希。
pub(crate) fn fnv1a64(bytes: impl IntoIterator<Item = u8>) -> u64 {
    bytes.into_iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(PRIME)
    })
}
