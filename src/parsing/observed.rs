//! 在同一只读句柄上限定观测前缀，文件追加部分留给消费方的下一次读取。

use super::{ParseRequest, ParsedUsage, UsageEmitter, UsageParser};
use crate::readers::file_source::map_source_io_error;
use crate::{LocalError, LocalErrorKind};
use std::io::{Read, Seek, SeekFrom};

/// 消费方已观测的文件区间，结束位置不包含在本轮输入内。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadRange {
    start: u64,
    end: u64,
}

impl ReadRange {
    /// 校验完整行续读起点不超过同一次观测的文件上界。
    ///
    /// # Errors
    /// 起点超过观测末端时拒绝读取，消费方应重建来源观察。
    pub fn new(start: u64, end: u64) -> Result<Self, LocalError> {
        if start > end {
            return Err(LocalError::new(
                LocalErrorKind::InvalidPath,
                "read range exceeds observed source",
            ));
        }
        Ok(Self { start, end })
    }
}

/// 从同一来源句柄读取观测前缀，并通过开放的解析器协议交付记录。
///
/// 该函数不枚举文件、不建立索引、不分配运行时，也不决定增量 generation 或保留窗口。
/// 消费方只能在成功结果中取得解析状态；半行与取消状态继续取原生报告。
///
/// # Errors
/// seek、读取、解析器或消费方回调失败时返回错误；不返回可提交的上下文。
pub fn read_observed_prefix<P: UsageParser + ?Sized>(
    parser: &P,
    mut reader: impl Read + Seek,
    range: ReadRange,
    request: ParseRequest<'_, P::Context>,
    emit: &mut UsageEmitter<'_>,
) -> Result<ParsedUsage<P::Context, P::Report>, LocalError> {
    reader
        .seek(SeekFrom::Start(range.start))
        .map_err(map_source_io_error)?;
    parser.parse(&mut reader.take(range.end - range.start), request, emit)
}
