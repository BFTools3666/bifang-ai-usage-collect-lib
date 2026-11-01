//! 四个本机 JSONL 解析器共用的有界切行与白名单反序列化。
//!
//! 预算内的行从内存缓冲反序列化；超过单行内存预算的完整行改为从已缓冲前缀接上
//! 剩余字节流式反序列化同一白名单类型。未声明的正文、工具输出和图片字段由 serde
//! 跳过而不缓冲，因此超长行里的用量不会再被整行丢弃。文件尾部尚未换行的行
//! （含超长行）不提交，等下次读取。

use std::io::{self, BufRead, BufReader, Read};

use serde::de::DeserializeOwned;

/// 一条以换行结束的完整行的反序列化结果。
pub enum JsonlRecord<T> {
    /// 白名单字段反序列化成功。
    Parsed(T),
    /// 空行（CRLF 去掉 `\r` 之后）。
    Empty,
    /// 预算内的行不是合法 JSON 记录。
    Malformed,
    /// 超过单行预算且不是合法 JSON 记录。
    OversizedMalformed,
}

/// 行回调决定是否继续读取。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineFlow {
    /// 继续读取下一行。
    Continue,
    /// 调用方预算已满，停止读取。
    Stop,
}

/// 切行结束时的字节进度，供增量 checkpoint 使用。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JsonlLineProgress {
    /// 本次实际消耗的原始字节数，含尚未结束的半行。
    pub bytes_consumed: u64,
    /// 截至最后一个完整换行的字节数。
    pub committed_bytes: u64,
    /// 末尾未闭合行的原始字节数。
    pub trailing_bytes: u64,
    /// 末尾未闭合行是否已超过单行预算。
    pub discarding_oversized_line: bool,
    /// 是否因取消提前停止。
    pub cancelled: bool,
}

/// 按行读取 JSONL 并把每条完整行反序列化为白名单类型 `T`。
///
/// 每读一块前、每处理完一行后以及超长行流式读取的每个缓冲块前检查取消。
pub fn read_jsonl_records<T, R, E>(
    reader: R,
    max_line_bytes: usize,
    is_cancelled: impl Fn() -> bool,
    map_io_error: impl Fn(io::Error) -> E,
    mut on_record: impl FnMut(JsonlRecord<T>) -> Result<LineFlow, E>,
) -> Result<JsonlLineProgress, E>
where
    T: DeserializeOwned,
    R: Read,
{
    let mut reader = BufReader::with_capacity(16 * 1024, reader);
    let mut line = Vec::with_capacity(max_line_bytes.min(16 * 1024));
    let mut progress = JsonlLineProgress::default();

    loop {
        if is_cancelled() {
            progress.cancelled = true;
            break;
        }
        let available = reader.fill_buf().map_err(&map_io_error)?;
        if available.is_empty() {
            break;
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let record = match newline {
            Some(end) if line.len().saturating_add(end) <= max_line_bytes => {
                line.extend_from_slice(&available[..end]);
                reader.consume(end + 1);
                progress.bytes_consumed = progress.bytes_consumed.saturating_add(end as u64 + 1);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                parse_buffered_line(&line)
            }
            None if line.len().saturating_add(available.len()) <= max_line_bytes => {
                let consumed = available.len();
                line.extend_from_slice(available);
                reader.consume(consumed);
                progress.bytes_consumed = progress.bytes_consumed.saturating_add(consumed as u64);
                continue;
            }
            _ => {
                let oversized = parse_oversized_line(&line, &mut reader, &is_cancelled);
                progress.bytes_consumed = progress
                    .bytes_consumed
                    .saturating_add(oversized.remainder_bytes);
                if is_cancelled() {
                    progress.cancelled = true;
                    progress.trailing_bytes =
                        (line.len() as u64).saturating_add(oversized.remainder_bytes);
                    progress.discarding_oversized_line = true;
                    return Ok(progress);
                }
                let record = oversized.record.map_err(&map_io_error)?;
                if !oversized.terminated {
                    progress.trailing_bytes =
                        (line.len() as u64).saturating_add(oversized.remainder_bytes);
                    progress.discarding_oversized_line = true;
                    return Ok(progress);
                }
                record
            }
        };
        line.clear();
        progress.committed_bytes = progress.bytes_consumed;
        if on_record(record)? == LineFlow::Stop {
            return Ok(progress);
        }
        if is_cancelled() {
            progress.cancelled = true;
            return Ok(progress);
        }
    }

    progress.trailing_bytes = line.len() as u64;
    Ok(progress)
}

/// 反序列化一条已完整缓冲的行。
fn parse_buffered_line<T: DeserializeOwned>(line: &[u8]) -> JsonlRecord<T> {
    if line.is_empty() {
        return JsonlRecord::Empty;
    }
    match serde_json::from_slice(line) {
        Ok(record) => JsonlRecord::Parsed(record),
        Err(_) => JsonlRecord::Malformed,
    }
}

/// 超长行流式反序列化的结果与字节进度。
struct OversizedLine<T> {
    /// 超长行的解析结果；无法解析时为 I/O 错误。
    record: io::Result<JsonlRecord<T>>,
    /// 超过读取预算、仍需丢弃的剩余字节数。
    remainder_bytes: u64,
    /// 该行是否已遇到换行符而结束。
    terminated: bool,
}

/// 从已缓冲前缀与剩余字节流式反序列化一整行，并把本行剩余字节读到换行为止。
fn parse_oversized_line<T, R>(
    prefix: &[u8],
    reader: &mut R,
    is_cancelled: &impl Fn() -> bool,
) -> OversizedLine<T>
where
    T: DeserializeOwned,
    R: BufRead,
{
    let mut remainder = LineRemainder {
        inner: reader,
        bytes: 0,
        terminated: false,
    };
    let parsed = serde_json::from_reader::<_, T>(BufReader::new(CancellableRead {
        inner: prefix.chain(&mut remainder),
        is_cancelled,
    }));
    let mut record = match parsed {
        Ok(record) => Ok(JsonlRecord::Parsed(record)),
        Err(error) if error.is_io() => Err(error.into()),
        Err(_) => Ok(JsonlRecord::OversizedMalformed),
    };
    if record.is_ok() {
        let drained = io::copy(
            &mut CancellableRead {
                inner: &mut remainder,
                is_cancelled,
            },
            &mut io::sink(),
        );
        if let Err(error) = drained {
            record = Err(error);
        }
    }
    OversizedLine {
        record,
        remainder_bytes: remainder.bytes,
        terminated: remainder.terminated,
    }
}

/// 只把底层缓冲读到当前行的换行符为止，记录消耗字节与本行是否已结束。
struct LineRemainder<'a, R> {
    /// 被读取的底层缓冲读取器。
    inner: &'a mut R,
    /// 已消耗的字节数。
    bytes: u64,
    /// 是否已读到本行换行符。
    terminated: bool,
}

impl<R: BufRead> Read for LineRemainder<'_, R> {
    /// 遇到换行即视为本行结束并消费该换行。
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.terminated || buffer.is_empty() {
            return Ok(0);
        }
        let available = self.inner.fill_buf()?;
        let limit = available.len().min(buffer.len());
        let (copied, consumed) = match available[..limit].iter().position(|byte| *byte == b'\n') {
            Some(end) => {
                self.terminated = true;
                (end, end + 1)
            }
            None => (limit, limit),
        };
        buffer[..copied].copy_from_slice(&available[..copied]);
        self.inner.consume(consumed);
        self.bytes = self.bytes.saturating_add(consumed as u64);
        Ok(copied)
    }
}

/// 构造取消读取错误；不得使用 `Interrupted`，std 与 serde 的逐字节读取会无限重试该类别。
pub fn cancelled_io_error() -> io::Error {
    io::Error::other("local source read cancelled")
}

/// 在底层读取每个缓冲块前观察取消，避免单个大文件或超长行延迟退出。
pub struct CancellableRead<'a, R, F> {
    /// 被包装的字节来源。
    pub inner: R,
    /// 返回调用方是否已要求停止读取。
    pub is_cancelled: &'a F,
}

impl<R, F> Read for CancellableRead<'_, R, F>
where
    R: Read,
    F: Fn() -> bool,
{
    /// 取消后返回非重试类错误，使 serde 立即停止继续解析本机字节。
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if (self.is_cancelled)() {
            return Err(cancelled_io_error());
        }
        self.inner.read(buffer)
    }
}

#[cfg(test)]
#[path = "jsonl_lines_tests.rs"]
mod tests;
