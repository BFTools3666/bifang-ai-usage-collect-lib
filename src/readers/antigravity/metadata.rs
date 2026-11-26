//! 仅投影会话 step 的时间、模型和 Token，不保留消息、工具输出或凭据。

use crate::{LocalError, LocalErrorKind, TokenUsage};

/// 单个元数据 blob 的上限；SQL 驱动在读取前施加相同限制。
pub const MAX_METADATA_BYTES: usize = 256 * 1024;
/// 防止恶意 blob 通过极多微小字段放大内存。
const MAX_FIELDS: usize = 4096;

/// 一个已完成模型步骤的计量事实；思考量已包含于输出量。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepUsage {
    /// 上游步骤创建时间，Unix 毫秒。
    pub occurred_at_epoch_ms: i64,
    /// 上游模型枚举；模型名称由独立 generation 元数据解释。
    pub model_id: u64,
    /// 输入包含缓存读取，输出包含思考；二者不可重复累加。
    pub usage: TokenUsage,
}

/// 解析 `steps.metadata` 的字段 1（时间）和 9（用量），忽略其他内容。
/// # Errors
/// 损坏 protobuf、时间或不自洽的用量返回脱敏错误；没有用量的步骤返回 `None`。
pub fn decode_step(bytes: &[u8]) -> Result<Option<StepUsage>, LocalError> {
    let message = Message::decode(bytes)?;
    let Some(usage) = message.bytes(9)? else { return Ok(None) };
    let usage = Message::decode(usage)?;
    let timestamp = Message::decode(message.bytes(1)?.ok_or_else(invalid)?)?;
    let seconds = i64::try_from(timestamp.number(1)?).map_err(|_| invalid())?;
    let nanos = timestamp.number(2)?;
    if nanos >= 1_000_000_000 { return Err(invalid()) }
    let occurred_at_epoch_ms = seconds.checked_mul(1000)
        .and_then(|value| value.checked_add((nanos / 1_000_000) as i64))
        .ok_or_else(invalid)?;
    let model_id = usage.number(1)?;
    let uncached = usage.number(2)?;
    let output = usage.number(3)?;
    let cached = usage.number(5)?;
    // Antigravity 的 tokens_sent 不包含 prompt cache；统一合同的输入必须包含缓存。
    let input = uncached.checked_add(cached).ok_or_else(invalid)?;
    let thinking = usage.number(9)?;
    // proto3 的省略标量为零；未知的缓存写入指标继续保持不可用。
    let tokens = TokenUsage::new_with_component_availability(
        Some(input), Some(cached), None, Some(output), Some(thinking), None,
    ).map_err(|_| invalid())?;
    Ok(Some(StepUsage { occurred_at_epoch_ms, model_id, usage: tokens }))
}

/// 从 `gen_metadata.data` 的 1.3 / 1.19 读取模型枚举与受控技术名称。
/// # Errors
/// 损坏或越界元数据失败；缺少名称时保留未知模型，不从会话文本猜测。
pub fn decode_model(bytes: &[u8]) -> Result<Option<(u64, String)>, LocalError> {
    let message = Message::decode(bytes)?;
    let Some(generation) = message.bytes(1)? else { return Ok(None) };
    let generation = Message::decode(generation)?;
    let Some(name) = generation.bytes(19)? else { return Ok(None) };
    let name = std::str::from_utf8(name).map_err(|_| invalid())?;
    let name = crate::safe_technical_label(name).ok_or_else(invalid)?;
    Ok(Some((generation.number(3)?, name)))
}

/// protobuf 线值只借用输入，不反序列化任意字符串。
enum Value<'a> { Number(u64), Bytes(&'a [u8]), Fixed }
/// 已检查长度的局部 protobuf 消息；不存在递归扫描。
struct Message<'a>(Vec<(u32, Value<'a>)>);

impl<'a> Message<'a> {
    /// 消费合法 wire type，拒绝越界、重复目标字段和组编码。
    fn decode(mut bytes: &'a [u8]) -> Result<Self, LocalError> {
        if bytes.len() > MAX_METADATA_BYTES { return Err(invalid()) }
        let mut fields = Vec::new();
        while !bytes.is_empty() {
            if fields.len() >= MAX_FIELDS { return Err(invalid()) }
            let tag = varint(&mut bytes)?;
            let number = u32::try_from(tag >> 3).map_err(|_| invalid())?;
            if number == 0 || number >= (1 << 29) { return Err(invalid()) }
            let value = match tag & 7 {
                0 => Value::Number(varint(&mut bytes)?),
                2 => {
                    let size = usize::try_from(varint(&mut bytes)?).map_err(|_| invalid())?;
                    Value::Bytes(take(&mut bytes, size)?)
                }
                1 => { take(&mut bytes, 8)?; Value::Fixed }
                5 => { take(&mut bytes, 4)?; Value::Fixed }
                _ => return Err(invalid()),
            };
            fields.push((number, value));
        }
        Ok(Self(fields))
    }

    /// 查询单值字段，避免重复字段引起不同消费者解释不一致。
    fn field(&self, number: u32) -> Result<Option<&Value<'a>>, LocalError> {
        let mut found = self.0.iter().filter(|(key, _)| *key == number);
        let value = found.next().map(|(_, value)| value);
        if found.next().is_some() { return Err(invalid()) }
        Ok(value)
    }

    /// 读取 proto3 无符号标量；缺席表示零。
    fn number(&self, number: u32) -> Result<u64, LocalError> {
        match self.field(number)? {
            None => Ok(0),
            Some(Value::Number(value)) => Ok(*value),
            _ => Err(invalid()),
        }
    }

    /// 读取借用的嵌套消息或技术标签。
    fn bytes(&self, number: u32) -> Result<Option<&'a [u8]>, LocalError> {
        match self.field(number)? {
            None => Ok(None),
            Some(Value::Bytes(value)) => Ok(Some(value)),
            _ => Err(invalid()),
        }
    }
}

/// 每个 varint 最多十字节，第十字节仅允许最低一位。
fn varint(bytes: &mut &[u8]) -> Result<u64, LocalError> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *take(bytes, 1)?.first().ok_or_else(invalid)?;
        if shift == 63 && byte > 1 { return Err(invalid()) }
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 { return Ok(value) }
    }
    Err(invalid())
}

/// 用 checked slice 保证截断内容不会触发 panic。
fn take<'a>(bytes: &mut &'a [u8], size: usize) -> Result<&'a [u8], LocalError> {
    let (value, rest) = bytes.split_at_checked(size).ok_or_else(invalid)?;
    *bytes = rest;
    Ok(value)
}

/// 解析失败不携带 blob 原文或路径。
fn invalid() -> LocalError {
    LocalError::new(LocalErrorKind::InvalidUsage, "antigravity usage metadata is invalid")
}

