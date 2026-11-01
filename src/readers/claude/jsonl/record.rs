//! 定义 Claude transcript 单行解析所需的最小 wire 字段，不接触消息正文。

use crate::UsageCall;
use serde::Deserialize;

/// transcript 单行记录只反序列化的最小字段集合，缺失字段一律容忍为 `None`。
#[derive(Deserialize)]
pub struct TranscriptRecord {
    /// 事件类型标签；只有 `assistant` 才可能是用量记录。
    #[serde(rename = "type")]
    pub kind: Option<String>,
    /// 会话 ID。
    #[serde(rename = "sessionId")]
    pub session_id: Option<String>,
    /// 记录时间戳原始字符串。
    pub timestamp: Option<String>,
    /// 承载用量事实的消息片段。
    pub message: Option<AssistantMessage>,
    /// 助手用量事件上的推理强度；缺失时保持未提供。
    #[serde(default, alias = "reasoning_effort", alias = "reasoningEffort")]
    pub effort: Option<String>,
}

/// 记录中承载用量事实的 assistant 消息片段。
#[derive(Deserialize)]
pub struct AssistantMessage {
    /// 消息 ID。
    pub id: Option<String>,
    /// 模型名。
    pub model: Option<String>,
    /// Token 用量字段。
    pub usage: Option<AssistantUsage>,
}

/// 折算 Token 用量所需的原始字段。
#[derive(Deserialize)]
pub struct AssistantUsage {
    /// 输入 Token。
    pub input_tokens: Option<u64>,
    /// 输出 Token。
    pub output_tokens: Option<u64>,
    /// 缓存读取的输入 Token。
    #[serde(default)]
    pub cache_read_input_tokens: Option<u64>,
    /// 缓存写入的输入 Token。
    #[serde(default)]
    pub cache_creation_input_tokens: Option<u64>,
}

/// 单行解析后的稳定分类结果，供逐类质量计数使用。
pub enum LineOutcome {
    /// 解析出一条可用的用量调用。
    Call(Box<UsageCall>),
    /// 已知的非用量事件类型，安全忽略。
    Ignored,
    /// 未识别的事件类型。
    Unknown,
    /// 行内容不是合法 JSON。
    Malformed,
    /// 超过单行内存预算且不是有效 JSON 对象。
    Oversized,
    /// 缺少判定用量所需的必要字段。
    MissingUsage,
    /// 用量字段存在但数值不合法（如溢出）。
    InvalidUsage,
    /// 时间戳缺失或无法解析。
    InvalidTimestamp,
}
