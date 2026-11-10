//! 可扩展的流式解析协议；输入、取消及记录消费均由宿主装配。

mod builtin;
mod observed;

use crate::readers::DEFAULT_MAX_JSONL_LINE_BYTES;
use crate::{CancellationSignal, LocalError, SourceProvenance, UsageCall};
use serde::Serialize;
use std::io::Read;

pub use builtin::{ClaudeParser, CodexParser, GlmParser, GrokParser};
pub use observed::{ReadRange, read_observed_prefix};

/// 解析器的稳定技术标识和语义版本；不依赖产品的来源枚举。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParserDescriptor {
    /// 来源格式的稳定标识，由实现者维护命名空间。
    pub id: &'static str,
    /// 解析或上下文语义改变时由实现者递增。
    pub version: u32,
}

/// 一次读取拥有的上下文与调用方提供的只读约束。
///
/// 上下文按值移交，失败时不会返回已被部分推进的状态。重新读取应恢复最后一次成功状态。
#[non_exhaustive]
pub struct ParseRequest<'a, C> {
    /// 本轮开始前的解析上下文。
    pub context: C,
    /// 来源追溯信息，仅允许安全元数据。
    pub provenance: &'a SourceProvenance,
    /// 宿主提供的协作取消检查，不要求库自己的令牌类型。
    pub cancellation: &'a dyn CancellationSignal,
    /// JSONL 普通行内存预算；超长行语义由具体解析器报告。
    pub max_line_bytes: usize,
}

impl<'a, C> ParseRequest<'a, C> {
    /// 建立默认单行预算的读取请求，不创建运行时或执行 I/O。
    pub fn new(
        context: C,
        provenance: &'a SourceProvenance,
        cancellation: &'a dyn CancellationSignal,
    ) -> Self {
        Self {
            context,
            provenance,
            cancellation,
            max_line_bytes: DEFAULT_MAX_JSONL_LINE_BYTES,
        }
    }

    /// 设置当前读取的单行内存预算，保持既有来源的超长行处理语义。
    pub fn max_line_bytes(mut self, maximum: usize) -> Self {
        self.max_line_bytes = maximum;
        self
    }
}

impl<C: std::fmt::Debug> std::fmt::Debug for ParseRequest<'_, C> {
    /// 调试输出只包含上下文类型已有的安全内容，不调用或输出宿主取消对象。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParseRequest")
            .field("context", &self.context)
            .field("provenance", self.provenance)
            .field("max_line_bytes", &self.max_line_bytes)
            .finish_non_exhaustive()
    }
}

/// 成功读取的上下文和完整报告；消费方自行决定是否保存部分取消结果。
#[derive(Debug)]
pub struct ParsedUsage<C, R> {
    /// 可与报告中的完整行偏移一起保存的安全上下文。
    pub context: C,
    /// 来源原生报告，包含检查点、告警和来源特有事实。
    pub report: R,
}

/// 逐调用输出接口，消费方可以收集或写入自己的存储。
pub type UsageEmitter<'a> = dyn FnMut(UsageCall) -> Result<(), LocalError> + 'a;

/// 可由其他应用实现的同步解析器，不拥有数据库、运行时、线程或产品配置。
///
/// 同一 Context/Report 组合可通过 `dyn UsageParser` 调用。扩展来源无需修改内置来源枚举。
/// 实现必须仅输出用量与安全元数据，保留取消、完整行检查点和来源特有报告。
/// sink 或输入失败时必须返回错误，不得向调用方交出已推进的上下文。
/// 记录回调具有流式语义；报告可能持有额外事实，例如 Codex 累计快照，不承诺整文件常量内存。
pub trait UsageParser: Send + Sync {
    /// 消费客户端可以自行恢复和保存的来源解析上下文。
    type Context;
    /// 保留来源特有检查点、告警和兼容事实的报告。
    type Report;

    /// 返回稳定来源标识和当前解析语义版本。
    fn descriptor(&self) -> ParserDescriptor;

    /// 读取调用方提供的流，并按记录回调交付规范化用量。
    ///
    /// # Errors
    /// 输入读取、Token 校验或消费方回调失败时返回错误；消费方不得提交失败调用的状态。
    fn parse(
        &self,
        reader: &mut dyn Read,
        request: ParseRequest<'_, Self::Context>,
        emit: &mut UsageEmitter<'_>,
    ) -> Result<ParsedUsage<Self::Context, Self::Report>, LocalError>;
}
