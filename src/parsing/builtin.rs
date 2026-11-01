//! 三种内置 JSONL 格式的协议适配；实际计量逻辑仍只由各自解析器实现。

use super::{ParseRequest, ParsedUsage, ParserDescriptor, UsageEmitter, UsageParser};
use crate::readers::{claude::jsonl as claude, grok::jsonl as grok, jsonl};
use crate::{LocalError, SourceClientKind};
use std::io::Read;

/// Codex rollout 的解析协议实现。
#[derive(Debug, Clone, Copy, Default)]
pub struct CodexParser;

impl UsageParser for CodexParser {
    /// Codex 单源的累计用量与身份续读上下文。
    type Context = jsonl::JsonlParseContext;
    /// 保留 Codex 累计快照和完整行检查点的原生报告。
    type Report = jsonl::JsonlParseReport;
    /// 标识 Codex 的计量与检查点语义版本。
    fn descriptor(&self) -> ParserDescriptor {
        ParserDescriptor {
            id: "codex-rollout",
            version: SourceClientKind::Codex.parser_version(),
        }
    }
    /// 委托现有 rollout 解析器并保留全部累计快照。
    fn parse(
        &self,
        reader: &mut dyn Read,
        request: ParseRequest<'_, Self::Context>,
        emit: &mut UsageEmitter<'_>,
    ) -> Result<ParsedUsage<Self::Context, Self::Report>, LocalError> {
        let ParseRequest {
            mut context,
            provenance,
            cancellation,
            max_line_bytes,
        } = request;
        let report = jsonl::parse_jsonl_stream(
            reader,
            max_line_bytes,
            &mut context,
            provenance,
            cancellation,
            emit,
        )?;
        Ok(ParsedUsage { context, report })
    }
}

/// Claude transcript 的解析协议实现。
#[derive(Debug, Clone, Copy, Default)]
pub struct ClaudeParser;

impl UsageParser for ClaudeParser {
    /// Claude 相邻响应归并与身份续读上下文。
    type Context = claude::ClaudeJsonlParseContext;
    /// 保留 Claude 告警和完整行检查点的原生报告。
    type Report = claude::ClaudeJsonlParseReport;
    /// 标识 Claude 的单调观察与检查点语义版本。
    fn descriptor(&self) -> ParserDescriptor {
        ParserDescriptor {
            id: "claude-transcript",
            version: SourceClientKind::ClaudeCode.parser_version(),
        }
    }
    /// 保留 transcript 相邻响应归并与字段可用性。
    fn parse(
        &self,
        reader: &mut dyn Read,
        request: ParseRequest<'_, Self::Context>,
        emit: &mut UsageEmitter<'_>,
    ) -> Result<ParsedUsage<Self::Context, Self::Report>, LocalError> {
        let ParseRequest {
            mut context,
            provenance,
            cancellation,
            max_line_bytes,
        } = request;
        let report = claude::parse_claude_jsonl_stream(
            reader,
            max_line_bytes,
            &mut context,
            provenance,
            cancellation,
            emit,
        )?;
        Ok(ParsedUsage { context, report })
    }
}

/// Grok updates 的解析协议实现。
#[derive(Debug, Clone, Copy, Default)]
pub struct GrokParser;

impl UsageParser for GrokParser {
    /// Grok 会话与轮次身份续读上下文。
    type Context = grok::GrokJsonlParseContext;
    /// 保留 Grok 完成轮次告警与检查点的原生报告。
    type Report = grok::GrokJsonlParseReport;
    /// 标识 Grok 已完成轮次的解析语义版本。
    fn descriptor(&self) -> ParserDescriptor {
        ParserDescriptor {
            id: "grok-updates",
            version: SourceClientKind::GrokBuildCli.parser_version(),
        }
    }
    /// 仅输出完成轮次且保留原始报告。
    fn parse(
        &self,
        reader: &mut dyn Read,
        request: ParseRequest<'_, Self::Context>,
        emit: &mut UsageEmitter<'_>,
    ) -> Result<ParsedUsage<Self::Context, Self::Report>, LocalError> {
        let ParseRequest {
            mut context,
            provenance,
            cancellation,
            max_line_bytes,
        } = request;
        let report = grok::parse_grok_jsonl_stream(
            reader,
            max_line_bytes,
            &mut context,
            provenance,
            cancellation,
            emit,
        )?;
        Ok(ParsedUsage { context, report })
    }
}
