# AI 用量扫描与统计

独立 Rust 依赖包，提供 Codex、Claude Code、Grok 和 WorkBuddy 本机用量信息的只读获取，以及 Token 规范化、去重和统计。最低 Rust 为 1.95，edition 为 2024。

此目录被产品根 workspace 显式排除，清单与锁文件独立维护。其他 Rust 客户端可直接引用：

```toml
[dependencies]
ai_usage_collect_scan = { version = "0.1.0", path = "../ai_usage_collect_scan" }
```

包遵守目录内的商业许可证，当前 `publish = false`，没有发布到 crates.io。

## 读取接口

| 能力 | API 路径 |
| --- | --- |
| Codex 快速/全盘发现 | `readers::discovery` |
| Claude Code、Claude Desktop 本地 Agent 发现 | `readers::claude::discovery` |
| Grok 发现 | `readers::grok::discovery` |
| 元数据候选发现与范围/进度 | `readers::metadata_discovery` |
| 本地卷、网络卷排除、用户目录 | `readers::volume_roots`、`readers::user_paths` |
| 已确认根内的文件枚举 | `readers::directory`、`readers::{claude,grok}::enumerate` |
| JSONL 流式解析 | `readers::jsonl::parse_jsonl_stream`、`readers::claude::jsonl::parse_claude_jsonl_stream`、`readers::grok::jsonl::parse_grok_jsonl_stream` |
| WorkBuddy 固定目录、用量与独立 Trace 投影 | `readers::workbuddy` |
| 规范化、去重、统计、日历窗口 | crate 根的 `canonicalize_usage_calls`、`aggregate_canonical_usage`、`build_usage_statistics` 等 |

发现 API 接受显式目录输入，也提供读取当前平台环境的便捷构造。目录验证会保留链接、网络卷、签名与取消边界；解析只返回用量和安全元数据，不返回提示词、回答或工具正文。带路径的发现结果仅供消费客户端原生层使用，不应直接发送到界面或网络。

同步读取由调用方安排在线程或受控 blocking worker 中执行。`ScanCancellation` 可跨线程请求停止；元数据发现协调器另有自身的取消与终态。WorkBuddy 的取消接口也直接接受 `ScanCancellation`。

## 消费解析结果

解析器接受 `Read` 和记录回调。消费方可以使用自己的内存集合、文件或数据库保存记录；回调返回错误时立即停止解析。

```rust,no_run
use std::fs::File;
use ai_usage_collect_scan::{
    ScanCancellation, SourceProvenance,
    aggregate_canonical_usage, canonicalize_usage_calls,
};
use ai_usage_collect_scan::readers::grok::jsonl::{
    GrokJsonlParseContext, parse_grok_jsonl_stream,
};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
// 实际应用先通过发现和枚举 API 取得经过验证的来源。
let file = File::open("updates.jsonl")?;
let provenance = SourceProvenance {
    source_id: "source-example".into(),
    root_id: "root-example".into(),
    relative_label: "sessions/project/session/updates.jsonl".into(),
    archived: false,
};
let mut calls = Vec::new();
let report = parse_grok_jsonl_stream(
    file,
    1024 * 1024,
    &mut GrokJsonlParseContext::new("session-example", None),
    &provenance,
    &ScanCancellation::new(),
    |call| { calls.push(call); Ok(()) },
)?;
let aggregate = aggregate_canonical_usage(&canonicalize_usage_calls(calls))?;
println!("读取 {} 条观察，去重后 {} 次调用", report.calls_emitted, aggregate.call_count);
# Ok(())
# }
```

Codex 的 `JsonlParseReport.snapshots` 必须与解析调用一起交给 `attach_session_snapshots`，保留 fork 所有权和累计快照对账口径。增量消费者自行保存解析上下文、已提交偏移、尾部未完成字节、超长行状态和来源身份；未完成 JSONL 行不能提交为已解析记录。包中的 checkpoint 和快照只是信息值，不执行数据库操作。

本包不包含数据库驱动、建库、迁移、索引写入、账号、上传或 GUI。产品客户端的 SQLite、事务与增量索引恢复继续由客户端实现。

`test-util` feature 默认关闭，仅提供跨 crate 回归需要的旧增量指纹解码工具；运行时解析上下文恢复使用 `JsonlParseContext::from_checkpoint`。

## 测试

```bash
cargo test --manifest-path ai_usage_collect_scan/Cargo.toml --locked
```

如果 Linux 的临时目录位于 tmpfs，请将测试进程的 `TMPDIR` 设置为本机持久磁盘目录，让合成来源通过本地卷策略。

[外部消费者测试](tests/consumer.rs) 从合成 Grok 目录完成发现、枚举、解析和聚合，校验源文件不变、未创建存储；同时验证两个 WorkBuddy 版本及子代理用量读取，并覆盖取消及回调失败。各来源的既有解析和统计测试随实现迁移。
