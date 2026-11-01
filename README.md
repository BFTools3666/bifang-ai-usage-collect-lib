# AI 用量扫描与统计

独立 Rust 依赖包，提供 Codex、Claude Code、Grok 和 WorkBuddy 本机用量的只读获取，以及 Token 校验、规范化、去重和通用统计。最低 Rust 为 1.95，edition 为 2024。

本目录被产品根 workspace 显式排除，清单、版本和锁文件独立维护。其他 Rust 应用直接引用本包即可，无需依赖毕方面板、Tauri、SQLite 或异步运行时：

```toml
[dependencies]
ai_usage_collect_scan = { version = "0.2.0", path = "../ai_usage_collect_scan" }
```

包遵守目录内的商业许可证，当前 `publish = false`，没有发布到 crates.io。

## 能力边界

| 本包提供 | 消费应用负责 |
| --- | --- |
| 来源发现、获批准目录的枚举与只读打开 | 启用哪些来源、任务调度、线程与进度展示 |
| 流式解析、用量校验、完整行检查点和来源报告 | 检查点持久化、增量重建、数据库与事务 |
| 标准调用、累计快照、覆盖事实和通用分组统计 | 保留天数、默认查询、分页、图表和联合视图 |
| 宿主可实现的取消检查与逐记录回调 | 回调产生的存储错误、取消结果的提交或回滚 |

库不写来源文件，不建立数据库，不包含面板、账号、上传或网络传输。产品的视图模型和存储策略位于 `ai_usage_collect_core`，SQLite 驱动和运行时装配位于 GUI backend。

## 单文件读取

已知来源根和文件时，`input::read_usage_file` 完成路径与链接检查、同一只读句柄打开、观测前缀解析和通用报告投影。下面的例子只在调用方内存中收集结果：

```rust,no_run
use ai_usage_collect_scan::{
    ScanCancellation, aggregate_canonical_usage, attach_session_snapshots,
    canonicalize_usage_calls,
};
use ai_usage_collect_scan::input::{
    JsonlFormat, SourceFileRequest, UsageReadOptions, read_usage_file,
};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
// 原生调用方通过发现或用户授权取得绝对路径，不接受界面传入的任意路径。
let root = std::path::PathBuf::from("/authorized/grok-root");
let request = SourceFileRequest {
    file_path: root.join("sessions/project/session/updates.jsonl"),
    root_path: root,
    root_id: "root-example".into(),
};
let mut calls = Vec::new();
let report = read_usage_file(
    JsonlFormat::Grok, request, UsageReadOptions::default(),
    &ScanCancellation::new(),
    &mut |call| { calls.push(call); Ok(()) },
)?;
// 取消可能带回部分结果，由消费方决定提交还是重试。
if !report.cancelled {
    let canonical = attach_session_snapshots(
        canonicalize_usage_calls(calls), report.session_snapshots,
    );
    let total = aggregate_canonical_usage(&canonical)?;
    println!("{} 次调用，{} Token", total.call_count, total.tokens.total_tokens);
}
# Ok(())
# }
```

Codex 的累计快照通过报告保留，必须参与 `attach_session_snapshots` 对账，不能丢弃或作为额外调用累加。记录通过回调流式交付，但报告可能收集快照，不承诺整文件常量内存。回调或输入返回错误后，应放弃本轮状态；数据库错误详情由客户端自行保管，回调只返回库的静态安全错误。

## 扩展与增量读取

仅抽取两个有实际替换需求的 Trait：

- `parsing::UsageParser`：关联 `Context` 和 `Report`，输入为标准 `Read`，输出标准 `UsageCall`。内置 `CodexParser`、`ClaudeParser`、`GrokParser` 与产品索引器共用此协议。外部应用可以实现新格式，无需修改 `SourceClientKind` 或产品来源闭集。
- `CancellationSignal`：宿主可实现自己的取消信号，也可直接传入借用宿主状态的 `Fn() -> bool + Send + Sync`；`ScanCancellation` 是库提供的便利实现。JSONL 读取和 WorkBuddy 读取共用该接口；发现协调器保留已有的生命周期与取消接口。

不额外定义文件系统、记录仓库或统计 Trait：标准 `Read` / `Seek` 和 `FnMut(UsageCall)` 已能替换输入与消费方式，纯统计函数可以直接组合。

解析器必须为 `logical_call_id`、线程、项目和来源身份维护稳定、跨来源无碰撞的命名空间；`ParserDescriptor.id` 不会自动参与调用去重。`dyn UsageParser` 要求指定相同的 Context/Report；该协议不自动注册来源发现或扩大产品允许的来源集合。内置来源枚举仍保留兼容的来源展示标签，第三方解析和通用统计不依赖这些标签。

需要增量读取时，先通过 `input::open_*_source` 得到 `OpenedUsageFile`，读取同一次打开的大小、修改时间和指纹，再用 `parsing::read_observed_prefix`、`ReadRange` 和 `ParseRequest` 读取指定前缀。范围外的追加留给下一轮；上下文按值移交，错误时不返回已推进的上下文。成功或取消报告的完整行偏移、未完成尾部、超长行状态，以及来源特有上下文应作为一组由客户端保存。指纹是既有首 64 字节内容摘要，不是操作系统物理文件身份。

底层 `readers::{jsonl,claude::jsonl,grok::jsonl}` 接口仍可直接使用。该层按引用修改上下文，失败后调用方应恢复最后一次已提交状态。异步宿主负责在自己拥有并回收的 blocking worker 中调用同步文件读取。

## 发现与 WorkBuddy

| 能力 | API 路径 |
| --- | --- |
| Codex 快速/全盘发现 | `readers::discovery` |
| Claude Code、Claude Desktop 本地 Agent 发现 | `readers::claude::discovery` |
| Grok 发现 | `readers::grok::discovery` |
| 元数据候选、范围和进度 | `readers::metadata_discovery` |
| 本地卷、网络卷排除和用户目录 | `readers::volume_roots`、`readers::user_paths` |
| 根内枚举 | `readers::directory`、`readers::{claude,grok}::enumerate` |
| WorkBuddy 国内/国际版固定布局读取 | `readers::workbuddy` |
| WorkBuddy 通用校验、去重和安全记录 | `workbuddy_facts` |
| 规范化与通用统计 | `canonicalize_usage_calls`、`aggregate_canonical_usage`、`group_usage`、`summarize_usage` |

发现接口接受显式目录输入，也提供当前平台环境的便利构造，保留本地卷、链接、签名与取消边界。原生路径和项目种子仅供本机身份派生，不应发往界面、网络或日志。解析过程只输出用量和安全元数据，不交付提示词、回答或工具正文。WorkBuddy 保留自身的记录模型和固定来源布局，不为统一接口强行转换为有损的 JSONL 调用模型；面板周期、小时趋势和来源联合规则由产品 core 实现。

## FFI 与 Node 接入

独立项目 `ai_usage_collect_scan_ffi` 提供版本化 C ABI，`ai_usage_collect_scan_node` 通过自己的 Node-API 桥接调用它。本扫描库仍保持安全 Rust 的只读获取和通用统计职责，不增加存储、面板或外语运行时。接口文档分别位于两个包装项目自己的 `docs/`，见 [C ABI](../ai_usage_collect_scan_ffi/docs/API.md) 和 [Node](../ai_usage_collect_scan_node/docs/API.md)。Rust Trait、`Vec`、`String` 和对象内存布局不作为外语 ABI。

## 从 0.1 迁移

0.2 是独立包的 API 边界调整：面板视图、保留天数、默认查询、索引状态以及数据库错误类型移回产品 core；第三方消费者改用通用聚合/分组函数。读取错误现在只包含来源和解析类别，产品通过 `From<ai_usage_collect_scan::LocalError>` 映射自身错误。原始解析代次和检查点格式保持不变。

`test-util` feature 默认关闭，仅提供跨 crate 回归需要的旧增量指纹解码工具；运行时上下文恢复使用各来源的 checkpoint 构造。

## 测试

```bash
cargo test --manifest-path ai_usage_collect_scan/Cargo.toml --all-features --locked
```

Linux 临时目录位于 tmpfs 时，将 `TMPDIR` 指向本机持久磁盘目录，让合成来源通过本地卷策略。

[消费者测试](tests/consumer.rs) 覆盖发现、枚举、读取、统计、WorkBuddy 双版本及子代理，并验证不修改来源或创建存储。[扩展测试](tests/parser_extension.rs) 用外部动态解析器与借用宿主状态的取消信号复用标准统计。[文件入口测试](tests/readonly_files.rs) 覆盖三种内置来源、快照、序列化、增量前缀、同一文件句柄、路径跳转和链接拒绝。
