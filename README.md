# AI 用量扫描与统计

独立 Rust 依赖包，提供 Codex、Claude Code、Grok、ZCode 和 WorkBuddy 本机用量的只读获取，以及 Token 校验、规范化、去重和通用统计。最低 Rust 为 1.95，edition 为 2024。

本目录被产品根 workspace 显式排除，清单、版本和锁文件独立维护。其他 Rust 应用直接引用本包即可，无需依赖毕方面板、Tauri、SQLite 或异步运行时：

```toml
[dependencies]
ai_usage_collect_scan = { version = "0.3.1", path = "../ai_usage_collect_scan" }
```

包遵守目录内的商业许可证，当前 `publish = false`，没有发布到 crates.io。

## 能力边界

| 本包提供 | 消费应用负责 |
| --- | --- |
| 来源发现、获批准目录的枚举与只读打开 | 启用哪些来源、任务调度、线程与进度展示 |
| 流式解析、用量校验、完整行检查点、来源报告及可选统计持久化 | 选择统计库保存路径、增量重建和调度 |
| 标准调用、累计快照、覆盖事实和通用分组统计 | 保留天数、默认查询、分页、图表和联合视图 |
| 宿主可实现的取消检查与逐记录回调 | 回调产生的存储错误、取消结果的提交或回滚 |

库不写来源文件，不包含面板、GUI 设置/账号、上传或网络传输。缺省只读；显式启用 `storage` 后提供按 Agent 分库的统计 SQLite、新格式 schema 初始化与校验、发现候选、来源登记、扫描状态、checkpoint 和事务快照。产品视图、保留策略和调度仍由客户端负责，GUI 自身业务/设置/账号库与统计库隔离。

## 按 Agent 保存统计

```toml
ai_usage_collect_scan = { version = "0.3.1", path = "../ai_usage_collect_scan", features = ["storage"] }
```

`storage::LocalIndex::open(base_dir, relative_path, client)` 要求绝对基准目录、非空相对路径和 `SourceClientKind`。例如同一基准目录分别使用 `codex.db`、`claude.db`、`grok.db`、`glm.db`。嵌套相对目录可自动创建；拒绝绝对路径、父目录跳转、链接、WorkBuddy、错 Agent 与未知业务数据库。库不决定产品目录，也不从当前进程读取 GUI 设置；调用者提供 Tokio 运行时。

```rust,no_run
# #[cfg(feature = "storage")]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use std::path::Path;
use ai_usage_collect_scan::{SourceClientKind, aggregate_canonical_usage};
use ai_usage_collect_scan::storage::LocalIndex;
let index = LocalIndex::open(
    Path::new("/application/statistics"), Path::new("codex.db"), SourceClientKind::Codex,
).await?;
let canonical = index.canonical_calls().await?;
let aggregate = aggregate_canonical_usage(&canonical)?;
# let _ = aggregate;
# Ok(())
# }
# fn main() {}
```

完整文件导入分为两步：在宿主拥有的 blocking worker 中调用 `PreparedFileIndex::read(format, request, options, cancel, emit)`，随后 await `index.commit_file(prepared, cancel)`。根 ID 使用 `stable_id(client.root_id_namespace(), &path_key(&canonical_root))`。事件在提交前交付，只有返回 `true` 才已发布；取消返回 `false`，回调/解析失败无可提交结果。数据库失败回滚该文件的事实和 checkpoint，来源始终只读。重复导入以新 generation 替换旧代次，不重复累计；大文件按有限批次写入，Claude 回退质量计入扫描告警。

单文件导入不证明整个根枚举完整，因此根覆盖保守记为 Partial。此便捷入口不输出来源专属增量上下文；后续 GUI 因缺少警告/上下文快照安全完整重建。需要增量索引时，使用同一 `LocalIndex` 的来源登记、checkpoint、批次写入和扫描事务原语；`begin_scan_attempt` 到发布/回滚之间，其他连接只见上次已提交结果。

统计存储只初始化空库或打开带当前格式标记的库，不接管旧 schema 或未知业务库。GUI 在产品数据目录下分别传入 `codex.db`、`claude.db`、`grok.db`、`glm.db`；旧统计库不探测、不读取、不导入、不删除，由用户重新扫描建立数据。每个 Agent 库共同保存发现候选、确认根及扫描位置、扫描状态、来源 checkpoint、调用和累计快照。候选可跨连接和进程恢复，确认登记与移除候选在同一事务内完成；宿主必须先持久化本轮候选再发布发现终态，取消时保留已发现位置并保守报告覆盖。

GUI 的 `gui-settings.sqlite3`、账号会话库和身份锚不属于本包；误传 GUI 业务数据库会被拒绝。保留策略、跨库联合视图和业务调度仍由客户端决定。

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

- `parsing::UsageParser`：关联 `Context` 和 `Report`，输入为标准 `Read`，输出标准 `UsageCall`。内置 `CodexParser`、`ClaudeParser`、`GrokParser`、`GlmParser` 与产品索引器共用此协议。外部应用可以实现新格式，无需修改 `SourceClientKind` 或产品来源闭集。
- `CancellationSignal`：宿主可实现自己的取消信号，也可直接传入借用宿主状态的 `Fn() -> bool + Send + Sync`；`ScanCancellation` 是库提供的便利实现。JSONL 读取和 WorkBuddy 读取共用该接口；发现协调器保留已有的生命周期与取消接口。

不额外定义文件系统、记录仓库或统计 Trait：标准 `Read` / `Seek` 和 `FnMut(UsageCall)` 已能替换输入与消费方式，纯统计函数可以直接组合。

解析器必须为 `logical_call_id`、线程、项目和来源身份维护稳定、跨来源无碰撞的命名空间；`ParserDescriptor.id` 不会自动参与调用去重。`dyn UsageParser` 要求指定相同的 Context/Report；该协议不自动注册来源发现或扩大产品允许的来源集合。内置来源枚举仍保留兼容的来源展示标签，第三方解析和通用统计不依赖这些标签。

需要增量读取时，先通过 `input::open_*_source` 得到 `OpenedUsageFile`，读取同一次打开的大小、修改时间和指纹，再用 `parsing::read_observed_prefix`、`ReadRange` 和 `ParseRequest` 读取指定前缀。范围外的追加留给下一轮；上下文按值移交，错误时不返回已推进的上下文。成功或取消报告的完整行偏移、未完成尾部、超长行状态，以及来源特有上下文应作为一组由客户端保存。指纹是既有首 64 字节内容摘要，不是操作系统物理文件身份。

底层 `readers::{jsonl,claude::jsonl,grok::jsonl,glm::jsonl}` 接口仍可直接使用。该层按引用修改上下文，失败后调用方应恢复最后一次已提交状态。异步宿主负责在自己拥有并回收的 blocking worker 中调用同步文件读取。

## 发现与 WorkBuddy

| 能力 | API 路径 |
| --- | --- |
| Codex 快速/全盘发现 | `readers::discovery` |
| Claude Code、Claude Desktop 本地 Agent 发现 | `readers::claude::discovery` |
| Grok 发现 | `readers::grok::discovery` |
| ZCode 发现 | `readers::glm::discovery` |
| 元数据候选、范围和进度 | `readers::metadata_discovery` |
| 本地卷、网络卷排除和用户目录 | `readers::volume_roots`、`readers::user_paths` |
| 根内枚举 | `readers::directory`、`readers::{claude,grok,glm}::enumerate` |
| WorkBuddy 国内/国际版固定布局读取 | `readers::workbuddy` |
| WorkBuddy 通用校验、去重和安全记录 | `workbuddy_facts` |
| 规范化与通用统计 | `canonicalize_usage_calls`、`aggregate_canonical_usage`、`group_usage`、`summarize_usage` |

发现接口接受显式目录输入，也提供当前平台环境的便利构造，保留本地卷、链接、签名与取消边界。原生路径和项目种子仅供本机身份派生，不应发往界面、网络或日志。解析过程只输出用量和安全元数据，不交付提示词、回答或工具正文。WorkBuddy 保留自身的记录模型和固定来源布局，不为统一接口强行转换为有损的 JSONL 调用模型；面板周期、小时趋势和来源联合规则由产品 core 实现。

## FFI 与 Node 接入

独立项目 `ai_usage_collect_scan_ffi` 提供版本化 C ABI，`ai_usage_collect_scan_node` 通过自己的 Node-API 桥接调用它。本扫描库使用安全 Rust 提供只读获取、通用统计及可选统计存储，不包含面板或外语运行时。接口文档分别位于两个包装项目自己的 `docs/`，见 [C ABI](../ai_usage_collect_scan_ffi/docs/API.md) 和 [Node](../ai_usage_collect_scan_node/docs/API.md)。Rust Trait、`Vec`、`String` 和对象内存布局不作为外语 ABI。

## Rust API 版本变化

0.2 将面板视图、保留天数和默认查询留在产品 core，第三方消费者使用通用聚合/分组函数。0.3 新增可选统计存储，共享索引四态、来源登记、checkpoint 值对象和包含数据库类别的统一 `LocalError`；产品 core 重用同一类型。

0.3 将 `RootCandidate.absolute_path` 从 `String` 改为原生 `PathBuf`，消费方需同步构造和访问代码；其 `Debug` 不再输出访问路径。原始解析代次保持不变，统计数据库采用新格式，不兼容旧库。C ABI 和 Node 的请求/结果字段及版本化 wire 合同保持不变，包装项目仍独立维护版本。

公开 `canonical_calls`、`usage_snapshot`、`usage_snapshot_since`、`index_state` 和普通聚合只读取已发布状态；调用与累计快照、索引存在性与状态计数分别在同一只读事务中取得。同一 `LocalIndex` 正持有未发布扫描事务时，这些入口立即返回 `LocalErrorKind::ScanBusy`，不等待自己占用的唯一连接。扫描内部策略继续使用 `has_current_parser_usage`，扫描摘要使用 `aggregate_active_scan_for_provider` 读取本事务暂存事实。需要并发读取上次发布结果时，另开同一数据库的 `LocalIndex` 即可。

`test-util` feature 默认关闭，提供跨 crate 回归需要的旧增量指纹解码及存储故障注入工具；运行时上下文恢复使用各来源的 checkpoint 构造。

## 测试

```bash
cargo test --manifest-path ai_usage_collect_scan/Cargo.toml --all-features --locked
```

Linux 临时目录位于 tmpfs 时，将 `TMPDIR` 指向本机持久磁盘目录，让合成来源通过本地卷策略。

[消费者测试](tests/consumer.rs) 覆盖发现、枚举、读取、统计、WorkBuddy 双版本及子代理，并验证不修改来源或创建存储。[扩展测试](tests/parser_extension.rs) 用外部动态解析器与借用宿主状态的取消信号复用标准统计。[文件入口测试](tests/readonly_files.rs) 覆盖四种内置来源、快照、序列化、增量前缀、同一文件句柄、路径跳转和链接拒绝。

ZCode（Z Code CLI）使用 `JsonlFormat::Glm`，只接受获授权 rollout 根内平铺的 `model-io-sess_*.jsonl` 普通文件。默认候选为 `~/.zcode/cli/rollout`，subagent 文件同样计入；仅提取 `model_io` 的模型、时间、会话身份与 `response.usage` 五项，不输出 request/response 正文或 headers。ZCode 无项目、推理强度及独立推理输出分项，缺失值保持未提供。

### ZCode 桌面用量库

启用既有 `storage` feature 后，`readers::glm::database::ZcodeUsageDatabase` 分页只读固定 `~/.zcode/cli/db/db.sqlite` 的 `model_usage` 白名单列；`LocalIndex::index_zcode_database` 在宿主已有扫描事务内写入本产品派生库。每轮读一致快照及 WAL，以 `computed_total_tokens` 为总量，保留真实模型名，包含付费与免费模型。默认发现同安装优先数据库，只有库缺失时选择 rollout，坏库不回退。CLI 的 `JsonlFormat::Glm` 接口保持 JSONL，不把 SQLite 字节交给 JSONL parser；稳定 `glm`/provider 编码保持兼容。
