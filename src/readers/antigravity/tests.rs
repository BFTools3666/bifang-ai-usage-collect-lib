//! 合成 protobuf 与临时目录验证；不依赖用户历史或正在运行的 App。

use super::metadata::{decode_model, decode_step};
use super::discovery::{self, AntigravityDiscoveryInputs};
use crate::readers::CancellationToken;

/// 夹具写入 proto3 无符号标量。
fn number(field: u64, value: u64) -> Vec<u8> {
    let mut bytes = unsigned(field << 3);
    bytes.extend(unsigned(value));
    bytes
}

/// 夹具写入嵌套消息。
fn message(field: u64, value: &[u8]) -> Vec<u8> {
    let mut bytes = unsigned((field << 3) | 2);
    bytes.extend(unsigned(value.len() as u64));
    bytes.extend(value);
    bytes
}

/// fixture 的 varint 编码只负责构造输入。
fn unsigned(mut value: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    while value > 127 {
        bytes.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    bytes.push(value as u8);
    bytes
}

/// 时间与计量字段来自本机验证过的布局，计数使用合成值。
fn step(input: u64, cached: u64, output: u64, thinking: u64) -> Vec<u8> {
    let timestamp = [number(1, 1_790_000_000), number(2, 123_000_000)].concat();
    let usage = [number(1, 1532), number(2, input), number(3, output), number(5, cached), number(9, thinking)].concat();
    [message(1, &timestamp), message(9, &usage)].concat()
}

/// 缓存和思考都是已有分项的子集，总量只累加输入和输出。
#[test]
fn cached_and_thinking_tokens_are_not_counted_twice() {
    let result = decode_step(&step(20, 80, 30, 20)).expect("valid step").expect("usage");
    assert_eq!(result.occurred_at_epoch_ms, 1_790_000_000_123);
    assert_eq!(result.model_id, 1532);
    assert_eq!(result.usage.input_tokens, Some(100));
    assert_eq!(result.usage.cached_input_tokens, Some(80));
    assert_eq!(result.usage.output_tokens, Some(30));
    assert_eq!(result.usage.reasoning_output_tokens, Some(20));
    assert_eq!(result.usage.cache_write_input_tokens, None);
    assert_eq!(result.usage.total_tokens, 130);
    assert!(result.usage.total_is_derived);
}

/// proto3 零标量可以省略，非用量步骤不制造零调用。
#[test]
fn omitted_zero_fields_and_non_usage_steps_are_distinct() {
    let result = decode_step(&[message(1, &number(1, 1_790_000_000)), message(9, &number(1, 1532))].concat())
        .expect("proto3 defaults").expect("usage message exists");
    assert_eq!(result.usage.total_tokens, 0);
    assert_eq!(result.usage.cached_input_tokens, Some(0));
    assert_eq!(decode_step(&message(1, &number(1, 1_790_000_000))).expect("ordinary step"), None);
}

/// 损坏或不自洽的计量不得变成可信零用量。
#[test]
fn malformed_and_inconsistent_metadata_fail_closed() {
    for bytes in [vec![0x80; 11], vec![0x0a, 0xff], vec![0], step(u64::MAX, 1, 3, 0), step(10, 0, 3, 4)] {
        assert!(decode_step(&bytes).is_err());
    }
    let timestamp_overflow = [message(1, &number(1, i64::MAX as u64)), message(9, &number(1, 1532))].concat();
    assert!(decode_step(&timestamp_overflow).is_err());
    let mut duplicated = step(10, 0, 3, 1);
    duplicated.extend(message(9, &number(2, 900)));
    assert!(decode_step(&duplicated).is_err());
}

/// generation 中只接受受控模型名，不从消息内容推断模型。
#[test]
fn generation_model_labels_are_bounded_technical_metadata() {
    let bytes = message(1, &[number(3, 1532), message(19, b"gemini-4-neon")].concat());
    assert_eq!(decode_model(&bytes).expect("model metadata"), Some((1532, "gemini-4-neon".to_owned())));
    let invalid = message(1, &[number(3, 1532), message(19, b"secret / user prompt")].concat());
    assert!(decode_model(&invalid).is_err());
    assert_eq!(decode_model(&[]).expect("no model"), None);
}

/// App 与 CLI 根可分别发现；摘要库和其他 Gemini 数据不属于会话来源。
#[test]
fn quick_discovery_uses_only_app_and_cli_conversation_roots() {
    let temp = tempfile::tempdir().expect("isolated user root");
    for client in ["antigravity", "antigravity-cli"] {
        let root = temp.path().join(".gemini").join(client).join("conversations");
        std::fs::create_dir_all(&root).expect("fixture root");
        std::fs::write(root.join("00000000-0000-0000-0000-000000000001.db"), b"SQLite format 3\0fixture")
            .expect("synthetic SQLite signature");
    }
    let inputs = AntigravityDiscoveryInputs { home_dir: Some(temp.path().to_owned()), registered_roots: Vec::new() };
    let result = discovery::discover_antigravity_quick(&inputs, &CancellationToken::default());
    assert_eq!(result.roots.len(), 2);
    assert!(result.roots.iter().all(|root| root.root_id.starts_with("antigravity-root-")));
    assert!(!discovery::is_conversation_file_name("conversation_summaries.db"));
    assert!(!discovery::is_conversation_file_name("../00000000-0000-0000-0000-000000000001.db"));
    assert!(!discovery::is_conversation_root(&temp.path().join(".gemini/conversations")));
}

/// 登记根不可读时保留原标识为未确认，不能误删旧快照。
#[test]
fn unavailable_registered_root_is_preserved_as_unconfirmed() {
    let temp = tempfile::tempdir().expect("isolated root");
    let inputs = AntigravityDiscoveryInputs {
        home_dir: None,
        registered_roots: vec![crate::RegisteredRoot { root_id: Some("antigravity-root-0000000000000001".to_owned()),
            path: temp.path().join("antigravity/conversations"), alias: "Antigravity".to_owned(), enabled: true }],
    };
    let result = discovery::discover_antigravity_quick(&inputs, &CancellationToken::default());
    assert!(result.confirmed_invalid_root_ids.is_empty());
    assert_eq!(result.unconfirmed_root_ids, ["antigravity-root-0000000000000001"]);
    assert_eq!(result.coverage.state, crate::CoverageState::Partial);
}

/// 用户给定的文件/字节预算必须在读取签名前生效。
#[test]
fn signature_budget_and_cancellation_do_not_read_a_database_header() {
    let temp = tempfile::tempdir().expect("isolated source");
    let root = temp.path().join("antigravity/conversations");
    std::fs::create_dir_all(&root).expect("source root");
    std::fs::write(root.join("00000000-0000-0000-0000-000000000001.db"), b"SQLite format 3\0")
        .expect("signature fixture");
    let options = crate::readers::FullDiscoveryOptions { max_signature_files: 0, ..Default::default() };
    let error = discovery::inspect_root(&root, &options, &CancellationToken::default()).expect_err("no file allowance");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    let cancel = CancellationToken::default();
    cancel.cancel();
    let error = discovery::inspect_root(&root, &Default::default(), &cancel).expect_err("cancelled");
    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
}

/// 新读取器拒绝主库和 WAL 链接，且不改写链接目标中的任何数据。
#[cfg(all(unix, feature = "storage"))]
#[tokio::test]
async fn database_and_wal_symlinks_are_rejected_without_modifying_the_target() {
    use std::os::unix::fs::symlink;
    use super::database::AntigravityUsageDatabase;
    let temp = tempfile::tempdir().expect("isolated source");
    let root = temp.path().join("antigravity/conversations");
    std::fs::create_dir_all(&root).expect("source root");
    let outside = temp.path().join("private-fixture");
    std::fs::write(&outside, b"private fixture bytes").expect("outside source");
    let name = "00000000-0000-0000-0000-000000000001.db";
    symlink(&outside, root.join(name)).expect("database symlink");
    let error = AntigravityUsageDatabase::open(&root, "antigravity-root-0000000000000001", name, i64::MIN)
        .await.expect_err("reject linked database");
    assert_eq!(error.kind(), crate::LocalErrorKind::InvalidPath);
    std::fs::remove_file(root.join(name)).expect("remove fixture link");
    std::fs::write(root.join(name), b"SQLite format 3\0").expect("plain database fixture");
    symlink(&outside, root.join(format!("{name}-wal"))).expect("WAL symlink");
    let error = AntigravityUsageDatabase::open(&root, "antigravity-root-0000000000000001", name, i64::MIN)
        .await.expect_err("reject linked WAL");
    assert_eq!(error.kind(), crate::LocalErrorKind::InvalidPath);
    assert_eq!(std::fs::read(outside).expect("outside content preserved"), b"private fixture bytes");
}
