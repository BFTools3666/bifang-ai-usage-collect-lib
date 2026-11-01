//! 共享有界切行与超长行白名单流式反序列化的回归。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::Deserialize;

use super::*;

/// 测试用白名单：只声明用量与标识字段，其余正文由 serde 跳过。
#[derive(Debug, Deserialize, PartialEq)]
struct Usage {
    /// 测试记录 ID；缺失为 `None`。
    id: Option<String>,
    /// 测试记录 token 数；缺失为 `None`。
    tokens: Option<u64>,
}

/// 测试结果的可比较形态。
#[derive(Debug, PartialEq)]
enum Seen {
    /// 成功解析出记录。
    Parsed(Usage),
    /// 空行。
    Empty,
    /// 不是合法 JSON。
    Malformed,
    /// 超长且无法解析的行。
    OversizedMalformed,
}

/// 用给定预算读取全部完整行。
fn read(input: &[u8], max_line_bytes: usize) -> (Vec<Seen>, JsonlLineProgress) {
    let mut seen = Vec::new();
    let progress = read_jsonl_records::<Usage, _, _>(
        input,
        max_line_bytes,
        || false,
        |error| error,
        |record| {
            seen.push(match record {
                JsonlRecord::Parsed(usage) => Seen::Parsed(usage),
                JsonlRecord::Empty => Seen::Empty,
                JsonlRecord::Malformed => Seen::Malformed,
                JsonlRecord::OversizedMalformed => Seen::OversizedMalformed,
            });
            Ok(LineFlow::Continue)
        },
    )
    .expect("in-memory read succeeds");
    (seen, progress)
}

/// 构造测试用量记录。
fn usage(id: &str, tokens: u64) -> Seen {
    Seen::Parsed(Usage {
        id: Some(id.to_owned()),
        tokens: Some(tokens),
    })
}

/// 预算内的行照常解析，CRLF 与空行正确处理，末尾半行不提交。
#[test]
fn lines_within_budget_parse_and_tail_is_not_committed() {
    let (seen, progress) = read(
        b"{\"id\":\"a\",\"tokens\":1}\r\n\n{bad}\n{\"id\":\"b\"",
        1024,
    );
    assert_eq!(seen, vec![usage("a", 1), Seen::Empty, Seen::Malformed]);
    assert_eq!(progress.committed_bytes, 30);
    assert_eq!(progress.trailing_bytes, 9);
    assert!(!progress.discarding_oversized_line);
}

/// 超过行预算的用量行不再被丢弃：正文被跳过，白名单字段照常取回，后续行不受影响。
#[test]
fn oversized_line_keeps_whitelisted_usage() {
    let body = "x".repeat(64 * 1024);
    let input = format!(
        "{{\"content\":\"{body}\",\"id\":\"big\",\"tokens\":7}}\r\n{{\"id\":\"next\",\"tokens\":2}}\n"
    );
    let (seen, progress) = read(input.as_bytes(), 1024);
    assert_eq!(seen, vec![usage("big", 7), usage("next", 2)]);
    assert_eq!(progress.committed_bytes, input.len() as u64);
}

/// 超长且不是合法 JSON 的行单独上报，并完整跳到下一行。
#[test]
fn oversized_malformed_line_is_reported_and_skipped() {
    let input = format!(
        "{{\"id\":{}\n{{\"id\":\"ok\",\"tokens\":1}}\n",
        "9".repeat(4096)
    );
    let (seen, _) = read(input.as_bytes(), 1024);
    assert_eq!(seen, vec![Seen::OversizedMalformed, usage("ok", 1)]);
}

/// 未换行的超长尾行不提交，记录丢弃标记，下次从行首重读。
#[test]
fn unterminated_oversized_tail_is_not_committed() {
    let body = "x".repeat(4096);
    let input = format!("{{\"id\":\"a\",\"tokens\":1}}\n{{\"content\":\"{body}\",\"tokens\":3}}");
    let (seen, progress) = read(input.as_bytes(), 1024);
    assert_eq!(seen, vec![usage("a", 1)]);
    assert_eq!(progress.committed_bytes, 22);
    assert_eq!(progress.bytes_consumed, input.len() as u64);
    assert_eq!(progress.trailing_bytes, input.len() as u64 - 22);
    assert!(progress.discarding_oversized_line);
}

/// 超长行流式解析中途取消必须立即返回；`Interrupted` 会被 std/serde 无限重试而卡死。
#[test]
fn cancellation_mid_oversized_line_returns() {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let body = "x".repeat(4 * 1024 * 1024);
        let input = format!("{{\"content\":\"{body}\",\"tokens\":3}}\n");
        let checks = Arc::new(AtomicUsize::new(8));
        let progress = read_jsonl_records::<Usage, _, _>(
            input.as_bytes(),
            1024,
            || {
                // 取消回调只在解析线程内同步调用，读后写无竞争；计数归零后持续报告取消。
                let left = checks.load(Ordering::SeqCst);
                if left == 0 {
                    return true;
                }
                checks.store(left - 1, Ordering::SeqCst);
                false
            },
            |error| error,
            |_| Ok(LineFlow::Continue),
        )
        .expect("cancellation is not an I/O failure");
        let _ = done.send(progress.cancelled && progress.committed_bytes == 0);
    });
    let cancelled = finished
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("cancelled oversized parse returns instead of retrying forever");
    assert!(cancelled);
}
