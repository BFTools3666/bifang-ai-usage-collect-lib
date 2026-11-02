use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseConnection};

use super::super::LocalErrorKind;
use super::shared::statement;
use super::*;
use crate::{Confidence, TokenUsage, UsageCall};

/// 使用当前完整 schema 隔离批次事务；本组无来源夹具，显式关闭外键检查。
async fn test_connection() -> DatabaseConnection {
    let mut options = ConnectOptions::new("sqlite::memory:");
    options
        .max_connections(1)
        .map_sqlx_sqlite_opts(|sqlite| sqlite.foreign_keys(false));
    let connection = Database::connect(options)
        .await
        .expect("in-memory database opens");
    crate::storage::schema::ensure_schema(&connection, None)
        .await
        .expect("current statistics schema initializes");
    connection
}

/// 构造不含正文或真实路径的规范化调用。
fn call(
    logical_call_id: &str,
    thread_key: &str,
    confidence: Confidence,
    input: u64,
    cached: Option<u64>,
    cache_write: Option<u64>,
    output: u64,
) -> UsageCall {
    UsageCall {
        logical_call_id: logical_call_id.to_owned(),
        occurred_at_epoch_ms: 1_000,
        model: Some("fixture-model".to_owned()),
        reasoning_effort: None,
        project_key: Some("fixture-project".to_owned()),
        thread_key: thread_key.to_owned(),
        project_label: None,
        thread_label: None,
        usage: TokenUsage::new_with_availability(
            input,
            cached,
            cache_write,
            output,
            Some(0),
            Some(input + output),
        )
        .expect("fixture usage is valid"),
        adapter_consistency_key: None,
        confidence,
        provenance: Vec::new(),
    }
}

/// 构造显式总量已知、输入与输出可用性独立变化的规范调用。
fn availability_call(
    logical_call_id: &str,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
) -> UsageCall {
    UsageCall {
        logical_call_id: logical_call_id.to_owned(),
        occurred_at_epoch_ms: 1_000,
        model: Some("fixture-model".to_owned()),
        reasoning_effort: None,
        project_key: None,
        thread_key: format!("thread-{logical_call_id}"),
        project_label: None,
        thread_label: None,
        usage: TokenUsage::new_with_component_availability(
            input_tokens,
            None,
            None,
            output_tokens,
            None,
            Some(10),
        )
        .expect("availability fixture is valid"),
        adapter_consistency_key: None,
        confidence: Confidence::Exact,
        provenance: Vec::new(),
    }
}

/// 在独立事务中插入 Codex 测试批次，模拟并发提交边界。
async fn insert_batch_in_own_transaction(
    connection: &DatabaseConnection,
    source_id: &str,
    generation: u64,
    batch: &mut Vec<UsageCall>,
) -> Result<u64, LocalError> {
    let transaction = connection.begin().await.into_local_result()?;
    let added = insert_usage_batch(&transaction, source_id, generation, batch)
        .await
        .into_local_result()?;
    transaction.commit().await.into_local_result()?;
    Ok(added)
}

/// 在独立事务中插入 Claude 测试批次，验证物理库隔离后的提交语义。
async fn insert_claude_batch_in_own_transaction(
    connection: &DatabaseConnection,
    source_id: &str,
    generation: u64,
    batch: &mut Vec<UsageCall>,
) -> Result<ClaudeBatchOutcome, LocalError> {
    let transaction = connection.begin().await.into_local_result()?;
    let outcome = insert_claude_usage_batch(&transaction, source_id, generation, batch)
        .await
        .into_local_result()?;
    transaction.commit().await.into_local_result()?;
    Ok(outcome)
}

/// 验证批次写入仍保持冲突更新与 exact 优先规则。
#[tokio::test]
async fn batch_preserves_exact_and_derived_rules() {
    let connection = test_connection().await;
    let mut empty = Vec::new();
    assert_eq!(
        insert_batch_in_own_transaction(&connection, "source", 1, &mut empty)
            .await
            .expect("empty batch is accepted"),
        0
    );

    let mut derived = vec![call(
        "derived",
        "thread",
        Confidence::Derived,
        10,
        Some(2),
        Some(1),
        1,
    )];
    assert_eq!(
        insert_batch_in_own_transaction(&connection, "source", 1, &mut derived)
            .await
            .expect("derived call is inserted"),
        1
    );

    let mut refreshed = vec![call(
        "derived",
        "thread",
        Confidence::Derived,
        12,
        Some(3),
        Some(1),
        2,
    )];
    assert_eq!(
        insert_batch_in_own_transaction(&connection, "source", 1, &mut refreshed)
            .await
            .expect("duplicate logical call is refreshed"),
        0
    );
    let refreshed_row = connection
        .query_one_raw(statement(
            "SELECT input_tokens FROM usage_calls WHERE logical_call_id = 'derived'",
            vec![],
        ))
        .await
        .expect("refreshed row query succeeds")
        .expect("refreshed row exists");
    assert_eq!(refreshed_row.try_get_by_index::<i64>(0).unwrap(), 12);

    let mut exact = vec![call(
        "exact",
        "thread",
        Confidence::Exact,
        20,
        Some(4),
        Some(2),
        3,
    )];
    assert_eq!(
        insert_batch_in_own_transaction(&connection, "source", 1, &mut exact)
            .await
            .expect("exact call replaces derived thread"),
        1
    );
    let mut late_derived = vec![call(
        "late-derived",
        "thread",
        Confidence::Derived,
        30,
        Some(5),
        Some(2),
        4,
    )];
    assert_eq!(
        insert_batch_in_own_transaction(&connection, "source", 1, &mut late_derived)
            .await
            .expect("late derived call is skipped"),
        0
    );
    let summary_row = connection
        .query_one_raw(statement(
            "SELECT COUNT(*), MIN(confidence) FROM usage_calls",
            vec![],
        ))
        .await
        .expect("canonical rows query succeeds")
        .expect("canonical rows are readable");
    assert_eq!(summary_row.try_get_by_index::<i64>(0).unwrap(), 1);
    assert_eq!(
        summary_row.try_get_by_index::<String>(1).unwrap(),
        "exact".to_owned()
    );
}

/// 验证 SQLite 内部的零占位不会抹掉输入/输出的独立可用性；四种组合跨写入和
/// 行解码后都必须保留 `Some(0)` 与 `None` 的区别。
#[tokio::test]
async fn round_trips_all_primary_token_availability_combinations() {
    let connection = test_connection().await;
    let mut batch = vec![
        availability_call("known-known", Some(0), Some(0)),
        availability_call("known-unknown", Some(0), None),
        availability_call("unknown-known", None, Some(0)),
        availability_call("unknown-unknown", None, None),
    ];
    assert_eq!(
        insert_batch_in_own_transaction(&connection, "source", 1, &mut batch)
            .await
            .expect("availability batch is inserted"),
        4
    );

    let rows = connection
        .query_all_raw(statement(
            "SELECT logical_call_id, occurred_at_epoch_ms, model,
                    reasoning_effort, project_key, thread_key,
                    project_label, thread_label,
                    input_tokens, cached_input_tokens, cache_write_input_tokens,
                    output_tokens, reasoning_output_tokens, total_tokens,
                    total_is_derived, confidence,
                    input_available, cached_input_available,
                    output_available, reasoning_output_available,
                    adapter_consistency_key,
                    source_id, 'root', 'fixture.jsonl', 0
             FROM usage_calls
             ORDER BY logical_call_id",
            vec![],
        ))
        .await
        .expect("availability rows are readable");
    let calls = rows
        .iter()
        .map(row_to_usage_call)
        .collect::<Result<Vec<_>, _>>()
        .expect("availability rows decode");

    let observed = calls
        .iter()
        .map(|call| {
            (
                call.logical_call_id.as_str(),
                call.usage.input_tokens,
                call.usage.output_tokens,
                call.usage.total_tokens,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        observed,
        vec![
            ("known-known", Some(0), Some(0), 10),
            ("known-unknown", Some(0), None, 10),
            ("unknown-known", None, Some(0), 10),
            ("unknown-unknown", None, None, 10),
        ]
    );
}

/// 推导总量形成后主分项被保守降级时，SQLite 往返必须保留可信总量与来源标记。
#[tokio::test]
async fn round_trips_derived_total_with_degraded_primary_component() {
    let connection = test_connection().await;
    let mut derived = availability_call("derived-partial", None, Some(0));
    derived.usage.total_is_derived = true;
    let mut batch = vec![derived.clone()];
    assert_eq!(
        insert_batch_in_own_transaction(&connection, "source", 1, &mut batch)
            .await
            .expect("derived partial call is inserted"),
        1
    );

    let row = connection
        .query_one_raw(statement(
            "SELECT logical_call_id, occurred_at_epoch_ms, model,
                    reasoning_effort, project_key, thread_key,
                    project_label, thread_label,
                    input_tokens, cached_input_tokens, cache_write_input_tokens,
                    output_tokens, reasoning_output_tokens, total_tokens,
                    total_is_derived, confidence,
                    input_available, cached_input_available,
                    output_available, reasoning_output_available,
                    adapter_consistency_key,
                    source_id, 'root', 'fixture.jsonl', 0
             FROM usage_calls
             WHERE logical_call_id = 'derived-partial'",
            vec![],
        ))
        .await
        .expect("derived partial row query succeeds")
        .expect("derived partial row exists");
    let restored = row_to_usage_call(&row).expect("derived partial row decodes");

    assert_eq!(restored.usage, derived.usage);
}

/// 可用性位为 false 只表示上游未提供字段，不能掩盖 SQLite 占位列自身的损坏。
#[tokio::test]
async fn row_decode_rejects_negative_unavailable_primary_placeholders() {
    for column in ["input_tokens", "output_tokens"] {
        let connection = test_connection().await;
        let mut batch = vec![availability_call("corrupt-placeholder", None, None)];
        insert_batch_in_own_transaction(&connection, "source", 1, &mut batch)
            .await
            .expect("availability call is inserted");
        connection
            .execute_unprepared(&format!(
                "UPDATE usage_calls SET {column} = -1 WHERE logical_call_id = 'corrupt-placeholder'"
            ))
            .await
            .expect("synthetic corruption is written");

        let row = connection
            .query_one_raw(statement(
                "SELECT logical_call_id, occurred_at_epoch_ms, model,
                        reasoning_effort, project_key, thread_key,
                        project_label, thread_label,
                        input_tokens, cached_input_tokens, cache_write_input_tokens,
                        output_tokens, reasoning_output_tokens, total_tokens,
                        total_is_derived, confidence,
                        input_available, cached_input_available,
                        output_available, reasoning_output_available,
                        adapter_consistency_key,
                        source_id, 'root', 'fixture.jsonl', 0
                 FROM usage_calls
                 WHERE logical_call_id = 'corrupt-placeholder'",
                vec![],
            ))
            .await
            .expect("corrupt row query completes")
            .expect("corrupt row exists");

        assert!(
            row_to_usage_call(&row).is_err(),
            "negative {column} must fail closed even when its availability flag is false"
        );
    }
}

/// Claude 单调预取也必须先验证不可用字段的底层占位，避免损坏行参与比较。
#[tokio::test]
async fn claude_prefetch_rejects_negative_unavailable_primary_placeholders() {
    for column in ["input_tokens", "output_tokens"] {
        let connection = test_connection().await;
        let mut initial = vec![availability_call("claude-corrupt", None, None)];
        insert_batch_in_own_transaction(&connection, "source", 1, &mut initial)
            .await
            .expect("availability call is inserted");
        connection
            .execute_unprepared(&format!(
                "UPDATE usage_calls SET {column} = -1 WHERE logical_call_id = 'claude-corrupt'"
            ))
            .await
            .expect("synthetic corruption is written");

        let mut replacement = vec![availability_call("claude-corrupt", Some(1), Some(1))];
        assert!(
            insert_claude_batch_in_own_transaction(&connection, "source", 1, &mut replacement,)
                .await
                .is_err(),
            "negative {column} must stop Claude prefetch even when its availability flag is false"
        );
    }
}

/// 验证 Claude 批次只接受逐字段单调观察并保留最后可信事实。
#[tokio::test]
async fn claude_batch_rejects_regressions() {
    let connection = test_connection().await;
    let mut initial = vec![call(
        "message",
        "thread",
        Confidence::Exact,
        10,
        Some(2),
        Some(1),
        1,
    )];
    let outcome = insert_claude_batch_in_own_transaction(&connection, "source", 1, &mut initial)
        .await
        .expect("initial Claude call is inserted");
    assert_eq!((outcome.added, outcome.regressed), (1, 0));

    let mut monotonic = vec![call(
        "message",
        "thread",
        Confidence::Exact,
        12,
        Some(3),
        Some(1),
        2,
    )];
    let outcome = insert_claude_batch_in_own_transaction(&connection, "source", 1, &mut monotonic)
        .await
        .expect("monotonic Claude call is updated");
    assert_eq!((outcome.added, outcome.regressed), (0, 0));

    let mut regressed = vec![
        call(
            "message",
            "thread",
            Confidence::Exact,
            11,
            Some(3),
            Some(1),
            1,
        ),
        call("message", "thread", Confidence::Exact, 13, None, Some(1), 3),
    ];
    let outcome = insert_claude_batch_in_own_transaction(&connection, "source", 1, &mut regressed)
        .await
        .expect("regressions are isolated warnings");
    assert_eq!((outcome.added, outcome.regressed), (0, 2));
    let trusted_row = connection
        .query_one_raw(statement(
            "SELECT input_tokens, output_tokens FROM usage_calls",
            vec![],
        ))
        .await
        .expect("trusted row query succeeds")
        .expect("trusted row remains readable");
    assert_eq!(trusted_row.try_get_by_index::<i64>(0).unwrap(), 12);
    assert_eq!(trusted_row.try_get_by_index::<i64>(1).unwrap(), 2);
}

/// 验证转换溢出与中途失败不会留下部分提交。
#[tokio::test]
async fn batch_keeps_overflow_classification_and_rolls_back() {
    let connection = test_connection().await;
    let mut overflow = vec![call(
        "overflow",
        "thread",
        Confidence::Exact,
        1,
        Some(0),
        Some(0),
        1,
    )];
    let error = insert_batch_in_own_transaction(&connection, "source", u64::MAX, &mut overflow)
        .await
        .expect_err("generation outside SQLite range is rejected");
    assert_eq!(error.kind(), LocalErrorKind::Overflow);

    connection
        .execute_unprepared(
            "CREATE TRIGGER reject_second_call
               BEFORE INSERT ON usage_calls
               WHEN NEW.logical_call_id = 'reject'
             BEGIN
               SELECT RAISE(ABORT, 'fixture failure');
             END;",
        )
        .await
        .expect("failure trigger is installed");
    let mut batch = vec![
        call(
            "first",
            "thread-a",
            Confidence::Exact,
            1,
            Some(0),
            Some(0),
            1,
        ),
        call(
            "reject",
            "thread-b",
            Confidence::Exact,
            1,
            Some(0),
            Some(0),
            1,
        ),
    ];
    let error = insert_batch_in_own_transaction(&connection, "source", 1, &mut batch)
        .await
        .expect_err("trigger aborts the transaction");
    assert_eq!(error.kind(), LocalErrorKind::Database);
    let count_row = connection
        .query_one_raw(statement("SELECT COUNT(*) FROM usage_calls", vec![]))
        .await
        .expect("row count query succeeds")
        .expect("row count is readable");
    assert_eq!(count_row.try_get_by_index::<i64>(0).unwrap(), 0);
}

/// 验证一致性检查点严格按来源和 generation 隔离，并恢复最大顺序键。
#[tokio::test]
async fn latest_adapter_consistency_key_is_source_and_generation_scoped() {
    let connection = test_connection().await;
    let mut older = call(
        "older",
        "thread-a",
        Confidence::Exact,
        10,
        Some(2),
        Some(0),
        1,
    );
    older.adapter_consistency_key =
        Some("codex-v6:00000000000000000001:10:2:0:1:0:11:0".to_owned());
    let mut newer = call(
        "newer",
        "thread-b",
        Confidence::Exact,
        20,
        Some(4),
        Some(0),
        2,
    );
    let expected = "codex-v6:00000000000000000002:20:4:0:2:0:22:0".to_owned();
    newer.adapter_consistency_key = Some(expected.clone());
    let mut batch = vec![newer, older];
    insert_batch_in_own_transaction(&connection, "source", 3, &mut batch)
        .await
        .expect("checkpoint calls are inserted");
    let index = LocalIndex {
        connection,
        active_scan_transaction: None,
        active_scan_record: None,
        defer_scan_publication: false,
        staged_scan_status: None,
        staged_scan_finished_at_epoch_ms: None,
        parser_version: 6,
        database_path: std::path::PathBuf::new(),
        client: None,
    };

    assert_eq!(
        index
            .latest_adapter_consistency_key("source", 3)
            .await
            .expect("latest checkpoint is readable"),
        Some(expected)
    );
    assert_eq!(
        index
            .latest_adapter_consistency_key("source", 4)
            .await
            .expect("other generation is readable"),
        None
    );
    assert_eq!(
        index
            .latest_adapter_consistency_key("other", 3)
            .await
            .expect("other source is readable"),
        None
    );
}
