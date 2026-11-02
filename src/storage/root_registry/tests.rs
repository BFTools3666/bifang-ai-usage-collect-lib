use super::*;
use crate::CoverageState;
use sea_orm::ConnectionTrait;

/// 验证周期任务只认领当时已排队的根，不会完成之后新增的候选。
#[tokio::test]
async fn background_claim_is_bounded_to_roots_present_at_start() {
    let temp = tempfile::tempdir().expect("temp app data is available");
    let root = temp.path().join("candidate-root");
    std::fs::create_dir_all(&root).expect("candidate root exists");
    let mut index = LocalIndex::open_in_app_data(temp.path(), 5)
        .await
        .expect("index opens");
    assert!(
        index
            .register_confirmed_root_fields(
                "root-candidate",
                &root,
                "candidate",
                DiscoveryMethod::FullDevice,
            )
            .await
            .expect("candidate registers")
    );
    assert!(
        index
            .known_roots()
            .await
            .expect("ready roots load")
            .is_empty()
    );

    let root_ids = index
        .claim_background_index_roots()
        .await
        .expect("background roots are claimed");
    assert_eq!(root_ids, ["root-candidate"]);
    assert_eq!(
        index
            .known_roots()
            .await
            .expect("indexing roots load")
            .len(),
        1
    );
    let later_root = temp.path().join("later-root");
    std::fs::create_dir_all(&later_root).expect("later root exists");
    index
        .register_confirmed_root_fields(
            "root-later",
            &later_root,
            "later",
            DiscoveryMethod::FullDevice,
        )
        .await
        .expect("later candidate registers");
    index
        .finish_background_index_roots(&root_ids)
        .await
        .expect("empty update finalizes");
    let records = index.usage_snapshot().await.expect("records load").roots;
    assert_eq!(
        records
            .iter()
            .find(|root| root.root_id == "root-candidate")
            .unwrap()
            .activation_state,
        RootActivationState::ValidationFailed
    );
    assert_eq!(
        records
            .iter()
            .find(|root| root.root_id == "root-later")
            .unwrap()
            .activation_state,
        RootActivationState::ConfirmedUnindexed
    );
}

/// 当前 parser 已完整读取合法来源时，即使没有任何调用也必须进入 ready。
#[tokio::test]
async fn ready_source_without_calls_does_not_fail_candidate_validation() {
    let temp = tempfile::tempdir().expect("temp app data is available");
    let root = temp.path().join("ready-no-calls");
    std::fs::create_dir_all(&root).expect("fixture root exists");
    let mut index = LocalIndex::open_in_app_data(temp.path(), 5)
        .await
        .expect("index opens");
    index
        .register_confirmed_root_fields(
            "root-ready-no-calls",
            &root,
            "ready no calls",
            DiscoveryMethod::FullDevice,
        )
        .await
        .expect("candidate registers");
    let claimed = index
        .claim_background_index_roots()
        .await
        .expect("candidate is claimed");
    index
        .commit_source_file_checkpoint(
            "source-ready-no-calls",
            "root-ready-no-calls",
            "sessions/rollout-ready.jsonl",
            "fixture-file",
            false,
            0,
            0,
            0,
            0,
            false,
            5,
            1,
            &crate::SourceParseCheckpoint {
                thread_key: "thread-ready".to_owned(),
                project_key: None,
                project_label: None,
                thread_label: None,
                model: None,
                reasoning_effort: None,
                call_sequence: 0,
                adapter_state: None,
            },
            None,
        )
        .await
        .expect("ready source checkpoint commits");
    index
        .finish_background_index_roots(&claimed)
        .await
        .expect("background validation finishes");

    let root = index
        .usage_snapshot()
        .await
        .expect("snapshot loads")
        .roots
        .into_iter()
        .find(|root| root.root_id == "root-ready-no-calls")
        .expect("registered root remains visible");
    assert_eq!(root.activation_state, RootActivationState::Ready);
}

/// 本轮发现根可立即进入索引，但停用、未知或并发新增的其他根不会被顺带认领。
#[tokio::test]
async fn current_scan_claim_is_bounded_to_discovered_root_ids() {
    let temp = tempfile::tempdir().expect("temp app data is available");
    let mut index = LocalIndex::open_in_app_data(temp.path(), 5)
        .await
        .expect("index opens");
    for root_id in ["root-recovered", "root-new", "root-disabled"] {
        let path = temp.path().join(root_id);
        std::fs::create_dir_all(&path).expect("fixture root exists");
        index
            .register_confirmed_root_fields(root_id, &path, root_id, DiscoveryMethod::Environment)
            .await
            .expect("fixture root registers");
    }
    let first_claim = index
        .claim_background_index_roots()
        .await
        .expect("initial roots are claimed");
    index
        .finish_background_index_roots(&first_claim)
        .await
        .expect("empty initial roots become validation failed");
    index
        .set_root_enabled("root-disabled", false)
        .await
        .expect("disabled fixture is updated");

    let claimed = index
        .claim_discovered_roots_for_current_scan(&[
            "root-recovered".to_owned(),
            "root-new".to_owned(),
            "root-recovered".to_owned(),
            "root-disabled".to_owned(),
            "root-unknown".to_owned(),
        ])
        .await
        .expect("exact discovered roots are claimed");

    assert_eq!(claimed, ["root-new", "root-recovered"]);
    let records = index.list_sources().await.expect("records load");
    assert!(records.iter().any(|root| {
        root.root_id == "root-recovered" && root.activation_state == RootActivationState::Indexing
    }));
    assert!(records.iter().any(|root| {
        root.root_id == "root-new" && root.activation_state == RootActivationState::Indexing
    }));
    assert!(records.iter().any(|root| {
        root.root_id == "root-disabled"
            && !root.enabled
            && root.activation_state == RootActivationState::ValidationFailed
    }));
}

/// 同一物理根再次登记必须幂等：第二次返回未新增，列表仍只有一行。
#[tokio::test]
async fn register_confirmed_root_fields_deduplicates_the_same_root_id() {
    let temp = tempfile::tempdir().expect("temp app data is available");
    let root = temp.path().join("same-root");
    std::fs::create_dir_all(&root).expect("same root exists");
    let mut index = LocalIndex::open_in_app_data(temp.path(), 5)
        .await
        .expect("index opens");
    assert!(
        index
            .register_confirmed_root_fields(
                "root-same",
                &root,
                "same",
                DiscoveryMethod::MetadataDiscovery,
            )
            .await
            .expect("first register inserts")
    );
    assert!(
        !index
            .register_confirmed_root_fields(
                "root-same",
                &root,
                "same-again",
                DiscoveryMethod::MetadataDiscovery,
            )
            .await
            .expect("second register is idempotent")
    );
    let records = index.all_roots().await.expect("registered roots load");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].root_id.as_deref(), Some("root-same"));
}

/// 第二步 Partial 更新失败时，第一步根删除必须随同一事务回滚且重开仍一致。
#[tokio::test]
async fn known_root_reconciliation_rolls_back_remove_when_partial_update_fails() {
    let temp = tempfile::tempdir().expect("temp app data is available");
    let remove_path = temp.path().join("remove-root");
    let partial_path = temp.path().join("partial-root");
    std::fs::create_dir_all(&remove_path).expect("remove fixture root exists");
    std::fs::create_dir_all(&partial_path).expect("partial fixture root exists");
    let mut index = LocalIndex::open_in_app_data(temp.path(), 5)
        .await
        .expect("index opens");
    index
        .register_root_fields(
            "root-remove",
            &remove_path,
            "remove",
            DiscoveryMethod::Registered,
        )
        .await
        .expect("remove fixture registers");
    index
        .register_root_fields(
            "root-partial",
            &partial_path,
            "partial",
            DiscoveryMethod::Registered,
        )
        .await
        .expect("partial fixture registers");
    for root_id in ["root-remove", "root-partial"] {
        index
            .record_root_coverage(root_id, CoverageState::Complete)
            .await
            .expect("initial coverage is stored");
    }
    index
        .connection
        .execute_unprepared(
            "CREATE TRIGGER reject_partial_reconciliation
               BEFORE UPDATE OF last_coverage_state ON source_roots
               WHEN OLD.root_id = 'root-partial' AND NEW.last_coverage_state = 'partial'
             BEGIN
               SELECT RAISE(ABORT, 'fixture failure');
             END;",
        )
        .await
        .expect("failure trigger is installed");

    let error = index
        .reconcile_known_roots(&["root-remove".to_owned()], &["root-partial".to_owned()])
        .await
        .expect_err("second reconciliation statement fails");
    assert_eq!(error.kind(), LocalErrorKind::Database);
    drop(index);

    let reopened = LocalIndex::open_in_app_data(temp.path(), 5)
        .await
        .expect("index reopens after rollback");
    let records = reopened.list_sources().await.expect("records reload");
    let removed = records
        .iter()
        .find(|root| root.root_id == "root-remove")
        .expect("first-step deletion was rolled back");
    let partial = records
        .iter()
        .find(|root| root.root_id == "root-partial")
        .expect("partial root remains registered");
    assert_eq!(removed.last_coverage, Some(CoverageState::Complete));
    assert_eq!(partial.last_coverage, Some(CoverageState::Complete));
}
