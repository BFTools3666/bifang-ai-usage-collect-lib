//! GLM rollout 根签名与快速发现的行为回归。

use std::fs;
use std::path::PathBuf;

use super::super::super::CancellationToken;
use super::super::discovery::discover_glm_quick;
use super::{GlmDiscoveryInputs, Inspection, SignatureBudget, default_rollout_dir, inspect_root};

/// 写入一个带可信用量事件的会话文件。
fn write_model_io_file(dir: &std::path::Path, name: &str) {
    fs::write(
        dir.join(name),
        concat!(
            "{\"type\":\"model_io\",\"requestId\":\"r1\",\"startedAt\":\"2026-11-02T17:30:12.147Z\",",
            "\"model\":{\"modelId\":\"GLM-5.3-Flash\"},",
            "\"response\":{\"usage\":{\"inputTokens\":10,\"outputTokens\":5,\"totalTokens\":15}}}\n"
        ),
    )
    .expect("write fixture file");
}

/// 建立独立临时目录作为测试根。
fn temp_root(tag: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("glm-discovery-tests-{tag}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// 平铺 `model-io-sess_*.jsonl` 含可信用量即确认根；空目录与非匹配文件拒绝。
#[test]
fn signature_accepts_rollout_layout_with_credible_usage() {
    let root = temp_root("signature");
    write_model_io_file(&root, "model-io-sess_sess-1.jsonl");
    let cancellation = CancellationToken::default();
    let mut budget = SignatureBudget::new(
        &crate::readers::FullDiscoveryOptions::default(),
        &cancellation,
    );
    assert!(matches!(
        inspect_root(&root, &mut budget),
        Inspection::Found(_)
    ));

    let empty = temp_root("empty");
    fs::create_dir_all(&empty).expect("create empty dir");
    let mut budget = SignatureBudget::new(
        &crate::readers::FullDiscoveryOptions::default(),
        &cancellation,
    );
    assert_eq!(inspect_root(&empty, &mut budget), Inspection::Rejected);

    let unrelated = temp_root("unrelated");
    fs::write(unrelated.join("notes.txt"), "plain").expect("write plain file");
    let mut budget = SignatureBudget::new(
        &crate::readers::FullDiscoveryOptions::default(),
        &cancellation,
    );
    assert_eq!(inspect_root(&unrelated, &mut budget), Inspection::Rejected);

    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&empty);
    let _ = fs::remove_dir_all(&unrelated);
}

/// 快速发现命中主目录下固定 `.zcode/cli/rollout` 默认根，缺失时静默不产生候选。
#[test]
fn quick_discovery_hits_default_rollout_dir() {
    let home = temp_root("quick-home");
    let rollout = default_rollout_dir(&home);
    fs::create_dir_all(&rollout).expect("create rollout dir");
    write_model_io_file(&rollout, "model-io-sess_subagent_agent_2.jsonl");
    let cancellation = CancellationToken::default();
    let result = discover_glm_quick(
        &GlmDiscoveryInputs {
            home_dir: Some(home.clone()),
            registered_roots: Vec::new(),
        },
        &cancellation,
    );
    assert_eq!(result.roots.len(), 1);
    assert_eq!(result.roots[0].alias, "rollout");
    assert!(result.roots[0].root_id.starts_with("glm-root-"));

    let missing_home = temp_root("quick-missing");
    let result = discover_glm_quick(
        &GlmDiscoveryInputs {
            home_dir: Some(missing_home.clone()),
            registered_roots: Vec::new(),
        },
        &cancellation,
    );
    assert!(result.roots.is_empty());
    let _ = fs::remove_dir_all(&home);
    let _ = fs::remove_dir_all(&missing_home);
}

/// 超出签名单行内存预算的合法记录仍可流式发现，正文无需进入缓冲。
#[test]
fn signature_streams_oversized_body_and_enforces_total_byte_budget() {
    let root = tempfile::tempdir().expect("isolated rollout exists");
    let value = serde_json::json!({
        "type": "model_io", "request": {"body": "private".repeat(200_000)},
        "response": {"usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15}}
    });
    fs::write(
        root.path().join("model-io-sess_long.jsonl"),
        format!("{value}\n"),
    )
    .expect("oversized signed record is written");
    let cancellation = CancellationToken::default();
    let options = crate::readers::FullDiscoveryOptions {
        max_signature_bytes: 2 * 1024 * 1024,
        ..Default::default()
    };
    let mut budget = SignatureBudget::new(&options, &cancellation);
    assert!(matches!(
        inspect_root(root.path(), &mut budget),
        Inspection::Found(_)
    ));
    let options = crate::readers::FullDiscoveryOptions {
        max_signature_bytes: 64,
        ..Default::default()
    };
    let mut budget = SignatureBudget::new(&options, &cancellation);
    assert_eq!(
        inspect_root(root.path(), &mut budget),
        Inspection::BudgetExhausted
    );
}

/// 缓存超过全部输入的矛盾事件不能作为可信根签名。
#[test]
fn signature_rejects_inconsistent_token_components() {
    let root = tempfile::tempdir().expect("isolated rollout exists");
    fs::write(
        root.path().join("model-io-sess_invalid.jsonl"),
        concat!(
            "{\"type\":\"model_io\",\"response\":{\"usage\":{",
            "\"inputTokens\":10,\"cacheReadTokens\":11,\"outputTokens\":5,\"totalTokens\":15}}}\n"
        ),
    )
    .expect("inconsistent signature is written");
    let cancellation = CancellationToken::default();
    let mut budget = SignatureBudget::new(
        &crate::readers::FullDiscoveryOptions::default(),
        &cancellation,
    );
    assert_eq!(inspect_root(root.path(), &mut budget), Inspection::Rejected);
}

/// 全设备、快速登记与手动打开均拒绝私有子树伪装的模型 I/O 文件。
#[test]
fn private_zcode_subtrees_never_become_usage_roots() {
    let temp = tempfile::tempdir().expect("isolated fake home exists");
    let home = temp.path();
    let approved = home.join(".zcode/cli/rollout");
    fs::create_dir_all(&approved).expect("approved rollout exists");
    write_model_io_file(&approved, "model-io-sess_real.jsonl");
    let private_roots: Vec<_> = [
        "credentials",
        "logs",
        "workspace/project/cli/rollout",
        "cli",
    ]
    .into_iter()
    .map(|relative| home.join(".zcode").join(relative))
    .collect();
    let cancellation = CancellationToken::default();
    for private in &private_roots {
        fs::create_dir_all(private).expect("private fixture directory exists");
        write_model_io_file(private, "model-io-sess_decoy.jsonl");
        let mut budget = SignatureBudget::new(
            &crate::readers::FullDiscoveryOptions::default(),
            &cancellation,
        );
        assert_eq!(inspect_root(private, &mut budget), Inspection::Rejected);
        let error = crate::input::open_glm_source(crate::input::SourceFileRequest {
            root_path: private.clone(),
            file_path: private.join("model-io-sess_decoy.jsonl"),
            root_id: "glm-root-0123456789abcdef".to_owned(),
        })
        .expect_err("private root must be rejected before opening content");
        assert_eq!(error.kind(), crate::LocalErrorKind::InvalidPath);
    }
    let quick = discover_glm_quick(
        &GlmDiscoveryInputs {
            home_dir: None,
            registered_roots: private_roots
                .iter()
                .enumerate()
                .map(|(index, path)| crate::RegisteredRoot {
                    root_id: Some(format!("private-{index}")),
                    path: path.clone(),
                    alias: "private decoy".to_owned(),
                    enabled: true,
                })
                .collect(),
        },
        &cancellation,
    );
    assert!(quick.roots.is_empty());
    let options = crate::readers::FullDiscoveryOptions {
        search_roots: vec![home.to_path_buf()],
        ..Default::default()
    };
    let full = super::discover_glm_full_device_with_progress(&options, &cancellation, |_| {});
    assert_eq!(full.roots.len(), 1);
    assert_eq!(
        full.roots[0].path,
        fs::canonicalize(&approved).expect("approved canonical path")
    );
}

/// `.zcode` 外显式授权的同布局根继续可用，不扩大私有子树例外。
#[test]
fn explicitly_authorized_external_rollout_still_opens() {
    let root = tempfile::tempdir().expect("isolated external root exists");
    write_model_io_file(root.path(), "model-io-sess_external.jsonl");
    let opened = crate::input::open_glm_source(crate::input::SourceFileRequest {
        root_path: root.path().to_path_buf(),
        file_path: root.path().join("model-io-sess_external.jsonl"),
        root_id: "glm-root-0123456789abcdef".to_owned(),
    })
    .expect("explicit external root with the approved flat layout opens");
    assert!(opened.metadata().observed_size > 0);
}
