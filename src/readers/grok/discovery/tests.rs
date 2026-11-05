//! Grok 快速发现收尾语义：预算耗尽时尚未检查的已登记根既不确认也不判定无效。

use std::fs;

use crate::CoverageState;
use tempfile::tempdir;

use super::*;

/// 构造一个已启用的已登记根。
fn registered(id: &str, path: std::path::PathBuf) -> RegisteredRoot {
    RegisteredRoot {
        root_id: Some(id.to_owned()),
        path,
        alias: id.to_owned(),
        enabled: true,
    }
}

/// 预算耗尽中止后，没来得及检查的已登记根必须标为未确认，与 Claude 的收尾语义一致。
#[test]
fn budget_exhaustion_marks_uninspected_registered_roots_unconfirmed() {
    let temp = tempdir().expect("temporary roots are available");
    let exhausting = temp.path().join("exhausting");
    let untouched = temp.path().join("untouched");
    // 第一个根的目录层级足以耗尽只允许 2 个目录的预算，第二个根因此不会被检查。
    fs::create_dir_all(exhausting.join("sessions/a/b/c")).expect("fixture is created");
    fs::create_dir_all(untouched.join("sessions/x")).expect("fixture is created");
    let inputs = GrokDiscoveryInputs {
        home_dir: None,
        grok_home: None,
        registered_roots: vec![
            registered("root-exhausting", exhausting),
            registered("root-untouched", untouched),
        ],
    };
    let options = FullDiscoveryOptions {
        max_signature_directories: 2,
        ..FullDiscoveryOptions::default()
    };

    let result = inspect_candidates(
        quick_candidates(&inputs),
        &CancellationToken::new(),
        &options,
    );

    assert_eq!(result.coverage.state, CoverageState::Partial);
    assert!(result.roots.is_empty());
    assert!(result.confirmed_invalid_root_ids.is_empty());
    assert_eq!(
        result.unconfirmed_root_ids,
        vec!["root-exhausting".to_owned(), "root-untouched".to_owned()]
    );
}
