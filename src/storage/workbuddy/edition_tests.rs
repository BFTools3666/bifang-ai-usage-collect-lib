//! 隔离卷夹具覆盖默认发现、全盘发现、持久 registry 与版本边界。

use super::*;
use crate::readers::{LocalVolumeRoots, workbuddy::discovery::discover_edition_sources_on_volumes};
use crate::{CoverageState, ScanCancellation};
use std::{fs, path::PathBuf};

/// 建立具有相同会话标识的独立来源，计量仍按来源根分别累计。
fn write_root(volume: &Path, suffix: &str, edition: WorkbuddyEdition, input: i64) -> PathBuf {
    let path = volume.join(suffix).join(edition.directory_name());
    let projects = path.join("projects/project");
    fs::create_dir_all(&projects).expect("create approved source layout");
    let event = super::super::tests::event("same-message", 1000, input);
    fs::write(projects.join("same-session.jsonl"), event).expect("put usage fixture");
    path
}

/// 快速仅命中内置目录，全盘保留三个同版来源和不同别名，快速复扫沿用登记。
#[tokio::test]
async fn workbuddy_quick_and_full_have_distinct_discovery_scope() {
    let folder = tempfile::tempdir().expect("isolate workbuddy volumes");
    let volumes_base = folder.path().canonicalize().expect("resolve isolated dir");
    let app = tempfile::tempdir().expect("isolate workbuddy index");
    let local = volumes_base.join("first-volume");
    let other = volumes_base.join("second-volume");
    let excluded = other.join("exclude");
    let edition = WorkbuddyEdition::Domestic;
    let built_in = write_root(&local, "home", edition, 10);
    let first_extra = write_root(&local, "external", edition, 20);
    let second_extra = write_root(&other, "external", edition, 30);
    write_root(&excluded, "network", edition, 100);
    let disabled = write_root(&other, "external", WorkbuddyEdition::International, 100);
    write_root(&disabled, "private-tree", edition, 200);
    let home = built_in.parent().expect("user home");
    let mode = WorkbuddyRefreshMode::Incremental;
    let mut index = WorkbuddyIndex::open_edition(app.path(), edition)
        .await
        .expect("open edition");
    index
        .refresh_edition_from_user_home(edition, home, 0, mode, ScanCancellation::new())
        .await
        .expect("quick");
    assert_eq!(
        index
            .sources()
            .await
            .expect("quick sources")
            .into_iter()
            .map(|root| root.path)
            .collect::<Vec<_>>(),
        [built_in.clone()]
    );
    let initial = index
        .load_inputs()
        .await
        .expect("quick snapshot")
        .expect("published quick snapshot");
    assert_eq!(initial.records.len(), 1);
    assert_eq!(initial.records[0].total_tokens, 13);

    let default = edition_source(built_in.clone(), true);
    let volumes = LocalVolumeRoots {
        search_roots: vec![local, other],
        excluded_roots: vec![excluded],
        ..LocalVolumeRoots::default()
    };
    let outcome = index
        .refresh_edition_with_discovery(edition, 0, mode, ScanCancellation::new(), move |cancel| {
            discover_edition_sources_on_volumes(edition, volumes, Some(&default), cancel)
        })
        .await
        .expect("full");
    assert_eq!(outcome.unchanged_files, 1);
    assert_eq!(outcome.roots_discovered, 3);
    assert_eq!(outcome.files_scanned, 3);
    let full = index
        .load_inputs()
        .await
        .expect("full snapshot")
        .expect("published full snapshot");
    assert_eq!(full.coverage.state, CoverageState::Complete);
    assert_eq!(full.coverage.roots_discovered, 3);
    assert_eq!(full.root_aliases.len(), 3);
    assert_eq!(
        full.records
            .iter()
            .map(|record| record.total_tokens)
            .sum::<i64>(),
        69
    );
    let rooted_paths = index
        .sources()
        .await
        .expect("cached sources")
        .into_iter()
        .map(|root| root.path)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        rooted_paths,
        [built_in.clone(), first_extra, second_extra]
            .into_iter()
            .collect()
    );
    assert_eq!(
        full.root_aliases
            .values()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3
    );
    let origin = index
        .sources_with_discovery()
        .await
        .expect("read real origins");
    assert_eq!(origin.iter().filter(|(_, built_in)| *built_in).count(), 1);
    assert_eq!(origin[0].0.path, built_in);
    drop(index);

    let mut reopened = WorkbuddyIndex::open_edition(app.path(), edition)
        .await
        .expect("reopen edition");
    let again = reopened
        .refresh_edition_from_user_home(edition, home, 0, mode, ScanCancellation::new())
        .await
        .expect("quick with persisted full-scan results");
    assert_eq!(again.unchanged_files, 3);
    assert_eq!(again.roots_discovered, 1);
    let reopened_sources = reopened.sources().await.expect("persisted roots");
    assert_eq!(reopened_sources.len(), 3);
    let foreign = WorkbuddyIndex::open_edition(app.path(), WorkbuddyEdition::International)
        .await
        .expect("separate edition");
    assert!(
        foreign
            .sources()
            .await
            .expect("no sources for disabled edition")
            .is_empty()
    );
}

/// 畸形内容的来源仍出现在根表，保存的计量与旧共享库不会被覆盖。
#[tokio::test]
async fn workbuddy_failed_index_keeps_independent_discoveries_and_previous_publication() {
    let temp = tempfile::tempdir().expect("isolated source and index");
    let parent = temp.path().canonicalize().expect("canonical fixture");
    let app = parent.join("index");
    let edition = WorkbuddyEdition::International;
    let root = write_root(&parent, "home", edition, 10);
    let home = root.parent().expect("fixture home");
    let mut index = WorkbuddyIndex::open_edition(&app, edition)
        .await
        .expect("index");
    let mode = WorkbuddyRefreshMode::Incremental;
    index
        .refresh_edition_from_user_home(edition, home, 0, mode, ScanCancellation::new())
        .await
        .expect("initial snapshot");
    let invalid = write_root(&parent, "other", edition, 20);
    fs::write(
        invalid.join("projects/project/same-session.jsonl"),
        b"{invalid}\n",
    )
    .expect("bad source");
    let invalid = edition_source(invalid, false);
    assert!(
        index
            .refresh_edition_with_discovery(edition, 0, mode, ScanCancellation::new(), move |_| Ok(
                vec![invalid]
            ),)
            .await
            .is_err()
    );
    assert_eq!(index.sources().await.expect("registered roots").len(), 2);
    let snapshot = index.load_inputs().await.expect("read").expect("retained");
    assert_eq!(snapshot.coverage.state, CoverageState::Partial);
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].total_tokens, 13);

    let legacy = WorkbuddyIndex::open(&app, Path::new("workbuddy.db"))
        .await
        .expect("legacy remains independent");
    assert!(
        legacy
            .load_inputs()
            .await
            .expect("legacy unchanged")
            .is_none()
    );
    assert!(
        WorkbuddyIndex::open(&app, Path::new(edition.database_file_name()))
            .await
            .is_err()
    );
    assert!(
        index
            .refresh_edition_from_user_home(
                WorkbuddyEdition::Domestic,
                home,
                0,
                mode,
                ScanCancellation::new()
            )
            .await
            .is_err()
    );
}
