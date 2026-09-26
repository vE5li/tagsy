//! Multi-daemon sync tests: several real `tagsyd` instances in one process,
//! linked over loopback, driven through their public API and their sync
//! directories, then checked for convergence.
//!
//! See `harness.rs` for how nodes run and how "settled" is decided, and
//! `snapshot.rs` for what "converged" means.
//!
//! Run with `cargo test -p tagsyd --test multi_daemon`. Set
//! `RUST_LOG=tagsyd=debug` for daemon logs. A failing test keeps its data
//! directory and prints its path.

mod bench;
mod harness;
mod scenarios;
mod script;
mod snapshot;

use harness::{Cluster, DirectorySpec};

/// The shape of the real deployment: a central node holding everything in a
/// Universal directory, and a phone that dials it with one TagBased directory.
fn hub_and_spoke() -> (Cluster, harness::NodeId, harness::NodeId, tagsy_core::TagId) {
    let mut cluster = Cluster::new();
    let phone_tag = cluster.declare_tag("phone");
    let central = cluster.add_node("central", vec![DirectorySpec::universal("store")]);
    let phone = cluster.add_node("phone", vec![DirectorySpec::tag_based("phone", &[
        phone_tag,
    ])]);
    cluster.link(phone, central);
    (cluster, central, phone, phone_tag)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_on_spoke_reaches_hub() {
    let (mut cluster, _central, phone, phone_tag) = hub_and_spoke();
    cluster.start_connected().await;

    cluster
        .upload(phone, "notes/today.txt", b"hello from the phone", vec![
            phone_tag,
        ])
        .await;
    cluster
        .upload(phone, "untagged.txt", b"stays catalog-only", vec![])
        .await;

    cluster.settle().await;
    cluster.assert_converged();
}

/// An API upload on the hub lands in its own Universal directory (and in the
/// spoke's, since it carries the spoke's tag) — not just in its catalog.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_on_hub_is_stored_locally() {
    let (mut cluster, central, _phone, phone_tag) = hub_and_spoke();
    cluster.start_connected().await;

    cluster
        .upload(central, "from-central.txt", b"uploaded on the hub", vec![
            phone_tag,
        ])
        .await;

    cluster.settle().await;
    cluster.assert_converged();
}

/// An API edit overwrites the uploader's own copies, not just the peers'.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_edit_updates_uploaders_directories() {
    let (mut cluster, _central, phone, phone_tag) = hub_and_spoke();
    cluster.start_connected().await;

    let file_id = cluster
        .upload(phone, "draft.txt", b"first draft", vec![phone_tag])
        .await;
    cluster.settle().await;
    cluster.edit(phone, file_id, b"second draft, longer").await;

    cluster.settle().await;
    cluster.assert_converged();
}

/// Regression: an API upload used to skip the uploader's own directories, so
/// the uploader's disk changed on its next reconnect (when the missing-content
/// sweep placed the file). Live and reconnect must agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_upload_state_is_stable_across_reconnect() {
    let (mut cluster, _central, phone, phone_tag) = hub_and_spoke();
    cluster.start_connected().await;

    cluster
        .upload(phone, "notes/today.txt", b"hello from the phone", vec![
            phone_tag,
        ])
        .await;
    cluster.settle().await;
    let live = cluster.normalized();

    cluster.restart(phone).await;
    cluster.wait_all_connected().await;
    cluster.settle().await;

    script::assert_same_state("live", &live, "after reconnect", &cluster.normalized());
    cluster.assert_converged();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_written_into_spoke_directory_reaches_hub() {
    let (mut cluster, _central, phone, _) = hub_and_spoke();
    cluster.start_connected().await;

    cluster.write_file(phone, "phone", "photos/cat.jpg", b"not really a jpeg");

    cluster.settle().await;
    cluster.assert_converged();
}

/// An upload no local directory takes waits in the outbox until a peer holds
/// it, then is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_is_released_from_outbox_after_handoff() {
    let (mut cluster, _central, phone, _) = hub_and_spoke();
    cluster.start_connected().await;

    cluster
        .upload(phone, "archive-only.txt", b"central keeps this", vec![])
        .await;

    cluster.settle().await;
    cluster.assert_converged();
    cluster.wait_for_empty_outbox(phone).await;
}

/// An upload made while no peer is reachable survives a restart of the
/// uploader (the outbox holds the only copy) and is handed off once a peer
/// connects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offline_upload_survives_restart_and_is_handed_off() {
    let (mut cluster, central, phone, _) = hub_and_spoke();
    cluster.start_connected().await;
    cluster.stop(central).await;

    cluster
        .upload(phone, "offline.txt", b"uploaded while alone", vec![])
        .await;
    assert_eq!(cluster.outbox_entries(phone).len(), 1);
    cluster.restart(phone).await;
    assert_eq!(
        cluster.outbox_entries(phone).len(),
        1,
        "the outbox survives a restart"
    );

    cluster.start(central).await;
    cluster.wait_all_connected().await;
    cluster.settle().await;
    cluster.assert_converged();
    cluster.wait_for_empty_outbox(phone).await;
}

/// An upload the uploader places in its own sync directory is released from
/// the outbox without any peer involved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_placed_locally_is_released_from_outbox() {
    let (mut cluster, central, _phone, _) = hub_and_spoke();
    cluster.start_connected().await;

    cluster
        .upload(
            central,
            "stored-here.txt",
            b"in the universal store",
            vec![],
        )
        .await;

    cluster.settle().await;
    cluster.assert_converged();
    cluster.wait_for_empty_outbox(central).await;
}

/// The CLI's path: upload and edit through the control socket, handing the
/// daemon a path it copies itself; the source can go right away.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_and_edit_over_the_control_socket() {
    use tagsy_api::Backend;

    let (mut cluster, _central, phone, phone_tag) = hub_and_spoke();
    cluster.start_connected().await;
    let client = cluster.control_client(phone).await;

    let source = cluster.scratch_file(phone, b"sent by the cli");
    let uploaded = client
        .upload_file(source.clone(), "from-cli.txt".to_owned(), vec![phone_tag])
        .await
        .expect("upload over the control socket");
    std::fs::remove_file(&source).unwrap();
    // The answer is the file as recorded, not a guess from the request.
    assert_eq!(uploaded.logical_path.as_str(), "from-cli.txt");
    assert_eq!(uploaded.version_number, 1);
    assert_eq!(uploaded.size, b"sent by the cli".len() as u64);
    let file_id = uploaded.file_id;
    cluster.settle().await;
    cluster.assert_converged();

    let source = cluster.scratch_file(phone, b"edited by the cli, longer");
    let edited = client
        .edit_file(file_id, source.clone())
        .await
        .expect("edit over the control socket");
    std::fs::remove_file(&source).unwrap();
    assert_eq!(edited.version_number, 2);
    assert_eq!(
        edited.content_hash,
        blake3::hash(b"edited by the cli, longer")
            .to_hex()
            .to_string()
    );
    cluster.settle().await;
    cluster.assert_converged();
    cluster.wait_for_empty_outbox(phone).await;
}

/// Every mutation answers once it is applied, with the entry it touched as it
/// now stands — so a read issued right after it already sees the change.
/// Before, mutations were only enqueued and a read-back raced the writer;
/// this goes over the control socket, the path the CLI takes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutations_answer_with_the_applied_entry() {
    use tagsy_api::{ApiError, Backend, DeletedRule, SubtagRule, TagStyle};

    let (mut cluster, _central, phone, _phone_tag) = hub_and_spoke();
    cluster.start_connected().await;
    let client = cluster.control_client(phone).await;
    let file_tags = async |file_id| {
        client
            .tags_for_file(file_id, SubtagRule::Exclude)
            .await
            .unwrap()
    };

    let tag = client
        .create_tag("work".to_owned(), TagStyle::default())
        .await
        .unwrap();
    assert_eq!(tag.name, "work");
    assert_eq!(
        client
            .get_tag(tag.id, DeletedRule::Exclude)
            .await
            .unwrap()
            .name,
        "work"
    );
    let renamed = client.rename_tag(tag.id, "job".to_owned()).await.unwrap();
    assert_eq!(renamed.name, "job");
    let style = TagStyle {
        dot_color: "#123456".to_owned(),
        ..TagStyle::default()
    };
    let restyled = client.set_tag_style(tag.id, style).await.unwrap();
    assert_eq!(restyled.style.dot_color, "#123456");
    assert_eq!(restyled.name, "job");

    let source = cluster.scratch_file(phone, b"content");
    let file = client
        .upload_file(source, "a.txt".to_owned(), vec![])
        .await
        .unwrap();
    let tagged = client.tag_file(tag.id, file.file_id).await.unwrap();
    assert_eq!(tagged.file_id, file.file_id);
    assert_eq!(file_tags(file.file_id).await, vec![tag.id]);
    let moved = client
        .move_file(file.file_id, "b.txt".to_owned())
        .await
        .unwrap();
    assert_eq!(moved.logical_path.as_str(), "b.txt");
    client.untag_file(tag.id, file.file_id).await.unwrap();
    assert!(file_tags(file.file_id).await.is_empty());

    let parent = client
        .create_tag("parent".to_owned(), TagStyle::default())
        .await
        .unwrap();
    let child = client.tag_tag(parent.id, tag.id).await.unwrap();
    assert_eq!(child.id, tag.id);
    let parents = client
        .tags_for_tag(tag.id, SubtagRule::Exclude)
        .await
        .unwrap();
    assert_eq!(parents, vec![parent.id]);
    client.untag_tag(parent.id, tag.id).await.unwrap();
    assert!(
        client
            .tags_for_tag(tag.id, SubtagRule::Exclude)
            .await
            .unwrap()
            .is_empty()
    );

    client.tag_file(parent.id, file.file_id).await.unwrap();
    let deleted = client.delete_file(file.file_id).await.unwrap();
    assert!(deleted.deleted);
    assert!(client.delete_tag(tag.id).await.unwrap().deleted);
    assert!(!client.restore_tag(tag.id).await.unwrap().deleted);

    // A purge reports the files as they stood, tags included, and is applied
    // by the time it answers.
    let outcome = client.purge_deleted(false).await.unwrap();
    assert_eq!(outcome.purged.len(), 1, "{outcome:?}");
    assert_eq!(outcome.purged[0].file.file_id, file.file_id);
    assert_eq!(outcome.purged[0].file.logical_path.as_str(), "b.txt");
    assert_eq!(outcome.purged[0].tags, vec![parent.id]);
    assert!(matches!(
        client.get_file(file.file_id, DeletedRule::Include).await,
        Err(ApiError::UnknownId)
    ));

    cluster.settle().await;
    cluster.assert_converged();
}

/// Regression: an on-demand fetch of content the node already holds used to
/// `return` out of `CatalogWriter::run`, silently stopping every later catalog
/// write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_fetch_keeps_catalog_writer_alive() {
    use tagsy_api::Backend;

    let mut cluster = Cluster::new();
    let central = cluster.add_node("central", vec![DirectorySpec::universal("store")]);
    cluster.start_connected().await;

    let bytes = b"already local";
    cluster.write_file(central, "store", "local.txt", bytes);
    cluster.settle().await;
    let file_id = cluster
        .backend(central)
        .resolve_file_id("local.txt".to_owned(), tagsy_api::DeletedRule::Exclude)
        .await
        .expect("dropped file was cataloged");

    let hash = blake3::hash(bytes).to_hex().to_string();
    cluster
        .backend(central)
        .fetch_file(file_id, hash)
        .await
        .expect("fetching locally held content succeeds");

    cluster
        .backend(central)
        .create_tag("after-fetch".to_owned(), Default::default())
        .await
        .expect("catalog writer still accepts commands");
    cluster.settle().await;

    let normalized = cluster.catalog(central).normalized();
    assert!(
        normalized
            .lines()
            .any(|line| line.starts_with("tag \"after-fetch\" deleted=false")),
        "tag created after the fetch was never applied: {normalized:?}"
    );
}

/// A phone whose TagBased directory maps two file ids to one physical
/// `index` file, both with the same bytes: the state git's `index.lock` →
/// `index` rename used to leave behind, before an arrival at a tracked path
/// became new content. Returns `(original, shadow)`: the id the watcher
/// cataloged first, and the one planted at the same path.
///
/// The watcher can no longer produce this, so it is planted: the shadow is
/// uploaded normally (placed as `index (1)`), then, with the phone stopped,
/// its row is pointed at `index` and its own copy removed.
async fn shared_physical_path() -> (
    Cluster,
    harness::NodeId,
    tagsy_core::FileId,
    tagsy_core::FileId,
) {
    use tagsy_api::Backend;
    use tagsy_core::PhysicalPath;
    use tagsyd::store::DirectoryIndex;

    let (mut cluster, central, phone, phone_tag) = hub_and_spoke();
    cluster.start_connected().await;

    cluster.write_file(phone, "phone", "index", b"same");
    cluster.settle().await;
    let original = cluster
        .backend(phone)
        .resolve_file_id("index".to_owned(), tagsy_api::DeletedRule::Exclude)
        .await
        .expect("the written file was cataloged");
    let shadow = cluster
        .upload(central, "index", b"same", vec![phone_tag])
        .await;
    cluster.settle().await;

    cluster.stop(phone).await;
    let index = DirectoryIndex::initialize(cluster.index_db_path(phone, "phone")).unwrap();
    let own_copy = index.get_file(shadow).expect("shadow placed on the phone");
    assert_eq!(own_copy.physical_path.as_str(), "index (1)");
    index
        .update_file_physical_path(shadow, &PhysicalPath::new("index"))
        .unwrap();
    drop(index);
    cluster.remove_file(phone, "phone", "index (1)");
    cluster.start(phone).await;
    cluster.wait_all_connected().await;
    cluster.settle().await;

    let index = DirectoryIndex::open_read_only(cluster.index_db_path(phone, "phone")).unwrap();
    for id in [original, shadow] {
        assert_eq!(
            index.get_file(id).unwrap().physical_path.as_str(),
            "index",
            "the shared path did not survive the restart"
        );
    }

    (cluster, phone, original, shadow)
}

/// Whatever removes one of two ids sharing a physical path must drop only
/// its row: the bytes still belong to the other.
async fn assert_shared_bytes_survive(cluster: &Cluster, phone: harness::NodeId) {
    cluster.settle().await;
    assert_eq!(
        std::fs::read(cluster.directory_path(phone, "phone").join("index")).ok(),
        Some(b"same".to_vec()),
        "the bytes the remaining id maps to were removed"
    );
    cluster.assert_converged();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_the_shadow_of_a_shared_path_keeps_its_bytes() {
    use tagsy_api::Backend;

    let (cluster, phone, _original, shadow) = shared_physical_path().await;
    cluster.backend(phone).delete_file(shadow).await.unwrap();
    assert_shared_bytes_survive(&cluster, phone).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_the_original_of_a_shared_path_keeps_its_bytes() {
    use tagsy_api::Backend;

    let (cluster, phone, original, _shadow) = shared_physical_path().await;
    cluster.backend(phone).delete_file(original).await.unwrap();
    assert_shared_bytes_survive(&cluster, phone).await;
}

/// Untagging drops a file from a TagBased directory through placement, not
/// `RemoveFile`; the same rule applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn untagging_one_id_of_a_shared_path_keeps_its_bytes() {
    use tagsy_api::Backend;

    let (cluster, phone, _original, shadow) = shared_physical_path().await;
    let phone_tag = cluster
        .backend(phone)
        .tags_for_file(shadow, tagsy_api::SubtagRule::Exclude)
        .await
        .unwrap()[0];
    cluster
        .backend(phone)
        .untag_file(phone_tag, shadow)
        .await
        .unwrap();
    assert_shared_bytes_survive(&cluster, phone).await;
}

/// `delete-duplicates` keeps the lowest id of each set, merges the others'
/// tags onto it, and soft-deletes the rest — on every node. A dry run reports
/// the same plan and changes nothing.
///
/// The scripted scenarios check convergence, but their oracle names files by
/// logical path, so it cannot tell which copy survived or which one holds a
/// tag. This pins both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_duplicates_keeps_lowest_id_and_merges_tags() {
    use std::collections::BTreeSet;

    use tagsy_api::{Backend, DeletedRule, SubtagRule};

    let (mut cluster, central, phone, phone_tag) = hub_and_spoke();
    cluster.start_connected().await;
    let other_tag = cluster
        .backend(central)
        .create_tag("other".to_owned(), Default::default())
        .await
        .expect("create tag")
        .id;

    let mut copies = Vec::new();
    for tags in [vec![phone_tag], vec![other_tag], vec![]] {
        copies.push(cluster.upload(central, "dup.txt", b"same", tags).await);
    }
    let unique = cluster
        .upload(central, "dup.txt", b"different", vec![])
        .await;
    cluster.settle().await;

    let kept = *copies.iter().min().unwrap();
    let mut deleted: Vec<_> = copies.iter().copied().filter(|id| *id != kept).collect();
    deleted.sort();
    let kept_tags: BTreeSet<_> = cluster
        .backend(central)
        .tags_for_file(kept, SubtagRule::Exclude)
        .await
        .unwrap()
        .into_iter()
        .collect();
    let expected_merged: BTreeSet<_> = [phone_tag, other_tag]
        .into_iter()
        .filter(|tag| !kept_tags.contains(tag))
        .collect();

    let dry = cluster
        .backend(phone)
        .delete_duplicates(true)
        .await
        .expect("dry run");
    assert!(dry.dry_run);
    assert_eq!(dry.groups.len(), 1, "one duplicate set: {dry:?}");
    let group = &dry.groups[0];
    assert_eq!(group.logical_path.as_str(), "dup.txt");
    let ids = |group: &tagsy_api::DuplicateGroup| {
        let deleted: Vec<_> = group.deleted.iter().map(|file| file.file_id).collect();
        (group.kept.file_id, deleted)
    };
    assert_eq!(ids(group), (kept, deleted.clone()));
    assert!(
        group.deleted.iter().all(|file| !file.deleted),
        "a dry run reports the files as they stand"
    );
    assert_eq!(
        group.tags_merged.iter().copied().collect::<BTreeSet<_>>(),
        expected_merged
    );
    cluster.settle().await;
    for id in copies.iter().chain([&unique]) {
        let file = cluster
            .backend(phone)
            .get_file(*id, DeletedRule::Include)
            .await
            .unwrap();
        assert!(!file.deleted, "the dry run deleted {id:?}");
    }

    let applied = cluster
        .backend(phone)
        .delete_duplicates(false)
        .await
        .expect("delete duplicates");
    assert!(!applied.dry_run);
    assert_eq!(applied.groups.len(), 1);
    let group = &applied.groups[0];
    assert_eq!(ids(group), ids(&dry.groups[0]));
    assert_eq!(group.tags_merged, dry.groups[0].tags_merged);
    // Reported as they stand once applied.
    assert!(!group.kept.deleted, "the survivor is reported deleted");
    assert!(
        group.deleted.iter().all(|file| file.deleted),
        "a duplicate is reported live: {group:?}"
    );
    cluster.settle().await;
    cluster.assert_converged();

    for node in [central, phone] {
        let backend = cluster.backend(node);
        let is_deleted = async |id| {
            backend
                .get_file(id, DeletedRule::Include)
                .await
                .unwrap()
                .deleted
        };
        assert!(!is_deleted(kept).await, "the survivor was deleted");
        assert!(!is_deleted(unique).await, "a non-duplicate was deleted");
        for id in &deleted {
            assert!(is_deleted(*id).await, "a duplicate survived");
        }
        let tags: BTreeSet<_> = backend
            .tags_for_file(kept, SubtagRule::Exclude)
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(tags, BTreeSet::from([phone_tag, other_tag]));
    }
    assert_eq!(
        std::fs::read(cluster.directory_path(phone, "phone").join("dup.txt")).unwrap(),
        b"same",
        "the survivor took the plain name in the phone's directory"
    );

    // Deduplicating again finds nothing.
    let again = cluster
        .backend(central)
        .delete_duplicates(false)
        .await
        .unwrap();
    assert!(again.groups.is_empty(), "{again:?}");
}

/// The duplicates are deleted *before* the survivor gains their tags, so a
/// TagBased directory that newly wants the survivor finds the logical path
/// free. The other way round, the survivor is placed next to the duplicate
/// still there — as `dup (1).txt`, a name it keeps once the duplicate goes.
///
/// A node that also holds everything in a Universal directory has the
/// survivor's bytes locally and places it at once, which makes the order
/// observable; a phone would have to fetch it first, by when the name is
/// usually free anyway.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_duplicates_frees_the_name_before_retagging() {
    use tagsy_api::Backend;

    let mut cluster = Cluster::new();
    let phone_tag = cluster.declare_tag("phone");
    let node = cluster.add_node("node", vec![
        DirectorySpec::universal("store"),
        DirectorySpec::tag_based("phone", &[phone_tag]),
    ]);
    cluster.start_connected().await;

    let first = cluster.upload(node, "dup.txt", b"same", vec![]).await;
    let second = cluster.upload(node, "dup.txt", b"same", vec![]).await;
    // Tag the copy that will be deleted, so the survivor must be placed.
    let doomed = first.max(second);
    cluster
        .backend(node)
        .tag_file(phone_tag, doomed)
        .await
        .unwrap();
    cluster.settle().await;

    cluster
        .backend(node)
        .delete_duplicates(false)
        .await
        .expect("delete duplicates");
    cluster.settle().await;

    cluster.assert_converged();
    let phone_directory = snapshot::disk_contents(&cluster.directory_path(node, "phone"));
    let names: Vec<&str> = phone_directory
        .lines()
        .map(|line| line.split_once(' ').unwrap().0)
        .collect();
    assert_eq!(names, ["dup.txt"]);
}

/// The cleanup `delete-duplicates` exists for: ids a rename-over left sharing
/// one physical file. Whichever id is kept, the file stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_duplicates_cleans_up_a_shared_path() {
    use tagsy_api::{Backend, DeletedRule};

    let (cluster, phone, original, shadow) = shared_physical_path().await;
    let outcome = cluster
        .backend(phone)
        .delete_duplicates(false)
        .await
        .expect("delete duplicates");
    assert_eq!(outcome.groups.len(), 1, "{outcome:?}");
    assert_eq!(outcome.groups[0].kept.file_id, original.min(shadow));
    assert_shared_bytes_survive(&cluster, phone).await;

    let deleted = original.max(shadow);
    let file = cluster
        .backend(phone)
        .get_file(deleted, DeletedRule::Include)
        .await
        .unwrap();
    assert!(file.deleted);
}
