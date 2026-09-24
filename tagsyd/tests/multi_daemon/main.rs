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
