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
#[ignore = "API uploads are not placed in the uploader's own sync directories (announce_provided); \
            only a later reconnect sweep places them"]
async fn upload_on_spoke_reaches_hub() {
    let (mut cluster, central, phone, phone_tag) = hub_and_spoke();
    cluster.start_all().await;
    cluster.wait_connected(central, phone).await;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_written_into_spoke_directory_reaches_hub() {
    let (mut cluster, central, phone, _) = hub_and_spoke();
    cluster.start_all().await;
    cluster.wait_connected(central, phone).await;

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
    cluster.start_all().await;

    // Dropped into the Universal directory, so the bytes are genuinely local
    // (an API upload is only served from its provider; see
    // `announce_provided`).
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
