//! Forwarding applied changes to peers, plus the origin-tag helper and the
//! metadata-only wire `Change` builder.
//!
//! Lifted verbatim from the nested items inside `handle_changes`; each already
//! took every dependency as an explicit parameter, so they are `pub(crate)`
//! free functions here.

use std::sync::Arc;

use tagsy_core::state::{Change, ChangeOrigin, Frame};
use tagsy_core::{FileId, LogicalPath, TagId};
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;

use crate::catalog::placement::{self, Placement};
use crate::configuration::{Configuration, RuntimeConfiguration};
use crate::file_bytes::FileBytes;
use crate::sync_directories::SyncDirectoryCommand;

/// Origin tag stored in `file_versions.origin` for locally-observed versions.
/// Peer-originated versions will use the originating peer's public key here
/// instead.
pub(crate) const LOCAL_ORIGIN: &str = "local";

/// Resolve the `origin` string to store in `file_versions.origin` for a
/// `Change` we just received.
pub(crate) fn version_origin(change_origin: &ChangeOrigin) -> &str {
    match change_origin {
        ChangeOrigin::Local { .. } => LOCAL_ORIGIN,
        ChangeOrigin::Peer { public_key } => public_key.as_str(),
    }
}

pub(crate) async fn forward_to_peers(
    configuration: &Configuration,
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    change: &Change,
    change_origin: &ChangeOrigin,
) {
    // TODO: Apply per-peer SyncType filtering once it's tracked (step 8).
    let runtime = runtime_configuration.read().await;
    for peer in &configuration.peers {
        if let ChangeOrigin::Peer { public_key } = &change_origin
            && public_key == &peer.public_key
        {
            // Nothing to do, the change originates from this peer.
            continue;
        }

        let Some(runtime_peer) = runtime.peers.get(&peer.public_key) else {
            log::warn!(
                "Peer {} ({}) missing from RuntimeConfiguration",
                peer.name,
                peer.public_key
            );
            continue;
        };

        let Some(outbound) = runtime_peer.outbound.as_ref() else {
            // TODO: Buffer or rely on reconciliation (step 6) when peer reconnects.
            log::debug!("Peer {} not connected; dropping outbound Change", peer.name);
            continue;
        };

        if let Err(error) = outbound.send(Frame::Change(change.clone())) {
            log::warn!("Failed to enqueue Change for peer {}: {error}", peer.name);
        }
    }
}

/// The metadata-only wire `Change` to announce to peers for a local content
/// ingestion. `Change` no longer carries bytes; peers pull them separately.
pub(crate) enum WireKind {
    Added {
        file_id: FileId,
        logical_path: LogicalPath,
        logical_path_modified_at: i64,
        content_hash: String,
        size: u64,
        tags: Vec<TagId>,
    },
    Changed {
        file_id: FileId,
        content_hash: String,
        size: u64,
    },
}

impl WireKind {
    pub(crate) fn into_change(self) -> Change {
        match self {
            WireKind::Added {
                file_id,
                logical_path,
                logical_path_modified_at,
                content_hash,
                size,
                tags,
            } => Change::FileMetadataAdded {
                file_id,
                logical_path,
                logical_path_modified_at,
                content_hash,
                size,
                tags,
            },
            WireKind::Changed {
                file_id,
                content_hash,
                size,
            } => Change::FileMetadataChanged {
                file_id,
                content_hash,
                size,
            },
        }
    }
}

/// Dispatch a local content ingestion to matching sync directories (streaming
/// the bytes to disk) and announce a metadata-only wire `Change` to peers.
///
/// The bytes are never buffered here for peers: `Change` is metadata-only, so a
/// peer that wants the content pulls it over a separate transfer. This keeps
/// large local ingests entirely off the heap regardless of how many peers are
/// connected.
///
/// Also publishes the change to UI subscribers. See `EVENT PUBLISHING` on
/// [`CatalogWriter::run`]: the caller `continue`s and so never reaches the shared
/// publish at the bottom of the loop, so this emits for itself.
///
/// [`CatalogWriter::run`]: crate::catalog::CatalogWriter::run
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dispatch_and_forward(
    configuration: &Configuration,
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    event_sender: &tokio::sync::broadcast::Sender<Change>,
    targets: Vec<Placement>,
    content: FileBytes,
    change_origin: &ChangeOrigin,
    wire: WireKind,
) {
    placement::place_content(command_sender, targets, content).await;
    let change = wire.into_change();
    forward_to_peers(configuration, runtime_configuration, &change, change_origin).await;
    let _ = event_sender.send(change);
}
