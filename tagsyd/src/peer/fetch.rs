//! On-demand content fetch and serve, built on the content-keyed relay
//! ([`super::relay`]) and the transfer stack ([`super::transfer`]).
//!
//! These are the entry points that are *not* a live peer-session pull: a
//! `CatalogCommand::Fetch` (from `tagsy edit`), deferred TagBased placement,
//! the restore availability probe, and the holder side that answers a peer's
//! `ChunkRequest` from local bytes or a registered provider. Moved out of
//! `lib.rs` (restructure 4.2) so `lib.rs` is just the runtime wiring; each was
//! already a free function taking every dependency explicitly.

use std::path::PathBuf;
use std::sync::Arc;

use tagsy_core::FileId;
use tagsy_core::state::ChangeOrigin;
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;

use crate::catalog::messages;
use crate::configuration::RuntimeConfiguration;
use crate::file_bytes::FileBytes;
use crate::peer::relay::ChunkRelay;
use crate::peer::transfer::{
    self, ChunkAnswer, ChunkReply, ChunkRequest, ReceiveOutcome, VerifiedHashCache,
};
use crate::sync_directories::SyncDirectoryCommand;

/// Read `file_id`'s bytes from local sync directories, but only return them if
/// they hash to `expected_hash`. Used by the on-demand fetch path to satisfy a
/// `CatalogCommand::Fetch` locally before flooding chunk requests to peers.
///
/// Returns `Some(bytes)` on a hash match, `None` if the file is absent locally
/// or its local content does not match the requested hash (in which case the
/// request should be served from peers).
pub(crate) async fn read_local_if_hash_matches(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    file_id: FileId,
    expected_hash: &str,
) -> Option<FileBytes> {
    let (respond_to, response) = tokio::sync::oneshot::channel();

    if command_sender
        .send(SyncDirectoryCommand::ReadFile {
            file_id,
            respond_to,
        })
        .is_err()
    {
        log::error!("command_sender closed; cannot read local bytes for fetch");
        return None;
    }

    match response.await {
        Ok(Some((_physical_path, file_bytes, content_hash))) if content_hash == expected_hash => {
            Some(file_bytes)
        }
        Ok(_) => None,
        Err(error) => {
            log::error!(
                "Directory manager dropped ReadFile responder for {}: {error}",
                file_id.to_string()
            );
            None
        }
    }
}
/// Answer a content-addressed `ChunkRequest` from a **local** source, if this
/// node holds `file_id`/`content_hash`.
///
/// Resolves a source in priority order — a matching file in our sync
/// directories, then a temporary provider (a local client serving on demand,
/// e.g. the CLI uploading) — and serves the canonical chunk at `offset` via
/// [`transfer::answer_chunk_request`].
///
/// The sync-directory case resolves only the file's *path* (via `LocalPath`,
/// which does **not** read or hash the bytes) and lets
/// [`transfer::answer_chunk_request`] verify against `content_hash` through the
/// `verified_hashes` cache — so the file is hashed **once** and every
/// subsequent chunk request is a cache hit plus a bounded seek/read. This keeps
/// serving a large file O(size) rather than O(size²/chunk): the previous
/// `ReadFile`-per-chunk path re-hashed the whole file on *every* request (the
/// cause of large-file download timeouts). A provider is looked up by its
/// `(file_id, content_hash)` registration key, which *is* its verification, and
/// is served **pre-verified** (re-hashing a provider would fire its
/// `on_complete` mid-serve and release the file — see
/// [`transfer::answer_chunk_request`]).
///
/// Returns `Some(ChunkAnswer::Data)` when we served bytes, `Some(Miss)` when we
/// hold the file but it does not match or the offset is malformed, and `None`
/// when no local source holds the file at all (the caller then relays the
/// request to other neighbours).
pub(crate) async fn answer_local_chunk(
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    pending_fetches: &ChunkRelay,
    verified_hashes: &VerifiedHashCache,
    file_id: FileId,
    content_hash: &str,
    offset: u64,
) -> Option<ChunkAnswer> {
    // 1. A file in our sync directories. Resolve just the path (no hashing);
    // `answer_chunk_request` verifies against `content_hash` via the cache
    // (hashing once, then serving from the cache on subsequent chunks).
    let (respond_to, response) = tokio::sync::oneshot::channel();
    if command_sender
        .send(SyncDirectoryCommand::LocalPath {
            file_id,
            respond_to,
        })
        .is_ok()
        && let Ok(Some(path)) = response.await
    {
        let source = FileBytes::FileToCopy(path.clone());
        return Some(
            transfer::answer_chunk_request(
                &source,
                Some(&path),
                verified_hashes,
                content_hash,
                offset,
                /* pre_verified */ false,
            )
            .await,
        );
    }

    // 2. A temporary provider (CLI upload/edit in flight), trusted by its
    // registration key — served pre-verified so we never re-hash it (which
    // would release the file after the first chunk).
    if let Some(provider) = pending_fetches.provider_for(file_id, content_hash).await {
        return Some(
            transfer::answer_chunk_request(
                &provider,
                None,
                verified_hashes,
                content_hash,
                offset,
                true,
            )
            .await,
        );
    }

    None
}
/// Spawn a **content-addressed receive** of `(file_id, content_hash)` into a
/// fresh temp file, sourcing each chunk through the content-keyed relay.
///
/// Each chunk the receiver wants is routed toward `toward` (the announcing
/// origin / peer we're reconciling with) when `Some`, else flooded across all
/// neighbours. Inbound `ChunkData`/`ChunkMiss` frames are delivered to the
/// receive by the relay's waiter table (which the per-chunk request registered
/// as a `Local` waiter), so no session-scoped demux is needed. The final
/// [`ReceiveOutcome`] is delivered on the returned oneshot.
///
/// This is the single receive entry point shared by live-sync/reconcile pulls
/// (driven by a peer session) and on-demand fetches / deferred placement
/// (driven inside `handle_changes`).
pub(crate) fn spawn_content_receive(
    pending_fetches: &ChunkRelay,
    file_id: FileId,
    content_hash: String,
    expected_size: u64,
    temp_path: PathBuf,
    toward: Option<String>,
    progress: Option<transfer::ProgressSink>,
) -> tokio::sync::oneshot::Receiver<ReceiveOutcome> {
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();

    log::debug!(
        "spawn_content_receive: {} [{}] size={expected_size} toward={}",
        file_id.to_string(),
        content_hash.get(..8).unwrap_or(&content_hash),
        toward.as_deref().unwrap_or("<flood>")
    );

    // The receive driver emits `ChunkRequest`s on `req` and awaits `ChunkReply`s
    // on `reply`. The bridge below routes each request through the relay,
    // passing a clone of the reply sender so the relay's waiter table can fan
    // the eventual `ChunkData`/`ChunkMiss` back to this receive.
    let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
    let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();

    let pending_fetches_bridge = pending_fetches.clone();
    let content_hash_bridge = content_hash.clone();
    tokio::spawn(async move {
        while let Some(ChunkRequest { offset }) = req_rx.recv().await {
            pending_fetches_bridge
                .request_chunk_local(
                    file_id,
                    content_hash_bridge.clone(),
                    offset,
                    toward.as_deref(),
                    reply_tx.clone(),
                )
                .await;
        }
    });

    tokio::spawn(async move {
        let outcome = match transfer::receive(
            content_hash,
            expected_size,
            temp_path,
            req_tx,
            reply_rx,
            progress,
        )
        .await
        {
            Ok(file_bytes) => ReceiveOutcome::Complete(file_bytes),
            Err(error) => ReceiveOutcome::Failed(error),
        };
        let _ = outcome_tx.send(outcome);
    });

    outcome_rx
}
/// Ask the peer that announced a change (`change_origin`) to serve us its
/// bytes: send a `StartReceive` command to that peer's live session, which owns
/// the receive machinery. No-op if the change is local-origin or the peer has
/// no live session (reconciliation will pick it up on the next connect).
pub(crate) async fn request_pull_from_origin(
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    change_origin: &ChangeOrigin,
    file_id: FileId,
    content_hash: String,
    expected_size: u64,
    placement: messages::MaterializePlacement,
) {
    let ChangeOrigin::Peer { public_key } = change_origin else {
        // Local-origin content already has its bytes; nothing to pull.
        return;
    };
    let commands = runtime_configuration
        .read()
        .await
        .peers
        .get(public_key)
        .and_then(|runtime_peer| runtime_peer.commands.clone());

    match commands {
        Some(commands) => {
            if commands
                .send(messages::PeerCommand::StartReceive {
                    file_id,
                    content_hash,
                    expected_size,
                    placement,
                })
                .is_err()
            {
                log::warn!(
                    "Peer {public_key} command channel closed; cannot pull {}; reconciliation \
                     will retry on reconnect",
                    file_id.to_string()
                );
            }
        }
        None => {
            log::debug!(
                "Announcing peer {public_key} has no live session; deferring pull of {} to \
                 reconciliation",
                file_id.to_string()
            );
        }
    }
}
/// On-demand fetch of `(file_id, content_hash)` through the content-keyed
/// relay, flooding across the live peer tree (no preferred direction). Returns
/// the completed temp file, or an error if no reachable holder could serve it.
///
/// Used by `tagsy edit`, deferred TagBased placement, and the restore
/// availability probe. Unlike a live-sync pull (which directs the first request
/// toward the announcing origin), the direction here is unknown, so the first
/// request for each chunk floods; whichever direction answers establishes the
/// route for subsequent chunks.
pub(crate) async fn fetch_via_relay(
    pending_fetches: &ChunkRelay,
    file_id: FileId,
    content_hash: String,
    expected_size: u64,
    progress: Option<transfer::ProgressSink>,
) -> Result<FileBytes, messages::FetchError> {
    let temp_dir = std::env::temp_dir().join(format!("tagsy-fetch-{}", std::process::id()));
    if let Err(error) = tokio::fs::create_dir_all(&temp_dir).await {
        log::warn!("Failed to create fetch temp dir: {error}");
    }
    let temp_path = temp_dir.join(uuid::Uuid::new_v4().to_string());
    let short = content_hash.get(..8).unwrap_or(&content_hash).to_owned();
    log::debug!(
        "fetch_via_relay: start {} [{short}] size={expected_size} (flood)",
        file_id.to_string()
    );
    let started = std::time::Instant::now();

    let outcome_rx = spawn_content_receive(
        pending_fetches,
        file_id,
        content_hash,
        expected_size,
        temp_path,
        None,
        progress,
    );

    match outcome_rx.await {
        Ok(ReceiveOutcome::Complete(content)) => {
            log::debug!(
                "fetch_via_relay: {} [{short}] complete in {:?}",
                file_id.to_string(),
                started.elapsed()
            );
            Ok(content)
        }
        Ok(ReceiveOutcome::Failed(error)) => {
            log::warn!(
                "fetch_via_relay: {} [{short}] failed in {:?}: {error}",
                file_id.to_string(),
                started.elapsed()
            );
            Err(messages::FetchError::NotAvailable)
        }
        Err(_) => {
            log::warn!(
                "fetch_via_relay: {} [{short}] receive task dropped",
                file_id.to_string()
            );
            Err(messages::FetchError::NotAvailable)
        }
    }
}
/// Availability probe: does *anyone* in the live peer tree still hold the bytes
/// for `(file_id, content_hash)`? Rather than a separate discovery message this
/// is a single offset-0 `ChunkRequest` routed through the relay (flooding),
/// whose returned bytes are discarded. Any `ChunkData` proves availability;
/// exhaustion (`ChunkMiss` from all directions) or the TTL proves absence.
///
/// Used by restore before clearing a tombstone, so we never announce a restore
/// whose bytes cannot be recovered.
pub(crate) async fn probe_availability(
    pending_fetches: &ChunkRelay,
    file_id: FileId,
    content_hash: String,
) -> bool {
    let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();
    // Direction unknown: flood. The relay registers this as a `Local` waiter for
    // offset 0 and forwards to all neighbours.
    pending_fetches
        .request_chunk_local(file_id, content_hash, 0, None, reply_tx)
        .await;

    matches!(reply_rx.recv().await, Some(ChunkReply::Data { .. }))
}
