//! The post-handshake peer session: [`run_peer_session`] drives a
//! fully-handshaken WebSocket link until it closes, shared by both the inbound
//! ([`handle_connection`](super::dial::handle_connection)) and outbound
//! ([`connect_to_peer`](super::dial::connect_to_peer)) paths. [`PeerContext`]
//! bundles the routing handles every session needs.

use std::collections::HashSet;
use std::sync::Arc;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tagsy_core::FileId;
use tagsy_core::state::{Change, ChangeOrigin, Frame, Sync as SyncMessage};
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_util::sync::CancellationToken;

use crate::catalog::messages::{self, CatalogCommand, Ingest};
#[cfg(feature = "preview-generation")]
use crate::catalog::previews::preview_extension_for;
use crate::catalog::previews::try_serve_generated_preview;
use crate::configuration::RuntimeConfiguration;
use crate::operations;
use crate::peer::fetch::{answer_local_chunk, spawn_content_receive};
use crate::peer::plan::{
    MissingContent, PeerDeletion, PeerMove, PeerRestore, SyncPlan, batch_manifest,
    build_local_manifest, plan_file_sync,
};
use crate::peer::plan_tags::{
    batch_tag_manifest, build_local_tag_manifest, build_tag_request_response, plan_tag_sync,
};
use crate::peer::relay::{ChunkRelay, PreviewRelay};
use crate::peer::transfer::{self, ChunkAnswer, ReceiveOutcome, VerifiedHashCache};
use crate::store::CatalogStore;
use crate::sync_directories::SyncDirectoryCommand;

/// What a peer-session receive materializes on completion: the received bytes
/// are written into our sync directories and the version recorded, placing per
/// `placement`. Carried alongside the receive's outcome so the session's
/// completion handler can dispatch.
///
/// (On-demand fetches — `tagsy edit`, deferred placement — do not go through
/// a peer session; they drive a receive directly via [`fetch_via_relay`] and
/// return the temp file to their waiter.)
///
/// [`fetch_via_relay`]: crate::peer::fetch::fetch_via_relay
pub(crate) struct ReceiverPurpose {
    file_id: FileId,
    content_hash: String,
    origin: ChangeOrigin,
    placement: messages::MaterializePlacement,
}

/// The shared routing handles every peer-connection task needs: the runtime
/// peer table, the pending on-demand fetches, and the two senders into the
/// change bus and sync-directory manager.
///
/// Bundled into one `Clone` struct so `handle_connection`, `connect_to_peer`,
/// and `run_peer_session` can pass a single context around instead of the same
/// four arguments each (which also keeps them under clippy's argument-count
/// lint). All four fields are cheap to clone (`Arc`s / channel senders).
#[derive(Clone)]
pub struct PeerContext {
    pub runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    pub pending_fetches: ChunkRelay,
    /// Content-keyed preview relay, sibling to `pending_fetches`. Every peer
    /// session holds a clone so a `PreviewRequest` forwarded on one link and
    /// its `PreviewData`/`PreviewMiss` arriving on another share one waiter
    /// table.
    pub pending_previews: PreviewRelay,
    pub change_sender: UnboundedSender<CatalogCommand>,
    pub command_sender: UnboundedSender<SyncDirectoryCommand>,
    /// Whether this device may *generate* previews locally (its policy permits
    /// it and the `preview-generation` feature is compiled in). When `false`, a
    /// peer `PreviewRequest` is answered only from our preview cache, never by
    /// decoding local bytes.
    pub can_generate_previews: bool,
    /// Node-wide cache of verified content hashes (`path -> (mtime, size,
    /// hash)`), so a holder answering repeated `ChunkRequest`s for the same
    /// unchanged file hashes it once. Shared across all peer sessions.
    pub verified_hashes: VerifiedHashCache,
    /// Live sync-operation registry, so peer sessions can surface what they are
    /// doing (serving/receiving files, reconciling, fetching) to the UI.
    pub operations: crate::operations::Operations,
    /// Live peer-connection registry. A session registers itself here for its
    /// whole lifetime so the UI can show which peers are connected right now —
    /// connection *state*, distinct from the operations above.
    pub connections: crate::connections::Connections,
    /// Process-wide gate bounding how many file byte-transfers run at once.
    /// Every session shares one clone so a bulk import announced across peers
    /// can't start an unbounded number of concurrent receives.
    pub pull_scheduler: crate::peer::pull_scheduler::PullScheduler,
    /// Max file entries per connection-time `Sync::Manifest` frame; the
    /// manifest is split into several frames of this size so no single
    /// WebSocket message approaches the size ceiling. From
    /// `Configuration::manifest_batch_size`.
    pub manifest_batch_size: usize,
    /// Max tag definitions/relationships per `Sync::TagManifest` frame. From
    /// `Configuration::tag_manifest_batch_size`.
    pub tag_manifest_batch_size: usize,
}

/// Drive a fully-handshaken WebSocket connection until it closes.
///
/// Shared between inbound (`handle_connection`) and outbound
/// (`connect_to_peer`) paths because the post-handshake behavior is identical:
/// build and send our manifest, register an outbound channel, then loop over
/// outbound `Frame`s and inbound WebSocket frames.
///
/// Opens its own read-only handle on the main DB. The DB is shared with
/// `handle_changes` and with other connection tasks; SQLite serializes these
/// accesses at the file level. Writes still only happen from `handle_changes`.
#[allow(clippy::too_many_arguments)]
pub async fn run_peer_session<S>(
    peer_public_key: &str,
    peer_name: &str,
    main_db_path: &std::path::Path,
    outgoing: SplitSink<WebSocketStream<S>, Message>,
    mut incoming: SplitStream<WebSocketStream<S>>,
    direction: operations::Direction,
    context: PeerContext,
    shutdown: &CancellationToken,
) where
    // `Send + 'static`: the write half is moved onto a spawned writer task.
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let PeerContext {
        runtime_configuration,
        pending_fetches,
        pending_previews,
        change_sender,
        command_sender,
        can_generate_previews,
        verified_hashes,
        operations,
        connections,
        pull_scheduler,
        manifest_batch_size,
        tag_manifest_batch_size,
    } = context;

    // Register this peer as connected for the life of the session. A connection
    // is *state*, not an operation: the guard's `Drop` (when the session ends,
    // cleanly or not) removes the peer from the connected set and broadcasts a
    // `Disconnected` — no `Aborted`-means-disconnected lie, and the operations
    // UI's work indicator no longer stays lit for the whole session.
    let _connection = connections.register(peer_public_key, peer_name, direction);

    // CatalogStore wraps a rusqlite Connection which is Send but not Sync.
    // We must never hold `&CatalogStore` across an `.await` in this task,
    // otherwise tokio::spawn rejects the future as non-Send. All sync helpers
    // below take `&CatalogStore` synchronously and return owned data; this
    // function does the awaits separately.
    //
    // The database path is supplied by the caller. This connection is
    // READ-ONLY: the session makes reconciliation decisions from it but routes
    // every write through `change_sender` to `handle_changes`, the sole
    // main-database writer.
    let database = match CatalogStore::initialize(main_db_path) {
        Ok(database) => database,
        Err(error) => {
            log::error!("Peer {peer_name}: failed to open main DB for session: {error:?}");
            return;
        }
    };

    let (peer_tx, peer_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
    // Sentinel clone retained for the lifetime of the session: we use
    // `same_channel` against the slot in `RuntimePeer.outbound` to know whether
    // the sender currently parked there is still ours (vs. one a sibling
    // session installed after we registered).
    let our_sender = peer_tx.clone();

    // Completed receives report their outcome here; the select loop drains it to
    // materialize the bytes. There is no per-transfer demux table any more:
    // inbound `ChunkData`/`ChunkMiss` are routed by the content-keyed relay
    // (`pending_fetches`), not by a session-scoped id.
    let (receiver_done_tx, mut receiver_done_rx) =
        tokio::sync::mpsc::unbounded_channel::<(ReceiverPurpose, ReceiveOutcome)>();

    // Temp directory for in-flight received files. Kept per-session under the
    // system temp dir; a completed receive's temp file is then materialized
    // (moved) into the sync directories.
    let transfer_temp_dir = std::env::temp_dir().join(format!(
        "tagsy-transfer-{}-{}",
        std::process::id(),
        peer_public_key
    ));

    // Start a content-addressed receive of `file_id`/`content_hash` from *this*
    // peer (the announcing origin / the peer we're reconciling with), tagged
    // with `purpose` (what to do with the bytes once received). The receive
    // sources each chunk through the content-keyed relay, directing the first
    // request toward this peer; a later chunk central has caught up on is served
    // by it like any other holder — no session to renegotiate. The outcome is
    // forwarded onto `receiver_done_rx`.
    //
    // Every pull is admitted through the process-wide `pull_scheduler`: it runs
    // only once a concurrency slot is free (queueing behind other transfers
    // across all peers), and a duplicate submission for content already in
    // flight is coalesced away. The "receiving file" operation is begun inside
    // the job — i.e. when the pull actually starts, not while it waits in the
    // queue — so the UI's active-transfer count reflects real in-flight work.
    let start_pull = {
        let pending_fetches = pending_fetches.clone();
        let receiver_done_tx = receiver_done_tx.clone();
        let transfer_temp_dir = transfer_temp_dir.clone();
        let operations = operations.clone();
        let pull_scheduler = pull_scheduler.clone();
        let peer_name = peer_name.to_owned();
        let peer_public_key = peer_public_key.to_owned();

        move |file_id: FileId,
              content_hash: String,
              expected_size: u64,
              purpose: ReceiverPurpose| {
            // Clone per submission so the spawned job owns its captures.
            let pending_fetches = pending_fetches.clone();
            let done_tx = receiver_done_tx.clone();
            let transfer_temp_dir = transfer_temp_dir.clone();
            let operations = operations.clone();
            let pull_scheduler = pull_scheduler.clone();
            let peer_name = peer_name.clone();
            let peer_public_key = peer_public_key.clone();
            let job_content_hash = content_hash.clone();

            async move {
                pull_scheduler
                    .submit(file_id, content_hash, move || async move {
                        let temp_path = transfer_temp_dir.join(uuid::Uuid::new_v4().to_string());

                        // Surface this pull as a live "receiving file" operation
                        // with byte progress. Begun here (post-admission) so a
                        // queued pull doesn't show as active before it runs.
                        let receiving = operations.begin(
                            operations::OperationKind::receiving_file(file_id, &peer_name),
                        );
                        let progress = {
                            let operations = operations.clone();
                            let id = receiving.id();
                            Box::new(move |done: u64, total: Option<u64>| {
                                operations.report_progress(id, done, total);
                            }) as transfer::ProgressSink
                        };

                        let outcome_rx = spawn_content_receive(
                            &pending_fetches,
                            file_id,
                            job_content_hash,
                            expected_size,
                            temp_path,
                            Some(peer_public_key.clone()),
                            Some(progress),
                        );

                        if let Ok(outcome) = outcome_rx.await {
                            match &outcome {
                                ReceiveOutcome::Complete(_) => receiving.complete(),
                                ReceiveOutcome::Failed(error) => receiving.fail(error.to_string()),
                            }
                            let _ = done_tx.send((purpose, outcome));
                        }
                        // If `outcome_rx` closed without a value, `receiving`
                        // drops here and the operation is marked aborted.
                    })
                    .await;
            }
        }
    };

    if let Err(error) = tokio::fs::create_dir_all(&transfer_temp_dir).await {
        log::warn!(
            "Failed to create transfer temp dir for {peer_name}: {error}; transfers to this peer \
             will fail"
        );
    }

    // Register our outbound sender so `forward_to_peers` can route live
    // changes through this connection.
    //
    // The slot can hold one of three things:
    // - `None`: free, install our sender, we own it.
    // - `Some(dead)`: a previous session's sender whose receiver has been dropped
    //   (this happens because the cleanup at the end of a session cannot detect "I
    //   am the dropped receiver"; `is_closed` returns false while we still hold our
    //   own receiver). We replace it transparently.
    // - `Some(live)`: a sibling session is actively running for this peer (e.g.
    //   both sides dialed each other at the same time). Fall back to inbound-only
    //   so we don't double-send.
    // Command channel for `handle_changes` to trigger byte pulls on this link.
    let (command_tx, mut command_rx) =
        tokio::sync::mpsc::unbounded_channel::<messages::PeerCommand>();

    let owns_outbound = {
        let mut runtime = runtime_configuration.write().await;

        match runtime.peers.get_mut(peer_public_key) {
            Some(runtime_peer) => {
                let slot_is_dead = runtime_peer
                    .outbound
                    .as_ref()
                    .map(|sender| sender.is_closed())
                    .unwrap_or(true);

                if slot_is_dead {
                    runtime_peer.outbound = Some(peer_tx);
                    runtime_peer.commands = Some(command_tx);
                    true
                } else {
                    log::debug!(
                        "Peer {peer_name} already has an outbound sender; inbound-only mode for \
                         this connection"
                    );
                    false
                }
            }
            None => {
                log::error!(
                    "Peer {peer_name} missing from RuntimeConfiguration; dropping connection"
                );
                return;
            }
        }
    };

    // Announce our *tag* manifest first thing post-handshake, before the file
    // manifest. Ordering is deliberate and matters for placement efficiency:
    //
    // - Frames travel over one ordered link, so the peer handles our `TagManifest`
    //   before our `Manifest`.
    // - Handling `TagManifest` enqueues the `FileTagged`/`FileUntagged`
    //   relationships onto the change bus; handling `Manifest` starts file pull
    //   *transfers* whose `Materialize` is only enqueued once the bytes finish
    //   arriving (many round-trips later).
    // - `handle_changes` is a single FIFO consumer, so relationships enqueued first
    //   are applied before any later `Materialize`.
    //
    // Net effect: when a peer brings both new tags and new files, the tags are
    // in place by the time files materialize, so each file lands in its
    // matching TagBased directories on the *first* placement — avoiding the
    // re-placement copy that `ApplyPlacement` would otherwise perform
    // (STREAMING_FOLLOWUPS §1.3). That fix still guarantees *correctness*
    // regardless of order; this ordering is purely the efficiency win.
    //
    // Relationship rows carry no FK on the tag definition (`entries` table), so
    // applying `FileTagged` before the corresponding `TagAdded` definition
    // (which may still be in flight via `TagRequest`) is safe.
    // The socket's write half (`outgoing`) is owned exclusively by a dedicated
    // *writer task* spawned below; this session task never writes to the socket
    // again. That decoupling is what makes the initial manifests safe to send
    // even when they are large: they are queued on `our_sender` (drained by the
    // writer as TCP allows) instead of being awaited inline here, so this task
    // is free to keep reading inbound frames. Sending a big manifest while the
    // peer is simultaneously sending us one (its `TagRequest`/manifest burst)
    // used to deadlock — both sides blocked writing, neither reading — because
    // the send was awaited on the same task that had to read. See the writer
    // task for how liveness (ping) and pongs are handled off this task too.
    //
    // Shared with the writer task, both lock-free:
    // - `last_activity`: milliseconds (since `session_start`) of the most recent
    //   inbound frame; the reader stores, the writer loads to decide when the peer
    //   has gone silent past `LIVENESS_TIMEOUT`.
    // - `pong_requested`: set by the reader when a WebSocket Ping arrives (we split
    //   the stream, so tungstenite can't auto-pong); the writer clears it and sends
    //   an empty Pong. A boolean suffices — the RFC lets a pong carry any payload
    //   (including none) and lets pongs be coalesced/dropped, so we don't echo the
    //   ping's bytes.
    // - `link_dead`: cancelled by the writer if a socket write fails, so this
    //   reader loop breaks promptly and teardown runs.
    let session_start = tokio::time::Instant::now();
    let last_activity = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let pong_requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let link_dead = CancellationToken::new();

    let writer_handle = {
        let last_activity = last_activity.clone();
        let pong_requested = pong_requested.clone();
        let link_dead = link_dead.clone();
        let peer_name = peer_name.to_owned();
        tokio::spawn(async move {
            run_writer(
                outgoing,
                peer_rx,
                session_start,
                last_activity,
                pong_requested,
                link_dead,
                &peer_name,
            )
            .await;
        })
    };

    // Announce our manifests by queueing them on the outbound channel (drained
    // by the writer), not by writing them inline — see the block comment above.
    //
    // Both manifests are split into batches of bounded size so no single
    // WebSocket message approaches the size ceiling on a large catalog. Frames
    // are queued in order (tag definitions, tag relationships, then files); the
    // receiver reconciles each independently, so the split is behavior-
    // preserving (see `batch_manifest` / `batch_tag_manifest`).
    match build_local_tag_manifest(&database) {
        Ok((definitions, relationships)) => {
            let frames = batch_tag_manifest(definitions, relationships, tag_manifest_batch_size);
            let count = frames.len();
            for (definitions, relationships) in frames {
                let frame = Frame::Sync(SyncMessage::TagManifest {
                    definitions,
                    relationships,
                });
                if our_sender.send(frame).is_err() {
                    log::warn!("Failed to queue tag manifest to {peer_name}: writer gone");
                    break;
                }
            }
            log::debug!("Queued tag manifest to {peer_name} in {count} frame(s)");
        }
        Err(error) => {
            log::error!("Peer {peer_name}: failed to build initial tag manifest: {error:?}");
        }
    }

    // Queue our file manifest right after the tag manifest (see the ordering
    // rationale above; the outbound channel preserves order). The peer compares
    // it against their own history and requests anything they need.
    match build_local_manifest(&database) {
        Ok(manifest) => {
            let total = manifest.len();
            let batches = batch_manifest(manifest, manifest_batch_size);
            let count = batches.len();
            for entries in batches {
                let frame = Frame::Sync(SyncMessage::Manifest { entries });
                if our_sender.send(frame).is_err() {
                    log::warn!("Failed to queue file manifest to {peer_name}: writer gone");
                    break;
                }
            }
            log::debug!("Queued file manifest to {peer_name}: {total} entries in {count} frame(s)");
        }
        Err(error) => {
            log::error!("Peer {peer_name}: failed to build initial manifest: {error:?}");
            // Continue without manifest; the peer's manifest still drives
            // anything they need to receive from us.
        }
    }

    // Now that a peer is reachable, recover any files this device should hold
    // but is missing the bytes for (a failed pull leaves such a gap, and the
    // transfer stack has no retry by design). Handed to `handle_changes` — the
    // sole catalog reader/writer, which also has the sync-directory channel to
    // learn what is missing on disk — so the sweep and its flood fetches run
    // there rather than blocking this session's frame loop. Fired once per
    // connect: reconnection is the sanctioned external recovery trigger. The
    // fetch floods the live peer tree, so any connected holder can serve it,
    // not only this peer.
    if change_sender
        .send(CatalogCommand::SweepMissingContent)
        .is_err()
    {
        log::warn!(
            "Failed to queue missing-content sweep on connect to {peer_name}: catalog writer gone"
        );
    }

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                log::info!("Shutdown requested; closing session with {peer_name}");
                break;
            }
            _ = link_dead.cancelled() => {
                // The writer task hit a socket error (failed send / dead ping)
                // and cancelled the link. Stop reading and tear down.
                log::info!("Link to {peer_name} closed by writer; ending session");
                break;
            }
            command = command_rx.recv() => {
                let Some(command) = command else {
                    // All command senders dropped (peer removed from runtime).
                    continue;
                };
                match command {
                    messages::PeerCommand::StartReceive {
                        file_id,
                        content_hash,
                        expected_size,
                        placement,
                    } => {
                        // `handle_changes` recorded a live change this peer
                        // announced and wants its bytes. Pull them over this
                        // link; materialize (and record the version) on
                        // completion.
                        let purpose = ReceiverPurpose {
                            file_id,
                            content_hash: content_hash.clone(),
                            origin: ChangeOrigin::Peer {
                                public_key: peer_public_key.to_owned(),
                            },
                            placement,
                        };
                        start_pull(file_id, content_hash, expected_size, purpose).await;
                    }
                }
            }
            completed = receiver_done_rx.recv() => {
                let Some((purpose, outcome)) = completed else {
                    // The done channel is never fully dropped while the session
                    // lives (we hold a sender clone), so `None` only at teardown.
                    continue;
                };
                let content = match outcome {
                    ReceiveOutcome::Complete(content) => content,
                    ReceiveOutcome::Failed(error) => {
                        log::warn!("Receive from {peer_name} failed: {error}");
                        continue;
                    }
                };
                let ReceiverPurpose {
                    file_id,
                    content_hash,
                    origin,
                    placement,
                } = purpose;
                log::debug!(
                    "Receive from {peer_name} completed for {}; materializing",
                    file_id.to_string()
                );
                if let Err(error) = change_sender.send(CatalogCommand::Materialize {
                    file_id,
                    content,
                    content_hash,
                    origin,
                    placement,
                }) {
                    log::error!(
                        "change_sender closed; cannot materialize receive for {}: {error}",
                        file_id.to_string()
                    );
                    break;
                }
            }
            inbound = incoming.next() => {
                let Some(message) = inbound else {
                    log::info!("Peer {peer_name} closed the connection");
                    break;
                };
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        log::warn!("Read error from {peer_name}: {error}");
                        break;
                    }
                };
                // Any inbound message — data, or a WebSocket ping/pong control
                // frame — proves the peer is still there, so it resets the
                // liveness clock the writer task reads.
                last_activity.store(
                    session_start.elapsed().as_millis() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                // WebSocket control frames don't carry a `Frame`. We split the
                // stream, so tungstenite can't auto-answer a peer's Ping for us;
                // flag it for the writer task to Pong (the writer owns the socket
                // now). Only Binary/Text data frames carry a MessagePack `Frame`
                // (see `send_frame`).
                let payload = match &message {
                    Message::Binary(bytes) => bytes.as_ref(),
                    Message::Text(text) => text.as_bytes(),
                    Message::Ping(_) => {
                        pong_requested.store(true, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    }
                    _ => continue,
                };
                let frame: Frame = match rmp_serde::from_slice(payload) {
                    Ok(frame) => frame,
                    Err(error) => {
                        log::error!(
                            "Failed to deserialize inbound Frame from {peer_name}: {error}"
                        );
                        continue;
                    }
                };
                match frame {
                    Frame::Change(change) => {
                        if let Err(error) = change_sender.send(CatalogCommand::Change(
                            Ingest::from_change(change),
                            ChangeOrigin::Peer {
                                public_key: peer_public_key.to_owned(),
                            },
                        )) {
                            log::error!(
                                "change_sender closed; cannot dispatch inbound Change: {error}"
                            );
                            break;
                        }
                    }
                    Frame::Sync(SyncMessage::Manifest { entries }) => {
                        // A peer's file manifest may arrive split across several
                        // `Manifest` frames (see the send site's `batch_manifest`).
                        // Each frame is reconciled independently here: reconciliation
                        // is per-entry and additive, and nothing treats a frame as
                        // "the complete set" (deletions are explicit per-entry flags,
                        // and the placement sweep below iterates only this frame's
                        // `announced_file_ids`). Do not add any cross-frame or
                        // whole-catalog assumption to this arm.
                        //
                        // Confirm the peer is registered (so the content
                        // receives below have a live link to drive) before running the
                        // synchronous reconciliation. Doing the DB work outside
                        // of any held `RwLockReadGuard` keeps this future `Send`
                        // (CatalogStore isn't Sync).
                        let peer_registered = runtime_configuration
                            .read()
                            .await
                            .peers
                            .get(peer_public_key)
                            .and_then(|runtime_peer| runtime_peer.outbound.clone())
                            .is_some();

                        if !peer_registered {
                            log::warn!(
                                "No outbound channel registered for {peer_name}; \
                                 cannot reconcile manifest"
                            );
                            continue;
                        }

                        // Capture the announced file_ids before `plan_file_sync`
                        // consumes `entries`; used for the placement sweep below.
                        let announced_file_ids: Vec<FileId> =
                            entries.iter().map(|entry| entry.file_id).collect();

                        let reconciling = operations.begin(
                            operations::OperationKind::reconciling_manifest(peer_name),
                        );
                        let SyncPlan {
                            pulls,
                            deletions,
                            restores,
                            moves,
                        } = plan_file_sync(peer_name, entries, &database);
                        reconciling.complete();

                        // Apply peer deletions that won last-writer-wins by
                        // enqueuing them through the sole DB writer.
                        for PeerDeletion {
                            file_id,
                            deleted_at,
                        } in deletions
                        {
                            if let Err(error) =
                                change_sender.send(CatalogCommand::Change(
                                    Ingest::from_change(Change::FileDeleted {
                                        file_id,
                                        deleted_at,
                                    }),
                                    ChangeOrigin::Peer {
                                        public_key: peer_public_key.to_owned(),
                                    },
                                ))
                            {
                                log::error!(
                                    "Reconciliation: failed to enqueue delete for {} \
                                     announced by {peer_name}: {error}",
                                    file_id.to_string()
                                );
                            }
                        }

                        // Apply peer restores that won last-writer-wins by
                        // enqueuing them as `Change::FileRestored` through the
                        // sole DB writer. This reuses the live-restore handler
                        // (three-way LWW guard, tombstone clear, byte pull,
                        // forward) — the offline-restore catch-up.
                        for PeerRestore {
                            file_id,
                            restored_at,
                            content_hash,
                            size,
                        } in restores
                        {
                            if let Err(error) =
                                change_sender.send(CatalogCommand::Change(
                                    Ingest::from_change(Change::FileRestored {
                                        file_id,
                                        content_hash,
                                        size,
                                        restored_at,
                                    }),
                                    ChangeOrigin::Peer {
                                        public_key: peer_public_key.to_owned(),
                                    },
                                ))
                            {
                                log::error!(
                                    "Reconciliation: failed to enqueue restore for {} \
                                     announced by {peer_name}: {error}",
                                    file_id.to_string()
                                );
                            }
                        }

                        // Apply peer moves that won last-writer-wins by enqueuing
                        // them as `Change::FileMoved` through the sole DB writer.
                        // This reuses the live-move handler, which re-applies the
                        // LWW guard, repositions the bytes in matching sync
                        // directories, and forwards. This is the offline-move
                        // catch-up: a rename made while we were disconnected.
                        for PeerMove {
                            file_id,
                            logical_path,
                            modified_at,
                        } in moves
                        {
                            if let Err(error) =
                                change_sender.send(CatalogCommand::Change(
                                    Ingest::from_change(Change::FileMoved {
                                        file_id,
                                        logical_path,
                                        modified_at,
                                    }),
                                    ChangeOrigin::Peer {
                                        public_key: peer_public_key.to_owned(),
                                    },
                                ))
                            {
                                log::error!(
                                    "Reconciliation: failed to enqueue move for {} \
                                     announced by {peer_name}: {error}",
                                    file_id.to_string()
                                );
                            }
                        }

                        // Files we are pulling as a result of catalog reconciliation
                        // (below); excluded from the placement sweep so we do not
                        // double-fetch them.
                        let pulling: HashSet<FileId> =
                            pulls.iter().map(|pull| pull.file_id).collect();
                        // Start a content-addressed receive for each wanted
                        // file, directing chunk requests toward this peer; it
                        // serves the canonical chunks it holds and any other
                        // holder can serve the rest via the relay. `placement`
                        // is `Create` for files we've never seen (using the
                        // manifest's `logical_path`) and `Change` for files we
                        // already know — see `plan_file_sync`.
                        for MissingContent {
                            file_id,
                            content_hash,
                            size,
                            logical_path_modified_at,
                            placement,
                        } in pulls
                        {
                            // Resolve the file's logical identity for the catalog
                            // write: from the placement for a `Create` (the file
                            // is new to us), or from the DB for a `Change` (we
                            // already know it). A missing logical path for a
                            // `Change` should not happen, but if it does we skip.
                            let logical_path = match &placement {
                                messages::MaterializePlacement::Create { logical_path, .. } => {
                                    logical_path.clone()
                                }
                                messages::MaterializePlacement::Change => {
                                    match database.logical_path_for_file_id(file_id) {
                                        Ok(logical_path) => logical_path,
                                        Err(error) => {
                                            log::error!(
                                                "Reconciliation: no logical path for known \
                                                 file {} ({error:?}); skipping",
                                                file_id.to_string()
                                            );
                                            continue;
                                        }
                                    }
                                }
                            };

                            // Hand the catalog write (files row + version) to
                            // `handle_changes`, the sole main-DB writer, rather
                            // than writing on this session's own connection. The
                            // byte pull below is a transfer, not a DB write, so it
                            // stays here. `file_versions` is byte-independent, so
                            // cataloging happens whether or not the pull completes.
                            if let Err(error) = change_sender.send(CatalogCommand::CatalogFile {
                                file_id,
                                logical_path,
                                logical_path_modified_at,
                                content_hash: content_hash.clone(),
                                size: size as u64,
                                origin: ChangeOrigin::Peer {
                                    public_key: peer_public_key.to_owned(),
                                },
                            }) {
                                log::error!(
                                    "Reconciliation: failed to enqueue catalog write for {} \
                                     announced by {peer_name}: {error}",
                                    file_id.to_string()
                                );
                                continue;
                            }
                            let purpose = ReceiverPurpose {
                                file_id,
                                content_hash: content_hash.clone(),
                                origin: ChangeOrigin::Peer {
                                    public_key: peer_public_key.to_owned(),
                                },
                                placement,
                            };
                            start_pull(file_id, content_hash, size as u64, purpose).await;
                        }

                        // Placement sweep: for every announced file whose catalog
                        // version already matched (so it was NOT in `wanted` and no
                        // pull was started), ask `handle_changes` to re-run tag
                        // placement. If a local TagBased sync directory now wants
                        // the file but we do not hold the bytes, that fetches them
                        // on demand — the connect-time counterpart to the live
                        // `FileTagged` recovery path. Files we are already pulling
                        // are skipped to avoid double-fetching.
                        //
                        // We deliberately hand this to `handle_changes` via the bus
                        // rather than fetching here: the fetch floods
                        // `ChunkRequest`s and awaits `ChunkData` replies that
                        // arrive as inbound frames on *this* session's select loop.
                        // Awaiting inline would block that loop and deadlock the
                        // fetch. Note: tag relationships from the peer's
                        // `TagManifest` apply asynchronously, so files not yet
                        // tagged are covered later by the live `FileTagged`
                        // handler; this sweep proactively covers files already
                        // tagged locally (e.g. from a prior session).
                        for file_id in announced_file_ids {
                            if pulling.contains(&file_id) {
                                continue;
                            }
                            if let Err(error) = change_sender
                                .send(CatalogCommand::ReconcilePlacement { file_id })
                            {
                                log::error!(
                                    "Reconciliation: failed to enqueue placement sweep \
                                     for {}: {error}",
                                    file_id.to_string()
                                );
                            }
                        }
                    }
                    // A peer asks us for the canonical chunk at `offset` of
                    // `file_id`/`content_hash`. If a local source (a sync
                    // directory or a temporary provider) verifies against
                    // `content_hash`, answer `ChunkData` directly; otherwise
                    // relay the request to our other neighbours (the relay fans
                    // the eventual reply back). A relay holds no bytes.
                    Frame::Sync(SyncMessage::ChunkRequest {
                        file_id,
                        content_hash,
                        offset,
                    }) => {
                        let short = content_hash.get(..8).unwrap_or(&content_hash);
                        log::debug!(
                            "peer[{peer_name}] <- ChunkRequest {} [{short}] offset={offset}",
                            file_id.to_string()
                        );
                        let answer = answer_local_chunk(
                            &command_sender,
                            &pending_fetches,
                            &verified_hashes,
                            file_id,
                            &content_hash,
                            offset,
                        )
                        .await;

                        match answer {
                            Some(ChunkAnswer::Data(bytes)) => {
                                log::debug!(
                                    "peer[{peer_name}] -> ChunkData [{short}] offset={offset} ({} bytes) served locally",
                                    bytes.len()
                                );
                                let _ = our_sender.send(Frame::Sync(SyncMessage::ChunkData {
                                    file_id,
                                    content_hash,
                                    offset,
                                    bytes,
                                }));
                            }
                            // We hold the content but it does not verify (an
                            // impossible-for-a-consistent-catalog case) — treat
                            // as absent and relay so another holder can serve it.
                            Some(ChunkAnswer::Miss) | None => {
                                log::debug!(
                                    "peer[{peer_name}]: [{short}] offset={offset} not served locally; relaying"
                                );
                                pending_fetches
                                    .relay_chunk_request(
                                        peer_public_key,
                                        file_id,
                                        content_hash,
                                        offset,
                                    )
                                    .await;
                            }
                        }
                    }
                    // Reply bytes arriving from an upstream: fan to every
                    // downstream waiter (local receives and relayed peers) for
                    // this key via the content-keyed table.
                    Frame::Sync(SyncMessage::ChunkData {
                        file_id,
                        content_hash,
                        offset,
                        bytes,
                    }) => {
                        log::debug!(
                            "peer[{peer_name}] <- ChunkData {} [{}] offset={offset} ({} bytes)",
                            file_id.to_string(),
                            content_hash.get(..8).unwrap_or(&content_hash),
                            bytes.len()
                        );
                        pending_fetches
                            .handle_chunk_data(file_id, content_hash, offset, bytes)
                            .await;
                    }
                    Frame::Sync(SyncMessage::ChunkMiss {
                        file_id,
                        content_hash,
                        offset,
                    }) => {
                        log::debug!(
                            "peer[{peer_name}] <- ChunkMiss {} [{}] offset={offset}",
                            file_id.to_string(),
                            content_hash.get(..8).unwrap_or(&content_hash)
                        );
                        pending_fetches
                            .handle_chunk_miss(peer_public_key, file_id, content_hash, offset)
                            .await;
                    }
                    Frame::Sync(SyncMessage::TagManifest {
                        definitions,
                        relationships,
                    }) => {
                        // Relationships carry their whole state (including the
                        // soft-delete flag), so apply them directly via the bus
                        // — last-writer-wins is enforced in the DB layer. For
                        // definitions, request the full payload of any the peer
                        // has newer than (or that are unknown to) us.
                        let outbound = runtime_configuration
                            .read()
                            .await
                            .peers
                            .get(peer_public_key)
                            .and_then(|runtime_peer| runtime_peer.outbound.clone());

                        let Some(outbound) = outbound else {
                            log::warn!(
                                "Peer {peer_name} is not connected; \
                                 not responding to TagManifest"
                            );
                            continue;
                        };

                        let reconciling_tags = operations
                            .begin(operations::OperationKind::reconciling_tags(peer_name));

                        plan_tag_sync(
                            peer_name,
                            peer_public_key,
                            definitions,
                            relationships,
                            &database,
                            &outbound,
                            &change_sender,
                        );
                        reconciling_tags.complete();
                    }
                    Frame::Sync(SyncMessage::TagRequest { tag_id }) => {
                        // Answer with the full tag definition as a
                        // `Change::TagAdded`. `TagNotFound` if we no longer hold
                        // the tag.
                        let outbound = runtime_configuration
                            .read()
                            .await
                            .peers
                            .get(peer_public_key)
                            .and_then(|runtime_peer| runtime_peer.outbound.clone());

                        let Some(outbound) = outbound else {
                            log::warn!(
                                "Peer {peer_name} is not connected; \
                                 not responding to TagRequest"
                            );
                            continue;
                        };

                        let frame = build_tag_request_response(peer_name, tag_id, &database);

                        if let Err(error) = outbound.send(frame) {
                            log::warn!(
                                "Failed to enqueue tag Sync response for {peer_name}: {error}"
                            );
                        }
                    }
                    Frame::Sync(SyncMessage::TagNotFound { tag_id }) => {
                        log::warn!(
                            "Peer {peer_name} reported TagNotFound for tag {}",
                            tag_id.to_string()
                        );
                    }
                    Frame::Sync(SyncMessage::PreviewRequest {
                        file_id,
                        content_hash,
                    }) => {
                        // Answer a peer's preview request in three tiers:
                        //   1. Cache hit — serve it. Always available (a DB read,
                        //      no generation support needed), so even a `Never`
                        //      device serves previews it fetched earlier.
                        //   2. Cache miss + we can generate + bytes are local —
                        //      generate, serve (and it gets cached when we, or
                        //      the requester, next resolve it).
                        //   3. Otherwise — relay across the tree.
                        // We answer `PreviewData` even for `Preview::None`: a
                        // holder that has the bytes but they are un-previewable
                        // is authoritative, so downstream caches that negative
                        // result rather than re-asking.
                        let short = content_hash.get(..8).unwrap_or(&content_hash);

                        // Tier 1: our cache. `preview_for` is a read on this
                        // session's read-only DB handle.
                        let cached = match database.preview_for(file_id, &content_hash) {
                            Ok(cached) => cached,
                            Err(error) => {
                                log::debug!(
                                    "peer[{peer_name}]: [{short}] preview cache lookup failed: \
                                     {error:?}; treating as miss"
                                );
                                None
                            }
                        };

                        if let Some(preview) = cached {
                            log::debug!(
                                "peer[{peer_name}]: served cached PreviewData {} [{short}]",
                                file_id.to_string()
                            );
                            let _ = our_sender.send(Frame::Sync(SyncMessage::PreviewData {
                                file_id,
                                content_hash,
                                preview,
                            }));
                            continue;
                        }

                        // Tier 2: generate from local bytes, if this device may
                        // generate and holds the content. Delegated to a
                        // `cfg`-gated helper so the feature-specific machinery
                        // (and its use of `can_generate_previews` /
                        // `command_sender`) lives outside this match arm and
                        // compiles away entirely without the feature.
                        //
                        // The extension (a type-detection hint) is looked up
                        // here from the session's DB handle; only meaningful
                        // when we can generate.
                        #[cfg(feature = "preview-generation")]
                        let extension = if can_generate_previews {
                            preview_extension_for(&database, file_id)
                        } else {
                            None
                        };
                        #[cfg(not(feature = "preview-generation"))]
                        let extension: Option<String> = None;
                        let served = try_serve_generated_preview(
                            &our_sender,
                            &command_sender,
                            can_generate_previews,
                            peer_name,
                            file_id,
                            &content_hash,
                            extension,
                        )
                        .await;

                        // Tier 3: relay to other neighbours.
                        if !served {
                            log::debug!(
                                "peer[{peer_name}]: [{short}] preview not served locally; relaying"
                            );
                            pending_previews
                                .relay_preview_request(peer_public_key, file_id, content_hash)
                                .await;
                        }
                    }
                    Frame::Sync(SyncMessage::PreviewData {
                        file_id,
                        content_hash,
                        preview,
                    }) => {
                        pending_previews
                            .handle_preview_data(file_id, content_hash, preview)
                            .await;
                    }
                    Frame::Sync(SyncMessage::PreviewMiss {
                        file_id,
                        content_hash,
                    }) => {
                        pending_previews
                            .handle_preview_miss(peer_public_key, file_id, content_hash)
                            .await;
                    }
                }
            }
        }
    }

    // Tell the writer task to stop (it owns the socket) and wait for it to
    // finish so the connection is fully closed before we return. Cancelling
    // `link_dead` is idempotent — if the writer already cancelled it (its own
    // write failure is what ended our loop), this is a no-op.
    link_dead.cancel();
    if let Err(error) = writer_handle.await {
        log::debug!("Writer task for {peer_name} ended abnormally: {error}");
    }

    clear_outbound_if_owned(
        &runtime_configuration,
        peer_public_key,
        owns_outbound,
        &our_sender,
    )
    .await;

    // This link is gone: prune it from the relay's waiter table so any chunk
    // key that was only reachable through it fails its downstream waiters
    // (rather than hanging until the TTL) and any request it was waiting on is
    // forgotten.
    pending_fetches.prune_link(peer_public_key).await;
    // Same for the preview relay: any preview key only reachable through this
    // link fails its downstream waiters (resolving them to `None`) rather than
    // hanging until the TTL.
    pending_previews.prune_link(peer_public_key).await;
}

/// The sole owner of a connection's socket write half.
///
/// Every byte sent to the peer goes through here: queued outbound `Frame`s
/// (drained from `peer_rx`), keepalive pings, and pongs answering the peer's
/// pings. Keeping all writes on this one task is what prevents the read side
/// (the session loop) from ever blocking on a write — the bug that deadlocked
/// large initial manifests.
///
/// Liveness lives here too: on each tick it emits a ping *only if the link has
/// been otherwise idle* (so a busy transfer doesn't add ping noise), answers
/// any parked pong, and — if the peer has been silent past `LIVENESS_TIMEOUT` —
/// cancels `link_dead` to end the session. Any socket write failure likewise
/// cancels `link_dead`, which breaks the reader loop and triggers teardown.
async fn run_writer<S>(
    mut outgoing: SplitSink<WebSocketStream<S>, Message>,
    mut peer_rx: tokio::sync::mpsc::UnboundedReceiver<Frame>,
    session_start: tokio::time::Instant,
    last_activity: Arc<std::sync::atomic::AtomicU64>,
    pong_requested: Arc<std::sync::atomic::AtomicBool>,
    link_dead: CancellationToken,
    peer_name: &str,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use std::sync::atomic::Ordering;
    // A WebSocket-over-TCP link that dies silently (flight mode, cable pull,
    // router reboot) never delivers a close frame. We ping periodically and, if
    // the peer goes silent past `LIVENESS_TIMEOUT`, end the session so the
    // connection guard drops and a `Disconnected` is broadcast. An outbound
    // peer's `dial.rs` loop then reconnects; there is no in-session retry.
    const PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);
    const LIVENESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);
    // Tick faster than PING_INTERVAL so a parked pong is answered promptly
    // rather than waiting up to a full ping period.
    const TICK: std::time::Duration = std::time::Duration::from_secs(5);

    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // skip the immediate first tick

    // When we last wrote anything real (a frame or a ping): drives the
    // "ping only when idle" decision.
    let mut last_write = tokio::time::Instant::now();

    loop {
        tokio::select! {
            _ = link_dead.cancelled() => {
                // Reader ended the session (peer closed / shutdown / read
                // error); nothing more to write.
                break;
            }
            outbound = peer_rx.recv() => {
                let Some(frame) = outbound else {
                    // All senders dropped (teardown / replaced); done.
                    break;
                };
                if let Err(error) = send_frame(&mut outgoing, &frame).await {
                    log::warn!("Outbound send to {peer_name} failed: {error}");
                    link_dead.cancel();
                    break;
                }
                last_write = tokio::time::Instant::now();
            }
            _ = tick.tick() => {
                // Answer a requested ping first — cheap and keeps the peer's
                // liveness clock fresh. An empty pong payload is RFC-legal; we
                // don't echo the ping's bytes.
                if pong_requested.swap(false, Ordering::Relaxed) {
                    if let Err(error) = outgoing.send(Message::Pong(Vec::new().into())).await {
                        log::info!("Pong to {peer_name} failed ({error}); closing session");
                        link_dead.cancel();
                        break;
                    }
                    last_write = tokio::time::Instant::now();
                }

                // Dead-peer check: has the peer been silent too long? Compare the
                // reader's last-activity stamp (ms since `session_start`) against
                // now.
                let now_ms = session_start.elapsed().as_millis() as u64;
                let idle_ms = now_ms.saturating_sub(last_activity.load(Ordering::Relaxed));
                if idle_ms >= LIVENESS_TIMEOUT.as_millis() as u64 {
                    log::info!("Peer {peer_name} silent past liveness timeout; closing session");
                    link_dead.cancel();
                    break;
                }

                // Ping only when the link is otherwise idle: if we've written
                // something (a frame or pong) within PING_INTERVAL, that traffic
                // already proves liveness and doubles as the peer's keepalive.
                if last_write.elapsed() >= PING_INTERVAL {
                    if let Err(error) = outgoing.send(Message::Ping(Vec::new().into())).await {
                        log::info!("Ping to {peer_name} failed ({error}); closing session");
                        link_dead.cancel();
                        break;
                    }
                    last_write = tokio::time::Instant::now();
                }
            }
        }
    }
}

pub(crate) async fn send_frame<S>(
    outgoing: &mut SplitSink<WebSocketStream<S>, Message>,
    frame: &Frame,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // Peer `Frame`s are encoded as MessagePack and sent as binary WebSocket
    // frames. This avoids serde_json's `Vec<u8>` -> array-of-integers blowup
    // (~4x on the wire), which dominated the payload for file transfers.
    let bytes = rmp_serde::to_vec_named(frame).map_err(|e| format!("serialize: {e}"))?;
    outgoing
        .send(Message::binary(bytes))
        .await
        .map_err(|e| format!("send: {e}"))
}

async fn clear_outbound_if_owned(
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    peer_public_key: &str,
    owns_outbound: bool,
    our_sender: &UnboundedSender<Frame>,
) {
    if !owns_outbound {
        return;
    }
    let mut runtime = runtime_configuration.write().await;
    if let Some(runtime_peer) = runtime.peers.get_mut(peer_public_key)
        && let Some(current) = runtime_peer.outbound.as_ref()
        && current.same_channel(our_sender)
    {
        // The slot still holds the sender we installed (no sibling session
        // replaced it). Drop it so the next session sees a free slot.
        //
        // We deliberately do not check `is_closed()` here: that check is
        // unreliable while we (the receiver's owner) are still alive, and
        // pointless once we're not. Identity via `same_channel` is the only
        // reliable test.
        runtime_peer.outbound = None;
        // The command channel is installed and cleared in lockstep with
        // `outbound` (same owner), so clear it here too.
        runtime_peer.commands = None;
    }
}
