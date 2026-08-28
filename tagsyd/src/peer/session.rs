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
    MissingContent, PeerDeletion, PeerMove, PeerRestore, SyncPlan, build_local_manifest,
    plan_file_sync,
};
use crate::peer::plan_tags::{build_local_tag_manifest, build_tag_request_response, plan_tag_sync};
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
    mut outgoing: SplitSink<WebSocketStream<S>, Message>,
    mut incoming: SplitStream<WebSocketStream<S>>,
    direction: operations::Direction,
    context: PeerContext,
    shutdown: &CancellationToken,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
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

    let (peer_tx, mut peer_rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
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
    let start_pull = {
        let pending_fetches = pending_fetches.clone();
        let receiver_done_tx = receiver_done_tx.clone();
        let transfer_temp_dir = transfer_temp_dir.clone();
        let operations = operations.clone();
        let peer_name = peer_name.to_owned();
        let peer_public_key = peer_public_key.to_owned();

        move |file_id: FileId,
              content_hash: String,
              expected_size: u64,
              purpose: ReceiverPurpose| {
            let temp_path = transfer_temp_dir.join(uuid::Uuid::new_v4().to_string());

            // Surface this pull as a live "receiving file" operation with byte
            // progress. The handle lives on the bridge task below and reaches a
            // terminal state from the receive outcome.
            let receiving = operations.begin(operations::OperationKind::receiving_file(
                file_id, &peer_name,
            ));
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
                content_hash,
                expected_size,
                temp_path,
                Some(peer_public_key.clone()),
                Some(progress),
            );

            let done_tx = receiver_done_tx.clone();
            tokio::spawn(async move {
                if let Ok(outcome) = outcome_rx.await {
                    match &outcome {
                        ReceiveOutcome::Complete(_) => receiving.complete(),
                        ReceiveOutcome::Failed(error) => receiving.fail(error.to_string()),
                    }
                    let _ = done_tx.send((purpose, outcome));
                }
                // If `outcome_rx` closed without a value, `receiving` drops
                // here and the operation is marked aborted.
            });
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
    match build_local_tag_manifest(&database) {
        Ok((definitions, relationships)) => {
            let frame = Frame::Sync(SyncMessage::TagManifest {
                definitions,
                relationships,
            });
            if let Err(error) = send_frame(&mut outgoing, &frame).await {
                log::warn!("Failed to send initial tag manifest to {peer_name}: {error}");
                clear_outbound_if_owned(
                    &runtime_configuration,
                    peer_public_key,
                    owns_outbound,
                    &our_sender,
                )
                .await;
                return;
            }
            log::debug!("Sent initial tag manifest to {peer_name}");
        }
        Err(error) => {
            log::error!("Peer {peer_name}: failed to build initial tag manifest: {error:?}");
        }
    }

    // Send our file manifest right after the tag manifest (see the ordering
    // rationale above). The peer compares it against their own history and
    // requests anything they need.
    match build_local_manifest(&database) {
        Ok(manifest) => {
            let frame = Frame::Sync(SyncMessage::Manifest { entries: manifest });
            if let Err(error) = send_frame(&mut outgoing, &frame).await {
                log::warn!("Failed to send initial manifest to {peer_name}: {error}");
                clear_outbound_if_owned(
                    &runtime_configuration,
                    peer_public_key,
                    owns_outbound,
                    &our_sender,
                )
                .await;
                return;
            }
            log::debug!("Sent initial manifest to {peer_name}");
        }
        Err(error) => {
            log::error!("Peer {peer_name}: failed to build initial manifest: {error:?}");
            // Continue without manifest; the peer's manifest still drives
            // anything they need to receive from us.
        }
    }

    // Session liveness. A WebSocket-over-TCP link that dies silently (flight
    // mode, cable pull, router reboot) never delivers a close frame, so
    // `incoming.next()` would block forever and the session — and thus the
    // connection guard, and thus the UI's "connected" state — would hang
    // indefinitely. This is the idle-session analogue of the transfer path's
    // `HOP_TIMEOUT`: we ping periodically and, if the peer goes silent past
    // `LIVENESS_TIMEOUT`, break the loop so the guard drops and a `Disconnected`
    // is broadcast. An outbound peer's `dial.rs` loop then reconnects when the
    // network returns; there is no in-session retry (see the transport notes in
    // AGENTS.md).
    const PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);
    const LIVENESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);

    let mut ping_interval = tokio::time::interval(PING_INTERVAL);
    // If the loop is busy past a tick (e.g. a long transfer), don't burst a
    // backlog of pings afterward — just resume the cadence.
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick fires immediately; skip it so we don't ping before the
    // link has had a chance to carry traffic.
    ping_interval.tick().await;

    let mut last_activity = tokio::time::Instant::now();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                log::info!("Shutdown requested; closing session with {peer_name}");
                break;
            }
            _ = ping_interval.tick() => {
                if last_activity.elapsed() >= LIVENESS_TIMEOUT {
                    log::info!(
                        "Peer {peer_name} silent for {:?}; closing dead session",
                        last_activity.elapsed()
                    );
                    break;
                }

                // Keep the link warm and surface a dead peer on the write path
                // even while idle. A failed ping send means the socket is gone.
                if let Err(error) = outgoing.send(Message::Ping(Vec::new().into())).await {
                    log::info!("Ping to {peer_name} failed ({error}); closing session");
                    break;
                }
            }
            outbound = peer_rx.recv() => {
                let Some(frame) = outbound else {
                    // Sender dropped (cleared during teardown or replaced).
                    break;
                };
                if let Err(error) = send_frame(&mut outgoing, &frame).await {
                    log::warn!("Outbound send to {peer_name} failed: {error}");
                    break;
                }
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
                        start_pull(file_id, content_hash, expected_size, purpose);
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
                // liveness clock. (tokio-tungstenite auto-answers our pings with
                // pongs, which surface here.)
                last_activity = tokio::time::Instant::now();
                // WebSocket control frames don't carry a `Frame`. We split the
                // stream, so tungstenite can't auto-answer a peer's Ping for us;
                // reply with a Pong ourselves (proper keepalive behavior) and
                // otherwise ignore control frames. Only Binary/Text data frames
                // carry a MessagePack `Frame` (see `send_frame`).
                let payload = match &message {
                    Message::Binary(bytes) => bytes.as_ref(),
                    Message::Text(text) => text.as_bytes(),
                    Message::Ping(data) => {
                        if let Err(error) =
                            outgoing.send(Message::Pong(data.clone())).await
                        {
                            log::info!("Pong to {peer_name} failed ({error}); closing session");
                            break;
                        }
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
                            start_pull(file_id, content_hash, size as u64, purpose);
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
