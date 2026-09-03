//! Daemon side of the control channel: bind the socket, accept connections,
//! decode [`ControlRequest`]s, dispatch each to the in-process [`ApiService`],
//! and stream [`ApiEvent`]s / operation events back.

use std::collections::HashMap;
use std::path::PathBuf;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tagsy_core::FileId;
use tagsy_ipc::{ControlFrame, ControlRequest, ControlResponse, decode_frame, encode_frame};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_util::sync::CancellationToken;

use crate::frontend::api::{ApiEvent, ApiService};
use crate::transport::{
    ConnectionStream, ConnectionUpdate, EventStream, OperationStream, OperationUpdate,
};

/// Bind the control socket and serve control clients until `shutdown` fires.
///
/// Binds a [`UnixListener`] at `socket_path`, removing any stale socket file
/// left by a previous run first (a leftover socket makes `bind` fail with
/// `AddrInUse`). Each accepted connection is handled on its own task by
/// [`handle_control_connection`]; all share the one in-process [`ApiService`].
///
/// This is wired into the section-3 shutdown path by the caller: it runs inside
/// the runtime driver's `select!` and returns when `shutdown` is cancelled, at
/// which point the socket file is removed on the way out.
pub async fn serve_control(
    api: ApiService,
    socket_path: PathBuf,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    // A leftover socket file (unclean shutdown) would make bind() fail with
    // AddrInUse. It is safe to remove: the runtime directory (/run/tagsy) is
    // owned by the single service user, and a second live daemon for the same
    // user is not a supported configuration. (systemd's RuntimeDirectory
    // normally clears this on start; removing it here also covers non-systemd
    // launches.)
    if socket_path.exists()
        && let Err(error) = tokio::fs::remove_file(&socket_path).await
    {
        log::warn!(
            "Failed to remove stale control socket {}: {error}",
            socket_path.display()
        );
    }

    let listener = UnixListener::bind(&socket_path)?;
    log::info!("Control socket listening on {}", socket_path.display());

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                log::info!("Shutdown requested; stopping control socket");
                break;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _address)) => {
                        tokio::spawn(handle_control_connection(
                            api.clone(),
                            stream,
                            shutdown.child_token(),
                        ));
                    }
                    Err(error) => {
                        log::warn!("Control socket accept error: {error}");
                        break;
                    }
                }
            }
        }
    }

    // Best-effort cleanup so the next daemon start binds cleanly.
    let _ = tokio::fs::remove_file(&socket_path).await;
    Ok(())
}

/// Serve a single control client for the life of its connection.
///
/// Completes the WebSocket handshake over the Unix stream, then loops:
/// - decode inbound [`ControlFrame::Request`]s, dispatch to the [`ApiService`],
///   and reply with a [`ControlFrame::Response`];
/// - on [`ControlRequest::Subscribe`], subscribe to the [`ApiService`] event
///   stream and forward every [`ApiEvent`] as a [`ControlFrame::Event`].
///
/// Only one subscription per connection is needed (the UI subscribes once);
/// a second `Subscribe` simply replaces the stream.
async fn handle_control_connection(
    api: ApiService,
    stream: UnixStream,
    shutdown: CancellationToken,
) {
    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws_stream) => ws_stream,
        Err(error) => {
            log::warn!("Control WebSocket handshake failed: {error}");
            return;
        }
    };
    log::debug!("Control client connected");

    let (mut outgoing, mut incoming) = ws_stream.split();

    // Populated once the client sends `Subscribe`. `None` until then so an
    // un-subscribed connection never wakes on the event branch.
    let mut events: Option<EventStream> = None;

    // Populated once the client sends `SubscribeOperations`. Independent of the
    // change-event subscription above so a client can take one, both, or
    // neither.
    let mut operation_events: Option<OperationStream> = None;

    // Populated once the client sends `SubscribeConnections`. Independent of the
    // two subscriptions above.
    let mut connection_events: Option<ConnectionStream> = None;

    // Provider protocol state for this connection. A `ProviderSource` (held by
    // the transfer subsystem) asks for a chunk by sending `(offset, reply)` on
    // `provider_req`; we assign a `chunk_id`, remember the reply oneshot, and
    // send a `ProviderChunkRequest` to the client. The client's
    // `ProviderChunkReply` resolves it. `active_provider` records what this
    // connection is currently serving so we can unregister it on disconnect.
    let (provider_req_tx, mut provider_req_rx) =
        mpsc::unbounded_channel::<crate::peer::transfer::ProviderChunkRequest>();
    let (provider_done_tx, mut provider_done_rx) = mpsc::unbounded_channel::<()>();
    let mut provider_pending: HashMap<u64, crate::peer::transfer::ProviderChunkReply> =
        HashMap::new();
    let mut next_chunk_id: u64 = 0;
    // (file_id, content_hash) currently registered as a provider on this conn.
    let mut active_provider: Option<(FileId, String)> = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                log::debug!("Shutdown requested; closing control client");
                break;
            }
            // Forward live events to a subscribed client. `recv` on a `None`
            // stream is never polled because of the `if let` guard.
            event = async { events.as_mut().unwrap().recv().await }, if events.is_some() => {
                match event {
                    Some(event) => {
                        if let Err(error) =
                            send_control(&mut outgoing, &ControlFrame::Event(event)).await
                        {
                            log::debug!("Failed to push control event: {error}");
                            break;
                        }
                    }
                    None => {
                        // The runtime's event bus closed (shutdown).
                        break;
                    }
                }
            }
            // Forward live operation events to a subscribed client. A lag on the
            // operation broadcast surfaces as `Resynced`, which we forward
            // verbatim so the client re-snapshots via `ListOperations`.
            operation = async { operation_events.as_mut().unwrap().recv().await }, if operation_events.is_some() => {
                match operation {
                    Some(OperationUpdate::Event(event)) => {
                        if let Err(error) =
                            send_control(&mut outgoing, &ControlFrame::OperationEvent(event)).await
                        {
                            log::debug!("Failed to push operation event: {error}");
                            break;
                        }
                    }
                    // A lag: the client should re-snapshot. There is no dedicated
                    // "resync operations" frame; the change-stream `Resynced`
                    // already prompts a full re-fetch of live state, so drop the
                    // marker here and let the next event catch the client up.
                    Some(OperationUpdate::Resynced) => {}
                    None => break,
                }
            }
            // Forward live connection events to a subscribed client. A lag
            // surfaces as `Resynced`, forwarded verbatim so the client
            // re-snapshots via `ConnectedPeers`.
            connection = async { connection_events.as_mut().unwrap().recv().await }, if connection_events.is_some() => {
                match connection {
                    Some(ConnectionUpdate::Event(event)) => {
                        if let Err(error) =
                            send_control(&mut outgoing, &ControlFrame::ConnectionEvent(event)).await
                        {
                            log::debug!("Failed to push connection event: {error}");
                            break;
                        }
                    }
                    // A lag: the client should re-snapshot. Mirrors the
                    // operation-stream handling above.
                    Some(ConnectionUpdate::Resynced) => {}
                    None => break,
                }
            }
            // A provider source wants a chunk from the client: forward it as a
            // `ProviderChunkRequest` and remember where to route the reply.
            request = provider_req_rx.recv() => {
                let Some((offset, reply)) = request else { continue; };
                let chunk_id = next_chunk_id;
                next_chunk_id += 1;
                provider_pending.insert(chunk_id, reply);
                if let Err(error) = send_control(
                    &mut outgoing,
                    &ControlFrame::ProviderChunkRequest { chunk_id, offset },
                )
                .await
                {
                    log::debug!("Failed to send provider chunk request: {error}");
                    break;
                }
            }
            // A transfer of the provided file completed: tell the client it may
            // release the file (via an event), and unregister the provider.
            done = provider_done_rx.recv() => {
                if done.is_none() { continue; }
                if let Some((file_id, content_hash)) = active_provider.take() {
                    api.unregister_provider(file_id, &content_hash).await;
                    if let Err(error) = send_control(
                        &mut outgoing,
                        &ControlFrame::Event(ApiEvent::ProviderReleased { file_id }),
                    )
                    .await
                    {
                        log::debug!("Failed to send provider-released event: {error}");
                        break;
                    }
                }
            }
            inbound = incoming.next() => {
                let Some(message) = inbound else {
                    log::debug!("Control client closed the connection");
                    break;
                };
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        log::debug!("Control client read error: {error}");
                        break;
                    }
                };
                // Ignore non-data frames (ping/pong/close handled by the lib).
                if message.is_ping() || message.is_pong() || message.is_close() {
                    continue;
                }
                let frame: ControlFrame = match decode_frame(&message) {
                    Ok(frame) => frame,
                    Err(error) => {
                        // A frame we cannot decode means the stream is out of
                        // sync (or the peer is buggy). Continuing to read would
                        // misinterpret subsequent bytes; close the connection so
                        // the client reconnects cleanly instead of hanging on a
                        // request whose response never comes.
                        log::warn!("Malformed control frame: {error}; closing connection");
                        break;
                    }
                };
                match frame {
                    ControlFrame::ProviderChunkReply { chunk_id, bytes, last } => {
                        if let Some(reply) = provider_pending.remove(&chunk_id) {
                            let _ = reply.send(Ok((bytes, last)));
                        } else {
                            log::warn!("Provider reply for unknown chunk id {chunk_id}");
                        }
                    }
                    ControlFrame::Request { id, request } => {
                        // Uploads/edits register a provider for this connection;
                        // capture the provider source + what it serves.
                        let response = dispatch(
                            &api,
                            request,
                            &mut events,
                            &mut operation_events,
                            &mut connection_events,
                            &provider_req_tx,
                            &provider_done_tx,
                            &mut active_provider,
                        )
                        .await;
                        if let Err(error) =
                            send_control(&mut outgoing, &ControlFrame::Response { id, response }).await
                        {
                            log::debug!("Failed to send control response: {error}");
                            break;
                        }
                    }
                    other => {
                        log::warn!("Control client sent an unexpected frame: {other:?}; ignoring");
                    }
                }
            }
        }
    }

    // Connection closing: drop any provider we registered so stale entries do
    // not linger.
    if let Some((file_id, content_hash)) = active_provider.take() {
        api.unregister_provider(file_id, &content_hash).await;
    }

    log::debug!("Control client disconnected");
}

/// Execute one [`ControlRequest`] against the in-process [`ApiService`] and
/// produce a [`ControlResponse`].
///
/// Most reads are synchronous on [`ApiService`] (each opens its own short-lived
/// read-only handle) and writes enqueue a `Change` and return immediately.
/// `FetchFile` and `LocalPathForFile` are genuinely async (a channel round-trip
/// into the daemon), so this function is `async`. Nothing holds a
/// `&CatalogStore` across an `.await`. `Subscribe` mutates the caller's
/// `events` slot.
#[allow(clippy::too_many_arguments)]
async fn dispatch(
    api: &ApiService,
    request: ControlRequest,
    events: &mut Option<EventStream>,
    operation_events: &mut Option<OperationStream>,
    connection_events: &mut Option<ConnectionStream>,
    provider_req_tx: &mpsc::UnboundedSender<crate::peer::transfer::ProviderChunkRequest>,
    provider_done_tx: &mpsc::UnboundedSender<()>,
    active_provider: &mut Option<(FileId, String)>,
) -> ControlResponse {
    match request {
        ControlRequest::ResolveFileId { prefix } => match api.resolve_file_id(&prefix) {
            Ok(file_id) => ControlResponse::FileId(file_id),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::ResolveTagId { prefix } => match api.resolve_tag_id(&prefix) {
            Ok(tag_id) => ControlResponse::TagId(tag_id),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagsForFile {
            file_id,
            subtag_rule,
        } => match api.tags_for_file(file_id, subtag_rule) {
            Ok(tag_ids) => ControlResponse::TagIds(tag_ids),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::RunQuery {
            query,
            subtag_rule,
            deleted_rule,
        } => match api.search(&query, subtag_rule, deleted_rule) {
            Ok(result) => ControlResponse::SearchResults(result),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::GetFile {
            file_id,
            deleted_rule,
        } => match api.get_file(file_id, deleted_rule) {
            Ok(file) => ControlResponse::File(file),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::Classify { name } => match api.classify(&name) {
            Ok(kind) => ControlResponse::FileKind(kind),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::GetTag {
            tag_id,
            deleted_rule,
        } => match api.get_tag(tag_id, deleted_rule) {
            Ok(tag) => ControlResponse::Tag(tag),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::SubtagsForTag {
            tag_id,
            subtag_rule,
        } => match api.subtags_for_tag(tag_id, subtag_rule) {
            Ok(tag_ids) => ControlResponse::TagIds(tag_ids),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagsForTag {
            tag_id,
            subtag_rule,
        } => match api.tags_for_tag(tag_id, subtag_rule) {
            Ok(tag_ids) => ControlResponse::TagIds(tag_ids),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::CreateTag { name, style } => match api.create_tag(name, style) {
            Ok(tag_id) => ControlResponse::TagId(tag_id),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::DeleteTag { tag_id } => match api.delete_tag(tag_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::RestoreTag { tag_id } => match api.restore_tag(tag_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::RenameTag { tag_id, name } => match api.rename_tag(tag_id, name) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::SetTagStyle { tag_id, style } => match api.set_tag_style(tag_id, style) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::UploadFile {
            path_name,
            content_hash,
            size,
            tags,
        } => {
            match api.upload_file(path_name, content_hash.clone(), size, tags) {
                Ok(file_id) => {
                    // Register this connection as the temporary provider so
                    // peers can pull the bytes on demand.
                    let source = std::sync::Arc::new(crate::peer::transfer::ProviderSource::new(
                        provider_req_tx.clone(),
                        provider_done_tx.clone(),
                    ));
                    api.register_provider(file_id, content_hash.clone(), source)
                        .await;
                    *active_provider = Some((file_id, content_hash));
                    ControlResponse::FileId(file_id)
                }
                Err(error) => ControlResponse::Error(error),
            }
        }
        ControlRequest::EditFile {
            file_id,
            content_hash,
            size,
        } => match api.edit_file(file_id, content_hash.clone(), size) {
            Ok(()) => {
                let source = std::sync::Arc::new(crate::peer::transfer::ProviderSource::new(
                    provider_req_tx.clone(),
                    provider_done_tx.clone(),
                ));
                api.register_provider(file_id, content_hash.clone(), source)
                    .await;
                *active_provider = Some((file_id, content_hash));
                ControlResponse::Ok
            }
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::FetchFile {
            file_id,
            expected_hash,
        } => match api.fetch_file(file_id, expected_hash).await {
            Ok(path) => ControlResponse::FilePath(path),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::BeginEdit { file_id } => match api.begin_edit(file_id).await {
            Ok(path) => ControlResponse::FilePath(path),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::FinishEdit { file_id, path } => {
            match api.finish_edit(file_id, path).await {
                Ok(outcome) => ControlResponse::EditOutcome(outcome),
                Err(error) => ControlResponse::Error(error),
            }
        }
        ControlRequest::CancelEdit { path } => match api.cancel_edit(path) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::GetPreview { file_id } => match api.get_preview(file_id).await {
            Ok(preview) => ControlResponse::Preview(preview),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::LocalPathForFile { file_id } => {
            match api.local_path_for_file(file_id).await {
                Ok(path) => ControlResponse::LocalPath(path),
                Err(error) => ControlResponse::Error(error),
            }
        }
        ControlRequest::StorageStats => match api.storage_stats().await {
            Ok(stats) => ControlResponse::StorageStats(stats),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::Backup => match api.backup().await {
            Ok(outcome) => ControlResponse::BackupComplete(outcome),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::DeleteFile { file_id } => match api.delete_file(file_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::RestoreFile { file_id } => match api.restore_file(file_id).await {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::MoveFile {
            file_id,
            logical_path,
        } => match api.move_file(file_id, logical_path) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagFile { tag_id, file_id } => match api.tag_file(tag_id, file_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::UntagFile { tag_id, file_id } => match api.untag_file(tag_id, file_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagTag {
            parent_id,
            subtag_id,
        } => match api.tag_tag(parent_id, subtag_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::UntagTag {
            parent_id,
            subtag_id,
        } => match api.untag_tag(parent_id, subtag_id) {
            Ok(()) => ControlResponse::Ok,
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::Subscribe => {
            *events = Some(EventStream::InProcess(api.subscribe()));
            ControlResponse::Subscribed
        }
        ControlRequest::PurgePreviews => match api.purge_previews().await {
            Ok(purged) => ControlResponse::PurgedPreviews(purged),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::EditorRules => ControlResponse::EditorRules(api.editor_rules()),
        ControlRequest::HomeSections => ControlResponse::HomeSections(api.home_sections()),
        ControlRequest::Retag { dry_run } => match api.retag(dry_run) {
            Ok(summary) => ControlResponse::Retagged(summary),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::TagRuleReport => match api.tag_rule_report() {
            Ok(report) => ControlResponse::TagRuleReport(report),
            Err(error) => ControlResponse::Error(error),
        },
        ControlRequest::ListOperations => ControlResponse::Operations(api.list_operations()),
        ControlRequest::SubscribeOperations => {
            *operation_events = Some(OperationStream::InProcess(api.subscribe_operations()));
            ControlResponse::OperationsSubscribed
        }
        ControlRequest::ConnectedPeers => ControlResponse::ConnectedPeers(api.connected_peers()),
        ControlRequest::SubscribeConnections => {
            *connection_events = Some(ConnectionStream::InProcess(api.subscribe_connections()));
            ControlResponse::ConnectionsSubscribed
        }
    }
}

async fn send_control(
    outgoing: &mut SplitSink<WebSocketStream<UnixStream>, Message>,
    frame: &ControlFrame,
) -> Result<(), String> {
    let message = encode_frame(frame)?;
    outgoing
        .send(message)
        .await
        .map_err(|error| format!("send: {error}"))
}
