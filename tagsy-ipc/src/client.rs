//! Client side of the control channel: [`IpcBackend`], the embedded Rust
//! client that connects to the daemon's control socket, serializes
//! [`Backend`] calls into [`ControlRequest`]s, and awaits the matching
//! [`ControlResponse`]. It is the Linux-daemon counterpart to
//! `InProcessBackend`; the Dart UI and the `tagsy` CLI never learn which they
//! hold.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tagsy_api::{
    ApiError, ApiEvent, Backend, BackupOutcome, DeletedRule, EditOutcome, EditorRule, EventStream,
    Operation, OperationEvent, OperationStream, RetagSummary, SearchResults, StorageStats,
    SubtagRule, Tag, TagRuleReport,
};
use tagsy_core::content::hash_and_len;
use tagsy_core::{FileId, FileInfo, Preview, TagId};
use tokio::net::UnixStream;
use tokio::sync::{Mutex, oneshot};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::protocol::{ControlFrame, ControlRequest, ControlResponse, decode_frame, encode_frame};

/// The IPC-client transport backend.
///
/// A thin embedded Rust client that connects to the daemon's control socket,
/// serializes [`Backend`] calls into [`ControlRequest`]s, and awaits
/// the matching [`ControlResponse`]. It is the Linux-daemon counterpart to
/// `InProcessBackend`; the Dart UI (and the `tagsy` CLI) never learn which
/// they hold.
///
/// A single background reader task owns the socket's read half and
/// demultiplexes inbound frames: [`ControlFrame::Response`]s are matched to
/// waiters by `id`; [`ControlFrame::Event`]s are pushed onto a broadcast
/// channel that [`subscribe`](Backend::subscribe) taps.
#[derive(Clone)]
pub struct IpcBackend {
    inner: Arc<IpcClientInner>,
}

/// Read the chunk of `path` starting at `offset` (bounded by the transfer chunk
/// size), returning the bytes and whether it reached end-of-file. Client side
/// of the provider protocol.
async fn read_provider_chunk(path: &Path, offset: u64) -> (Vec<u8>, bool) {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let chunk_size = tagsy_core::content::CHUNK_SIZE;
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) => {
            log::warn!("Provider: failed to open {}: {error}", path.display());
            return (Vec::new(), true);
        }
    };
    let total = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    let mut file = file;
    if file.seek(std::io::SeekFrom::Start(offset)).await.is_err() {
        return (Vec::new(), true);
    }
    let mut buffer = vec![0u8; chunk_size];
    let mut filled = 0;
    while filled < chunk_size {
        match file.read(&mut buffer[filled..]).await {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(error) => {
                log::warn!("Provider: read error on {}: {error}", path.display());
                return (Vec::new(), true);
            }
        }
    }
    buffer.truncate(filled);
    let last = offset + filled as u64 >= total;
    (buffer, last)
}

struct IpcClientInner {
    /// Write half of the control socket, behind a mutex so concurrent API
    /// calls serialize their frames without interleaving bytes.
    writer: Mutex<SplitSink<WebSocketStream<UnixStream>, Message>>,
    /// Correlation-id -> oneshot for the response of an in-flight request.
    pending: Mutex<HashMap<u64, oneshot::Sender<ControlResponse>>>,
    /// Monotonic request-id source.
    next_id: AtomicU64,
    /// Broadcast of events received on this connection. `subscribe` taps it.
    events: tokio::sync::broadcast::Sender<ApiEvent>,
    /// Broadcast of operation events received on this connection.
    /// `subscribe_operations` taps it.
    operation_events: tokio::sync::broadcast::Sender<OperationEvent>,
    /// The local file this client is currently serving as a temporary provider
    /// (an in-flight upload/edit). The reader task answers the daemon's
    /// `ProviderChunkRequest`s by reading chunks from this path.
    provider_path: Mutex<Option<PathBuf>>,
}

impl IpcBackend {
    /// Connect to the daemon's default control socket
    /// (`tagsy_core::paths::control_socket_path`).
    pub async fn connect_default() -> Result<Self, ApiError> {
        Self::connect(tagsy_core::paths::control_socket_path()).await
    }

    /// Connect to the daemon control socket at `socket_path`.
    ///
    /// Establishes the WebSocket handshake over the Unix stream (the daemon
    /// speaks WS to reuse the peer framing code) and spawns the demultiplexing
    /// reader task. Errors are surfaced as [`ApiError::Transport`] so the UI
    /// sees the single API error type.
    pub async fn connect(socket_path: impl AsRef<Path>) -> Result<Self, ApiError> {
        let socket_path = socket_path.as_ref();
        let stream = UnixStream::connect(socket_path).await.map_err(|error| {
            ApiError::Transport(format!(
                "connect to control socket {}: {error}",
                socket_path.display()
            ))
        })?;

        // tokio-tungstenite needs a client request even over a Unix socket;
        // the URI is a placeholder the daemon ignores (there is no routing).
        let request = "ws://localhost/"
            .into_client_request()
            .map_err(|error| ApiError::Transport(format!("build ws request: {error}")))?;
        let (ws_stream, _response) = tokio_tungstenite::client_async(request, stream)
            .await
            .map_err(|error| ApiError::Transport(format!("control ws handshake: {error}")))?;

        let (outgoing, mut incoming) = ws_stream.split();

        let (events, _) = tokio::sync::broadcast::channel(1024);
        let (operation_events, _) = tokio::sync::broadcast::channel(1024);
        let inner = Arc::new(IpcClientInner {
            writer: Mutex::new(outgoing),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            events: events.clone(),
            operation_events: operation_events.clone(),
            provider_path: Mutex::new(None),
        });

        // Reader task: demultiplex responses (to waiters) and events (to the
        // broadcast). Ends when the socket closes, waking any pending waiter
        // with a dropped sender (surfaced as a Transport error).
        let reader_inner = inner.clone();
        tokio::spawn(async move {
            while let Some(message) = incoming.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        log::debug!("Control client read error: {error}");
                        break;
                    }
                };
                if message.is_ping() || message.is_pong() || message.is_close() {
                    continue;
                }
                let frame: ControlFrame = match decode_frame(&message) {
                    Ok(frame) => frame,
                    Err(error) => {
                        // Out-of-sync stream: stop reading so pending waiters are
                        // failed (below) rather than left hanging on a response
                        // that will never be correctly decoded.
                        log::warn!("Malformed control frame from daemon: {error}; closing");
                        break;
                    }
                };
                match frame {
                    ControlFrame::Response { id, response } => {
                        if let Some(sender) = reader_inner.pending.lock().await.remove(&id) {
                            let _ = sender.send(response);
                        } else {
                            log::warn!("Control response for unknown request id {id}");
                        }
                    }
                    ControlFrame::Event(event) => {
                        // Best-effort: if no one is subscribed, drop.
                        let _ = reader_inner.events.send(event);
                    }
                    ControlFrame::OperationEvent(event) => {
                        // Best-effort: if no one is subscribed, drop.
                        let _ = reader_inner.operation_events.send(event);
                    }
                    // The daemon is pulling a chunk of the file we're currently
                    // providing (an in-flight upload/edit). Read it from the
                    // local file and reply.
                    ControlFrame::ProviderChunkRequest { chunk_id, offset } => {
                        let path = reader_inner.provider_path.lock().await.clone();
                        let (bytes, last) = match path {
                            Some(path) => read_provider_chunk(&path, offset).await,
                            None => {
                                log::warn!("Provider chunk requested but no active provider file");
                                (Vec::new(), true)
                            }
                        };
                        let reply = ControlFrame::ProviderChunkReply {
                            chunk_id,
                            bytes,
                            last,
                        };
                        let message = match encode_frame(&reply) {
                            Ok(message) => message,
                            Err(error) => {
                                log::warn!("serialize provider chunk reply: {error}");
                                continue;
                            }
                        };
                        let mut writer = reader_inner.writer.lock().await;
                        if let Err(error) = writer.send(message).await {
                            log::debug!("Failed to send provider chunk reply: {error}");
                            break;
                        }
                    }
                    ControlFrame::Request { .. } | ControlFrame::ProviderChunkReply { .. } => {
                        log::warn!("Daemon sent an unexpected frame to a client; ignoring");
                    }
                }
            }
            // Socket closed: fail every outstanding request so callers unblock.
            reader_inner.pending.lock().await.clear();
            log::debug!("Control client reader task ended");
        });

        let client = Self { inner };

        // Subscribe to the daemon's event stream **once, up front**, for the
        // whole life of the connection. The daemon only forwards `ApiEvent`s to
        // a client after it receives a `Subscribe` request (see `dispatch`);
        // without this, the reader task above would never observe any
        // `ControlFrame::Event`, so `Backend::subscribe` (which just
        // taps the local broadcast fed by that reader) would stay silent and
        // the UI would never live-update. This is the IPC-path counterpart to
        // the in-process backend, where `subscribe` reaches the live bus
        // directly. Repeated UI-side `subscribe` calls then share this one
        // daemon subscription.
        match client.call(ControlRequest::Subscribe).await? {
            ControlResponse::Subscribed => {}
            other => return Err(unexpected(other)),
        }

        // Likewise subscribe to the operation stream once, up front, so the
        // reader task observes `ControlFrame::OperationEvent`s and the local
        // `operation_events` broadcast is fed for `subscribe_operations` taps.
        match client.call(ControlRequest::SubscribeOperations).await? {
            ControlResponse::OperationsSubscribed => {}
            other => return Err(unexpected(other)),
        }

        Ok(client)
    }

    /// Send a request and await its response, correlating by a fresh id.
    async fn call(&self, request: ControlRequest) -> Result<ControlResponse, ApiError> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.inner.pending.lock().await.insert(id, sender);

        let frame = ControlFrame::Request { id, request };
        let message = encode_frame(&frame)
            .map_err(|error| ApiError::Transport(format!("serialize request: {error}")))?;
        {
            let mut writer = self.inner.writer.lock().await;
            writer
                .send(message)
                .await
                .map_err(|error| ApiError::Transport(format!("send request: {error}")))?;
        }

        receiver.await.map_err(|_| {
            ApiError::Transport("control connection closed before response".to_owned())
        })
    }
}

/// Block until the daemon reports `file_id` has been handed off
/// ([`ApiEvent::ProviderReleased`]), or the event stream ends.
async fn wait_for_release(
    events: &mut tokio::sync::broadcast::Receiver<ApiEvent>,
    file_id: FileId,
) {
    loop {
        match events.recv().await {
            Ok(ApiEvent::ProviderReleased { file_id: released }) if released == file_id => {
                return;
            }
            Ok(_) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// Collapse a [`ControlResponse`] error variant into the `Result` the
/// [`Backend`] surface expects; otherwise report the unexpected shape.
fn unexpected(response: ControlResponse) -> ApiError {
    match response {
        ControlResponse::Error(error) => error,
        other => ApiError::Transport(format!("unexpected control response: {other:?}")),
    }
}

// The `Backend` trait declares each method as
// `-> impl Future<...> + Send`; a plain `async fn` in the impl satisfies that
// (matching `InProcessBackend`). Each call maps 1:1 onto a `ControlRequest`
// and pattern-matches the expected `ControlResponse`, treating anything else
// (including `ControlResponse::Error`) via `unexpected`.
impl Backend for IpcBackend {
    async fn resolve_file_id(&self, prefix: String) -> Result<FileId, ApiError> {
        match self.call(ControlRequest::ResolveFileId { prefix }).await? {
            ControlResponse::FileId(file_id) => Ok(file_id),
            other => Err(unexpected(other)),
        }
    }

    async fn resolve_tag_id(&self, prefix: String) -> Result<TagId, ApiError> {
        match self.call(ControlRequest::ResolveTagId { prefix }).await? {
            ControlResponse::TagId(tag_id) => Ok(tag_id),
            other => Err(unexpected(other)),
        }
    }

    async fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self
            .call(ControlRequest::TagsForFile {
                file_id,
                subtag_rule,
            })
            .await?
        {
            ControlResponse::TagIds(tag_ids) => Ok(tag_ids),
            other => Err(unexpected(other)),
        }
    }

    async fn search(
        &self,
        query: String,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<SearchResults, ApiError> {
        match self
            .call(ControlRequest::RunQuery {
                query,
                subtag_rule,
                deleted_rule,
            })
            .await?
        {
            ControlResponse::SearchResults(result) => Ok(result),
            other => Err(unexpected(other)),
        }
    }

    async fn get_file(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> Result<FileInfo, ApiError> {
        match self
            .call(ControlRequest::GetFile {
                file_id,
                deleted_rule,
            })
            .await?
        {
            ControlResponse::File(file) => Ok(file),
            other => Err(unexpected(other)),
        }
    }

    async fn get_tag(&self, tag_id: TagId, deleted_rule: DeletedRule) -> Result<Tag, ApiError> {
        match self
            .call(ControlRequest::GetTag {
                tag_id,
                deleted_rule,
            })
            .await?
        {
            ControlResponse::Tag(tag) => Ok(tag),
            other => Err(unexpected(other)),
        }
    }

    async fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self
            .call(ControlRequest::SubtagsForTag {
                tag_id,
                subtag_rule,
            })
            .await?
        {
            ControlResponse::TagIds(tag_ids) => Ok(tag_ids),
            other => Err(unexpected(other)),
        }
    }

    async fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self
            .call(ControlRequest::TagsForTag {
                tag_id,
                subtag_rule,
            })
            .await?
        {
            ControlResponse::TagIds(tag_ids) => Ok(tag_ids),
            other => Err(unexpected(other)),
        }
    }

    async fn create_tag(&self, name: String, color: String) -> Result<TagId, ApiError> {
        match self.call(ControlRequest::CreateTag { name, color }).await? {
            ControlResponse::TagId(tag_id) => Ok(tag_id),
            other => Err(unexpected(other)),
        }
    }

    async fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        match self.call(ControlRequest::DeleteTag { tag_id }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn restore_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        match self.call(ControlRequest::RestoreTag { tag_id }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn rename_tag(&self, tag_id: TagId, name: String) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::RenameTag { tag_id, name })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn set_tag_color(&self, tag_id: TagId, color: String) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::SetTagColor { tag_id, color })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    /// Upload a file by serving it as a temporary chunk provider (no bytes are
    /// loaded into memory or sent up front). Computes the content hash by
    /// streaming `path`, registers `path` as the file this connection serves,
    /// sends the metadata upload request, then blocks until the daemon reports
    /// the content has been handed off (a peer completed pulling it), or the
    /// connection ends.
    async fn upload_file(
        &self,
        path: PathBuf,
        path_name: String,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        let (content_hash, size) = hash_and_len(&path).await?;
        // Subscribe before sending so we cannot miss the release event.
        let mut events = self.inner.events.subscribe();
        *self.inner.provider_path.lock().await = Some(path);

        let file_id = match self
            .call(ControlRequest::UploadFile {
                path_name,
                content_hash,
                size,
                tags,
            })
            .await?
        {
            ControlResponse::FileId(file_id) => file_id,
            other => return Err(unexpected(other)),
        };

        wait_for_release(&mut events, file_id).await;
        *self.inner.provider_path.lock().await = None;
        Ok(file_id)
    }

    /// Edit (replace) a file's content, serving the new bytes as a temporary
    /// provider. Same handoff semantics as [`Self::upload_file`].
    async fn edit_file(&self, file_id: FileId, path: PathBuf) -> Result<(), ApiError> {
        let (content_hash, size) = hash_and_len(&path).await?;
        let mut events = self.inner.events.subscribe();
        *self.inner.provider_path.lock().await = Some(path);

        match self
            .call(ControlRequest::EditFile {
                file_id,
                content_hash,
                size,
            })
            .await?
        {
            ControlResponse::Ok => {}
            other => return Err(unexpected(other)),
        }

        wait_for_release(&mut events, file_id).await;
        *self.inner.provider_path.lock().await = None;
        Ok(())
    }

    async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<PathBuf, ApiError> {
        match self
            .call(ControlRequest::FetchFile {
                file_id,
                expected_hash,
            })
            .await?
        {
            ControlResponse::FilePath(path) => Ok(path),
            other => Err(unexpected(other)),
        }
    }

    /// Start an external edit. Thin IPC wrapper: the daemon does the actual
    /// work (local-path check or on-demand fetch into a per-request temp) and
    /// returns the path. No chunk-provider setup is needed here — unlike
    /// [`Self::edit_file`], the client does not stream bytes in either
    /// direction. The daemon reads the edited bytes off its own filesystem
    /// when [`Self::finish_edit`] runs (client and daemon share it).
    async fn begin_edit(&self, file_id: FileId) -> Result<PathBuf, ApiError> {
        match self.call(ControlRequest::BeginEdit { file_id }).await? {
            ControlResponse::FilePath(path) => Ok(path),
            other => Err(unexpected(other)),
        }
    }

    async fn finish_edit(&self, file_id: FileId, path: PathBuf) -> Result<EditOutcome, ApiError> {
        match self
            .call(ControlRequest::FinishEdit { file_id, path })
            .await?
        {
            ControlResponse::EditOutcome(outcome) => Ok(outcome),
            other => Err(unexpected(other)),
        }
    }

    async fn cancel_edit(&self, path: PathBuf) -> Result<(), ApiError> {
        match self.call(ControlRequest::CancelEdit { path }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn get_preview(&self, file_id: FileId) -> Result<Preview, ApiError> {
        match self.call(ControlRequest::GetPreview { file_id }).await? {
            ControlResponse::Preview(preview) => Ok(preview),
            other => Err(unexpected(other)),
        }
    }

    async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        match self
            .call(ControlRequest::LocalPathForFile { file_id })
            .await?
        {
            ControlResponse::LocalPath(path) => Ok(path),
            other => Err(unexpected(other)),
        }
    }

    async fn storage_stats(&self) -> Result<StorageStats, ApiError> {
        match self.call(ControlRequest::StorageStats).await? {
            ControlResponse::StorageStats(stats) => Ok(stats),
            other => Err(unexpected(other)),
        }
    }

    async fn backup(&self) -> Result<BackupOutcome, ApiError> {
        // No client-side deadline: `call` awaits the daemon's response until it
        // arrives or the connection drops, so a large backup can take as long
        // as it needs without a spurious timeout.
        match self.call(ControlRequest::Backup).await? {
            ControlResponse::BackupComplete(outcome) => Ok(outcome),
            other => Err(unexpected(other)),
        }
    }

    async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        match self.call(ControlRequest::DeleteFile { file_id }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn restore_file(&self, file_id: FileId) -> Result<(), ApiError> {
        match self.call(ControlRequest::RestoreFile { file_id }).await? {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn move_file(&self, file_id: FileId, logical_path: String) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::MoveFile {
                file_id,
                logical_path,
            })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::TagFile { tag_id, file_id })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::UntagFile { tag_id, file_id })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn tag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::TagTag {
                parent_id,
                subtag_id,
            })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn untag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        match self
            .call(ControlRequest::UntagTag {
                parent_id,
                subtag_id,
            })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    async fn purge_previews(&self) -> Result<usize, ApiError> {
        match self.call(ControlRequest::PurgePreviews).await? {
            ControlResponse::PurgedPreviews(purged) => Ok(purged),
            other => Err(unexpected(other)),
        }
    }

    async fn editor_rules(&self) -> Result<Vec<EditorRule>, ApiError> {
        match self.call(ControlRequest::EditorRules).await? {
            ControlResponse::EditorRules(rules) => Ok(rules),
            other => Err(unexpected(other)),
        }
    }

    async fn retag(&self, dry_run: bool) -> Result<RetagSummary, ApiError> {
        match self.call(ControlRequest::Retag { dry_run }).await? {
            ControlResponse::Retagged(summary) => Ok(summary),
            other => Err(unexpected(other)),
        }
    }

    async fn tag_rule_report(&self) -> Result<TagRuleReport, ApiError> {
        match self.call(ControlRequest::TagRuleReport).await? {
            ControlResponse::TagRuleReport(report) => Ok(report),
            other => Err(unexpected(other)),
        }
    }

    fn subscribe(&self) -> EventStream {
        EventStream::Ipc(self.inner.events.subscribe())
    }

    async fn list_operations(&self) -> Result<Vec<Operation>, ApiError> {
        match self.call(ControlRequest::ListOperations).await? {
            ControlResponse::Operations(operations) => Ok(operations),
            other => Err(unexpected(other)),
        }
    }

    fn subscribe_operations(&self) -> OperationStream {
        OperationStream::Ipc(self.inner.operation_events.subscribe())
    }
}
