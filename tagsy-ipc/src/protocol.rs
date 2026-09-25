//! The control wire protocol: the request/response/frame vocabulary the UI and
//! daemon exchange over the local control socket, plus the binary codec.
//!
//! This is a *distinct* message category from the peer `Change`/`Sync`
//! protocol: peer framing is about sync; [`ControlFrame`] carries the UI-facing
//! API requests, responses and events. The daemon's server (`serve_control` in
//! `tagsyd`) and the [`IpcBackend`](crate::client::IpcBackend) client both
//! speak it.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tagsy_api::{
    ApiError, ApiEvent, BackupOutcome, DeletedRule, EditorRule, HomeSection, RetagSummary,
    SearchResults, StorageStats, SubtagRule, Tag, TagRuleReport,
};
use tagsy_core::{FileId, FileInfo, FileKind, Preview, TagId, TagStyle};
use tokio_tungstenite::tungstenite::protocol::Message;

/// A UI-facing API call, sent by a control client to the daemon.
///
/// One variant per [`ApiService`] method. Requests are matched to their
/// [`ControlResponse`] by the [`ControlFrame::Request`] `id`, so multiple calls
/// may be in flight on one connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    /// Resolve a user-supplied term (id, id prefix, or name/path) to a single
    /// `FileId`. `deleted_rule` governs whether a tombstoned file can be named.
    /// Answered with [`ControlResponse::FileId`] (or an `Error`).
    ResolveFileId {
        term: String,
        deleted_rule: DeletedRule,
    },
    /// Resolve a user-supplied term (id, id prefix, or name) to a single
    /// `TagId`. Answered with [`ControlResponse::TagId`] (or an `Error`).
    ResolveTagId {
        term: String,
        deleted_rule: DeletedRule,
    },
    TagsForFile {
        file_id: FileId,
        subtag_rule: SubtagRule,
    },
    /// Run a free-form query (`$tag`, `!tag`, and name substrings) and return
    /// both the matching files and tags. Tag tokens are resolved in the daemon.
    /// `deleted_rule` toggles whether tombstoned files/tags participate (see
    /// `ApiService::search`).
    /// Answered with [`ControlResponse::SearchResults`].
    RunQuery {
        query: String,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    },
    /// Get a single file's info by id. Answered with [`ControlResponse::File`]
    /// (or `Error(UnknownId)`). `deleted_rule` toggles tombstone visibility;
    /// see `ApiService::get_file`.
    GetFile {
        file_id: FileId,
        deleted_rule: DeletedRule,
    },
    /// Classify a file's logical name into its `FileKind` from the extension
    /// alone. Answered with [`ControlResponse::FileKind`].
    Classify {
        name: String,
    },
    /// Get a single tag by id. Answered with [`ControlResponse::Tag`] (or
    /// `Error(UnknownId)`).
    GetTag {
        tag_id: TagId,
        deleted_rule: DeletedRule,
    },
    SubtagsForTag {
        tag_id: TagId,
        subtag_rule: SubtagRule,
    },
    TagsForTag {
        tag_id: TagId,
        subtag_rule: SubtagRule,
    },
    // Writes.
    CreateTag {
        name: String,
        style: TagStyle,
    },
    DeleteTag {
        tag_id: TagId,
    },
    RestoreTag {
        tag_id: TagId,
    },
    RenameTag {
        tag_id: TagId,
        name: String,
    },
    SetTagStyle {
        tag_id: TagId,
        style: TagStyle,
    },
    /// Upload the file at `path` — an absolute path on this host, which the
    /// daemon reads itself (clients run as the daemon's user) and copies into
    /// its outbox before answering. Answered with [`ControlResponse::FileId`];
    /// the client may delete `path` afterwards.
    UploadFile {
        path: PathBuf,
        path_name: String,
        tags: Vec<TagId>,
    },
    /// Replace an existing file's content with the file at `path`, ingested
    /// like [`ControlRequest::UploadFile`]. Answered with
    /// [`ControlResponse::Ok`].
    EditFile {
        file_id: FileId,
        path: PathBuf,
    },
    /// Start an external edit. The daemon returns the path the client should
    /// hand to an editor — either the file's real sync-dir path (edit in
    /// place) or a per-request temp file under `fetch_temp_dir` named after
    /// the file's logical basename. Answered with
    /// [`ControlResponse::FilePath`].
    BeginEdit {
        file_id: FileId,
    },
    /// Complete an external edit. The daemon re-hashes the bytes at `path`,
    /// publishes a new version if different from the current recorded hash,
    /// and cleans up any daemon-owned temp. Answered with
    /// [`ControlResponse::EditOutcome`].
    FinishEdit {
        file_id: FileId,
        path: PathBuf,
    },
    /// Abort an external edit without publishing. Cleans up any daemon-owned
    /// temp at `path`. Answered with [`ControlResponse::Ok`].
    CancelEdit {
        path: PathBuf,
    },
    /// Fetch a file's content on demand (from a peer if not local). Answered
    /// with [`ControlResponse::FilePath`] or an error. `expected_hash` gates
    /// which content is accepted.
    FetchFile {
        file_id: FileId,
        expected_hash: String,
    },
    /// Get the preview for a file's current content (cached / generated /
    /// peer-fetched). Answered with [`ControlResponse::Preview`].
    GetPreview {
        file_id: FileId,
    },
    /// Resolve a file's absolute on-disk path if present locally. Answered with
    /// [`ControlResponse::LocalPath`].
    LocalPathForFile {
        file_id: FileId,
    },
    /// Report local vs. whole-catalog storage totals. Answered with
    /// [`ControlResponse::StorageStats`].
    StorageStats,
    /// Bundle the entire restorable state into a compressed archive in
    /// `TAGSY_BACKUP_DIR`. Answered with [`ControlResponse::BackupComplete`].
    Backup,
    DeleteFile {
        file_id: FileId,
    },
    RestoreFile {
        file_id: FileId,
    },
    MoveFile {
        file_id: FileId,
        logical_path: String,
    },
    TagFile {
        tag_id: TagId,
        file_id: FileId,
    },
    UntagFile {
        tag_id: TagId,
        file_id: FileId,
    },
    TagTag {
        parent_id: TagId,
        subtag_id: TagId,
    },
    UntagTag {
        parent_id: TagId,
        subtag_id: TagId,
    },
    /// Subscribe to the event stream. After this is accepted, the daemon starts
    /// emitting [`ControlFrame::Event`]s on this connection; the response is
    /// [`ControlResponse::Subscribed`].
    Subscribe,
    /// Purge the entire preview cache. Answered with
    /// [`ControlResponse::PurgedPreviews`] carrying the number of cached
    /// previews removed.
    PurgePreviews,
    /// Permanently purge broken files (cataloged but with no local bytes).
    /// Answered with [`ControlResponse::PurgedBroken`]. With `dry_run` the
    /// daemon reports which files would be purged without mutating anything.
    PurgeBroken {
        dry_run: bool,
    },
    /// Permanently purge soft-deleted (tombstoned) files. Answered with
    /// [`ControlResponse::PurgedDeleted`]. With `dry_run` the daemon reports
    /// which files would be purged without mutating anything.
    PurgeDeleted {
        dry_run: bool,
    },
    /// Soft-delete duplicate files (same logical path and content hash).
    /// Answered with [`ControlResponse::DuplicatesDeleted`]. With `dry_run` the
    /// daemon reports the duplicate sets without mutating anything.
    DeleteDuplicates {
        dry_run: bool,
    },
    /// Read the daemon's external-editor rules. Answered with
    /// [`ControlResponse::EditorRules`].
    EditorRules,
    /// Read the daemon's home-screen sections. Answered with
    /// [`ControlResponse::HomeSections`].
    HomeSections,
    /// Re-apply the configured tag rules to the existing catalog. Answered
    /// with [`ControlResponse::Retagged`]. With `dry_run` the daemon plans and
    /// reports the work without enqueuing it.
    Retag {
        dry_run: bool,
    },
    /// Diagnose the configured tag rules. Answered with
    /// [`ControlResponse::TagRuleReport`].
    TagRuleReport,
    /// Snapshot every currently-active sync operation. Answered with
    /// [`ControlResponse::Operations`].
    ListOperations,
    /// Subscribe to the operation stream. After this is accepted the daemon
    /// starts emitting [`ControlFrame::OperationEvent`]s on this connection;
    /// the response is [`ControlResponse::OperationsSubscribed`].
    SubscribeOperations,
    /// Snapshot every currently-connected peer. Answered with
    /// [`ControlResponse::ConnectedPeers`].
    ConnectedPeers,
    /// Subscribe to the connection stream. After this is accepted the daemon
    /// starts emitting [`ControlFrame::ConnectionEvent`]s on this connection;
    /// the response is [`ControlResponse::ConnectionsSubscribed`].
    SubscribeConnections,
    /// Sample the daemon's actor activity. Answered with
    /// [`ControlResponse::Activity`].
    Activity,
}

/// The result of a [`ControlRequest`], returned as [`ControlFrame::Response`].
///
/// Every variant is either the success payload of the matching request or the
/// single serializable [`ApiError`]. The client maps these back onto
/// the [`Backend`] return types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    /// A single file's info (answer to [`ControlRequest::GetFile`]).
    File(FileInfo),
    /// A file's classified kind (answer to [`ControlRequest::Classify`]).
    FileKind(FileKind),
    /// A single tag (answer to [`ControlRequest::GetTag`]).
    Tag(Tag),
    TagIds(Vec<TagId>),
    FileIds(Vec<FileId>),
    /// The files and tags matching a query (answer to
    /// [`ControlRequest::RunQuery`]).
    SearchResults(SearchResults),
    TagId(TagId),
    FileId(FileId),
    /// Path to a daemon-owned temp file holding a fetched file's content
    /// (answer to [`ControlRequest::FetchFile`]).
    ///
    /// The client and daemon are co-located and share this filesystem; the
    /// client consumes the temp file with move semantics (rename into place or
    /// delete). No file bytes cross the socket.
    FilePath(PathBuf),
    /// A file's preview (answer to [`ControlRequest::GetPreview`]).
    /// [`Preview::None`] is a valid result (the content has no preview).
    Preview(Preview),
    /// A file's absolute on-disk path, or `None` if not present locally (answer
    /// to [`ControlRequest::LocalPathForFile`]).
    LocalPath(Option<PathBuf>),
    /// Local vs. whole-catalog storage totals (answer to
    /// [`ControlRequest::StorageStats`]).
    StorageStats(StorageStats),
    /// Where a completed backup archive landed and how much it covers (answer
    /// to [`ControlRequest::Backup`]).
    BackupComplete(BackupOutcome),
    /// The outcome of an external edit (answer to
    /// [`ControlRequest::FinishEdit`]).
    EditOutcome(tagsy_api::EditOutcome),
    /// A write/command that returns no payload succeeded.
    Ok,
    /// The subscription was established; events will follow on this connection.
    Subscribed,
    /// The number of cached previews removed (answer to
    /// [`ControlRequest::PurgePreviews`]).
    PurgedPreviews(usize),
    /// The files purged (or, on a dry run, that would be purged) as broken
    /// (answer to [`ControlRequest::PurgeBroken`]).
    PurgedBroken(tagsy_api::PurgeOutcome),
    /// The files purged (or, on a dry run, that would be purged) as
    /// soft-deleted (answer to [`ControlRequest::PurgeDeleted`]).
    PurgedDeleted(tagsy_api::PurgeOutcome),
    /// The duplicate sets found, and whether their deletion was enqueued or
    /// only reported (answer to [`ControlRequest::DeleteDuplicates`]).
    DuplicatesDeleted(tagsy_api::DuplicateDeletionOutcome),
    /// The daemon's external-editor rules (answer to
    /// [`ControlRequest::EditorRules`]).
    EditorRules(Vec<EditorRule>),
    /// The daemon's home-screen sections (answer to
    /// [`ControlRequest::HomeSections`]).
    HomeSections(Vec<HomeSection>),
    /// What a retag did, or would do under `dry_run` (answer to
    /// [`ControlRequest::Retag`]).
    Retagged(RetagSummary),
    /// Tag-rule diagnostics (answer to [`ControlRequest::TagRuleReport`]).
    TagRuleReport(TagRuleReport),
    /// A snapshot of currently-active sync operations (answer to
    /// [`ControlRequest::ListOperations`]).
    Operations(Vec<tagsy_api::Operation>),
    /// The operation subscription was established; operation events will follow
    /// on this connection.
    OperationsSubscribed,
    /// A snapshot of currently-connected peers (answer to
    /// [`ControlRequest::ConnectedPeers`]).
    ConnectedPeers(Vec<tagsy_api::ConnectedPeer>),
    /// The connection subscription was established; connection events will
    /// follow on this connection.
    ConnectionsSubscribed,
    /// A sample of the daemon's actor activity (answer to
    /// [`ControlRequest::Activity`]).
    Activity(tagsy_api::ActivityInfo),
    /// The request failed. Carries the single UI-facing error type.
    Error(ApiError),
}

/// Every control-socket message, in either direction.
///
/// This is the control counterpart to the peer
/// [`Frame`](tagsy_core::state::Frame): same WebSocket text framing, disjoint
/// message set. `Request`/`Response` carry a correlation `id`; `Event` is
/// unsolicited (pushed after `Subscribe`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlFrame {
    /// Client -> daemon: an API call tagged with a per-connection request id.
    Request { id: u64, request: ControlRequest },
    /// Daemon -> client: the reply to the request with matching `id`.
    Response { id: u64, response: ControlResponse },
    /// Daemon -> client: an unsolicited event on a subscribed connection.
    Event(ApiEvent),
    /// Daemon -> client: an unsolicited sync-operation event on a connection
    /// that sent [`ControlRequest::SubscribeOperations`].
    OperationEvent(tagsy_api::OperationEvent),
    /// Daemon -> client: an unsolicited peer-connection event on a connection
    /// that sent [`ControlRequest::SubscribeConnections`].
    ConnectionEvent(tagsy_api::ConnectionEvent),
}

/// Encode a [`ControlFrame`] to a binary WebSocket message.
///
/// The control protocol uses **binary msgpack** (via `rmp_serde`), not JSON
/// text, mirroring the peer `Frame` wire format (also `rmp`). A byte payload
/// must be marked `#[serde(with = "serde_bytes")]` to be encoded compactly; a
/// plain `Vec<u8>` becomes an array of integers.
pub fn encode_frame(frame: &ControlFrame) -> Result<Message, String> {
    let bytes = rmp_serde::to_vec_named(frame).map_err(|error| format!("serialize: {error}"))?;
    Ok(Message::binary(bytes))
}

/// Decode a [`ControlFrame`] from an inbound WebSocket message.
///
/// Accepts binary (msgpack) frames. Non-data frames (ping/pong/close) are
/// filtered by callers before this is reached.
pub fn decode_frame(message: &Message) -> Result<ControlFrame, String> {
    rmp_serde::from_slice(&message.clone().into_data())
        .map_err(|error| format!("deserialize: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every control frame must round-trip through the binary codec unchanged.
    #[test]
    fn frames_round_trip_through_binary_codec() {
        let file_id = FileId::new();

        let frames = vec![
            ControlFrame::Request {
                id: 7,
                request: ControlRequest::EditFile {
                    file_id,
                    path: PathBuf::from("/home/user/notes.txt"),
                },
            },
            ControlFrame::Request {
                id: 8,
                request: ControlRequest::UploadFile {
                    path: PathBuf::from("/home/user/notes.txt"),
                    path_name: "notes.txt".to_owned(),
                    tags: vec![TagId::new()],
                },
            },
            ControlFrame::Response {
                id: 7,
                response: ControlResponse::FileId(file_id),
            },
        ];

        for frame in frames {
            let message = encode_frame(&frame).expect("encode");
            let decoded = decode_frame(&message).expect("decode");
            // Compare via debug repr (ControlFrame has no PartialEq).
            assert_eq!(format!("{frame:?}"), format!("{decoded:?}"));
        }
    }
}
