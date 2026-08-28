//! The port: the transport-agnostic [`Backend`] trait every frontend talks to,
//! and the two normalized event streams it hands back.
//!
//! The trait *declaration* lives here so the CLI and bridge can depend on the
//! contract without pulling in the daemon. The concrete implementations
//! (`InProcessBackend`, the IPC client, and the `AnyBackend` enum that
//! dispatches between them) live in `tagsyd`.

use std::future::Future;
use std::path::PathBuf;

use tagsy_core::state::Change;
use tagsy_core::{FileId, FileInfo, Preview, TagId};
use tokio::sync::broadcast;

use crate::operations::{Operation, OperationEvent};
use crate::{
    ApiError, ApiEvent, BackupOutcome, DeletedRule, EditOutcome, EditorRule, HomeSection,
    RetagSummary, SearchResults, StorageStats, SubtagRule, Tag, TagRuleReport,
};

/// The transport-agnostic UI-facing API.
///
/// This mirrors the daemon's in-process `ApiService` method-for-method, but
/// every operation is `async` so both the in-process backend (immediate) and
/// the IPC-client backend (socket round-trip) can implement it behind one
/// surface.
///
/// Implemented in `tagsyd` by `InProcessBackend` and the IPC client, and
/// dispatched through the `AnyBackend` enum.
///
/// The returned futures are declared `+ Send` (rather than plain `async fn`)
/// so callers — notably `flutter_rust_bridge`, which spawns them on a
/// multi-threaded runtime — can move them across threads.
pub trait Backend {
    /// Resolve a full-or-short file id `prefix` to a single [`FileId`]. Errors
    /// with `UnknownId` if nothing matches or `AmbiguousId` if several do.
    fn resolve_file_id(
        &self,
        prefix: String,
    ) -> impl Future<Output = Result<FileId, ApiError>> + Send;

    /// Resolve a full-or-short tag id `prefix` to a single [`TagId`]. Errors
    /// with `UnknownId` if nothing matches or `AmbiguousId` if several do.
    fn resolve_tag_id(
        &self,
        prefix: String,
    ) -> impl Future<Output = Result<TagId, ApiError>> + Send;

    /// List the tags applied to `file_id`.
    fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> impl Future<Output = Result<Vec<TagId>, ApiError>> + Send;

    /// Run a free-form query (`$tag`, `!tag`, and name substrings) and return
    /// both the matching files and tags. Tag tokens are resolved in the daemon.
    ///
    /// `deleted_rule` toggles the "show deleted rows" view.
    fn search(
        &self,
        query: String,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> impl Future<Output = Result<SearchResults, ApiError>> + Send;

    /// Get a single file's [`FileInfo`] by id (`UnknownId` if unknown).
    /// `deleted_rule` controls whether a tombstoned file reads as `UnknownId`
    /// or is returned with `FileInfo::deleted = true`.
    fn get_file(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> impl Future<Output = Result<FileInfo, ApiError>> + Send;

    /// Get a single tag by id (`UnknownId` if unknown). See [`Self::get_file`]
    /// for the `deleted_rule` semantics.
    fn get_tag(
        &self,
        tag_id: TagId,
        deleted_rule: DeletedRule,
    ) -> impl Future<Output = Result<Tag, ApiError>> + Send;

    /// List the subtags (children) of `tag_id` in the tag hierarchy.
    fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> impl Future<Output = Result<Vec<TagId>, ApiError>> + Send;

    /// List the tags applied to `tag_id` (the tags it is a subtag of).
    fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> impl Future<Output = Result<Vec<TagId>, ApiError>> + Send;

    /// Create a tag; returns the freshly-minted id.
    fn create_tag(
        &self,
        name: String,
        color: String,
    ) -> impl Future<Output = Result<TagId, ApiError>> + Send;

    /// Delete a tag.
    fn delete_tag(&self, tag_id: TagId) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Restore a soft-deleted tag.
    fn restore_tag(&self, tag_id: TagId) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Rename a tag.
    fn rename_tag(
        &self,
        tag_id: TagId,
        name: String,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Change a tag's color.
    fn set_tag_color(
        &self,
        tag_id: TagId,
        color: String,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Upload a file from a path on disk; returns the freshly-minted id.
    ///
    /// The bytes are never buffered whole: the backend hashes `path` by
    /// streaming it and then serves the content chunk-by-chunk on demand (the
    /// IPC backend over the control socket; the in-process backend straight
    /// from disk). `path_name` is the file's logical identity; `path` is
    /// where the bytes currently live.
    fn upload_file(
        &self,
        path: PathBuf,
        path_name: String,
        tags: Vec<TagId>,
    ) -> impl Future<Output = Result<FileId, ApiError>> + Send;

    /// Replace the content of an existing file with the bytes at `path`, served
    /// on demand exactly like [`upload_file`](Self::upload_file).
    fn edit_file(
        &self,
        file_id: FileId,
        path: PathBuf,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Start an external edit: return the on-disk path the caller should hand
    /// to an editor.
    ///
    /// Two branches, transparent to the caller:
    ///
    /// - The file is present in a local sync directory → the returned path is
    ///   that real on-disk file, edited in place. The daemon's filesystem
    ///   watcher will pick up the save and propagate a `FileMetadataChanged` on
    ///   its own; [`finish_edit`](Self::finish_edit) is still called and acts
    ///   as a "did the bytes change vs. the current DB hash?" belt-and-braces
    ///   check.
    /// - Otherwise the daemon fetches the content (from a peer if needed) into
    ///   an isolated per-request subdirectory under the daemon's fetch temp
    ///   dir, named with the file's logical basename so an external editor
    ///   dispatches by extension correctly. Move semantics: the caller must
    ///   consume via [`finish_edit`](Self::finish_edit) or
    ///   [`cancel_edit`](Self::cancel_edit).
    ///
    /// No daemon-side state is kept between `begin_edit` and
    /// `finish_edit`/`cancel_edit`; the caller's `file_id`+`path` fully
    /// describe the follow-up. A caller that crashes before finishing leaks
    /// only a temp file, which the daemon bulk-cleans on next start.
    fn begin_edit(&self, file_id: FileId)
    -> impl Future<Output = Result<PathBuf, ApiError>> + Send;

    /// Complete an in-flight external edit.
    ///
    /// `path` is the path returned by [`begin_edit`](Self::begin_edit) (the
    /// bytes at that path are the editor's output). The daemon re-hashes
    /// them, compares to the file's current recorded `content_hash`, and:
    ///
    /// - if equal → nothing to do (either the editor produced no change, or the
    ///   file was edited in place and the watcher already published the
    ///   change);
    /// - if different → publish a new version by streaming `path` to peers via
    ///   the same provider protocol as [`edit_file`](Self::edit_file).
    ///
    /// After that the daemon deletes `path` **only if it lives under** the
    /// daemon's fetch temp dir (the isolated per-request subdirectory it
    /// created in `begin_edit`). Paths under sync directories, or anywhere else
    /// the caller may have staged bytes, are left untouched.
    fn finish_edit(
        &self,
        file_id: FileId,
        path: PathBuf,
    ) -> impl Future<Output = Result<EditOutcome, ApiError>> + Send;

    /// Abort an in-flight external edit without uploading.
    ///
    /// `path` is the path returned by [`begin_edit`](Self::begin_edit).
    /// Cleans up the daemon-owned temp exactly as
    /// [`finish_edit`](Self::finish_edit) does (delete iff under the daemon's
    /// fetch temp dir).
    fn cancel_edit(&self, path: PathBuf) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Fetch a file's content on demand (from a peer if not present locally)
    /// and return the path to a temp file holding it. `expected_hash` gates
    /// which content is accepted.
    ///
    /// The path is handed to the caller with **move semantics**: it points at a
    /// daemon-owned temp file (both backends run co-located with the daemon and
    /// share its filesystem) that the caller must consume by renaming it into
    /// place or deleting it. The content is never buffered whole in memory.
    fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> impl Future<Output = Result<PathBuf, ApiError>> + Send;

    /// Get the preview for a file's current content (cached, generated locally,
    /// or fetched from a peer). [`Preview::None`] is a valid result.
    fn get_preview(
        &self,
        file_id: FileId,
    ) -> impl Future<Output = Result<Preview, ApiError>> + Send;

    /// Resolve a file's absolute on-disk path if present locally, else `None`.
    fn local_path_for_file(
        &self,
        file_id: FileId,
    ) -> impl Future<Output = Result<Option<PathBuf>, ApiError>> + Send;

    /// Report local vs. whole-catalog storage totals.
    fn storage_stats(&self) -> impl Future<Output = Result<StorageStats, ApiError>> + Send;

    /// Bundle the entire restorable state (both databases plus every sync
    /// directory's contents) into a compressed archive in `TAGSY_BACKUP_DIR`,
    /// returning where it landed. Errors if backups are not configured.
    fn backup(&self) -> impl Future<Output = Result<BackupOutcome, ApiError>> + Send;

    /// Delete a file.
    fn delete_file(&self, file_id: FileId) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Restore a soft-deleted file (best-effort; fails if no source holds its
    /// bytes).
    fn restore_file(&self, file_id: FileId) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Move (rename) a file to a new logical path.
    fn move_file(
        &self,
        file_id: FileId,
        logical_path: String,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Apply `tag_id` to `file_id`.
    fn tag_file(
        &self,
        tag_id: TagId,
        file_id: FileId,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Remove `tag_id` from `file_id`.
    fn untag_file(
        &self,
        tag_id: TagId,
        file_id: FileId,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Make `subtag_id` a subtag (child) of `parent_id`.
    fn tag_tag(
        &self,
        parent_id: TagId,
        subtag_id: TagId,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Remove `subtag_id` as a subtag of `parent_id`.
    fn untag_tag(
        &self,
        parent_id: TagId,
        subtag_id: TagId,
    ) -> impl Future<Output = Result<(), ApiError>> + Send;

    /// Purge the entire preview cache, returning how many cached previews were
    /// removed. Previews are hash-keyed and regenerated on demand, so this only
    /// forces re-evaluation on the next request.
    fn purge_previews(&self) -> impl Future<Output = Result<usize, ApiError>> + Send;

    /// The daemon's configured external-editor rules (see [`EditorRule`]). A
    /// snapshot read; the desktop UI calls this once when preparing to launch
    /// an editor.
    fn editor_rules(&self) -> impl Future<Output = Result<Vec<EditorRule>, ApiError>> + Send;

    /// The daemon's configured home-screen sections (see [`HomeSection`]). A
    /// snapshot read; the desktop UI calls this once to build the home screen
    /// shown when the search box is empty.
    fn home_sections(&self) -> impl Future<Output = Result<Vec<HomeSection>, ApiError>> + Send;

    /// Re-apply the configured tag rules to files already in the catalog,
    /// additively. With `dry_run` the work is planned and reported but nothing
    /// is enqueued.
    fn retag(&self, dry_run: bool) -> impl Future<Output = Result<RetagSummary, ApiError>> + Send;

    /// Diagnose the configured tag rules (invalid patterns, unknown tag ids).
    fn tag_rule_report(&self) -> impl Future<Output = Result<TagRuleReport, ApiError>> + Send;

    /// Subscribe to the live change stream. Returns an [`EventStream`] whose
    /// [`recv`](EventStream::recv) yields [`ApiEvent`]s.
    fn subscribe(&self) -> EventStream;

    /// Snapshot every currently-active sync operation (peer transfers,
    /// reconciliation, fetches, ...). The read the UI issues for its initial
    /// paint before applying live [`OperationEvent`]s from
    /// [`subscribe_operations`](Self::subscribe_operations).
    fn list_operations(&self) -> impl Future<Output = Result<Vec<Operation>, ApiError>> + Send;

    /// Subscribe to the live sync-operation stream. Returns an
    /// [`OperationStream`] whose [`recv`](OperationStream::recv) yields
    /// [`OperationUpdate`]s.
    fn subscribe_operations(&self) -> OperationStream;
}

/// The transport-agnostic event stream returned by [`Backend::subscribe`].
///
/// It normalizes the two delivery mechanisms behind one type so the UI (and
/// `flutter_rust_bridge`) sees a single stream shape regardless of transport.
/// Poll it with [`EventStream::recv`].
pub enum EventStream {
    /// In-process delivery: a direct subscription to the runtime's broadcast
    /// bus. Each item is a raw [`Change`] the runtime applied; [`recv`] wraps
    /// it in [`ApiEvent::Changed`].
    InProcess(broadcast::Receiver<Change>),
    /// IPC delivery: a subscription to the control client's broadcast of
    /// [`ApiEvent`]s decoded off the control socket. The [`ApiEvent`]s are
    /// already fully-formed (the daemon sends [`ApiEvent::Changed`] per change;
    /// a reconnecting client would receive [`ApiEvent::Resynced`]).
    Ipc(broadcast::Receiver<ApiEvent>),
}

impl EventStream {
    /// Await the next event.
    ///
    /// Returns:
    /// - `Some(ApiEvent::Changed(_))` for each applied change,
    /// - `Some(ApiEvent::Resynced)` when the subscriber lagged past the channel
    ///   capacity (the UI should re-fetch state), and
    /// - `None` once the stream is permanently closed (runtime shut down).
    pub async fn recv(&mut self) -> Option<ApiEvent> {
        match self {
            EventStream::InProcess(receiver) => match receiver.recv().await {
                Ok(change) => Some(ApiEvent::Changed(change)),
                // A slow subscriber fell behind: surface a resync request so
                // the UI re-fetches current state rather than silently
                // dropping changes.
                Err(broadcast::error::RecvError::Lagged(_)) => Some(ApiEvent::Resynced),
                // Sender dropped: the runtime is gone, the stream is done.
                Err(broadcast::error::RecvError::Closed) => None,
            },
            EventStream::Ipc(receiver) => match receiver.recv().await {
                // Already-decoded `ApiEvent`s arrive off the control socket.
                Ok(event) => Some(event),
                // The local client fell behind the daemon's event feed: same
                // remedy as in-process — ask the UI to re-fetch state.
                Err(broadcast::error::RecvError::Lagged(_)) => Some(ApiEvent::Resynced),
                // The control connection dropped (reader task ended).
                Err(broadcast::error::RecvError::Closed) => None,
            },
        }
    }
}

/// A live update on the operation stream, normalized across transports.
///
/// Mirrors the [`ApiEvent`] shape for the change stream: an in-process or IPC
/// subscriber that lags past the channel capacity gets a
/// [`Resynced`](OperationUpdate::Resynced) prompt to re-snapshot via
/// [`list_operations`](Backend::list_operations) rather than silently
/// dropping updates.
#[derive(Debug, Clone)]
pub enum OperationUpdate {
    /// The stream lagged (or reconnected over IPC); the UI should re-snapshot.
    Resynced,
    /// A concrete operation event (started / progress / terminal).
    Event(OperationEvent),
}

/// The transport-agnostic operation stream returned by
/// [`Backend::subscribe_operations`].
///
/// The operation counterpart of [`EventStream`]; same two delivery mechanisms
/// behind one type. Poll it with [`OperationStream::recv`].
pub enum OperationStream {
    /// In-process delivery: a direct subscription to the runtime's operation
    /// broadcast.
    InProcess(broadcast::Receiver<OperationEvent>),
    /// IPC delivery: a subscription to the control client's broadcast of
    /// operation events decoded off the control socket.
    Ipc(broadcast::Receiver<OperationEvent>),
}

impl OperationStream {
    /// Await the next operation update.
    ///
    /// Returns `Some(OperationUpdate::Event(_))` per operation event,
    /// `Some(OperationUpdate::Resynced)` when the subscriber lagged
    /// (re-snapshot needed), and `None` once the stream is permanently
    /// closed.
    pub async fn recv(&mut self) -> Option<OperationUpdate> {
        let receiver = match self {
            OperationStream::InProcess(receiver) | OperationStream::Ipc(receiver) => receiver,
        };
        match receiver.recv().await {
            Ok(event) => Some(OperationUpdate::Event(event)),
            Err(broadcast::error::RecvError::Lagged(_)) => Some(OperationUpdate::Resynced),
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }
}
