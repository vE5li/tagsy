//! Transport abstraction.
//!
//! The UI always talks to the same logical [API](crate::frontend::api). Only
//! the transport underneath differs:
//!
//! - **In-process** (Android, and optional single-process desktop): calls
//!   straight into [`ApiService`](crate::frontend::api::ApiService) / the
//!   change pipeline.
//! - **IPC-client** (Linux daemon mode): a thin embedded Rust client that
//!   connects to the daemon's control socket, serializes API calls, and returns
//!   results/events.
//!
//! This module defines the transport-agnostic surface as the
//! [`Backend`] trait and provides the **in-process** implementation
//! ([`InProcessBackend`]). The IPC-client backend
//! ([`IpcBackend`](crate::control::IpcBackend)) lives in the
//! `control` module.
//!
//! `flutter_rust_bridge` always targets [`AnyBackend`] on both platforms. On
//! Android it wraps the in-process backend; on Linux it will wrap the
//! IPC-client backend. The Dart UI never knows which — a single UI codebase is
//! preserved.
//!
//! ## Async surface
//!
//! Every method is `async`, even the reads which are synchronous on
//! [`ApiService`](crate::frontend::api::ApiService). This is deliberate: the
//! IPC-client backend is inherently asynchronous (a socket round-trip), so the
//! shared trait must be async for both. The in-process backend simply completes
//! immediately.
//!
//! ## Dispatch
//!
//! [`AnyBackend`] is an `enum` rather than a `dyn Backend`. `async fn` in
//! traits is not yet dyn-compatible without extra machinery, and the set of
//! backends is small, closed, and known at compile time. The enum gives static
//! dispatch and lets the event stream carry a concrete, `Send` type across the
//! FFI boundary.

use std::path::PathBuf;

// The port itself — the `Backend` trait and the normalized event streams — now
// lives in `tagsy-api`. This module keeps the daemon's *implementations*
// (`InProcessBackend`, `AnyBackend`) and re-exports the port so external
// callers keep using `tagsyd::transport::{Backend, EventStream, ...}`.
pub use tagsy_api::{
    Backend, ConnectionStream, ConnectionUpdate, EventStream, OperationStream, OperationUpdate,
};
use tagsy_core::{FileId, FileInfo, Preview, TagId, TagStyle};

use crate::configuration::{EditorRule, HomeSection};
use crate::connections::ConnectedPeer;
use crate::frontend::api::{
    ApiError, ApiService, BackupOutcome, EditOutcome, RetagSummary, SearchResults, StorageStats,
    TagRuleReport,
};
use crate::operations::Operation;
use crate::store::{DeletedRule, SubtagRule, Tag};

/// In-process transport backend.
///
/// Thinnest possible wrapper over
/// [`ApiService`](crate::frontend::api::ApiService): every call
/// delegates directly, completing immediately. Used on Android (single
/// process) and for single-process desktop.
///
/// The wrapped reads perform blocking SQLite work; in-process that is
/// acceptable because each read opens and drops its own short-lived read-only
/// handle (see [`ApiService`](crate::frontend::api::ApiService) docs) and does
/// not hold it across an `.await`.
#[derive(Clone)]
pub struct InProcessBackend {
    api: ApiService,
}

impl InProcessBackend {
    /// Wrap an [`ApiService`](crate::frontend::api::ApiService) handle produced
    /// by [`run`](crate::run).
    pub fn new(api: ApiService) -> Self {
        Self { api }
    }

    /// Borrow the underlying [`ApiService`](crate::frontend::api::ApiService).
    pub fn api(&self) -> &ApiService {
        &self.api
    }
}

impl Backend for InProcessBackend {
    async fn resolve_file_id(&self, prefix: String) -> Result<FileId, ApiError> {
        self.api.resolve_file_id(&prefix)
    }

    async fn resolve_tag_id(&self, prefix: String) -> Result<TagId, ApiError> {
        self.api.resolve_tag_id(&prefix)
    }

    async fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        self.api.tags_for_file(file_id, subtag_rule)
    }

    async fn search(
        &self,
        query: String,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<SearchResults, ApiError> {
        self.api.search(&query, subtag_rule, deleted_rule)
    }

    async fn get_file(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> Result<FileInfo, ApiError> {
        self.api.get_file(file_id, deleted_rule)
    }

    async fn get_tag(&self, tag_id: TagId, deleted_rule: DeletedRule) -> Result<Tag, ApiError> {
        self.api.get_tag(tag_id, deleted_rule)
    }

    async fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        self.api.subtags_for_tag(tag_id, subtag_rule)
    }

    async fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        self.api.tags_for_tag(tag_id, subtag_rule)
    }

    async fn create_tag(&self, name: String, style: TagStyle) -> Result<TagId, ApiError> {
        self.api.create_tag(name, style)
    }

    async fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.api.delete_tag(tag_id)
    }

    async fn restore_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.api.restore_tag(tag_id)
    }

    async fn rename_tag(&self, tag_id: TagId, name: String) -> Result<(), ApiError> {
        self.api.rename_tag(tag_id, name)
    }

    async fn set_tag_style(&self, tag_id: TagId, style: TagStyle) -> Result<(), ApiError> {
        self.api.set_tag_style(tag_id, style)
    }

    async fn upload_file(
        &self,
        path: PathBuf,
        path_name: String,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        // Hash by streaming the file, announce the upload, then register the
        // on-disk path as a `FileToCopy` chunk provider so peers pull the bytes
        // on demand straight from disk (never buffering the whole file). This is
        // the same provider mechanism the IPC/CLI path uses, sourced from the
        // local filesystem instead of the control socket.
        let (content_hash, size) = crate::file_bytes::hash_and_len(&path).await?;
        let source = crate::file_bytes::FileBytes::FileToCopy(path);
        let file_id = self
            .api
            .upload_file(path_name, content_hash.clone(), size, tags)?;
        self.api
            .register_provider(file_id, content_hash, std::sync::Arc::new(source))
            .await;
        Ok(file_id)
    }

    async fn edit_file(&self, file_id: FileId, path: PathBuf) -> Result<(), ApiError> {
        let (content_hash, size) = crate::file_bytes::hash_and_len(&path).await?;
        let source = crate::file_bytes::FileBytes::FileToCopy(path);
        self.api.edit_file(file_id, content_hash.clone(), size)?;
        self.api
            .register_provider(file_id, content_hash, std::sync::Arc::new(source))
            .await;
        Ok(())
    }

    async fn begin_edit(&self, file_id: FileId) -> Result<PathBuf, ApiError> {
        self.api.begin_edit(file_id).await
    }

    async fn finish_edit(&self, file_id: FileId, path: PathBuf) -> Result<EditOutcome, ApiError> {
        self.api.finish_edit(file_id, path).await
    }

    async fn cancel_edit(&self, path: PathBuf) -> Result<(), ApiError> {
        self.api.cancel_edit(path)
    }

    async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<PathBuf, ApiError> {
        self.api.fetch_file(file_id, expected_hash).await
    }

    async fn get_preview(&self, file_id: FileId) -> Result<Preview, ApiError> {
        self.api.get_preview(file_id).await
    }

    async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        self.api.local_path_for_file(file_id).await
    }

    async fn storage_stats(&self) -> Result<StorageStats, ApiError> {
        self.api.storage_stats().await
    }

    async fn backup(&self) -> Result<BackupOutcome, ApiError> {
        self.api.backup().await
    }

    async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.api.delete_file(file_id)
    }

    async fn restore_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.api.restore_file(file_id).await
    }

    async fn move_file(&self, file_id: FileId, logical_path: String) -> Result<(), ApiError> {
        self.api.move_file(file_id, logical_path)
    }

    async fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.api.tag_file(tag_id, file_id)
    }

    async fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.api.untag_file(tag_id, file_id)
    }

    async fn tag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        self.api.tag_tag(parent_id, subtag_id)
    }

    async fn untag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        self.api.untag_tag(parent_id, subtag_id)
    }

    async fn purge_previews(&self) -> Result<usize, ApiError> {
        self.api.purge_previews().await
    }

    async fn editor_rules(&self) -> Result<Vec<EditorRule>, ApiError> {
        Ok(self.api.editor_rules())
    }

    async fn home_sections(&self) -> Result<Vec<HomeSection>, ApiError> {
        Ok(self.api.home_sections())
    }

    async fn retag(&self, dry_run: bool) -> Result<RetagSummary, ApiError> {
        self.api.retag(dry_run)
    }

    async fn tag_rule_report(&self) -> Result<TagRuleReport, ApiError> {
        self.api.tag_rule_report()
    }

    fn subscribe(&self) -> EventStream {
        EventStream::InProcess(self.api.subscribe())
    }

    async fn list_operations(&self) -> Result<Vec<Operation>, ApiError> {
        Ok(self.api.list_operations())
    }

    fn subscribe_operations(&self) -> OperationStream {
        OperationStream::InProcess(self.api.subscribe_operations())
    }

    async fn connected_peers(&self) -> Result<Vec<ConnectedPeer>, ApiError> {
        Ok(self.api.connected_peers())
    }

    fn subscribe_connections(&self) -> ConnectionStream {
        ConnectionStream::InProcess(self.api.subscribe_connections())
    }
}

/// The transport-agnostic handle `flutter_rust_bridge` targets on every
/// platform.
///
/// An `enum` over the concrete backends, forwarding the whole
/// [`Backend`] surface to whichever variant is present. The Dart UI
/// holds one `AnyBackend` and never learns which transport backs it.
///
/// [`AnyBackend::InProcess`] is used on Android / single-process desktop;
/// [`AnyBackend::Ipc`] connects to the daemon control socket on the Linux
/// daemon topology.
#[derive(Clone)]
pub enum AnyBackend {
    /// In-process backend (Android / single-process desktop).
    InProcess(InProcessBackend),
    /// IPC-client backend talking to the daemon control socket.
    Ipc(crate::control::IpcBackend),
}

impl AnyBackend {
    /// Build an in-process backend from an
    /// [`ApiService`](crate::frontend::api::ApiService) handle.
    pub fn in_process(api: ApiService) -> Self {
        AnyBackend::InProcess(InProcessBackend::new(api))
    }

    /// Connect an IPC-client backend to the daemon's default control socket
    /// (section 7).
    pub async fn ipc_default() -> Result<Self, ApiError> {
        Ok(AnyBackend::Ipc(
            crate::control::IpcBackend::connect_default().await?,
        ))
    }
}

// The methods here **must** appear in the same order as the [`Backend`] trait
// declaration, and each must forward to its own same-named method on both
// variants. This block is pure hand-written boilerplate: `rustc` checks that
// every method is *present* and *type-correct*, but nothing checks that
// `delete_tag` forwards to `delete_tag` rather than `restore_tag` — two methods
// with identical `(TagId) -> Result<(), ApiError>` signatures forward
// interchangeably as far as the compiler is concerned. Keeping this block in
// trait order makes such a mismatch a visible diff against the trait rather
// than a silent behavioural bug. (`restore_tag`/`delete_tag` were in fact
// transposed here before 6.4.)
impl Backend for AnyBackend {
    async fn resolve_file_id(&self, prefix: String) -> Result<FileId, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.resolve_file_id(prefix).await,
            AnyBackend::Ipc(backend) => backend.resolve_file_id(prefix).await,
        }
    }

    async fn resolve_tag_id(&self, prefix: String) -> Result<TagId, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.resolve_tag_id(prefix).await,
            AnyBackend::Ipc(backend) => backend.resolve_tag_id(prefix).await,
        }
    }

    async fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.tags_for_file(file_id, subtag_rule).await,
            AnyBackend::Ipc(backend) => backend.tags_for_file(file_id, subtag_rule).await,
        }
    }

    async fn search(
        &self,
        query: String,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<SearchResults, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => {
                backend.search(query, subtag_rule, deleted_rule).await
            }
            AnyBackend::Ipc(backend) => backend.search(query, subtag_rule, deleted_rule).await,
        }
    }

    async fn get_file(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> Result<FileInfo, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.get_file(file_id, deleted_rule).await,
            AnyBackend::Ipc(backend) => backend.get_file(file_id, deleted_rule).await,
        }
    }

    async fn get_tag(&self, tag_id: TagId, deleted_rule: DeletedRule) -> Result<Tag, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.get_tag(tag_id, deleted_rule).await,
            AnyBackend::Ipc(backend) => backend.get_tag(tag_id, deleted_rule).await,
        }
    }

    async fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.subtags_for_tag(tag_id, subtag_rule).await,
            AnyBackend::Ipc(backend) => backend.subtags_for_tag(tag_id, subtag_rule).await,
        }
    }

    async fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.tags_for_tag(tag_id, subtag_rule).await,
            AnyBackend::Ipc(backend) => backend.tags_for_tag(tag_id, subtag_rule).await,
        }
    }

    async fn create_tag(&self, name: String, style: TagStyle) -> Result<TagId, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.create_tag(name, style).await,
            AnyBackend::Ipc(backend) => backend.create_tag(name, style).await,
        }
    }

    async fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.delete_tag(tag_id).await,
            AnyBackend::Ipc(backend) => backend.delete_tag(tag_id).await,
        }
    }

    async fn restore_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.restore_tag(tag_id).await,
            AnyBackend::Ipc(backend) => backend.restore_tag(tag_id).await,
        }
    }

    async fn rename_tag(&self, tag_id: TagId, name: String) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.rename_tag(tag_id, name).await,
            AnyBackend::Ipc(backend) => backend.rename_tag(tag_id, name).await,
        }
    }

    async fn set_tag_style(&self, tag_id: TagId, style: TagStyle) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.set_tag_style(tag_id, style).await,
            AnyBackend::Ipc(backend) => backend.set_tag_style(tag_id, style).await,
        }
    }

    async fn upload_file(
        &self,
        path: PathBuf,
        path_name: String,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.upload_file(path, path_name, tags).await,
            AnyBackend::Ipc(backend) => backend.upload_file(path, path_name, tags).await,
        }
    }

    async fn edit_file(&self, file_id: FileId, path: PathBuf) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.edit_file(file_id, path).await,
            AnyBackend::Ipc(backend) => backend.edit_file(file_id, path).await,
        }
    }

    async fn begin_edit(&self, file_id: FileId) -> Result<PathBuf, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.begin_edit(file_id).await,
            AnyBackend::Ipc(backend) => backend.begin_edit(file_id).await,
        }
    }

    async fn finish_edit(&self, file_id: FileId, path: PathBuf) -> Result<EditOutcome, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.finish_edit(file_id, path).await,
            AnyBackend::Ipc(backend) => backend.finish_edit(file_id, path).await,
        }
    }

    async fn cancel_edit(&self, path: PathBuf) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.cancel_edit(path).await,
            AnyBackend::Ipc(backend) => backend.cancel_edit(path).await,
        }
    }

    async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<PathBuf, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.fetch_file(file_id, expected_hash).await,
            AnyBackend::Ipc(backend) => backend.fetch_file(file_id, expected_hash).await,
        }
    }

    async fn get_preview(&self, file_id: FileId) -> Result<Preview, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.get_preview(file_id).await,
            AnyBackend::Ipc(backend) => backend.get_preview(file_id).await,
        }
    }

    async fn local_path_for_file(&self, file_id: FileId) -> Result<Option<PathBuf>, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.local_path_for_file(file_id).await,
            AnyBackend::Ipc(backend) => backend.local_path_for_file(file_id).await,
        }
    }

    async fn storage_stats(&self) -> Result<StorageStats, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.storage_stats().await,
            AnyBackend::Ipc(backend) => backend.storage_stats().await,
        }
    }

    async fn backup(&self) -> Result<BackupOutcome, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.backup().await,
            AnyBackend::Ipc(backend) => backend.backup().await,
        }
    }

    async fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.delete_file(file_id).await,
            AnyBackend::Ipc(backend) => backend.delete_file(file_id).await,
        }
    }

    async fn restore_file(&self, file_id: FileId) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.restore_file(file_id).await,
            AnyBackend::Ipc(backend) => backend.restore_file(file_id).await,
        }
    }

    async fn move_file(&self, file_id: FileId, logical_path: String) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.move_file(file_id, logical_path).await,
            AnyBackend::Ipc(backend) => backend.move_file(file_id, logical_path).await,
        }
    }

    async fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.tag_file(tag_id, file_id).await,
            AnyBackend::Ipc(backend) => backend.tag_file(tag_id, file_id).await,
        }
    }

    async fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.untag_file(tag_id, file_id).await,
            AnyBackend::Ipc(backend) => backend.untag_file(tag_id, file_id).await,
        }
    }

    async fn tag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.tag_tag(parent_id, subtag_id).await,
            AnyBackend::Ipc(backend) => backend.tag_tag(parent_id, subtag_id).await,
        }
    }

    async fn untag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.untag_tag(parent_id, subtag_id).await,
            AnyBackend::Ipc(backend) => backend.untag_tag(parent_id, subtag_id).await,
        }
    }

    async fn purge_previews(&self) -> Result<usize, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.purge_previews().await,
            AnyBackend::Ipc(backend) => backend.purge_previews().await,
        }
    }

    async fn editor_rules(&self) -> Result<Vec<EditorRule>, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.editor_rules().await,
            AnyBackend::Ipc(backend) => backend.editor_rules().await,
        }
    }

    async fn home_sections(&self) -> Result<Vec<HomeSection>, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.home_sections().await,
            AnyBackend::Ipc(backend) => backend.home_sections().await,
        }
    }

    async fn retag(&self, dry_run: bool) -> Result<RetagSummary, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.retag(dry_run).await,
            AnyBackend::Ipc(backend) => backend.retag(dry_run).await,
        }
    }

    async fn tag_rule_report(&self) -> Result<TagRuleReport, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.tag_rule_report().await,
            AnyBackend::Ipc(backend) => backend.tag_rule_report().await,
        }
    }

    fn subscribe(&self) -> EventStream {
        match self {
            AnyBackend::InProcess(backend) => backend.subscribe(),
            AnyBackend::Ipc(backend) => backend.subscribe(),
        }
    }

    async fn list_operations(&self) -> Result<Vec<Operation>, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.list_operations().await,
            AnyBackend::Ipc(backend) => backend.list_operations().await,
        }
    }

    fn subscribe_operations(&self) -> OperationStream {
        match self {
            AnyBackend::InProcess(backend) => backend.subscribe_operations(),
            AnyBackend::Ipc(backend) => backend.subscribe_operations(),
        }
    }

    async fn connected_peers(&self) -> Result<Vec<ConnectedPeer>, ApiError> {
        match self {
            AnyBackend::InProcess(backend) => backend.connected_peers().await,
            AnyBackend::Ipc(backend) => backend.connected_peers().await,
        }
    }

    fn subscribe_connections(&self) -> ConnectionStream {
        match self {
            AnyBackend::InProcess(backend) => backend.subscribe_connections(),
            AnyBackend::Ipc(backend) => backend.subscribe_connections(),
        }
    }
}
