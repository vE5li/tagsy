//! UI-facing API.
//!
//! This is the single, transport-agnostic API surface the UI talks to. It is
//! deliberately a **v1**: every operation maps 1:1 onto capabilities that
//! already exist in [`CatalogStore`](crate::store::CatalogStore) and the
//! change pipeline.
//!
//! ## Architecture
//!
//! The API is split into a read half and a write half because the core
//! enforces a single-writer model:
//!
//! - **Reads** open their own read-only [`CatalogStore`] handle from
//!   `main_db_path`, exactly as peer sessions do. A `&CatalogStore` is never
//!   held across an `.await`.
//! - **Writes** are expressed as [`Change`] values and pushed onto the ingest
//!   bus (`change_sender`). The single `handle_changes` task remains the only
//!   DB writer and performs idempotent persistence plus peer forwarding. This
//!   API adds no business logic and never writes the DB directly.
//!
//! Both process topologies (in-process on Android, IPC-to-daemon on Linux)
//! wrap this same [`ApiService`] handle; the Dart UI never knows which.
//!
//! ## Module layout
//!
//! The [`ApiService`] type lives here; its operations are split across sibling
//! modules by shape, each an `impl ApiService` block:
//!
//! - [`read`] — resolution, lookup, traversal and search (synchronous reads
//!   over a short-lived read handle);
//! - [`write`] — enqueue-based fire-and-forget mutations;
//! - [`request`] — async `oneshot` round-trips through the change pipeline,
//!   plus the shared timeout helper;
//! - [`edit`] — the begin/finish/cancel external-edit flow;
//! - [`backup`] — the tar+zstd archive builder ([`ApiService::backup`]);
//! - [`error`] — [`ApiError`] and its `From` impls;
//! - [`token`] — the pure search-query lexer.

mod backup;
mod edit;
mod error;
mod read;
mod request;
mod write;

pub(crate) mod token;

use std::path::PathBuf;
use std::sync::Arc;

// The UI-facing DTOs and `ApiError` cross the port and live in `tagsy-api`;
// re-exported here so the daemon's own code (and the transport/control layers)
// keep referencing `crate::frontend::api::{SearchResults, ApiEvent, ...}`.
// `error` still owns the `From<internal error>` conversions onto `ApiError`.
pub use tagsy_api::{
    ApiError, ApiEvent, BackupOutcome, EditOutcome, RetagSummary, SearchResults, StorageStats,
    TagRuleReport,
};
use tagsy_core::state::Change;
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedSender;

use crate::catalog::messages::CatalogCommand;
use crate::configuration::{CompiledTagRules, EditorRule, HomeSection};
use crate::peer::relay::ChunkRelay;
use crate::store::CatalogStore;
use crate::sync_directories::SyncDirectoryCommand;

/// The transport-agnostic UI-facing API handle.
///
/// Cheap to clone. Holds the pieces needed to serve reads (the DB path),
/// serve writes (the ingest-bus sender), and produce the event stream (a
/// broadcast subscription source). Constructed by [`run`](crate::run) and
/// wrapped by each transport backend.
#[derive(Clone)]
pub struct ApiService {
    main_db_path: PathBuf,
    change_sender: UnboundedSender<CatalogCommand>,
    /// Direct handle to the sync-directory manager, used only for read-only
    /// path lookups (`local_path_for_file`). Writes still go via
    /// `change_sender` and the `handle_changes` pipeline.
    command_sender: UnboundedSender<SyncDirectoryCommand>,
    events: broadcast::Sender<Change>,
    /// Fetch/transfer subsystem, used by the control layer to register a
    /// temporary chunk provider for an upload/edit (the client serves the bytes
    /// on demand).
    pending_fetches: ChunkRelay,
    /// Directory for daemon-owned temp files produced by `fetch_file`. A
    /// completed fetch materializes here and the path is handed to the caller
    /// with move semantics. See [`crate::paths::Paths::fetch_temp_dir`].
    fetch_temp_dir: PathBuf,
    /// Live sync-operation registry. Reads (`list_operations`) snapshot it;
    /// `subscribe_operations` taps its event broadcast. Fed by the peer
    /// sessions, not by this API.
    operations: crate::operations::Operations,
    /// Live peer-connection registry. Reads (`connected_peers`) snapshot it;
    /// `subscribe_connections` taps its event broadcast. Fed by the peer
    /// sessions (each registers itself for its lifetime), not by this API.
    connections: crate::connections::Connections,
    /// External-editor rules the desktop UI consults for its "edit" action
    /// (see [`crate::configuration::EditorRule`]). Snapshot of the startup
    /// configuration; the daemon does not act on these but stores them so
    /// every frontend attached to this device sees the same set.
    editor_rules: Vec<EditorRule>,
    /// Home-screen sections the desktop UI renders when the search box is empty
    /// (see [`crate::configuration::HomeSection`]). Snapshot of the startup
    /// configuration; the daemon does not act on these but stores them so every
    /// frontend attached to this device sees the same set.
    home_sections: Vec<HomeSection>,
    /// Compiled creation-time tag rules (see
    /// [`crate::configuration::TagRule`]). Shared with `handle_changes`, which
    /// applies them to newly-created files; this handle needs the same set to
    /// re-apply them to the existing catalog on demand ([`Self::retag`]) and
    /// to report broken rules.
    tag_rules: Arc<CompiledTagRules>,
    /// Resolved on-disk locations for this instance. Used by the archive
    /// builder to derive per-directory DB paths and the backup directory. The
    /// standalone `main_db_path` / `fetch_temp_dir` fields above are retained
    /// as-is and can be collapsed onto this later.
    paths: crate::paths::Paths,
}

impl ApiService {
    /// The overall deadline a caller waits for an on-demand fetch to complete.
    /// Must exceed [`crate::peer::transfer::HOP_TIMEOUT`] so intermediate hops
    /// can time out and report before this outer deadline fires.
    const FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// Build an API handle from the runtime's shared pieces.
    ///
    /// - `main_db_path`: the main DB path; each read opens its own read-only
    ///   handle on it (SQLite serializes file-level access).
    /// - `change_sender`: the ingest bus every mutation is pushed onto.
    /// - `command_sender`: the sync-directory manager command channel, used for
    ///   read-only path lookups.
    /// - `events`: the broadcast channel `handle_changes` publishes applied
    ///   changes to.
    /// - `paths`: resolved on-disk locations, used by the archive builder to
    ///   derive per-directory DB paths and the backup directory.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        main_db_path: PathBuf,
        change_sender: UnboundedSender<CatalogCommand>,
        command_sender: UnboundedSender<SyncDirectoryCommand>,
        events: broadcast::Sender<Change>,
        pending_fetches: ChunkRelay,
        fetch_temp_dir: PathBuf,
        operations: crate::operations::Operations,
        connections: crate::connections::Connections,
        editor_rules: Vec<EditorRule>,
        home_sections: Vec<HomeSection>,
        tag_rules: Arc<CompiledTagRules>,
        paths: crate::paths::Paths,
    ) -> Self {
        Self {
            main_db_path,
            change_sender,
            command_sender,
            events,
            pending_fetches,
            fetch_temp_dir,
            operations,
            connections,
            editor_rules,
            home_sections,
            tag_rules,
            paths,
        }
    }

    /// Snapshot of the desktop UI's tag-based editor rules (see
    /// [`crate::configuration::EditorRule`]). Read-only; taken from
    /// configuration at startup.
    pub fn editor_rules(&self) -> Vec<EditorRule> {
        self.editor_rules.clone()
    }

    /// Snapshot of the desktop UI's home-screen sections (see
    /// [`crate::configuration::HomeSection`]). Read-only; taken from
    /// configuration at startup.
    pub fn home_sections(&self) -> Vec<HomeSection> {
        self.home_sections.clone()
    }

    /// Open a fresh read-only DB handle for a single read call.
    ///
    /// `CatalogStore` is `Send + !Sync`; we never share one across `.await`,
    /// so each read opens its own handle and drops it before returning.
    fn open_read(&self) -> Result<CatalogStore, ApiError> {
        CatalogStore::initialize(&self.main_db_path).map_err(ApiError::from)
    }

    /// Subscribe to the live change stream.
    ///
    /// Yields every [`Change`] applied by `handle_changes` after this call.
    /// Delivery is best-effort: a slow subscriber that lags beyond the channel
    /// capacity observes a `RecvError::Lagged`, which the transport layer maps
    /// onto an [`ApiEvent::Resynced`] so the UI re-fetches state.
    pub fn subscribe(&self) -> broadcast::Receiver<Change> {
        self.events.subscribe()
    }

    /// Snapshot every currently-active sync operation.
    ///
    /// The read counterpart of
    /// [`subscribe_operations`](Self::subscribe_operations): the UI calls
    /// this for its initial paint (and after an IPC `Resynced`),
    /// then applies live [`OperationEvent`](crate::operations::OperationEvent)s
    /// on top. Order is unspecified; the caller sorts by `started_at`.
    pub fn list_operations(&self) -> Vec<crate::operations::Operation> {
        self.operations.snapshot()
    }

    /// Subscribe to the live sync-operation stream.
    ///
    /// Yields every [`OperationEvent`](crate::operations::OperationEvent)
    /// (started / progress / terminal) produced by the peer sessions after this
    /// call. Best-effort, exactly like [`subscribe`](Self::subscribe): a slow
    /// subscriber that lags past the channel capacity observes a
    /// `RecvError::Lagged`, which the transport maps onto a re-snapshot prompt.
    pub fn subscribe_operations(&self) -> broadcast::Receiver<crate::operations::OperationEvent> {
        self.operations.subscribe()
    }

    /// Snapshot every currently-connected peer.
    ///
    /// The read counterpart of
    /// [`subscribe_connections`](Self::subscribe_connections): the UI calls
    /// this for its initial paint of the connection indicator (and after an IPC
    /// `Resynced`), then applies live
    /// [`ConnectionEvent`](crate::connections::ConnectionEvent)s on top.
    pub fn connected_peers(&self) -> Vec<crate::connections::ConnectedPeer> {
        self.connections.snapshot()
    }

    /// Subscribe to the live peer-connection stream.
    ///
    /// Yields every [`ConnectionEvent`](crate::connections::ConnectionEvent)
    /// (a peer connected or disconnected) after this call. Best-effort, exactly
    /// like [`subscribe`](Self::subscribe).
    pub fn subscribe_connections(
        &self,
    ) -> broadcast::Receiver<crate::connections::ConnectionEvent> {
        self.connections.subscribe()
    }
}
