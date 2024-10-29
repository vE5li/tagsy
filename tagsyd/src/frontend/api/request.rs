//! Request/reply half of the API: async `oneshot` round-trips through the
//! change pipeline.
//!
//! Unlike the [`write`](super::write) mutations, these operations need the
//! daemon's answer — availability of bytes to restore, a fetched temp path, a
//! resolved preview — so each sends a [`CatalogCommand`] carrying a
//! `oneshot::Sender` and awaits the reply under a shared deadline. That
//! deadline / channel-closed handling is identical across all four and lives in
//! [`ApiService::await_reply`].

use std::path::PathBuf;

use tagsy_core::{FileId, Preview};
use tokio::sync::oneshot;

use super::{ApiError, ApiService};
use crate::catalog::messages::{CatalogCommand, FetchError, PreviewError, RestoreError};

impl ApiService {
    /// Await a daemon reply on `response` under [`Self::FETCH_TIMEOUT`],
    /// collapsing the two transport-level failure modes onto [`ApiError`]:
    ///
    /// - the deadline elapsing → [`FetchError::TimedOut`];
    /// - the responder being dropped without sending (runtime shutting down) →
    ///   `ApiError::Internal(recv_error_message)`.
    ///
    /// The channel's payload `T` is returned untouched — it is typically itself
    /// a `Result<_, SomeError>` that the caller maps onto [`ApiError`], since
    /// the *operation's* success/failure is a separate axis from whether a
    /// reply arrived at all.
    ///
    /// `recv_error_message` lets each caller name the shutdown in its own
    /// vocabulary (e.g. [`RestoreError::ShuttingDown`] vs.
    /// [`PreviewError::ShuttingDown`]) so the surfaced text still matches the
    /// operation.
    async fn await_reply<T>(
        response: oneshot::Receiver<T>,
        recv_error_message: String,
    ) -> Result<T, ApiError> {
        match tokio::time::timeout(Self::FETCH_TIMEOUT, response).await {
            Ok(Ok(payload)) => Ok(payload),
            // The responder was dropped without sending — treat as shutdown.
            Ok(Err(_recv_error)) => Err(ApiError::Internal(recv_error_message)),
            Err(_elapsed) => Err(ApiError::Internal(FetchError::TimedOut.to_string())),
        }
    }

    /// Restore a soft-deleted file — best-effort.
    ///
    /// Sends a [`CatalogCommand::Restore`] and awaits its outcome. The daemon
    /// checks whether the file's latest version is still recoverable (its own
    /// `keep_deleted_files` vault first, then a probe flooded across the peer
    /// tree). Only if the bytes are available does it clear the tombstone,
    /// record the restored version, announce a `Change::FileRestored` to peers,
    /// and pull the bytes into whichever local sync directories want them. If
    /// nothing holds the bytes the tombstone is left in place and this returns
    /// [`ApiError::ContentUnavailable`].
    ///
    /// Request-reply (unlike `delete_file`) because the outcome is only known
    /// after the async availability probe.
    pub async fn restore_file(&self, file_id: FileId) -> Result<(), ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.change_sender
            .send(CatalogCommand::Restore {
                file_id,
                respond_to,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;

        Self::await_reply(response, RestoreError::ShuttingDown.to_string())
            .await?
            .map_err(ApiError::from)
    }

    /// Fetch a file's content on demand, from a peer if not present locally,
    /// and return the path to a **daemon-owned temp file** holding it.
    ///
    /// Enqueues a [`CatalogCommand::Fetch`] onto the ingest bus;
    /// `handle_changes` checks the local sync directories first
    /// (hash-gated) and, failing that, drives a content-addressed receive that
    /// floods `Sync::ChunkRequest`s across the live peer tree. Awaits
    /// the reply with an overall timeout. `expected_hash` gates which content
    /// is accepted; the caller obtains it from the file's known metadata
    /// (`FileInfo::content_hash`).
    ///
    /// The returned path lives under [`crate::paths::Paths::fetch_temp_dir`]
    /// and is handed to the caller with **move semantics**: the caller must
    /// consume it (rename into place or delete). The whole file is never
    /// buffered into memory — a peer transfer already lands as a temp file
    /// on disk, and a locally-held copy is streamed into the fetch temp
    /// dir.
    ///
    /// The materialized path has the shape
    /// `<fetch_temp_dir>/<uuid>/<logical_basename>` (an isolated
    /// per-request subdirectory whose leaf carries the file's logical name,
    /// including extension). The extension is load-bearing: editors, share
    /// sheets, and downloads all key their behavior off it. Callers should
    /// clean up the *parent* directory (`<fetch_temp_dir>/<uuid>`) rather
    /// than just the file, so an unmoved temp leaves nothing behind. Any
    /// leftover subdirectories are also wiped in bulk on the next daemon
    /// start (see [`crate::paths::Paths::clean_fetch_temp_dir`]), so a
    /// missed cleanup only leaks until the next restart.
    pub async fn fetch_file(
        &self,
        file_id: FileId,
        expected_hash: String,
    ) -> Result<PathBuf, ApiError> {
        // Read the file's logical basename before enqueuing the fetch so a
        // completed fetch always has a name to land under. This is one extra
        // by-id read (cheap; same shape as `get_file`), and doing it here
        // means every caller — CLI download, UI download/share, and the
        // upcoming edit flow — gets the right on-disk name for free.
        let logical_basename = {
            let database = self.open_read()?;
            let info = database.file_info_from_id(file_id, crate::store::DeletedRule::Include)?;
            let basename = info.logical_path.basename().to_owned();
            // A pathological empty basename would resolve to
            // `<uuid>/` which some filesystems reject and which would in any
            // case give the editor no extension to dispatch on. Fall back to
            // the file id, which at least yields a stable, unique name.
            if basename.is_empty() {
                file_id.to_string()
            } else {
                basename
            }
        };

        let (respond_to, response) = oneshot::channel();
        self.change_sender
            .send(CatalogCommand::Fetch {
                file_id,
                expected_hash,
                respond_to,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;

        // The channel carries `Result<FileBytes, FetchError>`; unwrap the
        // transport layer first, then the operation's own result.
        let content = Self::await_reply(response, FetchError::ShuttingDown.to_string())
            .await?
            .map_err(ApiError::from)?;

        // Materialize into `<fetch_temp_dir>/<uuid>/<logical_basename>`. The
        // per-request `<uuid>` subdirectory isolates the file so it can carry
        // its real name (matching extension included) without colliding with
        // other in-flight fetches of the same logical basename.
        let subdir = self.fetch_temp_dir.join(uuid::Uuid::new_v4().to_string());
        tokio::fs::create_dir_all(&subdir).await.map_err(|error| {
            ApiError::Internal(format!("failed to create fetch temp subdir: {error}"))
        })?;
        let dest = subdir.join(&logical_basename);
        content.materialize_to(&dest).await.map_err(|error| {
            ApiError::Internal(format!("failed to stage fetched file: {error}"))
        })?;
        Ok(dest)
    }

    /// Get the preview for `file_id`'s current content.
    ///
    /// Enqueues a [`CatalogCommand::GetPreview`] onto the ingest bus;
    /// `handle_changes` returns any cached preview, else generates it locally
    /// (if the bytes are present) or requests it from a peer (first responder
    /// wins), caching the result in `previews_v1` before replying.
    ///
    /// A file with no previewable content resolves to [`Preview::None`] — that
    /// is a successful result, not an error. `ApiError::UnknownId` means the
    /// file id itself is unknown to the catalog.
    ///
    /// `ApiError::ContentUnavailable` is the *transient* case: the file exists
    /// but a preview could not be obtained right now (no local bytes to
    /// generate from and no reachable peer served one). It is deliberately not
    /// cached, so the UI should offer a retry rather than treat it as a
    /// permanent "no preview".
    pub async fn get_preview(&self, file_id: FileId) -> Result<Preview, ApiError> {
        // End-to-end stopwatch for the whole daemon-side request (bus enqueue →
        // handle_changes resolution → reply). Combined with the finer-grained
        // logs inside `handle_changes`, this shows how much time is the actual
        // work vs. queueing behind other messages on the single-writer bus.
        let api_start = std::time::Instant::now();
        log::debug!(
            "ApiService::get_preview: requesting preview for {}",
            file_id.to_string()
        );

        let (respond_to, response) = oneshot::channel();
        self.change_sender
            .send(CatalogCommand::GetPreview {
                file_id,
                respond_to,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;

        let result = Self::await_reply(response, PreviewError::ShuttingDown.to_string())
            .await
            .and_then(|reply| reply.map_err(ApiError::from));
        log::debug!(
            "ApiService::get_preview: preview for {} resolved in {:?} (ok={})",
            file_id.to_string(),
            api_start.elapsed(),
            result.is_ok()
        );
        result
    }

    /// Purge the entire preview cache, returning how many cached previews were
    /// removed.
    ///
    /// Enqueues a [`CatalogCommand::PurgePreviews`] onto the ingest bus so the
    /// wipe runs on the sole main-DB writer (`handle_changes`). Previews are
    /// hash-keyed and regenerated on demand, so this never affects correctness;
    /// it forces every file to be re-evaluated on its next preview request.
    /// Exposed to operators via the `tagsy purge-previews` CLI command.
    pub async fn purge_previews(&self) -> Result<usize, ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.change_sender
            .send(CatalogCommand::PurgePreviews { respond_to })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;

        Self::await_reply(response, "runtime is shutting down".to_owned())
            .await?
            .map_err(ApiError::from)
    }
}
