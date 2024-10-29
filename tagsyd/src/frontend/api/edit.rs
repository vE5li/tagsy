//! The external-edit flow: begin / finish / cancel.
//!
//! An edit is stateless on the daemon side — the caller's `file_id` plus the
//! path returned by [`ApiService::begin_edit`] fully describe the follow-up
//! [`ApiService::finish_edit`] / [`ApiService::cancel_edit`]. See
//! [`ApiService::begin_edit`] for the two branches (edit-in-place vs. fetch to
//! a temp) and [`ApiService::finish_edit`] for the temp-file lifetime rules.

use std::path::PathBuf;

use tagsy_core::FileId;

use super::{ApiError, ApiService, EditOutcome};
use crate::store::DeletedRule;

impl ApiService {
    /// Start an external edit: return the on-disk path the caller should hand
    /// to an editor.
    ///
    /// If the file lives in a local sync directory, returns that real
    /// on-disk path (edit-in-place; the watcher propagates the save). Otherwise
    /// fetches the content — from a peer if needed — into an isolated
    /// per-request subdirectory under [`crate::paths::Paths::fetch_temp_dir`],
    /// named with the file's logical basename (extension preserved so editors
    /// dispatch by MIME correctly), and returns that path with move semantics.
    ///
    /// No daemon-side state is kept across the edit. The caller's `file_id`
    /// plus the returned `path` fully describe the follow-up
    /// [`Self::finish_edit`] / [`Self::cancel_edit`]. A caller that crashes
    /// before finishing only leaks the temp file, which the daemon bulk-cleans
    /// on next start (see [`crate::paths::Paths::clean_fetch_temp_dir`]).
    pub async fn begin_edit(&self, file_id: FileId) -> Result<PathBuf, ApiError> {
        // Fast path: the bytes already live in a local sync directory. Give
        // the editor the real file; the watcher will pick up the save.
        if let Some(path) = self.local_path_for_file(file_id).await? {
            return Ok(path);
        }

        // Otherwise materialize into a caller-visible temp. `fetch_file`
        // handles the extension-preserving naming and the on-demand peer
        // pull. Getting the expected hash costs one by-id read; the file
        // must exist for us to fetch it.
        let info = self.get_file(file_id, DeletedRule::Include)?;
        self.fetch_file(file_id, info.content_hash).await
    }

    /// Complete an external edit started with [`Self::begin_edit`]: publish a
    /// new version if the bytes at `path` differ from the file's currently
    /// recorded content.
    ///
    /// Hashing is streaming; the bytes are never buffered whole. The
    /// comparison is against the DB's *current* `content_hash`, so an in-place
    /// edit whose save was already ingested by the watcher no-ops here
    /// automatically.
    ///
    /// # Temp file lifetime
    ///
    /// When the bytes changed, the daemon registers `path` as a chunk provider
    /// (via `FileToCopy`) so peers can pull the new content **on demand** —
    /// reads happen after this call returns, from `path` on disk. The temp
    /// is therefore **not deleted here**: doing so would break peers mid-pull
    /// with a "No such file or directory" error. The temp is left in place
    /// and cleaned up in bulk on the next daemon start (see
    /// [`crate::paths::Paths::clean_fetch_temp_dir`]), matching the "provider
    /// outlives the API call" semantics that
    /// [`crate::transport::Backend::upload_file`] has always had.
    ///
    /// The no-op branch (bytes unchanged) still cleans up: no provider was
    /// registered, no peer will ever read from `path`, so the temp is safe to
    /// remove immediately.
    pub async fn finish_edit(
        &self,
        file_id: FileId,
        path: PathBuf,
    ) -> Result<EditOutcome, ApiError> {
        // Compare the edited bytes against the file's current recorded hash.
        // If they match there is nothing to publish — either the editor
        // produced no change, or the watcher already ingested the in-place
        // save and updated the DB.
        let (edited_hash, edited_size) = crate::file_bytes::hash_and_len(&path).await?;
        let current_hash = self.get_file(file_id, DeletedRule::Include)?.content_hash;

        if edited_hash == current_hash {
            // No-op: nothing was published, nothing else will read `path`.
            self.cleanup_edit_path(&path);
            return Ok(EditOutcome { changed: false });
        }

        // Publish the new content by streaming it from `path` via the
        // usual chunk-provider protocol. Peers pull on demand *after* this
        // call returns, so `path` must remain readable until the daemon
        // restarts. See the method docs.
        let source = crate::file_bytes::FileBytes::FileToCopy(path);
        self.edit_file(file_id, edited_hash.clone(), edited_size)?;
        self.register_provider(file_id, edited_hash, std::sync::Arc::new(source))
            .await;
        Ok(EditOutcome { changed: true })
    }

    /// Abort an external edit started with [`Self::begin_edit`] without
    /// publishing. Cleans up any daemon-owned temp under
    /// [`crate::paths::Paths::fetch_temp_dir`]; other paths are left alone.
    pub fn cancel_edit(&self, path: PathBuf) -> Result<(), ApiError> {
        self.cleanup_edit_path(&path);
        Ok(())
    }

    /// Remove the per-request subdir the daemon created for a Branch B
    /// `begin_edit`, iff `path` lives under `fetch_temp_dir`. A path handed
    /// out for Branch A (a real sync-dir file) is silently ignored — we
    /// never delete user data.
    ///
    /// Best-effort: any I/O failure here is swallowed. The daemon bulk-wipes
    /// `fetch_temp_dir` on its next start regardless, so a missed cleanup is
    /// a bounded leak.
    fn cleanup_edit_path(&self, path: &std::path::Path) {
        // Only touch paths that are actually inside our fetch temp dir.
        // `starts_with` compares path components, so it is not fooled by
        // string-level tricks (e.g. a `..` in a caller-supplied path).
        if !path.starts_with(&self.fetch_temp_dir) {
            return;
        }
        // `fetch_file` materializes as `<fetch_temp_dir>/<uuid>/<basename>`,
        // so removing the parent (`<uuid>`) drops both the file and the
        // now-empty subdir.
        if let Some(parent) = path.parent()
            && parent != self.fetch_temp_dir
        {
            let _ = std::fs::remove_dir_all(parent);
        }
    }
}
