//! Central place for resolving on-disk locations under the tagsy data
//! directory. Keeping this in one struct avoids re-building paths by hand in
//! every call site.
//!
//! Each frontend constructs a [`Paths`] value explicitly:
//!
//! - the desktop binary reads the environment (see `main.rs`),
//! - Android would pass `getFilesDir()` through the bridge.
//!
//! No frontend-agnostic environment lookup lives here: a panic deep in the
//! library would crash an Android app without a shell environment.

use std::path::{Path, PathBuf};

/// Resolved on-disk locations for a single tagsy instance.
///
/// `data_dir` holds the databases (`main.db`, per-sync-directory `*.db`).
/// `identity_file` is the path to this machine's long-lived identity key.
#[derive(Debug, Clone)]
pub struct Paths {
    data_dir: PathBuf,
    backup_dir: Option<PathBuf>,
    identity_file: PathBuf,
}

impl Paths {
    /// Build a `Paths` from an explicit data directory and identity-file path.
    pub fn new(
        data_dir: impl Into<PathBuf>,
        backup_dir: Option<impl Into<PathBuf>>,
        identity_file: impl Into<PathBuf>,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            backup_dir: backup_dir.map(Into::into),
            identity_file: identity_file.into(),
        }
    }

    /// The tagsy data directory holding the databases.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// The directory for storing tagsy backups. `None` if backups are not
    /// enabled.
    pub fn backup_dir(&self) -> Option<&Path> {
        self.backup_dir.as_ref().map(AsRef::as_ref)
    }

    /// This machine's long-lived identity key.
    pub fn identity_path(&self) -> &Path {
        &self.identity_file
    }

    /// The main `CatalogStore` shared across the daemon.
    pub(crate) fn main_db_path(&self) -> PathBuf {
        self.data_dir.join("main.db")
    }

    /// The per-sync-directory `DirectoryIndex`, named after the
    /// directory it tracks (e.g. a directory `testcloud` maps to
    /// `testcloud.db`).
    pub(crate) fn sync_directory_db_path(&self, sync_directory: &Path) -> PathBuf {
        let name = sync_directory
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unnamed".to_owned());
        self.data_dir.join(format!("{name}.db"))
    }

    /// Directory holding daemon-owned temp files produced by on-demand fetches
    /// (`fetch_file`).
    ///
    /// A completed fetch materializes its bytes here and hands the caller the
    /// path with **move semantics**: the caller (the local CLI or the
    /// in-process UI, both co-located with the daemon and sharing this
    /// filesystem) must consume the file by renaming it into place or
    /// deleting it. If a caller crashes before consuming, the file is
    /// orphaned; [`Self::clean_fetch_temp_dir`] sweeps such leftovers on
    /// daemon start.
    ///
    /// It lives under `data_dir` (rather than the system temp dir) so the
    /// daemon owns and can clean it, and so a fetch temp and a download
    /// destination under the same data root tend to share a filesystem
    /// (cheap rename).
    pub(crate) fn fetch_temp_dir(&self) -> PathBuf {
        self.data_dir.join("fetch-temp")
    }

    /// Remove any orphaned files left in the fetch-temp directory by callers
    /// that crashed before consuming their fetched file, then ensure the
    /// directory exists. Best-effort: called on daemon start.
    pub(crate) async fn clean_fetch_temp_dir(&self) -> std::io::Result<()> {
        let dir = self.fetch_temp_dir();
        // Remove the whole directory (clearing any orphans) and recreate it.
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        tokio::fs::create_dir_all(&dir).await
    }
}

// The control-socket path is shared with clients (the CLI, the IPC backend), so
// it lives in `tagsy-core` below both; re-exported here for the daemon-side
// callers that reference `crate::paths::control_socket_path`.
pub use tagsy_core::paths::{CONTROL_SOCKET_PATH, control_socket_path};
