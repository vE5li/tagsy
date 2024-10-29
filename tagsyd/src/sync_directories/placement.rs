//! Placement of a file's bytes into the sync directories that should hold it:
//! resolving a collision-free physical path, applying tag-based placement
//! (add/remove/defer), and finding a local source copy to place from.

use std::path::{Path, PathBuf};

use tagsy_core::{FileId, LogicalPath, PhysicalPath, TagId};

use super::{OpenDirectory, SyncDirectories, SyncDirectoryError};
use crate::catalog::placement::contains_all_tags;
use crate::configuration::SyncType;
use crate::file_bytes::FileBytes;

impl SyncDirectories {
    /// Resolve a collision-free physical path for placing `file_id` at `base`
    /// within `sync_directory`.
    ///
    /// Two files may legitimately share a logical path (even within one
    /// TagBased directory), but their bytes must live at distinct on-disk
    /// locations. If `base` is already taken — by another file's DB row *or* by
    /// an untracked file on disk — a ` (N)` suffix is inserted before the file
    /// extension (`name.txt` -> `name (1).txt` -> `name (2).txt`, …) using the
    /// lowest free integer. The returned `PhysicalPath` is the exact name that
    /// must be used for *both* the on-disk write and the DB row, so the
    /// path -> file_id reverse index (`get_file_id`) stays consistent.
    ///
    /// `file_id` is self-excluded from the DB check so re-placing or no-op
    /// moving an already-placed file returns its own name rather than a new
    /// suffix. Returns `base` unchanged when there is no collision.
    pub(super) fn resolve_unique_physical(
        &self,
        sync_directory: &OpenDirectory,
        base: &PhysicalPath,
        file_id: FileId,
    ) -> PhysicalPath {
        let is_free = |candidate: &PhysicalPath| -> bool {
            // A different file_id already claiming this path in the DB is
            // always a collision.
            if sync_directory
                .database
                .physical_path_in_use_by_other(candidate, file_id)
                .unwrap_or(false)
            {
                return false;
            }
            // If nothing exists on disk at the candidate path, we're free.
            if !sync_directory.path.join(candidate.as_str()).exists() {
                return true;
            }
            // Something is on disk. It's only a collision if it belongs to a
            // *different* tracked file, or is untracked entirely — placing on
            // top of either would clobber data. If the on-disk file is *this
            // very file* (its own DB row points here) the path is already
            // correctly ours and must not be treated as taken; that would
            // otherwise cause replayed `MoveFile`s / re-placements to
            // pointlessly rename an already-correctly-placed file to
            // `name (1).ext`.
            matches!(sync_directory.database.get_file_id(candidate), Ok(owner) if owner == file_id)
        };

        if is_free(base) {
            return base.clone();
        }

        // Suffix the *final path component* only, before its extension, letting
        // `std::path` do the parsing: `file_stem`/`extension` peel just the last
        // extension (`bar.tar.gz` -> `bar.tar` + `gz`) and treat a dotfile as a
        // stem with no extension (`.env` -> `.env` + none), which is exactly the
        // behavior we want. The parent directory is preserved verbatim.
        let base_path = Path::new(base.as_str());
        let parent = base_path.parent();
        let stem = base_path.file_stem().unwrap_or_default().to_string_lossy();
        let extension = base_path.extension().map(|ext| ext.to_string_lossy());

        // Lowest free integer. Bounded loop guards against a pathological
        // directory; u32::MAX collisions is not a realistic state.
        for counter in 1..=u32::MAX {
            let file_name = match &extension {
                Some(ext) => format!("{stem} ({counter}).{ext}"),
                None => format!("{stem} ({counter})"),
            };
            let candidate_path = match parent {
                Some(parent) if !parent.as_os_str().is_empty() => parent.join(&file_name),
                _ => PathBuf::from(&file_name),
            };
            let candidate = PhysicalPath::new(candidate_path.to_string_lossy());
            if is_free(&candidate) {
                return candidate;
            }
        }

        // Unreachable in practice; fall back to the base rather than panic.
        base.clone()
    }

    /// Re-run tag-based placement for `file_id` against its current
    /// `file_tags`. See [`SyncDirectoryCommand::ApplyPlacement`] for
    /// the rationale.
    ///
    /// For each TagBased directory: if it should hold the file (its tags are a
    /// subset of `file_tags`) but does not, create it there sourcing the bytes
    /// from any directory that already holds the file; if it holds the file but
    /// should not, remove it. Universal directories are skipped. Idempotent.
    ///
    /// Returns `true` if a TagBased directory should hold the file but no local
    /// copy exists to source the bytes from — i.e. placement was *deferred* and
    /// the caller must fetch the bytes over the network. `false` otherwise.
    pub(super) async fn apply_placement(
        &self,
        file_id: FileId,
        logical_path: &LogicalPath,
        file_tags: &[TagId],
    ) -> Result<bool, SyncDirectoryError> {
        // Source path lazily: we only need it if some directory must gain the
        // file. Find the on-disk path in the first directory that already holds
        // it. We keep the *path* (not `FileBytes`) so each destination can build
        // its own `FileToCopy`, which leaves the source in place for the next.
        let mut source_path: Option<PathBuf> = None;
        let mut deferred = false;

        for sync_directory in &self.sync_directories {
            let SyncType::TagBased {
                tags: sync_directory_tags,
            } = &sync_directory.sync_type
            else {
                // Universal directories have no tag filter; their membership
                // never changes on a tag update.
                continue;
            };

            let should_hold = contains_all_tags(sync_directory_tags, file_tags);
            let currently_holds = sync_directory.database.get_file(file_id).is_ok();

            match (should_hold, currently_holds) {
                (true, false) => {
                    // Newly matching: place the file here. Source the bytes from
                    // a directory that already holds it; if none does, report the
                    // deferral so the caller fetches the bytes over the network.
                    if source_path.is_none() {
                        source_path = self.first_holding_path(file_id);
                    }

                    let Some(source_path) = &source_path else {
                        log::debug!(
                            "ApplyPlacement: no source copy of {} yet; deferring placement into \
                             {} (caller will fetch)",
                            file_id.to_string(),
                            sync_directory.path.to_string_lossy()
                        );
                        deferred = true;
                        continue;
                    };

                    // Resolve on-disk naming collisions: another file may
                    // already occupy this logical path here. We reach this arm
                    // only when the directory does not yet hold `file_id`, so
                    // any clash is genuinely with a *different* file and must be
                    // disambiguated with a suffix.
                    let base_physical_path =
                        sync_directory.sync_type.physical_for(logical_path, file_id);
                    let physical_path =
                        self.resolve_unique_physical(sync_directory, &base_physical_path, file_id);
                    let file_path = sync_directory.path.join(physical_path.as_str());

                    log::info!(
                        "ApplyPlacement: adding {} to {}",
                        file_id.to_string(),
                        file_path.to_string_lossy()
                    );

                    // Re-materialize from the source. `FileToCopy` leaves the
                    // source in place, so multiple destinations can share it.
                    std::fs::create_dir_all(file_path.parent().ok_or_else(|| {
                        SyncDirectoryError::FailedAddingFile(
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                format!("{} has no parent directory", file_path.display()),
                            )
                            .into(),
                        )
                    })?)
                    .map_err(|error| SyncDirectoryError::FailedAddingFile(error.into()))?;

                    let content = FileBytes::FileToCopy(source_path.clone());
                    // `FileToCopy` leaves the source in place, so hashing does
                    // not consume it; do it before materializing so the write
                    // is recognized as self-caused.
                    let content_hash = content
                        .hash()
                        .await
                        .map_err(|error| SyncDirectoryError::FailedAddingFile(error.into()))?;
                    content.materialize_to(&file_path).await.map_err(|error| {
                        log::error!(
                            "ApplyPlacement: failed to materialize {}: {error}",
                            file_path.display()
                        );
                        SyncDirectoryError::FailedAddingFile(error.into())
                    })?;

                    // `physical_path` is the suffix-resolved name, so the DB row
                    // matches the actual on-disk name (preserving the
                    // path -> file_id reverse index).
                    sync_directory
                        .database
                        .add_file(file_id, &physical_path)
                        .map_err(|error| SyncDirectoryError::FailedAddingFile(error.into()))?;

                    self.record_self_write(file_path, Some(content_hash));
                }
                (false, true) => {
                    // No longer matching: drop the file from this directory.
                    let file = sync_directory
                        .database
                        .get_file(file_id)
                        .map_err(|error| SyncDirectoryError::FailedRemovingFile(error.into()))?;
                    let file_path = sync_directory.path.join(file.physical_path.as_str());

                    log::info!(
                        "ApplyPlacement: removing {} from {}",
                        file_id.to_string(),
                        file_path.to_string_lossy()
                    );

                    std::fs::remove_file(&file_path)
                        .map_err(|error| SyncDirectoryError::FailedRemovingFile(error.into()))?;

                    if let Some(directory) = PathBuf::from(file.physical_path.as_str()).parent()
                        && !directory.as_os_str().is_empty()
                    {
                        self.try_remove_empty_directory(sync_directory.path.join(directory));
                    }

                    sync_directory
                        .database
                        .remove_file_by_id(file_id)
                        .map_err(|error| SyncDirectoryError::FailedRemovingFile(error.into()))?;

                    self.record_self_write(file_path, None);
                }
                // Already in the desired state: nothing to do.
                (true, true) | (false, false) => {}
            }
        }

        Ok(deferred)
    }

    /// The on-disk path of `file_id`'s bytes in the first sync directory that
    /// holds it (and where the file actually exists on disk), or `None` if no
    /// directory has it. Used as the copy source when re-placing the file.
    pub(super) fn first_holding_path(&self, file_id: FileId) -> Option<PathBuf> {
        for sync_directory in &self.sync_directories {
            let Ok(file) = sync_directory.database.get_file(file_id) else {
                continue;
            };

            let path = match &sync_directory.sync_type {
                SyncType::Universal { .. } => sync_directory.path.join(file_id.to_string()),
                SyncType::TagBased { .. } => sync_directory.path.join(file.physical_path.as_str()),
            };

            if path.exists() {
                return Some(path);
            }
        }

        None
    }
}
