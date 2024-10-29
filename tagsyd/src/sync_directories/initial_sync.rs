//! The startup pass that reconciles a sync directory's on-disk contents against
//! the catalog: files deleted, edited, or added while the daemon (and its
//! watcher) were not running. `run_initial_sync` drives it over every sync
//! directory; `initial_sync` is one pass, parameterised over the three points
//! where a Universal directory (named by `file_id`) and a TagBased directory
//! (named by physical path) differ:
//!
//!   1. **on-disk name** of a tracked file — its `file_id`, or its physical
//!      path.
//!   2. **untracked detection** in the on-disk walk — a Universal file is keyed
//!      by parsing its name as a `file_id`; a TagBased file by its physical
//!      path.
//!   3. **ingestion** of an untracked file — `upload_file` (Universal, moves
//!      the bytes out to their content-addressed home, no tags) vs `add_file`
//!      (TagBased, leaves the bytes in place, carries the directory's tags).

use std::collections::HashMap;

use tagsy_core::{FileId, PhysicalPath, TagId};
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

use super::{OpenDirectory, SyncDirectories};
use crate::configuration::SyncType;
use crate::store::{DatabaseError, SyncDirectoryFile};

/// How a sync directory names and ingests files — the axis the two initial-sync
/// passes differ on. Derived from a directory's [`SyncType`].
enum Naming<'a> {
    /// Universal: a tracked file's on-disk name is its `file_id`; an untracked
    /// file is uploaded (bytes moved to their content-addressed home) with no
    /// tags.
    ById,
    /// TagBased: a tracked file's on-disk name is its physical path; an
    /// untracked file is added in place, carrying the directory's `tags`.
    ByPath { tags: &'a [TagId] },
}

impl SyncDirectories {
    /// Reconcile one sync directory's on-disk state against its index.
    ///
    /// Two loops: the first walks the *tracked* files (from the index) and
    /// syncs any that were deleted or edited off-watch; the second walks
    /// the *disk* and ingests any file the index does not know about.
    async fn initial_sync(
        &self,
        sync_directory: &OpenDirectory,
        files: Vec<SyncDirectoryFile>,
        naming: Naming<'_>,
        last_known_hashes: &HashMap<FileId, String>,
    ) {
        // Pass 1: tracked files. The only per-kind difference is the on-disk
        // name of a tracked file.
        for sync_file in files {
            let relative_name = match naming {
                Naming::ById => sync_file.file_id.to_string(),
                Naming::ByPath { .. } => sync_file.physical_path.as_str().to_owned(),
            };
            let full_path = sync_directory.path.join(&relative_name);

            log::debug!("Checking file {}", full_path.to_string_lossy());

            if !full_path.exists() {
                log::info!(
                    "File {} was deleted without monitoring. Syncing deletion",
                    full_path.to_string_lossy()
                );

                if let Err(error) = self.remove_file_by_id(sync_directory, sync_file.file_id) {
                    log::error!(
                        "Failed to remove file {}: {:?}",
                        full_path.to_string_lossy(),
                        error
                    );
                }

                continue;
            }

            let (content, content_hash, size) = match self.get_file_content(&full_path).await {
                Ok(triple) => triple,
                Err(error) => {
                    log::error!("Failed to read file content: {:?}", error);
                    continue;
                }
            };

            let last_known_hash = last_known_hashes.get(&sync_file.file_id);
            if last_known_hash.map(String::as_str) != Some(content_hash.as_str()) {
                log::info!(
                    "File {} was changed without monitoring. Syncing change",
                    full_path.to_string_lossy()
                );

                if let Err(error) = self.update_file_content(
                    sync_directory,
                    sync_file.file_id,
                    content,
                    content_hash,
                    size,
                ) {
                    log::error!(
                        "Failed to update file {}: {:?}",
                        full_path.to_string_lossy(),
                        error
                    );
                }
                // `update_file_content` enqueues a `FileChanged`, which warms
                // the preview via `handle_content_change`; nothing to do here.
            } else {
                // Unchanged while offline: no `ContentChange` is emitted, so
                // retroactively warm its preview (no-op if already cached).
                self.maybe_eager_preview(sync_file.file_id);
            }
        }

        // Pass 2: on-disk files the index does not track — ingest them.
        for entry in WalkDir::new(&sync_directory.path)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
        {
            let Ok(relative_path) = entry.path().strip_prefix(&sync_directory.path) else {
                // Should be impossible: `WalkDir` is rooted at the sync
                // directory, so every entry is under it.
                log::error!("Walkdir returned a path outside of the sync directory");
                continue;
            };

            // Is this file already tracked? The lookup key is the per-kind
            // difference. A real database error (anything but `MissingFile`)
            // must skip this one file rather than crash the sole sync-directory
            // thread and abandon every other directory's initial sync.
            let tracked = match naming {
                Naming::ById => {
                    // FIX: Maybe this should not use to_string_lossy but rather
                    // to utf8 since a valid uuid will always be valid utf8?
                    match FileId::from_string(&relative_path.to_string_lossy()) {
                        Some(file_id) => match sync_directory.database.get_file(file_id) {
                            Ok(_) => true,
                            Err(DatabaseError::MissingFile) => false,
                            Err(error) => {
                                log::error!(
                                    "initial sync: DB error checking {}: {:?}; skipping",
                                    relative_path.to_string_lossy(),
                                    error
                                );
                                continue;
                            }
                        },
                        // A name that is not a valid `file_id` cannot be tracked
                        // in a Universal directory: treat it as untracked.
                        None => false,
                    }
                }
                Naming::ByPath { .. } => {
                    match sync_directory
                        .database
                        .get_file_id(&PhysicalPath::new(relative_path.to_string_lossy()))
                    {
                        Ok(_) => true,
                        Err(DatabaseError::MissingFile) => false,
                        Err(error) => {
                            log::error!(
                                "initial sync: DB error checking {}: {:?}; skipping",
                                relative_path.to_string_lossy(),
                                error
                            );
                            continue;
                        }
                    }
                }
            };

            if tracked {
                log::debug!(
                    "File {} is already tracked",
                    relative_path.to_string_lossy()
                );
                continue;
            }

            log::info!(
                "File {} was added without monitoring. Syncing addition",
                entry.path().to_string_lossy()
            );

            let (content, content_hash, size) = match self.get_file_content(entry.path()).await {
                Ok(triple) => triple,
                Err(error) => {
                    log::error!("Failed to read added file: {:?}", error);
                    continue;
                }
            };

            // Ingest: Universal uploads (moving bytes out, no tags), TagBased
            // adds in place with the directory's tags.
            let ingested = match naming {
                Naming::ById => self.upload_file(
                    sync_directory,
                    relative_path,
                    content,
                    content_hash,
                    size,
                    Vec::new(),
                ),
                Naming::ByPath { tags } => self.add_file(
                    sync_directory,
                    relative_path,
                    content,
                    content_hash,
                    size,
                    tags.to_vec(),
                ),
            };

            if let Err(error) = ingested {
                log::error!(
                    "Failed to ingest added file {}: {:?}",
                    relative_path.to_string_lossy(),
                    error
                );
            }
        }
    }

    pub(super) async fn run_initial_sync(
        &mut self,
        last_known_hashes: &HashMap<FileId, String>,
        shutdown: &CancellationToken,
    ) {
        for sync_directory in &self.sync_directories {
            // Cooperative shutdown: the initial sweep can be long (it hashes
            // every tracked file), so honour a shutdown between directories
            // rather than only after the whole sweep. Stopping here leaves the
            // per-directory index untouched — the next start re-runs the sweep
            // — so a partially-done initial sync is always safe to abandon.
            if shutdown.is_cancelled() {
                log::info!("Shutdown requested during initial sync; stopping");
                return;
            }

            log::debug!(
                "Checking for missed updates at {}",
                sync_directory.path.to_string_lossy()
            );

            let files = match self.get_all_files(sync_directory) {
                Ok(files) => files,
                Err(error) => {
                    log::error!("Failed to get list of tracked files: {:?}", error);
                    continue;
                }
            };

            let naming = match &sync_directory.sync_type {
                SyncType::Universal { .. } => Naming::ById,
                SyncType::TagBased { tags } => Naming::ByPath { tags },
            };
            self.initial_sync(sync_directory, files, naming, last_known_hashes)
                .await;
        }
    }
}
