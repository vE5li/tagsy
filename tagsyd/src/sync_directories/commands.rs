//! The `SyncDirectoryCommand` inbox and its handler: one arm per command
//! (create / change / move / remove / apply-placement / the four read-only
//! queries), each independent.

use std::collections::HashSet;
use std::path::PathBuf;

use tagsy_core::{FileId, LogicalPath, PhysicalPath, TagId};

use super::{SyncDirectories, SyncDirectoryError};
use crate::configuration::{SyncDirectory, SyncType};
use crate::file_bytes::FileBytes;
use crate::store::DatabaseError;

pub enum SyncDirectoryCommand {
    CreateFile {
        file_id: FileId,
        /// Where to place the bytes on disk within the target sync directory.
        /// Already resolved from the file's logical path by the caller via
        /// `SyncType::physical_for`, so the handler stores it verbatim.
        physical_path: PhysicalPath,
        content: FileBytes,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    ChangeFile {
        file_id: FileId,
        content: FileBytes,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    MoveFile {
        file_id: FileId,
        /// The new on-disk location within the target sync directory, already
        /// resolved from the file's new logical path via
        /// `SyncType::physical_for`.
        physical_path: PhysicalPath,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    RemoveFile {
        file_id: FileId,
        // Maybe a bit weird to have it like this? Not sure.
        // We currently need that to check which directory this event was meant for.
        sync_directory_path: PathBuf,
    },
    /// Re-evaluate which TagBased sync directories should hold `file_id` given
    /// its *current* tag set, and reconcile placement accordingly:
    ///
    /// - a TagBased directory that now matches (`contains_all_tags`) but does
    ///   not yet hold the file gains it (the bytes are sourced from another
    ///   directory that already holds the file);
    /// - a TagBased directory that no longer matches but currently holds it
    ///   drops it.
    ///
    /// Universal directories are untouched (they have no tag filter). This is
    /// the recovery path for the tag-vs-content reconciliation race: when a
    /// peer transfer materializes a file before its `FileTagged` relationships
    /// have been applied, the file is placed only where tags already matched
    /// (e.g. Universal dirs). Applying the tags later re-runs placement so the
    /// file lands in the TagBased directories it belongs to. Idempotent: a
    /// no-op when placement is already correct.
    ///
    /// If a TagBased directory now matches the file but no local copy exists to
    /// source the bytes from, placement cannot complete locally. In that case
    /// `respond_to` receives `true` ("deferred: needs bytes") and the caller is
    /// expected to fetch the bytes over the network (by the file's latest
    /// catalog version hash) and re-drive placement via `Materialize`.
    /// Otherwise (placement resolved, or nothing to place) it receives
    /// `false`.
    ApplyPlacement {
        file_id: FileId,
        /// The file's logical path, used to derive each TagBased directory's
        /// physical path via `SyncType::physical_for` when creating.
        logical_path: LogicalPath,
        /// The file's current tag set (from the main `CatalogStore`), against
        /// which each TagBased directory's tags are matched.
        file_tags: Vec<TagId>,
        /// `true` if a TagBased directory should hold the file but no local
        /// copy exists to source the bytes (the caller must fetch);
        /// `false` otherwise.
        respond_to: tokio::sync::oneshot::Sender<bool>,
    },
    /// Read the bytes for `file_id` from whichever sync directory currently
    /// holds it and respond on `respond_to`. Used by peer connection tasks to
    /// answer inbound `ChunkRequest`s (serving verified content by hash).
    ///
    /// Resolve `file_id` to the **absolute** on-disk path where its bytes
    /// currently live in the first sync directory that holds it, without
    /// reading the content. Responds with `None` if no sync directory has
    /// the file. Used by `tagsy edit` to open a locally-present file in
    /// place (so the filesystem watcher picks up the save and propagates
    /// it).
    LocalPath {
        file_id: FileId,
        respond_to: tokio::sync::oneshot::Sender<Option<PathBuf>>,
    },
    ReadFile {
        file_id: FileId,
        respond_to: tokio::sync::oneshot::Sender<Option<(PhysicalPath, FileBytes, String)>>,
    },
    /// Collect the set of file ids that are materialized on this device: every
    /// `file_id` that has a row in *some* sync directory's index. A file can
    /// live in several TagBased directories, so the set is deduplicated across
    /// them. The `ApiService` prices this set against the catalog's sizes to
    /// produce the "stored locally" side of the storage-stats indicator.
    LocalFileIds {
        respond_to: tokio::sync::oneshot::Sender<HashSet<FileId>>,
    },
    /// Given the catalog's live files (each with its logical path and current
    /// tag set), report which of them *should* be held locally but whose bytes
    /// are absent on disk. A file is missing when no sync directory that should
    /// hold it — Universal directories always, TagBased directories whose tags
    /// the file satisfies (`contains_all_tags`) — actually has the bytes on
    /// disk (`first_holding_path`).
    ///
    /// This is the connect-time recovery enumeration: because the receive path
    /// has no retries, a failed pull leaves a file cataloged at its correct
    /// version with no local bytes. The caller fetches each returned file once
    /// per peer connect (by its latest catalog version hash). Computed on
    /// demand — no "missing" state is stored.
    MissingContent {
        catalog_files: Vec<(FileId, LogicalPath, Vec<TagId>)>,
        respond_to: tokio::sync::oneshot::Sender<Vec<FileId>>,
    },
    /// Snapshot the sync directories this device is *currently* serving —
    /// each as a [`SyncDirectory`] carrying its absolute `path` and
    /// `sync_type`. This reflects live actor state (directories whose setup
    /// failed at startup are already dropped), not the possibly-stale startup
    /// configuration. Used by the backup builder to derive per-directory DB
    /// paths and to record each directory in the archive manifest.
    ListDirectories {
        respond_to: tokio::sync::oneshot::Sender<Vec<SyncDirectory>>,
    },
}

impl SyncDirectories {
    pub(super) async fn handle_command(
        &mut self,
        command: SyncDirectoryCommand,
    ) -> Result<(), SyncDirectoryError> {
        match command {
            SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;

                // Idempotency: several placement triggers can legitimately fire
                // for the same file (each `FileTagged` relationship reconcile,
                // plus the connect-time placement sweep), each potentially
                // fetching the bytes and emitting a `Materialize` -> `CreateFile`.
                // If this directory already tracks the file, the bytes are
                // already correctly placed; treat the repeat as a no-op success
                // rather than letting the duplicate insert hit the per-directory
                // DB primary key (which surfaced as `FailedAddingFile` and
                // dropped the file).
                //
                // We key idempotency on the *file_id* being present, not on the
                // physical path matching: the incoming `physical_path` is the
                // un-suffixed base derived by the caller from the logical path,
                // whereas the stored path may carry a collision suffix (` (N)`).
                // A file_id has exactly one row (and one on-disk copy) per
                // directory, so its mere presence means placement is done.
                if let Ok(existing) = sync_directory.database.get_file(file_id) {
                    log::debug!(
                        "CreateFile: {} already present in {} at {}; skipping (idempotent)",
                        file_id.to_string(),
                        sync_directory.path.to_string_lossy(),
                        existing.physical_path.as_str()
                    );
                    return Ok(());
                }

                // The caller resolved `physical_path` from the file's logical
                // path via `SyncType::physical_for` (the file_id for Universal,
                // the logical path for TagBased). A TagBased directory may
                // already hold a *different* file at that logical path, so
                // resolve any on-disk naming collision before writing; the
                // resolved name is used for both the write and the DB row.
                let physical_path =
                    self.resolve_unique_physical(sync_directory, &physical_path, file_id);
                let file_path = sync_directory.path.join(physical_path.as_str());

                log::info!("Adding file at {}", file_path.to_string_lossy());

                if let Some(parent) = file_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|error| {
                        log::error!(
                            "CreateFile: failed to create parent directory {} for {}: {error}",
                            parent.display(),
                            file_id.to_string()
                        );
                        SyncDirectoryError::FailedAddingFile(error.into())
                    })?;
                }

                // Hash the bytes before materializing (which consumes `content`)
                // so we can recognize the watcher event this write produces as
                // self-caused. The watcher may surface it as a `Create` or, when
                // the materialize is a rename into place, as a `Move`-in — the
                // path-keyed record matches either.
                let content_hash = content.hash().await.map_err(|error| {
                    log::error!(
                        "CreateFile: failed to HASH content for {} at {} (source {:?}): {error}",
                        file_id.to_string(),
                        file_path.display(),
                        content.path(),
                    );
                    SyncDirectoryError::FailedAddingFile(error.into())
                })?;

                content.materialize_to(&file_path).await.map_err(|error| {
                    log::error!(
                        "CreateFile: failed to MATERIALIZE {} at {}: {error}",
                        file_id.to_string(),
                        file_path.display()
                    );
                    SyncDirectoryError::FailedAddingFile(error.into())
                })?;

                sync_directory
                    .database
                    .add_file(file_id, &physical_path)
                    .map_err(|error| {
                        log::error!(
                            "CreateFile: failed to INSERT DB row for {} at {}: {:?}",
                            file_id.to_string(),
                            physical_path.as_str(),
                            error
                        );
                        SyncDirectoryError::FailedAddingFile(error.into())
                    })?;

                self.record_self_write(file_path, Some(content_hash));
            }
            SyncDirectoryCommand::ChangeFile {
                file_id,
                content,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;

                let physical_path = match &sync_directory.sync_type {
                    SyncType::Universal { .. } => PhysicalPath::new(file_id.to_string()),
                    SyncType::TagBased { .. } => {
                        sync_directory
                            .database
                            .get_file(file_id)
                            .map_err(|error| SyncDirectoryError::FailedChangingFile(error.into()))?
                            .physical_path
                    }
                };
                let file_path = sync_directory.path.join(physical_path.as_str());

                log::info!("Modifying file at {}", file_path.to_string_lossy());

                // Hash before materializing so the resulting `Modify` event can
                // be matched by content: a user edit landing on the same path
                // would hash differently and must *not* be suppressed.
                let content_hash = content
                    .hash()
                    .await
                    .map_err(|error| SyncDirectoryError::FailedChangingFile(error.into()))?;

                content.materialize_to(&file_path).await.map_err(|error| {
                    log::error!(
                        "Failed to materialize changed file at {}: {error}",
                        file_path.display()
                    );
                    SyncDirectoryError::FailedChangingFile(error.into())
                })?;

                self.record_self_write(file_path, Some(content_hash));
            }
            SyncDirectoryCommand::MoveFile {
                file_id,
                physical_path,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;

                match &sync_directory.sync_type {
                    SyncType::Universal { .. } => {
                        // Universal directories store files under their `file_id`
                        // on disk, so a logical rename never moves any bytes: the
                        // resolved `physical_path` is still the `file_id` and this
                        // is a no-op DB write kept for symmetry.
                        sync_directory
                            .database
                            .update_file_physical_path(file_id, &physical_path)
                            .map_err(|error| SyncDirectoryError::FailedMovingFile(error.into()))?;
                    }
                    SyncType::TagBased { .. } => {
                        let file = sync_directory
                            .database
                            .get_file(file_id)
                            .map_err(|error| SyncDirectoryError::FailedMovingFile(error.into()))?;

                        // The new logical path may collide with a *different*
                        // file already held here; resolve a suffix (self-excluded
                        // so keeping the same name is a no-op). The resolved name
                        // drives both the rename and the DB update.
                        let physical_path =
                            self.resolve_unique_physical(sync_directory, &physical_path, file_id);

                        // No-op move: `FileMoved` replay (at startup, or from a
                        // peer reconnect) can fire `MoveFile` for a file that is
                        // already correctly placed. Skipping here avoids a
                        // needless DB write, a `rename(P, P)` syscall, and
                        // stray `record_self_write` entries that would never
                        // be consumed (there is no watcher event for a no-op
                        // rename).
                        if file.physical_path == physical_path {
                            log::debug!(
                                "MoveFile: {} already at {}; skipping (no-op)",
                                file_id.to_string(),
                                physical_path.as_str()
                            );
                            return Ok(());
                        }

                        let old_file_path = sync_directory.path.join(file.physical_path.as_str());
                        let new_file_path = sync_directory.path.join(physical_path.as_str());

                        log::info!(
                            "Moving file from {} to {}",
                            old_file_path.to_string_lossy(),
                            new_file_path.to_string_lossy()
                        );

                        sync_directory
                            .database
                            .update_file_physical_path(file_id, &physical_path)
                            .map_err(|error| SyncDirectoryError::FailedMovingFile(error.into()))?;

                        // NOTE: the DB row was already updated above; a failure
                        // here leaves the index pointing at the new path while
                        // the bytes are still at the old one. That inconsistency
                        // predates this change — the fix worth making is
                        // reordering (rename first, then DB), which is a
                        // behaviour change out of scope here. For now, at least
                        // do not crash the sole sync-directory thread.
                        if let Some(parent) = new_file_path.parent() {
                            std::fs::create_dir_all(parent).map_err(|error| {
                                log::error!(
                                    "MoveFile: failed to create destination directory {}: {error}",
                                    parent.display()
                                );
                                SyncDirectoryError::FailedMovingFile(error.into())
                            })?;
                        }
                        std::fs::rename(&old_file_path, &new_file_path).map_err(|error| {
                            log::error!(
                                "MoveFile: failed to rename {} -> {}: {error}",
                                old_file_path.display(),
                                new_file_path.display()
                            );
                            SyncDirectoryError::FailedMovingFile(error.into())
                        })?;

                        // If the moved file was in a directory that is now empty, we want to remove
                        // the directory as well.
                        if let Some(directory) = PathBuf::from(file.physical_path.as_str()).parent()
                            && !directory.as_os_str().is_empty()
                            && let Some(old_parent) = old_file_path.parent()
                        {
                            self.try_remove_empty_directory(old_parent);
                        }

                        // The rename is self-caused. Depending on how the OS and
                        // debouncer report it we may see a combined `Move`, or a
                        // `Remove` at the old path plus a `Create`/`Move`-in at
                        // the new one. Record both endpoints (no content hash: a
                        // rename does not change bytes) so any of those shapes is
                        // recognized and ignored.
                        self.record_self_write(old_file_path, None);
                        self.record_self_write(new_file_path, None);
                    }
                };
            }
            SyncDirectoryCommand::RemoveFile {
                file_id,
                sync_directory_path,
            } => {
                let sync_directory = self.sync_directory_for_path(&sync_directory_path)?;
                // Tolerate a missing per-directory row: the file was either
                // never placed in this directory (e.g. a TagBased directory
                // whose tag filter never matched) or a previous `RemoveFile`
                // for the same `file_id` already cleaned it up. Either way
                // there is nothing to do here; treat it as a successful no-op
                // rather than an error to avoid spurious `FailedRemovingFile`
                // log noise on idempotent redeliveries.
                let file = match sync_directory.database.get_file(file_id) {
                    Ok(file) => file,
                    Err(DatabaseError::MissingFile) => {
                        log::debug!(
                            "RemoveFile: {} not tracked in {}; nothing to do",
                            file_id.to_string(),
                            sync_directory.path.to_string_lossy()
                        );
                        return Ok(());
                    }
                    Err(error) => {
                        return Err(SyncDirectoryError::FailedRemovingFile(error.into()));
                    }
                };

                // Recovery vault: a Universal directory with `keep_deleted_files`
                // retains its physical copy (and its per-directory DB row) on
                // delete, so an accidental deletion can be undone here. The file
                // is still gone from the catalog and every other directory; this
                // one just doesn't drop its bytes.
                if let SyncType::Universal {
                    keep_deleted_files: true,
                } = &sync_directory.sync_type
                {
                    log::info!(
                        "Keeping deleted file {} in {} (keep_deleted_files)",
                        file_id.to_string(),
                        sync_directory.path.to_string_lossy()
                    );
                    return Ok(());
                }

                log::info!(
                    "Removing file {} from {}",
                    file.physical_path,
                    sync_directory.path.to_string_lossy()
                );

                let file_path = match &sync_directory.sync_type {
                    SyncType::Universal { .. } => sync_directory.path.join(file_id.to_string()),
                    SyncType::TagBased { .. } => {
                        sync_directory.path.join(file.physical_path.as_str())
                    }
                };

                // Tolerate the file already being gone: a same-content duplicate
                // can leave two file_ids pointing at the same physical path, so a
                // second `RemoveFile` for that path finds nothing on disk. That is
                // not an error — we still want to clean up this file_id's DB row
                // below. Any other IO error is logged but must not crash the sole
                // sync-directory thread.
                match std::fs::remove_file(&file_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        log::debug!(
                            "RemoveFile: {} already gone; cleaning up DB row only",
                            file_path.to_string_lossy()
                        );
                    }
                    Err(error) => {
                        log::error!(
                            "RemoveFile: failed to remove {}: {error}",
                            file_path.to_string_lossy()
                        );
                        return Err(SyncDirectoryError::FailedRemovingFile(error.into()));
                    }
                }

                // If the removed file was in a directory that is now empty, we want to remove
                // the directory as well.
                if let SyncType::TagBased { .. } = &sync_directory.sync_type
                    && let Some(directory) = PathBuf::from(file.physical_path.as_str()).parent()
                    && !directory.as_os_str().is_empty()
                {
                    let directory_path = sync_directory.path.join(directory);
                    self.try_remove_empty_directory(directory_path);
                }

                sync_directory
                    .database
                    .remove_file_by_id(file_id)
                    .map_err(|error| SyncDirectoryError::FailedRemovingFile(error.into()))?;

                self.record_self_write(file_path, None);
            }
            SyncDirectoryCommand::ApplyPlacement {
                file_id,
                logical_path,
                file_tags,
                respond_to,
            } => {
                let deferred = self
                    .apply_placement(file_id, &logical_path, &file_tags)
                    .await?;
                // Best-effort: the caller may have dropped the receiver.
                let _ = respond_to.send(deferred);
            }
            SyncDirectoryCommand::LocalPath {
                file_id,
                respond_to,
            } => {
                // Resolve the absolute on-disk path of the first sync directory
                // that has `file_id`, without reading the bytes. We do not
                // verify the file exists on disk here; the DB row is treated as
                // authoritative (mirrors how `ReadFile` trusts it before the
                // read). The caller opens the path directly.
                let mut response: Option<PathBuf> = None;
                for sync_directory in &self.sync_directories {
                    let file = match sync_directory.database.get_file(file_id) {
                        Ok(file) => file,
                        Err(_) => continue,
                    };
                    let absolute_path = sync_directory.path.join(file.physical_path.as_str());
                    response = Some(absolute_path);
                    break;
                }
                let _ = respond_to.send(response);
            }
            SyncDirectoryCommand::LocalFileIds { respond_to } => {
                // Union the file ids across every sync directory's index. A DB
                // row means "this directory holds this file's bytes on disk"
                // (mirrors how `LocalPath` trusts the row rather than statting).
                // A file present in multiple TagBased directories collapses to
                // one entry via the set.
                let mut ids: HashSet<FileId> = HashSet::new();
                for sync_directory in &self.sync_directories {
                    match sync_directory.database.get_all_files() {
                        Ok(files) => ids.extend(files.into_iter().map(|file| file.file_id)),
                        Err(_) => continue,
                    }
                }
                let _ = respond_to.send(ids);
            }
            SyncDirectoryCommand::MissingContent {
                catalog_files,
                respond_to,
            } => {
                // For each live catalog file, decide whether *some* sync
                // directory that should hold it actually has the bytes on disk.
                // Universal directories should hold every file; TagBased ones
                // only files whose tags they match. `first_holding_path` does
                // the authoritative on-disk (`.exists()`) check across all
                // directories, so a file whose index row survived but whose
                // bytes are gone still counts as missing.
                let mut missing: Vec<FileId> = Vec::new();
                for (file_id, _logical_path, file_tags) in catalog_files {
                    let should_be_local = self.sync_directories.iter().any(|sync_directory| {
                        match &sync_directory.sync_type {
                            SyncType::Universal { .. } => true,
                            SyncType::TagBased {
                                tags: sync_directory_tags,
                            } => crate::catalog::placement::contains_all_tags(
                                sync_directory_tags,
                                &file_tags,
                            ),
                        }
                    });
                    if !should_be_local {
                        continue;
                    }
                    if self.first_holding_path(file_id).is_none() {
                        missing.push(file_id);
                    }
                }
                let _ = respond_to.send(missing);
            }
            SyncDirectoryCommand::ListDirectories { respond_to } => {
                // Reconstruct a `SyncDirectory` per open directory. `OpenDirectory`
                // holds the same `path` + `sync_type` the config supplied; the
                // live `database` handle is not part of the snapshot. This is
                // the runtime-accurate set: any directory that failed setup at
                // startup was already filtered out of `self.sync_directories`.
                let directories = self
                    .sync_directories
                    .iter()
                    .map(|sync_directory| SyncDirectory {
                        path: sync_directory.path.clone(),
                        sync_type: sync_directory.sync_type.clone(),
                    })
                    .collect();
                let _ = respond_to.send(directories);
            }
            SyncDirectoryCommand::ReadFile {
                file_id,
                respond_to,
            } => {
                // Walk our sync directories looking for the first one that
                // claims to have `file_id` in its database. For TagBased
                // directories the on-disk path is the recorded relative path;
                // for Universal directories the file is stored under its
                // `file_id`. If a database row points at a missing file on
                // disk we log and continue to the next directory.
                let mut response: Option<(PhysicalPath, FileBytes, String)> = None;
                for sync_directory in &self.sync_directories {
                    let file = match sync_directory.database.get_file(file_id) {
                        Ok(file) => file,
                        Err(_) => continue,
                    };
                    // `physical_path`/`absolute_path` describe where the bytes
                    // live in *this* sync directory. For Universal directories
                    // the file is stored under its `file_id`; for TagBased it is
                    // stored under its recorded relative path. This is the
                    // *physical* path only: the caller substitutes the logical
                    // (human-readable) name from the main database before sending
                    // to a peer, since the per-directory DB may only hold the
                    // physical name (the `file_id` for Universal).
                    //
                    // We return the bytes as a `FileToCopy` referencing the
                    // on-disk file rather than reading them here: the caller
                    // buffers them into a wire `Change` only when actually
                    // answering a peer, so an unfulfilled request never reads
                    // the file.
                    let physical_path = file.physical_path.clone();
                    let absolute_path = sync_directory.path.join(physical_path.as_str());
                    // `get_file_content` hashes the entire file (O(size)); time it
                    // so a slow preview/fetch on a large file is attributable to
                    // this verify step rather than the preview generation itself.
                    let read_start = std::time::Instant::now();
                    let (content, content_hash, size) =
                        match self.get_file_content(&absolute_path).await {
                            Ok(triple) => triple,
                            Err(error) => {
                                log::warn!(
                                    "ReadFile: {} reported {} but read failed: {:?}",
                                    sync_directory.path.to_string_lossy(),
                                    absolute_path.to_string_lossy(),
                                    error
                                );
                                continue;
                            }
                        };
                    log::debug!(
                        "ReadFile: hashed {} ({} bytes) in {:?}",
                        absolute_path.to_string_lossy(),
                        size,
                        read_start.elapsed()
                    );
                    response = Some((physical_path, content, content_hash));
                    break;
                }
                let _ = respond_to.send(response);
            }
        }

        Ok(())
    }
}
