//! The filesystem-watcher side: `handle_event` translates a debounced watcher
//! event (create / move / modify / remove) into catalog changes, suppressing
//! the daemon's own writes. The `Move` arm covers the three disjoint cases
//! (intra-directory rename, moved out, moved in).

use std::path::{Path, PathBuf};

use walkdir::WalkDir;

use super::self_write::Observed;
use super::watch::DebouncedEventKind;
use super::{SyncDirectories, SyncDirectoryError};
use crate::configuration::SyncType;

/// Make `path` relative to `base`, mapping the "impossible" `strip_prefix`
/// failure to a [`SyncDirectoryError::PathOutsideSyncDirectory`] instead of
/// panicking. Every path the watcher reports is under a watched root, so this
/// only guards against the theoretical case — but on the sole sync-directory
/// thread a panic is fatal to *all* directories, so we skip the one file.
fn relative_within<'a>(path: &'a Path, base: &Path) -> Result<&'a Path, SyncDirectoryError> {
    path.strip_prefix(base)
        .map_err(|source| SyncDirectoryError::PathOutsideSyncDirectory {
            path: path.to_path_buf(),
            source,
        })
}

/// If `event` is a create / modify / remove of a file named `.gitignore`, the
/// directory that `.gitignore` governs (its parent). Any other event — or a
/// `.gitignore` move, whose two endpoints are handled as their own
/// create/remove halves — yields `None`.
fn gitignore_change_directory(event: &DebouncedEventKind) -> Option<PathBuf> {
    let path = match event {
        DebouncedEventKind::Create { file_name }
        | DebouncedEventKind::Modify { file_name }
        | DebouncedEventKind::Remove { file_name } => file_name,
        DebouncedEventKind::Move { .. }
        | DebouncedEventKind::DirCreate { .. }
        | DebouncedEventKind::DirRemove { .. } => return None,
    };

    if path.file_name().is_some_and(|name| name == ".gitignore") {
        path.parent().map(Path::to_path_buf)
    } else {
        None
    }
}

impl SyncDirectories {
    pub(super) async fn handle_event(
        &self,
        event: DebouncedEventKind,
    ) -> Result<(), SyncDirectoryError> {
        // A `.gitignore` create/modify/remove can newly *un*-ignore a subtree.
        // Because the non-recursive watcher installs no descriptors under an
        // ignored subtree, no events flow from inside it, so re-admission cannot
        // be discovered from file events there — it must be driven from the
        // `.gitignore` change itself: re-watch and ingest the now-visible files.
        // This runs *in addition to* the normal handling below (which syncs the
        // `.gitignore` file itself like any other file).
        //
        // The reverse direction (a `.gitignore` edit that newly *ignores* a
        // subtree) deliberately does nothing at runtime: watches are only ever
        // added on an edit, never removed, so an already-synced file under a
        // freshly-ignored directory keeps syncing — the standing invariant that
        // ignore gates ingestion only and never stops syncing an existing file.
        // A newly-ignored subtree is re-pruned on the next daemon restart.
        if let Some(gitignore_dir) = gitignore_change_directory(&event) {
            self.readmit_after_gitignore_change(&gitignore_dir).await?;
        }

        match event {
            DebouncedEventKind::Create { file_name } => {
                // A Create for a path the daemon just wrote is our own
                // operation (most often a peer-received file placed into a
                // Universal directory under its `file_id`).
                if self.take_matching_self_write(&file_name, Observed::Arrival) {
                    log::debug!(
                        "Ignoring Create for {} (our own operation)",
                        file_name.to_string_lossy()
                    );
                    return Ok(());
                }

                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let sync_relative_path = relative_within(&file_name, &sync_directory.path)?;

                if sync_directory.is_ignored(sync_relative_path) {
                    log::debug!(
                        "Ignoring Create of gitignored file {}",
                        sync_relative_path.to_string_lossy()
                    );
                    return Ok(());
                }

                let (content, content_hash, size) = self.get_file_content(&file_name).await?;

                match &sync_directory.sync_type {
                    SyncType::Universal { .. } => {
                        self.upload_file(
                            sync_directory,
                            sync_relative_path,
                            content,
                            content_hash,
                            size,
                            Vec::new(),
                        )?;
                    }
                    SyncType::TagBased { tags } => {
                        self.add_file(
                            sync_directory,
                            sync_relative_path,
                            content,
                            content_hash,
                            size,
                            tags.to_vec(),
                        )?;
                    }
                }
            }
            DebouncedEventKind::Move { from, to } => {
                let Some(any_path) = from.as_ref().or(to.as_ref()) else {
                    log::warn!("Received a Move event with neither from nor to; ignoring");
                    return Ok(());
                };
                let sync_directory = self.sync_directory_for_path(any_path)?;

                if let Some(from) = &from
                    && let Some(to) = &to
                {
                    // Move within the directory.

                    if let SyncType::Universal { .. } = sync_directory.sync_type {
                        // A Universal directory stores files under their `file_id`
                        // on disk; a rename *within* it has no logical meaning and
                        // must not propagate. This event is normally one we caused
                        // ourselves — materializing a received/uploaded file moves
                        // it into place under its `file_id` (a rename the watcher
                        // reports as a Move) — and should have been skipped. If a
                        // user manually renamed a UUID file it likewise carries no
                        // logical meaning. Either way: ignore it, never crash.
                        // (A *logical* rename arrives as a `FileMoved` change and
                        // is handled in `handle_command`.)
                        log::debug!(
                            "Ignoring intra-Universal move {} -> {} (no logical meaning)",
                            from.to_string_lossy(),
                            to.to_string_lossy()
                        );
                        return Ok(());
                    };

                    // A rename the daemon performed itself (`MoveFile`) records
                    // both endpoints as self-writes. The debouncer may deliver
                    // it as this combined `Move`; consume the records and ignore
                    // it so we do not re-announce our own move. (If it instead
                    // arrives split as a Remove + Create/Move-in, those arms
                    // consume the same records.)
                    let from_self = self.take_matching_self_write(from, Observed::Removal);
                    let to_self = self.take_matching_self_write(to, Observed::Arrival);
                    if from_self || to_self {
                        log::debug!(
                            "Ignoring intra-directory move {} -> {} (our own operation)",
                            from.to_string_lossy(),
                            to.to_string_lossy()
                        );
                        return Ok(());
                    }

                    let relative_from = relative_within(from, &sync_directory.path)?;
                    let Ok(relative_to) = to.strip_prefix(&sync_directory.path) else {
                        // TODO: Handle a move *out* to a different sync directory
                        // as a delete-here + add-there. For now, ignore rather
                        // than crash.
                        log::warn!(
                            "Ignoring move of {} out to another location (cross-directory moves \
                             not yet handled)",
                            from.to_string_lossy()
                        );
                        return Ok(());
                    };

                    if let Ok(file_id) = self.get_file_id(sync_directory, relative_from) {
                        self.move_file_within_directory(sync_directory, file_id, relative_to)?;
                    } else {
                        for sync_file in self.get_all_files_at(sync_directory, relative_from)? {
                            let path = PathBuf::from(sync_file.physical_path.as_str());
                            let Ok(relative_path) = path.strip_prefix(relative_from) else {
                                // Skip just this file rather than abandon the
                                // whole directory move; a stored physical path
                                // that is not under `relative_from` is a stale
                                // row, not a reason to crash.
                                log::warn!(
                                    "Skipping move of {}: not under {}",
                                    path.to_string_lossy(),
                                    relative_from.to_string_lossy()
                                );
                                continue;
                            };
                            let new_path = relative_to.join(relative_path);

                            self.move_file_within_directory(
                                sync_directory,
                                sync_file.file_id,
                                new_path,
                            )?;
                        }
                    }
                } else if let Some(from) = from {
                    // File was moved outside of the synced directory.

                    let relative_from = relative_within(&from, &sync_directory.path)?;

                    if let Ok(file_id) = self.get_file_id(sync_directory, relative_from) {
                        self.remove_file_by_id(sync_directory, file_id)?;
                    } else {
                        for sync_file in self.get_all_files_at(sync_directory, relative_from)? {
                            self.remove_file_by_id(sync_directory, sync_file.file_id)?;
                        }
                    }
                } else if let Some(to) = to {
                    // File was moved here from outside of the synced directory.
                    //
                    // This is also how the watcher reports our *own* placement:
                    // materializing a peer-received file renames it in from the
                    // daemon temp dir, arriving as `Move { from: None, to }`.

                    if to.is_file() {
                        if self.take_matching_self_write(&to, Observed::Arrival) {
                            log::debug!(
                                "Ignoring move-in of {} (our own operation)",
                                to.to_string_lossy()
                            );
                            return Ok(());
                        }

                        let sync_relative_path = relative_within(&to, &sync_directory.path)?;

                        if sync_directory.is_ignored(sync_relative_path) {
                            log::debug!(
                                "Ignoring move-in of gitignored file {}",
                                sync_relative_path.to_string_lossy()
                            );
                            return Ok(());
                        }

                        let (content, content_hash, size) = self.get_file_content(&to).await?;

                        match &sync_directory.sync_type {
                            SyncType::Universal { .. } => {
                                self.upload_file(
                                    sync_directory,
                                    sync_relative_path,
                                    content,
                                    content_hash,
                                    size,
                                    Vec::new(),
                                )?;
                            }
                            SyncType::TagBased { tags } => {
                                self.add_file(
                                    sync_directory,
                                    sync_relative_path,
                                    content,
                                    content_hash,
                                    size,
                                    tags.to_vec(),
                                )?;
                            }
                        }
                    } else if to.is_dir() {
                        // Moving a directory in also brings its subtree under the
                        // non-recursive watcher: register watches (pruning
                        // ignored subtrees) *and* ingest its existing files.
                        self.watch_and_ingest_directory(sync_directory, &to).await?;
                    } else {
                        log::warn!(
                            "A file that is not a regular file or a directory was detected. This \
                             is unsupported at the moment"
                        );
                    }
                } else {
                    log::error!("Received an empty move. This should never happen");
                }
            }
            DebouncedEventKind::Modify { file_name } => {
                let (content, content_hash, size) = self.get_file_content(&file_name).await?;

                // Suppress only if the on-disk content matches what the daemon
                // just wrote here.
                if self.take_matching_self_write(&file_name, Observed::Modification(&content_hash))
                {
                    log::debug!(
                        "Ignoring Modify of {} (our own operation)",
                        file_name.to_string_lossy()
                    );
                    return Ok(());
                }

                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let sync_relative_path = relative_within(&file_name, &sync_directory.path)?;
                let file_id = self.get_file_id(sync_directory, sync_relative_path)?;

                self.update_file_content(sync_directory, file_id, content, content_hash, size)?;
            }
            DebouncedEventKind::Remove { file_name } => {
                // A removal the daemon caused itself (delete, move-out, or the
                // source side of a rename) has no content to match on, so a
                // presence match consumes the record and ignores the event.
                if self.take_matching_self_write(&file_name, Observed::Removal) {
                    log::debug!(
                        "Ignoring Remove of {} (our own operation)",
                        file_name.to_string_lossy()
                    );
                    return Ok(());
                }

                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let sync_relative_path = relative_within(&file_name, &sync_directory.path)?;
                let file_id = self.get_file_id(sync_directory, sync_relative_path)?;

                self.remove_file_by_id(sync_directory, file_id)?;
            }
            DebouncedEventKind::DirCreate { path } => {
                // Under the non-recursive watcher a new directory gets no watch
                // for free — install one now (unless it is `.gitignore`d). And
                // because files can appear inside it between its creation and
                // this handler running (`mkdir -p a/b/c; touch a/b/c/f`, or a
                // directory populated then created), walk it to ingest anything
                // already there. Both are what `watch_and_ingest_directory`
                // does; together they close the create-then-populate race.
                let sync_directory = self.sync_directory_for_path(&path)?;

                let sync_relative_path = relative_within(&path, &sync_directory.path)?;
                if sync_directory.is_ignored_dir(sync_relative_path) {
                    log::debug!(
                        "Not watching gitignored directory {}",
                        sync_relative_path.to_string_lossy()
                    );
                    return Ok(());
                }

                self.watch_and_ingest_directory(sync_directory, &path)
                    .await?;
            }
            DebouncedEventKind::DirRemove { path } => {
                // Drop the subtree's descriptors from the watch set. Individual
                // file removals beneath it arrive as their own `Remove` events
                // (which sync the catalog deletions), so there is nothing to
                // ingest here — only watches to release.
                self.dispatcher.borrow_mut().unwatch_tree(&path);
            }
        }

        Ok(())
    }

    /// React to a `.gitignore` change under `gitignore_dir` by re-admitting any
    /// subtree it newly un-ignores: re-watch and ingest the now-visible,
    /// not-yet-tracked files. Idempotent — [`watch_and_ingest_directory`] skips
    /// directories already watched and files already tracked, so an edit that
    /// changes nothing (or one that only *added* ignore rules) does no work
    /// beyond the walk.
    ///
    /// `gitignore_dir` may not belong to any sync directory (a stray path);
    /// that is not an error, just nothing to do.
    async fn readmit_after_gitignore_change(
        &self,
        gitignore_dir: &Path,
    ) -> Result<(), SyncDirectoryError> {
        let Ok(sync_directory) = self.sync_directory_for_path(gitignore_dir) else {
            return Ok(());
        };

        log::debug!(
            "Re-evaluating watches under {} after a .gitignore change",
            gitignore_dir.to_string_lossy()
        );

        self.watch_and_ingest_directory(sync_directory, gitignore_dir)
            .await
    }

    /// Register `directory`'s subtree with the non-recursive watcher (pruning
    /// `.gitignore`d subtrees) and ingest every not-yet-tracked file beneath
    /// it. Used both when a directory is moved in and when one is created,
    /// and by the `.gitignore` re-admission path. The walk prunes ignored
    /// subdirectories so a moved-in/created tree containing its own
    /// `target/` neither watches nor ingests it.
    async fn watch_and_ingest_directory(
        &self,
        sync_directory: &super::OpenDirectory,
        directory: &Path,
    ) -> Result<(), SyncDirectoryError> {
        // Install watches first so files created *after* the walk still produce
        // events; the walk then covers everything already present. Any file
        // landing in the gap between the two is caught by whichever side sees it.
        self.dispatcher
            .borrow_mut()
            .watch_tree(directory, &|candidate: &Path| match candidate
                .strip_prefix(&sync_directory.path)
            {
                Ok(relative) if relative.as_os_str().is_empty() => false,
                Ok(relative) => sync_directory.is_ignored_dir(relative),
                Err(_) => false,
            });

        for entry in WalkDir::new(directory)
            .into_iter()
            .filter_entry(|entry| {
                if !entry.file_type().is_dir() {
                    return true;
                }
                match entry.path().strip_prefix(&sync_directory.path) {
                    Ok(relative) if relative.as_os_str().is_empty() => true,
                    Ok(relative) => !sync_directory.is_ignored_dir(relative),
                    Err(_) => true,
                }
            })
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
        {
            if self.take_matching_self_write(entry.path(), Observed::Arrival) {
                log::debug!(
                    "Ignoring move-in of {} (our own operation)",
                    entry.path().to_string_lossy()
                );
                continue;
            }

            let Ok(sync_relative_path) = entry.path().strip_prefix(&sync_directory.path) else {
                // The walk is rooted under the sync directory, so this is
                // unreachable — but skip the one entry rather than crash the
                // thread if it ever isn't.
                log::warn!(
                    "Skipping ingest of {}: not under {}",
                    entry.path().to_string_lossy(),
                    sync_directory.path.to_string_lossy()
                );
                continue;
            };

            if sync_directory.is_ignored(sync_relative_path) {
                log::debug!(
                    "Ignoring gitignored file {}",
                    sync_relative_path.to_string_lossy()
                );
                continue;
            }

            // Keep re-admission idempotent: a `.gitignore` edit that un-ignores
            // a subtree re-walks it, and some of its files may already be
            // tracked (e.g. admitted before the ignore, or by a concurrent
            // path). Skip those rather than re-ingest. A DB error skips just this
            // file.
            match sync_directory.is_file_tracked(sync_relative_path) {
                Ok(true) => {
                    log::debug!(
                        "File {} is already tracked",
                        sync_relative_path.to_string_lossy()
                    );
                    continue;
                }
                Ok(false) => {}
                Err(error) => {
                    log::error!(
                        "DB error checking {}: {error:?}; skipping",
                        sync_relative_path.to_string_lossy()
                    );
                    continue;
                }
            }

            let (content, content_hash, size) = self.get_file_content(entry.path()).await?;

            match &sync_directory.sync_type {
                SyncType::Universal { .. } => {
                    self.upload_file(
                        sync_directory,
                        sync_relative_path,
                        content,
                        content_hash,
                        size,
                        Vec::new(),
                    )?;
                }
                SyncType::TagBased { tags } => {
                    self.add_file(
                        sync_directory,
                        sync_relative_path,
                        content,
                        content_hash,
                        size,
                        tags.to_vec(),
                    )?;
                }
            }
        }

        Ok(())
    }
}
