//! The filesystem-watcher side: `handle_event` translates a debounced watcher
//! event (create / move / modify / remove) into catalog changes, suppressing
//! the daemon's own writes. The `Move` arm covers the three disjoint cases
//! (intra-directory rename, moved out, moved in).

use std::path::{Path, PathBuf};

use walkdir::WalkDir;

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

impl SyncDirectories {
    pub(super) async fn handle_event(
        &self,
        event: DebouncedEventKind,
    ) -> Result<(), SyncDirectoryError> {
        match event {
            DebouncedEventKind::Create { file_name } => {
                // A Create for a path the daemon just wrote is our own
                // operation (most often a peer-received file placed into a
                // Universal directory under its `file_id`).
                if self.take_matching_self_write(&file_name, None) {
                    log::debug!(
                        "Ignoring Create for {} (our own operation)",
                        file_name.to_string_lossy()
                    );
                    return Ok(());
                }

                let sync_directory = self.sync_directory_for_path(&file_name)?;
                let sync_relative_path = relative_within(&file_name, &sync_directory.path)?;

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
                    let from_self = self.take_matching_self_write(from, None);
                    let to_self = self.take_matching_self_write(to, None);
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
                        if self.take_matching_self_write(&to, None) {
                            log::debug!(
                                "Ignoring move-in of {} (our own operation)",
                                to.to_string_lossy()
                            );
                            return Ok(());
                        }

                        let sync_relative_path = relative_within(&to, &sync_directory.path)?;

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
                        for entry in WalkDir::new(&to)
                            .into_iter()
                            .filter_map(|entry| entry.ok())
                            .filter(|entry| entry.file_type().is_file())
                        {
                            if self.take_matching_self_write(entry.path(), None) {
                                log::debug!(
                                    "Ignoring move-in of {} (our own operation)",
                                    entry.path().to_string_lossy()
                                );
                                continue;
                            }

                            let Ok(sync_relative_path) =
                                entry.path().strip_prefix(&sync_directory.path)
                            else {
                                // The walk is rooted at `to`, itself under the
                                // sync directory, so this is unreachable — but
                                // skip the one entry rather than crash the
                                // thread if it ever isn't.
                                log::warn!(
                                    "Skipping move-in of {}: not under {}",
                                    entry.path().to_string_lossy(),
                                    sync_directory.path.to_string_lossy()
                                );
                                continue;
                            };

                            let (content, content_hash, size) =
                                self.get_file_content(entry.path()).await?;

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
                if self.take_matching_self_write(&file_name, Some(&content_hash)) {
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
                if self.take_matching_self_write(&file_name, None) {
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
        }

        Ok(())
    }
}
