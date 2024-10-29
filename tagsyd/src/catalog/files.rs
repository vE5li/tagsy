//! The file-lifecycle arms of the catalog's metadata dispatch:
//! `FileMetadataAdded`/`FileMetadataChanged` (peer announcements),
//! `FileMoved`, `FileDeleted`, `FileRestored`, plus the three command arms that
//! catalog bytes/versions (`CatalogFile`, `Materialize`, `AnnounceProvided`).
//!
//! Each returns `Some(publish)` when it handled the change (`publish` = whether
//! the shared UI event should fire), or `None` for a non-file change (so the
//! caller falls through to [`super::tagging`]).

use std::sync::Arc;

use tagsy_core::TagId;
use tagsy_core::state::{Change, ChangeOrigin};
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;

use crate::catalog::messages::{self, CatalogCommand};
use crate::catalog::placement::{self, Placement};
use crate::catalog::previews::maybe_eager_preview;
use crate::configuration::{Configuration, RuntimeConfiguration, SyncType};
use crate::peer::relay::ChunkRelay;
use crate::store::{self, CatalogStore};
use crate::sync_directories::SyncDirectoryCommand;
use crate::{clock, operations};

/// Apply a file-lifecycle metadata change. Returns `Some(publish)` if `change`
/// was a file variant, else `None`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_change(
    configuration: &Configuration,
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    database: &mut CatalogStore,
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    _change_sender: &UnboundedSender<CatalogCommand>,
    _pending_fetches: &ChunkRelay,
    _operations: &operations::Operations,
    change: &Change,
    change_origin: &ChangeOrigin,
) -> Option<bool> {
    match change {
        // A metadata-only `FileMetadataAdded` announcement — always from a
        // peer (local ingestion carries bytes and arrives as
        // `Ingest::Content`). Record the file + version into the catalog and
        // forward onward; the bytes are pulled separately (and may never be
        // pulled at all if no local sync directory wants them).
        Change::FileMetadataAdded {
            file_id,
            logical_path,
            logical_path_modified_at,
            content_hash,
            size,
            tags,
        } => {
            // Metadata-only announcement from a peer. `file_versions` is the
            // byte-independent *catalog* of versions we know exist in the
            // network — NOT a record of bytes we hold (that is the
            // per-sync-directory databases). So we record the version here,
            // on announcement, regardless of whether we ever pull the bytes.
            let already_exists = database.file_exists(*file_id).unwrap_or_else(|error| {
                log::error!(
                    "file_exists check failed for {}: {:?}; assuming new",
                    file_id.to_string(),
                    error
                );
                false
            });

            if !already_exists {
                // Seed the path clock from the *originating* device's stamp
                // carried on the announcement (not our receive time), so a
                // later `FileMoved` orders against the true creation time.
                if let Err(error) =
                    database.add_file(*file_id, logical_path, *logical_path_modified_at)
                {
                    log::error!(
                        "Failed to add file {} ({}): {:?}; skipping change",
                        file_id.to_string(),
                        logical_path,
                        error
                    );
                    return Some(false);
                }
                // Persist the tags carried on the announcement into our
                // catalog. Downstream this same list also drives placement
                // (`MaterializePlacement::Create`), but placement only
                // *filters* sync directories — it never writes the
                // relationships. Without this write a peer would know the
                // file but show it untagged, since the upload path carries
                // tags on the creation change rather than as separate
                // `FileTagged` messages. Stamp with the file's creation
                // clock so LWW orders identically on every device.
                for tag_id in tags {
                    if let Err(error) =
                        database.tag_file(*tag_id, *file_id, *logical_path_modified_at)
                    {
                        log::error!(
                            "FileMetadataAdded: failed to tag file {} with {}: {:?}",
                            file_id.to_string(),
                            tag_id.to_string(),
                            error
                        );
                    }
                }
            } else {
                // Skip only if this is the version we already hold as latest
                // in the catalog, not merely present somewhere in history: a
                // revert to an older hash is a genuine new version and must
                // be appended (and its bytes re-pulled where wanted).
                let current_hash = database
                    .latest_version(*file_id)
                    .ok()
                    .flatten()
                    .map(|version| version.content_hash);
                if current_hash.as_deref() == Some(content_hash.as_str()) {
                    log::debug!(
                        "Ignoring no-op FileMetadataAdded for {} (already the current version)",
                        file_id.to_string()
                    );
                    // Still forward so the announcement propagates the tree.
                    super::forward::forward_to_peers(
                        configuration,
                        runtime_configuration,
                        change,
                        change_origin,
                    )
                    .await;
                    return Some(false);
                }
            }

            // Record the version into the catalog now, on announcement.
            if let Err(error) = database.record_version(
                *file_id,
                content_hash,
                super::forward::version_origin(change_origin),
                *size as i64,
            ) {
                log::error!(
                    "FileMetadataAdded: failed to record version for {}: {:?}",
                    file_id.to_string(),
                    error
                );
            }
            // A newer version supersedes any local tombstone (restore after
            // delete). No-op if not tombstoned.
            if let Err(error) = database.restore_file(*file_id) {
                log::error!(
                    "FileMetadataAdded: failed to clear tombstone for {}: {:?}",
                    file_id.to_string(),
                    error
                );
            }

            // Forward the announcement to our other peers immediately so the
            // catalog propagates across the whole tree, independent of
            // whether we pull the bytes. A downstream peer that then sends a
            // `ChunkRequest` against us before (or without) us holding the
            // bytes gets a `ChunkMiss` (we relay it onward), so it fetches
            // from another holder — this is the fix for the central-relay
            // race the design targets.
            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;

            // Trigger a byte pull from the announcing peer to place the file
            // into any matching local sync directory. If none matches, the
            // pull still runs today but the bytes are dropped at placement;
            // that is optimized separately.
            crate::peer::fetch::request_pull_from_origin(
                runtime_configuration,
                change_origin,
                *file_id,
                content_hash.clone(),
                *size,
                messages::MaterializePlacement::Create {
                    logical_path: logical_path.clone(),
                    tags: tags.clone(),
                },
            )
            .await;
            Some(true)
        }
        // A metadata-only `FileMetadataChanged` announcement — always from a
        // peer. Record the new version into the catalog and forward it; pull
        // the bytes where a local sync directory wants them.
        Change::FileMetadataChanged {
            file_id,
            content_hash,
            size,
        } => {
            // Skip only if this hash is already our latest catalog version.
            // It is NOT enough for the hash to appear somewhere in history: a
            // revert back to an older hash (present in history but not the
            // latest) is a genuine new version we must append (and re-pull
            // the bytes for where wanted). A whole-history check here
            // previously kept the wrong bytes on disk and hung `edit`.
            let current_hash = database
                .latest_version(*file_id)
                .ok()
                .flatten()
                .map(|version| version.content_hash);
            if current_hash.as_deref() == Some(content_hash.as_str()) {
                log::debug!(
                    "Ignoring no-op FileMetadataChanged for {} (already the current version)",
                    file_id.to_string()
                );
                // Already our latest catalog version. Announce onward so the
                // change still propagates the tree.
                super::forward::forward_to_peers(
                    configuration,
                    runtime_configuration,
                    change,
                    change_origin,
                )
                .await;
            } else {
                // Record the new version into the catalog now, on
                // announcement (independent of whether we pull the bytes).
                if let Err(error) = database.record_version(
                    *file_id,
                    content_hash,
                    super::forward::version_origin(change_origin),
                    *size as i64,
                ) {
                    log::error!(
                        "FileMetadataChanged: failed to record version for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }
                // A newer version supersedes any local tombstone (restore
                // after delete). No-op if not tombstoned.
                if let Err(error) = database.restore_file(*file_id) {
                    log::error!(
                        "FileMetadataChanged: failed to clear tombstone for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }

                // Forward immediately so the catalog propagates tree-wide
                // regardless of whether we pull the bytes.
                super::forward::forward_to_peers(
                    configuration,
                    runtime_configuration,
                    change,
                    change_origin,
                )
                .await;

                // Pull the new bytes to update any local sync directory that
                // holds this file.
                crate::peer::fetch::request_pull_from_origin(
                    runtime_configuration,
                    change_origin,
                    *file_id,
                    content_hash.clone(),
                    *size,
                    messages::MaterializePlacement::Change,
                )
                .await;
            }
            Some(true)
        }
        Change::FileMoved {
            file_id,
            logical_path,
            modified_at,
        } => {
            // TODO: Don't unwrap.
            // TODO: Should this be include? Currently this WILL NOT WORK since add file
            // doesn't consider subtags. We would need to get a list of *all* tags (incuding
            // subdags) when adding the file to make it work.
            // -> Maybe make it configurable in the config, per-sync directory.
            let file_tags = match database.tag_ids_for_file(*file_id, store::SubtagRule::Exclude) {
                Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
                Err(error) => {
                    log::error!(
                        "FileMoved: failed to get tags for {}: {:?}; skipping",
                        file_id.to_string(),
                        error
                    );
                    return Some(false);
                }
            };

            // Last-writer-wins on the path clock: apply only if this move is
            // strictly newer than our recorded path change. If it lost, do
            // not reposition bytes or forward it (mirrors FileDeleted).
            match database.update_file_logical_path(*file_id, logical_path, *modified_at) {
                Ok(true) => {}
                Ok(false) => {
                    log::debug!(
                        "Ignoring FileMoved for {} (a newer path change supersedes it)",
                        file_id.to_string()
                    );
                    return Some(false);
                }
                Err(error) => {
                    log::error!(
                        "Failed to update logical path for file {}: {:?}; skipping",
                        file_id.to_string(),
                        error
                    );
                    return Some(false);
                }
            }

            for sync_directory in &configuration.sync_directories {
                if let ChangeOrigin::Local { directory_path } = change_origin
                    && directory_path == &sync_directory.path
                {
                    // If the file is already modified in the origin, we don't need to take
                    // any action.
                    return Some(false);
                };

                if let SyncType::TagBased {
                    tags: sync_directory_tags,
                } = &sync_directory.sync_type
                    && !placement::contains_all_tags(sync_directory_tags, &file_tags)
                {
                    // If the directory is tag based and the file *does not* have all the
                    // tags the sync directory does, skip this sync directory.
                    return Some(false);
                }

                // This means the event didn't originate from this sync directory itself and
                // the tags match, thus we may want to apply the change. Resolve where this
                // directory should physically place the file from its new logical path.
                let physical_path = sync_directory
                    .sync_type
                    .physical_for(logical_path, *file_id);
                // TODO: Handle result.
                let _ = command_sender.send(SyncDirectoryCommand::MoveFile {
                    file_id: *file_id,
                    physical_path,
                    sync_directory_path: sync_directory.path.clone(),
                });
            }

            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;
            Some(true)
        }
        Change::FileDeleted {
            file_id,
            deleted_at,
        } => {
            // Soft-delete: `remove_file` sets the tombstone
            // (`deleted = 1`, `deleted_at`) instead of removing the row, and
            // applies last-writer-wins — the delete is only applied if
            // `deleted_at` is newer than the file's latest version
            // `observed_at`. The `file_versions` history is kept so the
            // tombstone reconciles offline-safely and can be restored by a
            // newer edit (restore-after-delete).
            //
            // TODO: Should this be include? Currently this WILL NOT WORK since add file
            // doesn't consider subtags. We would need to get a list of *all* tags (incuding
            // subdags) when adding the file to make it work.
            // -> Maybe make it configurable in the config, per-sync directory.
            let file_tags = match database.tag_ids_for_file(*file_id, store::SubtagRule::Exclude) {
                Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
                Err(error) => {
                    log::error!(
                        "FileDeleted: failed to get tags for {}: {:?}; skipping",
                        file_id.to_string(),
                        error
                    );
                    return Some(false);
                }
            };

            // Idempotent-redelivery guard: if we already hold a tombstone
            // for this file, we're in the same terminal state as the
            // sender. Skip the DB write, the per-sync-directory fan-out,
            // and the forward. Without this, a peer redelivering a delete
            // we've already applied would spuriously re-run `RemoveFile`
            // (which fails with `FailedRemovingFile` because the
            // per-sync-directory row is already gone) and re-broadcast the
            // change, causing tombstones to pile up across the mesh on
            // every reconnect.
            match database.file_deletion_state(*file_id) {
                Ok(Some(state)) if state.deleted => {
                    log::debug!(
                        "Ignoring FileDeleted for {} (already tombstoned)",
                        file_id.to_string()
                    );
                    return Some(false);
                }
                Ok(_) => {}
                Err(error) => {
                    log::error!(
                        "FileDeleted: failed to read deletion state for {}: {:?}; skipping",
                        file_id.to_string(),
                        error
                    );
                    return Some(false);
                }
            }

            match database.remove_file(*file_id, *deleted_at) {
                Ok(true) => {}
                Ok(false) => {
                    // A newer edit or restore out-dated this delete
                    // (last-writer-wins): the file stays live. Do not
                    // remove it from sync directories or forward the
                    // delete.
                    log::debug!(
                        "Ignoring FileDeleted for {} (a newer version supersedes it)",
                        file_id.to_string()
                    );
                    return Some(false);
                }
                Err(error) => {
                    log::error!(
                        "Failed to remove file {}: {:?}; skipping",
                        file_id.to_string(),
                        error
                    );
                    return Some(false);
                }
            }

            for sync_directory in &configuration.sync_directories {
                if let ChangeOrigin::Local { directory_path } = change_origin
                    && directory_path == &sync_directory.path
                {
                    // If the file came from this directory, it is already removed. We
                    // can just skip this directory.
                    return Some(false);
                };

                if let SyncType::TagBased {
                    tags: sync_directory_tags,
                } = &sync_directory.sync_type
                    && !placement::contains_all_tags(sync_directory_tags, &file_tags)
                {
                    // If the directory is tag based and the file *does not* have all the
                    // tags the sync directory does, skip this sync directory.
                    return Some(false);
                }

                // This means the event didn't originate from this sync directory itself,
                // thus we may want to apply it.
                // TODO: Handle result.
                let _ = command_sender.send(SyncDirectoryCommand::RemoveFile {
                    file_id: *file_id,
                    sync_directory_path: sync_directory.path.clone(),
                });
            }

            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;
            Some(true)
        }
        // An inbound `FileRestored` from a peer: the peer un-deleted a file
        // and already confirmed its bytes were recoverable, so this is
        // authoritative. Mirror `FileMetadataChanged` — record the restored
        // version (its `restored_at` becomes the version's `observed_at`,
        // beating any local `deleted_at` under LWW), clear our tombstone,
        // forward onward, and pull the bytes into any local sync directory
        // that wants them. No local-availability gate here: only the
        // *originating* device gates restore on availability.
        Change::FileRestored {
            file_id,
            content_hash,
            size,
            restored_at,
        } => {
            // Skip only if this hash is already our latest catalog version
            // AND the file is already live — otherwise a restore that clears
            // a tombstone (or reverts to an older-but-restored hash) is a
            // genuine state change we must apply.
            let current_hash = database
                .latest_version(*file_id)
                .ok()
                .flatten()
                .map(|version| version.content_hash);
            let already_live = matches!(
                database.file_deletion_state(*file_id),
                Ok(Some(state)) if !state.deleted
            );

            if current_hash.as_deref() == Some(content_hash.as_str()) && already_live {
                log::debug!(
                    "Ignoring no-op FileRestored for {} (already the current, live version)",
                    file_id.to_string()
                );
                super::forward::forward_to_peers(
                    configuration,
                    runtime_configuration,
                    change,
                    change_origin,
                )
                .await;
                return Some(false);
            }

            // Apply the restore under three-way LWW using the peer's
            // `restored_at` stamp (preserved verbatim from the wire), so it
            // orders correctly against our own `deleted_at`. No version is
            // fabricated: the restored version is the file's latest existing
            // version, which we already have in our history. If a newer
            // local delete out-votes the restore, `apply_restore` leaves the
            // tombstone and we skip the byte pull.
            let restored = match database.apply_restore(*file_id, *restored_at) {
                Ok(restored) => restored,
                Err(error) => {
                    log::error!(
                        "FileRestored: failed to apply restore for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                    false
                }
            };

            // Always forward so the announcement propagates the tree, even
            // if it lost LWW locally (a downstream peer may still be behind).
            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;

            // Pull the bytes to update any local sync directory that should
            // hold this now-live file — only if the restore actually won
            // (otherwise the file stays tombstoned and wants no bytes).
            if restored {
                crate::peer::fetch::request_pull_from_origin(
                    runtime_configuration,
                    change_origin,
                    *file_id,
                    content_hash.clone(),
                    *size,
                    messages::MaterializePlacement::Change,
                )
                .await;
            }
            Some(true)
        }
        _ => None,
    }
}

/// `CatalogCommand::CatalogFile`: record a file + version on behalf of a peer
/// session's `Manifest` reconciliation, then forward it onward.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn catalog_file(
    configuration: &Configuration,
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    database: &mut CatalogStore,
    file_id: tagsy_core::FileId,
    logical_path: tagsy_core::LogicalPath,
    logical_path_modified_at: i64,
    content_hash: String,
    size: u64,
    origin: ChangeOrigin,
) {
    // A peer session's `Manifest` reconciliation decided to catalog
    // this file/version. We are the sole main-DB writer, so the
    // write happens here. Insert the `files` row if new, then append
    // the version (byte-independent catalog; the bytes are pulled
    // separately on the session link). Seed the path clock from the
    // manifest entry's originating stamp (not our receive time).
    let is_new = !database.file_exists(file_id).unwrap_or(false);
    if is_new
        && let Err(error) = database.add_file(file_id, &logical_path, logical_path_modified_at)
    {
        log::error!(
            "CatalogFile: failed to add file {} ({}): {:?}; skipping version record",
            file_id.to_string(),
            logical_path,
            error
        );
        return;
    }

    if let Err(error) = database.record_version(
        file_id,
        &content_hash,
        super::forward::version_origin(&origin),
        size as i64,
    ) {
        log::error!(
            "CatalogFile: failed to record version for {}: {:?}",
            file_id.to_string(),
            error
        );
    }
    // Cataloging a version means the peer holds content newer than
    // (or equal to) any local tombstone — clear it so a
    // previously-deleted file becomes live again (restore after
    // delete). No-op when the file was not tombstoned.
    if let Err(error) = database.restore_file(file_id) {
        log::error!(
            "CatalogFile: failed to clear tombstone for {}: {:?}",
            file_id.to_string(),
            error
        );
    }

    // Announce this reconcile-derived version onward so it
    // propagates transitively across the peer tree. Without this a
    // change learned via `Manifest` reconciliation would dead-end
    // here: a hub (e.g. `central`) that catches an offline-created
    // file up from one peer via reconcile would never relay it to
    // its other continuously-connected peers, which only ever hear
    // live `FileMetadata{Added,Changed}` — never this catalog write.
    // We reconcile pairwise, but not every pair of peers reconciles
    // directly, so transitive forwarding is required for
    // convergence. Mirror the live handlers: a brand-new file is a
    // `FileMetadataAdded` (tags empty — they reconcile separately via
    // `TagManifest`, exactly as this reconcile's own `Create`
    // placement left them); a new version of a known file is a
    // `FileMetadataChanged`. The `content_hash`/`origin` carry the
    // three-way LWW clocks unchanged so downstream reconciliation is
    // unaffected.
    let change = if is_new {
        Change::FileMetadataAdded {
            file_id,
            logical_path,
            logical_path_modified_at,
            content_hash,
            size,
            tags: Vec::new(),
        }
    } else {
        Change::FileMetadataChanged {
            file_id,
            content_hash,
            size,
        }
    };
    super::forward::forward_to_peers(configuration, runtime_configuration, &change, &origin).await;
}

/// `CatalogCommand::Materialize`: place bytes that arrived over a peer transfer
/// into matching sync directories (the version was recorded at announce time).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn materialize(
    configuration: &Configuration,
    database: &mut CatalogStore,
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    change_sender: &UnboundedSender<CatalogCommand>,
    event_sender: &tokio::sync::broadcast::Sender<Change>,
    file_id: tagsy_core::FileId,
    content: crate::file_bytes::FileBytes,
    content_hash: String,
    origin: ChangeOrigin,
    placement: messages::MaterializePlacement,
) {
    // Bytes arrived over a peer transfer. The version was already
    // recorded into the catalog when the triggering announcement was
    // handled (`FileMetadataAdded`/`Changed` or `Manifest`
    // reconcile), so we do NOT record it here — `Materialize` is now
    // purely about placing the bytes into matching sync directories.
    // Forwarding to peers likewise already happened at announce time.
    log::debug!(
        "Materializing received content for {} ({})",
        file_id.to_string(),
        content_hash
    );

    // Build the local placement targets for the arrived bytes.
    let targets = match placement {
        messages::MaterializePlacement::Create { logical_path, tags } => {
            // New file: create it in every matching sync directory,
            // deriving each directory's physical path from the
            // logical path.
            //
            // Tag-filter using the *union* of the carried tags and
            // the file's current DB tags. The carried tags cover a
            // live `FileMetadataAdded` (whose `FileTagged`
            // relationships may not be applied yet); the DB tags
            // cover a `Manifest` reconcile pull, which carries empty
            // tags because it cannot know them at pull time — but by
            // the time this `Materialize` runs, the `TagManifest`'s
            // `FileTagged` changes have been applied (they are
            // enqueued before the pull's transfer completes), so the
            // DB has them. Without this, reconcile-pulled files
            // matched no TagBased directory and were dropped.
            let db_tags = database
                .tag_ids_for_file(file_id, store::SubtagRule::Exclude)
                .map(|iter| iter.into_iter().collect::<Vec<TagId>>())
                .unwrap_or_else(|error| {
                    log::error!(
                        "Materialize: failed to read tags for {}: {:?}; using carried tags only",
                        file_id.to_string(),
                        error
                    );
                    Vec::new()
                });
            let effective_tags = placement::effective_placement_tags(&tags, &db_tags);

            let mut targets = Vec::new();
            for sync_directory in &configuration.sync_directories {
                if let SyncType::TagBased {
                    tags: sync_directory_tags,
                } = &sync_directory.sync_type
                    && !placement::contains_all_tags(sync_directory_tags, &effective_tags)
                {
                    return;
                }
                let physical_path = sync_directory
                    .sync_type
                    .physical_for(&logical_path, file_id);
                targets.push(Placement::Create {
                    file_id,
                    physical_path,
                    sync_directory_path: sync_directory.path.clone(),
                });
            }
            targets
        }
        messages::MaterializePlacement::Change => {
            // Existing file: overwrite it in the sync directories
            // that already hold it (tag-filtered by current tags).
            let file_tags = database
                .tag_ids_for_file(file_id, store::SubtagRule::Exclude)
                .map(|iter| iter.into_iter().collect::<Vec<TagId>>())
                .unwrap_or_else(|error| {
                    log::error!(
                        "Materialize: failed to read tags for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                    Vec::new()
                });
            // Peer-origin: no origin directory to skip. Sentinel
            // empty path never matches a real sync directory.
            let sentinel = ChangeOrigin::Local {
                directory_path: std::path::PathBuf::new(),
            };
            placement::placements_for(configuration, &sentinel, file_id, &file_tags)
        }
    };
    placement::place_content(command_sender, targets, content).await;
    // No `forward_to_peers` here: the announcement was already
    // forwarded when it was first handled (announce time). `origin`
    // is unused now that we neither record nor re-announce here.
    let _ = origin;
    // Bytes for this version are now on disk locally: on an
    // eager-preview device, warm the preview cache now so a later
    // peer `PreviewRequest` is a cache hit rather than a decode.
    maybe_eager_preview(configuration, change_sender, file_id);

    // Publish to UI-facing API subscribers. The catalog already
    // published at announce time, but that fires *before* the bytes
    // exist locally, so anything keyed on local presence (a file
    // detail view switching from the remote thumbnail to the
    // full-fidelity on-disk preview, a tag-triggered fetch landing)
    // would stay stale until the view is reopened. This is the
    // "bytes are now on disk" edge.
    //
    // Synthetic, local-only: the event bus is typed as `Change`, so
    // we re-send the metadata change we already announced rather
    // than modelling byte arrival properly. It is never forwarded to
    // peers, so the duplicate cannot escape this device. See
    // `EVENT PUBLISHING` on `handle_changes`.
    let size = database
        .latest_version(file_id)
        .unwrap_or_else(|error| {
            log::error!(
                "Materialize: latest_version failed for {}: {:?}; reporting size 0",
                file_id.to_string(),
                error
            );
            None
        })
        .map(|version| version.size.max(0) as u64)
        .unwrap_or(0);
    let _ = event_sender.send(Change::FileMetadataChanged {
        file_id,
        content_hash,
        size,
    });
}

/// `CatalogCommand::AnnounceProvided`: a local client (CLI) uploaded/edited a
/// file it serves on demand — record it locally and announce metadata-only.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn announce_provided(
    configuration: &Configuration,
    tag_rules: &crate::configuration::CompiledTagRules,
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    database: &mut CatalogStore,
    event_sender: &tokio::sync::broadcast::Sender<Change>,
    file_id: tagsy_core::FileId,
    logical_path: Option<tagsy_core::LogicalPath>,
    content_hash: String,
    size: u64,
    mut tags: Vec<TagId>,
) {
    // A local client (CLI) uploaded/edited a file it serves on
    // demand. Record it locally and announce metadata-only to peers;
    // peers pull the bytes from the registered provider. No local
    // sync-directory placement: a CLI upload targets peers (files
    // already in a sync directory are synced without the CLI).
    let change = match logical_path {
        Some(logical_path) => {
            // Genuinely local (CLI) creation: "now" is the true
            // origin time. Stamp the same value onto the outgoing
            // announcement so peers seed an identical path clock.
            let logical_path_modified_at = clock::now_millis();
            if let Err(error) = database.add_file(file_id, &logical_path, logical_path_modified_at)
            {
                log::error!(
                    "AnnounceProvided: failed to add file {} ({}): {:?}",
                    file_id.to_string(),
                    logical_path,
                    error
                );
                return;
            }
            // Creation-time tag rules. This is one of exactly two
            // places a file is *created by this device* (the other
            // is the local `ContentChange::FileAdded` branch in
            // `handle_content_change`), and therefore one of
            // exactly two places rules may run. An
            // `AnnounceProvided` is always local — a peer's
            // announcement arrives as `Change::FileMetadataAdded`
            // and is handled further down, deliberately without
            // rules.
            //
            // Merged before the tagging loop and before the change
            // is built, so rule tags are persisted locally and
            // carried to peers exactly like caller-supplied ones.
            super::content::apply_tag_rules(tag_rules, &logical_path, &mut tags);

            // Persist the upload's tags into the local catalog. The
            // outgoing `FileMetadataAdded` carries them to peers, but
            // the local DB is only updated here — without this a
            // locally-uploaded file would appear untagged on this
            // device (its tags only materializing on peers, or on a
            // later byte-pull placement). Stamp them with the same
            // creation clock as the file so LWW orders consistently.
            for tag_id in &tags {
                if let Err(error) = database.tag_file(*tag_id, file_id, logical_path_modified_at) {
                    log::error!(
                        "AnnounceProvided: failed to tag file {} with {}: {:?}",
                        file_id.to_string(),
                        tag_id.to_string(),
                        error
                    );
                }
            }
            Change::FileMetadataAdded {
                file_id,
                logical_path,
                logical_path_modified_at,
                content_hash: content_hash.clone(),
                size,
                tags,
            }
        }
        None => Change::FileMetadataChanged {
            file_id,
            content_hash: content_hash.clone(),
            size,
        },
    };
    let origin = ChangeOrigin::Local {
        directory_path: std::path::PathBuf::new(),
    };
    if let Err(error) = database.record_version(
        file_id,
        &content_hash,
        super::forward::version_origin(&origin),
        size as i64,
    ) {
        log::error!(
            "AnnounceProvided: failed to record version for {}: {:?}",
            file_id.to_string(),
            error
        );
    }
    super::forward::forward_to_peers(configuration, runtime_configuration, &change, &origin).await;
    // Publish to UI-facing API subscribers so an open file view
    // picks up the new version on the device that *made* the edit.
    // Peers learn of it through the forwarded `Change` above, which
    // they ingest as `Ingest::Meta` and publish from the shared site
    // at the bottom of the loop; without this the originating device
    // is the only one that never refreshes. See `EVENT PUBLISHING`
    // on `handle_changes`.
    let _ = event_sender.send(change);
}
