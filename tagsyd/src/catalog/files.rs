//! The file-lifecycle arms of the catalog's metadata dispatch:
//! `FileMetadataAdded`/`FileMetadataChanged` (peer announcements),
//! `FileMoved`, `FileDeleted`, `FileRestored`, plus the three command arms that
//! catalog bytes/versions (`CatalogFile`, `Materialize`, `AnnounceUpload`).
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
use crate::catalog::placement;
use crate::catalog::previews::maybe_eager_preview;
use crate::configuration::{Configuration, RuntimeConfiguration, SyncType};
use crate::peer::relay::ChunkRelay;
use crate::store::{self, CatalogStore};
use crate::sync_directories::SyncDirectoryCommand;
use crate::{clock, operations};

/// Whether a version stamped `observed_at` is newer than `file_id`'s current
/// latest version — the content half of last-writer-wins, with the same
/// strict comparison reconciliation uses (`peer::plan::decide_request`).
///
/// A version that is not newer is superseded and must not be recorded:
/// versions are ordered by number, i.e. by arrival, so appending it would make
/// an older edit the latest (concurrent edits on disconnected devices reach a
/// node in either order). An unknown file, or one with no versions, accepts
/// any version.
fn supersedes_latest(
    database: &CatalogStore,
    file_id: tagsy_core::FileId,
    observed_at: i64,
) -> bool {
    match database.latest_version(file_id) {
        Ok(Some(latest)) => observed_at > latest.observed_at,
        Ok(None) => true,
        Err(error) => {
            log::error!(
                "latest_version failed for {}: {error:?}; accepting the version",
                file_id.to_string()
            );
            true
        }
    }
}

/// Whether recording a peer's version `content_hash` of `file_id` calls for
/// pulling its bytes. Checked *before* recording it. Not when the version is
/// the content we already hold as latest, recorded again: the bytes are
/// unchanged — unless the file is tombstoned here, where the version revives
/// it and the bytes dropped on delete must be placed again.
fn version_needs_transfer(
    database: &CatalogStore,
    file_id: tagsy_core::FileId,
    content_hash: &str,
) -> bool {
    let same_content = database
        .latest_version(file_id)
        .ok()
        .flatten()
        .is_some_and(|latest| latest.content_hash == content_hash);
    let deleted = database
        .file_deletion_state(file_id)
        .ok()
        .flatten()
        .is_some_and(|state| state.deleted);
    !same_content || deleted
}

/// Ask the announcing peer for a version's bytes, unless no local sync
/// directory would take them ([`placement::materialize_targets`] is the same
/// decision `Materialize` places by). Call after recording the version, so the
/// tombstone and tags it reads are the ones the bytes will be placed against.
#[allow(clippy::too_many_arguments)]
async fn pull_if_wanted(
    configuration: &Configuration,
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    database: &mut CatalogStore,
    change_origin: &ChangeOrigin,
    file_id: tagsy_core::FileId,
    content_hash: &str,
    size: u64,
    placement: messages::MaterializePlacement,
) {
    if placement::materialize_targets(configuration, database, file_id, &placement).is_empty() {
        log::debug!(
            "Not pulling {} [{}]: no local sync directory wants it",
            file_id.to_string(),
            content_hash.get(..8).unwrap_or(content_hash)
        );
        return;
    }
    crate::peer::fetch::request_pull_from_origin(
        runtime_configuration,
        change_origin,
        file_id,
        content_hash.to_owned(),
        size,
        placement,
    )
    .await;
}

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
    // Purge enforcement: a purged file id takes absolute priority over the
    // catalog. Before applying any file-lifecycle change (local or peer), drop
    // it if the id has been purged — a stale re-announcement of a broken file
    // must never re-enter the catalog. This is the single chokepoint that makes
    // "purged wins over the catalog" checkable rather than conventional; it lives
    // here in the sole writer so no read path has to defend against it.
    //
    // `FilePurged` itself is exempt: it is the fact that *establishes* the purge
    // and is handled below (idempotent via `record_purge`).
    if let Some(file_id) = purge_guard_file_id(change) {
        match database.is_purged(file_id) {
            Ok(true) => {
                log::debug!(
                    "Dropping {} for purged file {} (purge takes priority over the catalog)",
                    change_variant_name(change),
                    file_id.to_string(),
                );
                return Some(false);
            }
            Ok(false) => {}
            Err(error) => {
                log::error!(
                    "Failed to check purge state for {}: {:?}; applying change anyway",
                    file_id.to_string(),
                    error
                );
            }
        }
    }

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
            observed_at,
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
                // Skip only if this is not newer than the version we already
                // hold as latest. A version is ordered by its `observed_at`,
                // not identified by its hash: a revert to an older hash, or
                // the same bytes recorded again, is a genuine new version and
                // must be appended — it is the content half of the three-way
                // LWW, so it can overrule a delete (and its bytes are
                // re-pulled where wanted).
                if !supersedes_latest(database, *file_id, *observed_at) {
                    log::debug!(
                        "Ignoring FileMetadataAdded for {}: not newer than our latest version",
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
            }

            let transfer = version_needs_transfer(database, *file_id, content_hash);

            // Record the version into the catalog now, on announcement, with
            // the *originating* device's `observed_at` (preserved verbatim over
            // the wire), never our receive time — this is the content half of
            // the three-way delete/edit/restore LWW.
            if let Err(error) = database.record_version_at(
                *file_id,
                content_hash,
                super::forward::version_origin(change_origin),
                *size as i64,
                *observed_at,
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

            // Pull the bytes from the announcing peer to place the file into
            // any matching local sync directory.
            if transfer {
                pull_if_wanted(
                    configuration,
                    runtime_configuration,
                    database,
                    change_origin,
                    *file_id,
                    content_hash,
                    *size,
                    messages::MaterializePlacement::Create {
                        logical_path: logical_path.clone(),
                        tags: tags.clone(),
                    },
                )
                .await;
            }
            Some(true)
        }
        // A metadata-only `FileMetadataChanged` announcement — always from a
        // peer. Record the new version into the catalog and forward it; pull
        // the bytes where a local sync directory wants them.
        Change::FileMetadataChanged {
            file_id,
            content_hash,
            size,
            observed_at,
        } => {
            // Skip only if this is not newer than our latest catalog version.
            // A version is ordered by its `observed_at`, not identified by its
            // hash: a revert back to an older hash, or the same bytes recorded
            // again, is a genuine new version we must append (and re-pull the
            // bytes for where wanted). Skipping a same-hash version left a
            // device holding a tombstone the newer version had overruled
            // everywhere else.
            if !supersedes_latest(database, *file_id, *observed_at) {
                log::debug!(
                    "Ignoring FileMetadataChanged for {} (not newer than our latest version)",
                    file_id.to_string()
                );
                // Announce onward so the change still propagates the tree.
                super::forward::forward_to_peers(
                    configuration,
                    runtime_configuration,
                    change,
                    change_origin,
                )
                .await;
            } else {
                let transfer = version_needs_transfer(database, *file_id, content_hash);

                // Record the new version into the catalog now, on
                // announcement (independent of whether we pull the bytes), with
                // the *originating* device's `observed_at` preserved verbatim —
                // never our receive time (the content half of the three-way
                // LWW).
                if let Err(error) = database.record_version_at(
                    *file_id,
                    content_hash,
                    super::forward::version_origin(change_origin),
                    *size as i64,
                    *observed_at,
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
                if transfer {
                    pull_if_wanted(
                        configuration,
                        runtime_configuration,
                        database,
                        change_origin,
                        *file_id,
                        content_hash,
                        *size,
                        messages::MaterializePlacement::Change,
                    )
                    .await;
                }
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
                    continue;
                };

                if let SyncType::TagBased {
                    tags: sync_directory_tags,
                } = &sync_directory.sync_type
                    && !placement::contains_all_tags(sync_directory_tags, &file_tags)
                {
                    // If the directory is tag based and the file *does not* have all the
                    // tags the sync directory does, skip this sync directory.
                    continue;
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
            //
            // A `None` state means the file is genuinely unknown to us. A live
            // `Change::FileDeleted` carries only `file_id`/`deleted_at` — no
            // path or version — so we cannot reconstruct a faithful tombstoned
            // row from it here (that needs the manifest's fuller data, handled
            // by `catalog_tombstone`). This happens when a delete races ahead of
            // its create over the wire, or a relay forwards a delete for a file
            // the middle node never cataloged. Don't fabricate a partial row and
            // don't silently conflate it with the LWW-superseded case below: log
            // it and let it converge on the next manifest exchange, which does
            // carry the path/version needed to reconstruct the tombstone.
            match database.file_deletion_state(*file_id) {
                Ok(Some(state)) if state.deleted => {
                    log::debug!(
                        "Ignoring FileDeleted for {} (already tombstoned)",
                        file_id.to_string()
                    );
                    return Some(false);
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    log::debug!(
                        "FileDeleted for unknown file {}; cannot reconstruct a row from the wire \
                         delete alone — will converge via the next manifest exchange",
                        file_id.to_string()
                    );
                    return Some(false);
                }
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
                    continue;
                };

                if let SyncType::TagBased {
                    tags: sync_directory_tags,
                } = &sync_directory.sync_type
                    && !placement::contains_all_tags(sync_directory_tags, &file_tags)
                {
                    // If the directory is tag based and the file *does not* have all the
                    // tags the sync directory does, skip this sync directory.
                    continue;
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
                pull_if_wanted(
                    configuration,
                    runtime_configuration,
                    database,
                    change_origin,
                    *file_id,
                    content_hash,
                    *size,
                    messages::MaterializePlacement::Change,
                )
                .await;
            }
            Some(true)
        }
        // A permanent purge of a broken file — from the local operator
        // (`purge-broken`) or reconciled from a peer. Terminal and
        // irreversible: record the id in the permanent purge set, hard-delete
        // the file's catalog rows, drop its on-disk bytes, and forward the
        // purge onward so it propagates across the mesh.
        Change::FilePurged { file_id } => {
            // Read the file's tags *before* stripping, so we can fan `RemoveFile`
            // out to exactly the sync directories that hold it (a TagBased
            // directory only holds files carrying all its tags). Once stripped,
            // this information is gone.
            let file_tags = match database.tag_ids_for_file(*file_id, store::SubtagRule::Exclude) {
                Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
                Err(error) => {
                    log::error!(
                        "FilePurged: failed to get tags for {}: {:?}; proceeding with empty tag \
                         set",
                        file_id.to_string(),
                        error
                    );
                    Vec::new()
                }
            };

            // Idempotent-redelivery guard: `record_purge` returns false if the
            // id was already purged. Normally we are then in the same terminal
            // state as the sender and can skip the strip, the per-sync-directory
            // fan-out, and the forward, so a purge redelivered on every
            // reconnect does not re-run the (failing) `RemoveFile` or re-flood
            // the mesh (mirroring the "already tombstoned" guard in
            // `FileDeleted`).
            //
            // BUT: if the id is already purged yet a `files_v2` row still
            // exists, the catalog has drifted from the purge set — e.g. a
            // resurrection slipped in through a reconciliation path before it
            // honored the purge guard, or a prior `strip_purged_file` failed
            // partway. Re-run the strip to repair it (idempotent), so the purge
            // is self-healing across upgrades rather than requiring the id to be
            // purged afresh.
            let newly_recorded = match database.record_purge(*file_id) {
                Ok(recorded) => recorded,
                Err(error) => {
                    log::error!(
                        "FilePurged: failed to record purge for {}: {:?}; skipping",
                        file_id.to_string(),
                        error
                    );
                    return Some(false);
                }
            };
            if !newly_recorded {
                let row_lingers = database.file_exists(*file_id).unwrap_or(false);
                if !row_lingers {
                    log::debug!(
                        "Ignoring FilePurged for {} (already purged, catalog clean)",
                        file_id.to_string()
                    );
                    return Some(false);
                }
                log::warn!(
                    "FilePurged: {} already purged but a catalog row lingers; re-stripping",
                    file_id.to_string()
                );
            }

            if let Err(error) = database.strip_purged_file(*file_id) {
                log::error!(
                    "FilePurged: failed to strip catalog rows for {}: {:?}; the purge is recorded \
                     but its rows may linger",
                    file_id.to_string(),
                    error
                );
            }

            // Drop the on-disk bytes from every sync directory that held the
            // file, reusing the same fan-out as `FileDeleted`.
            for sync_directory in &configuration.sync_directories {
                if let ChangeOrigin::Local { directory_path } = change_origin
                    && directory_path == &sync_directory.path
                {
                    continue;
                };

                if let SyncType::TagBased {
                    tags: sync_directory_tags,
                } = &sync_directory.sync_type
                    && !placement::contains_all_tags(sync_directory_tags, &file_tags)
                {
                    continue;
                }

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
        _ => None,
    }
}

/// The `file_id` a purge-guard check applies to, or `None` for changes the
/// guard does not gate. Every file-lifecycle change except `FilePurged` itself
/// (which *establishes* the purge) is gated, so a purged id can never re-enter
/// the catalog through any of them.
fn purge_guard_file_id(change: &Change) -> Option<tagsy_core::FileId> {
    match change {
        Change::FileMetadataAdded { file_id, .. }
        | Change::FileMoved { file_id, .. }
        | Change::FileMetadataChanged { file_id, .. }
        | Change::FileDeleted { file_id, .. }
        | Change::FileRestored { file_id, .. } => Some(*file_id),
        _ => None,
    }
}

/// A short human name for a `Change` variant, for the purge-guard drop log.
fn change_variant_name(change: &Change) -> &'static str {
    match change {
        Change::FileMetadataAdded { .. } => "FileMetadataAdded",
        Change::FileMoved { .. } => "FileMoved",
        Change::FileMetadataChanged { .. } => "FileMetadataChanged",
        Change::FileDeleted { .. } => "FileDeleted",
        Change::FileRestored { .. } => "FileRestored",
        Change::FilePurged { .. } => "FilePurged",
        _ => "Change",
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
    observed_at: i64,
    origin: ChangeOrigin,
    pull: Option<messages::MaterializePlacement>,
) {
    // Purge enforcement: a purged file id takes absolute priority over the
    // catalog. This reconciliation path re-materializes a file the peer's file
    // `Manifest` still advertises as live, so it MUST honor the same guard as
    // `apply_change` — otherwise a peer that has not yet learned the purge would
    // resurrect the row on every reconnect (a `files_v2`/`file_versions_v1` row
    // reappearing for a purged id). Drop it silently; the peer converges when it
    // learns the purge itself.
    match database.is_purged(file_id) {
        Ok(true) => {
            log::debug!(
                "CatalogFile: dropping {} (purged; takes priority over the catalog)",
                file_id.to_string()
            );
            return;
        }
        Ok(false) => {}
        Err(error) => {
            log::error!(
                "CatalogFile: failed to check purge state for {}: {:?}; proceeding",
                file_id.to_string(),
                error
            );
        }
    }

    // A peer session's `Manifest` reconciliation decided to catalog
    // this file/version. We are the sole main-DB writer, so the
    // write happens here. Insert the `files` row if new, then append
    // the version (byte-independent catalog; the bytes are pulled
    // separately on the session link). Seed the path clock from the
    // manifest entry's originating stamp (not our receive time).
    let is_new = !database.file_exists(file_id).unwrap_or(false);
    // Reconciliation decided from a snapshot; a newer version may have been
    // recorded since (e.g. a live announcement on another link).
    if !is_new && !supersedes_latest(database, file_id, observed_at) {
        log::debug!(
            "CatalogFile: dropping {} [{}]: older than our latest version",
            file_id.to_string(),
            content_hash.get(..8).unwrap_or(&content_hash)
        );
        return;
    }
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

    // Record the version with the *originating* device's `observed_at` (from
    // the manifest entry), never our receive time — this is the content half of
    // the three-way delete/edit/restore LWW, and restamping it would make a
    // peer's later `deleted_at` lose and resurrect a file that is dead
    // everywhere else.
    if let Err(error) = database.record_version_at(
        file_id,
        &content_hash,
        super::forward::version_origin(&origin),
        size as i64,
        observed_at,
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

    // Pull the bytes the reconciliation asked for, if a local sync directory
    // wants them. Decided here rather than in the session: only the writer
    // sees the tags the peer's `TagManifest` just applied (queued ahead of
    // this command) and this version's effect on the tombstone.
    if let Some(placement) = pull {
        pull_if_wanted(
            configuration,
            runtime_configuration,
            database,
            &origin,
            file_id,
            &content_hash,
            size,
            placement,
        )
        .await;
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
            observed_at,
            tags: Vec::new(),
        }
    } else {
        Change::FileMetadataChanged {
            file_id,
            content_hash,
            size,
            observed_at,
        }
    };
    super::forward::forward_to_peers(configuration, runtime_configuration, &change, &origin).await;
}

/// `CatalogCommand::CatalogTombstone`: reconstruct a tombstoned file (row +
/// latest version, both already deleted) for a file this device has never seen
/// but a peer advertises as deleted, then forward the delete onward.
///
/// The sibling of [`catalog_file`] for the delete case: a file created *and*
/// deleted on another device while we were offline arrives only as a tombstone
/// in the manifest, with no preceding create for the live-delete path to build
/// on. Without this the tombstone is dropped and our catalog stays permanently
/// unaware of a file the rest of the mesh knows about (surfacing as a recurring
/// `MissingFile` placement-sweep line on every reconnect).
///
/// No bytes are pulled (a deleted file needs none) and no sync-directory
/// placement runs (there is nothing live to place). We only record the catalog
/// row and forward.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn catalog_tombstone(
    configuration: &Configuration,
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    database: &mut CatalogStore,
    file_id: tagsy_core::FileId,
    logical_path: tagsy_core::LogicalPath,
    logical_path_modified_at: i64,
    content_hash: String,
    size: u64,
    observed_at: i64,
    deleted_at: i64,
    restored_at: i64,
    origin: ChangeOrigin,
) {
    // Purge enforcement, same as `catalog_file`: never reconstruct a row for a
    // purged id, even as a tombstone. A purge is terminal and outranks a delete.
    match database.is_purged(file_id) {
        Ok(true) => {
            log::debug!(
                "CatalogTombstone: dropping {} (purged; takes priority over the catalog)",
                file_id.to_string()
            );
            return;
        }
        Ok(false) => {}
        Err(error) => {
            log::error!(
                "CatalogTombstone: failed to check purge state for {}: {:?}; proceeding",
                file_id.to_string(),
                error
            );
        }
    }

    match database.add_tombstoned_file(
        file_id,
        &logical_path,
        logical_path_modified_at,
        &content_hash,
        size as i64,
        observed_at,
        super::forward::version_origin(&origin),
        deleted_at,
        restored_at,
    ) {
        Ok(true) => {}
        Ok(false) => {
            // The file already had a row (a create raced ahead of this
            // reconstruction, or a duplicate manifest frame). The existing
            // delete/LWW paths own it; nothing to do and nothing to forward.
            log::debug!(
                "CatalogTombstone: file {} already known; skipping reconstruction",
                file_id.to_string()
            );
            return;
        }
        Err(error) => {
            log::error!(
                "CatalogTombstone: failed to reconstruct tombstone for {} ({}): {:?}",
                file_id.to_string(),
                logical_path,
                error
            );
            return;
        }
    }

    // Forward the delete onward so the tombstone propagates transitively across
    // the peer tree (mirroring `catalog_file`'s forward): a hub that catches up
    // an offline-created-then-deleted file from one peer must relay the delete
    // to its other peers, which never saw a live `FileDeleted` for it. The
    // `deleted_at` carries the LWW clock unchanged.
    let change = Change::FileDeleted {
        file_id,
        deleted_at,
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

    // The file may have been deleted — or purged, which strips its row
    // entirely — while its bytes were in flight (e.g. a user deletes it while a
    // sweep or pull is fetching it). Placing them now would put a file on disk
    // that the catalog says is gone. Only a live catalog row gets bytes.
    match database.file_deletion_state(file_id) {
        Ok(Some(state)) if !state.deleted => {}
        Ok(state) => {
            log::debug!(
                "Materialize: {} is {} in the catalog; not placing its bytes",
                file_id.to_string(),
                if state.is_some() { "deleted" } else { "gone" }
            );
            return;
        }
        Err(error) => {
            log::error!(
                "Materialize: failed to read deletion state for {}: {error:?}; not placing",
                file_id.to_string()
            );
            return;
        }
    }

    // Only the catalog's current latest version belongs on disk. Bytes of an
    // older version can still arrive — a pull started before a newer version
    // was recorded — and must not overwrite the newer content.
    match database.latest_version(file_id) {
        Ok(Some(latest)) if latest.content_hash != content_hash => {
            log::debug!(
                "Materialize: {} [{}] is no longer the latest version; not placing",
                file_id.to_string(),
                content_hash.get(..8).unwrap_or(&content_hash)
            );
            return;
        }
        _ => {}
    }

    // Build the local placement targets for the arrived bytes.
    let targets = placement::materialize_targets(configuration, database, file_id, &placement);
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
    let latest = database.latest_version(file_id).unwrap_or_else(|error| {
        log::error!(
            "Materialize: latest_version failed for {}: {:?}; reporting size 0",
            file_id.to_string(),
            error
        );
        None
    });
    let size = latest
        .as_ref()
        .map(|version| version.size.max(0) as u64)
        .unwrap_or(0);
    // Synthetic, local-only event: carry the already-recorded version's
    // `observed_at` (not a fresh `now()`), so the UI event mirrors the catalog
    // exactly. Never forwarded to peers.
    let observed_at = latest
        .as_ref()
        .map(|version| version.observed_at)
        .unwrap_or(0);
    let _ = event_sender.send(Change::FileMetadataChanged {
        file_id,
        content_hash,
        size,
        observed_at,
    });
}

/// `CatalogCommand::AnnounceUpload`: a local client (CLI / UI) uploaded or
/// edited a file it serves on demand — record it, announce it metadata-only,
/// and place its bytes into this node's own matching sync directories.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn announce_upload(
    configuration: &Configuration,
    tag_rules: &crate::configuration::CompiledTagRules,
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    database: &mut CatalogStore,
    event_sender: &tokio::sync::broadcast::Sender<Change>,
    pending_fetches: &ChunkRelay,
    pull_scheduler: &crate::peer::pull_scheduler::PullScheduler,
    change_sender: &UnboundedSender<CatalogCommand>,
    operations: &operations::Operations,
    file_id: tagsy_core::FileId,
    logical_path: Option<tagsy_core::LogicalPath>,
    content_hash: String,
    size: u64,
    mut tags: Vec<TagId>,
) {
    // A local client uploaded/edited a file, whose bytes are already in the
    // outbox. Record it, announce metadata-only to peers (who pull from the
    // outbox), and place it into our own matching sync
    // directories exactly as if a peer had announced it (see the end of this
    // function) — so the result is the same whether this device later
    // reconnects or not.
    //
    // This device is the *origin* of the version: stamp `observed_at` once with
    // our wall clock and carry the identical value to both the local
    // `file_versions` row and the outgoing announcement, so every peer records
    // the same content LWW clock (never their own receive time).
    let observed_at = clock::now_millis();
    let change = match logical_path {
        Some(logical_path) => {
            // Genuinely local (CLI) creation: "now" is the true
            // origin time. Stamp the same value onto the outgoing
            // announcement so peers seed an identical path clock.
            let logical_path_modified_at = clock::now_millis();
            if let Err(error) = database.add_file(file_id, &logical_path, logical_path_modified_at)
            {
                log::error!(
                    "AnnounceUpload: failed to add file {} ({}): {:?}",
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
            // `AnnounceUpload` is always local — a peer's
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
                        "AnnounceUpload: failed to tag file {} with {}: {:?}",
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
                observed_at,
                tags,
            }
        }
        None => Change::FileMetadataChanged {
            file_id,
            content_hash: content_hash.clone(),
            size,
            observed_at,
        },
    };
    let origin = ChangeOrigin::Local {
        directory_path: std::path::PathBuf::new(),
    };
    if let Err(error) = database.record_version_at(
        file_id,
        &content_hash,
        super::forward::version_origin(&origin),
        size as i64,
        observed_at,
    ) {
        log::error!(
            "AnnounceUpload: failed to record version for {}: {:?}",
            file_id.to_string(),
            error
        );
    }
    // A newer version supersedes an older tombstone (the edit half of the
    // three-way LWW), exactly as when a peer announces it. No-op otherwise.
    if let Err(error) = database.restore_file(file_id) {
        log::error!(
            "AnnounceUpload: failed to clear tombstone for {}: {:?}",
            file_id.to_string(),
            error
        );
    }
    super::forward::forward_to_peers(configuration, runtime_configuration, &change, &origin).await;

    // Local placement: pull the bytes from the outbox (the relay asks it
    // before any peer) and `Materialize` them, the same pipeline a
    // peer-announced file takes. A new file is created in every matching
    // directory; an edit puts it in place where it belongs. Skipped when no
    // local directory would take the file; the outbox keeps it for peers.
    let placement = match &change {
        Change::FileMetadataAdded {
            logical_path, tags, ..
        } => messages::MaterializePlacement::Create {
            logical_path: logical_path.clone(),
            tags: tags.clone(),
        },
        _ => messages::MaterializePlacement::Change,
    };
    if !placement::materialize_targets(configuration, database, file_id, &placement).is_empty() {
        let pending_fetches = pending_fetches.clone();
        let pull_scheduler = pull_scheduler.clone();
        let change_sender = change_sender.clone();
        let operations = operations.clone();
        let latest_version = Some((content_hash, size));
        // Spawned, never awaited: the pull ends by enqueueing a `Materialize`
        // onto this actor's own inbox (see `ReconcilePlacement`).
        tokio::spawn(async move {
            placement::fetch_and_materialize(
                &pending_fetches,
                &pull_scheduler,
                &change_sender,
                &operations,
                file_id,
                placement,
                latest_version,
            )
            .await;
        });
    }

    // Publish to UI-facing API subscribers so an open file view
    // picks up the new version on the device that *made* the edit.
    // Peers learn of it through the forwarded `Change` above, which
    // they ingest as `Ingest::Meta` and publish from the shared site
    // at the bottom of the loop; without this the originating device
    // is the only one that never refreshes. See `EVENT PUBLISHING`
    // on `handle_changes`.
    let _ = event_sender.send(change);
}
