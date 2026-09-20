//! Reconciling a peer's purge manifest (the set of permanently-purged file ids)
//! against ours.
//!
//! A purge is set-union across peers: presence of an id is the whole state, so
//! reconciliation is purely additive and idempotent. This module is the purge
//! counterpart of [`crate::peer::plan`] (files) and [`crate::peer::plan_tags`]
//! (tags), and is deliberately the simplest of the three — there is no clock to
//! order, no content to pull, and nothing to reconstruct.

use tagsy_core::FileId;

use crate::store::CatalogStore;

/// Build our local purge manifest: every file id in the permanent purge set.
/// The purge counterpart of [`crate::peer::plan::build_local_manifest`].
pub fn build_local_purge_manifest(database: &CatalogStore) -> Result<Vec<FileId>, String> {
    database
        .purged_ids()
        .map_err(|error| format!("purged_ids: {error:?}"))
}

/// Split a full purge manifest into batches, each destined for its own
/// `Sync::PurgeManifest` frame, so a large purge set never approaches a single
/// WebSocket message's size limit. Each entry is a bare id, so the batches are
/// tiny. Empty input yields no frames; `batch_size` is clamped to at least 1.
pub fn batch_purge_manifest(entries: Vec<FileId>, batch_size: usize) -> Vec<Vec<FileId>> {
    let batch_size = batch_size.max(1);
    entries
        .chunks(batch_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

/// Reconcile a peer's purge manifest against ours.
///
/// Returns the ids to apply a purge for locally: any id the peer has purged
/// that we either (a) do not yet hold as purged, or (b) hold as purged but
/// whose `files_v2` row still lingers (catalog-vs-purge-set drift — e.g. a
/// resurrection that slipped in before the reconciliation paths honored the
/// purge guard, or a partial earlier strip). Case (b) makes the purge
/// self-healing across upgrades: the writer's `FilePurged` handler re-strips a
/// lingering row.
///
/// Reconciliation stays per-entry and idempotent: an id we already hold purged
/// *and* have no row for is skipped, so a redelivered or duplicated manifest
/// for a converged catalog does no repeated work. A lookup error for one id
/// logs and skips that id (conservatively *not* purging on a read failure)
/// rather than aborting the whole batch.
pub fn plan_purge_sync(
    peer_name: &str,
    entries: Vec<FileId>,
    database: &CatalogStore,
) -> Vec<FileId> {
    let mut to_purge = Vec::new();
    for file_id in entries {
        let purged = match database.is_purged(file_id) {
            Ok(purged) => purged,
            Err(error) => {
                log::error!(
                    "Purge reconciliation lookup failed for {} from {peer_name}: {error:?}; \
                     skipping",
                    file_id.to_string()
                );
                continue;
            }
        };

        if !purged {
            log::debug!(
                "Applying peer purge for {} from {peer_name}",
                file_id.to_string()
            );
            to_purge.push(file_id);
            continue;
        }

        // Already purged locally: only act if the catalog has drifted (a row
        // still exists for a purged id), in which case re-emit so the writer
        // re-strips it. Otherwise there is nothing to do.
        match database.file_exists(file_id) {
            Ok(true) => {
                log::warn!(
                    "Purge for {} from {peer_name} already recorded but a catalog row lingers; \
                     re-applying to repair drift",
                    file_id.to_string()
                );
                to_purge.push(file_id);
            }
            Ok(false) => {}
            Err(error) => {
                log::error!(
                    "Purge reconciliation existence check failed for {} from {peer_name}: \
                     {error:?}; skipping",
                    file_id.to_string()
                );
            }
        }
    }
    to_purge
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::CatalogStore;

    fn memory_db() -> CatalogStore {
        CatalogStore::initialize(":memory:").expect("open in-memory db")
    }

    #[test]
    fn batch_splits_and_clamps() {
        let ids: Vec<FileId> = (0..5).map(|_| FileId::new()).collect();
        let frames = batch_purge_manifest(ids.clone(), 2);
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].len(), 2);
        assert_eq!(frames[2].len(), 1);

        // batch_size 0 is clamped to 1.
        let frames = batch_purge_manifest(ids, 0);
        assert_eq!(frames.len(), 5);

        assert!(batch_purge_manifest(Vec::new(), 4).is_empty());
    }

    #[test]
    fn plan_skips_ids_we_already_hold_with_clean_catalog() {
        let database = memory_db();
        let already = FileId::new();
        let fresh = FileId::new();
        database.record_purge(already).unwrap();

        // `already` is purged and has no catalog row, so it is skipped; only the
        // fresh id is emitted.
        let to_purge = plan_purge_sync("peer", vec![already, fresh], &database);
        assert_eq!(to_purge, vec![fresh]);
    }

    #[test]
    fn plan_reapplies_purge_when_catalog_row_lingers() {
        use tagsy_core::LogicalPath;

        let mut database = memory_db();
        let drifted = FileId::new();

        // Simulate drift: the id is in the purge set, yet a `files_v2` row
        // (and a version) still exists — as if a reconciliation path resurrected
        // it before honoring the purge guard.
        database.record_purge(drifted).unwrap();
        database
            .add_file(drifted, &LogicalPath::new("resurrected.bin"), 0)
            .unwrap();
        database.record_version(drifted, "hash", "peer", 1).unwrap();

        let to_purge = plan_purge_sync("peer", vec![drifted], &database);
        assert_eq!(
            to_purge,
            vec![drifted],
            "a purged id whose row lingers must be re-emitted so the writer re-strips it"
        );
    }
}
