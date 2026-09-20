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
/// Returns the ids the peer has purged that we do not already hold as purged —
/// i.e. the new purges to apply locally. Reconciliation is per-entry and
/// idempotent: an id we already have is skipped, so a redelivered or duplicated
/// manifest converges with no repeated work. A lookup error for one id logs and
/// skips that id (conservatively *not* purging on a read failure) rather than
/// aborting the whole batch.
pub fn plan_purge_sync(
    peer_name: &str,
    entries: Vec<FileId>,
    database: &CatalogStore,
) -> Vec<FileId> {
    let mut to_purge = Vec::new();
    for file_id in entries {
        match database.is_purged(file_id) {
            Ok(true) => {}
            Ok(false) => {
                log::debug!(
                    "Applying peer purge for {} from {peer_name}",
                    file_id.to_string()
                );
                to_purge.push(file_id);
            }
            Err(error) => {
                log::error!(
                    "Purge reconciliation lookup failed for {} from {peer_name}: {error:?}; \
                     skipping",
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
    fn plan_skips_ids_we_already_hold() {
        let database = memory_db();
        let already = FileId::new();
        let fresh = FileId::new();
        database.record_purge(already).unwrap();

        let to_purge = plan_purge_sync("peer", vec![already, fresh], &database);
        assert_eq!(to_purge, vec![fresh]);
    }
}
