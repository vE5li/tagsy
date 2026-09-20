//! The `purged_files_v1` table: the permanent set of file ids purged as broken.
//!
//! A purge is a terminal, irreversible fact that takes absolute priority over
//! every other catalog record about its id (see [`CatalogStore::is_purged`],
//! the enforcement predicate the writer consults before applying any file
//! fact). The merge rule across peers is set-union — presence of the id is the
//! whole state — so the row carries only the id and no clock.
//!
//! [`strip_purged_file`] is the hard-delete that removes an id's remaining
//! catalog rows once it is purged; the purge row itself is never removed, so a
//! later stale re-announcement of that id is rejected by `is_purged`.
//!
//! [`strip_purged_file`]: CatalogStore::strip_purged_file

use tagsy_core::FileId;

use super::CatalogStore;
use super::previews::delete_previews_for;
use super::types::DatabaseError;

impl CatalogStore {
    /// Record a purge for `file_id`. Idempotent: re-purging an already-purged
    /// id is a no-op. Returns `true` iff the id was newly inserted (so the
    /// caller can skip re-running the strip/fan-out/forward for a purge that
    /// has already converged).
    pub fn record_purge(&self, file_id: FileId) -> Result<bool, DatabaseError> {
        let affected = self.connection.execute(
            "INSERT OR IGNORE INTO purged_files_v1 (file_id) VALUES (?1)",
            [file_id],
        )?;
        Ok(affected > 0)
    }

    /// The enforcement predicate: is this id in the permanent purge set? The
    /// writer consults this before applying any file fact (local or peer), and
    /// drops the fact if it returns `true`.
    pub fn is_purged(&self, file_id: FileId) -> Result<bool, DatabaseError> {
        let count: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM purged_files_v1 WHERE file_id = ?1",
            [file_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Every purged id. Used to build the purge manifest advertised to peers so
    /// a peer offline at purge time learns of it on reconnect.
    pub fn purged_ids(&self) -> Result<Vec<FileId>, DatabaseError> {
        let mut statement = self
            .connection
            .prepare("SELECT file_id FROM purged_files_v1")?;
        let ids = statement
            .query_map([], |row| row.get::<_, FileId>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ids)
    }

    /// Hard-delete every catalog row that references a purged file, across the
    /// tables that carry its metadata. Unlike a tombstone (which deliberately
    /// keeps the row and its version history), a purge removes everything: the
    /// authoritative "this existed and is gone" record is now the purge row
    /// itself.
    ///
    /// Removed here: the `files_v2` row, its full `file_versions_v1` history,
    /// its cached previews, and its file-tag relationships in `entries_v1`
    /// (edges with `type = 0` and `target_id = file_id`). The on-disk bytes are
    /// dropped separately by the writer fanning out `RemoveFile` to sync
    /// directories.
    pub fn strip_purged_file(&self, file_id: FileId) -> Result<(), DatabaseError> {
        let transaction = self.connection.unchecked_transaction()?;
        transaction.execute("DELETE FROM files_v2 WHERE id = ?1", [file_id])?;
        transaction.execute("DELETE FROM file_versions_v1 WHERE file_id = ?1", [file_id])?;
        // File-tag edges: `type = 0` and the file id sits in `target_id`.
        transaction.execute(
            "DELETE FROM entries_v1 WHERE target_id = ?1 AND type = 0",
            [file_id.to_string()],
        )?;
        delete_previews_for(&transaction, file_id)?;
        transaction.commit()?;
        Ok(())
    }

    /// Startup self-heal: strip every catalog row whose id is in
    /// `purged_files_v1`. Returns how many `files_v2` rows were removed.
    ///
    /// A purge is terminal, so a purged id must never carry catalog rows. This
    /// repairs any drift where a row exists for a purged id — the state left
    /// behind by an older build whose manifest-reconciliation paths
    /// (`catalog_file` / `catalog_tombstone`) re-materialized a purged file
    /// because they did not yet consult the purge set. Because the purge set is
    /// append-only (ids are never removed), every device that ever applied a
    /// purge still knows the id here, so this converges the catalog with **no
    /// dependence on peers** — apply the fix, restart, and the drift is gone.
    ///
    /// It is a set-based no-op on a clean catalog (nothing matches), so it is
    /// cheap to run unconditionally on every startup. Runs in a single
    /// transaction across all four tables.
    pub fn reconcile_purged_files(&self) -> Result<usize, DatabaseError> {
        let transaction = self.connection.unchecked_transaction()?;
        // Order does not matter (no FKs), but delete the dependent rows first
        // for clarity. Each targets only rows whose id/target is purged.
        transaction.execute(
            "DELETE FROM file_versions_v1 WHERE file_id IN (SELECT file_id FROM purged_files_v1)",
            [],
        )?;
        transaction.execute(
            "DELETE FROM previews_v1 WHERE file_id IN (SELECT file_id FROM purged_files_v1)",
            [],
        )?;
        // File-tag edges live in `entries_v1` with `type = 0`; `target_id`
        // stores the file id as text, matching `purged_files_v1.file_id`.
        transaction.execute(
            "DELETE FROM entries_v1 WHERE type = 0 AND target_id IN (SELECT file_id FROM \
             purged_files_v1)",
            [],
        )?;
        let removed = transaction.execute(
            "DELETE FROM files_v2 WHERE id IN (SELECT file_id FROM purged_files_v1)",
            [],
        )?;
        transaction.commit()?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::LogicalPath;

    use super::*;
    use crate::store::fixtures::memory_db;

    #[test]
    fn record_purge_is_idempotent() {
        let database = memory_db();
        let file_id = FileId::new();

        assert!(!database.is_purged(file_id).unwrap());
        assert!(
            database.record_purge(file_id).unwrap(),
            "first insert is new"
        );
        assert!(database.is_purged(file_id).unwrap());
        assert!(
            !database.record_purge(file_id).unwrap(),
            "re-purge is a no-op"
        );
    }

    #[test]
    fn purged_ids_lists_every_purge() {
        let database = memory_db();
        let a = FileId::new();
        let b = FileId::new();
        database.record_purge(a).unwrap();
        database.record_purge(b).unwrap();

        let mut ids = database.purged_ids().unwrap();
        ids.sort();
        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!(ids, expected);
    }

    #[test]
    fn strip_removes_file_versions_and_survives_purge_row() {
        let mut database = memory_db();
        let file_id = FileId::new();
        let logical_path = LogicalPath::new("broken.bin");
        database.add_file(file_id, &logical_path, 0).unwrap();
        database
            .record_version(file_id, "hash", "local", 3)
            .unwrap();

        assert!(database.file_exists(file_id).unwrap());

        database.record_purge(file_id).unwrap();
        database.strip_purged_file(file_id).unwrap();

        assert!(!database.file_exists(file_id).unwrap());
        assert!(database.latest_version(file_id).unwrap().is_none());
        // The purge row itself survives the strip so a later re-announcement is
        // rejected.
        assert!(database.is_purged(file_id).unwrap());
    }

    #[test]
    fn reconcile_strips_drifted_rows_and_leaves_clean_catalog_untouched() {
        let mut database = memory_db();

        // A drifted purged file: purged, yet a row + version still exist.
        let drifted = FileId::new();
        database.record_purge(drifted).unwrap();
        database
            .add_file(drifted, &LogicalPath::new("resurrected.bin"), 0)
            .unwrap();
        database.record_version(drifted, "h", "peer", 1).unwrap();

        // A normal live file that is NOT purged must survive reconciliation.
        let live = FileId::new();
        database
            .add_file(live, &LogicalPath::new("keep.bin"), 0)
            .unwrap();
        database.record_version(live, "h2", "local", 2).unwrap();

        let removed = database.reconcile_purged_files().unwrap();
        assert_eq!(removed, 1, "exactly the drifted file's row is stripped");

        assert!(!database.file_exists(drifted).unwrap());
        assert!(database.latest_version(drifted).unwrap().is_none());
        assert!(
            database.is_purged(drifted).unwrap(),
            "the purge row survives"
        );

        assert!(database.file_exists(live).unwrap(), "live file untouched");

        // Idempotent: a second run on a now-clean catalog removes nothing.
        assert_eq!(database.reconcile_purged_files().unwrap(), 0);
    }
}
