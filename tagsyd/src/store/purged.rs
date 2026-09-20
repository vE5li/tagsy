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
}
