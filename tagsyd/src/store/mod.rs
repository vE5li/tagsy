//! The catalog store: every table the daemon persists, and the only type that
//! can write them.
//!
//! [`CatalogStore`] owns one SQLite connection over the main catalog —
//! files, tags, the relationships between them, version history and the
//! preview cache. It is split here one module per table, with the graph
//! walkers, search and short-id resolution as their own kernels on top:
//!
//! | Module | Owns |
//! |---|---|
//! | `schema` | every `CREATE TABLE` / migration, for both databases |
//! | `types` | the shared value types and [`DatabaseError`] |
//! | `files` | `files_v2` — existence, logical path, tombstone LWW, manifests |
//! | `tags` | `tags_v2` — tag definitions and name/pattern lookup |
//! | `entries` | `entries_v1` — the tag graph and its traversals |
//! | `versions` | `file_versions_v1` — the append-only content log |
//! | `previews` | `previews_v1` — the hash-keyed preview cache |
//! | `purged` | `purged_files_v1` — the permanent purge set |
//! | `query` | search, composed from the modules above |
//! | `short_id` | shortest-unique-prefix ids and their resolution |
//! | `directory_index` | the separate per-sync-directory `(file_id, path)` map |
//!
//! `CatalogStore` is `Send + !Sync` (a `rusqlite::Connection` is), so a
//! `&CatalogStore` must never be held across an `.await`. Readers open their
//! own short-lived handle; the daemon's catalog actor holds the only
//! `&mut` one.

use std::path::Path;

use rusqlite::Connection;

mod directory_index;
mod entries;
mod files;
mod previews;
mod purged;
mod query;
mod schema;
mod short_id;
mod tags;
mod types;
mod versions;

#[cfg(test)]
mod fixtures;

pub use directory_index::{DirectoryIndex, SyncDirectoryFile};
pub use files::ManifestRow;
pub use query::{QueryTerm, TextPattern};
pub use short_id::normalize_id_prefix;
// The read-filter enums and the `Tag` row cross the port and live in
// `tagsy-api`; re-exported here so the many `crate::store::{Tag, DeletedRule,
// SubtagRule}` call sites keep resolving.
pub use tagsy_api::{DeletedRule, SubtagRule, Tag};
pub use types::{DatabaseError, DeletionState, FileVersion};
pub use versions::VersionHistory;

/// A handle on the main catalog database.
///
/// The `connection` field is private to this module tree, which is what makes
/// the per-table modules below the only code that can issue SQL against the
/// catalog.
#[derive(Debug)]
pub struct CatalogStore {
    connection: Connection,
}

/// Open a connection to one of the daemon's SQLite databases, configured the
/// way every connection to them must be.
///
/// - `journal_mode = WAL`: readers (API reads, peer sessions) never block the
///   single writer, and a commit is one append to the log rather than a
///   rollback-journal round trip. The mode is persistent in the file; setting
///   it again is a no-op.
/// - `synchronous = NORMAL`: a commit does not fsync; the log is synced at
///   checkpoints. The database is never corrupted, and a daemon crash or kill
///   loses nothing. Power loss or an OS crash can roll back the commits since
///   the last checkpoint — recoverable by design, since peers re-advertise
///   everything on reconnect and the startup scan re-detects on-disk files. The
///   default (`FULL`, with a rollback journal) cost several fsyncs per commit,
///   which made every write path ~50× slower (see the scale benchmark in
///   `tests/multi_daemon/bench.rs`).
pub(crate) fn open_connection(
    database_path: impl AsRef<Path>,
) -> Result<Connection, DatabaseError> {
    let connection =
        Connection::open(database_path).map_err(|_| DatabaseError::UnableToOpenOrCreate)?;
    connection.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
    Ok(connection)
}

impl CatalogStore {
    pub fn initialize(database_path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let connection = open_connection(database_path)?;

        // Run migrations here. Each `migrate_*_to_vN` runs before the matching
        // `create_*_vN` so a restored older-version backup is walked forward
        // through every intermediate version on startup (see AGENTS.md).
        schema::migrate_files_to_v2(&connection)?;
        schema::migrate_tags_to_v2(&connection)?;

        schema::create_files_v2(&connection)?;
        schema::create_tags_v2(&connection)?;
        schema::create_entries_v1(&connection)?;
        schema::create_file_versions_v1(&connection)?;
        schema::create_previews_v1(&connection)?;
        schema::create_purged_files_v1(&connection)?;

        let store = Self { connection };

        // Startup self-heal: a purge is terminal, so no purged id may carry
        // catalog rows. Strip any drift (a row that exists for a purged id) left
        // by an older build whose reconciliation paths re-materialized purged
        // files. Set-based and a no-op on a clean catalog; the purge set is
        // append-only, so this converges every device on restart without needing
        // a peer to re-advertise the purge.
        match store.reconcile_purged_files() {
            Ok(0) => {}
            Ok(removed) => log::warn!(
                "Startup purge reconciliation: stripped {removed} catalog row(s) that lingered \
                 for purged files"
            ),
            Err(error) => log::error!(
                "Startup purge reconciliation failed: {error:?}; purged files may retain catalog \
                 rows until the next peer reconcile"
            ),
        }

        Ok(store)
    }

    /// Write a transactionally consistent, defragmented copy of this database
    /// to `dest` via SQLite's `VACUUM INTO`. Unlike a plain file copy, this is
    /// safe against a live connection: SQLite serializes it against any
    /// in-flight write, so the snapshot never captures a torn page. `dest` must
    /// not already exist. Used by the backup builder to stage the main catalog.
    pub fn vacuum_into(&self, dest: &Path) -> Result<(), DatabaseError> {
        self.connection
            .execute("VACUUM INTO ?1", [dest.to_string_lossy().as_ref()])?;
        Ok(())
    }
}
