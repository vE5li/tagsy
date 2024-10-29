//! Shared value types and the error enum every store operation returns.
//!
//! Nothing here touches SQL; these are the vocabulary the per-table modules
//! speak in.

use serde::{Deserialize, Serialize};
use tagsy_api::DeletedRule;
use tagsy_core::FileId;

/// A single recorded version of a file's content.
///
/// Rows in `file_versions` are append-only. The `version_number` is a per-file
/// monotonically increasing counter (starts at 1) that defines ordering between
/// versions of the same file. `observed_at` is the unix-millis wall-clock time
/// at which we recorded the version and is metadata only — do not use it for
/// ordering.
#[derive(Debug, Clone)]
pub struct FileVersion {
    pub file_id: FileId,
    pub content_hash: String,
    pub observed_at: i64,
    pub version_number: i64,
    pub origin: String,
    /// The version's content size in bytes. Read from disk (or the in-memory
    /// buffer) at the same time the content hash is computed.
    pub size: i64,
}

/// A file's soft-delete tombstone state: the materialized `deleted` flag plus
/// the two wall-clocks that (together with the latest version `observed_at`)
/// decide it under three-way last-writer-wins.
///
/// The file is live iff `max(latest observed_at, restored_at) > deleted_at`;
/// `deleted` caches that decision so reads keep a simple `WHERE deleted = 0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletionState {
    pub deleted: bool,
    /// Unix-millis of the last delete (0 if never deleted).
    pub deleted_at: i64,
    /// Unix-millis of the last explicit restore (0 if never restored).
    pub restored_at: i64,
}

// `SubtagRule` and `DeletedRule` are the read-filter enums; they cross the port
// and so now live in `tagsy-api` (re-exported from `store::mod`). Their
// SQL-fragment helpers stay here — the store owns the SQL — as free functions,
// since inherent methods can only be defined in the type's own crate.

/// SQL fragment appended after other `WHERE` clauses to enforce `rule`.
/// `Exclude` yields `" AND deleted = 0"`; `Include` yields `""`.
///
/// Callers embed this into a query that already has at least one `WHERE`
/// clause so the leading `AND` is well-formed. When there is no prior clause,
/// use [`where_deleted_clause`] instead.
pub(super) fn and_deleted_clause(rule: DeletedRule) -> &'static str {
    match rule {
        DeletedRule::Exclude => " AND deleted = 0",
        DeletedRule::Include => "",
    }
}

/// SQL fragment for a standalone `WHERE`: `" WHERE deleted = 0"` under
/// `Exclude`, empty under `Include`.
pub(super) fn where_deleted_clause(rule: DeletedRule) -> &'static str {
    match rule {
        DeletedRule::Exclude => " WHERE deleted = 0",
        DeletedRule::Include => "",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum DatabaseError {
    #[error("unable to open or create database")]
    UnableToOpenOrCreate,
    #[error("failed to execute database command")]
    FailedToExecuteCommand,
    #[error("file path is not valid UTF-8")]
    NonUtf8FilePath,
    #[error("file not found")]
    MissingFile,
    #[error("tag not found")]
    MissingTag,
    #[error("invalid tag name")]
    InvalidTagName,
    #[error("invalid color")]
    InvalidColor,
    #[error("a tag cannot be its own subtag")]
    CantTagItself,
    /// A short-id prefix matched more than one row, so it cannot be resolved to
    /// a single id. Carries the ambiguous prefix that was queried.
    #[error("ambiguous id prefix '{0}': matches multiple rows")]
    AmbiguousIdPrefix(String),
    /// A raw failure from the underlying SQLite driver.
    ///
    /// `message` is the rendered `rusqlite::Error` and is the only part that
    /// crosses the wire, which keeps the whole enum unconditionally
    /// serializable. `cause` keeps the original error for in-process callers
    /// (so [`std::error::Error::source`] chains and the cause can be downcast);
    /// it is skipped by serde and is therefore `None` on any deserialized
    /// value.
    ///
    /// `Arc`-wrapped because `rusqlite::Error` is not `Clone` but
    /// `DatabaseError` is (transitively through `ApiError`, which we keep
    /// clonable for the UI/IPC layer).
    #[error("sqlite error: {message}")]
    Sqlite {
        message: String,
        #[serde(skip)]
        #[source]
        cause: Option<std::sync::Arc<rusqlite::Error>>,
    },
}

impl From<rusqlite::Error> for DatabaseError {
    fn from(error: rusqlite::Error) -> Self {
        DatabaseError::Sqlite {
            message: error.to_string(),
            cause: Some(std::sync::Arc::new(error)),
        }
    }
}
