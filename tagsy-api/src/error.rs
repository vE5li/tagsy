//! [`ApiError`] — the single serializable error the UI-facing API surfaces.
//!
//! Most `From` impls that map an internal daemon failure onto it stay in
//! `tagsyd` (they reference daemon-internal error types this crate deliberately
//! cannot see). The one exception is [`From<FileBytesError>`], whose source now
//! lives in `tagsy-core`: with both types below `tagsyd`, the orphan rule
//! places the conversion here.

use serde::{Deserialize, Serialize};
use tagsy_core::content::FileBytesError;

/// Errors surfaced to the UI.
///
/// A single serializable error type so the transport can carry one shape over
/// the wire, and — because every variant is either a unit or a `String` — one
/// that `flutter_rust_bridge` can mirror into a real Dart sealed class rather
/// than an opaque handle. Keep it that way: a variant carrying a foreign type
/// (as `Database(DatabaseError)` once did) forces Dart back to matching on
/// rendered text, which is silently wrong the moment the text changes.
///
/// The distinction the UI actually depends on is
/// [`UnknownId`](Self::UnknownId) versus
/// [`ContentUnavailable`](Self::ContentUnavailable): "this entity does not
/// exist" is permanent and should navigate away, while "nobody reachable has
/// these bytes" is transient and should offer to retry.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum ApiError {
    /// No such `FileId`/`TagId` in the catalog. Permanent: the entity is gone
    /// (or never existed), and retrying will not help.
    #[error("not found")]
    UnknownId,
    /// The entity exists, but its bytes could not be obtained: no reachable
    /// peer currently holds the requested content hash. Transient — a retry
    /// once the holder is online will succeed.
    #[error("content unavailable: no reachable device holds it")]
    ContentUnavailable,
    /// A resolution term — an id prefix or a name/path — matched more than one
    /// row, so it could not be resolved to a single id. Carries the term.
    #[error("ambiguous term '{0}': matches multiple entries")]
    AmbiguousId(String),
    /// A caller-supplied argument was invalid (e.g. empty tag name).
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// `purge-broken` was invoked on a node with no Universal sync directory.
    ///
    /// Purge is permanent and irreversible, and its "broken" verdict is
    /// "cataloged but the bytes are absent locally". That verdict is only
    /// authoritative on a node that is *supposed* to hold every file's bytes —
    /// i.e. one with a Universal sync directory. Without one, "missing locally"
    /// is expected for most files and says nothing about whether they are
    /// broken, so the command refuses to run rather than risk permanently
    /// erasing recoverable files.
    #[error(
        "purge-broken requires a Universal sync directory: without one, a file's bytes being \
         absent locally does not mean it is broken"
    )]
    PurgeRequiresUniversalDirectory,
    /// IPC-only: socket/protocol failure. Never produced in-process.
    #[error("transport error: {0}")]
    Transport(String),
    /// An unexpected internal failure (e.g. a change could not be enqueued
    /// because the runtime is shutting down).
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<FileBytesError> for ApiError {
    fn from(error: FileBytesError) -> Self {
        // Reading a caller-supplied local path (an upload source or an edit
        // result) failed. That is a failure of the client's side of the
        // exchange rather than of the catalog, so it maps to `Transport`.
        // `FileBytesError`'s own `Display` already names the path.
        ApiError::Transport(error.to_string())
    }
}
