//! The self-write echo-suppression tracker.
//!
//! When the daemon materializes bytes to disk it records the write here so the
//! filesystem-watcher event it will inevitably see for that same path is
//! recognised as self-caused and dropped, rather than re-ingested. Its own
//! `TODO: Make this a more robust messaging framework` (below) flags this as a
//! stopgap.

use std::path::{Path, PathBuf};

use super::SyncDirectories;

/// A record that the daemon *itself* just wrote to a given on-disk path, laid
/// down at the moment of the write in `handle_command` (or during placement
/// reconciliation) and consulted in `handle_event` to decide whether an
/// incoming watcher event merely reflects that self-write — in which case it
/// must be ignored — or is a genuine user action that must be processed.
pub(super) struct SelfWrite {
    /// BLAKE3 hash of the bytes the daemon materialized at this path, or `None`
    /// for a self-caused removal / pure rename (no content to match on).
    content_hash: Option<String>,
}

impl SyncDirectories {
    /// Record that the daemon itself just wrote `path`, so the resulting
    /// watcher event(s) are recognized as self-caused and ignored.
    /// `content_hash` is the hash of the bytes materialized there (or
    /// `None` for a removal or a pure rename). See [`SelfWrite`].
    pub(super) fn record_self_write(&self, path: PathBuf, content_hash: Option<String>) {
        self.self_writes
            .borrow_mut()
            .insert(path, SelfWrite { content_hash });
    }

    /// Decide whether a watcher event for `path` reflects a self-write and, if
    /// so, consume the record (first-match wins).
    ///
    /// - Ingest events (Create / move-in): any pending self-write for the path
    ///   is our own materialization; pass `observed_hash: None` to match on
    ///   presence alone.
    /// - `Modify`: pass the freshly hashed on-disk content as `observed_hash`.
    ///   The event is suppressed only when it equals the hash we materialized,
    ///   so a genuine user edit (different hash) is *not* swallowed. A pending
    ///   record whose hash does not match is left in place.
    /// - `Remove`: pass `observed_hash: None`; a presence match suppresses it.
    pub(super) fn take_matching_self_write(
        &self,
        path: &Path,
        observed_hash: Option<&str>,
    ) -> bool {
        let mut self_writes = self.self_writes.borrow_mut();
        let Some(record) = self_writes.get(path) else {
            return false;
        };

        // For a `Modify` we only suppress when the on-disk content matches what
        // we wrote. If the caller supplies a hash and the record has one, they
        // must agree; a mismatch means a real edit landed on top of (or instead
        // of) our write, so let it through and keep the record for the event we
        // actually expected.
        if let (Some(observed), Some(recorded)) = (observed_hash, record.content_hash.as_deref())
            && observed != recorded
        {
            return false;
        }

        self_writes.remove(path);
        true
    }
}
