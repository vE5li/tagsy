//! The self-write echo-suppression tracker.
//!
//! When the daemon changes a sync directory itself (placing received bytes,
//! deleting, renaming) it records what the watcher is about to report, so that
//! event is recognized as self-caused and dropped rather than re-ingested as a
//! user change.
//!
//! Records are **typed and queued per path**. One path can see several
//! daemon writes inside a single debounce window — a delete followed by the
//! re-creation a newer edit triggers — and the watcher then reports a `Remove`
//! followed by a `Create`. Each event consumes the oldest record of *its own
//! kind*: a `Remove` can never swallow the record an arrival is waiting for.
//! (With one untyped record per path, the second write overwrote the first,
//! the `Remove` consumed it, and the `Create` was ingested as a brand-new user
//! file — duplicating the file under a fresh id.)
//!
//! Records the watcher never reports — the debouncer cancels a create followed
//! by a remove — would otherwise linger and swallow a later genuine user event
//! on that path, so each record expires after [`SELF_WRITE_TTL`].

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How long a self-write record waits for its watcher event.
///
/// Deliberately generous. Events reach the actor in order, but can queue
/// behind a long backlog (a large startup scan, a bulk import). Expiring a
/// record too early turns the daemon's own write into a duplicate file —
/// unrecoverable — whereas keeping a stale one too long at worst swallows one
/// user event on that path, which the startup scan re-detects by hash.
pub(super) const SELF_WRITE_TTL: Duration = Duration::from_secs(600);

/// What the daemon expects the watcher to report for a path it just touched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Expected {
    /// The path disappears: a delete, or the source side of a move.
    Removal,
    /// The path (re)appears or changes, holding these bytes. `None` when the
    /// bytes are unchanged and unhashed (the destination of a rename).
    Content(Option<String>),
}

/// What the watcher reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Observed<'a> {
    Removal,
    /// A create or move-in: matched by presence alone.
    Arrival,
    /// An in-place modification, with the freshly hashed on-disk content: only
    /// suppressed when it equals what the daemon wrote, so a user edit landing
    /// on top of a daemon write still goes through.
    Modification(&'a str),
}

#[derive(Debug)]
struct Record {
    expected: Expected,
    recorded_at: Instant,
}

impl Record {
    fn matches(&self, observed: Observed<'_>) -> bool {
        match (&self.expected, observed) {
            (Expected::Removal, Observed::Removal) => true,
            (Expected::Content(_), Observed::Arrival) => true,
            (Expected::Content(None), Observed::Modification(_)) => true,
            (Expected::Content(Some(recorded)), Observed::Modification(observed)) => {
                recorded == observed
            }
            _ => false,
        }
    }
}

/// Pending self-write records, queued per path in the order written.
#[derive(Debug, Default)]
pub(super) struct SelfWrites {
    records: HashMap<PathBuf, VecDeque<Record>>,
}

impl SelfWrites {
    /// Record that the daemon just changed `path`, expecting `expected`.
    pub(super) fn record(&mut self, path: PathBuf, expected: Expected) {
        self.record_at(path, expected, Instant::now());
    }

    fn record_at(&mut self, path: PathBuf, expected: Expected, now: Instant) {
        self.records.entry(path).or_default().push_back(Record {
            expected,
            recorded_at: now,
        });
    }

    /// If `observed` on `path` is one of our own writes, consume the oldest
    /// matching record and return `true`.
    pub(super) fn take(&mut self, path: &Path, observed: Observed<'_>) -> bool {
        self.take_at(path, observed, Instant::now())
    }

    fn take_at(&mut self, path: &Path, observed: Observed<'_>, now: Instant) -> bool {
        let Some(queue) = self.records.get_mut(path) else {
            return false;
        };
        queue.retain(|record| now.duration_since(record.recorded_at) < SELF_WRITE_TTL);
        let position = queue.iter().position(|record| record.matches(observed));
        if let Some(position) = position {
            queue.remove(position);
        }
        if queue.is_empty() {
            self.records.remove(path);
        }
        position.is_some()
    }
}

impl super::SyncDirectories {
    /// Record that the daemon itself just changed `path`, so the watcher
    /// event it causes is recognized and ignored. See [`SelfWrites`].
    pub(super) fn record_self_write(&self, path: PathBuf, expected: Expected) {
        self.self_writes.borrow_mut().record(path, expected);
    }

    /// Whether a watcher event reflects one of our own writes (consuming the
    /// record if so). See [`SelfWrites::take`].
    pub(super) fn take_matching_self_write(&self, path: &Path, observed: Observed<'_>) -> bool {
        self.self_writes.borrow_mut().take(path, observed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path() -> PathBuf {
        PathBuf::from("/sync/doc.txt")
    }

    /// The regression: a delete then a re-create of the same path before the
    /// watcher reports either. Each event must consume its own record.
    #[test]
    fn removal_then_recreation_each_match_their_own_event() {
        let mut writes = SelfWrites::default();
        writes.record(path(), Expected::Removal);
        writes.record(path(), Expected::Content(Some("h2".to_owned())));

        assert!(writes.take(&path(), Observed::Removal));
        assert!(writes.take(&path(), Observed::Arrival));
        // Both consumed: a later user event goes through.
        assert!(!writes.take(&path(), Observed::Arrival));
    }

    #[test]
    fn a_removal_never_consumes_an_arrival_record() {
        let mut writes = SelfWrites::default();
        writes.record(path(), Expected::Content(None));
        assert!(!writes.take(&path(), Observed::Removal));
        assert!(writes.take(&path(), Observed::Arrival));
    }

    #[test]
    fn modification_matches_only_the_written_content() {
        let mut writes = SelfWrites::default();
        writes.record(path(), Expected::Content(Some("ours".to_owned())));
        assert!(!writes.take(&path(), Observed::Modification("user-edit")));
        assert!(writes.take(&path(), Observed::Modification("ours")));
    }

    #[test]
    fn records_expire() {
        let mut writes = SelfWrites::default();
        let then = Instant::now();
        writes.record_at(path(), Expected::Removal, then);
        assert!(!writes.take_at(&path(), Observed::Removal, then + SELF_WRITE_TTL));
        assert!(writes.records.is_empty());
    }
}
