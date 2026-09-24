//! Daemon **activity** crossing the port: how much work each actor has queued
//! or in hand right now.
//!
//! Distinct from [`Operation`](crate::Operation)s, which describe individual
//! user-visible units of work (a transfer, a reconcile). Activity is a set of
//! gauges and counters over the actors themselves, answering "is this daemon
//! still busy?" — the question a multi-daemon test asks before comparing two
//! catalogs, and the one a benchmark asks to know a phase has finished.
//!
//! Every value is a point-in-time sample. There is deliberately no
//! "sync complete" flag: reconciliation carries no completeness signal (see
//! AGENTS.md, "Reconciliation is additive, per-entry, and idempotent"), so the
//! most a daemon can honestly report is that it currently has nothing to do.
//! A caller that needs quiescence should observe [`ActivityInfo::is_idle`]
//! with unchanged [`InboxActivity::processed`] counters across two samples.
//!
//! The live gauges that produce these values stay in `tagsyd`; only the
//! serde-able snapshot lives here.

use serde::{Deserialize, Serialize};

/// A snapshot of one actor's inbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct InboxActivity {
    /// Messages waiting in the inbox, sampled when the actor last dequeued
    /// one. Always `0` while the actor is not [`busy`](Self::busy): an idle
    /// actor is blocked on an empty inbox.
    pub queued: u64,
    /// Whether the actor is in the middle of handling a message (or, for the
    /// sync-directory actor, its startup scan).
    pub busy: bool,
    /// Monotonic count of messages handled since startup. Two samples with the
    /// same count bracket an interval in which the actor did nothing.
    pub processed: u64,
}

impl InboxActivity {
    /// No message in hand and none waiting.
    pub fn is_idle(&self) -> bool {
        !self.busy && self.queued == 0
    }
}

/// A snapshot of every live peer session, summed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SessionActivity {
    /// Sessions in the middle of handling an inbound frame or a command
    /// (e.g. planning a received manifest).
    pub busy: u64,
    /// Frames queued to be written to peers, including any being written.
    pub outbound_queued: u64,
    /// Monotonic count of inbound frames and commands handled, across every
    /// session since startup.
    pub processed: u64,
}

impl SessionActivity {
    /// No session handling anything and nothing waiting to be sent.
    pub fn is_idle(&self) -> bool {
        self.busy == 0 && self.outbound_queued == 0
    }
}

/// A snapshot of everything a daemon currently has in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ActivityInfo {
    /// The `CatalogWriter` inbox — every catalog mutation passes through it.
    pub catalog: InboxActivity,
    /// The `SyncDirectories` inbox: commands from the catalog plus settled
    /// filesystem events from the watcher.
    pub sync_directories: InboxActivity,
    /// The peer sessions, one per connected socket.
    pub peer_sessions: SessionActivity,
    /// Raw filesystem events still inside the debounce window, not yet
    /// delivered to the sync-directory inbox.
    pub pending_filesystem_events: u64,
    /// Whether the sync-directory actor has finished its startup scan of every
    /// sync directory. Until it has, it answers no commands.
    pub initial_scan_complete: bool,
    /// Byte transfers admitted to the pull scheduler but waiting for a slot.
    pub pulls_queued: u64,
    /// Byte transfers currently running.
    pub pulls_running: u64,
}

impl ActivityInfo {
    /// Nothing queued, in hand, debouncing, or transferring, and the startup
    /// scan is done.
    ///
    /// A single idle sample can race a message that is just being sent; see
    /// the module docs for the two-sample quiescence check.
    pub fn is_idle(&self) -> bool {
        self.catalog.is_idle()
            && self.sync_directories.is_idle()
            && self.peer_sessions.is_idle()
            && self.pending_filesystem_events == 0
            && self.initial_scan_complete
            && self.pulls_queued == 0
            && self.pulls_running == 0
    }
}
