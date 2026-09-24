//! Live activity gauges behind
//! [`Backend::activity`](tagsy_api::Backend::activity).
//!
//! Each state-owning actor updates its own [`InboxGauge`] as it handles
//! messages; [`Activity`] bundles the gauges with the pull scheduler so the API
//! can sample everything at once into an [`ActivityInfo`]. Gauges are plain
//! atomics: the actors never wait on them, and a sample is best-effort by
//! nature (see the `tagsy_api::activity` module docs for how callers turn
//! samples into a quiescence check).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tagsy_api::{ActivityInfo, InboxActivity, SessionActivity};

use crate::peer::pull_scheduler::PullScheduler;

/// One actor's inbox gauge. Cheap to clone (shared state behind an `Arc`).
#[derive(Clone, Default)]
pub struct InboxGauge {
    state: Arc<InboxGaugeState>,
}

#[derive(Default)]
struct InboxGaugeState {
    queued: AtomicU64,
    busy: AtomicBool,
    processed: AtomicU64,
}

impl InboxGauge {
    /// Mark the actor busy with one message, recording how many are still
    /// `queued` behind it. The returned guard marks it idle again, and counts
    /// the message as processed, when dropped — so every exit from a handler
    /// (including `continue`) is accounted for.
    pub fn begin(&self, queued: usize) -> BusyGuard<'_> {
        self.state.queued.store(queued as u64, Ordering::Relaxed);
        self.state.busy.store(true, Ordering::Release);
        BusyGuard { gauge: self }
    }

    pub fn snapshot(&self) -> InboxActivity {
        let busy = self.state.busy.load(Ordering::Acquire);
        InboxActivity {
            // An idle actor is blocked on an empty inbox; a stale `queued`
            // from its last dequeue would be misleading.
            queued: if busy {
                self.state.queued.load(Ordering::Relaxed)
            } else {
                0
            },
            busy,
            processed: self.state.processed.load(Ordering::Relaxed),
        }
    }
}

/// Held by an actor for the duration of one message. See [`InboxGauge::begin`].
pub struct BusyGuard<'a> {
    gauge: &'a InboxGauge,
}

impl Drop for BusyGuard<'_> {
    fn drop(&mut self) {
        let state = &self.gauge.state;
        state.processed.fetch_add(1, Ordering::Relaxed);
        state.queued.store(0, Ordering::Relaxed);
        state.busy.store(false, Ordering::Release);
    }
}

/// Gauges shared by every peer session (one per socket), summed.
#[derive(Clone, Default)]
pub struct SessionGauge {
    state: Arc<SessionGaugeState>,
}

#[derive(Default)]
struct SessionGaugeState {
    busy: AtomicU64,
    outbound_queued: AtomicU64,
    processed: AtomicU64,
}

impl SessionGauge {
    /// Mark one session busy with one inbound frame or command until the
    /// guard drops.
    pub fn begin(&self) -> SessionBusyGuard<'_> {
        self.state.busy.fetch_add(1, Ordering::AcqRel);
        SessionBusyGuard { gauge: self }
    }

    /// A per-session reporter for that session's outbound queue length; it
    /// withdraws whatever it last reported when dropped (session teardown).
    pub fn outbound(&self) -> OutboundReporter {
        OutboundReporter {
            gauge: self.clone(),
            reported: 0,
        }
    }

    pub fn snapshot(&self) -> SessionActivity {
        SessionActivity {
            busy: self.state.busy.load(Ordering::Acquire),
            outbound_queued: self.state.outbound_queued.load(Ordering::Acquire),
            processed: self.state.processed.load(Ordering::Relaxed),
        }
    }
}

/// Held by a session for the duration of one inbound frame or command.
pub struct SessionBusyGuard<'a> {
    gauge: &'a SessionGauge,
}

impl Drop for SessionBusyGuard<'_> {
    fn drop(&mut self) {
        let state = &self.gauge.state;
        state.processed.fetch_add(1, Ordering::Relaxed);
        state.busy.fetch_sub(1, Ordering::AcqRel);
    }
}

/// One session's contribution to [`SessionActivity::outbound_queued`].
pub struct OutboundReporter {
    gauge: SessionGauge,
    reported: u64,
}

impl OutboundReporter {
    /// This session now has `queued` frames waiting (or being written).
    pub fn report(&mut self, queued: usize) {
        let queued = queued as u64;
        let total = &self.gauge.state.outbound_queued;
        if queued > self.reported {
            total.fetch_add(queued - self.reported, Ordering::AcqRel);
        } else {
            total.fetch_sub(self.reported - queued, Ordering::AcqRel);
        }
        self.reported = queued;
    }
}

impl Drop for OutboundReporter {
    fn drop(&mut self) {
        self.report(0);
    }
}

/// Gauges owned by the `SyncDirectories` actor and its watcher.
#[derive(Clone, Default)]
pub struct SyncDirectoryGauges {
    pub inbox: InboxGauge,
    /// Raw filesystem events inside the debounce window. Written by the
    /// watcher under the debouncer's lock after every push and extraction.
    pub pending_filesystem_events: Arc<AtomicU64>,
    /// Set once the startup scan (`run_initial_sync`) has finished.
    pub initial_scan_complete: Arc<AtomicBool>,
}

/// Every activity gauge in one daemon. Cheap to clone (one `Arc`).
#[derive(Clone)]
pub struct Activity {
    inner: Arc<Inner>,
}

struct Inner {
    catalog: InboxGauge,
    sync_directories: SyncDirectoryGauges,
    peer_sessions: SessionGauge,
    pulls: PullScheduler,
}

impl Activity {
    pub fn new(pulls: PullScheduler) -> Self {
        Self {
            inner: Arc::new(Inner {
                catalog: InboxGauge::default(),
                sync_directories: SyncDirectoryGauges::default(),
                peer_sessions: SessionGauge::default(),
                pulls,
            }),
        }
    }

    /// The gauge `CatalogWriter` updates.
    pub fn catalog(&self) -> &InboxGauge {
        &self.inner.catalog
    }

    /// The gauges `SyncDirectories` and its watcher update.
    pub fn sync_directories(&self) -> &SyncDirectoryGauges {
        &self.inner.sync_directories
    }

    /// The gauge every peer session updates.
    pub fn peer_sessions(&self) -> &SessionGauge {
        &self.inner.peer_sessions
    }

    pub async fn snapshot(&self) -> ActivityInfo {
        let inner = &self.inner;
        let (pulls_queued, pulls_running) = inner.pulls.load().await;
        ActivityInfo {
            catalog: inner.catalog.snapshot(),
            sync_directories: inner.sync_directories.inbox.snapshot(),
            peer_sessions: inner.peer_sessions.snapshot(),
            pending_filesystem_events: inner
                .sync_directories
                .pending_filesystem_events
                .load(Ordering::Relaxed),
            initial_scan_complete: inner
                .sync_directories
                .initial_scan_complete
                .load(Ordering::Acquire),
            pulls_queued,
            pulls_running,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_gauge_sums_sessions_and_withdraws_on_teardown() {
        let gauge = SessionGauge::default();
        let mut first = gauge.outbound();
        let mut second = gauge.outbound();
        first.report(3);
        second.report(2);
        assert_eq!(gauge.snapshot().outbound_queued, 5);
        first.report(1);
        assert_eq!(gauge.snapshot().outbound_queued, 3);
        drop(second);
        assert_eq!(gauge.snapshot().outbound_queued, 1);

        {
            let _one = gauge.begin();
            let _two = gauge.begin();
            assert_eq!(gauge.snapshot().busy, 2);
        }
        let sample = gauge.snapshot();
        assert_eq!((sample.busy, sample.processed), (0, 2));
    }

    #[test]
    fn guard_marks_busy_then_counts_processed() {
        let gauge = InboxGauge::default();
        assert_eq!(gauge.snapshot(), InboxActivity::default());

        {
            let _busy = gauge.begin(3);
            let sample = gauge.snapshot();
            assert!(sample.busy);
            assert_eq!(sample.queued, 3);
            assert_eq!(sample.processed, 0);
        }

        let sample = gauge.snapshot();
        assert!(sample.is_idle());
        assert_eq!(sample.processed, 1);
    }
}
