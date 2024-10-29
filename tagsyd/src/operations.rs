//! Live sync-operation reporting.
//!
//! The peer-sync engine does a lot of work that, until now, was only observable
//! through the daemon logs: connecting to peers, serving a file a peer asked
//! for, fetching a file from a peer, reconciling a manifest, and so on. This
//! module surfaces that same work to the UI as first-class **operations** so a
//! frontend can render "what is tagsy doing right now" without scraping logs.
//!
//! ## Operations vs. events
//!
//! An [`Operation`] is a *lifecycle* thing: it has a start, an optional stream
//! of progress updates, and a terminal outcome (completed / failed / aborted).
//! This is deliberately distinct from the change-event stream
//! ([`ApiEvent`](crate::frontend::api::ApiEvent)), which carries instantaneous
//! store-mutation events. The two live on separate channels.
//!
//! ## Model
//!
//! [`Operations`] is a cheap-to-clone registry shared across the runtime (it
//! lives on [`PeerContext`](crate::peer::session::PeerContext) and the
//! [`ApiService`](crate::frontend::api::ApiService)). Each unit of work calls
//! [`Operations::begin`], which allocates an [`OperationId`], records an
//! `Active` [`Operation`], broadcasts an [`OperationEvent::Started`], and
//! returns an [`OperationHandle`]. The handle reports progress with
//! [`OperationHandle::progress`] and finishes with
//! [`OperationHandle::complete`] / [`OperationHandle::fail`]. If the handle is
//! dropped without a terminal call (a task cancelled, a link dropped
//! mid-transfer), its `Drop` marks the operation `Aborted` so a stale row never
//! lingers in the "active" set.
//!
//! ## Delivery
//!
//! Two ways, mirroring the change stream so an IPC client reconnecting mid-work
//! is not left blind:
//!
//! - [`Operations::snapshot`] returns every currently-active operation (for the
//!   initial paint, and after an IPC `Resynced`).
//! - [`Operations::subscribe`] taps a `broadcast` of [`OperationEvent`]s (live
//!   updates). Terminal operations are removed from the active set as their
//!   terminal event is broadcast.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// The operation *data* types cross the port and live in `tagsy-api`; this module
// keeps only the live registry that mints and broadcasts them. Re-exported so
// `crate::operations::{Operation, OperationKind, ...}` call sites are unchanged.
pub use tagsy_api::{
    Direction, Operation, OperationEvent, OperationId, OperationKind, OperationStatus, Progress,
};
use tokio::sync::broadcast;

/// Capacity of the operation-event broadcast channel. A slow subscriber that
/// lags past this observes `Lagged`, mapped by the transport to a `Resynced`
/// that prompts a fresh [`Operations::snapshot`].
const CHANNEL_CAPACITY: usize = 1024;

/// The shared registry of live sync operations.
///
/// Cheap to clone (an `Arc` around the shared state plus a
/// `broadcast::Sender`). Held by the
/// [`ApiService`](crate::frontend::api::ApiService) (to serve `snapshot`/
/// `subscribe`) and by every peer session via
/// [`PeerContext`](crate::peer::session::PeerContext) (to `begin` work).
#[derive(Clone)]
pub struct Operations {
    inner: Arc<Inner>,
}

struct Inner {
    /// The currently-active operations, keyed by id. Terminal operations are
    /// removed as their terminal event is broadcast.
    active: Mutex<HashMap<OperationId, Operation>>,
    /// Monotonic id source.
    next_id: AtomicU64,
    /// Broadcast of live [`OperationEvent`]s. `subscribe` taps it.
    events: broadcast::Sender<OperationEvent>,
}

impl Default for Operations {
    fn default() -> Self {
        Self::new()
    }
}

impl Operations {
    /// Create an empty registry.
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            inner: Arc::new(Inner {
                active: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(0),
                events,
            }),
        }
    }

    /// Begin a new operation of `kind`.
    ///
    /// Records it as `Active` (no progress yet), broadcasts
    /// [`OperationEvent::Started`], and returns a handle. Dropping the handle
    /// without a terminal call marks the operation `Aborted`.
    pub fn begin(&self, kind: OperationKind) -> OperationHandle {
        let id = OperationId::from_u64(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        let now = crate::clock::now_millis();
        let operation = Operation {
            id,
            kind,
            status: OperationStatus::Active { progress: None },
            started_at: now,
            updated_at: now,
        };

        {
            let mut active = self.inner.active.lock().expect("operations lock poisoned");
            active.insert(id, operation.clone());
        }
        let _ = self.inner.events.send(OperationEvent::Started(operation));

        OperationHandle {
            operations: self.clone(),
            id,
            finished: false,
        }
    }

    /// Every operation currently active, for an initial UI paint or an IPC
    /// re-snapshot. Order is unspecified; the UI sorts by `started_at`.
    pub fn snapshot(&self) -> Vec<Operation> {
        self.inner
            .active
            .lock()
            .expect("operations lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Subscribe to live [`OperationEvent`]s.
    pub fn subscribe(&self) -> broadcast::Receiver<OperationEvent> {
        self.inner.events.subscribe()
    }

    /// Report progress for `id` by id (rather than through its
    /// [`OperationHandle`]).
    ///
    /// Useful when the handle has been moved elsewhere (e.g. into the task that
    /// awaits a transfer outcome) but a separate progress callback still needs
    /// to update the same operation. A no-op if the operation already finished.
    pub fn report_progress(&self, id: OperationId, done: u64, total: Option<u64>) {
        self.set_progress(id, Progress { done, total });
    }

    /// Update `id`'s progress in place and broadcast an `Updated` event. A
    /// no-op if the operation already finished (its handle went terminal).
    fn set_progress(&self, id: OperationId, progress: Progress) {
        let updated = {
            let mut active = self.inner.active.lock().expect("operations lock poisoned");
            match active.get_mut(&id) {
                Some(operation) => {
                    operation.status = OperationStatus::Active {
                        progress: Some(progress),
                    };
                    operation.updated_at = crate::clock::now_millis();
                    Some(operation.clone())
                }
                None => None,
            }
        };
        if let Some(operation) = updated {
            let _ = self.inner.events.send(OperationEvent::Updated(operation));
        }
    }

    /// Apply a terminal `status` to `id`: remove it from the active set and
    /// broadcast a final `Updated` event carrying the terminal status.
    fn finish(&self, id: OperationId, status: OperationStatus) {
        let finished = {
            let mut active = self.inner.active.lock().expect("operations lock poisoned");
            active.remove(&id).map(|mut operation| {
                operation.status = status;
                operation.updated_at = crate::clock::now_millis();
                operation
            })
        };
        if let Some(operation) = finished {
            let _ = self.inner.events.send(OperationEvent::Updated(operation));
        }
    }
}

/// A handle to one in-progress [`Operation`], returned by
/// [`Operations::begin`].
///
/// Report progress with [`progress`](Self::progress); finish with
/// [`complete`](Self::complete) or [`fail`](Self::fail). Dropping the handle
/// without a terminal call marks the operation
/// [`Aborted`](OperationStatus::Aborted) so an interrupted transfer/connection
/// never leaves a stale "active" row.
pub struct OperationHandle {
    operations: Operations,
    id: OperationId,
    /// Set once a terminal call has been made, so `Drop` does not
    /// double-finish.
    finished: bool,
}

impl OperationHandle {
    /// This operation's id.
    pub fn id(&self) -> OperationId {
        self.id
    }

    /// Report progress: `done` units of an optional `total`.
    pub fn progress(&self, done: u64, total: Option<u64>) {
        self.operations
            .set_progress(self.id, Progress { done, total });
    }

    /// Mark the operation completed successfully.
    pub fn complete(mut self) {
        self.finished = true;
        self.operations.finish(self.id, OperationStatus::Completed);
    }

    /// Mark the operation failed with a human-readable `reason`.
    pub fn fail(mut self, reason: impl Into<String>) {
        self.finished = true;
        self.operations.finish(self.id, OperationStatus::Failed {
            reason: reason.into(),
        });
    }
}

impl Drop for OperationHandle {
    fn drop(&mut self) {
        if !self.finished {
            self.operations.finish(self.id, OperationStatus::Aborted);
        }
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::FileId;

    use super::*;

    #[test]
    fn begin_records_active_and_broadcasts_started() {
        let operations = Operations::new();
        let mut subscriber = operations.subscribe();

        let handle = operations.begin(OperationKind::fetching(FileId::new()));

        let snapshot = operations.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].id, handle.id());
        assert!(matches!(snapshot[0].status, OperationStatus::Active {
            progress: None
        }));

        let event = subscriber.try_recv().expect("started event");
        assert!(matches!(event, OperationEvent::Started(_)));
    }

    #[test]
    fn progress_updates_active_operation() {
        let operations = Operations::new();
        let handle = operations.begin(OperationKind::receiving_file(FileId::new(), "peer"));
        let mut subscriber = operations.subscribe();

        handle.progress(50, Some(100));

        let snapshot = operations.snapshot();
        assert_eq!(snapshot[0].status, OperationStatus::Active {
            progress: Some(Progress {
                done: 50,
                total: Some(100)
            })
        });
        assert!(matches!(
            subscriber.try_recv().expect("update"),
            OperationEvent::Updated(_)
        ));
    }

    #[test]
    fn complete_removes_from_active_and_broadcasts_terminal() {
        let operations = Operations::new();
        let handle = operations.begin(OperationKind::fetching(FileId::new()));
        let mut subscriber = operations.subscribe();

        handle.complete();

        assert!(operations.snapshot().is_empty());
        let event = subscriber.try_recv().expect("terminal event");
        match event {
            OperationEvent::Updated(operation) => {
                assert_eq!(operation.status, OperationStatus::Completed);
            }
            other => panic!("expected Updated, got {other:?}"),
        }
    }

    #[test]
    fn dropping_handle_marks_aborted() {
        let operations = Operations::new();
        let mut subscriber = operations.subscribe();

        {
            let _handle = operations.begin(OperationKind::fetching(FileId::new()));
            // subscriber sees Started
            assert!(matches!(
                subscriber.try_recv().expect("started"),
                OperationEvent::Started(_)
            ));
        } // handle dropped here

        assert!(operations.snapshot().is_empty());
        match subscriber.try_recv().expect("aborted event") {
            OperationEvent::Updated(operation) => {
                assert_eq!(operation.status, OperationStatus::Aborted);
            }
            other => panic!("expected Updated(Aborted), got {other:?}"),
        }
    }
}
