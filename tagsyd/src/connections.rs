//! Live peer-connection tracking.
//!
//! A peer link is **state, not an operation.** An [`Operation`] has a start, a
//! progress stream, and a terminal outcome; a connection has none of that
//! shape — it is simply up or down. Modeling it as a never-completing "active"
//! operation (as the daemon once did) made the UI's work indicator light up
//! permanently and forced a clean disconnect to be reported as `Aborted`. This
//! registry gives connections their own home: a set of currently-connected
//! peers plus a stream of connect/disconnect edges, entirely separate from the
//! operation stream.
//!
//! The connect *attempt* is still an operation
//! ([`ConnectingToPeer`](tagsy_api::OperationKind::ConnectingToPeer)) — it
//! genuinely starts and ends. It hands off to this registry at the moment the
//! session goes live.
//!
//! ## Model
//!
//! [`Connections`] is a cheap-to-clone registry shared across the runtime (it
//! lives on [`PeerContext`](crate::peer::session::PeerContext) and the
//! [`ApiService`](crate::frontend::api::ApiService)). A session calls
//! [`Connections::register`] when it goes live, which records the peer,
//! broadcasts [`ConnectionEvent::Connected`], and returns a
//! [`ConnectionGuard`]. When the guard drops (the session ends for any reason),
//! the peer is removed and [`ConnectionEvent::Disconnected`] is broadcast.
//!
//! ## Duplicate sessions
//!
//! A single peer can briefly hold two sessions at once (both sides dial each
//! other simultaneously). The registry reference-counts sessions per peer, so
//! `Connected` fires only on the *first* session for a peer and `Disconnected`
//! only when the *last* one ends — a subscriber sees one clean connect/
//! disconnect pair per peer, not per socket.
//!
//! ## Delivery
//!
//! Mirrors the operation and change streams:
//!
//! - [`Connections::snapshot`] returns every connected peer (initial paint /
//!   IPC re-snapshot).
//! - [`Connections::subscribe`] taps a `broadcast` of [`ConnectionEvent`]s.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub use tagsy_api::{ConnectedPeer, ConnectionEvent, Direction};
use tokio::sync::broadcast;

/// Capacity of the connection-event broadcast channel. A slow subscriber that
/// lags past this observes `Lagged`, mapped by the transport to a `Resynced`
/// that prompts a fresh [`Connections::snapshot`].
const CHANNEL_CAPACITY: usize = 256;

/// The shared registry of live peer connections.
///
/// Cheap to clone (an `Arc` around the shared state plus a
/// `broadcast::Sender`). Held by the
/// [`ApiService`](crate::frontend::api::ApiService) (to serve `snapshot`/
/// `subscribe`) and by every peer session via
/// [`PeerContext`](crate::peer::session::PeerContext) (to `register`).
#[derive(Clone)]
pub struct Connections {
    inner: Arc<Inner>,
}

/// One connected peer plus how many live sessions currently back it.
struct Entry {
    peer: ConnectedPeer,
    sessions: usize,
}

struct Inner {
    /// Connected peers keyed by public key, with a per-peer session refcount.
    peers: Mutex<HashMap<String, Entry>>,
    /// Broadcast of live [`ConnectionEvent`]s. `subscribe` taps it.
    events: broadcast::Sender<ConnectionEvent>,
}

impl Default for Connections {
    fn default() -> Self {
        Self::new()
    }
}

impl Connections {
    /// Create an empty registry.
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            inner: Arc::new(Inner {
                peers: Mutex::new(HashMap::new()),
                events,
            }),
        }
    }

    /// Register a now-live session with `peer_public_key`.
    ///
    /// The first session for a peer records it and broadcasts
    /// [`ConnectionEvent::Connected`]; a second concurrent session only bumps
    /// the refcount. Returns a [`ConnectionGuard`] whose `Drop` releases the
    /// session (and disconnects the peer once the last one goes).
    pub fn register(
        &self,
        peer_public_key: &str,
        peer_name: &str,
        direction: Direction,
    ) -> ConnectionGuard {
        let connected = {
            let mut peers = self.inner.peers.lock().expect("connections lock poisoned");
            match peers.get_mut(peer_public_key) {
                Some(entry) => {
                    entry.sessions += 1;
                    None
                }
                None => {
                    let peer = ConnectedPeer {
                        peer_name: peer_name.to_owned(),
                        public_key: peer_public_key.to_owned(),
                        direction,
                        since: crate::clock::now_millis(),
                    };
                    peers.insert(peer_public_key.to_owned(), Entry {
                        peer: peer.clone(),
                        sessions: 1,
                    });
                    Some(peer)
                }
            }
        };
        if let Some(peer) = connected {
            let _ = self.inner.events.send(ConnectionEvent::Connected(peer));
        }

        ConnectionGuard {
            connections: self.clone(),
            public_key: peer_public_key.to_owned(),
        }
    }

    /// Every currently-connected peer, for an initial UI paint or an IPC
    /// re-snapshot. Order is unspecified.
    pub fn snapshot(&self) -> Vec<ConnectedPeer> {
        self.inner
            .peers
            .lock()
            .expect("connections lock poisoned")
            .values()
            .map(|entry| entry.peer.clone())
            .collect()
    }

    /// Subscribe to live [`ConnectionEvent`]s.
    pub fn subscribe(&self) -> broadcast::Receiver<ConnectionEvent> {
        self.inner.events.subscribe()
    }

    /// Release one session for `public_key`. Broadcasts
    /// [`ConnectionEvent::Disconnected`] only when the last session ends.
    fn release(&self, public_key: &str) {
        let disconnected = {
            let mut peers = self.inner.peers.lock().expect("connections lock poisoned");
            match peers.get_mut(public_key) {
                Some(entry) => {
                    entry.sessions -= 1;
                    if entry.sessions == 0 {
                        peers.remove(public_key);
                        true
                    } else {
                        false
                    }
                }
                None => false,
            }
        };
        if disconnected {
            let _ = self.inner.events.send(ConnectionEvent::Disconnected {
                public_key: public_key.to_owned(),
            });
        }
    }
}

/// A handle to one live peer session, returned by [`Connections::register`].
///
/// Held for the lifetime of the session; its `Drop` releases the session so a
/// peer that goes away — cleanly or not — always leaves the connected set.
pub struct ConnectionGuard {
    connections: Connections,
    public_key: String,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.connections.release(&self.public_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_records_and_broadcasts_connected() {
        let connections = Connections::new();
        let mut subscriber = connections.subscribe();

        let _guard = connections.register("pk-a", "peer-a", Direction::Outbound);

        let snapshot = connections.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].public_key, "pk-a");

        match subscriber.try_recv().expect("connected event") {
            ConnectionEvent::Connected(peer) => assert_eq!(peer.public_key, "pk-a"),
            other => panic!("expected Connected, got {other:?}"),
        }
    }

    #[test]
    fn dropping_guard_disconnects() {
        let connections = Connections::new();
        let mut subscriber = connections.subscribe();

        {
            let _guard = connections.register("pk-a", "peer-a", Direction::Inbound);
            assert!(matches!(
                subscriber.try_recv().expect("connected"),
                ConnectionEvent::Connected(_)
            ));
        } // guard dropped here

        assert!(connections.snapshot().is_empty());
        match subscriber.try_recv().expect("disconnected event") {
            ConnectionEvent::Disconnected { public_key } => assert_eq!(public_key, "pk-a"),
            other => panic!("expected Disconnected, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_sessions_fire_one_pair() {
        let connections = Connections::new();
        let mut subscriber = connections.subscribe();

        let first = connections.register("pk-a", "peer-a", Direction::Outbound);
        let second = connections.register("pk-a", "peer-a", Direction::Inbound);

        // Only one Connected despite two sessions.
        assert!(matches!(
            subscriber.try_recv().expect("connected"),
            ConnectionEvent::Connected(_)
        ));
        assert!(subscriber.try_recv().is_err());
        assert_eq!(connections.snapshot().len(), 1);

        drop(first);
        // Still connected via the second session; no event yet.
        assert!(subscriber.try_recv().is_err());
        assert_eq!(connections.snapshot().len(), 1);

        drop(second);
        match subscriber.try_recv().expect("disconnected") {
            ConnectionEvent::Disconnected { public_key } => assert_eq!(public_key, "pk-a"),
            other => panic!("expected Disconnected, got {other:?}"),
        }
        assert!(connections.snapshot().is_empty());
    }
}
