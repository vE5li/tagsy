//! Peer **connection state** crossing the port: the set of peers currently
//! connected, and the events by which that set changes.
//!
//! This is deliberately *not* an [`Operation`](crate::Operation). An operation
//! is a lifecycle thing — it starts, optionally reports progress, and reaches a
//! terminal outcome. A live peer link has none of that shape: it is a *level*,
//! not an edge. Connecting is the operation ([`ConnectingToPeer`]); the
//! resulting connection is state, and it belongs on its own stream so the UI
//! can render a calm "who is connected right now" indicator instead of an
//! ever-present "work in flight" row.
//!
//! The live registry that mints and broadcasts these (`Connections`,
//! `ConnectionGuard`) is runtime machinery and stays in `tagsyd`; only the
//! serde-able data lives here.
//!
//! [`ConnectingToPeer`]: crate::OperationKind::ConnectingToPeer

use serde::{Deserialize, Serialize};

use crate::Direction;

/// One peer we currently hold a live session with.
///
/// `public_key` is the stable identity of the peer and the key by which a
/// [`ConnectionEvent::Disconnected`] refers back to this entry; `peer_name` is
/// the human-facing label. `since` is wall-clock milliseconds (same clock as
/// operations' `started_at`) so the UI can show how long the link has been up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectedPeer {
    pub peer_name: String,
    pub public_key: String,
    pub direction: Direction,
    pub since: i64,
}

/// A change to the set of connected peers.
///
/// Delivery is best-effort (matching the change and operation buses): a
/// subscriber that lags past the channel capacity observes a gap, which the
/// transport maps onto a `Resynced`-style prompt to re-snapshot via
/// [`connected_peers`](crate::Backend::connected_peers).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionEvent {
    /// A peer session came up.
    Connected(ConnectedPeer),
    /// A peer session ended. Identified by `public_key` (the same identity the
    /// [`Connected`](ConnectionEvent::Connected) carried) so a subscriber can
    /// drop the matching row.
    Disconnected { public_key: String },
}
