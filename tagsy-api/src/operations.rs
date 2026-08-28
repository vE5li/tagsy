//! The operation data types crossing the port: the id, kind, progress, status,
//! the [`Operation`] snapshot and the [`OperationEvent`] stream item.
//!
//! The live registry that mints and broadcasts these (`Operations`,
//! `OperationHandle`) is runtime machinery and stays in `tagsyd`; only the
//! serde-able data lives here.

use serde::{Deserialize, Serialize};
use tagsy_core::FileId;

/// A process-unique identifier for one live [`Operation`].
///
/// Unlike [`FileId`](tagsy_core::FileId)/[`TagId`](tagsy_core::TagId) this is
/// not a persisted UUID: operations are ephemeral runtime state, so a monotonic
/// counter is both sufficient and cheaper. It is stable for the life of the
/// operation, letting the UI update the same row from `Started` through
/// progress to the terminal event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationId(u64);

impl OperationId {
    /// Mint an id from a raw counter value. Called only by the runtime registry
    /// in `tagsyd`, which owns the monotonic counter.
    pub fn from_u64(value: u64) -> Self {
        OperationId(value)
    }

    /// The identifier as a plain integer (for display / the Dart DTO).
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Which end of a peer link a connection (or an operation running over it) was
/// established from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// We initiated the connection to the peer.
    Outbound,
    /// The peer connected to us.
    Inbound,
}

/// The kind of work an [`Operation`] represents, plus its descriptive payload.
///
/// One variant per user-meaningful sync activity. Ids are carried as their
/// canonical string form so the type is transport- and FFI-friendly (the wire
/// protocol and `flutter_rust_bridge` both prefer flat, `serde`-able shapes)
/// and the UI can link an operation back to the file/tag it concerns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationKind {
    /// Establishing an outbound connection to a configured peer.
    ///
    /// A connect *attempt* is a genuine operation: it starts, and it ends —
    /// `Completed` on a successful handshake, `Failed`/`Aborted` otherwise. The
    /// resulting live link is **not** an operation; it is connection *state*,
    /// tracked separately (see [`ConnectedPeer`] / the connection stream).
    ConnectingToPeer { peer_name: String, url: String },
    /// Receiving a file's bytes from a peer. "fetching 123 from peer B".
    ///
    /// Note: there is no sender-side counterpart. In the content-addressed
    /// chunk model a holder answers each `ChunkRequest` statelessly (no
    /// transfer session), so there is nothing to anchor a "serving file"
    /// operation to — serving is invisible to the operations UI by design.
    ReceivingFile { file_id: String, peer_name: String },
    /// An on-demand fetch originated locally (flooded to peers via the relay).
    Fetching { file_id: String },
    /// Reconciling a peer's file manifest against our catalog.
    ReconcilingManifest { peer_name: String },
    /// Reconciling a peer's tag definitions and relationships.
    ReconcilingTags { peer_name: String },
    /// Fetching a file to place it locally per a tag-based sync directory.
    PlacingFile { file_id: String },
}

/// Convenience constructors so call sites read naturally at the emit points.
impl OperationKind {
    pub fn connecting_to_peer(peer_name: impl Into<String>, url: impl Into<String>) -> Self {
        OperationKind::ConnectingToPeer {
            peer_name: peer_name.into(),
            url: url.into(),
        }
    }

    pub fn receiving_file(file_id: FileId, peer_name: impl Into<String>) -> Self {
        OperationKind::ReceivingFile {
            file_id: file_id.to_string(),
            peer_name: peer_name.into(),
        }
    }

    pub fn fetching(file_id: FileId) -> Self {
        OperationKind::Fetching {
            file_id: file_id.to_string(),
        }
    }

    pub fn reconciling_manifest(peer_name: impl Into<String>) -> Self {
        OperationKind::ReconcilingManifest {
            peer_name: peer_name.into(),
        }
    }

    pub fn reconciling_tags(peer_name: impl Into<String>) -> Self {
        OperationKind::ReconcilingTags {
            peer_name: peer_name.into(),
        }
    }

    pub fn placing_file(file_id: FileId) -> Self {
        OperationKind::PlacingFile {
            file_id: file_id.to_string(),
        }
    }
}

/// How far along an operation is, when it has a countable notion of progress.
///
/// `total` is optional because some operations know only the running count
/// (e.g. a fetch whose peer never announced a size), so the UI shows an
/// indeterminate spinner rather than a bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// Units done so far (bytes transferred, entries reconciled, ...).
    pub done: u64,
    /// Total units, if known.
    pub total: Option<u64>,
}

/// The lifecycle state of an [`Operation`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationStatus {
    /// Running. Carries the latest [`Progress`] if the operation reports any.
    Active { progress: Option<Progress> },
    /// Finished successfully.
    Completed,
    /// Finished with an error. Carries a human-readable reason.
    Failed { reason: String },
    /// Ended without a terminal outcome (handle dropped: task cancelled, link
    /// dropped mid-transfer, runtime shutting down).
    Aborted,
}

impl OperationStatus {
    /// Whether this is a terminal (no-longer-active) status.
    pub fn is_terminal(&self) -> bool {
        !matches!(self, OperationStatus::Active { .. })
    }
}

/// A single live (or just-finished) unit of sync work.
///
/// Snapshotted by the operations registry and carried in [`OperationEvent`]s.
/// `started_at`/`updated_at` are wall-clock milliseconds (same clock as the
/// change pipeline's `modified_at`) so the UI can sort and age operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub id: OperationId,
    pub kind: OperationKind,
    pub status: OperationStatus,
    pub started_at: i64,
    pub updated_at: i64,
}

/// A live update on the operation stream.
///
/// Delivery is best-effort (matching the change bus): a subscriber that lags
/// past the channel capacity observes a gap, which the transport maps onto a
/// [`Resynced`](crate::ApiEvent::Resynced)-style prompt to re-snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationEvent {
    /// A new operation began.
    Started(Operation),
    /// An existing operation's status changed (progress or terminal outcome).
    ///
    /// Carries the full [`Operation`] so a subscriber that missed the `Started`
    /// (e.g. it subscribed mid-flight) can still render the row.
    Updated(Operation),
}

impl OperationEvent {
    /// The operation this event concerns.
    pub fn operation(&self) -> &Operation {
        match self {
            OperationEvent::Started(operation) | OperationEvent::Updated(operation) => operation,
        }
    }
}
