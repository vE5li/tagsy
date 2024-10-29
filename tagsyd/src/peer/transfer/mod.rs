//! Content-addressed chunk transfer over peer links.
//!
//! There is one byte-movement mechanism. A chunk is treated as **pure content,
//! not a message**: its identity *is* `(file_id, content_hash, offset)`, and
//! because chunking is deterministic (offset a multiple of [`CHUNK_SIZE`], the
//! canonical length `min(CHUNK_SIZE, size - offset)` derived from the version's
//! authoritative size), that key denotes one exact, bit-identical byte range on
//! every peer whose copy hashes to `content_hash`. Nothing else correlates a
//! request to its reply — no transfer session, no per-request cookie, no
//! open/close handshake.
//!
//! This module provides:
//!
//! - [`receive`] — the single receiver driver. Given a `(file_id, content_hash,
//!   expected_size)` and a way to send [`Sync::ChunkRequest`]s and await
//!   [`ChunkReply`]s, it keeps a window of requests in flight, streams replies
//!   into a temp file with incremental BLAKE3, and verifies the whole file at
//!   the end. Where each chunk's *first* request goes is a routing policy
//!   supplied by the caller, not part of the driver.
//! - [`answer_chunk_request`] — the stateless holder side. It answers a single
//!   `ChunkRequest` from a [`ChunkSource`] after verifying (via a
//!   [`VerifiedHashCache`]) that the source's content matches `content_hash`.
//! - [`ChunkSource`] / [`ProviderSource`] — where servable bytes live.
//!
//! Integrity is **end-to-end**: only the origin receiver verifies the
//! accumulated hash against `content_hash`. Relays (see [`relay`]) hold no
//! bytes and verify nothing.
//!
//! The three sides live in sibling modules: [`receive`] (receiver driver),
//! [`serve`] (holder side + the verified-hash cache), and [`source`] (where
//! servable bytes come from). This file holds the vocabulary shared across
//! them — the wire constants, the request/reply types, and the error enum.
//!
//! [`Sync::ChunkRequest`]: tagsy_core::state::Sync::ChunkRequest
//! [`relay`]: crate::peer::relay
//! [`receive`]: crate::peer::transfer::receive
//! [`serve`]: crate::peer::transfer::serve
//! [`source`]: crate::peer::transfer::source

use std::time::Duration;

pub mod receive;
pub mod serve;
pub mod source;

pub use receive::receive;
pub use serve::{ChunkAnswer, VerifiedHashCache, answer_chunk_request};
pub use source::{
    ChunkFuture, ChunkSource, ProviderChunkReply, ProviderChunkRequest, ProviderSource,
};

/// A sink for byte-transfer progress.
///
/// The receiver reports the running total of bytes written (and the known
/// total) through this so a caller — the peer session — can surface a live
/// [`Operation`](crate::operations) with a progress bar. It is a thin boxed
/// callback rather than a hard dependency on the operations module, so the
/// driver stays unit-testable in isolation. Reporting is best-effort and never
/// affects transfer correctness.
pub type ProgressSink = Box<dyn Fn(u64, Option<u64>) + Send + Sync>;

/// Bytes per chunk. This is part of the **wire contract**: it defines chunk
/// boundaries and the canonical chunk length every node derives from the
/// version's size, so changing it is a protocol-breaking change and must go
/// with a `PROTOCOL_VERSION` bump.
///
/// The canonical value lives in [`tagsy_core::content::CHUNK_SIZE`] so the IPC
/// client (`tagsy-ipc`) reads the same number without depending on the transfer
/// subsystem; re-exported here for the many `crate::peer::transfer::CHUNK_SIZE`
/// call sites.
pub use tagsy_core::content::CHUNK_SIZE;

/// How many chunk requests the receiver keeps in flight at once. A larger
/// window hides per-chunk round-trip latency. Kept small so a relayed transfer
/// bounds in-flight bytes per hop to `WINDOW * CHUNK_SIZE`.
pub const WINDOW: u64 = 8;

/// How long a relay waiter entry lives before it is presumed dead, and how long
/// the receiver's per-chunk liveness guard waits with no progress. One tunable
/// across the relay layer and the receiver.
///
/// Lives here, at the bottom of the byte-movement stack, so that both halves
/// can see it: [`crate::peer::relay`] already depends on
/// this module, and the receiver below uses it directly.
pub const HOP_TIMEOUT: Duration = Duration::from_secs(8);

/// A reply to one of the receiver's outstanding `ChunkRequest`s, demuxed by the
/// peer session for a specific in-flight receive. The `file_id` /
/// `content_hash` are fixed for the whole receive, so only the `offset` (and,
/// for `Data`, the bytes) are carried here — the reply is matched to a pending
/// request by `offset`.
#[derive(Debug)]
pub enum ChunkReply {
    /// The canonical bytes at `offset`.
    Data { offset: u64, bytes: Vec<u8> },
    /// This direction cannot serve `offset` (missing content or the file
    /// changed). A miss from *all* directions fails the receive.
    Miss { offset: u64 },
}

/// A `ChunkRequest` the receiver wants sent. The peer session routes it toward
/// a holder (per its routing policy) and wraps it as
/// [`Sync::ChunkRequest`](tagsy_core::state::Sync::ChunkRequest).
#[derive(Debug)]
pub struct ChunkRequest {
    pub offset: u64,
}

/// Why a receive failed.
#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    /// A chunk was missed from every reachable direction (the version is
    /// superseded, or the only holder is unreachable). No retry helps.
    #[error("chunk at offset {offset} unavailable from any peer")]
    ChunkUnavailable { offset: u64 },
    /// A *connected* peer accepted the request but went silent for
    /// [`HOP_TIMEOUT`]: no chunk was written within
    /// the per-chunk liveness window. The one guard against hanging forever.
    #[error("transfer stalled (liveness timeout)")]
    LivenessTimeout,
    /// The reassembled content did not hash to the expected value.
    #[error("content hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
    /// A local I/O error writing the temp file.
    #[error("transfer I/O error: {0}")]
    Io(#[source] std::io::Error),
    /// The inbound reply channel closed before the receive completed (the link
    /// dropped).
    #[error("transfer channel closed early")]
    ChannelClosed,
}

/// The outcome of a receive, delivered once it finishes.
pub enum ReceiveOutcome {
    /// The bytes arrived and hashed correctly; here is the temp file.
    Complete(FileBytes),
    /// The receive failed (unavailable / liveness timeout / hash mismatch /
    /// I/O / link drop).
    Failed(TransferError),
}

/// A short, log-friendly prefix of a hex content hash (first 8 chars), so log
/// lines can correlate a transfer without dumping the full 64-char digest.
pub(super) fn short_hash(hash: &str) -> &str {
    hash.get(..8).unwrap_or(hash)
}

use crate::file_bytes::FileBytes;
