//! Peer-facing logic: the session machinery that drives a WebSocket link to a
//! single peer, and the pure planning functions that decide what to sync.
//!
//! - [`handshake`] — this device's cryptographic identity and the handshake it
//!   exchanges with a peer (was `identity.rs`).
//! - [`dial`] — establishing a connection (inbound accept and outbound dial)
//!   and running the handshake before handing off to a session.
//! - [`session`] — [`session::run_peer_session`], the post-handshake loop
//!   shared by both directions, and its [`session::PeerContext`] routing
//!   bundle.
//! - [`plan`] / [`plan_tags`] — the pure functions that reconcile a peer's
//!   manifests against our local state.
//! - [`pull_scheduler`] — a process-wide admission gate that bounds how many
//!   file byte-transfers run at once, so a bulk import can't flood the link.

pub mod dial;
pub mod fetch;
pub mod handshake;
pub mod plan;
pub mod plan_tags;
pub mod pull_scheduler;
pub mod relay;
pub mod session;
pub mod transfer;
