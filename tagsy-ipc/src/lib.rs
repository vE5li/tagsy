//! `tagsy-ipc` — the local control-socket protocol and its client.
//!
//! On Linux the sync engine and the DB live in a long-running daemon while the
//! UI is a separate process; they talk over a Unix-domain control socket. This
//! crate holds the wire vocabulary both sides speak ([`protocol`]) and the
//! **client** half ([`IpcBackend`]) that dials the socket and implements the
//! [`Backend`](tagsy_api::Backend) port.
//!
//! The **server** half (`serve_control` and its dispatch) stays in `tagsyd`:
//! it needs the daemon's `ApiService` and the transfer subsystem. It depends on
//! this crate for the protocol types and codec.

pub mod client;
pub mod protocol;

pub use client::IpcBackend;
pub use protocol::{ControlFrame, ControlRequest, ControlResponse, decode_frame, encode_frame};
