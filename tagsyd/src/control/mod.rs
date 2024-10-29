//! Daemon local control endpoint.
//!
//! On Linux the sync engine and the DB live in a long-running (systemd) daemon,
//! while the UI is a *separate* process. Because [`CatalogStore`] is
//! single-owner, the UI cannot open the DB itself — it must ask the daemon.
//! This module is that channel.
//!
//! ## Transport
//!
//! A **Unix-domain-socket** control listener at
//! [`paths::control_socket_path`](crate::paths::control_socket_path) (the fixed
//! `/run/tagsy/tagsy.sock`) — **not** a TCP port. Security is entirely
//! filesystem permissions: the runtime directory `/run/tagsy` is created
//! `0700` and owned by the service user (via `RuntimeDirectory=tagsy` in the
//! systemd unit), so only that user can connect and nothing is exposed on any
//! network interface. There is therefore **no auth handshake** for local
//! control (unlike the ed25519 peer handshake on the peer-sync port).
//!
//! The existing WebSocket/`Frame` framing (tokio + tokio-tungstenite) is reused
//! over the [`UnixListener`](tokio::net::UnixListener), so the networking code
//! stays unified. The wire payloads, however, are a **distinct message
//! category** from the peer `Change`/`Sync` protocol: peer framing is about
//! *sync*; control framing ([`ControlFrame`]) carries the UI-facing API
//! requests/responses/events.
//!
//! ## Relationship to the peer-sync port
//!
//! The `0.0.0.0:{listen_port}` listener is unchanged — it remains the remote
//! peer-sync port. UI control is **never** routed through it. A local UI client
//! is conceptually "another kind of client" of the daemon: it issues
//! queries/commands and subscribes to the change stream, reusing the same
//! broadcast plumbing (`event_sender` / [`ApiService::subscribe`]) that
//! `forward_to_peers` uses for peers.
//!
//! ## Module layout
//!
//! - [`protocol`] — the wire vocabulary ([`ControlRequest`] /
//!   [`ControlResponse`] / [`ControlFrame`]) and the binary codec, shared by
//!   both halves.
//! - [`server`] — the **daemon side** ([`serve_control`]): accepts connections,
//!   decodes [`ControlRequest`]s, dispatches them to the in-process
//!   [`ApiService`](crate::frontend::api::ApiService), and streams
//!   [`ApiEvent`](crate::frontend::api::ApiEvent)s back.
//! - [`client`] — the **client side** ([`IpcBackend`]): connects to the socket,
//!   serializes API calls, and returns results/events. It implements
//!   [`Backend`](crate::transport::Backend), so the Dart UI and the `tagsy` CLI
//!   talk to it exactly as they would the in-process backend.
//!
//! [`CatalogStore`]: crate::store::CatalogStore

pub mod server;

pub use server::serve_control;
// The protocol vocabulary and the IPC client moved to the `tagsy-ipc` crate
// (they sit below the daemon: the CLI and bridge dial the socket without
// depending on `tagsyd`). Re-exported here so the daemon's own callers keep
// using `crate::control::{IpcBackend, ControlFrame, ...}`.
pub use tagsy_ipc::{ControlFrame, ControlRequest, ControlResponse, IpcBackend};
