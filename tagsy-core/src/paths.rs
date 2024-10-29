//! Shared, device-independent path constants.
//!
//! Only the control-socket location lives here — it is the one path both the
//! daemon (which binds it) and any client (which dials it) must agree on, so it
//! belongs below both in the crate graph. Device-specific storage layout stays
//! in `tagsyd`.

use std::path::PathBuf;

/// The fixed path of the daemon's local control socket.
///
/// A Unix-domain socket under the systemd `RuntimeDirectory` (`/run/tagsy`,
/// created `0700` and owned by the service user), so filesystem permission is
/// the entire security model for local control — nothing is exposed on any
/// network interface, so no auth handshake is needed here.
///
/// Callers that genuinely need a different location (tests, a non-systemd
/// launch) pass an explicit path to `serve_control` / `IpcBackend::connect`
/// and the CLI `--socket` flag, rather than relying on discovery here.
pub const CONTROL_SOCKET_PATH: &str = "/run/tagsy/tagsy.sock";

/// Path to the daemon's local control socket.
///
/// Returns the fixed [`CONTROL_SOCKET_PATH`]. See its docs for why there is no
/// environment lookup or fallback.
pub fn control_socket_path() -> PathBuf {
    PathBuf::from(CONTROL_SOCKET_PATH)
}
