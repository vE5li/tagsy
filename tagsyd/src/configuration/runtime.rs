//! Mutable per-session state, keyed by peer — **not** configuration.
//!
//! This lives beside the config types only for historical reasons; it is the
//! runtime mirror of the configured [`peers`](super::Configuration::peers), one
//! [`RuntimePeer`] each, tracking the live outbound channels for a peer while a
//! session is up. It is the sole reason this subtree touches
//! [`tagsy_core::state::Frame`] and [`PeerCommand`], so keeping it in its own
//! module leaves [`mod`](super) a pure config leaf.

use std::collections::HashMap;

use tagsy_core::state::Frame;
use tokio::sync::mpsc::UnboundedSender;

use super::Configuration;
use crate::catalog::messages::PeerCommand;

pub struct ConnectionStatistics {}

pub struct RuntimePeer {
    pub sync_type: Option<super::SyncType>,
    pub statistics: ConnectionStatistics,
    /// Sender into the outbound WebSocket task for this peer.
    /// `None` when no connection is currently established.
    ///
    /// Carries `Frame` (not raw `Change`) because reconciliation and chunk
    /// transfer messages (`Sync::Manifest`, `Sync::ChunkRequest`, ...) share
    /// the same outbound queue as live changes. `forward_to_peers` wraps in
    /// `Frame::Change`.
    pub outbound: Option<UnboundedSender<Frame>>,
    /// Command channel into this peer's live session, used by `handle_changes`
    /// to trigger a byte pull for a change this peer just announced. `None`
    /// when no session is established. Registered/cleared alongside
    /// `outbound`.
    pub commands: Option<UnboundedSender<PeerCommand>>,
}

impl Default for RuntimePeer {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimePeer {
    pub fn new() -> Self {
        Self {
            sync_type: None,
            statistics: ConnectionStatistics {},
            outbound: None,
            commands: None,
        }
    }
}

pub struct RuntimeConfiguration {
    pub peers: HashMap<String, RuntimePeer>,
}

impl RuntimeConfiguration {
    pub fn new(configuration: &Configuration) -> Self {
        let peers = configuration
            .peers
            .iter()
            .map(|peer| (peer.public_key.clone(), RuntimePeer::new()))
            .collect();

        Self { peers }
    }
}
