//! Shared test fixtures for both relays: an empty [`RuntimeConfiguration`] and
//! a builder that populates it with `count` fake connected peers. Previously
//! copy-pasted verbatim into each relay's test module (including the whole
//! `Configuration` literal).

use std::sync::Arc;

use tagsy_core::state::Frame;
use tokio::sync::RwLock;

use crate::configuration::{
    Configuration, PreviewGenerationPolicy, RuntimeConfiguration, RuntimePeer,
};

/// An empty runtime configuration (no sync directories, no peers) suitable for
/// constructing a relay under test.
pub(crate) fn runtime_for_test() -> Arc<RwLock<RuntimeConfiguration>> {
    let configuration = Configuration {
        sync_directories: Vec::new(),
        listen_port: None,
        peers: Vec::new(),
        tags: Vec::new(),
        preview_generation_policy: PreviewGenerationPolicy::Lazy,
        editor_rules: Vec::new(),
        tag_rules: Vec::new(),
        home_sections: Vec::new(),
    };
    Arc::new(RwLock::new(RuntimeConfiguration::new(&configuration)))
}

/// Build a runtime with `count` fake connected peers named `peer0`.. and return
/// it plus each peer's public key and its inbound frame receiver (what that
/// peer would see arriving on the wire).
pub(crate) async fn engine_with_peers(
    count: usize,
) -> (
    Arc<RwLock<RuntimeConfiguration>>,
    Vec<(String, tokio::sync::mpsc::UnboundedReceiver<Frame>)>,
) {
    let runtime = runtime_for_test();
    let mut peers = Vec::new();
    {
        let mut guard = runtime.write().await;
        for i in 0..count {
            let public_key = format!("peer{i}");
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Frame>();
            let mut runtime_peer = RuntimePeer::new();
            runtime_peer.outbound = Some(tx);
            guard.peers.insert(public_key.clone(), runtime_peer);
            peers.push((public_key, rx));
        }
    }
    (runtime, peers)
}
