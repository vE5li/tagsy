//! Content-keyed relays across the live peer tree.
//!
//! There are two relays — one for byte [`chunks`] and one for [`previews`] —
//! and they share one state machine. A relay maintains a **content-keyed waiter
//! table**: for each in-flight key it records which downstream links (peer
//! sessions or local receivers) are waiting, and which upstream neighbours it
//! forwarded the request to. The mechanics are identical for both payloads:
//!
//! - On a request from a downstream link for a key we have no entry for,
//!   forward it to every neighbour except the sender and record them in
//!   `upstream_outstanding`; if an entry already exists, add the link to
//!   `downstream` (request coalescing — one upstream fetch fanned to all
//!   waiters).
//! - On data from an upstream, fan it to *every* downstream waiter and drop the
//!   entry (first-writer-wins; later duplicates find no entry).
//! - On a miss from an upstream, remove it from `upstream_outstanding`; when
//!   that empties, fan a miss to all downstream waiters and drop.
//! - On link drop or TTL expiry, prune / fan a miss accordingly.
//!
//! A relay holds **no payload buffers** — only the waiter table, whose size is
//! bounded by the number of distinct in-flight keys. Integrity is end-to-end;
//! the relay verifies nothing.
//!
//! The generic machinery lives here: [`WaiterTable`] (the shared state machine)
//! and [`PeerDirectory`] (the peer-plumbing helper), parameterised over a
//! [`RelayProtocol`] that supplies the key type, payload types, and the three
//! frame constructors that differ between chunks and previews. The two concrete
//! relays are thin wrappers in [`chunks`] and [`previews`].

pub mod chunks;
pub mod previews;

#[cfg(test)]
pub(crate) mod testing;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

pub use chunks::ChunkRelay;
pub use previews::{PreviewRelay, PreviewReply};
use tagsy_core::FileId;
use tagsy_core::state::Frame;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

use crate::configuration::RuntimeConfiguration;
use crate::peer::transfer::HOP_TIMEOUT;

/// A short, log-friendly prefix of a hex content hash (first 8 chars).
pub(crate) fn short_hash(hash: &str) -> &str {
    hash.get(..8).unwrap_or(hash)
}

/// Reference to a peer's outbound frame queue plus its public key.
pub(crate) struct PeerOutbound {
    pub public_key: String,
    pub sender: tokio::sync::mpsc::UnboundedSender<Frame>,
}

/// The peer-plumbing shared by both relays: snapshots of connected peers'
/// outbound senders, resolved against the live [`RuntimeConfiguration`].
///
/// Cheap to clone (an `Arc`).
#[derive(Clone)]
pub(crate) struct PeerDirectory {
    runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
}

impl PeerDirectory {
    fn new(runtime_configuration: Arc<RwLock<RuntimeConfiguration>>) -> Self {
        Self {
            runtime_configuration,
        }
    }

    /// Snapshot every connected peer's outbound sender, optionally excluding
    /// one public key (the peer a request came from — never echo it back).
    pub(crate) async fn connected_peers(&self, exclude: Option<&str>) -> Vec<PeerOutbound> {
        self.runtime_configuration
            .read()
            .await
            .peers
            .iter()
            .filter(|(public_key, _)| exclude != Some(public_key.as_str()))
            .filter_map(|(public_key, runtime_peer)| {
                runtime_peer.outbound.as_ref().map(|sender| PeerOutbound {
                    public_key: public_key.clone(),
                    sender: sender.clone(),
                })
            })
            .collect()
    }

    /// Resolve a single peer's outbound sender by public key.
    pub(crate) async fn peer_outbound(
        &self,
        public_key: &str,
    ) -> Option<tokio::sync::mpsc::UnboundedSender<Frame>> {
        self.runtime_configuration
            .read()
            .await
            .peers
            .get(public_key)
            .and_then(|runtime_peer| runtime_peer.outbound.clone())
    }
}

/// A link waiting for a payload: either a peer we must forward the reply to, or
/// a local requester whose reply channel we deliver on. The local channel type
/// differs between relays (an `mpsc` for the multi-offset chunk stream, a
/// `oneshot` for a single preview), so it is supplied by the protocol.
pub(crate) enum Waiter<P: RelayProtocol> {
    /// A neighbour peer (by public key) that sent us a request; the reply is
    /// sent back to it as a wire frame.
    Peer(String),
    /// A local requester; the reply is delivered on its channel.
    Local(P::LocalReply),
}

/// One outstanding key we are relaying / awaiting.
pub(crate) struct WaiterEntry<P: RelayProtocol> {
    pub downstream: Vec<Waiter<P>>,
    /// Neighbours we forwarded the request to and have not yet heard a terminal
    /// reply from. When this drains to empty (all missed), we fan a miss
    /// downstream.
    pub upstream_outstanding: HashSet<String>,
    /// TTL; armed when the entry is created and not refreshed by coalescing
    /// joiners (so a stream of joiners can't keep a dead upstream alive).
    pub deadline: Instant,
}

/// The type-specific surface a relay must supply to the generic waiter table:
/// the key that identifies one in-flight payload, the local reply channel type,
/// the payload carried by data replies, and the three wire-frame constructors
/// (request / data / miss) plus the local-delivery methods.
///
/// Everything else — coalescing, fan-out, exhaustion, pruning, TTL — is the
/// same for both relays and lives in [`WaiterTable`].
pub(crate) trait RelayProtocol: Clone + Send + Sync + 'static {
    /// The content key identifying one canonical payload across all peers.
    type Key: Clone + Eq + Hash + Send + Sync;
    /// The channel a local requester's reply is delivered on.
    type LocalReply: Send;
    /// The payload carried by a `data` reply (bytes for a chunk, a `Preview`
    /// for a preview).
    type Data: Clone + Send;

    /// Split a key back into `(file_id, content_hash)` for logging and for
    /// building wire frames. (The chunk key also carries an offset, threaded
    /// through the frame constructors below.)
    fn key_parts(key: &Self::Key) -> (FileId, &str);

    /// Build the `…Request` frame this relay forwards upstream for `key`.
    fn request_frame(key: &Self::Key) -> Frame;
    /// Build the `…Data` frame forwarded to a downstream *peer* waiter.
    fn data_frame(key: &Self::Key, data: &Self::Data) -> Frame;
    /// Build the `…Miss` frame forwarded to a downstream *peer* waiter.
    fn miss_frame(key: &Self::Key) -> Frame;

    /// Deliver a `data` reply to a local waiter.
    fn deliver_data(reply: Self::LocalReply, key: &Self::Key, data: &Self::Data);
    /// Deliver a `miss` reply to a local waiter.
    fn deliver_miss(reply: Self::LocalReply, key: &Self::Key);

    /// Whether a local reply channel has been dropped by the requester. Used by
    /// [`WaiterTable::prune_link`] to drop dead local waiters; takes a
    /// reference (unlike the consuming delivery methods) so the check does not
    /// consume the channel. Both channel types expose `is_closed(&self)`.
    fn local_is_closed(reply: &Self::LocalReply) -> bool;
}

/// The generic content-keyed waiter table shared by both relays. Holds the map
/// of in-flight keys and the peer directory; every peer session holds a clone
/// (via the concrete relay wrapper) so requests forwarded on one session and
/// replies arriving on another share one table.
pub(crate) struct WaiterTable<P: RelayProtocol> {
    inner: Arc<Mutex<HashMap<P::Key, WaiterEntry<P>>>>,
    peers: PeerDirectory,
}

impl<P: RelayProtocol> Clone for WaiterTable<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            peers: self.peers.clone(),
        }
    }
}

impl<P: RelayProtocol> WaiterTable<P> {
    pub(crate) fn new(runtime_configuration: Arc<RwLock<RuntimeConfiguration>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            peers: PeerDirectory::new(runtime_configuration),
        }
    }

    pub(crate) fn peers(&self) -> &PeerDirectory {
        &self.peers
    }

    #[cfg(test)]
    pub(crate) async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }

    /// Register a downstream waiter for `key`, forwarding the request to
    /// `targets` if this creates the entry (coalescing onto an existing one
    /// otherwise). Shared by both the local-request and relay-request paths;
    /// the caller decides `targets` (a single preferred upstream, or a flood)
    /// and supplies the [`Waiter`].
    ///
    /// Returns whether a new entry (and thus an upstream forward) was created.
    async fn enqueue(&self, key: P::Key, waiter: Waiter<P>, targets: &[PeerOutbound]) -> bool {
        let (_, content_hash) = P::key_parts(&key);
        let short = short_hash(content_hash).to_owned();

        let mut newly_created = false;
        {
            let mut table = self.inner.lock().await;
            match table.get_mut(&key) {
                Some(entry) => {
                    entry.downstream.push(waiter);
                    log::trace!(
                        "relay[{short}]: request coalesced onto existing entry ({} downstream)",
                        entry.downstream.len()
                    );
                }
                None => {
                    let upstream_outstanding: HashSet<String> =
                        targets.iter().map(|peer| peer.public_key.clone()).collect();
                    log::debug!(
                        "relay[{short}]: new entry, forwarding to {} upstream {:?}",
                        upstream_outstanding.len(),
                        upstream_outstanding
                    );
                    table.insert(key.clone(), WaiterEntry {
                        downstream: vec![waiter],
                        upstream_outstanding,
                        deadline: Instant::now() + HOP_TIMEOUT,
                    });
                    newly_created = true;
                }
            }
        }

        if newly_created {
            let request = P::request_frame(&key);
            for peer in targets {
                let _ = peer.sender.send(request.clone());
            }
            self.arm_ttl(key);
        }
        newly_created
    }

    /// Fan a `data` reply to every downstream waiter of `key` and drop the
    /// entry (first-writer-wins). Late duplicates find no entry and are
    /// dropped.
    pub(crate) async fn handle_data(&self, key: P::Key, data: P::Data) {
        let (_, content_hash) = P::key_parts(&key);
        let short = short_hash(content_hash).to_owned();
        let entry = {
            let mut table = self.inner.lock().await;
            table.remove(&key)
        };
        let Some(entry) = entry else {
            log::trace!("relay[{short}]: data but no waiter entry (late/duplicate); dropping");
            return;
        };

        let mut local = 0usize;
        let mut peers = 0usize;
        for waiter in entry.downstream {
            match waiter {
                Waiter::Local(reply) => {
                    local += 1;
                    P::deliver_data(reply, &key, &data);
                }
                Waiter::Peer(public_key) => {
                    peers += 1;
                    if let Some(sender) = self.peers.peer_outbound(&public_key).await {
                        let _ = sender.send(P::data_frame(&key, &data));
                    }
                }
            }
        }
        log::debug!("relay[{short}]: data fanned to {local} local + {peers} peer waiter(s)");
    }

    /// Remove `from_public_key` from `key`'s `upstream_outstanding`; if that
    /// empties (all upstreams missed), fan a miss to all downstream waiters and
    /// drop the entry.
    pub(crate) async fn handle_miss(&self, from_public_key: &str, key: P::Key) {
        let (_, content_hash) = P::key_parts(&key);
        let short = short_hash(content_hash).to_owned();
        let exhausted = {
            let mut table = self.inner.lock().await;
            match table.get_mut(&key) {
                Some(entry) => {
                    entry.upstream_outstanding.remove(from_public_key);
                    if entry.upstream_outstanding.is_empty() {
                        log::debug!(
                            "relay[{short}]: miss from {from_public_key}: all upstreams \
                             exhausted; fanning miss down"
                        );
                        table.remove(&key)
                    } else {
                        log::trace!(
                            "relay[{short}]: miss from {from_public_key}: {} upstream(s) still \
                             outstanding",
                            entry.upstream_outstanding.len()
                        );
                        None
                    }
                }
                None => {
                    log::trace!(
                        "relay[{short}]: miss from {from_public_key} but no waiter entry; ignoring"
                    );
                    None
                }
            }
        };

        if let Some(entry) = exhausted {
            self.fan_miss(key, entry).await;
        }
    }

    /// Prune a dropped link from every entry's `downstream` and
    /// `upstream_outstanding`, applying the same emptying rules (an entry whose
    /// upstreams all vanished fans a miss down; an entry with no remaining
    /// downstream is dropped).
    pub(crate) async fn prune_link(&self, public_key: &str) {
        let mut exhausted: Vec<(P::Key, WaiterEntry<P>)> = Vec::new();
        {
            let mut table = self.inner.lock().await;
            let mut to_remove: Vec<P::Key> = Vec::new();
            for (key, entry) in table.iter_mut() {
                entry.downstream.retain(|waiter| match waiter {
                    Waiter::Peer(peer) => peer != public_key,
                    Waiter::Local(reply) => !P::local_is_closed(reply),
                });
                entry.upstream_outstanding.remove(public_key);
                if entry.downstream.is_empty() || entry.upstream_outstanding.is_empty() {
                    to_remove.push(key.clone());
                }
            }
            for key in to_remove {
                if let Some(entry) = table.remove(&key) {
                    exhausted.push((key, entry));
                }
            }
        }
        if !exhausted.is_empty() {
            log::debug!(
                "relay: link {public_key} dropped; failing {} affected waiter entry/entries",
                exhausted.len()
            );
        }
        for (key, entry) in exhausted {
            self.fan_miss(key, entry).await;
        }
    }

    /// Deliver a miss to every downstream waiter of a dropped entry.
    async fn fan_miss(&self, key: P::Key, entry: WaiterEntry<P>) {
        for waiter in entry.downstream {
            match waiter {
                Waiter::Local(reply) => P::deliver_miss(reply, &key),
                Waiter::Peer(public_key) => {
                    if let Some(sender) = self.peers.peer_outbound(&public_key).await {
                        let _ = sender.send(P::miss_frame(&key));
                    }
                }
            }
        }
    }

    /// Spawn a task that, after [`HOP_TIMEOUT`], drops `key` if it is still
    /// pending with the same deadline and fans a miss to its downstream
    /// waiters. The TTL is not refreshed by coalescing joiners.
    fn arm_ttl(&self, key: P::Key) {
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(HOP_TIMEOUT).await;
            let now = Instant::now();
            let entry = {
                let mut table = this.inner.lock().await;
                match table.get(&key) {
                    // Only expire if the deadline has actually passed (a fresh
                    // entry reusing the key after a drop would have a later
                    // deadline; leave it alone).
                    Some(entry) if entry.deadline <= now => table.remove(&key),
                    _ => None,
                }
            };
            if let Some(entry) = entry {
                let (_, content_hash) = P::key_parts(&key);
                log::debug!(
                    "relay[{}]: TTL expired; fanning miss to {} downstream waiter(s)",
                    short_hash(content_hash),
                    entry.downstream.len()
                );
                this.fan_miss(key, entry).await;
            }
        });
    }
}
