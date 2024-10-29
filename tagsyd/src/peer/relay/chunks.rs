//! The content-keyed **chunk** relay: [`ChunkRelay`] wraps the generic
//! [`WaiterTable`] with the chunk-specific protocol and the temporary-provider
//! registry.
//!
//! A chunk's identity *is* `(file_id, content_hash, offset)`; nothing else
//! correlates a request to its reply. Local receivers (files this node is
//! pulling) are modelled as `Local` downstream waiters, so multi-source and
//! coalescing fall out for free: the receiver's `ChunkRequest`s go through the
//! same table as relayed ones.
//!
//! The provider registry (temporary local chunk sources, e.g. the CLI serving
//! an in-flight upload) lives here too, since providers are "things that can
//! answer a `ChunkRequest`".

use std::collections::HashMap;
use std::sync::Arc;

use tagsy_core::FileId;
use tagsy_core::state::{Frame, Sync as SyncMessage};
use tokio::sync::{Mutex, RwLock};

use crate::configuration::RuntimeConfiguration;
use crate::peer::relay::{PeerOutbound, RelayProtocol, Waiter, WaiterTable, short_hash};
use crate::peer::transfer::{ChunkReply, ChunkSource};

/// The content key identifying one canonical chunk across all peers.
type ChunkKey = (FileId, String, u64);

/// A registered temporary chunk provider (e.g. the CLI serving an upload).
type ProviderRegistry = HashMap<(FileId, String), Arc<dyn ChunkSource>>;

/// The chunk-specific protocol supplied to the generic waiter table.
#[derive(Clone)]
pub(crate) struct ChunkProtocol;

impl RelayProtocol for ChunkProtocol {
    type Data = Vec<u8>;
    type Key = ChunkKey;
    type LocalReply = tokio::sync::mpsc::UnboundedSender<ChunkReply>;

    fn key_parts(key: &Self::Key) -> (FileId, &str) {
        (key.0, key.1.as_str())
    }

    fn request_frame(key: &Self::Key) -> Frame {
        let (file_id, content_hash, offset) = key.clone();
        Frame::Sync(SyncMessage::ChunkRequest {
            file_id,
            content_hash,
            offset,
        })
    }

    fn data_frame(key: &Self::Key, data: &Self::Data) -> Frame {
        let (file_id, content_hash, offset) = key.clone();
        Frame::Sync(SyncMessage::ChunkData {
            file_id,
            content_hash,
            offset,
            bytes: data.clone(),
        })
    }

    fn miss_frame(key: &Self::Key) -> Frame {
        let (file_id, content_hash, offset) = key.clone();
        Frame::Sync(SyncMessage::ChunkMiss {
            file_id,
            content_hash,
            offset,
        })
    }

    fn deliver_data(reply: Self::LocalReply, key: &Self::Key, data: &Self::Data) {
        let _ = reply.send(ChunkReply::Data {
            offset: key.2,
            bytes: data.clone(),
        });
    }

    fn deliver_miss(reply: Self::LocalReply, key: &Self::Key) {
        let _ = reply.send(ChunkReply::Miss { offset: key.2 });
    }

    fn local_is_closed(reply: &Self::LocalReply) -> bool {
        reply.is_closed()
    }
}

/// The content-keyed chunk relay: the shared waiter table plus the provider
/// registry.
///
/// Cheap to clone (every field is an `Arc`); every peer session holds a clone
/// so requests forwarded on one session and replies arriving on another share
/// one table.
#[derive(Clone)]
pub struct ChunkRelay {
    table: WaiterTable<ChunkProtocol>,
    providers: Arc<Mutex<ProviderRegistry>>,
}

impl ChunkRelay {
    pub fn new(runtime_configuration: Arc<RwLock<RuntimeConfiguration>>) -> Self {
        Self {
            table: WaiterTable::new(runtime_configuration),
            providers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // ---- Provider registry ------------------------------------------------

    /// Register a temporary chunk provider (the CLI) for
    /// `file_id`/`content_hash`. A `ChunkRequest` for this file that no sync
    /// directory can serve will stream chunks from `source`.
    pub async fn register_provider(
        &self,
        file_id: FileId,
        content_hash: String,
        source: Arc<dyn ChunkSource>,
    ) {
        self.providers
            .lock()
            .await
            .insert((file_id, content_hash), source);
    }

    /// Remove a temporary provider (the client released the file).
    pub async fn unregister_provider(&self, file_id: FileId, content_hash: &str) {
        self.providers
            .lock()
            .await
            .remove(&(file_id, content_hash.to_owned()));
    }

    /// Look up a registered provider for `file_id`/`content_hash`.
    pub async fn provider_for(
        &self,
        file_id: FileId,
        content_hash: &str,
    ) -> Option<Arc<dyn ChunkSource>> {
        self.providers
            .lock()
            .await
            .get(&(file_id, content_hash.to_owned()))
            .cloned()
    }

    // ---- Local receiver requests ------------------------------------------

    /// Route a **local receiver's** `ChunkRequest` for `(file_id, content_hash,
    /// offset)`, delivering the eventual reply on `reply_tx`.
    ///
    /// `toward` is the routing policy: the neighbour most likely to hold the
    /// content (the announcing origin / last-good direction). When `Some`, the
    /// request is directed there; when `None`, it floods to all connected
    /// neighbours. Coalesces onto an existing entry for the same key.
    ///
    /// If there are no connected peers to ask, the reply is immediately a
    /// `ChunkMiss` (the receive then fails, as intended).
    pub async fn request_chunk_local(
        &self,
        file_id: FileId,
        content_hash: String,
        offset: u64,
        toward: Option<&str>,
        reply_tx: tokio::sync::mpsc::UnboundedSender<ChunkReply>,
    ) {
        let key = (file_id, content_hash.clone(), offset);

        // Choose the upstream neighbours to forward to.
        let targets: Vec<PeerOutbound> = match toward {
            Some(public_key) => match self.table.peers().peer_outbound(public_key).await {
                Some(sender) => vec![PeerOutbound {
                    public_key: public_key.to_owned(),
                    sender,
                }],
                // The preferred direction is gone; fall back to flooding.
                None => self.table.peers().connected_peers(None).await,
            },
            None => self.table.peers().connected_peers(None).await,
        };

        let short = short_hash(&content_hash);
        if targets.is_empty() {
            log::debug!(
                "relay[{short}]: local request offset={offset}: no connected peers; local miss"
            );
            let _ = reply_tx.send(ChunkReply::Miss { offset });
            return;
        }

        self.table
            .enqueue(key, Waiter::Local(reply_tx), &targets)
            .await;
    }

    // ---- Inbound frame handling (relay) -----------------------------------

    /// Handle an inbound `ChunkRequest` from `from_public_key` that this node
    /// could not serve locally (the caller already checked its sync directories
    /// / providers). Coalesce onto an existing entry, or forward to all
    /// neighbours except the sender. With no other neighbours, answer
    /// `ChunkMiss` straight back.
    pub async fn relay_chunk_request(
        &self,
        from_public_key: &str,
        file_id: FileId,
        content_hash: String,
        offset: u64,
    ) {
        let key = (file_id, content_hash.clone(), offset);
        let short = short_hash(&content_hash);

        let peers = self
            .table
            .peers()
            .connected_peers(Some(from_public_key))
            .await;
        if peers.is_empty() {
            log::debug!(
                "relay[{short}]: request offset={offset} from {from_public_key}: no other \
                 neighbours; miss back"
            );
            if let Some(sender) = self.table.peers().peer_outbound(from_public_key).await {
                let _ = sender.send(Frame::Sync(SyncMessage::ChunkMiss {
                    file_id,
                    content_hash,
                    offset,
                }));
            }
            return;
        }

        self.table
            .enqueue(key, Waiter::Peer(from_public_key.to_owned()), &peers)
            .await;
    }

    /// Handle an inbound `ChunkData` from an upstream. Fan the bytes to every
    /// downstream waiter and drop the entry (first-writer-wins). Late
    /// duplicates find no entry and are dropped.
    pub async fn handle_chunk_data(
        &self,
        file_id: FileId,
        content_hash: String,
        offset: u64,
        bytes: Vec<u8>,
    ) {
        self.table
            .handle_data((file_id, content_hash, offset), bytes)
            .await;
    }

    /// Handle an inbound `ChunkMiss` from `from_public_key`. Remove it from the
    /// entry's `upstream_outstanding`; if that empties (all upstreams missed),
    /// fan `ChunkMiss` to all downstream waiters and drop the entry.
    pub async fn handle_chunk_miss(
        &self,
        from_public_key: &str,
        file_id: FileId,
        content_hash: String,
        offset: u64,
    ) {
        self.table
            .handle_miss(from_public_key, (file_id, content_hash, offset))
            .await;
    }

    /// Prune a dropped link from every entry, failing any waiter that was only
    /// reachable through it (rather than hanging until the TTL).
    pub async fn prune_link(&self, public_key: &str) {
        self.table.prune_link(public_key).await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::peer::relay::testing::{engine_with_peers, runtime_for_test};
    use crate::peer::transfer::HOP_TIMEOUT;

    fn engine() -> ChunkRelay {
        ChunkRelay::new(runtime_for_test())
    }

    async fn engine_with_n_peers(
        count: usize,
    ) -> (
        ChunkRelay,
        Vec<(String, tokio::sync::mpsc::UnboundedReceiver<Frame>)>,
    ) {
        let (runtime, peers) = engine_with_peers(count).await;
        (ChunkRelay::new(runtime), peers)
    }

    /// With no connected peers, a local request immediately misses.
    #[tokio::test]
    async fn local_request_no_peers_misses() {
        let engine = engine();
        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel();
        engine
            .request_chunk_local(FileId::new(), "hash".to_owned(), 0, None, reply_tx)
            .await;
        match reply_rx.recv().await {
            Some(ChunkReply::Miss { offset }) => assert_eq!(offset, 0),
            other => panic!("expected miss, got {other:?}"),
        }
    }

    /// Provider register / lookup / unregister round-trips.
    #[tokio::test]
    async fn provider_registry_roundtrip() {
        let engine = engine();
        let file_id = FileId::new();
        let source: Arc<dyn ChunkSource> =
            Arc::new(crate::file_bytes::FileBytes::InMemory(b"x".to_vec()));
        engine
            .register_provider(file_id, "hash".to_owned(), source)
            .await;
        assert!(engine.provider_for(file_id, "hash").await.is_some());
        engine.unregister_provider(file_id, "hash").await;
        assert!(engine.provider_for(file_id, "hash").await.is_none());
    }

    /// Coalescing: two downstream waiters for the same key cause exactly one
    /// upstream fetch, and a single `ChunkData` fans out to both.
    #[tokio::test]
    async fn coalesces_and_fans_out() {
        let (engine, mut peers) = engine_with_n_peers(1).await;
        let upstream = peers[0].0.clone();
        let file_id = FileId::new();

        // Two local receivers request the same (file, hash, offset).
        let (reply_a_tx, mut reply_a_rx) = tokio::sync::mpsc::unbounded_channel();
        let (reply_b_tx, mut reply_b_rx) = tokio::sync::mpsc::unbounded_channel();
        engine
            .request_chunk_local(file_id, "h".to_owned(), 0, Some(&upstream), reply_a_tx)
            .await;
        engine
            .request_chunk_local(file_id, "h".to_owned(), 0, Some(&upstream), reply_b_tx)
            .await;

        // Exactly one ChunkRequest forwarded upstream (coalesced).
        let upstream_rx = &mut peers[0].1;
        assert!(matches!(
            upstream_rx.recv().await,
            Some(Frame::Sync(SyncMessage::ChunkRequest { offset: 0, .. }))
        ));
        assert!(
            upstream_rx.try_recv().is_err(),
            "second request not coalesced"
        );

        // The upstream answers once; both downstreams get the bytes.
        engine
            .handle_chunk_data(file_id, "h".to_owned(), 0, b"payload".to_vec())
            .await;
        match reply_a_rx.recv().await {
            Some(ChunkReply::Data { bytes, .. }) => assert_eq!(bytes, b"payload"),
            other => panic!("A: expected data, got {other:?}"),
        }
        match reply_b_rx.recv().await {
            Some(ChunkReply::Data { bytes, .. }) => assert_eq!(bytes, b"payload"),
            other => panic!("B: expected data, got {other:?}"),
        }
    }

    /// Exhaustion: a `ChunkMiss` from every upstream fans `ChunkMiss` down.
    #[tokio::test]
    async fn exhaustion_fans_miss() {
        let (engine, mut peers) = engine_with_n_peers(2).await;
        let file_id = FileId::new();

        // A relayed request from a *third* peer floods to peer0 and peer1.
        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel();
        engine
            .request_chunk_local(file_id, "h".to_owned(), 0, None, reply_tx)
            .await;
        // Both upstreams saw the request.
        assert!(peers[0].1.recv().await.is_some());
        assert!(peers[1].1.recv().await.is_some());

        // First miss: not yet exhausted, no downstream reply.
        engine
            .handle_chunk_miss("peer0", file_id, "h".to_owned(), 0)
            .await;
        assert!(reply_rx.try_recv().is_err());

        // Second (last) miss: exhausted, fan ChunkMiss to the local waiter.
        engine
            .handle_chunk_miss("peer1", file_id, "h".to_owned(), 0)
            .await;
        assert!(matches!(
            reply_rx.recv().await,
            Some(ChunkReply::Miss { offset: 0 })
        ));
    }

    /// Link-drop pruning: dropping the only upstream fails the downstream
    /// waiter (rather than hanging until the TTL).
    #[tokio::test]
    async fn link_drop_prunes_and_fails() {
        let (engine, mut peers) = engine_with_n_peers(1).await;
        let upstream = peers[0].0.clone();
        let file_id = FileId::new();

        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel();
        engine
            .request_chunk_local(file_id, "h".to_owned(), 0, Some(&upstream), reply_tx)
            .await;
        assert!(peers[0].1.recv().await.is_some());

        engine.prune_link(&upstream).await;
        assert!(matches!(
            reply_rx.recv().await,
            Some(ChunkReply::Miss { offset: 0 })
        ));
    }

    /// TTL expiry fans `ChunkMiss` to downstream waiters when no upstream ever
    /// answers.
    #[tokio::test(start_paused = true)]
    async fn ttl_expiry_fans_miss() {
        let (engine, mut peers) = engine_with_n_peers(1).await;
        let upstream = peers[0].0.clone();
        let file_id = FileId::new();

        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::unbounded_channel();
        engine
            .request_chunk_local(file_id, "h".to_owned(), 0, Some(&upstream), reply_tx)
            .await;
        assert!(peers[0].1.recv().await.is_some());

        // Advance past the TTL; the armed task expires the entry.
        tokio::time::advance(HOP_TIMEOUT + Duration::from_millis(1)).await;
        // Let the spawned TTL task run.
        tokio::task::yield_now().await;
        match reply_rx.recv().await {
            Some(ChunkReply::Miss { offset: 0 }) => {}
            other => panic!("expected miss from TTL, got {other:?}"),
        }
    }

    /// A relay holds no byte buffers: the waiter table only tracks link
    /// handles, never bytes.
    #[tokio::test]
    async fn relay_holds_no_bytes() {
        let (engine, mut peers) = engine_with_n_peers(2).await;
        let file_id = FileId::new();

        // peer0 asks us; we don't hold it, so we relay to peer1.
        engine
            .relay_chunk_request("peer0", file_id, "h".to_owned(), 0)
            .await;
        // peer1 (the only non-sender neighbour) got the forwarded request.
        assert!(matches!(
            peers[1].1.recv().await,
            Some(Frame::Sync(SyncMessage::ChunkRequest { offset: 0, .. }))
        ));
        // peer0 (the sender) is never echoed to.
        assert!(peers[0].1.try_recv().is_err());

        // When peer1 answers, the bytes are forwarded straight to peer0 and the
        // entry is dropped — nothing is cached.
        engine
            .handle_chunk_data(file_id, "h".to_owned(), 0, b"bytes".to_vec())
            .await;
        assert!(matches!(
            peers[0].1.recv().await,
            Some(Frame::Sync(SyncMessage::ChunkData { .. }))
        ));
        assert!(engine.table.is_empty().await);
    }
}
