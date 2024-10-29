//! The content-keyed **preview** relay: [`PreviewRelay`] wraps the generic
//! [`WaiterTable`] with the preview-specific protocol.
//!
//! The preview analogue of the [chunk relay](super::chunks), but simpler: a
//! preview is one small blob, not a windowed byte stream, so there is no
//! offset, no chunking, no integrity re-hashing, and no provider registry. A
//! preview's request identity is `(file_id, content_hash)`; a holder either
//! produces a preview of that exact content ([`Sync::PreviewData`]) or misses
//! ([`Sync::PreviewMiss`]).
//!
//! A missed preview is *not* an error to the caller: a local request that
//! exhausts every direction resolves to [`Preview::None`] (see
//! `PreviewReply::Miss` handling at the call site).
//!
//! [`Sync::PreviewData`]: tagsy_core::state::Sync::PreviewData
//! [`Sync::PreviewMiss`]: tagsy_core::state::Sync::PreviewMiss

use std::sync::Arc;

use tagsy_core::state::{Frame, Sync as SyncMessage};
use tagsy_core::{FileId, Preview};
use tokio::sync::RwLock;

use crate::configuration::RuntimeConfiguration;
use crate::peer::relay::{RelayProtocol, Waiter, WaiterTable};

/// The content key identifying one canonical preview across all peers.
type PreviewKey = (FileId, String);

/// The outcome delivered to a local preview requester.
#[derive(Debug, Clone)]
pub enum PreviewReply {
    /// A peer produced a preview of the requested content.
    Data(Preview),
    /// No reachable peer could serve a preview of this content.
    Miss,
}

/// The preview-specific protocol supplied to the generic waiter table.
#[derive(Clone)]
pub(crate) struct PreviewProtocol;

impl RelayProtocol for PreviewProtocol {
    type Data = Preview;
    type Key = PreviewKey;
    type LocalReply = tokio::sync::oneshot::Sender<PreviewReply>;

    fn key_parts(key: &Self::Key) -> (FileId, &str) {
        (key.0, key.1.as_str())
    }

    fn request_frame(key: &Self::Key) -> Frame {
        let (file_id, content_hash) = key.clone();
        Frame::Sync(SyncMessage::PreviewRequest {
            file_id,
            content_hash,
        })
    }

    fn data_frame(key: &Self::Key, data: &Self::Data) -> Frame {
        let (file_id, content_hash) = key.clone();
        Frame::Sync(SyncMessage::PreviewData {
            file_id,
            content_hash,
            preview: data.clone(),
        })
    }

    fn miss_frame(key: &Self::Key) -> Frame {
        let (file_id, content_hash) = key.clone();
        Frame::Sync(SyncMessage::PreviewMiss {
            file_id,
            content_hash,
        })
    }

    fn deliver_data(reply: Self::LocalReply, _key: &Self::Key, data: &Self::Data) {
        let _ = reply.send(PreviewReply::Data(data.clone()));
    }

    fn deliver_miss(reply: Self::LocalReply, _key: &Self::Key) {
        let _ = reply.send(PreviewReply::Miss);
    }

    fn local_is_closed(reply: &Self::LocalReply) -> bool {
        reply.is_closed()
    }
}

/// The content-keyed preview relay. Cheap to clone; every peer session holds a
/// clone so a request forwarded on one session and a reply arriving on another
/// share one table.
#[derive(Clone)]
pub struct PreviewRelay {
    table: WaiterTable<PreviewProtocol>,
}

impl PreviewRelay {
    pub fn new(runtime_configuration: Arc<RwLock<RuntimeConfiguration>>) -> Self {
        Self {
            table: WaiterTable::new(runtime_configuration),
        }
    }

    // ---- Local requests ---------------------------------------------------

    /// Route a **local** preview request for `(file_id, content_hash)`,
    /// delivering the eventual reply on `reply_tx`.
    ///
    /// Floods to every connected neighbour (unlike chunks, there is no
    /// "announcing origin" direction to prefer). Coalesces onto an existing
    /// entry for the same key. With no connected peers, replies `Miss`
    /// immediately.
    pub async fn request_preview_local(
        &self,
        file_id: FileId,
        content_hash: String,
        reply_tx: tokio::sync::oneshot::Sender<PreviewReply>,
    ) {
        let key = (file_id, content_hash);
        let peers = self.table.peers().connected_peers(None).await;

        if peers.is_empty() {
            let _ = reply_tx.send(PreviewReply::Miss);
            return;
        }

        self.table
            .enqueue(key, Waiter::Local(reply_tx), &peers)
            .await;
    }

    // ---- Inbound frame handling (relay) -----------------------------------

    /// Handle an inbound `PreviewRequest` from `from_public_key` that this node
    /// could not serve locally (the caller already checked local presence).
    /// Coalesce onto an existing entry, or forward to all neighbours except the
    /// sender. With no other neighbours, answer `PreviewMiss` straight back.
    pub async fn relay_preview_request(
        &self,
        from_public_key: &str,
        file_id: FileId,
        content_hash: String,
    ) {
        let key = (file_id, content_hash.clone());
        let peers = self
            .table
            .peers()
            .connected_peers(Some(from_public_key))
            .await;

        if peers.is_empty() {
            if let Some(sender) = self.table.peers().peer_outbound(from_public_key).await {
                let _ = sender.send(Frame::Sync(SyncMessage::PreviewMiss {
                    file_id,
                    content_hash,
                }));
            }
            return;
        }

        self.table
            .enqueue(key, Waiter::Peer(from_public_key.to_owned()), &peers)
            .await;
    }

    /// Handle an inbound `PreviewData` from an upstream. Fan it to every
    /// downstream waiter and drop the entry (first-responder-wins). Late
    /// duplicates find no entry and are dropped.
    pub async fn handle_preview_data(
        &self,
        file_id: FileId,
        content_hash: String,
        preview: Preview,
    ) {
        self.table
            .handle_data((file_id, content_hash), preview)
            .await;
    }

    /// Handle an inbound `PreviewMiss` from `from_public_key`. Remove it from
    /// the entry's `upstream_outstanding`; if that empties, fan `PreviewMiss`
    /// to all downstream waiters and drop the entry.
    pub async fn handle_preview_miss(
        &self,
        from_public_key: &str,
        file_id: FileId,
        content_hash: String,
    ) {
        self.table
            .handle_miss(from_public_key, (file_id, content_hash))
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
    use super::*;
    use crate::peer::relay::testing::{engine_with_peers, runtime_for_test};

    fn engine() -> PreviewRelay {
        PreviewRelay::new(runtime_for_test())
    }

    async fn engine_with_n_peers(
        count: usize,
    ) -> (
        PreviewRelay,
        Vec<(String, tokio::sync::mpsc::UnboundedReceiver<Frame>)>,
    ) {
        let (runtime, peers) = engine_with_peers(count).await;
        (PreviewRelay::new(runtime), peers)
    }

    #[tokio::test]
    async fn local_request_no_peers_misses() {
        let engine = engine();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        engine
            .request_preview_local(FileId::new(), "hash".to_owned(), reply_tx)
            .await;
        assert!(matches!(reply_rx.await, Ok(PreviewReply::Miss)));
    }

    #[tokio::test]
    async fn coalesces_and_fans_out() {
        let (engine, mut peers) = engine_with_n_peers(1).await;
        let file_id = FileId::new();

        let (reply_a_tx, reply_a_rx) = tokio::sync::oneshot::channel();
        let (reply_b_tx, reply_b_rx) = tokio::sync::oneshot::channel();
        engine
            .request_preview_local(file_id, "h".to_owned(), reply_a_tx)
            .await;
        engine
            .request_preview_local(file_id, "h".to_owned(), reply_b_tx)
            .await;

        // Exactly one PreviewRequest forwarded upstream (coalesced).
        let upstream_rx = &mut peers[0].1;
        assert!(matches!(
            upstream_rx.recv().await,
            Some(Frame::Sync(SyncMessage::PreviewRequest { .. }))
        ));
        assert!(upstream_rx.try_recv().is_err(), "second not coalesced");

        engine
            .handle_preview_data(file_id, "h".to_owned(), Preview::Text("hi".to_owned()))
            .await;
        assert!(matches!(
            reply_a_rx.await,
            Ok(PreviewReply::Data(Preview::Text(text))) if text == "hi"
        ));
        assert!(matches!(
            reply_b_rx.await,
            Ok(PreviewReply::Data(Preview::Text(text))) if text == "hi"
        ));
    }

    #[tokio::test]
    async fn exhaustion_fans_miss() {
        let (engine, mut peers) = engine_with_n_peers(2).await;
        let file_id = FileId::new();

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        engine
            .request_preview_local(file_id, "h".to_owned(), reply_tx)
            .await;
        assert!(peers[0].1.recv().await.is_some());
        assert!(peers[1].1.recv().await.is_some());

        engine
            .handle_preview_miss("peer0", file_id, "h".to_owned())
            .await;
        engine
            .handle_preview_miss("peer1", file_id, "h".to_owned())
            .await;
        assert!(matches!(reply_rx.await, Ok(PreviewReply::Miss)));
    }

    #[tokio::test]
    async fn relay_forwards_and_drops() {
        let (engine, mut peers) = engine_with_n_peers(2).await;
        let file_id = FileId::new();

        engine
            .relay_preview_request("peer0", file_id, "h".to_owned())
            .await;
        assert!(matches!(
            peers[1].1.recv().await,
            Some(Frame::Sync(SyncMessage::PreviewRequest { .. }))
        ));
        assert!(peers[0].1.try_recv().is_err());

        engine
            .handle_preview_data(file_id, "h".to_owned(), Preview::None)
            .await;
        assert!(matches!(
            peers[0].1.recv().await,
            Some(Frame::Sync(SyncMessage::PreviewData { .. }))
        ));
        assert!(engine.table.is_empty().await);
    }
}
