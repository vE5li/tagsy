//! Where servable bytes come from. [`ChunkSource`] is the holder-side trait the
//! serve path reads chunks through; its impls cover a local on-disk
//! [`FileBytes`] and a remote [`ProviderSource`] (e.g. the CLI over the control
//! socket).

use tokio::sync::mpsc::UnboundedSender;

use crate::file_bytes::FileBytes;

/// A source of file bytes a holder reads chunks from.
///
/// Dyn-compatible (boxed future) so the provider registry can hold an
/// `Arc<dyn ChunkSource>` — a local on-disk [`FileBytes`] or a remote provider
/// such as the CLI over the control socket — behind one type.
pub type ChunkFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(Vec<u8>, bool), String>> + Send + 'a>,
>;

pub trait ChunkSource: Send + Sync {
    /// Read up to `max_len` bytes at `offset`, returning the bytes and whether
    /// this chunk reaches the end of the content.
    fn read_chunk_at(&self, offset: u64, max_len: usize) -> ChunkFuture<'_>;
}

impl ChunkSource for std::sync::Arc<dyn ChunkSource> {
    fn read_chunk_at(&self, offset: u64, max_len: usize) -> ChunkFuture<'_> {
        (**self).read_chunk_at(offset, max_len)
    }
}

impl ChunkSource for FileBytes {
    fn read_chunk_at(&self, offset: u64, max_len: usize) -> ChunkFuture<'_> {
        Box::pin(async move {
            FileBytes::read_chunk_at(self, offset, max_len)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

/// The one-shot a provider chunk reply is delivered on: `(bytes, is_last)` or
/// an error string.
pub type ProviderChunkReply = tokio::sync::oneshot::Sender<Result<(Vec<u8>, bool), String>>;

/// One chunk request routed to a remote provider (the CLI over the control
/// socket): the requested `offset` and the one-shot to deliver the reply on.
pub type ProviderChunkRequest = (u64, ProviderChunkReply);

/// A [`ChunkSource`] backed by a remote provider reached over a request channel
/// (e.g. the CLI over the control connection). Each `read_chunk_at` sends a
/// [`ProviderChunkRequest`] and awaits the reply, so the whole file is never
/// buffered daemon-side.
///
/// When it observes the final chunk (`last == true`) it fires `on_complete`
/// once, so the daemon can signal the client to release the file after the
/// bytes have been served.
#[derive(Clone)]
pub struct ProviderSource {
    requests: UnboundedSender<ProviderChunkRequest>,
    on_complete: UnboundedSender<()>,
}

impl ProviderSource {
    pub fn new(
        requests: UnboundedSender<ProviderChunkRequest>,
        on_complete: UnboundedSender<()>,
    ) -> Self {
        Self {
            requests,
            on_complete,
        }
    }
}

impl ChunkSource for ProviderSource {
    fn read_chunk_at(&self, offset: u64, _max_len: usize) -> ChunkFuture<'_> {
        Box::pin(async move {
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            self.requests
                .send((offset, reply_tx))
                .map_err(|_| "provider gone".to_owned())?;
            let (bytes, last) = reply_rx
                .await
                .map_err(|_| "provider dropped before replying".to_owned())??;
            if last {
                let _ = self.on_complete.send(());
            }
            Ok((bytes, last))
        })
    }
}
