//! Where servable bytes come from. [`ChunkSource`] is the holder-side trait the
//! serve path reads chunks through.

use crate::file_bytes::FileBytes;

/// A source of file bytes a holder reads chunks from.
///
/// Dyn-compatible (boxed future), so a source can be held as
/// `Arc<dyn ChunkSource>`.
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
