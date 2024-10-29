//! The content-addressed receiver driver: [`receive`] keeps a window of
//! [`ChunkRequest`]s in flight, streams [`ChunkReply`]s into a temp file with
//! incremental BLAKE3, and verifies the whole file against `content_hash` at
//! the end.

use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::file_bytes::FileBytes;
use crate::peer::transfer::{
    CHUNK_SIZE, ChunkReply, ChunkRequest, HOP_TIMEOUT, ProgressSink, TransferError, WINDOW,
    short_hash,
};

/// Drive a **content-addressed receive** to completion.
///
/// - Keeps up to [`WINDOW`] `ChunkRequest`s in flight, each emitted on
///   `requests` (the peer session routes them toward a holder).
/// - Streams `ChunkReply::Data` into a temp file at `temp_path` in offset order
///   (buffering out-of-order replies), hashing incrementally.
/// - Terminates and verifies when `expected_size` bytes have been written; a
///   zero-length file is one request at `offset = 0` returning empty bytes.
/// - A `ChunkReply::Miss` for any offset, a closed reply channel, or the
///   per-chunk liveness timeout fails the receive immediately (no retry, no
///   re-flood; recovery is external).
///
/// On success the temp file *is* the content, returned as a
/// [`FileBytes::FileToMove`] so the caller can rename it into place. On any
/// error the temp file is removed.
pub async fn receive(
    content_hash: String,
    expected_size: u64,
    temp_path: PathBuf,
    requests: UnboundedSender<ChunkRequest>,
    mut replies: UnboundedReceiver<ChunkReply>,
    progress: Option<ProgressSink>,
) -> Result<FileBytes, TransferError> {
    let result = receive_inner(
        &content_hash,
        expected_size,
        &temp_path,
        &requests,
        &mut replies,
        progress.as_ref(),
    )
    .await;

    if result.is_err() {
        let _ = tokio::fs::remove_file(&temp_path).await;
    }

    result.map(|()| FileBytes::FileToMove(temp_path))
}

async fn receive_inner(
    content_hash: &str,
    expected_size: u64,
    temp_path: &Path,
    requests: &UnboundedSender<ChunkRequest>,
    replies: &mut UnboundedReceiver<ChunkReply>,
    progress: Option<&ProgressSink>,
) -> Result<(), TransferError> {
    let short = short_hash(content_hash);
    let mut file = tokio::fs::File::create(temp_path)
        .await
        .map_err(TransferError::Io)?;
    let mut hasher = blake3::Hasher::new();

    // A zero-length file still needs exactly one request (offset 0) to receive
    // the empty chunk; otherwise never request an offset at or beyond the
    // authoritative size.
    let request_ceiling = expected_size.max(1);
    let may_request = |offset: u64| offset < request_ceiling;
    let total_chunks = expected_size.max(1).div_ceil(CHUNK_SIZE as u64);

    log::debug!(
        "receive[{short}]: start; expected_size={expected_size} ({total_chunks} chunk(s)), \
         window={WINDOW}"
    );

    let mut next_request_offset: u64 = 0;
    let mut in_flight: u64 = 0;
    let mut write_offset: u64 = 0;
    let mut pending: std::collections::BTreeMap<u64, Vec<u8>> = Default::default();

    // Prime the window, capped so we never request past EOF.
    while in_flight < WINDOW && may_request(next_request_offset) {
        log::trace!(
            "receive[{short}]: request offset={next_request_offset} (priming, in_flight will be \
             {})",
            in_flight + 1
        );
        requests
            .send(ChunkRequest {
                offset: next_request_offset,
            })
            .map_err(|_| TransferError::ChannelClosed)?;
        next_request_offset += CHUNK_SIZE as u64;
        in_flight += 1;
    }
    log::debug!("receive[{short}]: primed {in_flight} request(s) in flight");

    loop {
        // Per-chunk liveness timeout: reset on each successful write (below).
        // A connected-but-silent peer trips this rather than hanging forever.
        let message = match tokio::time::timeout(HOP_TIMEOUT, replies.recv()).await {
            Ok(Some(message)) => message,
            Ok(None) => {
                log::warn!(
                    "receive[{short}]: reply channel closed at \
                     write_offset={write_offset}/{expected_size}"
                );
                return Err(TransferError::ChannelClosed);
            }
            Err(_) => {
                log::warn!(
                    "receive[{short}]: liveness timeout ({:?}) at \
                     write_offset={write_offset}/{expected_size}, in_flight={in_flight}",
                    HOP_TIMEOUT
                );
                return Err(TransferError::LivenessTimeout);
            }
        };

        match message {
            ChunkReply::Data { offset, bytes } => {
                let len = bytes.len();
                in_flight = in_flight.saturating_sub(1);
                // Duplicate for an already-written offset: drop it (races are
                // free — bytes for a key are bit-identical).
                if offset < write_offset {
                    log::trace!(
                        "receive[{short}]: duplicate data offset={offset} ({len} bytes) already \
                         written; dropping"
                    );
                    continue;
                }
                log::trace!(
                    "receive[{short}]: got data offset={offset} ({len} bytes), in_flight now \
                     {in_flight}"
                );
                pending.entry(offset).or_insert(bytes);

                // Flush any contiguous chunks starting at write_offset.
                while let Some(chunk) = pending.remove(&write_offset) {
                    hasher.update(&chunk);
                    file.write_all(&chunk).await.map_err(TransferError::Io)?;
                    write_offset += chunk.len() as u64;
                    if let Some(report) = progress {
                        report(write_offset, Some(expected_size));
                    }
                }

                // Completion: we have written the whole file.
                if write_offset >= expected_size {
                    file.flush().await.map_err(TransferError::Io)?;
                    let actual = hasher.finalize().to_hex().to_string();
                    if actual == content_hash {
                        log::debug!(
                            "receive[{short}]: complete; wrote {write_offset} bytes, hash verified"
                        );
                        return Ok(());
                    }
                    log::warn!(
                        "receive[{short}]: hash mismatch after {write_offset} bytes (got {})",
                        short_hash(&actual)
                    );
                    return Err(TransferError::HashMismatch {
                        expected: content_hash.to_owned(),
                        actual,
                    });
                }

                // Refill the window, capped so we never request past EOF.
                while in_flight < WINDOW && may_request(next_request_offset) {
                    log::trace!(
                        "receive[{short}]: request offset={next_request_offset} (refill, \
                         in_flight will be {})",
                        in_flight + 1
                    );
                    requests
                        .send(ChunkRequest {
                            offset: next_request_offset,
                        })
                        .map_err(|_| TransferError::ChannelClosed)?;
                    next_request_offset += CHUNK_SIZE as u64;
                    in_flight += 1;
                }
            }
            ChunkReply::Miss { offset } => {
                // A miss for an already-written offset (a late duplicate) is
                // harmless: ignore it. Otherwise the chunk is unavailable from
                // every direction the peer session tried, so the receive fails.
                if offset < write_offset {
                    log::trace!(
                        "receive[{short}]: stale miss offset={offset} already written; ignoring"
                    );
                    continue;
                }
                log::warn!(
                    "receive[{short}]: chunk offset={offset} unavailable from any peer at \
                     write_offset={write_offset}/{expected_size}"
                );
                return Err(TransferError::ChunkUnavailable { offset });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn temp_path(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "tagsy-transfer-test-{}-{}-{}",
            label,
            std::process::id(),
            unique
        ))
    }

    /// A stateless chunk-answering stub: serves canonical chunks of `bytes`.
    /// Drives a [`receive`] against it, answering each request from `bytes`.
    async fn drive_receive_from(bytes: Vec<u8>) -> Result<Vec<u8>, TransferError> {
        let content_hash = blake3::hash(&bytes).to_hex().to_string();
        let size = bytes.len() as u64;
        let dest = temp_path("dest");

        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();

        let serve_bytes = bytes.clone();
        let server = tokio::spawn(async move {
            while let Some(ChunkRequest { offset }) = req_rx.recv().await {
                let start = (offset as usize).min(serve_bytes.len());
                let end = (start + CHUNK_SIZE).min(serve_bytes.len());
                let chunk = serve_bytes[start..end].to_vec();
                if reply_tx
                    .send(ChunkReply::Data {
                        offset,
                        bytes: chunk,
                    })
                    .is_err()
                {
                    break;
                }
            }
        });

        let received = receive(content_hash, size, dest.clone(), req_tx, reply_rx, None).await;
        let _ = server.await;

        let result = received.map(|file_bytes| {
            let path = file_bytes.path().unwrap().to_path_buf();
            std::fs::read(&path).unwrap()
        });
        let _ = std::fs::remove_file(&dest);
        result
    }

    #[tokio::test]
    async fn receive_small() {
        let bytes = b"hello transfer".to_vec();
        assert_eq!(drive_receive_from(bytes.clone()).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn receive_empty() {
        assert!(drive_receive_from(Vec::new()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn receive_multi_chunk() {
        let bytes: Vec<u8> = (0..(CHUNK_SIZE * 5 + 123)).map(|i| i as u8).collect();
        assert_eq!(drive_receive_from(bytes.clone()).await.unwrap(), bytes);
    }

    /// A duplicate `Data` for an already-written offset is ignored, and the
    /// receive still completes with the correct hash.
    #[tokio::test]
    async fn duplicate_data_ignored() {
        let bytes: Vec<u8> = (0..(CHUNK_SIZE + 10)).map(|i| i as u8).collect();
        let content_hash = blake3::hash(&bytes).to_hex().to_string();
        let size = bytes.len() as u64;
        let dest = temp_path("dup");

        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();

        let serve_bytes = bytes.clone();
        tokio::spawn(async move {
            while let Some(ChunkRequest { offset }) = req_rx.recv().await {
                let start = (offset as usize).min(serve_bytes.len());
                let end = (start + CHUNK_SIZE).min(serve_bytes.len());
                let chunk = serve_bytes[start..end].to_vec();
                // Answer twice for offset 0 to exercise the dedup path.
                let _ = reply_tx.send(ChunkReply::Data {
                    offset,
                    bytes: chunk.clone(),
                });
                if offset == 0 {
                    let _ = reply_tx.send(ChunkReply::Data {
                        offset,
                        bytes: chunk,
                    });
                }
            }
        });

        let received = receive(content_hash, size, dest.clone(), req_tx, reply_rx, None)
            .await
            .map(|fb| std::fs::read(fb.path().unwrap()).unwrap());
        let _ = std::fs::remove_file(&dest);
        assert_eq!(received.unwrap(), bytes);
    }

    /// A total miss mid-stream fails the receive immediately (no retry).
    #[tokio::test]
    async fn total_miss_fails() {
        let bytes: Vec<u8> = (0..(CHUNK_SIZE * 2)).map(|i| i as u8).collect();
        let content_hash = blake3::hash(&bytes).to_hex().to_string();
        let size = bytes.len() as u64;
        let dest = temp_path("miss");

        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();

        let serve_bytes = bytes.clone();
        tokio::spawn(async move {
            while let Some(ChunkRequest { offset }) = req_rx.recv().await {
                if offset == 0 {
                    let end = CHUNK_SIZE.min(serve_bytes.len());
                    let _ = reply_tx.send(ChunkReply::Data {
                        offset,
                        bytes: serve_bytes[..end].to_vec(),
                    });
                } else {
                    let _ = reply_tx.send(ChunkReply::Miss { offset });
                }
            }
        });

        let received = receive(content_hash, size, dest.clone(), req_tx, reply_rx, None).await;
        assert!(matches!(
            received,
            Err(TransferError::ChunkUnavailable { .. })
        ));
        assert!(!dest.exists());
    }

    /// A wrong expected hash fails the receive after all bytes arrive.
    #[tokio::test]
    async fn hash_mismatch_rejected() {
        let bytes = b"real bytes".to_vec();
        let wrong_hash = blake3::hash(b"different").to_hex().to_string();
        let size = bytes.len() as u64;
        let dest = temp_path("mismatch");

        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();

        let serve_bytes = bytes.clone();
        tokio::spawn(async move {
            while let Some(ChunkRequest { offset }) = req_rx.recv().await {
                let start = (offset as usize).min(serve_bytes.len());
                let end = (start + CHUNK_SIZE).min(serve_bytes.len());
                let _ = reply_tx.send(ChunkReply::Data {
                    offset,
                    bytes: serve_bytes[start..end].to_vec(),
                });
            }
        });

        let received = receive(wrong_hash, size, dest.clone(), req_tx, reply_rx, None).await;
        assert!(matches!(received, Err(TransferError::HashMismatch { .. })));
        assert!(!dest.exists());
    }

    /// A per-chunk no-progress stall trips the liveness timeout and fails: a
    /// *connected* peer accepted the request but never answers, and both the
    /// request and reply channels stay open (only the liveness guard can fire).
    #[tokio::test(start_paused = true)]
    async fn liveness_timeout_fails() {
        let size = (CHUNK_SIZE * 2) as u64;
        let content_hash = blake3::hash(&vec![0u8; size as usize]).to_hex().to_string();
        let dest = temp_path("stall");

        let (req_tx, req_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkRequest>();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkReply>();
        // Keep both channels open but never answer, so neither a closed channel
        // nor a miss can occur — only the liveness timeout.
        let _held_req = req_rx;
        let _held_reply = reply_tx;

        let received = receive(content_hash, size, dest.clone(), req_tx, reply_rx, None).await;
        assert!(matches!(received, Err(TransferError::LivenessTimeout)));
        assert!(!dest.exists());
    }
}
