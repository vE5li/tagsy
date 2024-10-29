//! The stateless holder side: [`answer_chunk_request`] serves a single
//! content-addressed `ChunkRequest` from a [`ChunkSource`], verifying the
//! source against `content_hash` via a [`VerifiedHashCache`] so a file is
//! hashed once and every subsequent chunk is a cache hit.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

use crate::peer::transfer::source::ChunkSource;
use crate::peer::transfer::{CHUNK_SIZE, short_hash};

/// A per-holder cache of verified content hashes, keyed by the on-disk path
/// backing a file: `path -> (mtime, size, hash)`. Lets a holder answer repeated
/// `ChunkRequest`s for the same file without re-hashing it every time, while
/// still invalidating on any mtime/size change (a file edited mid-serve stops
/// matching, so the holder answers `ChunkMiss`).
///
/// Cheap to clone (an `Arc<Mutex<..>>`); every peer session shares one.
#[derive(Clone, Default)]
pub struct VerifiedHashCache {
    inner: std::sync::Arc<Mutex<HashMap<PathBuf, VerifiedEntry>>>,
}

#[derive(Clone)]
struct VerifiedEntry {
    mtime: Option<SystemTime>,
    size: u64,
    hash: String,
}

impl VerifiedHashCache {
    pub fn new() -> Self {
        Self::default()
    }

    fn get(&self, path: &Path, mtime: Option<SystemTime>, size: u64) -> Option<String> {
        let map = self.inner.lock().unwrap();
        map.get(path).and_then(|entry| {
            (entry.mtime == mtime && entry.size == size).then(|| entry.hash.clone())
        })
    }

    fn put(&self, path: PathBuf, mtime: Option<SystemTime>, size: u64, hash: String) {
        self.inner
            .lock()
            .unwrap()
            .insert(path, VerifiedEntry { mtime, size, hash });
    }
}

/// The result of answering a `ChunkRequest`: either the canonical bytes at the
/// requested offset, or a miss (this holder cannot serve the key).
pub enum ChunkAnswer {
    Data(Vec<u8>),
    Miss,
}

/// Answer a single content-addressed `ChunkRequest` for `content_hash` from
/// `source`, serving the canonical chunk at `offset`.
///
/// `pre_verified` says the caller has already established that `source`'s bytes
/// hash to `content_hash` — true for a provider (looked up by its
/// `(file_id, content_hash)` registration key) and for a sync-directory file
/// whose `ReadFile` already returned a matching hash. When `pre_verified`, no
/// hashing is done here.
///
/// **Providers must be `pre_verified`.** Re-hashing a [`ProviderSource`] reads
/// the whole file *through the provider* and, on reaching the end, fires the
/// provider's `on_complete` — which the daemon interprets as "the transfer is
/// done, release the file". Hashing it here would therefore release the file
/// after the first chunk and make every later chunk unavailable. Providers are
/// trusted by their registration key instead.
///
/// When `!pre_verified` (e.g. a sync-directory file we want to (re)confirm),
/// verification is cached by `cache` keyed on the source's on-disk path +
/// mtime/size, so it is paid once per unchanged file; a source with no on-disk
/// path is hashed each call.
///
/// `offset` MUST be `CHUNK_SIZE`-aligned; a misaligned request is a
/// [`ChunkAnswer::Miss`]. An out-of-range offset yields an empty
/// [`ChunkAnswer::Data`] for a matching source (harmless — the receiver
/// terminates on size), consistent with `read_chunk_at`.
///
/// [`ProviderSource`]: crate::peer::transfer::source::ProviderSource
pub async fn answer_chunk_request<S: ChunkSource>(
    source: &S,
    source_path: Option<&Path>,
    cache: &VerifiedHashCache,
    content_hash: &str,
    offset: u64,
    pre_verified: bool,
) -> ChunkAnswer {
    let short = short_hash(content_hash);

    // Malformed request: offsets must land on chunk boundaries.
    if !offset.is_multiple_of(CHUNK_SIZE as u64) {
        log::warn!("answer[{short}]: misaligned offset={offset}; miss");
        return ChunkAnswer::Miss;
    }

    if !pre_verified {
        // Verify the source matches `content_hash`, using the cache when
        // possible. Never applied to a provider (see the doc note above).
        let verified = match source_path {
            Some(path) => {
                let (mtime, size) = match tokio::fs::metadata(path).await {
                    Ok(metadata) => (metadata.modified().ok(), metadata.len()),
                    Err(error) => {
                        log::debug!(
                            "answer[{short}]: metadata failed for {}: {error}; miss",
                            path.display()
                        );
                        return ChunkAnswer::Miss;
                    }
                };
                match cache.get(path, mtime, size) {
                    Some(cached) => {
                        log::trace!(
                            "answer[{short}]: offset={offset} verified from cache (size={size})"
                        );
                        cached == content_hash
                    }
                    None => {
                        log::debug!(
                            "answer[{short}]: offset={offset} cache miss; hashing {} ({size} \
                             bytes)",
                            path.display()
                        );
                        let started = std::time::Instant::now();
                        let hash = match hash_source(source).await {
                            Some(hash) => hash,
                            None => {
                                log::debug!("answer[{short}]: hashing failed; miss");
                                return ChunkAnswer::Miss;
                            }
                        };
                        log::debug!(
                            "answer[{short}]: hashed {} in {:?} -> {}",
                            path.display(),
                            started.elapsed(),
                            short_hash(&hash)
                        );
                        cache.put(path.to_path_buf(), mtime, size, hash.clone());
                        hash == content_hash
                    }
                }
            }
            None => match hash_source(source).await {
                Some(hash) => hash == content_hash,
                None => return ChunkAnswer::Miss,
            },
        };

        if !verified {
            log::debug!("answer[{short}]: offset={offset} source does not match hash; miss");
            return ChunkAnswer::Miss;
        }
    }

    match source.read_chunk_at(offset, CHUNK_SIZE).await {
        Ok((bytes, _last)) => {
            log::trace!(
                "answer[{short}]: serving offset={offset} ({} bytes){}",
                bytes.len(),
                if pre_verified { " [pre-verified]" } else { "" }
            );
            ChunkAnswer::Data(bytes)
        }
        Err(error) => {
            log::debug!("answer[{short}]: read failed at offset={offset}: {error}; miss");
            ChunkAnswer::Miss
        }
    }
}

/// Stream-hash a [`ChunkSource`] by reading it in `CHUNK_SIZE` windows until
/// the end. Returns `None` on a read error.
async fn hash_source<S: ChunkSource>(source: &S) -> Option<String> {
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0u64;
    loop {
        let (bytes, last) = source.read_chunk_at(offset, CHUNK_SIZE).await.ok()?;
        hasher.update(&bytes);
        offset += bytes.len() as u64;
        if last || bytes.is_empty() {
            break;
        }
    }
    Some(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_bytes::FileBytes;
    use crate::peer::transfer::source::{ProviderChunkRequest, ProviderSource};

    /// The serve side verifies against `content_hash` and serves the canonical
    /// chunk; a misaligned offset is a miss; a wrong hash is a miss.
    #[tokio::test]
    async fn answer_serves_and_verifies() {
        let bytes: Vec<u8> = (0..(CHUNK_SIZE + 5)).map(|i| i as u8).collect();
        let hash = blake3::hash(&bytes).to_hex().to_string();
        let source = FileBytes::InMemory(bytes.clone());
        let cache = VerifiedHashCache::new();

        // Aligned, correct hash: serves the first chunk.
        match answer_chunk_request(&source, None, &cache, &hash, 0, false).await {
            ChunkAnswer::Data(chunk) => assert_eq!(chunk, bytes[..CHUNK_SIZE]),
            ChunkAnswer::Miss => panic!("expected data"),
        }
        // Misaligned offset: miss (even when pre_verified).
        assert!(matches!(
            answer_chunk_request(&source, None, &cache, &hash, 1, true).await,
            ChunkAnswer::Miss
        ));
        // Wrong hash: miss.
        let wrong = blake3::hash(b"nope").to_hex().to_string();
        assert!(matches!(
            answer_chunk_request(&source, None, &cache, &wrong, 0, false).await,
            ChunkAnswer::Miss
        ));
    }

    /// A pre-verified source is served without any hashing — the regression
    /// guard for the CLI-upload bug: re-hashing a `ProviderSource` streams the
    /// whole file and fires its `on_complete` at EOF, which released the file
    /// after the first chunk and made later chunks unavailable. With
    /// `pre_verified`, `on_complete` never fires from serving, and each chunk
    /// is served independently.
    #[tokio::test]
    async fn provider_pre_verified_serves_all_chunks_without_completing() {
        let bytes: Vec<u8> = (0..(CHUNK_SIZE * 3 + 9)).map(|i| i as u8).collect();
        let hash = blake3::hash(&bytes).to_hex().to_string();

        // Wire a fake provider client that answers chunk requests from `bytes`.
        let (req_tx, mut req_rx) = tokio::sync::mpsc::unbounded_channel::<ProviderChunkRequest>();
        let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let provider = ProviderSource::new(req_tx, done_tx);

        let serve_bytes = bytes.clone();
        tokio::spawn(async move {
            while let Some((offset, reply)) = req_rx.recv().await {
                let start = (offset as usize).min(serve_bytes.len());
                let end = (start + CHUNK_SIZE).min(serve_bytes.len());
                let last = end >= serve_bytes.len();
                let _ = reply.send(Ok((serve_bytes[start..end].to_vec(), last)));
            }
        });

        let cache = VerifiedHashCache::new();
        // Serve every chunk pre-verified (as the daemon does for a registered
        // provider); each must return the right bytes.
        let mut offset = 0u64;
        while offset < bytes.len() as u64 {
            match answer_chunk_request(&provider, None, &cache, &hash, offset, true).await {
                ChunkAnswer::Data(chunk) => {
                    let start = offset as usize;
                    let end = (start + CHUNK_SIZE).min(bytes.len());
                    assert_eq!(chunk, bytes[start..end], "chunk at {offset} mismatched");
                }
                ChunkAnswer::Miss => panic!("chunk at {offset} unexpectedly missed"),
            }
            offset += CHUNK_SIZE as u64;
        }

        // `on_complete` fires exactly once — when the *final* chunk is served
        // (its provider reply carried `last = true`) — not during any earlier
        // verification. Crucially it did not fire before the last chunk, so no
        // chunk was ever unavailable.
        assert!(
            done_rx.try_recv().is_ok(),
            "expected one on_complete at EOF"
        );
        assert!(
            done_rx.try_recv().is_err(),
            "on_complete must fire only once"
        );
    }

    /// The verified-hash cache invalidates on mtime/size change: a file edited
    /// after being cached stops matching its old hash.
    #[tokio::test]
    async fn verified_cache_invalidates_on_change() {
        let dir = std::env::temp_dir().join(format!(
            "tagsy-cache-test-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f.bin");
        std::fs::write(&path, b"first").unwrap();

        let cache = VerifiedHashCache::new();
        let first_hash = blake3::hash(b"first").to_hex().to_string();
        let source = FileBytes::FileToCopy(path.clone());

        // First serve populates the cache.
        assert!(matches!(
            answer_chunk_request(&source, Some(&path), &cache, &first_hash, 0, false).await,
            ChunkAnswer::Data(_)
        ));

        // Edit the file: the old hash must no longer verify (cache invalidated
        // by mtime/size), and the new hash must serve.
        // Change the size too (invalidates regardless of mtime resolution).
        std::fs::write(&path, b"second content longer").unwrap();
        assert!(matches!(
            answer_chunk_request(&source, Some(&path), &cache, &first_hash, 0, false).await,
            ChunkAnswer::Miss
        ));
        let second_hash = blake3::hash(b"second content longer").to_hex().to_string();
        assert!(matches!(
            answer_chunk_request(&source, Some(&path), &cache, &second_hash, 0, false).await,
            ChunkAnswer::Data(_)
        ));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
