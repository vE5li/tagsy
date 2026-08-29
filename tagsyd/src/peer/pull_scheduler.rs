//! Process-wide admission control for file byte-transfers (pulls).
//!
//! A bulk change — dropping a thousand files into a sync directory on one
//! device — announces every file to its peers in quick succession. Without a
//! gate, each announcement immediately starts its own content receive: a fresh
//! flood of `ChunkRequest`s onto the relay, a temp file, and a window of
//! in-flight chunks, all at once. A few dozen files is fine; a few thousand
//! saturates the link and the file-descriptor table and spikes memory.
//!
//! [`PullScheduler`] bounds the number of *concurrent* receives across the
//! whole daemon (all peers share one gate) with a [`Semaphore`]. Submissions
//! never block the caller: a submitted pull is spawned onto a governor task
//! that first acquires a permit — queueing behind running pulls — then runs the
//! receive, then releases the permit so the next queued pull proceeds. Only the
//! tiny submission futures wait in memory; no bytes, temp files, or chunk
//! floods exist until a slot is free.
//!
//! Content addressing makes queued pulls cheap and safe to coalesce: a pull is
//! keyed by `(file_id, content_hash)`, and the same key denotes one
//! bit-identical byte sequence everywhere. The scheduler drops a submission
//! whose key is already in flight (running or queued), so the connect-time
//! reconcile sweep and a concurrent live announce for the same file don't start
//! two receives.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use tagsy_core::FileId;
use tokio::sync::{Mutex, Semaphore};

/// A process-wide gate limiting how many pulls run concurrently. Cheap to
/// clone (all state is behind `Arc`); every peer session holds a clone so the
/// limit is shared across peers, not per-connection.
#[derive(Clone)]
pub struct PullScheduler {
    /// One permit per allowed concurrent pull. A governor holds a permit for
    /// the whole duration of its receive.
    permits: Arc<Semaphore>,
    /// Keys of pulls currently in flight (queued or running), so a duplicate
    /// submission for the same content is dropped rather than started twice.
    in_flight: Arc<Mutex<HashSet<(FileId, String)>>>,
}

impl PullScheduler {
    /// Create a scheduler allowing `max_concurrent` simultaneous pulls. A value
    /// of zero is clamped to one (a scheduler that never runs anything would
    /// silently stall all sync), matching the "at least make progress" spirit
    /// of the rest of the transfer stack.
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_concurrent.max(1))),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Submit a pull for `(file_id, content_hash)`, running `job` once a
    /// concurrency slot is free.
    ///
    /// Returns immediately (non-blocking). If a pull for the same key is
    /// already in flight, `job` is dropped un-run and this is a no-op — the
    /// running one will deliver the bytes. `job` is the receive-driving
    /// work (typically the session's `spawn_content_receive` bridge plus
    /// its completion routing); it is `await`ed inside the governor while
    /// the permit is held, and the permit + dedup entry are released when
    /// it resolves.
    ///
    /// The `job` future must be self-contained (own its captures), since it
    /// runs on a detached governor task with no lifetime tie to the caller.
    pub async fn submit<F, Fut>(&self, file_id: FileId, content_hash: String, job: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let key = (file_id, content_hash);
        {
            let mut in_flight = self.in_flight.lock().await;
            if !in_flight.insert(key.clone()) {
                log::debug!(
                    "PullScheduler: {} [{}] already in flight; coalescing (dropping duplicate)",
                    key.0.to_string(),
                    key.1.get(..8).unwrap_or(&key.1)
                );
                return;
            }
        }

        let permits = self.permits.clone();
        let in_flight = self.in_flight.clone();
        tokio::spawn(async move {
            // Queue here until a slot frees. `acquire_owned` only errors if the
            // semaphore is closed, which we never do; treat that as "give up".
            let _permit = match permits.acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    in_flight.lock().await.remove(&key);
                    return;
                }
            };

            job().await;

            // Permit drops here (freeing the slot); clear the dedup entry so a
            // later change to the same file can be pulled again.
            in_flight.lock().await.remove(&key);
        });
    }

    /// Number of pull slots currently free (for tests / diagnostics).
    #[cfg(test)]
    pub fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Never more than `max_concurrent` jobs run at once, even when many are
    /// submitted at the same instant.
    #[tokio::test(start_paused = true)]
    async fn caps_concurrency() {
        let scheduler = PullScheduler::new(2);
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        for i in 0..10u8 {
            let running = running.clone();
            let peak = peak.clone();
            scheduler
                .submit(FileId::new(), format!("hash{i}"), move || async move {
                    let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    running.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
        }

        // Let every queued job drain.
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    /// A duplicate key submitted while the first is in flight is dropped: the
    /// job runs exactly once.
    #[tokio::test(start_paused = true)]
    async fn coalesces_duplicate_key() {
        let scheduler = PullScheduler::new(4);
        let runs = Arc::new(AtomicUsize::new(0));
        let key = FileId::new();

        for _ in 0..5 {
            let runs = runs.clone();
            scheduler
                .submit(key, "samehash".to_owned(), move || async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                })
                .await;
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    /// Once a key's job finishes, the same key can be pulled again (dedup is
    /// per-in-flight, not permanent).
    #[tokio::test(start_paused = true)]
    async fn key_reusable_after_completion() {
        let scheduler = PullScheduler::new(4);
        let runs = Arc::new(AtomicUsize::new(0));
        let key = FileId::new();

        let runs1 = runs.clone();
        scheduler
            .submit(key, "h".to_owned(), move || async move {
                runs1.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let runs2 = runs.clone();
        scheduler
            .submit(key, "h".to_owned(), move || async move {
                runs2.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }
}
