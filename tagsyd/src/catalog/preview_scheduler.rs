//! Process-wide admission control for preview *resolution* (local generation
//! or peer fetch).
//!
//! The sibling of [`crate::peer::pull_scheduler::PullScheduler`], on the other
//! axis: that one bounds concurrent *byte-transfer pulls*; this one bounds
//! concurrent *preview resolutions*. They gate different work and never
//! overlap — a device whose bytes are all local (a central server) does no
//! pulls yet can still stampede on preview generation.
//!
//! The failure this prevents: a cold preview cache paired with a large catalog
//! — after a `PurgePreviews`, or a backup restore, or first sync — turns
//! startup preview-warming into a thundering herd. Every unchanged file's
//! [`crate::catalog::previews::maybe_eager_preview`] enqueues a `GetPreview`;
//! each miss reads and re-hashes the whole file (`ReadFile+verify`, O(size))
//! and then runs decode/resize/encode in `spawn_blocking`. Without a gate,
//! thousands run at once and memory spikes until the process is killed.
//!
//! [`PreviewScheduler`] bounds how many resolutions run concurrently across the
//! whole daemon (one shared [`Semaphore`]). Submission never blocks the caller:
//! the job is spawned onto a task that first acquires a permit — queueing
//! behind running resolutions — then runs, then releases it so the next queued
//! one proceeds. Only the tiny submission futures wait in memory; no file bytes
//! are read until a slot is free.
//!
//! Unlike [`PullScheduler`], this does **no** duplicate coalescing. Preview
//! resolution is idempotent (its `ApplyPreview` cache write is
//! last-writer-wins) and initial-sync warming enqueues each file exactly once,
//! so a rare concurrent duplicate merely regenerates — never a wrong result,
//! and never the dropped-responder hazard that coalescing a client's
//! `GetPreview` would introduce.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::Semaphore;

/// A process-wide gate limiting how many preview resolutions run concurrently.
/// Cheap to clone (all state is behind `Arc`); the catalog writer holds one and
/// submits every off-loop resolution through it.
#[derive(Clone)]
pub struct PreviewScheduler {
    /// One permit per allowed concurrent resolution. A job holds a permit for
    /// the whole duration of its resolve.
    permits: Arc<Semaphore>,
}

impl PreviewScheduler {
    /// Create a scheduler allowing `max_concurrent` simultaneous resolutions. A
    /// value of zero is clamped to one (a scheduler that never runs anything
    /// would silently stall all preview generation), matching the "at least
    /// make progress" spirit of [`PullScheduler`].
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_concurrent.max(1))),
        }
    }

    /// Submit a preview resolution, running `job` once a concurrency slot is
    /// free.
    ///
    /// Returns immediately (non-blocking). `job` is the resolve-and-apply work
    /// (typically the `resolve_preview` call plus the `ApplyPreview` re-entry);
    /// it is `await`ed inside the governor while the permit is held, and the
    /// permit is released when it resolves.
    ///
    /// The `job` future must be self-contained (own its captures), since it
    /// runs on a detached governor task with no lifetime tie to the caller.
    pub fn submit<Fut>(&self, job: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let permits = self.permits.clone();
        tokio::spawn(async move {
            // Queue here until a slot frees. `acquire_owned` only errors if the
            // semaphore is closed, which we never do; treat that as "give up".
            let Ok(_permit) = permits.acquire_owned().await else {
                return;
            };

            job.await;

            // Permit drops here (freeing the slot) for the next queued job.
        });
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
        let scheduler = PreviewScheduler::new(2);
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        for _ in 0..10u8 {
            let running = running.clone();
            let peak = peak.clone();
            scheduler.submit(async move {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                running.fetch_sub(1, Ordering::SeqCst);
            });
        }

        // Let every queued job drain.
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    /// Every submitted job runs — there is no coalescing, so identical work is
    /// not dropped.
    #[tokio::test(start_paused = true)]
    async fn runs_every_submission() {
        let scheduler = PreviewScheduler::new(4);
        let runs = Arc::new(AtomicUsize::new(0));

        for _ in 0..5 {
            let runs = runs.clone();
            scheduler.submit(async move {
                runs.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            });
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 5);
    }
}
