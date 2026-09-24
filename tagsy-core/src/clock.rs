//! Wall-clock access.
//!
//! Its own module because a clock is not persistence: every last-writer-wins
//! decision is stamped from here, and keeping that one call in one place is
//! what makes "never restamp a peer's timestamp" auditable. It lives in
//! `tagsy-core` so both the daemon and a client optimistically rendering a
//! just-created row read the same clock.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// The last stamp handed out by [`now_millis`] in this process.
static LAST_STAMP: AtomicI64 = AtomicI64::new(0);

/// Current wall-clock time as unix milliseconds, **strictly increasing**
/// across calls within this process.
///
/// Last-writer-wins compares stamps with a strict "newer wins", so two local
/// changes to one entity stamped in the same millisecond would silently drop
/// the second (an upload immediately followed by a delete kept the file).
/// Each call therefore returns at least one more than the previous one; it
/// only runs ahead of the wall clock while stamps are requested faster than
/// one per millisecond, and falls back in step as soon as the rate drops.
///
/// Used to stamp locally-originated changes. Peer changes carry their own
/// stamps and must NOT be restamped with this (that would let a receiver's
/// clock override the last-writer-wins comparison).
pub fn now_millis() -> i64 {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);
    let mut last = LAST_STAMP.load(Ordering::Relaxed);
    loop {
        let next = wall.max(last + 1);
        match LAST_STAMP.compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(current) => last = current,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_strictly_increase_even_within_one_millisecond() {
        let stamps: Vec<i64> = (0..10_000).map(|_| now_millis()).collect();
        assert!(stamps.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn stamps_track_the_wall_clock() {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let stamp = now_millis();
        // Other tests in this process may have pushed it slightly ahead.
        assert!(stamp >= wall && stamp < wall + 60_000);
    }
}
