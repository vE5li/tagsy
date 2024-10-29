//! Wall-clock access.
//!
//! The clock moved to `tagsy-core` so a client (the CLI) can stamp an
//! optimistic local render with the same clock the daemon uses, without
//! depending on `tagsyd`. Re-exported here so the ~37 daemon call sites keep
//! using `crate::clock::now_millis`.

pub use tagsy_core::clock::now_millis;
