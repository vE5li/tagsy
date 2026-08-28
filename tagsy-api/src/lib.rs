//! `tagsy-api` — **the port**: the `Backend` trait every frontend talks to, and
//! every data type that crosses it.
//!
//! This crate sits directly above [`tagsy_core`] and depends on nothing from
//! the daemon. Its job is to be the compiler-enforced boundary: the CLI and the
//! Flutter bridge depend on *this* contract, not on `tagsyd`'s internals, so
//! "a frontend cannot reach behind the daemon" is a fact the crate graph
//! guarantees rather than a convention.
//!
//! The concrete implementations of [`Backend`] (the in-process backend, the IPC
//! client, and the `AnyBackend` enum) live in `tagsyd`; only the trait
//! *declaration* and the DTOs are here.

mod backend;
pub mod connections;
mod error;
pub mod operations;
mod types;

pub use backend::{
    Backend, ConnectionStream, ConnectionUpdate, EventStream, OperationStream, OperationUpdate,
};
pub use connections::{ConnectedPeer, ConnectionEvent};
pub use error::ApiError;
pub use operations::{
    Direction, Operation, OperationEvent, OperationId, OperationKind, OperationStatus, Progress,
};
pub use types::{
    ApiEvent, BackupOutcome, DeletedRule, EditOutcome, EditorRule, HomeSection, RetagSummary,
    SearchResults, StorageStats, SubtagRule, Tag, TagRuleReport,
};
