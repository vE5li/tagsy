//! The frontend-facing surface: everything the UI (Flutter, CLI) reaches
//! through, independent of which transport carries the call.
//!
//! Today this is just the [`api`] module — the transport-agnostic
//! [`ApiService`](api::ApiService) that implements every UI operation. The
//! transport backends (`transport.rs`) and the IPC server/client (`control.rs`)
//! join it here in later phases of the restructure.

pub mod api;
