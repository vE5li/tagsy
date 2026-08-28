//! Core tagsy runtime as a library.
//!
//! The runtime is callable as a library function ([`run`]): the desktop binary
//! (`main.rs`) is a thin CLI wrapper, and other frontends (e.g. an Android
//! native library) can link this crate and call [`run`] directly without a
//! `main()`.
//!
//! All business logic (peer sync, the DB pipeline, change handling) lives
//! here behind [`run`]. Frontends supply:
//!
//! - a [`Configuration`](configuration::Configuration),
//! - a [`RunPaths`] describing where the data directory and identity key live,
//! - a [`ShutdownSignal`] used to stop the runtime cleanly.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tagsy_core::FileId;
use tagsy_core::state::{Change, ChangeOrigin};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;

use crate::catalog::messages::CatalogCommand;
use crate::catalog::previews::PREVIEW_GENERATION_COMPILED;
use crate::configuration::{CompiledTagRules, Configuration, RuntimeConfiguration};
use crate::paths::Paths;
use crate::peer::dial::{connect_to_peer, handle_connection};
use crate::peer::handshake::Identity;
use crate::peer::session::PeerContext;
use crate::peer::transfer::VerifiedHashCache;
use crate::store::CatalogStore;
use crate::sync_directories::{SyncDirectories, SyncDirectoryCommand};

pub mod catalog;
pub mod clock;
pub mod configuration;
pub mod connections;
pub mod control;
pub mod file_bytes;
pub mod frontend;
pub mod operations;
pub mod paths;
pub mod peer;
#[cfg(feature = "preview-generation")]
pub mod preview;
pub mod store;
pub mod sync_directories;
pub mod transport;

/// Cooperative shutdown handle for [`run`].
///
/// A thin wrapper around a [`CancellationToken`]. The caller holds the
/// [`ShutdownSignal`] and calls [`ShutdownSignal::shutdown`] (e.g. from a
/// Ctrl-C handler, a systemd stop, or the Android service `onDestroy`); the
/// running [`run`] future observes the cancellation, stops accepting new work,
/// drains its tasks, and returns cleanly.
#[derive(Debug, Clone, Default)]
pub struct ShutdownSignal {
    token: CancellationToken,
}

impl ShutdownSignal {
    /// Create a fresh, un-triggered shutdown signal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request shutdown. Idempotent; safe to call from any task/thread.
    pub fn shutdown(&self) {
        self.token.cancel();
    }

    /// Has shutdown been requested yet?
    pub fn is_shutdown(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Access the underlying token (e.g. to derive child tokens for tasks).
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }
}

/// Errors that can abort startup of [`run`].
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// The identity key could not be loaded from `identity_file`.
    #[error("failed to load identity key at {}: {source}", path.display())]
    Identity {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Opening the main database failed.
    #[error("failed to open main database: {0}")]
    Database(#[source] store::DatabaseError),
    /// Binding the peer-sync listener failed.
    #[error("failed to bind peer listener to {address}: {source}")]
    Bind {
        address: String,
        #[source]
        source: std::io::Error,
    },
}

/// Enqueue a `Change::TagAdded` for every tag declared in the configuration, so
/// their definitions are guaranteed to exist before any tagging/reconciliation
/// runs. Called from [`run`] before `handle_changes` starts draining the bus.
/// Best-effort per tag: an empty name is skipped (the DB rejects it anyway) and
/// a closed channel is logged.
///
/// `modified_at` stamped on config-declared tag definitions. Deliberately the
/// lowest possible value so a declaration acts as a last-writer-wins *floor*:
/// `add_tag`'s guard (`excluded.modified_at > tags.modified_at`) means any real
/// edit — always stamped with a positive wall-clock `now_millis()` — wins, and
/// a re-declared tag on the next boot never clobbers a rename/recolor made in
/// between. See [`TagDeclaration`](configuration::TagDeclaration).
fn enqueue_declared_tags(
    change_sender: &UnboundedSender<CatalogCommand>,
    configuration: &Configuration,
) {
    const DECLARED_TAG_MODIFIED_AT: i64 = i64::MIN;

    for tag in &configuration.tags {
        if tag.name.trim().is_empty() {
            log::warn!(
                "Skipping config tag declaration {} with empty name",
                tag.id.to_string()
            );
            continue;
        }

        // Normalize an empty color to the same default the API uses, so a
        // declared tag renders consistently with a UI-created one.
        let color = if tag.color.trim().is_empty() {
            "#F44336".to_owned()
        } else {
            tag.color.clone()
        };

        let change = Change::TagAdded {
            tag_id: tag.id,
            tag_name: tag.name.clone(),
            color,
            metadata: None,
            modified_at: DECLARED_TAG_MODIFIED_AT,
        };
        let change_origin = ChangeOrigin::Local {
            directory_path: std::path::PathBuf::new(),
        };

        if let Err(error) = change_sender.send(CatalogCommand::change(change, change_origin)) {
            log::error!(
                "Failed to enqueue declared tag {} ({}): {error}",
                tag.name,
                tag.id.to_string()
            );
        }
    }
}

/// Start the tagsy sync engine, returning a UI-facing
/// [`ApiService`](frontend::api::ApiService) handle alongside the runtime
/// driver future.
///
/// This is the former body of the `Run` CLI subcommand, lifted into a library
/// function so it can be driven by any frontend. It performs all fallible
/// startup (loading the identity, opening the main DB, binding the peer
/// listener) up front and returns:
///
/// - an [`ApiService`](frontend::api::ApiService) the caller can use
///   immediately to serve the UI (reads, writes, event subscription), and
/// - a driver future that runs the accept loop / idle-until-shutdown and then
///   drains the spawned tasks. The caller must poll it to completion (e.g.
///   `tokio::spawn` it, or `.await` it) for the runtime to make progress; it
///   returns once `shutdown` is triggered.
///
/// Every frontend (desktop binary, Android in-process backend, host harness)
/// uses this: the ones that do not need the
/// [`ApiService`](frontend::api::ApiService) simply await the driver and drop
/// the handle.
pub async fn run(
    configuration: Configuration,
    paths: Paths,
    shutdown: ShutdownSignal,
) -> Result<
    (
        frontend::api::ApiService,
        impl std::future::Future<Output = Result<(), RunError>>,
    ),
    RunError,
> {
    let runtime_configuration = Arc::new(RwLock::new(RuntimeConfiguration::new(&configuration)));

    // Reconcile the preview-generation policy against what this binary can
    // actually do. A policy that wants to generate (`Lazy`/`Eager`) needs the
    // `preview-generation` feature compiled in; if it is not, we cannot honor
    // the policy, so we log an error and fall back to behaving as `Never`
    // (cache + serve + relay only). This is a soft fallback, not a fatal error:
    // the daemon still runs and still participates in the preview network.
    let policy = configuration.preview_generation_policy;
    let can_generate_previews = if policy.generates() && !PREVIEW_GENERATION_COMPILED {
        log::error!(
            "preview_generation_policy is {:?} but this build was compiled without the \
             `preview-generation` feature; falling back to no local generation (Never). This \
             device will only cache and serve previews obtained from peers.",
            policy
        );
        false
    } else {
        policy.generates() && PREVIEW_GENERATION_COMPILED
    };
    log::info!(
        "Preview generation policy: {:?} (local generation {})",
        policy,
        if can_generate_previews {
            "enabled"
        } else {
            "disabled"
        }
    );

    // Compile the tag rules once. Shared (not cloned) because a `Regex` is
    // expensive to build and both consumers only ever read: `handle_changes`
    // matches every newly-created file against them, and `ApiService` needs the
    // same set to re-apply them on demand (`retag`).
    //
    // A rule that fails to compile is dropped from the matcher set but retained
    // as an error, and never prevents startup — see `CompiledTagRules`.
    let tag_rules = Arc::new(CompiledTagRules::compile(&configuration.tag_rules));
    for error in tag_rules.errors() {
        log::error!("{error}; this rule is disabled, all others still apply");
    }

    // Shared content-keyed chunk relay. Every peer session and `handle_changes`
    // holds a clone: requests forwarded on one session and replies arriving on
    // another share one waiter table, so multi-source pulls and relay coalescing
    // work across links. Also owns the temporary-provider registry (CLI
    // uploads). Cheap to clone (Arcs).
    let pending_fetches = crate::peer::relay::ChunkRelay::new(runtime_configuration.clone());

    // Sibling of `pending_fetches` for previews: a content-keyed waiter table
    // shared by every peer session and `handle_changes`, so a preview requested
    // on one link and answered on another resolve together. Cheap to clone.
    let pending_previews = crate::peer::relay::PreviewRelay::new(runtime_configuration.clone());

    let identity = Identity::load(paths.identity_path()).map_err(|source| RunError::Identity {
        path: paths.identity_path().to_path_buf(),
        source,
    })?;
    let identity = Arc::new(identity);

    let main_db_path = paths.main_db_path();

    // Open the main DB. It will be owned by `handle_changes` (the only task
    // that mutates it). Before handing it off, snapshot the latest content
    // hash per file so `SyncDirectories` can detect files that changed on
    // disk while we were offline without ever touching the main DB itself.
    let database = CatalogStore::initialize(&main_db_path).map_err(RunError::Database)?;

    let last_known_hashes = database
        .latest_content_hashes()
        .map_err(RunError::Database)?;

    let (change_sender, change_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();

    // Guarantee the config-declared tag definitions exist before anything else.
    // These are enqueued now, while `handle_changes` has not yet started
    // draining the bus, so they are the *first* changes it applies — before any
    // peer connects and before any `FileTagged`/reconciliation runs. That way a
    // `SyncType::TagBased` directory referencing a declared id always resolves.
    enqueue_declared_tags(&change_sender, &configuration);

    // Broadcast of applied changes for the UI-facing API event stream.
    // `handle_changes` publishes every change it applies here;
    // API subscribers receive them best-effort. Capacity bounds how far a slow
    // subscriber may lag before it observes `Lagged` (mapped to `Resynced` by
    // the transport). Sized generously; the UI is expected to keep up.
    let (event_sender, _event_receiver) = tokio::sync::broadcast::channel(1024);

    // Live sync-operation registry, shared by the UI-facing API (to snapshot /
    // subscribe) and every peer session (to report work in progress).
    let operations = crate::operations::Operations::new();

    // Live peer-connection registry. Connections are *state*, not operations,
    // so they get their own registry: the UI-facing API snapshots / subscribes
    // it, and every peer session registers itself in it for the session's life.
    let connections = crate::connections::Connections::new();

    let fetch_temp_dir = paths.fetch_temp_dir();
    if let Err(error) = paths.clean_fetch_temp_dir().await {
        log::warn!(
            "Failed to prepare fetch temp dir {}: {error}",
            fetch_temp_dir.display()
        );
    }

    // The UI-facing API handle. Reads open their own read-only DB handle on
    // `main_db_path`; writes go onto `change_sender`; events come from
    // `event_sender`.
    let api = frontend::api::ApiService::new(
        main_db_path.clone(),
        change_sender.clone(),
        command_sender.clone(),
        event_sender.clone(),
        pending_fetches.clone(),
        fetch_temp_dir,
        operations.clone(),
        connections.clone(),
        configuration.editor_rules.clone(),
        configuration.home_sections.clone(),
        tag_rules.clone(),
        paths.clone(),
    );

    // The sync-directory manager is inherently single-threaded: it holds
    // `RefCell`s that are `!Send`, and it now `.await`s file I/O (streaming
    // materialization) while borrowing them. Rather than force `Send` on all of
    // that, run it on a dedicated OS thread with a current-thread runtime +
    // `LocalSet`. A oneshot lets the shutdown path below join it like the other
    // tasks.
    let (sync_directories_done_tx, sync_directories_handle) = tokio::sync::oneshot::channel();
    let sync_directories_thread = {
        let configuration = configuration.clone();
        let paths = paths.clone();
        let change_sender = change_sender.clone();
        let shutdown_child = shutdown.token().child_token();

        std::thread::Builder::new()
            .name("tagsy-sync-directories".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build sync-directory runtime");

                let local = tokio::task::LocalSet::new();
                local.block_on(
                    &runtime,
                    handle_sync_directories(
                        configuration,
                        paths,
                        last_known_hashes,
                        change_sender,
                        command_receiver,
                        shutdown_child,
                    ),
                );

                let _ = sync_directories_done_tx.send(());
            })
            .expect("failed to spawn sync-directory thread")
    };

    let catalog = catalog::CatalogWriter {
        configuration: configuration.clone(),
        tag_rules: tag_rules.clone(),
        runtime_configuration: runtime_configuration.clone(),
        pending_fetches: pending_fetches.clone(),
        pending_previews: pending_previews.clone(),
        database,
        change_sender: change_sender.clone(),
        command_sender: command_sender.clone(),
        event_sender,
        operations: operations.clone(),
        shutdown: shutdown.token().child_token(),
    };
    let changes_handle = tokio::spawn(catalog.run(change_receiver));

    // The routing handles every peer-connection task needs. Built once and
    // cloned per spawned task (below, and in the accept loop inside `driver`).
    let peer_context = PeerContext {
        runtime_configuration: runtime_configuration.clone(),
        pending_fetches: pending_fetches.clone(),
        pending_previews: pending_previews.clone(),
        change_sender: change_sender.clone(),
        command_sender: command_sender.clone(),
        can_generate_previews,
        verified_hashes: VerifiedHashCache::new(),
        operations: operations.clone(),
        connections: connections.clone(),
    };

    let mut peer_handles = Vec::new();
    for peer in &configuration.peers {
        if peer.address.is_some() {
            peer_handles.push(tokio::spawn(connect_to_peer(
                identity.clone(),
                peer.clone(),
                main_db_path.clone(),
                peer_context.clone(),
                shutdown.token().child_token(),
            )));
        }
    }

    // Bind the peer-sync listener up front (if configured) so bind failures
    // surface to the caller before we hand back the `ApiService`, rather than
    // inside the driver future.
    let listener = if let Some(listen_port) = configuration.listen_port {
        let bind_address = format!("0.0.0.0:{listen_port}");
        let listener = TcpListener::bind(&bind_address)
            .await
            .map_err(|source| RunError::Bind {
                address: bind_address.clone(),
                source,
            })?;
        log::info!("Listening for peer connections on {bind_address}");
        Some(listener)
    } else {
        log::info!("No listen_port configured; not accepting inbound peer connections");
        None
    };

    // The driver future: runs the accept loop (or idles until shutdown), then
    // cancels and drains all spawned tasks. The caller polls it to completion.
    let driver = async move {
        if let Some(listener) = listener {
            loop {
                tokio::select! {
                    _ = shutdown.token().cancelled() => {
                        log::info!("Shutdown requested; stopping peer listener");
                        break;
                    }
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, address)) => {
                                tokio::spawn(handle_connection(
                                    configuration.clone(),
                                    identity.clone(),
                                    main_db_path.clone(),
                                    peer_context.clone(),
                                    stream,
                                    address,
                                    shutdown.token().child_token(),
                                ));
                            }
                            Err(error) => {
                                log::warn!("Peer listener accept error: {error}");
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            // Keep the runtime alive so the spawned tasks can run, until shutdown.
            shutdown.token().cancelled().await;
            log::info!("Shutdown requested; stopping runtime");
        }

        // Ensure the long-lived tasks observe cancellation, then drain them.
        shutdown.shutdown();

        // Dropping the senders lets the receiving tasks fall out of their loops
        // once their channels are empty.
        drop(change_sender);
        drop(command_sender);

        let _ = sync_directories_handle.await;
        // Join the dedicated OS thread now that its runtime has finished.
        let _ = sync_directories_thread.join();
        let _ = changes_handle.await;
        for handle in peer_handles {
            let _ = handle.await;
        }

        log::info!("tagsy runtime stopped cleanly");
        Ok(())
    };

    Ok((api, driver))
}

async fn handle_sync_directories(
    configuration: Configuration,
    paths: Paths,
    last_known_hashes: HashMap<FileId, String>,
    change_sender: UnboundedSender<CatalogCommand>,
    command_receiver: UnboundedReceiver<SyncDirectoryCommand>,
    shutdown: CancellationToken,
) {
    let mut manager =
        SyncDirectories::new(configuration, &paths, change_sender, command_receiver).await;

    // Cooperative shutdown: `run` observes `shutdown` as a branch of its own
    // select loop and returns normally between whole events, so an in-flight
    // handler (which `.await`s file I/O) is never dropped mid-write. Do not
    // wrap this in an outer `select!` that races the token against `run` — that
    // is exactly the abrupt cancellation the cooperative loop exists to avoid.
    manager.run(last_known_hashes, shutdown).await;
}
