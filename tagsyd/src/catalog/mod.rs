//! The catalog: the authoritative index of what exists (files, tags, the graph
//! between them, version history) — kept independently of the bytes it
//! describes, which are content-addressed and may live on any peer or nowhere
//! reachable. The persistence is [`CatalogStore`]; the actor that guards it is
//! [`CatalogWriter`].
//!
//! [`CatalogWriter`] owns the only `&mut CatalogStore` and drains one `mpsc` of
//! [`CatalogCommand`]s (see [`messages`]), applying each on a single thread.
//! That single-ownership *is* the central "only the catalog writer writes the
//! main DB" invariant — expressed as ownership rather than convention, and now
//! stated in the type's name.
//!
//! The per-arm logic lives in sibling modules ([`content`], [`files`],
//! [`tagging`], [`forward`]); [`CatalogWriter::run`] holds the receive loop and
//! the dispatch that routes each command to them.
//!
//! # EVENT PUBLISHING (KNOWN-SUBOPTIMAL)
//!
//! Applied changes are published to UI-facing API subscribers over
//! `event_sender`. The intended single publish site is the fall-through at the
//! very bottom of the message loop, but most arms `continue` before reaching
//! it, so each one that must notify the UI emits for itself. The emit sites are
//! therefore hand-maintained and easy to forget:
//!
//!   1. bottom-of-loop fall-through   — every `Ingest::Meta` change (this is
//!      how a device learns about peer edits)
//!   2. `Change::FileRestored` arm    — `continue`s
//!   3. `CatalogCommand::AnnounceProvided` arm — `continue`s; the local client
//!      upload/edit path (`ApiService::upload_file` / `ApiService::edit_file`)
//!   4. `CatalogCommand::Materialize` arm — `continue`s; "bytes are now on
//!      disk"
//!   5. `dispatch_and_forward`         — reached from `Ingest::Content`, which
//!      `continue`s; sync-directory watcher edits
//!
//! Arms that deliberately do NOT publish (they change no user-visible catalog
//! state): `Fetch`, `GetPreview`, `ApplyPreview`, `PurgePreviews`,
//! `ReconcilePlacement`, `CatalogFile`.
//!
//! TODO: Make publishing structural instead of hand-maintained (see the git
//! history of `handle_changes` for the full options list).

pub mod content;
pub mod files;
pub mod forward;
pub mod messages;
pub mod placement;
pub mod previews;
pub mod tagging;

use std::sync::Arc;

use tagsy_core::state::{Change, ChangeOrigin};
use tokio::sync::RwLock;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;

use crate::catalog::messages::{CatalogCommand, Ingest};
#[cfg(feature = "preview-generation")]
use crate::catalog::previews::preview_extension_for;
use crate::catalog::previews::{PREVIEW_GENERATION_COMPILED, resolve_preview};
use crate::configuration::{CompiledTagRules, Configuration, RuntimeConfiguration};
use crate::peer::relay::{ChunkRelay, PreviewRelay};
use crate::store::CatalogStore;
use crate::sync_directories::SyncDirectoryCommand;
use crate::{clock, operations};

/// The change-handling actor: the sole writer to the main [`CatalogStore`].
///
/// Holds the long-lived handles the pipeline needs. Built once in
/// [`crate::run`] and driven to completion by [`CatalogWriter::run`], which
/// owns the receive loop.
pub struct CatalogWriter {
    pub configuration: Configuration,
    pub tag_rules: Arc<CompiledTagRules>,
    pub runtime_configuration: Arc<RwLock<RuntimeConfiguration>>,
    pub pending_fetches: ChunkRelay,
    pub pending_previews: PreviewRelay,
    pub database: CatalogStore,
    pub change_sender: UnboundedSender<CatalogCommand>,
    pub command_sender: UnboundedSender<SyncDirectoryCommand>,
    pub event_sender: tokio::sync::broadcast::Sender<Change>,
    pub operations: operations::Operations,
    pub shutdown: CancellationToken,
}

impl CatalogWriter {
    /// Drive the change-handling loop until shutdown or all senders drop.
    ///
    /// This is the single change-handling pipeline task; it owns the sole
    /// `&mut CatalogStore` and drains `change_receiver` on one thread.
    #[allow(clippy::too_many_lines)]
    pub async fn run(self, mut change_receiver: UnboundedReceiver<CatalogCommand>) {
        let CatalogWriter {
            configuration,
            tag_rules,
            runtime_configuration,
            pending_fetches,
            pending_previews,
            mut database,
            change_sender,
            command_sender,
            event_sender,
            operations,
            shutdown,
        } = self;

        log::info!("handle_changes task started; awaiting changes");

        loop {
            let message = tokio::select! {
                _ = shutdown.cancelled() => {
                    log::info!("Shutdown requested; stopping change handler");
                    break;
                }
                received = change_receiver.recv() => {
                    match received {
                        Some(item) => item,
                        None => {
                            log::warn!(
                                "handle_changes: change_receiver returned None \
                                 (all senders dropped); exiting"
                            );
                            break;
                        }
                    }
                }
            };

            // Route the two bus message kinds. A `Fetch` is an on-demand request
            // for a file's bytes (from `tagsy edit`): satisfy it locally if we
            // hold matching content, otherwise drive a content-addressed receive
            // that floods `ChunkRequest`s to peers. A `Change` falls through to the
            // DB-writer pipeline below.
            let ingest = match message {
                CatalogCommand::Change(ingest, change_origin) => (ingest, change_origin),
                CatalogCommand::Fetch {
                    file_id,
                    expected_hash,
                    respond_to,
                } => {
                    if let Some(file_bytes) = crate::peer::fetch::read_local_if_hash_matches(
                        &command_sender,
                        file_id,
                        &expected_hash,
                    )
                    .await
                    {
                        let _ = respond_to.send(Ok(file_bytes));
                        return;
                    }

                    // Resolve the version's authoritative size (needed to bound the
                    // receive) from the catalog. The catalog is byte-independent, so
                    // this is known for any file we know about even if its bytes are
                    // not local. Without a matching version we cannot fetch by hash.
                    let expected_size = match database.latest_version(file_id) {
                        Ok(Some(version)) if version.content_hash == expected_hash => {
                            version.size as u64
                        }
                        _ => {
                            let _ = respond_to.send(Err(messages::FetchError::NotAvailable));
                            return;
                        }
                    };

                    // Surface this on-demand fetch as a live operation, then drive
                    // the content-addressed receive off-loop (flooding across the
                    // peer tree) so the single-threaded consumer is not blocked.
                    let fetching = operations.begin(operations::OperationKind::fetching(file_id));
                    let pending_fetches_fetch = pending_fetches.clone();
                    tokio::spawn(async move {
                        let result = crate::peer::fetch::fetch_via_relay(
                            &pending_fetches_fetch,
                            file_id,
                            expected_hash,
                            expected_size,
                            None,
                        )
                        .await;
                        match &result {
                            Ok(_) => fetching.complete(),
                            Err(error) => fetching.fail(error.to_string()),
                        }
                        let _ = respond_to.send(result);
                    });

                    continue;
                }
                CatalogCommand::Restore {
                    file_id,
                    respond_to,
                } => {
                    // User-initiated restore of a soft-deleted file. Read the file's
                    // latest known version *while it is still tombstoned* (its
                    // version history is retained on soft delete). The catalog is
                    // not mutated here — only once the bytes are confirmed
                    // recoverable (see `ApplyRestore`).
                    let deletion_state =
                        database
                            .file_deletion_state(file_id)
                            .unwrap_or_else(|error| {
                                log::error!(
                                    "Restore: file_deletion_state failed for {}: {:?}; treating \
                                     as unknown",
                                    file_id.to_string(),
                                    error
                                );
                                None
                            });

                    let is_deleted = matches!(deletion_state, Some(state) if state.deleted);
                    if !is_deleted {
                        // Not tombstoned (or unknown): nothing to restore.
                        let _ = respond_to.send(Err(messages::RestoreError::NotDeleted));
                        continue;
                    }

                    let latest = database.latest_version(file_id).unwrap_or_else(|error| {
                        log::error!(
                            "Restore: latest_version failed for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                        None
                    });

                    // A tombstoned file always has a version in practice (one is
                    // recorded at creation before it could ever be deleted). Guard
                    // defensively: without a version there is no hash to restore by.
                    let Some(latest) = latest else {
                        log::error!(
                            "Restore: {} is tombstoned but has no recorded version; cannot restore",
                            file_id.to_string()
                        );
                        let _ = respond_to.send(Err(messages::RestoreError::NotAvailable));
                        continue;
                    };

                    let content_hash = latest.content_hash;
                    let size = latest.size as u64;
                    // Stamp the restore now: recorded as the restored version's
                    // `observed_at` so it beats any lingering peer `deleted_at`.
                    let restored_at = clock::now_millis();

                    // Run the availability probe off-loop so the (potentially slow)
                    // peer round-trip never blocks the sole DB writer. It checks the
                    // local `keep_deleted_files` vault first, then floods a probe to
                    // peers. On success it re-enters via `ApplyRestore` (handled on
                    // this loop); on failure it replies `Err` directly.
                    let command_sender_probe = command_sender.clone();
                    let pending_fetches_probe = pending_fetches.clone();
                    let change_sender_probe = change_sender.clone();
                    tokio::spawn(async move {
                        // Local vault (or any local copy) first: cheap and avoids a
                        // network round-trip when we kept the bytes ourselves.
                        let locally_available = crate::peer::fetch::read_local_if_hash_matches(
                            &command_sender_probe,
                            file_id,
                            &content_hash,
                        )
                        .await
                        .is_some();

                        let available = if locally_available {
                            true
                        } else {
                            crate::peer::fetch::probe_availability(
                                &pending_fetches_probe,
                                file_id,
                                content_hash.clone(),
                            )
                            .await
                        };

                        if !available {
                            log::debug!(
                                "Restore: {} has no recoverable bytes (vault/peers); failing \
                                 restore",
                                file_id.to_string()
                            );
                            let _ = respond_to.send(Err(messages::RestoreError::NotAvailable));
                            return;
                        }

                        // Bytes are recoverable: hand the catalog mutation back to
                        // the DB-writer loop.
                        if change_sender_probe
                            .send(CatalogCommand::ApplyRestore {
                                file_id,
                                content_hash,
                                size,
                                restored_at,
                                respond_to,
                            })
                            .is_err()
                        {
                            log::error!(
                                "Restore: change channel closed; cannot apply restore for {}",
                                file_id.to_string()
                            );
                            // `respond_to` moved into the failed send; the
                            // waiter times out,
                            // which is the shutting-down case.
                        }
                    });

                    continue;
                }
                CatalogCommand::ApplyRestore {
                    file_id,
                    content_hash,
                    size,
                    restored_at,
                    respond_to,
                } => {
                    // The probe confirmed the bytes are recoverable. Apply the
                    // restore on the DB-writer loop: set the `restored_at` clock and
                    // clear the tombstone (no fabricated version — the three-way LWW
                    // handles it), forward the un-delete to peers, then drive
                    // placement so the bytes are pulled ONLY into directories that
                    // want them.
                    match database.apply_restore(file_id, restored_at) {
                        Ok(true) => {}
                        Ok(false) => {
                            // A delete newer than our restore stamp still wins. This
                            // should not happen for a user-initiated restore stamped
                            // "now", but stay defensive rather than lie about success.
                            log::warn!(
                                "ApplyRestore: {} still tombstoned after restore (a newer delete \
                                 wins); failing",
                                file_id.to_string()
                            );
                            let _ = respond_to.send(Err(messages::RestoreError::NotAvailable));
                            continue;
                        }
                        Err(error) => {
                            log::error!(
                                "ApplyRestore: failed to apply restore for {}: {:?}",
                                file_id.to_string(),
                                error
                            );
                            let _ = respond_to.send(Err(messages::RestoreError::NotAvailable));
                            continue;
                        }
                    }

                    let change = Change::FileRestored {
                        file_id,
                        content_hash,
                        size,
                        restored_at,
                    };
                    forward::forward_to_peers(
                        &configuration,
                        &runtime_configuration,
                        &change,
                        &ChangeOrigin::Local {
                            directory_path: std::path::PathBuf::new(),
                        },
                    )
                    .await;

                    // Re-drive placement: pull the bytes into any sync directory
                    // that should hold the now-live file (Universal dirs, matching
                    // TagBased dirs), sourcing them from the vault or a peer. If no
                    // local directory wants the file, nothing is pulled.
                    if let Some(deferred) =
                        placement::plan_placement(&command_sender, &database, file_id)
                    {
                        let pending_fetches = pending_fetches.clone();
                        let change_sender = change_sender.clone();
                        let operations = operations.clone();
                        tokio::spawn(async move {
                            placement::fetch_and_place_deferred(
                                &pending_fetches,
                                &change_sender,
                                &operations,
                                deferred,
                            )
                            .await;
                        });
                    }

                    // Publish to UI-facing API subscribers so the deleted-files view
                    // refreshes (the file is now live). This arm `continue`s and so
                    // bypasses the shared publish at the bottom of the loop; emit
                    // here, mirroring it. See `EVENT PUBLISHING` on `handle_changes`.
                    let _ = event_sender.send(change);

                    let _ = respond_to.send(Ok(()));
                    continue;
                }
                CatalogCommand::GetPreview {
                    file_id,
                    respond_to,
                } => {
                    // Overall stopwatch for this preview request, threaded through to
                    // the `ApplyPreview` re-entry so we can log the full
                    // request→reply latency in one place.
                    let request_start = std::time::Instant::now();

                    // Resolve the file's current content hash. An unknown file (no
                    // recorded version) has nothing to key a preview by.
                    let content_hash = match database.latest_version(file_id) {
                        Ok(Some(version)) => version.content_hash,
                        Ok(None) => {
                            let _ = respond_to.send(Err(messages::PreviewError::UnknownFile));
                            continue;
                        }
                        Err(error) => {
                            log::error!(
                                "GetPreview: latest_version failed for {}: {:?}",
                                file_id.to_string(),
                                error
                            );
                            let _ = respond_to.send(Err(messages::PreviewError::UnknownFile));
                            continue;
                        }
                    };

                    // Cache hit (including a cached `Preview::None`): answer now.
                    match database.preview_for(file_id, &content_hash) {
                        Ok(Some(preview)) => {
                            log::debug!(
                                "GetPreview: {} served from cache in {:?}",
                                file_id.to_string(),
                                request_start.elapsed()
                            );
                            let _ = respond_to.send(Ok(preview));
                            continue;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            log::error!(
                                "GetPreview: preview_for failed for {}: {:?}",
                                file_id.to_string(),
                                error
                            );
                            // Fall through and try to (re)generate.
                        }
                    }

                    log::debug!(
                        "GetPreview: {} cache miss; resolving off-loop (hash resolution + cache \
                         lookup took {:?})",
                        file_id.to_string(),
                        request_start.elapsed()
                    );

                    // Cache miss: resolve the preview off the writer loop, then
                    // re-enter via `ApplyPreview` to cache it and reply. This
                    // mirrors `Fetch`→`Materialize`: generation (`spawn_blocking`)
                    // and any peer round-trip must never block the sole DB writer.
                    //
                    // `can_generate` gates *local* generation: only a device whose
                    // policy generates (and whose build has the feature) decodes
                    // locally; otherwise `resolve_preview` goes straight to peers.
                    let can_generate = PREVIEW_GENERATION_COMPILED
                        && configuration.preview_generation_policy.generates();
                    // The file's extension is a type-detection hint for local
                    // generation; look it up here while we hold the DB handle (the
                    // spawned task has none). Only meaningful when we can generate.
                    #[cfg(feature = "preview-generation")]
                    let extension = if can_generate {
                        preview_extension_for(&database, file_id)
                    } else {
                        None
                    };
                    #[cfg(not(feature = "preview-generation"))]
                    let extension: Option<String> = None;
                    let command_sender_preview = command_sender.clone();
                    let pending_previews_preview = pending_previews.clone();
                    let change_sender_preview = change_sender.clone();
                    tokio::spawn(async move {
                        let resolve_start = std::time::Instant::now();
                        let result = resolve_preview(
                            &command_sender_preview,
                            &pending_previews_preview,
                            file_id,
                            &content_hash,
                            can_generate,
                            extension,
                        )
                        .await;
                        log::debug!(
                            "GetPreview: {} resolve_preview took {:?} (ok={}, total since \
                             request: {:?})",
                            file_id.to_string(),
                            resolve_start.elapsed(),
                            result.is_ok(),
                            request_start.elapsed()
                        );

                        if change_sender_preview
                            .send(CatalogCommand::ApplyPreview {
                                file_id,
                                content_hash,
                                result,
                                respond_to,
                            })
                            .is_err()
                        {
                            log::error!(
                                "GetPreview: change channel closed; cannot apply preview for {}",
                                file_id.to_string()
                            );
                            // `respond_to` moved into the failed send; the
                            // waiter observes the
                            // shutting-down case via timeout.
                        }
                    });

                    continue;
                }
                CatalogCommand::ApplyPreview {
                    file_id,
                    content_hash,
                    result,
                    respond_to,
                } => {
                    // Cache the resolved preview on the writer loop, then reply.
                    // Only an authoritative `Ok(preview)` (including a cacheable
                    // `Preview::None`) is written; a transient `Err` (e.g.
                    // `Unavailable` — local generation produced nothing and no peer
                    // served one) is forwarded to the caller unchanged and left
                    // *out* of the cache, so the next request re-attempts.
                    //
                    // Caching is best-effort: a DB error still returns the preview
                    // to the caller (they just don't get the cache benefit next
                    // time).
                    if let Ok(preview) = &result {
                        let cache_write_start = std::time::Instant::now();
                        if let Err(error) = database.record_preview(file_id, &content_hash, preview)
                        {
                            log::error!(
                                "ApplyPreview: record_preview failed for {}: {:?}",
                                file_id.to_string(),
                                error
                            );
                        }
                        log::debug!(
                            "ApplyPreview: {} cache write took {:?}; replying to caller",
                            file_id.to_string(),
                            cache_write_start.elapsed()
                        );
                    } else {
                        log::debug!(
                            "ApplyPreview: {} resolved transiently unavailable; not caching, \
                             replying to caller",
                            file_id.to_string()
                        );
                    }
                    let _ = respond_to.send(result);
                    continue;
                }
                CatalogCommand::PurgePreviews { respond_to } => {
                    // Operator-initiated wipe of the whole preview cache, handled on
                    // the sole DB writer. Previews are hash-keyed and regenerated on
                    // demand, so this only forces re-evaluation on the next request.
                    let result = database.purge_previews();
                    match &result {
                        Ok(purged) => log::info!("PurgePreviews: purged {purged} cached previews"),
                        Err(error) => {
                            log::error!("PurgePreviews: failed to purge previews: {error:?}")
                        }
                    }
                    let _ = respond_to.send(result);
                    continue;
                }
                CatalogCommand::ReconcilePlacement { file_id } => {
                    // Connect-time placement sweep, handed off from a peer session so
                    // the fetch runs here (not on the session's frame loop). If a
                    // TagBased sync directory wants this file but we lack its bytes,
                    // fetch them on demand and place them.
                    //
                    // The synchronous DB step (`plan_placement`) runs on this loop,
                    // but the follow-up (`fetch_and_place_deferred`) must NOT be
                    // awaited here: it blocks for the whole network fetch, and
                    // it finishes by enqueueing a `CatalogCommand::Materialize` onto
                    // *this* loop's own channel. Awaiting it therefore stalls the
                    // single-threaded consumer so the `Materialize` it produces can
                    // never be dequeued — the file is fetched, "materialized", but
                    // never placed, and the next reconcile re-fetches it forever.
                    // Spawn it instead (it holds only owned, Send data by design) so
                    // the loop stays free to process the resulting `Materialize`.
                    if let Some(deferred) =
                        placement::plan_placement(&command_sender, &database, file_id)
                    {
                        let pending_fetches = pending_fetches.clone();
                        let change_sender = change_sender.clone();
                        let operations = operations.clone();
                        tokio::spawn(async move {
                            placement::fetch_and_place_deferred(
                                &pending_fetches,
                                &change_sender,
                                &operations,
                                deferred,
                            )
                            .await;
                        });
                    }
                    continue;
                }
                CatalogCommand::CatalogFile {
                    file_id,
                    logical_path,
                    logical_path_modified_at,
                    content_hash,
                    size,
                    origin,
                } => {
                    files::catalog_file(
                        &configuration,
                        &runtime_configuration,
                        &mut database,
                        file_id,
                        logical_path,
                        logical_path_modified_at,
                        content_hash,
                        size,
                        origin,
                    )
                    .await;
                    continue;
                }
                CatalogCommand::Materialize {
                    file_id,
                    content,
                    content_hash,
                    origin,
                    placement,
                } => {
                    files::materialize(
                        &configuration,
                        &mut database,
                        &command_sender,
                        &change_sender,
                        &event_sender,
                        file_id,
                        content,
                        content_hash,
                        origin,
                        placement,
                    )
                    .await;
                    continue;
                }
                CatalogCommand::AnnounceProvided {
                    file_id,
                    logical_path,
                    content_hash,
                    size,
                    tags,
                } => {
                    files::announce_provided(
                        &configuration,
                        &tag_rules,
                        &runtime_configuration,
                        &mut database,
                        &event_sender,
                        file_id,
                        logical_path,
                        content_hash,
                        size,
                        tags,
                    )
                    .await;
                    continue;
                }
            };

            // Content-bearing ingestions (`ContentChange::FileAdded`/`FileChanged`)
            // carry a `FileBytes` that may still live on disk; they are handled
            // separately so the bytes are streamed into sync directories and only
            // buffered into a wire `Change` at the peer-forward boundary. Every
            // other change is pure metadata and flows through the wire-`Change`
            // match below.
            let (change, change_origin) = match ingest {
                (Ingest::Content(content_change), change_origin) => {
                    content::handle_content_change(
                        &configuration,
                        &tag_rules,
                        &runtime_configuration,
                        &mut database,
                        &command_sender,
                        &change_sender,
                        &event_sender,
                        content_change,
                        change_origin,
                    )
                    .await;
                    continue;
                }
                (Ingest::Meta(change), change_origin) => (change, change_origin),
            };

            // Apply the metadata change: file-lifecycle arms live in
            // [`files`], tag / file-tag arms in [`tagging`]. Each returns
            // `Some(publish)` when it handled the change; the first that does
            // wins. Every current `Change` variant is handled by one of them.
            let published = match files::apply_change(
                &configuration,
                &runtime_configuration,
                &mut database,
                &command_sender,
                &change_sender,
                &pending_fetches,
                &operations,
                &change,
                &change_origin,
            )
            .await
            {
                Some(publish) => publish,
                None => tagging::apply_change(
                    &configuration,
                    &runtime_configuration,
                    &mut database,
                    &command_sender,
                    &change_sender,
                    &pending_fetches,
                    &operations,
                    &change,
                    &change_origin,
                )
                .await
                .unwrap_or(true),
            };

            // Publish the applied change to UI-facing API subscribers, unless
            // the handling arm already emitted for itself (it returned
            // `false`). See `EVENT PUBLISHING` above.
            //
            // Best-effort: if there are no subscribers, or the channel is full
            // and a subscriber lags, the send/receive machinery handles it (the
            // subscriber observes `Lagged`, mapped to `Resynced` by the
            // transport).
            if published {
                let _ = event_sender.send(change);
            }
        }

        log::info!("handle_changes task exited");
    }
}
