//! Local content ingestion: applying creation-time tag rules and handling a
//! [`ContentChange`] (`FileAdded`/`FileChanged` carrying
//! [`FileBytes`](crate::file_bytes::FileBytes)).
//!
//! Lifted verbatim from the nested items inside `handle_changes`.

use std::sync::Arc;

use tagsy_core::state::{Change, ChangeOrigin};
use tagsy_core::{LogicalPath, TagId};
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;

use crate::catalog::messages::{CatalogCommand, ContentChange};
use crate::catalog::placement::{self, Placement};
use crate::catalog::previews::maybe_eager_preview;
use crate::clock;
use crate::configuration::{CompiledTagRules, Configuration, RuntimeConfiguration, SyncType};
use crate::store::{self, CatalogStore};
use crate::sync_directories::SyncDirectoryCommand;

/// `tags`, skipping any the caller already supplied.
///
/// Deduplication is not strictly required for correctness — `tag_file` is
/// an idempotent last-writer-wins upsert — but a duplicate would be
/// announced twice to every peer and would show up twice in the outgoing
/// change's tag list, so it is cheaper to drop it here.
pub(crate) fn apply_tag_rules(
    tag_rules: &CompiledTagRules,
    logical_path: &LogicalPath,
    tags: &mut Vec<TagId>,
) {
    if tag_rules.is_empty() {
        return;
    }

    for tag_id in tag_rules.tags_for(logical_path) {
        if tags.contains(&tag_id) {
            continue;
        }
        log::debug!(
            "Tag rule matched {}: applying tag {}",
            logical_path,
            tag_id.to_string()
        );
        tags.push(tag_id);
    }
}

/// Handle a [`ContentChange`] (`FileAdded`/`FileChanged` carrying
/// [`FileBytes`]): persist the version, dispatch bytes to matching sync
/// directories, and forward a wire `Change` to peers.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_content_change(
    configuration: &Configuration,
    tag_rules: &CompiledTagRules,
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    database: &mut CatalogStore,
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    change_sender: &UnboundedSender<CatalogCommand>,
    event_sender: &tokio::sync::broadcast::Sender<Change>,
    content_change: ContentChange,
    change_origin: ChangeOrigin,
) {
    match content_change {
        ContentChange::FileAdded {
            file_id,
            logical_path,
            content,
            content_hash,
            size,
            mut tags,
        } => {
            // Reconciliation and live edits can both deliver a `FileAdded`
            // for a `file_id` we already know. Branch on existence to stay
            // idempotent (see the historical notes preserved below).
            let already_exists = database.file_exists(file_id).unwrap_or_else(|error| {
                log::error!(
                    "file_exists check failed for {}: {:?}; assuming new",
                    file_id.to_string(),
                    error
                );
                false
            });

            if !already_exists {
                // Seed the path's LWW clock with our wall clock now: this is
                // a genuinely local creation, so "now" is the true origin
                // time. We stamp the same value onto the outgoing
                // `FileMetadataAdded` (via `super::forward::WireKind::Added`) so every peer
                // seeds an identical clock and a later move orders against
                // the real creation time, not each peer's receive time.
                let logical_path_modified_at = clock::now_millis();
                if let Err(error) =
                    database.add_file(file_id, &logical_path, logical_path_modified_at)
                {
                    // Do not panic: a single bad inbound change must not
                    // take down the sole DB writer.
                    log::error!(
                        "Failed to add file {} ({}): {:?}; skipping change",
                        file_id.to_string(),
                        logical_path,
                        error
                    );
                    return;
                }

                if let Err(error) = database.record_version(
                    file_id,
                    &content_hash,
                    super::forward::version_origin(&change_origin),
                    size as i64,
                ) {
                    log::error!(
                        "Failed to record initial version for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }

                // Apply the carried tags for a *locally*-ingested file.
                //
                // A local `FileAdded` from a TagBased directory carries that
                // directory's required tags: they are the ground truth for
                // this new file and must become real file-tag relationships,
                // both persisted here and propagated to peers as
                // `FileTagged` changes (so the relationship reconciles the
                // same way an API `tag_file` would). Peer-originated adds are
                // NOT tagged here — their relationships arrive via their own
                // tag manifest / `FileTagged` changes, so applying `tags`
                // again would restamp `modified_at` and clobber LWW state.
                //
                // `tags` is also used below for local dispatch filtering.
                if let ChangeOrigin::Local { .. } = &change_origin {
                    // Creation-time tag rules, merged *before* the tagging
                    // loop and before `tags` is used for dispatch
                    // filtering (`contains_all_tags`) and for the outgoing
                    // `super::forward::WireKind::Added`. Order matters: applying them later
                    // would tag the file locally but leave it out of the
                    // `TagBased` sync directories that the new tag makes it
                    // belong to, so the same file would be placed
                    // differently depending on whether its tag came from a
                    // rule or from the caller.
                    //
                    // Inside the `Local` guard because rules run only on
                    // the device that creates a file; an inbound peer file
                    // already carries the tags its origin's rules assigned.
                    apply_tag_rules(tag_rules, &logical_path, &mut tags);

                    for tag_id in &tags {
                        let modified_at = clock::now_millis();
                        if let Err(error) = database.tag_file(*tag_id, file_id, modified_at) {
                            log::error!(
                                "Failed to tag locally-added file {} with {}: {:?}",
                                file_id.to_string(),
                                tag_id.to_string(),
                                error
                            );
                            continue;
                        }

                        super::forward::forward_to_peers(
                            configuration,
                            runtime_configuration,
                            &Change::FileTagged {
                                file_id,
                                tag_id: *tag_id,
                                metadata: None,
                                modified_at,
                            },
                            &change_origin,
                        )
                        .await;
                    }
                }

                let mut targets = Vec::new();
                for sync_directory in &configuration.sync_directories {
                    if let ChangeOrigin::Local { directory_path } = &change_origin
                        && directory_path == &sync_directory.path
                        && let SyncType::TagBased { .. } = &sync_directory.sync_type
                    {
                        continue;
                    };

                    if let SyncType::TagBased {
                        tags: sync_directory_tags,
                    } = &sync_directory.sync_type
                        && !placement::contains_all_tags(sync_directory_tags, &tags)
                    {
                        continue;
                    }

                    let physical_path = sync_directory
                        .sync_type
                        .physical_for(&logical_path, file_id);
                    targets.push(Placement::Create {
                        file_id,
                        physical_path,
                        sync_directory_path: sync_directory.path.clone(),
                    });
                }

                super::forward::dispatch_and_forward(
                    configuration,
                    runtime_configuration,
                    command_sender,
                    event_sender,
                    targets,
                    content,
                    &change_origin,
                    super::forward::WireKind::Added {
                        file_id,
                        logical_path,
                        logical_path_modified_at,
                        content_hash,
                        size,
                        tags,
                    },
                )
                .await;
                // Bytes just landed in our sync directories: warm the
                // preview cache now on an eager-preview device.
                maybe_eager_preview(configuration, change_sender, file_id);
            } else {
                // Known file: decide by whether this is already the version
                // we currently hold (latest). Matching an *older* historical
                // hash is a legitimate revert and must be promoted to a new
                // version — not ignored. (Materialization echoes are already
                // suppressed upstream by the directory manager's
                // already-tracked / skip-queue guards, so this need only
                // guard against a true no-op re-announcement of the current
                // content.)
                let current_hash = database
                    .latest_version(file_id)
                    .unwrap_or_else(|error| {
                        log::error!(
                            "latest_version failed for known file {}: {:?}; treating as no-op",
                            file_id.to_string(),
                            error
                        );
                        None
                    })
                    .map(|version| version.content_hash);

                if current_hash.as_deref() == Some(content_hash.as_str()) {
                    log::debug!(
                        "Ignoring no-op FileAdded for {} (already the current version)",
                        file_id.to_string()
                    );
                    return;
                }

                log::debug!(
                    "Promoting FileAdded for known file {} to FileChanged (new content_hash)",
                    file_id.to_string()
                );
                if let Err(error) = database.record_version(
                    file_id,
                    &content_hash,
                    super::forward::version_origin(&change_origin),
                    size as i64,
                ) {
                    log::error!(
                        "Failed to record version for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }
                // A new local version supersedes any tombstone (restore
                // after delete). No-op if not tombstoned.
                if let Err(error) = database.restore_file(file_id) {
                    log::error!(
                        "Failed to clear tombstone for {}: {:?}",
                        file_id.to_string(),
                        error
                    );
                }

                let local_file_tags = database
                    .tag_ids_for_file(file_id, store::SubtagRule::Exclude)
                    .map(|iter| iter.into_iter().collect::<Vec<TagId>>())
                    .unwrap_or_else(|error| {
                        log::error!(
                            "Failed to read local tags for {}: {:?}",
                            file_id.to_string(),
                            error
                        );
                        Vec::new()
                    });

                let targets = placement::placements_for(
                    configuration,
                    &change_origin,
                    file_id,
                    &local_file_tags,
                );
                super::forward::dispatch_and_forward(
                    configuration,
                    runtime_configuration,
                    command_sender,
                    event_sender,
                    targets,
                    content,
                    &change_origin,
                    super::forward::WireKind::Changed {
                        file_id,
                        content_hash,
                        size,
                    },
                )
                .await;
                maybe_eager_preview(configuration, change_sender, file_id);
            }
        }
        ContentChange::FileChanged {
            file_id,
            content,
            content_hash,
            size,
        } => {
            let file_tags = match database.tag_ids_for_file(file_id, store::SubtagRule::Exclude) {
                Ok(tags) => tags.into_iter().collect::<Vec<TagId>>(),
                Err(error) => {
                    log::error!(
                        "FileChanged: failed to get tags for {}: {:?}; skipping",
                        file_id.to_string(),
                        error
                    );
                    return;
                }
            };

            if let Err(error) = database.record_version(
                file_id,
                &content_hash,
                super::forward::version_origin(&change_origin),
                size as i64,
            ) {
                log::error!(
                    "Failed to record version for {}: {:?}",
                    file_id.to_string(),
                    error
                );
            }
            // A new local version supersedes any tombstone (restore after
            // delete). No-op if not tombstoned.
            if let Err(error) = database.restore_file(file_id) {
                log::error!(
                    "Failed to clear tombstone for {}: {:?}",
                    file_id.to_string(),
                    error
                );
            }

            let targets =
                placement::placements_for(configuration, &change_origin, file_id, &file_tags);
            super::forward::dispatch_and_forward(
                configuration,
                runtime_configuration,
                command_sender,
                event_sender,
                targets,
                content,
                &change_origin,
                super::forward::WireKind::Changed {
                    file_id,
                    content_hash,
                    size,
                },
            )
            .await;
            maybe_eager_preview(configuration, change_sender, file_id);
        }
    }
}
