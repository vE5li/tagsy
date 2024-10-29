//! The tag and file-tag arms of the catalog's metadata dispatch: `TagAdded`,
//! `TagRenamed`, `TagRecolored`, `TagChanged`, `TagRemoved`, `FileTagged`,
//! `FileTagChanged`, `FileUntagged`, `TagTagged`, `TagTagChanged`,
//! `TagUntagged`.
//!
//! Each returns `Some(publish)` when it handled the change, or `None`
//! otherwise. `File(Un)Tagged` live here (not in [`super::files`]) because they
//! mutate tag relationships and re-run tag-based placement.

use std::sync::Arc;

use tagsy_core::state::{Change, ChangeOrigin};
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;

use crate::catalog::messages::CatalogCommand;
use crate::catalog::placement;
use crate::configuration::{Configuration, RuntimeConfiguration};
use crate::operations;
use crate::peer::relay::ChunkRelay;
use crate::store::CatalogStore;
use crate::sync_directories::SyncDirectoryCommand;

/// Apply a tag / file-tag metadata change. Returns `Some(publish)` if `change`
/// was a tag variant, else `None`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_change(
    configuration: &Configuration,
    runtime_configuration: &Arc<RwLock<RuntimeConfiguration>>,
    database: &mut CatalogStore,
    command_sender: &UnboundedSender<SyncDirectoryCommand>,
    change_sender: &UnboundedSender<CatalogCommand>,
    pending_fetches: &ChunkRelay,
    operations: &operations::Operations,
    change: &Change,
    change_origin: &ChangeOrigin,
) -> Option<bool> {
    match change {
        // Every tag mutation below carries `modified_at`, stamped on the
        // originating device and preserved across the wire. It is passed
        // straight to the DB layer, which applies last-writer-wins: an
        // older change is a no-op. This makes both live application and
        // reconciliation replay idempotent and convergent.
        Change::TagAdded {
            tag_id,
            tag_name,
            color,
            metadata: _,
            modified_at,
        } => {
            if let Err(error) = database.add_tag(*tag_id, tag_name, color, *modified_at) {
                log::error!(
                    "Failed to add tag {} ({}): {:?}",
                    tag_id.to_string(),
                    tag_name,
                    error
                );
            }
            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;
            Some(true)
        }
        Change::TagRenamed {
            tag_id,
            tag_name,
            modified_at,
        } => {
            if let Err(error) = database.update_tag_name(*tag_id, tag_name, *modified_at) {
                log::error!("Failed to rename tag {}: {:?}", tag_id.to_string(), error);
            }
            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;
            Some(true)
        }
        Change::TagRecolored {
            tag_id,
            color,
            modified_at,
        } => {
            // Carries the full new color; applied with the same `modified_at`
            // LWW guard as the other tag mutations, then forwarded so peers
            // converge. Mirrors `TagRenamed`.
            if let Err(error) = database.update_tag_color(*tag_id, color, *modified_at) {
                log::error!("Failed to recolor tag {}: {:?}", tag_id.to_string(), error);
            }
            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;
            Some(true)
        }
        Change::TagChanged {
            tag_id: _,
            metadata: _,
            modified_at: _,
        } => {
            // Tag metadata is not yet stored (the whole
            // `MetadataFormat` API is `todo!()` in
            // tagsy-core). When metadata lands, apply
            // it here with the same `modified_at` LWW guard as the
            // other tag mutations and forward.
            // Deliberately not forwarded until
            // then, so we never propagate state we can't apply.
            Some(true)
        }
        Change::TagRemoved {
            tag_id,
            modified_at,
        } => {
            // Soft-delete: set the tombstone (`deleted = 1`) and bump
            // `modified_at` to the delete time. A tag reuses its
            // `modified_at` as its last-writer-wins clock, so the delete is
            // applied only if it is newer than the stored value (a newer
            // rename/recolor resurrects the tag). Forwarded either way so
            // the tombstone propagates; a stale delete is a DB no-op.
            match database.remove_tag(*tag_id, *modified_at) {
                Ok(true) => {}
                Ok(false) => {
                    log::debug!(
                        "Ignoring TagRemoved for {} (a newer edit supersedes it)",
                        tag_id.to_string()
                    );
                }
                Err(error) => {
                    log::error!("Failed to remove tag {}: {:?}", tag_id.to_string(), error);
                }
            }
            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;
            Some(true)
        }
        Change::FileTagged {
            file_id,
            tag_id,
            metadata: _,
            modified_at,
        } => {
            if let Err(error) = database.tag_file(*tag_id, *file_id, *modified_at) {
                log::error!(
                    "Failed to tag file {} with {}: {:?}",
                    file_id.to_string(),
                    tag_id.to_string(),
                    error
                );
            }

            // The file's tag set changed, so its tag-based placement may be
            // stale: a file that just gained a directory's tags should be
            // materialized there. This is also the recovery path for the
            // tag-vs-content reconciliation race (a peer transfer that
            // materialized before this `FileTagged` arrived placed the file
            // only where tags already matched). Re-run placement now, and if
            // the bytes are not local, fetch them.
            //
            // The synchronous DB step runs here on the loop, but the
            // follow-up (`fetch_and_place_deferred`) must NOT be awaited on
            // this loop: it blocks for the whole network fetch, and it
            // finishes by enqueueing a `CatalogCommand::Materialize` onto
            // *this* loop's own channel. Awaiting it stalls the
            // single-threaded consumer (so the `Materialize` it produces
            // can never be dequeued) and, in the meantime, blocks every
            // other `CatalogCommand` behind it — including UI-visible
            // change events. Spawn instead; the follow-up holds only
            // owned, `Send` data by design. See the mirror comment on
            // `CatalogCommand::ReconcilePlacement`.
            if let Some(deferred) = placement::plan_placement(command_sender, database, *file_id) {
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

            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;
            Some(true)
        }
        Change::FileTagChanged {
            file_id: _,
            tag_id: _,
            metadata: _,
            modified_at: _,
        } => {
            // Relationship metadata: deferred with the rest of the
            // metadata API. See `TagChanged`.
            Some(true)
        }
        Change::FileUntagged {
            file_id,
            tag_id,
            modified_at,
        } => {
            if let Err(error) = database.untag_file(*tag_id, *file_id, *modified_at) {
                log::error!(
                    "Failed to untag file {} from {}: {:?}",
                    file_id.to_string(),
                    tag_id.to_string(),
                    error
                );
            }

            // The file's tag set changed: a file that just lost a
            // directory's tags should be dropped from it. Re-run placement
            // (symmetric with `FileTagged`). A removal never defers, but use
            // the same two-step API for consistency.
            //
            // Spawn the async follow-up for the same reason as
            // `FileTagged` (see the comment there): even though the
            // untag path never actually defers a fetch, awaiting it on
            // this loop would still block every subsequent
            // `CatalogCommand` until the manager replies, and keeping the
            // two arms structurally identical avoids future footguns.
            if let Some(deferred) = placement::plan_placement(command_sender, database, *file_id) {
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

            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;
            Some(true)
        }
        Change::TagTagged {
            taggee_id,
            tag_id,
            metadata: _,
            modified_at,
        } => {
            if let Err(error) = database.tag_tag(*tag_id, *taggee_id, *modified_at) {
                log::error!(
                    "Failed to tag tag {} with {}: {:?}",
                    taggee_id.to_string(),
                    tag_id.to_string(),
                    error
                );
            }

            // NOTE: Currently this is correct, but if we change the subtag rules on the
            // sync directories we will have to update the sync directories
            // here too.

            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;
            Some(true)
        }
        Change::TagTagChanged {
            taggee_id: _,
            tag_id: _,
            metadata: _,
            modified_at: _,
        } => {
            // Relationship metadata: deferred with the rest of the
            // metadata API. See `TagChanged`.
            Some(true)
        }
        Change::TagUntagged {
            taggee_id,
            tag_id,
            modified_at,
        } => {
            if let Err(error) = database.untag_tag(*tag_id, *taggee_id, *modified_at) {
                log::error!(
                    "Failed to untag tag {} from {}: {:?}",
                    taggee_id.to_string(),
                    tag_id.to_string(),
                    error
                );
            }

            // NOTE: Currently this is correct, but if we change the subtag rules on the
            // sync directories we will have to update the sync directories
            // here too.

            super::forward::forward_to_peers(
                configuration,
                runtime_configuration,
                change,
                change_origin,
            )
            .await;
            Some(true)
        }
        _ => None,
    }
}
