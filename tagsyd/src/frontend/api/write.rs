//! Write half of the API: enqueue-based, fire-and-forget mutations.
//!
//! Each method expresses a mutation as a [`Change`] pushed onto the ingest bus
//! (or, for the provider-backed uploads, a
//! [`CatalogCommand::AnnounceProvided`]). The single `handle_changes` task
//! remains the only DB writer; these methods add no business logic and never
//! touch the database directly.

use std::path::PathBuf;
use std::sync::Arc;

use tagsy_core::state::{Change, ChangeOrigin};
use tagsy_core::{FileId, LogicalPath, TagId, TagStyle};

use super::{ApiError, ApiService};
use crate::catalog::messages::{CatalogCommand, Ingest};
use crate::peer::transfer::ChunkSource;
use crate::store::DeletedRule;

impl ApiService {
    /// Enqueue a locally-originated change onto the ingest bus.
    ///
    /// `directory_path` in the [`ChangeOrigin::Local`] is a sentinel that must
    /// not match any configured sync directory, so `handle_changes` dispatches
    /// the change to every matching sync directory rather than skipping one as
    /// the "source". An empty path never matches a real sync-directory path.
    pub(super) fn enqueue(&self, change: Change) -> Result<(), ApiError> {
        self.change_sender
            .send(CatalogCommand::Change(
                Ingest::from_change(change),
                ChangeOrigin::Local {
                    directory_path: PathBuf::new(),
                },
            ))
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))
    }

    /// Create a tag. Mints a fresh `TagId` and enqueues `Change::TagAdded`;
    /// the id is returned immediately (persistence is asynchronous — observe
    /// the event stream for confirmation).
    ///
    /// `style` is the tag's full initial visual style. Callers with no styling
    /// preference pass `TagStyle::default()`; the empty-color special-case that
    /// used to live here is gone — an unset dot color is no longer
    /// representable, every property has a concrete value.
    pub fn create_tag(&self, name: String, style: TagStyle) -> Result<TagId, ApiError> {
        if name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("tag name is empty".to_owned()));
        }
        // A locally-originated mutation is stamped with our wall clock now; the
        // timestamp then rides the change unchanged to peers for LWW.
        let tag_id = TagId::new();
        self.enqueue(Change::TagAdded {
            tag_id,
            tag_name: name,
            style,
            metadata: None,
            modified_at: crate::clock::now_millis(),
        })?;
        Ok(tag_id)
    }

    /// Delete a tag. Enqueues `Change::TagRemoved`, stamped with our wall clock
    /// now: a tag reuses `modified_at` as its last-writer-wins clock, so the
    /// delete carries the timestamp here.
    pub fn delete_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        self.enqueue(Change::TagRemoved {
            tag_id,
            modified_at: crate::clock::now_millis(),
        })
    }

    /// Restore a soft-deleted tag.
    ///
    /// Unlike a file, a tag carries no content and reuses `modified_at` as its
    /// single last-writer-wins clock, so a restore is simply re-announcing the
    /// tag's current definition with a fresh timestamp: `add_tag` upserts with
    /// `deleted = 0` and wins LWW over the (older) delete, both locally and on
    /// every peer. It therefore reuses the `Change::TagAdded` path rather than
    /// a bespoke wire variant, and is fire-and-forget (no bytes to recover,
    /// so it cannot "fail to find a source" the way a file restore can).
    ///
    /// Returns [`ApiError::UnknownId`] if the tag is unknown. Reading it with
    /// `Include` means an already-live tag is re-announced harmlessly (the LWW
    /// guard makes it a no-op if nothing changed).
    pub fn restore_tag(&self, tag_id: TagId) -> Result<(), ApiError> {
        let tag = {
            let database = self.open_read()?;
            database.tag_from_id(tag_id, DeletedRule::Include)?
        };
        self.enqueue(Change::TagAdded {
            tag_id,
            tag_name: tag.name,
            style: tag.style,
            metadata: None,
            modified_at: crate::clock::now_millis(),
        })
    }

    /// Rename a tag. Enqueues `Change::TagRenamed`, stamped with our wall clock
    /// now for last-writer-wins reconciliation.
    pub fn rename_tag(&self, tag_id: TagId, name: String) -> Result<(), ApiError> {
        if name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("tag name is empty".to_owned()));
        }
        self.enqueue(Change::TagRenamed {
            tag_id,
            tag_name: name,
            modified_at: crate::clock::now_millis(),
        })
    }

    /// Replace a tag's visual style. Enqueues `Change::TagRestyled` carrying
    /// the full new [`TagStyle`], stamped with our wall clock now for
    /// last-writer-wins. Dot color is one property of the style, so this is
    /// also how a recolor is performed (the former `set_tag_color` is gone).
    pub fn set_tag_style(&self, tag_id: TagId, style: TagStyle) -> Result<(), ApiError> {
        self.enqueue(Change::TagRestyled {
            tag_id,
            style,
            modified_at: crate::clock::now_millis(),
        })
    }

    /// Upload a file whose bytes the client provides on demand.
    ///
    /// The client has already computed `content_hash` (by streaming its own
    /// file) and will serve the bytes chunk-by-chunk as a temporary provider;
    /// no bytes are passed here. Mints a `FileId`, records the file + version,
    /// and announces a metadata-only `FileMetadataAdded` to peers, which then
    /// pull the content from the provider the control layer registers.
    pub fn upload_file(
        &self,
        path_name: String,
        content_hash: String,
        size: u64,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        if path_name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("path is empty".to_owned()));
        }
        let file_id = FileId::new();
        self.change_sender
            .send(CatalogCommand::AnnounceProvided {
                file_id,
                logical_path: Some(LogicalPath::new(path_name)),
                content_hash,
                size,
                tags,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;
        Ok(file_id)
    }

    /// Register a temporary chunk provider for a file the client is serving on
    /// demand. Delegates to the transfer subsystem's provider registry.
    pub async fn register_provider(
        &self,
        file_id: FileId,
        content_hash: String,
        source: Arc<dyn ChunkSource>,
    ) {
        self.pending_fetches
            .register_provider(file_id, content_hash, source)
            .await;
    }

    /// Remove a temporary provider (the client released the file).
    pub async fn unregister_provider(&self, file_id: FileId, content_hash: &str) {
        self.pending_fetches
            .unregister_provider(file_id, content_hash)
            .await;
    }

    /// Replace the content of an existing file, provided on demand by the
    /// client (see [`Self::upload_file`]). Records the new version and
    /// announces a metadata-only `FileMetadataChanged` to peers, which pull
    /// from the provider.
    pub fn edit_file(
        &self,
        file_id: FileId,
        content_hash: String,
        size: u64,
    ) -> Result<(), ApiError> {
        self.change_sender
            .send(CatalogCommand::AnnounceProvided {
                file_id,
                logical_path: None,
                content_hash,
                size,
                tags: Vec::new(),
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))
    }

    /// Delete a file. Enqueues `Change::FileDeleted`, stamped with our wall
    /// clock now for last-writer-wins against a later edit.
    pub fn delete_file(&self, file_id: FileId) -> Result<(), ApiError> {
        self.enqueue(Change::FileDeleted {
            file_id,
            deleted_at: crate::clock::now_millis(),
        })
    }

    /// Move (rename) a file to a new logical path. Enqueues
    /// `Change::FileMoved`, stamped with our wall clock now as the path's
    /// last-writer-wins clock; each receiving sync directory derives its own
    /// physical placement.
    pub fn move_file(&self, file_id: FileId, logical_path: String) -> Result<(), ApiError> {
        if logical_path.trim().is_empty() {
            return Err(ApiError::InvalidArgument("path is empty".to_owned()));
        }
        self.enqueue(Change::FileMoved {
            file_id,
            logical_path: LogicalPath::new(logical_path),
            modified_at: crate::clock::now_millis(),
        })
    }

    /// Apply `tag_id` to `file_id`. Enqueues `Change::FileTagged`.
    pub fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.enqueue(Change::FileTagged {
            file_id,
            tag_id,
            metadata: None,
            modified_at: crate::clock::now_millis(),
        })
    }

    /// Remove `tag_id` from `file_id`. Enqueues `Change::FileUntagged`.
    pub fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<(), ApiError> {
        self.enqueue(Change::FileUntagged {
            file_id,
            tag_id,
            modified_at: crate::clock::now_millis(),
        })
    }

    /// Make `subtag_id` a subtag (child) of `parent_id` in the tag hierarchy.
    /// Enqueues `Change::TagTagged`.
    ///
    /// A tag cannot be its own subtag; that is rejected here (with
    /// [`ApiError::InvalidArgument`]) rather than only being caught by the
    /// database inside the change pipeline, so the caller learns immediately.
    pub fn tag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        if parent_id == subtag_id {
            return Err(ApiError::InvalidArgument(
                "a tag cannot be its own subtag".to_owned(),
            ));
        }
        self.enqueue(Change::TagTagged {
            taggee_id: subtag_id,
            tag_id: parent_id,
            metadata: None,
            modified_at: crate::clock::now_millis(),
        })
    }

    /// Remove `subtag_id` as a subtag of `parent_id`. Enqueues
    /// `Change::TagUntagged`.
    pub fn untag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<(), ApiError> {
        self.enqueue(Change::TagUntagged {
            taggee_id: subtag_id,
            tag_id: parent_id,
            modified_at: crate::clock::now_millis(),
        })
    }
}
