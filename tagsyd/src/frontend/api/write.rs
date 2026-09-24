//! Write half of the API: enqueue-based, fire-and-forget mutations.
//!
//! Each method expresses a mutation as a [`Change`] pushed onto the ingest bus
//! (or, for uploads, a [`CatalogCommand::AnnounceUpload`]). The single
//! `handle_changes` task remains the only DB writer; these methods add no
//! business logic and never touch the database directly.

use std::path::PathBuf;

use tagsy_core::state::{Change, ChangeOrigin};
use tagsy_core::{FileId, LogicalPath, TagId, TagStyle};

use super::{ApiError, ApiService};
use crate::catalog::messages::{CatalogCommand, Ingest};
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

    /// Upload the file at `source` as a new file named `path_name`.
    ///
    /// The bytes are first copied into the daemon's outbox (hashed while
    /// copying, synced to disk) and only then announced: from the moment this
    /// returns the daemon holds its own copy, so the caller may delete
    /// `source`. The catalog then records the file and version, places the
    /// bytes into every matching local sync directory, and announces a
    /// metadata-only `FileMetadataAdded` to peers, who pull from the outbox
    /// whenever they connect. See [`crate::outbox`].
    ///
    /// `source` is read by the daemon itself: callers share its host and user
    /// (the control socket is only reachable by the daemon's own user).
    pub async fn upload_file(
        &self,
        source: PathBuf,
        path_name: String,
        tags: Vec<TagId>,
    ) -> Result<FileId, ApiError> {
        if path_name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("path is empty".to_owned()));
        }
        let file_id = FileId::new();
        self.ingest_upload(&source, file_id, Some(LogicalPath::new(path_name)), tags)
            .await?;
        Ok(file_id)
    }

    /// Replace the content of an existing file with the bytes at `source`,
    /// ingested exactly like [`Self::upload_file`]. Records the new version,
    /// puts it in place in every local sync directory that should hold the
    /// file, and announces a metadata-only `FileMetadataChanged` to peers.
    pub async fn edit_file(&self, file_id: FileId, source: PathBuf) -> Result<(), ApiError> {
        self.ingest_upload(&source, file_id, None, Vec::new()).await
    }

    /// Copy `source` into the outbox as a version of `file_id`, then enqueue
    /// the [`CatalogCommand::AnnounceUpload`]. `logical_path` is `Some` for a
    /// new file, `None` for a new version of an existing one.
    async fn ingest_upload(
        &self,
        source: &std::path::Path,
        file_id: FileId,
        logical_path: Option<LogicalPath>,
        tags: Vec<TagId>,
    ) -> Result<(), ApiError> {
        let outbox = self.pending_fetches.outbox();
        let (content_hash, size) = outbox
            .ingest(source, file_id)
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))?;
        let announcement = CatalogCommand::AnnounceUpload {
            file_id,
            logical_path,
            content_hash: content_hash.clone(),
            size,
            tags,
        };
        if self.change_sender.send(announcement).is_err() {
            outbox.remove(file_id, &content_hash).await;
            return Err(ApiError::Internal("runtime is shutting down".to_owned()));
        }
        Ok(())
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
