//! Write half of the API: mutations applied by the catalog writer.
//!
//! Each method expresses a mutation as a [`Change`] sent to the writer as a
//! [`CatalogCommand::LocalChange`] (or, for uploads, a
//! [`CatalogCommand::AnnounceUpload`]) and waits until the writer has applied
//! it, answering with the entry it touched as it now stands. The writer
//! remains the only DB writer; these methods add no business logic and never
//! touch the database directly.
//!
//! The wait has no deadline of its own: the writer always answers, and it
//! drops the reply only when it shuts down. A change queued behind a large
//! sync simply takes as long as the queue ahead of it.

use std::path::PathBuf;

use tagsy_core::state::{Change, ChangeOrigin};
use tagsy_core::{FileId, FileInfo, LogicalPath, TagId, TagStyle};
use tokio::sync::oneshot;

use super::{ApiError, ApiService};
use crate::catalog::messages::{CatalogCommand, ChangeReply, Ingest};
use crate::store::{DeletedRule, Tag};

impl ApiService {
    /// Enqueue a locally-originated change without waiting for it.
    ///
    /// Only for bulk work whose caller reports what it *enqueued*
    /// ([`Self::retag`]); every API mutation goes through
    /// [`Self::apply_to_file`] / [`Self::apply_to_tag`] instead.
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

    /// Have the writer apply `change`, then answer with `file_id`'s entry.
    async fn apply_to_file(&self, change: Change, file_id: FileId) -> Result<FileInfo, ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.send_to_writer(CatalogCommand::LocalChange {
            change,
            reply: ChangeReply::File {
                file_id,
                respond_to,
            },
        })?;
        Self::await_applied(response).await
    }

    /// Have the writer apply `change`, then answer with `tag_id`'s entry.
    async fn apply_to_tag(&self, change: Change, tag_id: TagId) -> Result<Tag, ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.send_to_writer(CatalogCommand::LocalChange {
            change,
            reply: ChangeReply::Tag { tag_id, respond_to },
        })?;
        Self::await_applied(response).await
    }

    /// Create a tag. Mints a fresh `TagId`, has `Change::TagAdded` applied, and
    /// returns the new tag.
    ///
    /// `style` is the tag's full initial visual style. Callers with no styling
    /// preference pass `TagStyle::default()`; the empty-color special-case that
    /// used to live here is gone — an unset dot color is no longer
    /// representable, every property has a concrete value.
    pub async fn create_tag(&self, name: String, style: TagStyle) -> Result<Tag, ApiError> {
        if name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("tag name is empty".to_owned()));
        }
        // A locally-originated mutation is stamped with our wall clock now; the
        // timestamp then rides the change unchanged to peers for LWW.
        let tag_id = TagId::new();
        let change = Change::TagAdded {
            tag_id,
            tag_name: name,
            style,
            metadata: None,
            modified_at: crate::clock::now_millis(),
        };
        self.apply_to_tag(change, tag_id).await
    }

    /// Delete a tag. Applies `Change::TagRemoved`, stamped with our wall clock
    /// now: a tag reuses `modified_at` as its last-writer-wins clock, so the
    /// delete carries the timestamp here. Returns the (tombstoned) tag.
    pub async fn delete_tag(&self, tag_id: TagId) -> Result<Tag, ApiError> {
        let change = Change::TagRemoved {
            tag_id,
            modified_at: crate::clock::now_millis(),
        };
        self.apply_to_tag(change, tag_id).await
    }

    /// Restore a soft-deleted tag, returning it.
    ///
    /// Unlike a file, a tag carries no content and reuses `modified_at` as its
    /// single last-writer-wins clock, so a restore is simply re-announcing the
    /// tag's current definition with a fresh timestamp: `add_tag` upserts with
    /// `deleted = 0` and wins LWW over the (older) delete, both locally and on
    /// every peer. It therefore reuses the `Change::TagAdded` path rather than
    /// a bespoke wire variant (no bytes to recover, so it cannot "fail to find
    /// a source" the way a file restore can).
    ///
    /// Returns [`ApiError::UnknownId`] if the tag is unknown. Reading it with
    /// `Include` means an already-live tag is re-announced harmlessly (the LWW
    /// guard makes it a no-op if nothing changed).
    pub async fn restore_tag(&self, tag_id: TagId) -> Result<Tag, ApiError> {
        let tag = {
            let database = self.open_read()?;
            database.tag_from_id(tag_id, DeletedRule::Include)?
        };
        let change = Change::TagAdded {
            tag_id,
            tag_name: tag.name,
            style: tag.style,
            metadata: None,
            modified_at: crate::clock::now_millis(),
        };
        self.apply_to_tag(change, tag_id).await
    }

    /// Rename a tag. Applies `Change::TagRenamed`, stamped with our wall clock
    /// now for last-writer-wins reconciliation, and returns the tag.
    pub async fn rename_tag(&self, tag_id: TagId, name: String) -> Result<Tag, ApiError> {
        if name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("tag name is empty".to_owned()));
        }
        let change = Change::TagRenamed {
            tag_id,
            tag_name: name,
            modified_at: crate::clock::now_millis(),
        };
        self.apply_to_tag(change, tag_id).await
    }

    /// Replace a tag's visual style. Applies `Change::TagRestyled` carrying
    /// the full new [`TagStyle`], stamped with our wall clock now for
    /// last-writer-wins, and returns the tag. Dot color is one property of the
    /// style, so this is also how a recolor is performed.
    pub async fn set_tag_style(&self, tag_id: TagId, style: TagStyle) -> Result<Tag, ApiError> {
        let change = Change::TagRestyled {
            tag_id,
            style,
            modified_at: crate::clock::now_millis(),
        };
        self.apply_to_tag(change, tag_id).await
    }

    /// Upload the file at `source` as a new file named `path_name`, returning
    /// the file as recorded.
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
    ) -> Result<FileInfo, ApiError> {
        if path_name.trim().is_empty() {
            return Err(ApiError::InvalidArgument("path is empty".to_owned()));
        }
        let file_id = FileId::new();
        self.ingest_upload(&source, file_id, Some(LogicalPath::new(path_name)), tags)
            .await
    }

    /// Replace the content of an existing file with the bytes at `source`,
    /// ingested exactly like [`Self::upload_file`]. Records the new version,
    /// puts it in place in every local sync directory that should hold the
    /// file, announces a metadata-only `FileMetadataChanged` to peers, and
    /// returns the file at its new version.
    pub async fn edit_file(&self, file_id: FileId, source: PathBuf) -> Result<FileInfo, ApiError> {
        self.ingest_upload(&source, file_id, None, Vec::new()).await
    }

    /// Copy `source` into the outbox as a version of `file_id`, then have the
    /// writer record it ([`CatalogCommand::AnnounceUpload`]). `logical_path`
    /// is `Some` for a new file, `None` for a new version of an existing one.
    async fn ingest_upload(
        &self,
        source: &std::path::Path,
        file_id: FileId,
        logical_path: Option<LogicalPath>,
        tags: Vec<TagId>,
    ) -> Result<FileInfo, ApiError> {
        let outbox = self.pending_fetches.outbox();
        let (content_hash, size) = outbox
            .ingest(source, file_id)
            .await
            .map_err(|error| ApiError::Internal(error.to_string()))?;
        let (respond_to, response) = oneshot::channel();
        let announcement = CatalogCommand::AnnounceUpload {
            file_id,
            logical_path,
            content_hash: content_hash.clone(),
            size,
            tags,
            respond_to,
        };
        if self.change_sender.send(announcement).is_err() {
            outbox.remove(file_id, &content_hash).await;
            return Err(ApiError::Internal("runtime is shutting down".to_owned()));
        }
        Self::await_applied(response).await
    }

    /// Delete a file. Applies `Change::FileDeleted`, stamped with our wall
    /// clock now for last-writer-wins against a later edit, and returns the
    /// (tombstoned) file.
    pub async fn delete_file(&self, file_id: FileId) -> Result<FileInfo, ApiError> {
        let change = Change::FileDeleted {
            file_id,
            deleted_at: crate::clock::now_millis(),
        };
        self.apply_to_file(change, file_id).await
    }

    /// Move (rename) a file to a new logical path. Applies
    /// `Change::FileMoved`, stamped with our wall clock now as the path's
    /// last-writer-wins clock (each receiving sync directory derives its own
    /// physical placement), and returns the file.
    pub async fn move_file(
        &self,
        file_id: FileId,
        logical_path: String,
    ) -> Result<FileInfo, ApiError> {
        if logical_path.trim().is_empty() {
            return Err(ApiError::InvalidArgument("path is empty".to_owned()));
        }
        let change = Change::FileMoved {
            file_id,
            logical_path: LogicalPath::new(logical_path),
            modified_at: crate::clock::now_millis(),
        };
        self.apply_to_file(change, file_id).await
    }

    /// Apply `tag_id` to `file_id` (`Change::FileTagged`), returning the file.
    pub async fn tag_file(&self, tag_id: TagId, file_id: FileId) -> Result<FileInfo, ApiError> {
        self.apply_to_file(Self::file_tagged(tag_id, file_id), file_id)
            .await
    }

    /// The change [`Self::tag_file`] applies; [`Self::retag`] enqueues the
    /// same.
    pub(super) fn file_tagged(tag_id: TagId, file_id: FileId) -> Change {
        Change::FileTagged {
            file_id,
            tag_id,
            metadata: None,
            modified_at: crate::clock::now_millis(),
        }
    }

    /// Remove `tag_id` from `file_id` (`Change::FileUntagged`), returning the
    /// file.
    pub async fn untag_file(&self, tag_id: TagId, file_id: FileId) -> Result<FileInfo, ApiError> {
        let change = Change::FileUntagged {
            file_id,
            tag_id,
            modified_at: crate::clock::now_millis(),
        };
        self.apply_to_file(change, file_id).await
    }

    /// Make `subtag_id` a subtag (child) of `parent_id` in the tag hierarchy
    /// (`Change::TagTagged`), returning the subtag.
    ///
    /// A tag cannot be its own subtag; that is rejected here (with
    /// [`ApiError::InvalidArgument`]) rather than only being caught by the
    /// database inside the change pipeline, so the caller learns immediately.
    pub async fn tag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<Tag, ApiError> {
        if parent_id == subtag_id {
            return Err(ApiError::InvalidArgument(
                "a tag cannot be its own subtag".to_owned(),
            ));
        }
        let change = Change::TagTagged {
            taggee_id: subtag_id,
            tag_id: parent_id,
            metadata: None,
            modified_at: crate::clock::now_millis(),
        };
        self.apply_to_tag(change, subtag_id).await
    }

    /// Remove `subtag_id` as a subtag of `parent_id` (`Change::TagUntagged`),
    /// returning the subtag.
    pub async fn untag_tag(&self, parent_id: TagId, subtag_id: TagId) -> Result<Tag, ApiError> {
        let change = Change::TagUntagged {
            taggee_id: subtag_id,
            tag_id: parent_id,
            modified_at: crate::clock::now_millis(),
        };
        self.apply_to_tag(change, subtag_id).await
    }
}
