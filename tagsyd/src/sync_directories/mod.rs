//! The sync directories: this device's on-disk sync directories and their
//! per-directory indexes.
//!
//! [`SyncDirectories`] is the actor that owns the filesystem side — the set of
//! configured sync directories (each a config [`SyncDirectory`] opened as an
//! [`OpenDirectory`]). It drains a [`SyncDirectoryCommand`] inbox (from the
//! catalog) and a debounced filesystem-watcher event stream, materialising
//! catalog decisions to disk and reporting user edits back.
//!
//! [`SyncDirectory`]: crate::configuration::SyncDirectory
//!
//! The per-arm logic lives in sibling modules: `commands` (the command inbox),
//! `events` (the watcher side), `initial_sync` (the startup passes),
//! `placement` (physical-path resolution and tag placement), and `self_write`
//! (echo suppression). [`SyncDirectories::run`] holds the select loop.

mod commands;
mod events;
mod initial_sync;
mod placement;
mod self_write;
mod watch;

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub use commands::SyncDirectoryCommand;
use ignore::gitignore::GitignoreBuilder;
use tagsy_core::state::{Change, ChangeOrigin};
use tagsy_core::{FileId, PhysicalPath, TagId};
use tokio_util::sync::CancellationToken;
use watch::{DebouncedEventKind, WatchDispatcher};

use crate::catalog::messages::{CatalogCommand, ContentChange, Ingest};
use crate::configuration::{Configuration, SyncType};
use crate::file_bytes::FileBytes;
use crate::paths::Paths;
use crate::store::{DatabaseError, DirectoryIndex, SyncDirectoryFile};

/// A boxed underlying cause. The failing operations here wrap several unrelated
/// error types — [`DatabaseError`],
/// [`FileBytesError`](crate::file_bytes::FileBytesError), `std::io::Error` — so
/// each variant carries its cause as a trait object rather than forcing one
/// concrete type. The `#[error]` message says *which* operation failed; the
/// `#[source]` says *why*.
type Cause = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
enum SyncDirectoryError {
    /// A path was handed to us that lies outside every watched sync directory.
    /// A lookup miss, not an underlying failure, so it carries no source.
    #[error("directory is not monitored")]
    UnmonitoredDirectory,
    /// A path the watcher reported (or a walk produced) is not under the sync
    /// directory it was matched to, so it cannot be made relative to it. In
    /// practice impossible — the watcher only reports paths under a watched
    /// root, and every walk is rooted there — but a `strip_prefix` failure must
    /// skip the one file rather than panic on the sole sync-directory thread.
    #[error("path {path} is not under its sync directory")]
    PathOutsideSyncDirectory {
        path: PathBuf,
        #[source]
        source: std::path::StripPrefixError,
    },
    #[error("failed to read file")]
    FailedToReadFile(#[source] Cause),
    #[error("tracked file is missing")]
    MissingTrackedFile(#[source] Cause),
    #[error("failed to add file")]
    FailedAddingFile(#[source] Cause),
    #[error("failed to change file")]
    FailedChangingFile(#[source] Cause),
    #[error("failed to move file")]
    FailedMovingFile(#[source] Cause),
    #[error("failed to remove file")]
    FailedRemovingFile(#[source] Cause),
}

struct OpenDirectory {
    path: PathBuf,
    sync_type: SyncType,
    database: DirectoryIndex,
    /// Whether files matched by a `.gitignore` are skipped on ingest. When
    /// false, [`OpenDirectory::is_ignored`] short-circuits to false and no
    /// ignore file is ever read. Also read by the `ListDirectories` command to
    /// reconstruct the config-accurate [`SyncDirectory`].
    respect_gitignore: bool,
    /// This directory's configured debounce window, in milliseconds. Not used
    /// by the ingest path itself (the window lives in the shared debouncer,
    /// keyed on `path`); retained only so the `ListDirectories` command can
    /// reconstruct the config-accurate [`SyncDirectory`].
    debounce_ms: u64,
}

impl OpenDirectory {
    /// Whether an ingest of the file at `relative_path` (relative to this
    /// directory's root) should be skipped because a `.gitignore` rule matches
    /// it. Always false when the directory did not opt into
    /// `respect_gitignore`.
    ///
    /// The matcher is built **fresh per call** from the `.gitignore` files
    /// along the path's own ancestry — the sync root's, then each intermediate
    /// directory's, down to the file's parent — so a directory that is itself a
    /// git repo (e.g. `foo/bar/` under a synced `foo/`) has its own
    /// `.gitignore` honored, not just the root's. There is deliberately **no
    /// cached matcher state**: an ignore file edited, added, or removed between
    /// events is picked up on the next check with nothing to invalidate. The
    /// cost is re-reading a handful of small files per ingest, which only
    /// happens on the discovery paths (startup walk, watcher create/move-in),
    /// never in steady state.
    ///
    /// Each `.gitignore` is compiled into its **own** matcher rooted at its own
    /// directory, not merged into one matcher rooted at the sync directory.
    /// This matters for *anchored* rules (a leading or embedded `/`, and
    /// the directory-form `build/`): they anchor relative to the directory
    /// the `.gitignore` lives in, which the `ignore` crate expresses
    /// through the matcher's root. Merging every file into a single
    /// sync-root-rooted builder would mis-anchor a rule like `app/build/`
    /// from a nested `foo/.gitignore` to `<sync-root>/app/build/` instead
    /// of `<sync-root>/foo/app/build/`, so nothing below a
    /// synced-but-nested repo would ever match. We therefore consult the
    /// matchers deepest-first and take the first definitive verdict (ignore
    /// or whitelist), which is git's precedence rule.
    ///
    /// This gates *local ingestion only*; a file the catalog places here
    /// (already tracked, or received from a peer) never reaches this check.
    fn is_ignored(&self, relative_path: &Path) -> bool {
        self.is_ignored_impl(relative_path, false)
    }

    /// Whether the *directory* at `relative_path` (relative to this directory's
    /// root) is excluded by a `.gitignore` rule — the coarser question
    /// [`is_ignored`](Self::is_ignored) does not answer, used to prune whole
    /// subtrees from the filesystem watcher rather than filter files one at a
    /// time.
    ///
    /// This is deliberately distinct from the file check: a rule like `*.log`
    /// excludes files but never lets a directory be pruned, whereas a
    /// directory-form rule (`target/`, `node_modules/`, `build/`) does. Only
    /// the latter returns true here, so a watch subtree is dropped exactly
    /// when git itself would ignore the whole directory. The matcher
    /// construction, deepest-first precedence, and stateless-per-call
    /// semantics are identical to the file check — see
    /// [`is_ignored`](Self::is_ignored).
    fn is_ignored_dir(&self, relative_path: &Path) -> bool {
        self.is_ignored_impl(relative_path, true)
    }

    /// The shared ancestry matcher-walk behind [`is_ignored`](Self::is_ignored)
    /// and [`is_ignored_dir`](Self::is_ignored_dir). `is_dir` is forwarded to
    /// the `ignore` crate so a trailing-slash directory rule (`build/`) matches
    /// a directory path but not a like-named file, matching git's semantics.
    fn is_ignored_impl(&self, relative_path: &Path, is_dir: bool) -> bool {
        if !self.respect_gitignore {
            return false;
        }

        // The absolute path is what each per-directory matcher expects: a
        // matcher rooted at `<sync>/foo` strips that prefix internally to anchor
        // its rules, so we must hand it the full on-disk path, not the
        // sync-relative one.
        let absolute_path = self.path.join(relative_path);

        // Walk the path's ancestry deepest-first (leaf → root), compiling each
        // directory's `.gitignore` into a matcher rooted at that directory. The
        // first definitive verdict wins, giving the deepest `.gitignore`
        // precedence — git's rule.
        for ancestor_dir in absolute_path.ancestors().skip(1) {
            let gitignore_file = ancestor_dir.join(".gitignore");
            if !gitignore_file.is_file() {
                continue;
            }

            let mut builder = GitignoreBuilder::new(ancestor_dir);
            if let Some(error) = builder.add(&gitignore_file) {
                log::debug!(
                    "Skipping unusable .gitignore {}: {error}",
                    gitignore_file.to_string_lossy()
                );
                continue;
            }

            let gitignore = match builder.build() {
                Ok(gitignore) => gitignore,
                Err(error) => {
                    // An unparseable ignore file must not take a file out of
                    // sync by accident: skip it (as if absent) and log.
                    log::error!(
                        "Failed to compile {}; ignoring its rules: {error}",
                        gitignore_file.to_string_lossy()
                    );
                    continue;
                }
            };

            // `matched_path_or_any_parents` so a rule on a directory (`build/`,
            // `node_modules/`) also excludes everything beneath it — the leaf
            // path is all we have at a watcher event, with no hierarchy walk.
            let matched = gitignore.matched_path_or_any_parents(&absolute_path, is_dir);
            if matched.is_ignore() {
                return true;
            }
            if matched.is_whitelist() {
                return false;
            }
        }

        false
    }

    /// Register this directory's tree with the watcher non-recursively, pruning
    /// every `.gitignore`d subtree. Returns the directories newly watched
    /// (empty on the common re-registration of an already-watched tree), so
    /// a caller re-admitting a subtree can rescan exactly those.
    ///
    /// The prune predicate maps each candidate directory's absolute path back
    /// to this directory's sync-relative space and asks
    /// [`is_ignored_dir`](Self::is_ignored_dir). The sync root itself is never
    /// pruned: it has no sync-relative path to test and a user-configured root
    /// is watched unconditionally.
    fn watch_tree(&self, dispatcher: &mut WatchDispatcher) -> Vec<PathBuf> {
        let root = self.path.clone();
        let should_prune = move |candidate: &Path| -> bool {
            match candidate.strip_prefix(&root) {
                // The root's own strip yields an empty path; never prune it.
                Ok(relative) if relative.as_os_str().is_empty() => false,
                Ok(relative) => self.is_ignored_dir(relative),
                // A candidate outside the root should not happen (the walk is
                // rooted here); be conservative and do not prune.
                Err(_) => false,
            }
        };
        dispatcher.watch_tree(&self.path, &should_prune)
    }

    /// Whether the file at sync-relative `relative_path` is already tracked in
    /// this directory's index. The lookup key is the per-kind difference a
    /// Universal directory names files by `file_id`, a TagBased one by physical
    /// path — mirroring the two initial-sync `Naming` branches. A real DB error
    /// (anything but `MissingFile`) is surfaced as `Err` so the caller can skip
    /// just that file; a missing file is a clean `Ok(false)`.
    ///
    /// Used to keep re-ingest paths (the `.gitignore` re-admission walk, which
    /// may revisit files already tracked) idempotent, the same guard the
    /// initial-sync untracked walk applies.
    fn is_file_tracked(&self, relative_path: &Path) -> Result<bool, DatabaseError> {
        match &self.sync_type {
            SyncType::Universal { .. } => {
                match FileId::from_string(&relative_path.to_string_lossy()) {
                    Some(file_id) => match self.database.get_file(file_id) {
                        Ok(_) => Ok(true),
                        Err(DatabaseError::MissingFile) => Ok(false),
                        Err(error) => Err(error),
                    },
                    // A name that is not a valid `file_id` cannot be tracked in a
                    // Universal directory.
                    None => Ok(false),
                }
            }
            SyncType::TagBased { .. } => {
                match self
                    .database
                    .get_file_id(&PhysicalPath::new(relative_path.to_string_lossy()))
                {
                    Ok(_) => Ok(true),
                    Err(DatabaseError::MissingFile) => Ok(false),
                    Err(error) => Err(error),
                }
            }
        }
    }
}

pub struct SyncDirectories {
    sync_directories: Vec<OpenDirectory>,
    change_sender: tokio::sync::mpsc::UnboundedSender<CatalogCommand>,
    /// The filesystem watcher, behind a `RefCell` because the non-recursive
    /// scheme mutates the watch set from `handle_event` (`&self`) — a new
    /// directory adds watches, a removed one drops them — the same interior
    /// mutability the `self_writes` table uses, and safe for the same reason:
    /// this actor runs single-threaded on one task.
    dispatcher: RefCell<WatchDispatcher>,
    watcher_events: tokio::sync::mpsc::UnboundedReceiver<DebouncedEventKind>,
    command_receiver: tokio::sync::mpsc::UnboundedReceiver<SyncDirectoryCommand>,
    // TODO: Make this a more robust messaging framework instead of a ref cell.
    self_writes: RefCell<self_write::SelfWrites>,
    /// Whether this device eagerly warms the preview cache. Mirrors
    /// [`Configuration::preview_generation_policy`] being
    /// [`Eager`](crate::configuration::PreviewGenerationPolicy::Eager);
    /// consulted during `run_initial_sync` so that files which were *unchanged*
    /// while the daemon was off (and so produce no `ContentChange` on startup)
    /// still get a preview generated retroactively. Live changes already flow
    /// through `handle_content_change`, which warms them itself.
    eager_previews: bool,
    /// Activity gauges for this actor and its watcher (see
    /// [`crate::activity`]).
    gauges: crate::activity::SyncDirectoryGauges,
}

impl SyncDirectories {
    pub async fn new(
        configuration: Configuration,
        paths: &Paths,
        change_sender: tokio::sync::mpsc::UnboundedSender<CatalogCommand>,
        command_receiver: tokio::sync::mpsc::UnboundedReceiver<SyncDirectoryCommand>,
        gauges: crate::activity::SyncDirectoryGauges,
    ) -> Self {
        // Each sync directory's configured debounce window, keyed on its root,
        // so the debouncer settles a directory's events on its own schedule.
        // Built before the dispatcher so the windows are in place from the
        // first event onward — there is no register-after-construct gap.
        let debounce_windows = configuration
            .sync_directories
            .iter()
            .map(|sync_directory| {
                (
                    sync_directory.path.clone(),
                    std::time::Duration::from_millis(sync_directory.debounce_ms),
                )
            })
            .collect();

        let (mut dispatcher, watcher_events) =
            WatchDispatcher::new(debounce_windows, gauges.pending_filesystem_events.clone())
                .await
                .expect("Failed to set up debouncer");

        let eager_previews = configuration.preview_generation_policy.is_eager();

        let sync_directories = configuration
            .sync_directories
            .iter()
            .filter_map(|sync_directory| {
                let path = sync_directory.path.clone();

                log::debug!(
                    "Setting up sync directory at {}",
                    sync_directory.path.to_string_lossy()
                );

                // A directory whose setup fails is *dropped* — the daemon keeps
                // running and syncs the others — so each failure must log at
                // error level with the directory path and the concrete
                // consequence ("will NOT be synced"), never a bare message.
                // Otherwise a mistyped or unmounted path degrades sync silently.
                if let Err(error) = std::fs::create_dir_all(&path) {
                    log::error!(
                        "Sync directory {} will NOT be synced: failed to create it: {error}",
                        path.to_string_lossy()
                    );
                    return None;
                }

                let database_path = paths.sync_directory_db_path(&path);

                let database = match DirectoryIndex::initialize(database_path) {
                    Ok(database) => database,
                    Err(error) => {
                        log::error!(
                            "Sync directory {} will NOT be synced: failed to open its index \
                             database: {error:?}",
                            path.to_string_lossy()
                        );
                        return None;
                    }
                };

                let open_directory = OpenDirectory {
                    path,
                    sync_type: sync_directory.sync_type.clone(),
                    database,
                    respect_gitignore: sync_directory.respect_gitignore,
                    debounce_ms: sync_directory.debounce_ms,
                };

                // Register the tree non-recursively, pruning every `.gitignore`d
                // subtree so it installs no inotify descriptors — the whole
                // point of managing the watch set by hand. The sync root itself
                // is never pruned (it has no sync-relative path to test, and a
                // root the user configured is watched unconditionally); only
                // directories *beneath* it are subject to `is_ignored_dir`.
                open_directory.watch_tree(&mut dispatcher);

                Some(open_directory)
            })
            .collect::<Vec<_>>();

        Self {
            sync_directories,
            change_sender,
            dispatcher: RefCell::new(dispatcher),
            watcher_events,
            command_receiver,
            self_writes: Default::default(),
            eager_previews,
            gauges,
        }
    }

    /// Messages waiting in either of this actor's two inboxes.
    fn queued(&self) -> usize {
        self.command_receiver.len() + self.watcher_events.len()
    }

    /// Retroactively warm the preview cache for a locally-present, *unchanged*
    /// file during the initial sync.
    ///
    /// Live changes (add/modify) already route through the ingest bus and get a
    /// preview via `handle_content_change`; a file that was untouched while the
    /// daemon was off produces no such change, so on an eager-preview device we
    /// nudge it here. Fire-and-forget `GetPreview` (reply discarded): it reuses
    /// the resolve-and-cache path, generates off the writer loop, and is a
    /// cheap no-op when the preview is already cached — so re-running the
    /// initial sync does not re-decode. A no-op unless `eager_previews` is
    /// set.
    fn maybe_eager_preview(&self, file_id: FileId) {
        if !self.eager_previews {
            return;
        }
        let (respond_to, _discard) = tokio::sync::oneshot::channel();
        let _ = self.change_sender.send(CatalogCommand::GetPreview {
            file_id,
            respond_to,
        });
    }

    /// Enqueue an applied change onto the ingest bus. Infallible from this
    /// side: the send is best-effort (the receiver is the long-lived catalog
    /// task, which only drops at shutdown), so there is nothing here for a
    /// caller to handle — hence no `Result`.
    ///
    /// FIX: Put this into a queue for proper retry handling instead.
    fn send_change(&self, sync_directory: &OpenDirectory, change: Change) {
        let change_origin = ChangeOrigin::Local {
            directory_path: sync_directory.path.clone(),
        };

        let _ = self.change_sender.send(CatalogCommand::Change(
            Ingest::from_change(change),
            change_origin,
        ));
    }

    /// Send a content-bearing change carrying [`FileBytes`] (which may still
    /// live on disk) onto the ingest bus as an [`Ingest::Content`]. Infallible;
    /// see [`send_change`](Self::send_change).
    fn send_content_change(&self, sync_directory: &OpenDirectory, content_change: ContentChange) {
        let change_origin = ChangeOrigin::Local {
            directory_path: sync_directory.path.clone(),
        };

        let _ = self.change_sender.send(CatalogCommand::Change(
            Ingest::Content(content_change),
            change_origin,
        ));
    }

    fn add_file(
        &self,
        sync_directory: &OpenDirectory,
        path: impl AsRef<Path>,
        content: FileBytes,
        content_hash: String,
        size: u64,
        tags: Vec<TagId>,
    ) -> Result<(), SyncDirectoryError> {
        let file_id = FileId::new();

        // Ingestion boundary: this file's on-disk relative path within the sync
        // directory *is* its logical identity. `add_file` is only used by
        // TagBased directories (Universal uses `upload_file`), so the physical
        // and logical paths coincide here; we still derive them explicitly.
        let logical_path = PhysicalPath::new(path.as_ref().to_string_lossy()).into_logical();
        let physical_path = sync_directory
            .sync_type
            .physical_for(&logical_path, file_id);

        sync_directory
            .database
            .add_file(file_id, &physical_path)
            .map_err(|error| SyncDirectoryError::FailedAddingFile(error.into()))?;

        // TagBased ingestion leaves the file in place: the content is a copy of
        // the on-disk source (`get_file_content` returns `FileToCopy`).
        // This device observed the file: stamp `observed_at` with our wall clock
        // as the version's origin time (carried verbatim to peers).
        self.send_content_change(sync_directory, ContentChange::FileAdded {
            file_id,
            logical_path,
            content,
            content_hash,
            size,
            observed_at: crate::clock::now_millis(),
            tags,
        });
        Ok(())
    }

    fn upload_file(
        &self,
        sync_directory: &OpenDirectory,
        path: impl AsRef<Path>,
        content: FileBytes,
        content_hash: String,
        size: u64,
        tags: Vec<TagId>,
    ) -> Result<(), SyncDirectoryError> {
        let file_id = FileId::new();

        // Ingestion boundary: the on-disk relative path becomes the file's
        // logical identity. (For a Universal directory that relative path is
        // itself the file's `file_id`, so an uploaded Universal file's logical
        // path is its id until a real name is supplied elsewhere.)
        let logical_path = PhysicalPath::new(path.as_ref().to_string_lossy()).into_logical();

        let full_path = sync_directory.path.join(path.as_ref());

        // A Universal upload removes the source from this directory: the bytes
        // move into their content-addressed location. So the content is handed
        // downstream as a *move*, and the consumer (or the fan-out in
        // `handle_changes`) is responsible for relocating/removing the source
        // rather than us deleting it eagerly here.
        //
        // The move out of this directory still produces a `Remove` watcher
        // event we must ignore; record the self-write up front (no content hash:
        // a removal has no bytes to match on).
        self.record_self_write(full_path.clone(), self_write::Expected::Removal);

        log::info!(
            "File {} was uploaded; its bytes will be moved out of this directory",
            full_path.to_string_lossy()
        );

        self.send_content_change(sync_directory, ContentChange::FileAdded {
            file_id,
            logical_path,
            content: content.into_move(),
            content_hash,
            size,
            observed_at: crate::clock::now_millis(),
            tags,
        });

        Ok(())
    }

    fn update_file_content(
        &self,
        sync_directory: &OpenDirectory,
        file_id: FileId,
        content: FileBytes,
        content_hash: String,
        size: u64,
    ) -> Result<(), SyncDirectoryError> {
        self.send_content_change(sync_directory, ContentChange::FileChanged {
            file_id,
            content,
            content_hash,
            size,
            observed_at: crate::clock::now_millis(),
        });
        Ok(())
    }

    /// Handle a file being moved/renamed *within* a sync directory on this
    /// device. The new on-disk relative path is an ingestion boundary: it
    /// defines the file's new logical identity. We update our own physical
    /// record and announce the new logical path to peers via `FileMoved`.
    ///
    /// Only meaningful for `TagBased` directories (a Universal directory has no
    /// user-visible on-disk names to move); callers guarantee that.
    fn move_file_within_directory(
        &self,
        sync_directory: &OpenDirectory,
        file_id: FileId,
        new_relative_path: impl AsRef<Path>,
    ) -> Result<(), SyncDirectoryError> {
        let logical_path =
            PhysicalPath::new(new_relative_path.as_ref().to_string_lossy()).into_logical();
        let physical_path = sync_directory
            .sync_type
            .physical_for(&logical_path, file_id);

        sync_directory
            .database
            .update_file_physical_path(file_id, &physical_path)
            .map_err(|error| SyncDirectoryError::FailedChangingFile(error.into()))?;

        self.send_change(sync_directory, Change::FileMoved {
            file_id,
            logical_path,
            // Stamp the move with our wall clock now: this is the path's
            // last-writer-wins clock, preserved verbatim as the change
            // propagates so an offline move reconciles on reconnect.
            modified_at: crate::clock::now_millis(),
        });
        Ok(())
    }

    fn remove_file_by_id(
        &self,
        sync_directory: &OpenDirectory,
        file_id: FileId,
    ) -> Result<(), SyncDirectoryError> {
        sync_directory
            .database
            .remove_file_by_id(file_id)
            .map_err(|error| SyncDirectoryError::FailedRemovingFile(error.into()))?;

        self.send_change(sync_directory, Change::FileDeleted {
            file_id,
            deleted_at: crate::clock::now_millis(),
        });
        Ok(())
    }

    fn get_all_files(
        &self,
        sync_directory: &OpenDirectory,
    ) -> Result<Vec<SyncDirectoryFile>, SyncDirectoryError> {
        sync_directory
            .database
            .get_all_files()
            .map_err(|error| SyncDirectoryError::MissingTrackedFile(error.into()))
    }

    fn get_all_files_at(
        &self,
        sync_directory: &OpenDirectory,
        physical_path: impl AsRef<Path>,
    ) -> Result<Vec<SyncDirectoryFile>, SyncDirectoryError> {
        let physical_path = PhysicalPath::new(physical_path.as_ref().to_string_lossy());
        sync_directory
            .database
            .get_all_files_at(&physical_path)
            .map_err(|error| SyncDirectoryError::MissingTrackedFile(error.into()))
    }

    /// Describe the content at `path` for ingestion without buffering it into
    /// memory: returns a [`FileBytes::FileToCopy`] referencing the file, its
    /// BLAKE3 hash (computed by streaming the file, so a large file is never
    /// held in memory at once), and its size in bytes.
    ///
    /// The size is read here, at hash time, since the file is already being
    /// opened/streamed and its exact byte length is known.
    ///
    /// `FileToCopy` is the safe default (the source is left in place).
    /// Producers whose ingestion should *consume* the source (e.g. a
    /// Universal upload) convert it to [`FileBytes::FileToMove`] via
    /// [`FileBytes::into_move`].
    async fn get_file_content(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(FileBytes, String, u64), SyncDirectoryError> {
        let content = FileBytes::FileToCopy(path.as_ref().to_path_buf());
        let content_hash = content
            .hash()
            .await
            .map_err(|error| SyncDirectoryError::FailedToReadFile(error.into()))?;
        let size = content
            .byte_len()
            .await
            .map_err(|error| SyncDirectoryError::FailedToReadFile(error.into()))?;
        Ok((content, content_hash, size))
    }

    fn sync_directory_for_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<&OpenDirectory, SyncDirectoryError> {
        self.sync_directories
            .iter()
            .find(|sync_directory| path.as_ref().starts_with(&sync_directory.path))
            .ok_or(SyncDirectoryError::UnmonitoredDirectory)
    }

    fn get_file_id(
        &self,
        sync_directory: &OpenDirectory,
        physical_path: impl AsRef<Path>,
    ) -> Result<FileId, SyncDirectoryError> {
        let physical_path = PhysicalPath::new(physical_path.as_ref().to_string_lossy());
        sync_directory
            .database
            .get_file_id(&physical_path)
            .map_err(|error| SyncDirectoryError::MissingTrackedFile(error.into()))
    }

    fn try_remove_empty_directory(&self, directory_path: impl AsRef<Path>) {
        if let Ok(mut read_dir) = directory_path.as_ref().read_dir()
            && read_dir.next().is_none()
        {
            log::info!(
                "Removing empty directory {}",
                directory_path.as_ref().to_string_lossy()
            );

            // Best-effort cleanup: a leftover empty directory is cosmetic, not a
            // reason to crash the sole sync-directory thread. (A race — another
            // process writing into it between the emptiness check and the
            // remove — is the realistic failure, and is harmless.)
            if let Err(error) = std::fs::remove_dir(&directory_path) {
                log::warn!(
                    "Failed to remove empty directory {}: {error}",
                    directory_path.as_ref().to_string_lossy()
                );
            }
        }
    }

    /// Run the directory manager.
    ///
    /// `last_known_hashes` is the last-known content hash per `FileId` as
    /// observed by previous runs of the daemon, loaded once from the main
    /// DB's `file_versions` table at startup. Used exclusively during
    /// `run_initial_sync` to decide whether an on-disk file changed while the
    /// daemon was offline; it is dropped once the initial sync finishes.
    ///
    /// Shutdown-safety invariant: **cancellation is cooperative**. `shutdown`
    /// is a third branch of the `select!` below, so a shutdown request is only
    /// observed *between* whole events — never midway through a handler. When
    /// it fires we `break` and return normally; a handler already in flight
    /// (`handle_command` / `handle_event`, both of which `.await` file I/O)
    /// runs to completion first, because `select!` only polls a new branch at
    /// an await point *between* iterations, not inside the branch future it is
    /// currently driving.
    ///
    /// This is what lets the handlers `.await` (streaming materialization,
    /// hash) safely: were shutdown instead a race against this whole future
    /// (as it once was), dropping it could abandon a partial file mirror or
    /// a DB-write-then-`record_self_write` sequence halfway through. Keep
    /// the cooperative `break` here; do not reintroduce an outer `select!`
    /// that races `run()` against the token.
    pub async fn run(
        &mut self,
        last_known_hashes: HashMap<FileId, String>,
        shutdown: CancellationToken,
        operations: crate::operations::Operations,
    ) {
        // The gauge is cloned out so the busy guard does not borrow `self`
        // across the `&mut self` handlers below.
        let inbox = self.gauges.inbox.clone();

        {
            let _busy = inbox.begin(self.queued());
            self.run_initial_sync(&last_known_hashes, &shutdown, &operations)
                .await;
        }
        self.gauges
            .initial_scan_complete
            .store(true, std::sync::atomic::Ordering::Release);

        log::info!("Directories are fully synced");

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    log::info!("Shutdown requested; stopping sync directory manager");
                    break;
                },
                command = self.command_receiver.recv() => {
                    let Some(command) = command else {
                        // TODO: Maybe this is an error?
                        break;
                    };

                    let _busy = inbox.begin(self.queued());
                    if let Err(error) = self.handle_command(command).await {
                        log::error!("Failed to handle command: {:?}", error);
                    }
                },
                watcher_event = self.watcher_events.recv() => {
                    let Some(event) = watcher_event else {
                        // TODO: Maybe this is an error?
                        break;
                    };

                    log::debug!("Received event: {:?}", event);

                    let _busy = inbox.begin(self.queued());
                    if let Err(error) = self.handle_event(event).await {
                        log::error!("Failed to handle event: {:?}", error);
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use tagsy_core::LogicalPath;

    use super::*;
    use crate::configuration::{Configuration, SyncDirectory};

    /// A unique temp directory for a test, created eagerly.
    fn temp_dir(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!(
            "tagsy-dirmgr-test-{}-{}-{}",
            label,
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&base).unwrap();
        base
    }

    /// Build a `SyncDirectories` with a single Universal sync directory at
    /// `sync_dir`, returning the manager (the change receiver is discarded; the
    /// tests only exercise `handle_command`, which does not emit changes).
    async fn universal_manager(data_dir: &Path, sync_dir: &Path) -> SyncDirectories {
        universal_manager_with(data_dir, sync_dir, false).await
    }

    /// As [`universal_manager`] but with an explicit `keep_deleted_files` flag
    /// on the Universal directory.
    async fn universal_manager_with(
        data_dir: &Path,
        sync_dir: &Path,
        keep_deleted_files: bool,
    ) -> SyncDirectories {
        let configuration = Configuration {
            sync_directories: vec![SyncDirectory {
                path: sync_dir.to_path_buf(),
                sync_type: SyncType::Universal { keep_deleted_files },
                respect_gitignore: false,
                debounce_ms: 500,
            }],
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
            preview_generation_policy: crate::configuration::PreviewGenerationPolicy::Lazy,
            max_concurrent_pulls: crate::configuration::default_max_concurrent_pulls(),
            max_concurrent_preview_generations:
                crate::configuration::default_max_concurrent_preview_generations(),
            manifest_batch_size: crate::configuration::default_manifest_batch_size(),
            tag_manifest_batch_size: crate::configuration::default_tag_manifest_batch_size(),
            purge_manifest_batch_size: crate::configuration::default_purge_manifest_batch_size(),
            reconnect_interval_ms: crate::configuration::default_reconnect_interval_ms(),
            outbox_release_interval_ms: crate::configuration::default_outbox_release_interval_ms(),
            editor_rules: Vec::new(),
            tag_rules: Vec::new(),
            home_sections: Vec::new(),
        };
        let paths = Paths::new(data_dir, None::<PathBuf>, data_dir.join("identity"));
        let (change_sender, _change_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (_command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
        SyncDirectories::new(
            configuration,
            &paths,
            change_sender,
            command_receiver,
            Default::default(),
        )
        .await
    }

    /// `CreateFile` carrying a `FileToMove` renames the source into the file's
    /// content-addressed (`file_id`) location and removes the source. This is
    /// the Universal-upload materialization path.
    #[tokio::test]
    async fn create_file_with_move_relocates_source() {
        let data_dir = temp_dir("move-data");
        let sync_dir = temp_dir("move-sync");
        let mut manager = universal_manager(&data_dir, &sync_dir).await;

        // A source file sitting in the sync directory under a human name (as if
        // just dropped in for upload).
        let source = sync_dir.join("photo.jpg");
        std::fs::write(&source, b"image-bytes").unwrap();

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::FileToMove(source.clone()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        assert!(
            !source.exists(),
            "FileToMove must consume the source at {}",
            source.display()
        );
        assert_eq!(
            std::fs::read(&destination).unwrap(),
            b"image-bytes",
            "bytes must land at the content-addressed destination"
        );
    }

    /// A watcher event for a path outside every sync directory is rejected with
    /// an error, never a panic. The sole sync-directory thread must survive a
    /// malformed event: the `run` loop logs the `Err` and carries on. This pins
    /// the item-1 contract (no fatal `unwrap` on watcher-supplied paths) at the
    /// handler boundary.
    #[tokio::test]
    async fn event_for_unmonitored_path_errors_not_panics() {
        let data_dir = temp_dir("stray-data");
        let sync_dir = temp_dir("stray-sync");
        let manager = universal_manager(&data_dir, &sync_dir).await;

        // A path under no sync directory at all.
        let stray = temp_dir("stray-elsewhere").join("ghost.txt");
        std::fs::write(&stray, b"bytes").unwrap();

        let result = manager
            .handle_event(DebouncedEventKind::Modify { file_name: stray })
            .await;

        assert!(
            matches!(result, Err(SyncDirectoryError::UnmonitoredDirectory)),
            "a stray path must surface an error, not crash the thread: {result:?}"
        );
    }

    /// `CreateFile` carrying a `FileToCopy` writes the destination and leaves
    /// the source untouched (the tag-based / keep-source path).
    #[tokio::test]
    async fn create_file_with_copy_preserves_source() {
        let data_dir = temp_dir("copy-data");
        let sync_dir = temp_dir("copy-sync");
        let mut manager = universal_manager(&data_dir, &sync_dir).await;

        // Source lives outside the sync directory (a copy target must not be
        // removed regardless of where it is).
        let external = temp_dir("copy-external");
        let source = external.join("doc.txt");
        std::fs::write(&source, b"doc-bytes").unwrap();

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::FileToCopy(source.clone()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        assert!(
            source.exists(),
            "FileToCopy must leave the source in place at {}",
            source.display()
        );
        assert_eq!(std::fs::read(&destination).unwrap(), b"doc-bytes");
    }

    /// A repeated `CreateFile` for a file the directory already holds at the
    /// same physical path is a no-op success, not a `FailedAddingFile` error.
    /// Several placement triggers (per-`FileTagged` reconciles + the connect
    /// sweep) can legitimately fetch and re-place the same file; the sink must
    /// tolerate the duplicate rather than dropping the file on a PK violation.
    #[tokio::test]
    async fn create_file_is_idempotent_for_same_file() {
        let data_dir = temp_dir("idem-data");
        let sync_dir = temp_dir("idem-sync");
        let mut manager = universal_manager(&data_dir, &sync_dir).await;

        let external = temp_dir("idem-external");
        let source = external.join("doc.txt");
        std::fs::write(&source, b"doc-bytes").unwrap();

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        let create = |content| SyncDirectoryCommand::CreateFile {
            file_id,
            physical_path: physical_path.clone(),
            content,
            sync_directory_path: sync_dir.clone(),
        };

        manager
            .handle_command(create(FileBytes::FileToCopy(source.clone())))
            .await
            .expect("first CreateFile must succeed");

        // Second CreateFile for the same file_id + physical path: must be a
        // no-op success, not an error.
        manager
            .handle_command(create(FileBytes::FileToCopy(source.clone())))
            .await
            .expect("repeated CreateFile must be an idempotent no-op, not an error");

        assert_eq!(
            std::fs::read(&destination).unwrap(),
            b"doc-bytes",
            "the file must still be present after the repeated create"
        );
    }

    /// `ChangeFile` for a file this directory does not hold creates it — the
    /// case of a file a newer edit brought back after a delete removed it
    /// here — and indexes it, so later lookups find the bytes.
    #[tokio::test]
    async fn change_file_creates_file_not_held() {
        let data_dir = temp_dir("change-missing-data");
        let sync_dir = temp_dir("change-missing-sync");
        let mut manager = universal_manager(&data_dir, &sync_dir).await;

        let file_id = FileId::new();
        manager
            .handle_command(SyncDirectoryCommand::ChangeFile {
                file_id,
                logical_path: LogicalPath::new("doc.txt"),
                content: FileBytes::InMemory(b"resurrected".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        let destination = sync_dir.join(file_id.to_string());
        assert_eq!(std::fs::read(&destination).unwrap(), b"resurrected");
        assert_eq!(manager.first_holding_path(file_id), Some(destination));
    }

    /// `ChangeFile` carrying a `FileToCopy` overwrites the existing bytes at
    /// the file's on-disk location.
    #[tokio::test]
    async fn change_file_overwrites_destination() {
        let data_dir = temp_dir("change-data");
        let sync_dir = temp_dir("change-sync");
        let mut manager = universal_manager(&data_dir, &sync_dir).await;

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        // Seed an initial version so there is something to overwrite.
        std::fs::write(&destination, b"old-bytes").unwrap();

        let external = temp_dir("change-external");
        let source = external.join("new.bin");
        std::fs::write(&source, b"new-bytes").unwrap();

        manager
            .handle_command(SyncDirectoryCommand::ChangeFile {
                file_id,
                logical_path: LogicalPath::new("doc.txt"),
                content: FileBytes::FileToCopy(source.clone()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        assert_eq!(std::fs::read(&destination).unwrap(), b"new-bytes");
        assert!(source.exists(), "FileToCopy must leave the source in place");
    }

    /// Regression: materializing a peer-received file into a Universal
    /// directory arrives at the watcher as `Move { from: None, to }` (a
    /// rename in from the daemon temp dir). It must NOT be re-ingested as a
    /// new upload — doing so minted a duplicate file whose logical path was
    /// the real file's `file_id` and looped. A move-in of an
    /// already-tracked path is ignored.
    #[tokio::test]
    async fn move_in_of_tracked_file_is_not_reingested() {
        let data_dir = temp_dir("reingest-data");
        let sync_dir = temp_dir("reingest-sync");

        // Keep the change receiver so we can assert nothing new is emitted.
        let configuration = Configuration {
            sync_directories: vec![SyncDirectory {
                path: sync_dir.to_path_buf(),
                sync_type: SyncType::Universal {
                    keep_deleted_files: false,
                },
                respect_gitignore: false,
                debounce_ms: 500,
            }],
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
            preview_generation_policy: crate::configuration::PreviewGenerationPolicy::Lazy,
            max_concurrent_pulls: crate::configuration::default_max_concurrent_pulls(),
            max_concurrent_preview_generations:
                crate::configuration::default_max_concurrent_preview_generations(),
            manifest_batch_size: crate::configuration::default_manifest_batch_size(),
            tag_manifest_batch_size: crate::configuration::default_tag_manifest_batch_size(),
            purge_manifest_batch_size: crate::configuration::default_purge_manifest_batch_size(),
            reconnect_interval_ms: crate::configuration::default_reconnect_interval_ms(),
            outbox_release_interval_ms: crate::configuration::default_outbox_release_interval_ms(),
            editor_rules: Vec::new(),
            tag_rules: Vec::new(),
            home_sections: Vec::new(),
        };
        let paths = Paths::new(&data_dir, None::<PathBuf>, data_dir.join("identity"));
        let (change_sender, mut change_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (_command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut manager = SyncDirectories::new(
            configuration,
            &paths,
            change_sender,
            command_receiver,
            Default::default(),
        )
        .await;

        // Materialize a received file: writes it under its file_id and tracks it.
        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());
        let external = temp_dir("reingest-external");
        let source = external.join("incoming.bin");
        std::fs::write(&source, b"received-bytes").unwrap();
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::FileToCopy(source),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        // Now simulate the watcher's move-in event for that same file.
        manager
            .handle_event(DebouncedEventKind::Move {
                from: None,
                to: Some(destination.clone()),
            })
            .await
            .unwrap();

        // No change must have been emitted (no re-ingestion / new FileAdded).
        assert!(
            change_receiver.try_recv().is_err(),
            "move-in of an already-tracked file must not emit a change (no re-ingestion)"
        );
        // The sync-directory DB must still hold exactly the one file.
        assert!(
            manager.sync_directories[0]
                .database
                .get_file(file_id)
                .is_ok(),
            "the original file must remain tracked"
        );
    }

    /// Build a single-Universal-directory manager, returning the manager and
    /// its change receiver so a test can assert on emitted changes.
    async fn universal_manager_with_receiver(
        data_dir: &Path,
        sync_dir: &Path,
    ) -> (
        SyncDirectories,
        tokio::sync::mpsc::UnboundedReceiver<CatalogCommand>,
    ) {
        let configuration = Configuration {
            sync_directories: vec![SyncDirectory {
                path: sync_dir.to_path_buf(),
                sync_type: SyncType::Universal {
                    keep_deleted_files: false,
                },
                respect_gitignore: false,
                debounce_ms: 500,
            }],
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
            preview_generation_policy: crate::configuration::PreviewGenerationPolicy::Lazy,
            max_concurrent_pulls: crate::configuration::default_max_concurrent_pulls(),
            max_concurrent_preview_generations:
                crate::configuration::default_max_concurrent_preview_generations(),
            manifest_batch_size: crate::configuration::default_manifest_batch_size(),
            tag_manifest_batch_size: crate::configuration::default_tag_manifest_batch_size(),
            purge_manifest_batch_size: crate::configuration::default_purge_manifest_batch_size(),
            reconnect_interval_ms: crate::configuration::default_reconnect_interval_ms(),
            outbox_release_interval_ms: crate::configuration::default_outbox_release_interval_ms(),
            editor_rules: Vec::new(),
            tag_rules: Vec::new(),
            home_sections: Vec::new(),
        };
        let paths = Paths::new(data_dir, None::<PathBuf>, data_dir.join("identity"));
        let (change_sender, change_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (_command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
        let manager = SyncDirectories::new(
            configuration,
            &paths,
            change_sender,
            command_receiver,
            Default::default(),
        )
        .await;
        (manager, change_receiver)
    }

    /// A `Modify` whose on-disk content matches the bytes the daemon just wrote
    /// (via `ChangeFile`) is recognized as our own write and suppressed — no
    /// change is re-emitted. A path-only guard cannot distinguish this from a
    /// real edit; the hash comparison is what makes the distinction reliable.
    #[tokio::test]
    async fn self_caused_modify_is_suppressed() {
        let data_dir = temp_dir("selfmod-data");
        let sync_dir = temp_dir("selfmod-sync");
        let (mut manager, mut change_receiver) =
            universal_manager_with_receiver(&data_dir, &sync_dir).await;

        // Track a file, then drain the FileAdded emitted by the create.
        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::InMemory(b"v1".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        // Daemon changes the content itself; this records a self-write for the
        // new bytes and writes them to disk. `ChangeFile` legitimately emits a
        // FileChanged for its own edit — drain everything so far.
        manager
            .handle_command(SyncDirectoryCommand::ChangeFile {
                file_id,
                logical_path: LogicalPath::new("doc.txt"),
                content: FileBytes::InMemory(b"v2".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        while change_receiver.try_recv().is_ok() {}

        // The watcher now reports the Modify for that same write. It must be
        // suppressed: on-disk content ("v2") matches the recorded hash.
        assert_eq!(std::fs::read(&destination).unwrap(), b"v2");
        manager
            .handle_event(DebouncedEventKind::Modify {
                file_name: destination.clone(),
            })
            .await
            .unwrap();

        assert!(
            change_receiver.try_recv().is_err(),
            "a self-caused Modify must not re-emit a change"
        );
    }

    /// A `Modify` whose on-disk content differs from what the daemon last wrote
    /// is a genuine user edit and must be propagated — the self-write record
    /// for a stale hash must not swallow it.
    #[tokio::test]
    async fn user_edit_after_self_write_is_propagated() {
        let data_dir = temp_dir("useredit-data");
        let sync_dir = temp_dir("useredit-sync");
        let (mut manager, mut change_receiver) =
            universal_manager_with_receiver(&data_dir, &sync_dir).await;

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::InMemory(b"v1".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        while change_receiver.try_recv().is_ok() {}

        // The user edits the file to different content than the daemon last
        // materialized. The self-write record (hash of "v1") is still pending
        // from the create, but the on-disk hash now differs.
        std::fs::write(&destination, b"user-edited").unwrap();
        manager
            .handle_event(DebouncedEventKind::Modify {
                file_name: destination.clone(),
            })
            .await
            .unwrap();

        assert!(
            change_receiver.try_recv().is_ok(),
            "a user edit with different content must be propagated"
        );
    }

    /// Self-write suppression is keyed by path, not by the predicted event
    /// variant: a materialize the watcher surfaces as a plain `Create` (rather
    /// than the `Move`-in of the other regression test) is still recognized and
    /// ignored.
    #[tokio::test]
    async fn self_caused_create_variant_is_suppressed() {
        let data_dir = temp_dir("selfcreate-data");
        let sync_dir = temp_dir("selfcreate-sync");
        let (mut manager, mut change_receiver) =
            universal_manager_with_receiver(&data_dir, &sync_dir).await;

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::InMemory(b"bytes".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        while change_receiver.try_recv().is_ok() {}

        manager
            .handle_event(DebouncedEventKind::Create {
                file_name: destination,
            })
            .await
            .unwrap();

        assert!(
            change_receiver.try_recv().is_err(),
            "a self-caused Create must not re-emit a change (no re-ingestion)"
        );
    }

    /// Build a manager with a Universal directory (index 0) and a TagBased
    /// directory (index 1) requiring `tags`.
    async fn mixed_manager(
        data_dir: &Path,
        universal_dir: &Path,
        tagged_dir: &Path,
        tags: Vec<TagId>,
    ) -> SyncDirectories {
        let configuration = Configuration {
            sync_directories: vec![
                SyncDirectory {
                    path: universal_dir.to_path_buf(),
                    sync_type: SyncType::Universal {
                        keep_deleted_files: false,
                    },
                    respect_gitignore: false,
                    debounce_ms: 500,
                },
                SyncDirectory {
                    path: tagged_dir.to_path_buf(),
                    sync_type: SyncType::TagBased { tags },
                    respect_gitignore: false,
                    debounce_ms: 500,
                },
            ],
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
            preview_generation_policy: crate::configuration::PreviewGenerationPolicy::Lazy,
            max_concurrent_pulls: crate::configuration::default_max_concurrent_pulls(),
            max_concurrent_preview_generations:
                crate::configuration::default_max_concurrent_preview_generations(),
            manifest_batch_size: crate::configuration::default_manifest_batch_size(),
            tag_manifest_batch_size: crate::configuration::default_tag_manifest_batch_size(),
            purge_manifest_batch_size: crate::configuration::default_purge_manifest_batch_size(),
            reconnect_interval_ms: crate::configuration::default_reconnect_interval_ms(),
            outbox_release_interval_ms: crate::configuration::default_outbox_release_interval_ms(),
            editor_rules: Vec::new(),
            tag_rules: Vec::new(),
            home_sections: Vec::new(),
        };
        let paths = Paths::new(&data_dir, None::<PathBuf>, data_dir.join("identity"));
        let (change_sender, _change_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (_command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
        SyncDirectories::new(
            configuration,
            &paths,
            change_sender,
            command_receiver,
            Default::default(),
        )
        .await
    }

    /// Regression: when one materialized file fans out to multiple sync
    /// directories, the fan-out gives earlier targets a `FileToCopy` and the
    /// last target a `FileToMove` (no eager source delete). Both destinations
    /// must end up with the bytes, and the move must consume the shared source.
    /// The previous code eagerly deleted the move source right after enqueuing
    /// the (asynchronously processed) copies, so the copies raced a deleted
    /// source and failed with `FailedAddingFile`, dropping the file everywhere.
    #[tokio::test]
    async fn fan_out_copy_then_move_places_into_all_directories() {
        let data_dir = temp_dir("fanout-data");
        let universal_dir = temp_dir("fanout-universal");
        let tagged_dir = temp_dir("fanout-tagged");
        let tag = TagId::new();
        let mut manager = mixed_manager(&data_dir, &universal_dir, &tagged_dir, vec![tag]).await;

        let file_id = FileId::new();
        // A single shared source, as a completed transfer's temp file would be.
        let external = temp_dir("fanout-external");
        let source = external.join("transfer-temp");
        std::fs::write(&source, b"shared-bytes").unwrap();

        // Mirror the fan-out command sequence for two targets: the earlier
        // target copies (source preserved), the last target moves (source
        // consumed). Commands are processed in order.
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new(file_id.to_string()),
                content: FileBytes::FileToCopy(source.clone()),
                sync_directory_path: universal_dir.clone(),
            })
            .await
            .expect("copy target must succeed while the source still exists");

        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new("photo.jpg"),
                content: FileBytes::FileToMove(source.clone()),
                sync_directory_path: tagged_dir.clone(),
            })
            .await
            .expect("move target must succeed and consume the source");

        assert_eq!(
            std::fs::read(universal_dir.join(file_id.to_string())).unwrap(),
            b"shared-bytes",
            "the copy target must have the bytes"
        );
        assert_eq!(
            std::fs::read(tagged_dir.join("photo.jpg")).unwrap(),
            b"shared-bytes",
            "the move target must have the bytes"
        );
        assert!(
            !source.exists(),
            "the trailing move must consume the shared source"
        );
    }

    /// Regression for the tag-vs-content reconciliation race
    /// (STREAMING_FOLLOWUPS §1.3): a peer transfer materialized a file
    /// before its tags were applied, so it landed only in the Universal
    /// directory (which has no tag filter). When the tags arrive,
    /// `ApplyPlacement` must place the file into the now-matching
    /// TagBased directory, sourcing the bytes from the Universal copy.
    #[tokio::test]
    async fn reconcile_places_file_into_newly_matching_tag_directory() {
        let data_dir = temp_dir("reconcile-add-data");
        let universal_dir = temp_dir("reconcile-add-universal");
        let tagged_dir = temp_dir("reconcile-add-tagged");
        let tag = TagId::new();
        let mut manager = mixed_manager(&data_dir, &universal_dir, &tagged_dir, vec![tag]).await;

        // Simulate the race: the file was materialized into the Universal dir
        // only (tags not yet known), stored under its file_id.
        let file_id = FileId::new();
        let logical_path = LogicalPath::new("photo.jpg");
        let external = temp_dir("reconcile-add-external");
        let source = external.join("incoming.bin");
        std::fs::write(&source, b"received-bytes").unwrap();
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new(file_id.to_string()),
                content: FileBytes::FileToCopy(source),
                sync_directory_path: universal_dir.clone(),
            })
            .await
            .unwrap();

        // The TagBased dir does not hold it yet.
        assert!(
            manager.sync_directories[1]
                .database
                .get_file(file_id)
                .is_err()
        );

        // Tags arrive: the file now matches the TagBased directory.
        let (respond_to, deferred) = tokio::sync::oneshot::channel();
        manager
            .handle_command(SyncDirectoryCommand::ApplyPlacement {
                file_id,
                logical_path: logical_path.clone(),
                file_tags: vec![tag],
                respond_to,
            })
            .await
            .unwrap();
        assert!(
            !deferred.await.unwrap(),
            "a local source copy exists, so placement must complete (not defer)"
        );

        // The file now lives in the TagBased dir under its logical path, with
        // the correct bytes, and remains in the Universal dir.
        let tagged_destination = tagged_dir.join(logical_path.as_str());
        assert_eq!(
            std::fs::read(&tagged_destination).unwrap(),
            b"received-bytes",
            "file must be placed into the newly-matching TagBased directory"
        );
        assert!(
            manager.sync_directories[1]
                .database
                .get_file(file_id)
                .is_ok(),
            "TagBased dir DB must track the newly-placed file"
        );
        assert!(
            universal_dir.join(file_id.to_string()).exists(),
            "Universal copy must remain in place (FileToCopy source)"
        );
    }

    /// Symmetric with the add case: a file that loses a TagBased directory's
    /// tags must be dropped from it, while the Universal copy is untouched.
    #[tokio::test]
    async fn reconcile_removes_file_from_no_longer_matching_tag_directory() {
        let data_dir = temp_dir("reconcile-remove-data");
        let universal_dir = temp_dir("reconcile-remove-universal");
        let tagged_dir = temp_dir("reconcile-remove-tagged");
        let tag = TagId::new();
        let mut manager = mixed_manager(&data_dir, &universal_dir, &tagged_dir, vec![tag]).await;

        let file_id = FileId::new();
        let logical_path = LogicalPath::new("photo.jpg");
        let external = temp_dir("reconcile-remove-external");
        let source = external.join("incoming.bin");
        std::fs::write(&source, b"received-bytes").unwrap();

        // The file is present in both directories.
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new(file_id.to_string()),
                content: FileBytes::FileToCopy(source.clone()),
                sync_directory_path: universal_dir.clone(),
            })
            .await
            .unwrap();
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new(logical_path.as_str()),
                content: FileBytes::FileToCopy(source),
                sync_directory_path: tagged_dir.clone(),
            })
            .await
            .unwrap();

        let tagged_destination = tagged_dir.join(logical_path.as_str());
        assert!(tagged_destination.exists());

        // The file is untagged: it no longer matches the TagBased directory.
        manager
            .handle_command(SyncDirectoryCommand::ApplyPlacement {
                file_id,
                logical_path,
                file_tags: Vec::new(),
                respond_to: {
                    let (respond_to, _) = tokio::sync::oneshot::channel();
                    respond_to
                },
            })
            .await
            .unwrap();

        assert!(
            !tagged_destination.exists(),
            "file must be removed from the TagBased directory it no longer matches"
        );
        assert!(
            manager.sync_directories[1]
                .database
                .get_file(file_id)
                .is_err(),
            "TagBased dir DB must no longer track the file"
        );
        assert!(
            universal_dir.join(file_id.to_string()).exists(),
            "Universal copy must be untouched"
        );
    }

    /// When no directory yet holds the file, a reconcile that would add it
    /// reports the deferral (so the caller fetches the bytes over the network)
    /// and creates nothing locally.
    #[tokio::test]
    async fn reconcile_defers_when_no_source_copy_exists() {
        let data_dir = temp_dir("reconcile-defer-data");
        let universal_dir = temp_dir("reconcile-defer-universal");
        let tagged_dir = temp_dir("reconcile-defer-tagged");
        let tag = TagId::new();
        let mut manager = mixed_manager(&data_dir, &universal_dir, &tagged_dir, vec![tag]).await;

        let file_id = FileId::new();
        let logical_path = LogicalPath::new("photo.jpg");

        let (respond_to, deferred) = tokio::sync::oneshot::channel();
        manager
            .handle_command(SyncDirectoryCommand::ApplyPlacement {
                file_id,
                logical_path: logical_path.clone(),
                file_tags: vec![tag],
                respond_to,
            })
            .await
            .unwrap();

        assert!(
            deferred.await.unwrap(),
            "with no source copy, placement must report deferred so the caller fetches"
        );
        assert!(
            !tagged_dir.join(logical_path.as_str()).exists(),
            "with no source copy, no file is created locally (bytes fetched by caller)"
        );
        assert!(
            manager.sync_directories[1]
                .database
                .get_file(file_id)
                .is_err(),
            "TagBased dir DB must not track a file that was never materialized"
        );
    }

    /// Create then `RemoveFile` a file in a Universal directory with
    /// `keep_deleted_files = true`: the bytes and the per-directory DB row must
    /// survive so the file can be recovered.
    #[tokio::test]
    async fn keep_deleted_files_retains_bytes_on_remove() {
        let data_dir = temp_dir("keep-data");
        let sync_dir = temp_dir("keep-sync");
        let mut manager = universal_manager_with(&data_dir, &sync_dir, true).await;

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::InMemory(b"precious".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        assert!(destination.exists());

        manager
            .handle_command(SyncDirectoryCommand::RemoveFile {
                file_id,
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        assert!(
            destination.exists(),
            "keep_deleted_files must retain the physical copy on delete"
        );
        assert_eq!(std::fs::read(&destination).unwrap(), b"precious");
        assert!(
            manager.sync_directories[0]
                .database
                .get_file(file_id)
                .is_ok(),
            "keep_deleted_files must retain the per-directory DB row for recovery"
        );
    }

    /// The default (`keep_deleted_files = false`) still deletes the bytes on
    /// `RemoveFile`.
    #[tokio::test]
    async fn default_universal_deletes_bytes_on_remove() {
        let data_dir = temp_dir("del-data");
        let sync_dir = temp_dir("del-sync");
        let mut manager = universal_manager_with(&data_dir, &sync_dir, false).await;

        let file_id = FileId::new();
        let physical_path = PhysicalPath::new(file_id.to_string());
        let destination = sync_dir.join(physical_path.as_str());

        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path,
                content: FileBytes::InMemory(b"disposable".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        assert!(destination.exists());

        manager
            .handle_command(SyncDirectoryCommand::RemoveFile {
                file_id,
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();

        assert!(
            !destination.exists(),
            "default Universal must delete the physical copy on remove"
        );
        assert!(
            manager.sync_directories[0]
                .database
                .get_file(file_id)
                .is_err(),
            "default Universal must drop the per-directory DB row"
        );
    }

    // ---- initial-sync coverage (5.2) --------------------------------------
    //
    // `run_initial_sync` reconciles what is on disk against the per-directory
    // index on startup, catching every change that happened while the daemon
    // (and its watcher) were not running: files deleted, edited, or added
    // out-of-band. These pin the three outcomes for *both* directory kinds —
    // Universal (named by `file_id`) and TagBased (named by physical path) —
    // before 5.2 collapses the two passes into one, so the dedup cannot drift.

    /// A single-TagBased-directory manager plus its change receiver.
    async fn tag_based_manager_with_receiver(
        data_dir: &Path,
        sync_dir: &Path,
        tags: Vec<TagId>,
    ) -> (
        SyncDirectories,
        tokio::sync::mpsc::UnboundedReceiver<CatalogCommand>,
    ) {
        let configuration = Configuration {
            sync_directories: vec![SyncDirectory {
                path: sync_dir.to_path_buf(),
                sync_type: SyncType::TagBased { tags },
                respect_gitignore: false,
                debounce_ms: 500,
            }],
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
            preview_generation_policy: crate::configuration::PreviewGenerationPolicy::Lazy,
            max_concurrent_pulls: crate::configuration::default_max_concurrent_pulls(),
            max_concurrent_preview_generations:
                crate::configuration::default_max_concurrent_preview_generations(),
            manifest_batch_size: crate::configuration::default_manifest_batch_size(),
            tag_manifest_batch_size: crate::configuration::default_tag_manifest_batch_size(),
            purge_manifest_batch_size: crate::configuration::default_purge_manifest_batch_size(),
            reconnect_interval_ms: crate::configuration::default_reconnect_interval_ms(),
            outbox_release_interval_ms: crate::configuration::default_outbox_release_interval_ms(),
            editor_rules: Vec::new(),
            tag_rules: Vec::new(),
            home_sections: Vec::new(),
        };
        let paths = Paths::new(data_dir, None::<PathBuf>, data_dir.join("identity"));
        let (change_sender, change_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (_command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
        let manager = SyncDirectories::new(
            configuration,
            &paths,
            change_sender,
            command_receiver,
            Default::default(),
        )
        .await;
        (manager, change_receiver)
    }

    /// Drain every message currently queued on a change receiver.
    fn drain(
        receiver: &mut tokio::sync::mpsc::UnboundedReceiver<CatalogCommand>,
    ) -> Vec<CatalogCommand> {
        let mut out = Vec::new();
        while let Ok(message) = receiver.try_recv() {
            out.push(message);
        }
        out
    }

    /// The `file_id` of a `Change` message carrying a `FileDeleted`.
    fn deleted_file_id(message: &CatalogCommand) -> Option<FileId> {
        match message {
            CatalogCommand::Change(Ingest::Meta(Change::FileDeleted { file_id, .. }), _) => {
                Some(*file_id)
            }
            _ => None,
        }
    }

    /// The `file_id` of a `Change` carrying a content `FileChanged` ingestion.
    fn changed_file_id(message: &CatalogCommand) -> Option<FileId> {
        match message {
            CatalogCommand::Change(
                Ingest::Content(ContentChange::FileChanged { file_id, .. }),
                _,
            ) => Some(*file_id),
            _ => None,
        }
    }

    /// The tags of a `Change` carrying a content `FileAdded` ingestion.
    fn added_tags(message: &CatalogCommand) -> Option<Vec<TagId>> {
        match message {
            CatalogCommand::Change(Ingest::Content(ContentChange::FileAdded { tags, .. }), _) => {
                Some(tags.clone())
            }
            _ => None,
        }
    }

    /// Universal initial sync: a tracked file deleted from disk while offline
    /// is reconciled as a `FileDeleted`.
    #[tokio::test]
    async fn initial_sync_universal_syncs_offline_deletion() {
        let data_dir = temp_dir("isync-u-del-data");
        let sync_dir = temp_dir("isync-u-del-sync");
        let (mut manager, mut rx) = universal_manager_with_receiver(&data_dir, &sync_dir).await;

        // Track a file (named by its file_id on disk, as Universal does).
        let file_id = FileId::new();
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new(file_id.to_string()),
                content: FileBytes::InMemory(b"v1".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        let _ = drain(&mut rx);

        // Delete it out-of-band, then reconcile.
        std::fs::remove_file(sync_dir.join(file_id.to_string())).unwrap();
        manager
            .run_initial_sync(
                &HashMap::new(),
                &CancellationToken::new(),
                &crate::operations::Operations::new(),
            )
            .await;

        let deletes: Vec<FileId> = drain(&mut rx).iter().filter_map(deleted_file_id).collect();
        assert_eq!(
            deletes,
            vec![file_id],
            "offline deletion must sync a FileDeleted"
        );
    }

    /// Universal initial sync: a tracked file edited on disk while offline (its
    /// hash no longer matches `last_known_hashes`) is reconciled as a
    /// `FileChanged`.
    #[tokio::test]
    async fn initial_sync_universal_syncs_offline_edit() {
        let data_dir = temp_dir("isync-u-edit-data");
        let sync_dir = temp_dir("isync-u-edit-sync");
        let (mut manager, mut rx) = universal_manager_with_receiver(&data_dir, &sync_dir).await;

        let file_id = FileId::new();
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new(file_id.to_string()),
                content: FileBytes::InMemory(b"v1".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        let _ = drain(&mut rx);

        // Overwrite on disk, and tell the reconcile the *old* hash was the last
        // one we knew, so the new content reads as a change.
        let path = sync_dir.join(file_id.to_string());
        std::fs::write(&path, b"v2-longer").unwrap();
        let last_known: HashMap<FileId, String> =
            HashMap::from([(file_id, blake3::hash(b"v1").to_hex().to_string())]);
        let operations = crate::operations::Operations::new();
        let mut operation_events = operations.subscribe();
        manager
            .run_initial_sync(&last_known, &CancellationToken::new(), &operations)
            .await;

        // The scan surfaces as one operation that ends completed, having
        // counted the file it checked.
        let mut events = Vec::new();
        while let Ok(event) = operation_events.try_recv() {
            events.push(event.operation().clone());
        }
        let last = events
            .last()
            .expect("the scan was reported as an operation");
        assert!(
            events
                .iter()
                .all(|op| op.kind == crate::operations::OperationKind::ScanningSyncDirectories)
        );
        assert!(matches!(last.status, tagsy_api::OperationStatus::Completed));
        assert!(events.iter().any(|op| matches!(
            op.status,
            tagsy_api::OperationStatus::Active { progress: Some(progress) } if progress.done >= 1
        )));

        let changed: Vec<FileId> = drain(&mut rx).iter().filter_map(changed_file_id).collect();
        assert_eq!(
            changed,
            vec![file_id],
            "offline edit must sync a FileChanged"
        );
    }

    /// Universal initial sync: a file added to disk while offline (untracked in
    /// the index, named by anything) is ingested as an upload — `FileAdded`
    /// with no tags.
    #[tokio::test]
    async fn initial_sync_universal_ingests_offline_addition() {
        let data_dir = temp_dir("isync-u-add-data");
        let sync_dir = temp_dir("isync-u-add-sync");
        let (mut manager, mut rx) = universal_manager_with_receiver(&data_dir, &sync_dir).await;

        std::fs::write(sync_dir.join("dropped.txt"), b"new-bytes").unwrap();
        manager
            .run_initial_sync(
                &HashMap::new(),
                &CancellationToken::new(),
                &crate::operations::Operations::new(),
            )
            .await;

        let adds: Vec<Vec<TagId>> = drain(&mut rx).iter().filter_map(added_tags).collect();
        assert_eq!(
            adds,
            vec![Vec::<TagId>::new()],
            "an offline-added Universal file is uploaded with no tags"
        );
    }

    /// TagBased initial sync: a tracked file deleted from disk while offline is
    /// reconciled as a `FileDeleted`.
    #[tokio::test]
    async fn initial_sync_tag_based_syncs_offline_deletion() {
        let tag = TagId::new();
        let data_dir = temp_dir("isync-t-del-data");
        let sync_dir = temp_dir("isync-t-del-sync");
        let (mut manager, mut rx) =
            tag_based_manager_with_receiver(&data_dir, &sync_dir, vec![tag]).await;

        // TagBased files are named by their logical/physical path on disk.
        let file_id = FileId::new();
        manager
            .handle_command(SyncDirectoryCommand::CreateFile {
                file_id,
                physical_path: PhysicalPath::new("notes/todo.md"),
                content: FileBytes::InMemory(b"v1".to_vec()),
                sync_directory_path: sync_dir.clone(),
            })
            .await
            .unwrap();
        let _ = drain(&mut rx);

        std::fs::remove_file(sync_dir.join("notes/todo.md")).unwrap();
        manager
            .run_initial_sync(
                &HashMap::new(),
                &CancellationToken::new(),
                &crate::operations::Operations::new(),
            )
            .await;

        let deletes: Vec<FileId> = drain(&mut rx).iter().filter_map(deleted_file_id).collect();
        assert_eq!(
            deletes,
            vec![file_id],
            "offline deletion must sync a FileDeleted"
        );
    }

    /// TagBased initial sync: a file added to disk while offline (untracked by
    /// physical path) is ingested with the directory's tags.
    #[tokio::test]
    async fn initial_sync_tag_based_ingests_offline_addition_with_tags() {
        let tag = TagId::new();
        let data_dir = temp_dir("isync-t-add-data");
        let sync_dir = temp_dir("isync-t-add-sync");
        let (mut manager, mut rx) =
            tag_based_manager_with_receiver(&data_dir, &sync_dir, vec![tag]).await;

        std::fs::write(sync_dir.join("report.md"), b"new-bytes").unwrap();
        manager
            .run_initial_sync(
                &HashMap::new(),
                &CancellationToken::new(),
                &crate::operations::Operations::new(),
            )
            .await;

        let adds: Vec<Vec<TagId>> = drain(&mut rx).iter().filter_map(added_tags).collect();
        assert_eq!(
            adds,
            vec![vec![tag]],
            "an offline-added TagBased file carries the directory's tags"
        );
    }

    /// `ListDirectories` snapshots the live set: every open directory is
    /// returned with its absolute path and `sync_type`, in order. This is the
    /// read the backup builder relies on to enumerate sync directories at
    /// backup time.
    #[tokio::test]
    async fn list_directories_snapshots_open_directories() {
        let data_dir = temp_dir("list-data");
        let universal_dir = temp_dir("list-universal");
        let tagged_dir = temp_dir("list-tagged");
        let tag = TagId::new();
        let mut manager = mixed_manager(&data_dir, &universal_dir, &tagged_dir, vec![tag]).await;

        let (respond_to, response) = tokio::sync::oneshot::channel();
        manager
            .handle_command(SyncDirectoryCommand::ListDirectories { respond_to })
            .await
            .unwrap();
        let directories = response.await.expect("handler must respond");

        assert_eq!(directories.len(), 2, "both open directories are reported");
        assert_eq!(directories[0].path, universal_dir);
        assert!(matches!(
            directories[0].sync_type,
            SyncType::Universal { .. }
        ));
        assert_eq!(directories[1].path, tagged_dir);
        assert!(matches!(
            directories[1].sync_type,
            SyncType::TagBased { .. }
        ));
    }

    /// Build a bare `OpenDirectory` rooted at `sync_dir` with the given
    /// `respect_gitignore`, for exercising [`OpenDirectory::is_ignored`] in
    /// isolation (no manager, no watcher).
    fn open_directory_at(sync_dir: &Path, respect_gitignore: bool) -> OpenDirectory {
        let database =
            DirectoryIndex::initialize(sync_dir.join("index.db")).expect("open index db");
        OpenDirectory {
            path: sync_dir.to_path_buf(),
            sync_type: SyncType::Universal {
                keep_deleted_files: false,
            },
            database,
            respect_gitignore,
            debounce_ms: 500,
        }
    }

    /// A `.gitignore` at the sync root applies to files under it, and a
    /// directory-form rule (`build/`) excludes everything beneath it.
    #[test]
    fn root_gitignore_is_honored() {
        let sync_dir = temp_dir("gi-root");
        std::fs::write(sync_dir.join(".gitignore"), "*.log\nbuild/\n").unwrap();

        let dir = open_directory_at(&sync_dir, true);

        assert!(dir.is_ignored(Path::new("app.log")));
        assert!(dir.is_ignored(Path::new("build/output.bin")));
        assert!(!dir.is_ignored(Path::new("src/main.rs")));
    }

    /// `is_ignored_dir` answers the coarser watch-pruning question: a
    /// directory-form rule (`build/`, `target/`) prunes its subtree, while a
    /// file-only pattern (`*.log`) never prunes a directory even one that would
    /// hold matching files. Nested-repo rooting works the same as the file
    /// check.
    #[test]
    fn is_ignored_dir_prunes_only_directory_rules() {
        let sync_dir = temp_dir("gi-dir-prune");
        std::fs::write(sync_dir.join(".gitignore"), "*.log\nbuild/\n").unwrap();

        std::fs::create_dir_all(sync_dir.join("bar")).unwrap();
        std::fs::write(sync_dir.join("bar/.gitignore"), "target/\n").unwrap();

        let dir = open_directory_at(&sync_dir, true);

        // A directory-form rule prunes the subtree.
        assert!(dir.is_ignored_dir(Path::new("build")));
        // A nested repo's own directory rule prunes, rooted at its own dir.
        assert!(dir.is_ignored_dir(Path::new("bar/target")));
        // A file-only pattern never prunes a directory.
        assert!(!dir.is_ignored_dir(Path::new("logs")));
        // An ordinary source directory is not pruned.
        assert!(!dir.is_ignored_dir(Path::new("src")));
        // A nested rule does not leak to a sibling.
        assert!(!dir.is_ignored_dir(Path::new("baz/target")));
    }

    /// The whole point of the fresh-per-call design: a nested directory that is
    /// its own repo has its own `.gitignore` honored, not just the sync root's.
    /// Mirrors syncing `foo/` where `foo/bar` and `foo/baz` are separate repos.
    #[test]
    fn nested_repo_gitignores_are_honored() {
        let sync_dir = temp_dir("gi-nested");
        // Root repo ignores *.tmp everywhere.
        std::fs::write(sync_dir.join(".gitignore"), "*.tmp\n").unwrap();

        // foo/bar is its own repo ignoring its target/ output.
        std::fs::create_dir_all(sync_dir.join("bar")).unwrap();
        std::fs::write(sync_dir.join("bar/.gitignore"), "target/\n").unwrap();

        // foo/baz is its own repo ignoring node_modules/.
        std::fs::create_dir_all(sync_dir.join("baz")).unwrap();
        std::fs::write(sync_dir.join("baz/.gitignore"), "node_modules/\n").unwrap();

        let dir = open_directory_at(&sync_dir, true);

        // Root rule reaches into nested dirs.
        assert!(dir.is_ignored(Path::new("bar/scratch.tmp")));
        // Each nested repo's own rule is applied.
        assert!(dir.is_ignored(Path::new("bar/target/lib.rlib")));
        assert!(dir.is_ignored(Path::new("baz/node_modules/pkg/index.js")));
        // A nested rule does not leak to a sibling repo.
        assert!(!dir.is_ignored(Path::new("baz/target/keep.txt")));
        // Ordinary source in a nested repo is synced.
        assert!(!dir.is_ignored(Path::new("bar/src/lib.rs")));
    }

    /// Regression: an *anchored* rule (a leading/embedded `/`, or the
    /// directory-form `build/`) in a `.gitignore` that is itself nested below
    /// the sync root must anchor relative to that `.gitignore`'s directory, not
    /// to the sync root. Mirrors the real case that motivated this: syncing
    /// `~/projects` where `projects/tagsy/.gitignore` carries `app/build/`; the
    /// path is `tagsy/app/build/...`, and the rule must match it.
    ///
    /// Before per-directory rooting, one sync-root-rooted matcher anchored the
    /// rule to `<sync>/app/build/` and never matched anything under
    /// `tagsy/app/build/`, so build artifacts synced anyway.
    #[test]
    fn anchored_rule_in_nested_gitignore_anchors_to_its_own_dir() {
        let sync_dir = temp_dir("gi-anchored-nested");

        // `tagsy/` is a repo nested one level below the sync root, ignoring its
        // own `app/build/` (an anchored, directory-form rule).
        std::fs::create_dir_all(sync_dir.join("tagsy")).unwrap();
        std::fs::write(sync_dir.join("tagsy/.gitignore"), "app/build/\n/gen\n").unwrap();

        let dir = open_directory_at(&sync_dir, true);

        // The anchored directory rule matches everything beneath it.
        assert!(dir.is_ignored(Path::new(
            "tagsy/app/build/linux/x64/release/cmake_install.cmake"
        )));
        // A root-anchored rule (`/gen`) matches only at the .gitignore's dir.
        assert!(dir.is_ignored(Path::new("tagsy/gen")));
        assert!(dir.is_ignored(Path::new("tagsy/gen/output.rs")));
        // The same rule text must NOT match at the sync root (mis-anchoring),
        // nor a same-named directory that is not under the anchor.
        assert!(!dir.is_ignored(Path::new("app/build/output.bin")));
        assert!(!dir.is_ignored(Path::new("tagsy/app/src/main.dart")));
    }

    /// With `respect_gitignore` off, no `.gitignore` is consulted at all — and
    /// the implicit `.git` exclusion does not apply either, since the whole
    /// check short-circuits.
    #[test]
    fn respect_gitignore_disabled_ignores_nothing() {
        let sync_dir = temp_dir("gi-off");
        std::fs::write(sync_dir.join(".gitignore"), "*.log\n").unwrap();

        let dir = open_directory_at(&sync_dir, false);

        assert!(!dir.is_ignored(Path::new("app.log")));
        assert!(!dir.is_ignored(Path::new(".git/config")));
    }

    /// A newly-added nested `.gitignore` is picked up on the next check with no
    /// state to invalidate — the property that motivated building fresh per
    /// call.
    #[test]
    fn added_gitignore_takes_effect_without_cache_invalidation() {
        let sync_dir = temp_dir("gi-fresh");
        std::fs::create_dir_all(sync_dir.join("bar")).unwrap();
        let dir = open_directory_at(&sync_dir, true);

        // No ignore file yet: the file would sync.
        assert!(!dir.is_ignored(Path::new("bar/secret.key")));

        // Add one after the directory is already open.
        std::fs::write(sync_dir.join("bar/.gitignore"), "*.key\n").unwrap();

        // Next check honors it immediately, no re-open required.
        assert!(dir.is_ignored(Path::new("bar/secret.key")));
    }
}
