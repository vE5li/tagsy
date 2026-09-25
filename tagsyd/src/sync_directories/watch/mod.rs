//! The filesystem watcher: [`WatchDispatcher`] owns a `notify` recommended
//! watcher and a background task that periodically drains settled events from
//! the [`Debouncer`] onto a channel. The debounce/coalesce state machine and
//! the event vocabulary live in [`debounce`].
//!
//! The watcher is driven **non-recursively**: the dispatcher registers one
//! `notify` watch per directory and tracks the live set itself, so a subtree
//! that a `.gitignore` excludes (`target/`, `node_modules/`) installs no
//! inotify descriptors at all. This is the whole reason the dispatcher is
//! stateful — `RecursiveMode::Recursive` would watch every descendant
//! regardless, which on a large ignored tree exhausts
//! `fs.inotify.max_user_watches`. Because non-recursive descriptors do not
//! cascade, the dispatcher must add and remove each descendant by hand
//! ([`watch_tree`](WatchDispatcher::watch_tree) /
//! [`unwatch_tree`](WatchDispatcher::unwatch_tree)).

pub mod debounce;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use debounce::DebouncedEventKind;
use debounce::Debouncer;
pub use notify;
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use walkdir::WalkDir;

pub struct WatchDispatcher {
    stop: Arc<AtomicBool>,
    watcher: RecommendedWatcher,
    /// Every directory currently registered with the `notify` watcher. Kept
    /// because non-recursive watches do not cascade: to drop a subtree (a
    /// directory removed, moved out, or newly `.gitignore`d) the dispatcher
    /// must unwatch each descendant descriptor individually, and it can
    /// only know which those are by having recorded them. The set is the
    /// dispatcher's single source of truth for "what am I watching right
    /// now".
    watched: HashSet<PathBuf>,
    _task: tokio::task::JoinHandle<()>,
}

impl WatchDispatcher {
    /// Construct a dispatcher whose debouncer applies a per-sync-root debounce
    /// window. `debounce_windows` pairs each sync directory's root with the
    /// window its events should settle on; an event under no listed root uses
    /// the debouncer's built-in fallback. Passing the windows in at
    /// construction (rather than registering them afterward) means every event
    /// is debounced correctly from the first one on.
    ///
    /// `pending_events` is kept equal to the number of events inside the
    /// debounce window (updated under the debouncer's lock after every push and
    /// extraction) so the activity API can see changes not yet delivered.
    pub async fn new(
        debounce_windows: Vec<(PathBuf, Duration)>,
        pending_events: Arc<AtomicU64>,
    ) -> Result<
        (
            WatchDispatcher,
            tokio::sync::mpsc::UnboundedReceiver<DebouncedEventKind>,
        ),
        notify::Error,
    > {
        let debouncer = Arc::new(Mutex::new(Debouncer::new(debounce_windows)));

        let stop = Arc::new(AtomicBool::new(false));
        let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel();

        let task = {
            let debouncer = debouncer.clone();
            let stop = stop.clone();
            let pending_events = pending_events.clone();

            tokio::spawn(async move {
                loop {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }

                    tokio::time::sleep(Duration::from_millis(250)).await;

                    let mut debouncer = debouncer.lock().unwrap();
                    // Send before lowering the pending count, so an event is
                    // always visible in one of the two places.
                    for event in debouncer.extract_finalized() {
                        let _ = event_sender.send(event);
                    }
                    pending_events.store(debouncer.pending() as u64, Ordering::Relaxed);
                }
            })
        };

        let watcher = RecommendedWatcher::new(
            move |result: Result<Event, notify::Error>| {
                if let Ok(event) = result {
                    let mut debouncer = debouncer.lock().unwrap();
                    debouncer.push_raw(event);
                    pending_events.store(debouncer.pending() as u64, Ordering::Relaxed);
                }
            },
            notify::Config::default(),
        )?;

        Ok((
            WatchDispatcher {
                stop,
                watcher,
                watched: HashSet::new(),
                _task: task,
            },
            event_receiver,
        ))
    }

    /// Register a single directory non-recursively, recording it in the watched
    /// set. Idempotent: re-watching an already-watched directory is a no-op.
    /// A failure to install the descriptor is logged and swallowed so one bad
    /// directory does not abort a whole-tree walk — the caller keeps syncing
    /// what it can.
    fn watch_one(&mut self, directory: &Path) {
        if self.watched.contains(directory) {
            return;
        }
        match self.watcher.watch(directory, RecursiveMode::NonRecursive) {
            Ok(()) => {
                self.watched.insert(directory.to_path_buf());
            }
            Err(error) => {
                log::error!(
                    "Failed to watch directory {}: {error}",
                    directory.to_string_lossy()
                );
            }
        }
    }

    /// Register `root` and every directory beneath it that `should_prune` does
    /// not reject, non-recursively. `should_prune` is called with each
    /// directory's absolute path; when it returns true that directory and its
    /// entire subtree are skipped (no descriptor installed, no descent), which
    /// is how a `.gitignore`d subtree costs zero inotify watches.
    ///
    /// Returns the directories newly watched by this call, so a caller
    /// re-admitting a subtree can rescan exactly those for files that appeared
    /// while unwatched.
    pub fn watch_tree(
        &mut self,
        root: &Path,
        should_prune: &dyn Fn(&Path) -> bool,
    ) -> Vec<PathBuf> {
        let mut newly_watched = Vec::new();
        let walker = WalkDir::new(root).into_iter().filter_entry(|entry| {
            // Only directories are watched; prune an ignored directory
            // (and its whole subtree, since `filter_entry` stops descent).
            entry.file_type().is_dir() && !should_prune(entry.path())
        });

        for entry in walker.filter_map(|entry| entry.ok()) {
            let directory = entry.path();
            if !self.watched.contains(directory) {
                self.watch_one(directory);
                if self.watched.contains(directory) {
                    newly_watched.push(directory.to_path_buf());
                }
            }
        }

        newly_watched
    }

    /// Drop `root` and every watched descendant from the `notify` watcher and
    /// the watched set. Used when a directory is removed, moved out, or becomes
    /// newly `.gitignore`d. `notify` on Linux auto-drops descriptors for
    /// deleted inodes, so an `unwatch` of an already-gone path may error; that
    /// is expected and logged at debug, and the set is reconciled regardless so
    /// no stale entry leaks.
    pub fn unwatch_tree(&mut self, root: &Path) {
        let to_remove: Vec<PathBuf> = self
            .watched
            .iter()
            .filter(|watched| watched.as_path() == root || watched.starts_with(root))
            .cloned()
            .collect();

        for directory in to_remove {
            if let Err(error) = self.watcher.unwatch(&directory) {
                log::debug!(
                    "Unwatch of {} failed (likely already gone): {error}",
                    directory.to_string_lossy()
                );
            }
            self.watched.remove(&directory);
        }
    }

    /// Whether `directory` is currently watched. Exposed for tests and for
    /// callers deciding whether a directory event needs a fresh `watch_tree`.
    #[cfg(test)]
    pub fn is_watching(&self, directory: &Path) -> bool {
        self.watched.contains(directory)
    }
}

impl Drop for WatchDispatcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!(
            "tagsy-watch-test-{}-{}-{}",
            label,
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&base).unwrap();
        base
    }

    /// `watch_tree` registers every directory in the tree except the subtrees
    /// the prune predicate rejects, and reports exactly the directories it
    /// newly watched. This is the core of the descriptor saving: an ignored
    /// `target/` installs no watch at all.
    #[tokio::test]
    async fn watch_tree_prunes_rejected_subtrees() {
        let root = temp_dir("prune");
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(root.join("target/debug/deps")).unwrap();
        std::fs::create_dir_all(root.join("nested/keep")).unwrap();

        let (mut dispatcher, _events) = WatchDispatcher::new(vec![], Default::default())
            .await
            .unwrap();

        // Prune anything whose path component is `target`.
        let prune = |candidate: &Path| candidate.components().any(|c| c.as_os_str() == "target");
        let newly = dispatcher.watch_tree(&root, &prune);

        // Watched: root, src, nested, nested/keep.
        assert!(dispatcher.is_watching(&root));
        assert!(dispatcher.is_watching(&root.join("src")));
        assert!(dispatcher.is_watching(&root.join("nested")));
        assert!(dispatcher.is_watching(&root.join("nested/keep")));

        // Pruned: target and everything beneath it — zero descriptors.
        assert!(!dispatcher.is_watching(&root.join("target")));
        assert!(!dispatcher.is_watching(&root.join("target/debug")));
        assert!(!dispatcher.is_watching(&root.join("target/debug/deps")));

        // The newly-watched report matches the watched set.
        assert_eq!(newly.len(), 4);
    }

    /// `watch_tree` is idempotent: re-registering an already-watched tree adds
    /// nothing and reports no new directories, so the `.gitignore` re-admission
    /// path can call it freely.
    #[tokio::test]
    async fn watch_tree_is_idempotent() {
        let root = temp_dir("idempotent");
        std::fs::create_dir_all(root.join("a/b")).unwrap();

        let (mut dispatcher, _events) = WatchDispatcher::new(vec![], Default::default())
            .await
            .unwrap();
        let no_prune = |_: &Path| false;

        let first = dispatcher.watch_tree(&root, &no_prune);
        assert_eq!(first.len(), 3); // root, a, a/b

        let second = dispatcher.watch_tree(&root, &no_prune);
        assert!(second.is_empty());
    }

    /// A subtree that was previously pruned and then re-admitted (mirroring a
    /// `.gitignore` un-ignore) is reported as newly-watched on the second call,
    /// so the caller knows exactly which directories to rescan for files.
    #[tokio::test]
    async fn watch_tree_reports_newly_admitted_subtree() {
        let root = temp_dir("readmit");
        std::fs::create_dir_all(root.join("keep")).unwrap();
        std::fs::create_dir_all(root.join("build/out")).unwrap();

        let (mut dispatcher, _events) = WatchDispatcher::new(vec![], Default::default())
            .await
            .unwrap();

        // First pass prunes build/.
        let prune_build =
            |candidate: &Path| candidate.components().any(|c| c.as_os_str() == "build");
        dispatcher.watch_tree(&root, &prune_build);
        assert!(!dispatcher.is_watching(&root.join("build")));

        // Second pass no longer prunes it: build/ and build/out are newly
        // watched, and only those.
        let no_prune = |_: &Path| false;
        let newly = dispatcher.watch_tree(&root, &no_prune);

        assert!(dispatcher.is_watching(&root.join("build")));
        assert!(dispatcher.is_watching(&root.join("build/out")));
        let newly: std::collections::HashSet<_> = newly.into_iter().collect();
        assert_eq!(
            newly,
            [root.join("build"), root.join("build/out")]
                .into_iter()
                .collect()
        );
    }

    /// `unwatch_tree` drops the root and every watched descendant from the set,
    /// while leaving sibling subtrees untouched.
    #[tokio::test]
    async fn unwatch_tree_drops_subtree_only() {
        let root = temp_dir("unwatch");
        std::fs::create_dir_all(root.join("a/b/c")).unwrap();
        std::fs::create_dir_all(root.join("other")).unwrap();

        let (mut dispatcher, _events) = WatchDispatcher::new(vec![], Default::default())
            .await
            .unwrap();
        dispatcher.watch_tree(&root, &|_: &Path| false);

        dispatcher.unwatch_tree(&root.join("a"));

        // The `a` subtree is gone.
        assert!(!dispatcher.is_watching(&root.join("a")));
        assert!(!dispatcher.is_watching(&root.join("a/b")));
        assert!(!dispatcher.is_watching(&root.join("a/b/c")));
        // Siblings and root remain.
        assert!(dispatcher.is_watching(&root));
        assert!(dispatcher.is_watching(&root.join("other")));
    }
}
