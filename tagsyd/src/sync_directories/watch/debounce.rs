//! The debounce state machine that turns raw `notify` events into settled
//! [`DebouncedEventKind`]s.
//!
//! A raw event is first *translated* ([`translate`]) from `notify`'s vocabulary
//! into zero or one `DebouncedEventKind`, then *coalesced*
//! ([`Debouncer::push`]) against the events already queued — collapsing the
//! noisy multi-event sequences the filesystem and editors emit (a Vim save, a
//! rename pair, a create-then-write) into the single logical change they
//! represent. Events age out of the queue after a quiet period
//! ([`Debouncer::extract_finalized`]).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
use notify::{Event, EventKind};

/// Fallback debounce window for an event whose path matches no registered sync
/// root — every real event does match one (its path lives under the root that
/// produced it), so this only covers the theoretical gap between a `notify`
/// event arriving and its root being registered. Kept at the historical 500 ms
/// so an unmatched event behaves exactly as before per-directory windows
/// existed.
const DEFAULT_DEBOUNCE_WINDOW: Duration = Duration::from_millis(500);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DebouncedEventKind {
    /// Creating of a *file*.
    Create { file_name: PathBuf },
    /// Move of a a *file or directory*.
    Move {
        from: Option<PathBuf>,
        to: Option<PathBuf>,
    },
    /// Modification of a *file*.
    Modify { file_name: PathBuf },
    /// Removal of a *file*.
    Remove { file_name: PathBuf },
    /// Creation of a *directory*. Distinct from [`Create`](Self::Create)
    /// because the non-recursive watcher must react to it structurally — a new
    /// directory needs a fresh `notify` watch (and a walk of its contents to
    /// close the create-then-populate race), not an ingest. It carries the
    /// directory path and is deliberately inert to every file-coalescing merge
    /// rule.
    DirCreate { path: PathBuf },
    /// Removal of a *directory*. The counterpart to
    /// [`DirCreate`](Self::DirCreate): the watcher must drop the subtree's
    /// descriptors. Individual file removals beneath it arrive as their own
    /// [`Remove`](Self::Remove) events.
    DirRemove { path: PathBuf },
}

impl DebouncedEventKind {
    pub fn is_create(&self, path: impl AsRef<Path>) -> bool {
        if let Self::Create { file_name } = self
            && file_name == path.as_ref()
        {
            true
        } else {
            false
        }
    }

    pub fn is_modify(&self, path: impl AsRef<Path>) -> bool {
        if let Self::Modify { file_name } = self
            && file_name == path.as_ref()
        {
            true
        } else {
            false
        }
    }

    pub fn is_move_from_to(&self, path: impl AsRef<Path>) -> bool {
        if let Self::Move { from, to } = self
            && from.is_some()
            && to.as_ref().is_some_and(|to| to == path.as_ref())
        {
            true
        } else {
            false
        }
    }

    pub fn is_move_from(&self, path: impl AsRef<Path>) -> bool {
        if let Self::Move { from, to } = self
            && from.as_ref().is_some_and(|from| from == path.as_ref())
            && to.is_none()
        {
            true
        } else {
            false
        }
    }

    pub fn is_move_to(&self, path: impl AsRef<Path>) -> bool {
        if let Self::Move { from, to } = self
            && from.is_none()
            && to.as_ref().is_some_and(|to| to == path.as_ref())
        {
            true
        } else {
            false
        }
    }

    /// The path this event concerns, used to pick which sync directory's
    /// debounce window applies. A `Move` reports two paths (`from`/`to`); the
    /// destination is preferred because a settled move is ingested against
    /// where the file now lives, falling back to the source when only a `from`
    /// is present (a move *out* of a watched tree).
    fn primary_path(&self) -> Option<&Path> {
        match self {
            Self::Create { file_name }
            | Self::Modify { file_name }
            | Self::Remove { file_name } => Some(file_name),
            Self::DirCreate { path } | Self::DirRemove { path } => Some(path),
            Self::Move { from, to } => to.as_deref().or(from.as_deref()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DebouncingEvent {
    pub kind: DebouncedEventKind,
    pub timestamp: Instant,
}

/// Translate one raw `notify` [`Event`] into zero or one
/// [`DebouncedEventKind`].
///
/// This is the pure `notify`-vocabulary → our-vocabulary mapping, with no
/// reference to the queue: a file create/remove becomes `Create`/`Remove` and a
/// *directory* create/remove becomes `DirCreate`/`DirRemove` (the non-recursive
/// watcher must react to directory structure by adding/dropping watches), data
/// modifies become `Modify`, the three rename modes (`To` / `From` / `Both`)
/// become the corresponding `Move`, and metadata / access / other events are
/// dropped.
///
/// Assumes events are not bundled (one path per event, except a `Both` rename
/// which carries two); this matches the recommended watcher's behaviour.
pub(super) fn translate(mut event: Event) -> Option<DebouncedEventKind> {
    match event.kind {
        EventKind::Create(create_kind) => {
            assert_eq!(event.paths.len(), 1, "Wrong number of paths");
            let path = event.paths.remove(0);
            match create_kind {
                CreateKind::File => Some(DebouncedEventKind::Create { file_name: path }),
                // A new directory needs a fresh non-recursive watch; surface it
                // rather than dropping it (the recursive watcher used to make
                // this implicit).
                CreateKind::Folder => Some(DebouncedEventKind::DirCreate { path }),
                // `Any`/`Other`: kind unknown. Dropping (as before) is the safe
                // choice — guessing "file" would try to ingest a directory as a
                // file, and guessing "directory" would install a stray watch.
                // On Linux `notify` always reports File/Folder, so this is only
                // the theoretical fallback.
                CreateKind::Any | CreateKind::Other => None,
            }
        }
        EventKind::Modify(modify_kind) => match modify_kind {
            ModifyKind::Data(_) => {
                assert_eq!(event.paths.len(), 1, "Wrong number of paths");

                let file_name = event.paths.remove(0);
                Some(DebouncedEventKind::Modify { file_name })
            }
            ModifyKind::Name(rename_mode) => match rename_mode {
                RenameMode::To => {
                    assert_eq!(event.paths.len(), 1, "Wrong number of paths");

                    let to = event.paths.remove(0);
                    Some(DebouncedEventKind::Move {
                        from: None,
                        to: Some(to),
                    })
                }
                RenameMode::From => {
                    assert_eq!(event.paths.len(), 1, "Wrong number of paths");

                    let from = event.paths.remove(0);
                    Some(DebouncedEventKind::Move {
                        from: Some(from),
                        to: None,
                    })
                }
                RenameMode::Both => {
                    assert_eq!(event.paths.len(), 2, "Wrong number of paths");

                    let from = event.paths.remove(0);
                    let to = event.paths.remove(0);

                    Some(DebouncedEventKind::Move {
                        from: Some(from),
                        to: Some(to),
                    })
                }
                RenameMode::Any | RenameMode::Other => None,
            },
            // For now we also ignore metadata changes.
            ModifyKind::Any | ModifyKind::Metadata(_) | ModifyKind::Other => None,
        },
        EventKind::Remove(remove_kind) => {
            assert_eq!(event.paths.len(), 1, "Wrong number of paths");
            let path = event.paths.remove(0);
            match remove_kind {
                RemoveKind::File => Some(DebouncedEventKind::Remove { file_name: path }),
                // A removed directory's descriptors must be dropped from the
                // watch set; surface it rather than dropping it.
                RemoveKind::Folder => Some(DebouncedEventKind::DirRemove { path }),
                // Unknown kind: drop, as before (see the Create arm).
                RemoveKind::Any | RemoveKind::Other => None,
            }
        }
        // Not used, skip adding it.
        EventKind::Any | EventKind::Access(_) | EventKind::Other => None,
    }
}

/// A sync root and the debounce window its events should use. One is built per
/// configured sync directory at [`Debouncer::new`]; `window_for_in` finds the
/// longest matching root for a given event path so a nested sync directory (if
/// one ever existed) wins over an ancestor.
#[derive(Clone, Debug)]
struct WindowRegistration {
    root: PathBuf,
    window: Duration,
}

#[derive(Default)]
pub struct Debouncer {
    queued: Vec<DebouncingEvent>,
    /// Per-sync-root debounce windows. An event's settle time is the window of
    /// the deepest registered root that contains its path; an event under no
    /// registered root falls back to [`DEFAULT_DEBOUNCE_WINDOW`].
    windows: Vec<WindowRegistration>,
}

impl Debouncer {
    /// Construct a debouncer that settles each sync root's events on its own
    /// window. `windows` pairs each configured sync directory's root with its
    /// debounce window; an event under no listed root uses
    /// [`DEFAULT_DEBOUNCE_WINDOW`].
    pub fn new(windows: Vec<(PathBuf, Duration)>) -> Self {
        Self {
            queued: Vec::new(),
            windows: windows
                .into_iter()
                .map(|(root, window)| WindowRegistration { root, window })
                .collect(),
        }
    }

    /// The debounce window that applies to an event, chosen by the deepest
    /// registered sync root that contains the event's path. Falls back to
    /// [`DEFAULT_DEBOUNCE_WINDOW`] when the path matches no root (or carries no
    /// path).
    ///
    /// Takes the registration slice rather than `&self` so it can be called
    /// from inside a `retain` closure that already borrows `self.queued`
    /// mutably.
    fn window_for_in(windows: &[WindowRegistration], event: &DebouncedEventKind) -> Duration {
        let Some(path) = event.primary_path() else {
            return DEFAULT_DEBOUNCE_WINDOW;
        };

        windows
            .iter()
            .filter(|registration| path.starts_with(&registration.root))
            // The deepest (longest) matching root wins, so a nested sync
            // directory overrides an ancestor.
            .max_by_key(|registration| registration.root.as_os_str().len())
            .map(|registration| registration.window)
            .unwrap_or(DEFAULT_DEBOUNCE_WINDOW)
    }

    /// Translate a raw `notify` event and coalesce it into the queue.
    pub fn push_raw(&mut self, event: Event) {
        if let Some(kind) = translate(event) {
            self.push(kind);
        }
    }

    /// Coalesce one already-translated [`DebouncedEventKind`] into the queue,
    /// applying the seven merge rules. If none applies, the event is queued.
    ///
    /// The rules exist to collapse the noisy multi-event sequences the
    /// filesystem and editors emit into the single logical change they mean.
    /// Each is commented with the real-world sequence that produces it.
    fn push(&mut self, new_event: DebouncedEventKind) {
        let timestamp = Instant::now();

        // Merge modify + delete events.
        // This results in the modify events being removed.
        if let DebouncedEventKind::Remove { file_name, .. } = &new_event {
            for index in (0..self.queued.len()).rev() {
                let event = &self.queued[index];

                // If we find the creation event we stop.
                if event.kind.is_create(file_name) {
                    break;
                }

                if event.kind.is_modify(file_name) {
                    self.queued.remove(index);
                }
            }
        }

        // Merge create + delete events.
        // This results in them canceling out.
        if let DebouncedEventKind::Remove { file_name, .. } = &new_event
            && let Some(index_from_back) = self
                .queued
                .iter()
                .rev()
                .position(|event| event.kind.is_create(file_name))
        {
            let index = self.queued.len() - index_from_back - 1;
            self.queued.remove(index);
            // Skip insertion of the remove event.
            return;
        }

        // Try to find the pattern that Vim/Neovim create when editing files.
        // The editor will rename the original file with a suffix, create a new file
        // with the new content, and delete the original file. For us, this
        // should just be a `Modify`.
        //
        // TODO: Maybe this matching here is too eager and might cause issues?
        if let DebouncedEventKind::Remove { file_name, .. } = &new_event
            && let Some(rename_index_from_back) = self
                .queued
                .iter()
                .rev()
                .position(|event| event.kind.is_move_from_to(file_name))
        {
            let rename_index = self.queued.len() - rename_index_from_back - 1;

            let DebouncedEventKind::Move { from, .. } = self.queued[rename_index].kind.clone()
            else {
                unreachable!();
            };

            if let Some(from) = from
                && let Some(create_index_from_back) = self
                    .queued
                    .iter()
                    .rev()
                    .position(|event| event.kind.is_create(&from))
            {
                let create_index = self.queued.len() - create_index_from_back - 1;

                // Sanity check: can likely be removed in the future.
                assert!(
                    rename_index < create_index,
                    "Wound Vim/Neovim edit pattern but the order is wrong"
                );

                self.queued[create_index].kind = DebouncedEventKind::Modify { file_name: from };
                self.queued.remove(rename_index);

                // Skip insertion of the remove event.
                return;
            }
        }

        // Merge multiple moves.
        // This happens when renaming a file withing the synced directory.
        //
        // NOTE: This code relies on the fact that the `Move` with `from` and `to` is
        // emitted after the single `from` and `to` events. It also assumes
        // that there are no events in-between and that `from` is sent before `to`.
        if let DebouncedEventKind::Move { from, to } = &new_event
            && let Some(from) = from
            && let Some(to) = to
        {
            let to_index = self.queued.len().saturating_sub(1);
            let from_index = to_index.saturating_sub(1);

            if let Some(potential_from) = self.queued.get(from_index)
                && let Some(potential_to) = self.queued.get(to_index)
                && potential_from.kind.is_move_from(from)
                && potential_to.kind.is_move_to(to)
            {
                self.queued.remove(to_index);
                self.queued.remove(from_index);
            }
        }

        // Merge create + rename.
        // This happens when creating a symlink for example.
        if let DebouncedEventKind::Move { from, to } = &new_event
            && let Some(from) = from
            && let Some(to) = to
        {
            for event in self.queued.iter_mut().rev() {
                if let DebouncedEventKind::Create { file_name } = &mut event.kind
                    && file_name == from
                {
                    *file_name = to.clone();
                    // Skip insertion of the rename event.
                    return;
                }
            }
        }

        // Merge create + modify.
        // This happens when piping into a non-existen file for example.
        if let DebouncedEventKind::Modify { file_name } = &new_event {
            for event in self.queued.iter_mut().rev() {
                if let DebouncedEventKind::Create {
                    file_name: create_file_name,
                } = &event.kind
                    && create_file_name == file_name
                {
                    // Skip insertion of the modify event.
                    return;
                }
            }
        }

        // Merge multiple modifies.
        // This will happen all the time due to the fact that both content and metadata
        // modifications create this event.
        if let DebouncedEventKind::Modify { file_name } = &new_event {
            for event in self.queued.iter_mut().rev() {
                if let DebouncedEventKind::Modify {
                    file_name: modify_file_name,
                } = &event.kind
                    && modify_file_name == file_name
                {
                    // Skip insertion of the modify event.
                    return;
                }
            }
        }

        self.queued.push(DebouncingEvent {
            kind: new_event,
            timestamp,
        });
    }

    /// Events still inside their debounce window.
    pub fn pending(&self) -> usize {
        self.queued.len()
    }

    pub fn extract_finalized(&mut self) -> Vec<DebouncedEventKind> {
        let mut debounced_events = Vec::new();

        // Borrow `windows` separately from `queued` so the `retain` closure can
        // consult the per-root windows while mutating the queue.
        let windows = &self.windows;
        self.queued.retain(|event| {
            if event.timestamp.elapsed() > Self::window_for_in(windows, &event.kind) {
                // TODO: Optimize to not clone.
                debounced_events.push(event.kind.clone());
                return false;
            }

            true
        });

        debounced_events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drain the queue's current `kind`s without waiting for the debounce
    /// window (the merge rules are what we're testing, not the timing).
    fn queued_kinds(debouncer: &Debouncer) -> Vec<DebouncedEventKind> {
        debouncer.queued.iter().map(|e| e.kind.clone()).collect()
    }

    fn path(name: &str) -> PathBuf {
        PathBuf::from(name)
    }

    fn create(name: &str) -> DebouncedEventKind {
        DebouncedEventKind::Create {
            file_name: path(name),
        }
    }
    fn modify(name: &str) -> DebouncedEventKind {
        DebouncedEventKind::Modify {
            file_name: path(name),
        }
    }
    fn remove(name: &str) -> DebouncedEventKind {
        DebouncedEventKind::Remove {
            file_name: path(name),
        }
    }
    fn move_from(name: &str) -> DebouncedEventKind {
        DebouncedEventKind::Move {
            from: Some(path(name)),
            to: None,
        }
    }
    fn move_to(name: &str) -> DebouncedEventKind {
        DebouncedEventKind::Move {
            from: None,
            to: Some(path(name)),
        }
    }
    fn move_both(from: &str, to: &str) -> DebouncedEventKind {
        DebouncedEventKind::Move {
            from: Some(path(from)),
            to: Some(path(to)),
        }
    }
    fn dir_create(name: &str) -> DebouncedEventKind {
        DebouncedEventKind::DirCreate { path: path(name) }
    }
    fn dir_remove(name: &str) -> DebouncedEventKind {
        DebouncedEventKind::DirRemove { path: path(name) }
    }

    use notify::event::{CreateKind, RemoveKind};
    use notify::{Event, EventKind};

    fn raw(kind: EventKind, paths: &[&str]) -> Event {
        Event {
            kind,
            paths: paths.iter().map(PathBuf::from).collect(),
            attrs: Default::default(),
        }
    }

    /// A directory create/remove now translates to `DirCreate`/`DirRemove`
    /// rather than being dropped — the non-recursive watcher must react to
    /// directory structure. File create/remove still translate as before.
    #[test]
    fn directory_create_and_remove_translate_to_dir_events() {
        assert_eq!(
            translate(raw(EventKind::Create(CreateKind::Folder), &["d"])),
            Some(dir_create("d"))
        );
        assert_eq!(
            translate(raw(EventKind::Remove(RemoveKind::Folder), &["d"])),
            Some(dir_remove("d"))
        );
        assert_eq!(
            translate(raw(EventKind::Create(CreateKind::File), &["f"])),
            Some(create("f"))
        );
        assert_eq!(
            translate(raw(EventKind::Remove(RemoveKind::File), &["f"])),
            Some(remove("f"))
        );
    }

    /// An unknown-kind create/remove is still dropped (guessing file-vs-dir is
    /// unsafe); on Linux `notify` always reports File/Folder so this is only
    /// the theoretical fallback.
    #[test]
    fn unknown_kind_create_and_remove_are_dropped() {
        assert_eq!(
            translate(raw(EventKind::Create(CreateKind::Any), &["x"])),
            None
        );
        assert_eq!(
            translate(raw(EventKind::Remove(RemoveKind::Any), &["x"])),
            None
        );
    }

    /// Directory events are inert to every file-coalescing merge rule: they are
    /// distinct variants none of the seven rules match, so they queue through
    /// untouched and never merge with, cancel, or rewrite a file event of the
    /// same path.
    #[test]
    fn directory_events_are_inert_to_merge_rules() {
        // A DirCreate + DirRemove of the same path do NOT cancel (that rule is
        // file-only): both survive.
        let debouncer = debouncer_of([dir_create("d"), dir_remove("d")]);
        assert_eq!(queued_kinds(&debouncer), vec![
            dir_create("d"),
            dir_remove("d")
        ]);

        // A DirCreate is untouched by the file rules operating on the same
        // path: the file Modify+Remove coalesce between themselves (rule 1
        // clears the Modify), but the DirCreate survives regardless — it never
        // absorbs the Modify nor is cleared by the Remove.
        let debouncer = debouncer_of([dir_create("d"), modify("d"), remove("d")]);
        assert_eq!(queued_kinds(&debouncer), vec![dir_create("d"), remove("d")]);
    }

    fn debouncer_of(events: impl IntoIterator<Item = DebouncedEventKind>) -> Debouncer {
        let mut debouncer = Debouncer::default();
        for event in events {
            debouncer.push(event);
        }
        debouncer
    }

    /// Rule 1 (modify + delete): a `Remove` clears queued `Modify`s of the same
    /// file that follow its creation — but a `Modify` for a *different* file is
    /// left alone.
    #[test]
    fn modify_then_delete_drops_the_modifies() {
        let debouncer = debouncer_of([modify("a"), modify("other"), remove("a")]);
        // The two `modify("a")` are gone; `modify("other")` and the `remove("a")`
        // remain.
        assert_eq!(queued_kinds(&debouncer), vec![modify("other"), remove("a")]);
    }

    /// Rule 1 boundary: the scan stops at the file's own `Create`, so a
    /// `Modify` recorded *before* the create is not touched (it belongs to
    /// a prior life of that path).
    #[test]
    fn delete_stops_clearing_modifies_at_create() {
        let debouncer = debouncer_of([modify("a"), create("a"), modify("a"), remove("a")]);
        // create("a") + remove("a") cancel (rule 2), the post-create modify is
        // cleared (rule 1), and the pre-create modify survives.
        assert_eq!(queued_kinds(&debouncer), vec![modify("a")]);
    }

    /// Rule 2 (create + delete): a create followed by a delete of the same file
    /// cancel out entirely.
    #[test]
    fn create_then_delete_cancels_out() {
        let debouncer = debouncer_of([create("a"), remove("a")]);
        assert!(queued_kinds(&debouncer).is_empty());
    }

    /// Rule 3 (Vim/Neovim edit): the editor renames the original file aside,
    /// writes a fresh file at the original name, then deletes the aside copy —
    /// `move(a → a~)`, `create(a)`, `remove(a~)`. The whole dance collapses to
    /// a single `Modify(a)`.
    ///
    /// Ordering matters: the `move` must arrive *before* the `create`,
    /// otherwise rule 5 (create + rename) would rewrite the create and rule
    /// 3 would never see its `create(from)`.
    #[test]
    fn vim_edit_pattern_collapses_to_modify() {
        let debouncer = debouncer_of([move_both("a", "a~"), create("a"), remove("a~")]);
        assert_eq!(queued_kinds(&debouncer), vec![modify("a")]);
    }

    /// Rule 4 (multiple moves): a single `from` event, a single `to` event,
    /// then the combined `from→to` — the two singles are removed, leaving
    /// just the combined move.
    #[test]
    fn split_move_pair_is_absorbed_by_combined_move() {
        let debouncer = debouncer_of([move_from("a"), move_to("b"), move_both("a", "b")]);
        assert_eq!(queued_kinds(&debouncer), vec![move_both("a", "b")]);
    }

    /// Rule 5 (create + rename): a create followed by a rename of that new file
    /// rewrites the create's name to the rename target (e.g. a symlink
    /// appearing then being renamed) — no separate move is queued.
    #[test]
    fn create_then_rename_rewrites_the_create() {
        let debouncer = debouncer_of([create("a"), move_both("a", "b")]);
        assert_eq!(queued_kinds(&debouncer), vec![create("b")]);
    }

    /// Rule 6 (create + modify): a modify of a just-created file is dropped —
    /// the create already implies the content.
    #[test]
    fn modify_after_create_is_dropped() {
        let debouncer = debouncer_of([create("a"), modify("a")]);
        assert_eq!(queued_kinds(&debouncer), vec![create("a")]);
    }

    /// Rule 7 (multiple modifies): repeated modifies of the same file collapse
    /// to one (metadata + data both fire this event).
    #[test]
    fn repeated_modifies_collapse_to_one() {
        let debouncer = debouncer_of([modify("a"), modify("a"), modify("a")]);
        assert_eq!(queued_kinds(&debouncer), vec![modify("a")]);
    }

    /// No rule applies: unrelated events for different files are all kept, in
    /// order.
    #[test]
    fn unrelated_events_are_all_kept() {
        let debouncer = debouncer_of([create("a"), modify("b"), remove("c")]);
        assert_eq!(queued_kinds(&debouncer), vec![
            create("a"),
            modify("b"),
            remove("c")
        ]);
    }

    fn window_registrations(
        entries: impl IntoIterator<Item = (&'static str, u64)>,
    ) -> Vec<WindowRegistration> {
        entries
            .into_iter()
            .map(|(root, millis)| WindowRegistration {
                root: PathBuf::from(root),
                window: Duration::from_millis(millis),
            })
            .collect()
    }

    /// An event's window is the one registered for the sync root that contains
    /// its path; events under different roots get different windows.
    #[test]
    fn window_is_selected_by_containing_root() {
        let windows = window_registrations([("/notes", 100), ("/media", 3000)]);

        assert_eq!(
            Debouncer::window_for_in(&windows, &modify("/notes/todo.md")),
            Duration::from_millis(100)
        );
        assert_eq!(
            Debouncer::window_for_in(&windows, &modify("/media/clip.mp4")),
            Duration::from_millis(3000)
        );
    }

    /// A path under no registered root falls back to the default window, so an
    /// event that arrives before its root is known still settles.
    #[test]
    fn unmatched_path_uses_default_window() {
        let windows = window_registrations([("/notes", 100)]);
        assert_eq!(
            Debouncer::window_for_in(&windows, &modify("/elsewhere/file")),
            DEFAULT_DEBOUNCE_WINDOW
        );
    }

    /// When a sync root nests inside another, the deepest (longest) matching
    /// root wins, so the inner directory's window governs its own files rather
    /// than the ancestor's.
    #[test]
    fn deepest_matching_root_wins() {
        let windows = window_registrations([("/root", 100), ("/root/inner", 3000)]);
        assert_eq!(
            Debouncer::window_for_in(&windows, &modify("/root/inner/file")),
            Duration::from_millis(3000)
        );
        assert_eq!(
            Debouncer::window_for_in(&windows, &modify("/root/other")),
            Duration::from_millis(100)
        );
    }

    /// A `Move` is classified by its destination when present (that is where a
    /// settled move is ingested), falling back to the source for a move out.
    #[test]
    fn move_uses_destination_root_then_source() {
        let windows = window_registrations([("/slow", 3000), ("/fast", 50)]);

        // Move into /fast from /slow: the destination's window applies.
        assert_eq!(
            Debouncer::window_for_in(&windows, &move_both("/slow/a", "/fast/a")),
            Duration::from_millis(50)
        );
        // Move out with only a source: fall back to the source's window.
        assert_eq!(
            Debouncer::window_for_in(&windows, &move_from("/slow/a")),
            Duration::from_millis(3000)
        );
    }
}
