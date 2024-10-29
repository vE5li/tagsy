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

/// How long an event sits in the queue with no superseding event before it is
/// considered settled and emitted.
const DEBOUNCE_WINDOW: Duration = Duration::from_millis(500);

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
/// reference to the queue: directory create/remove is dropped (directories are
/// tracked only through their files), data modifies become `Modify`, the three
/// rename modes (`To` / `From` / `Both`) become the corresponding `Move`, and
/// metadata / access / other events are dropped.
///
/// Assumes events are not bundled (one path per event, except a `Both` rename
/// which carries two); this matches the recommended watcher's behaviour.
pub(super) fn translate(mut event: Event) -> Option<DebouncedEventKind> {
    match event.kind {
        EventKind::Create(create_kind) => {
            // We don't track the creation/deletion of directories. Directories are only
            // tracked implicitely through the files that are contained in them.
            if create_kind == CreateKind::File {
                assert_eq!(event.paths.len(), 1, "Wrong number of paths");

                let file_name = event.paths.remove(0);
                Some(DebouncedEventKind::Create { file_name })
            } else {
                None
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
            // We don't track the creation/deletion of directories. Directories are only
            // tracked implicitely through the files that are contained in them.
            if remove_kind == RemoveKind::File {
                assert_eq!(event.paths.len(), 1, "Wrong number of paths");

                let file_name = event.paths.remove(0);
                Some(DebouncedEventKind::Remove { file_name })
            } else {
                None
            }
        }
        // Not used, skip adding it.
        EventKind::Any | EventKind::Access(_) | EventKind::Other => None,
    }
}

#[derive(Default)]
pub struct Debouncer {
    queued: Vec<DebouncingEvent>,
}

impl Debouncer {
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

    pub fn extract_finalized(&mut self) -> Vec<DebouncedEventKind> {
        let mut debounced_events = Vec::new();

        self.queued.retain(|event| {
            if event.timestamp.elapsed() > DEBOUNCE_WINDOW {
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
}
