//! Creation-time tag rules: the serde-facing [`TagRule`] and its compiled,
//! regex-holding form [`CompiledTagRules`].
//!
//! Split out of the config module proper because [`CompiledTagRules`] holds a
//! live [`regex::Regex`] — neither `Deserialize` nor cheap to clone — while the
//! [`Configuration`](super::Configuration) it is built from is both.

use regex::Regex;
use serde::{Deserialize, Serialize};
use tagsy_core::{LogicalPath, TagId};

/// A rule assigning tags to a newly-created file whose logical path matches a
/// regular expression.
///
/// # When rules run
///
/// Rules are evaluated **exactly once per file, at the moment this device
/// creates it** — a client upload
/// ([`crate::frontend::api::ApiService::upload_file`]) or a file appearing in a
/// local sync directory. They are deliberately *not* re-run when a file is
/// later moved ([`crate::frontend::api::ApiService::move_file`]).
///
/// That asymmetry is the whole design, so it is worth stating why. If a rule
/// re-ran on every move, a rename would have to decide what to do with the
/// tags the *previous* path had granted: leaving them makes tags accumulate
/// monotonically as garbage, while removing them silently destroys tags the
/// user applied by hand (nothing records which tag came from a rule and which
/// from a person). Both answers are wrong, and the choice between them is not
/// separable from user intent. Running only at creation sidesteps the question
/// entirely: a rule is a *default*, applied when a file has no history to
/// contradict it, and everything afterwards belongs to the user.
///
/// The consequence is that editing this list does not retroactively affect
/// existing files. That is intentional and recoverable: `tagsy retag`
/// re-applies the current rules to the existing catalog on demand (additively;
/// see [`crate::frontend::api::ApiService::retag`]).
///
/// Rules run only on the device that *creates* the file. The resulting tags
/// propagate to peers as ordinary `FileTagged` changes, so a peer must not
/// re-apply its own rules to an inbound file — otherwise two devices with
/// different rule sets would fight over the same file forever.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagRule {
    /// A regular expression ([`regex`] syntax) matched against the file's
    /// **full logical path** (e.g. `photos/holiday/cat.jpg`), not just its
    /// basename. Matching the full path is what lets a rule key on location
    /// (`^photos/`) as well as on type (`\.md$`); a basename-only rule is
    /// still expressible, since `[^/]*` cannot cross a separator.
    ///
    /// The match is a **search, not a full-string match**: the pattern may
    /// match anywhere in the path unless anchored with `^` / `$`. So `\.md$`
    /// means "ends in `.md`" while a bare `md` would also match
    /// `admin/notes.txt`.
    ///
    /// A pattern that fails to compile disables *that rule only*; the daemon
    /// starts normally and the failure is reported by `tagsy retag --check`.
    /// See [`CompiledTagRules`] for why this is not a fatal error.
    pub pattern: String,
    /// Tags applied to every matching file. Ids, not names: a tag can be
    /// renamed at any time ([`crate::frontend::api::ApiService::rename_tag`])
    /// and a rule keyed by name would silently stop matching afterwards.
    ///
    /// The ids are **not** validated at startup.
    /// [`Configuration::tags`](super::Configuration::tags) is a floor, not the
    /// full set of tags — tags created through the UI or synced from a peer are
    /// equally real, and a tag this rule names may not exist *yet*. `tagsy
    /// retag --check` reports ids that resolve against neither the declarations
    /// nor the live database, which is the point at which the answer is
    /// actually meaningful.
    pub tags: Vec<TagId>,
}

/// A [`TagRule`] whose pattern failed to compile, retained so the failure can
/// be reported on demand rather than only observed in the startup log.
#[derive(Debug, thiserror::Error)]
#[error("tag rule {index} has an invalid pattern {pattern:?}: {source}")]
pub struct TagRuleError {
    /// Index of the offending rule in
    /// [`Configuration::tag_rules`](super::Configuration::tag_rules), so the
    /// operator can find it in a list of visually similar patterns.
    pub index: usize,
    pub pattern: String,
    #[source]
    pub source: regex::Error,
}

/// The compiled form of
/// [`Configuration::tag_rules`](super::Configuration::tag_rules), built once at
/// startup.
///
/// Separate from [`Configuration`](super::Configuration) because a compiled
/// [`Regex`] is neither `Deserialize` nor cheap to clone, while `Configuration`
/// is both cloned and round-tripped through serde.
///
/// # Why a bad rule is not a startup error
///
/// An invalid pattern disables that one rule and leaves the daemon running.
/// The tempting alternative — refuse to start — is wrong on two counts.
/// Availability: file synchronization is the daemon's job, and a typo in an
/// auxiliary tagging convenience is not a reason to stop doing it. On Android
/// the configuration is a build-time asset baked into the APK, so a fatal
/// parse error there is unfixable without a rebuild.
///
/// The usual objection to skipping is that the failure becomes silent, and
/// files quietly go untagged forever. Two things answer it: the errors are
/// *kept* (see [`Self::errors`]) and reported by `tagsy retag --check`
/// rather than being discarded at compile time, and `tagsy retag` can apply
/// the corrected rules to files created while the rule was broken. The damage
/// is therefore both visible and undoable, which is what made fail-closed
/// unnecessary.
#[derive(Debug, Default)]
pub struct CompiledTagRules {
    matchers: Vec<CompiledTagRule>,
    errors: Vec<TagRuleError>,
}

#[derive(Debug)]
struct CompiledTagRule {
    pattern: Regex,
    tags: Vec<TagId>,
}

impl CompiledTagRules {
    /// Compile every rule, collecting failures instead of returning them.
    /// Infallible by design — see the type-level docs.
    pub fn compile(rules: &[TagRule]) -> Self {
        let mut matchers = Vec::new();
        let mut errors = Vec::new();

        for (index, rule) in rules.iter().enumerate() {
            match Regex::new(&rule.pattern) {
                Ok(pattern) => matchers.push(CompiledTagRule {
                    pattern,
                    tags: rule.tags.clone(),
                }),
                Err(source) => errors.push(TagRuleError {
                    index,
                    pattern: rule.pattern.clone(),
                    source,
                }),
            }
        }

        Self { matchers, errors }
    }

    /// The tags every matching rule assigns to `path`, in rule declaration
    /// order and deduplicated.
    ///
    /// The union of *all* matches, not the first match — unlike
    /// [`EditorRule`](super::EditorRule), where the rules answer "which single
    /// program opens this?" and first-match-wins is forced. Tagging is
    /// additive: a `\.md$` rule and a `^notes/` rule describe independent facts
    /// about `notes/todo.md` and both should hold. First-match-wins would make
    /// the two rules' relative order silently load-bearing.
    pub fn tags_for(&self, path: &LogicalPath) -> Vec<TagId> {
        let mut tags: Vec<TagId> = Vec::new();

        for matcher in &self.matchers {
            if !matcher.pattern.is_match(path.as_str()) {
                continue;
            }
            for tag_id in &matcher.tags {
                if !tags.contains(tag_id) {
                    tags.push(*tag_id);
                }
            }
        }

        tags
    }

    /// Rules that failed to compile. Empty for a valid configuration.
    pub fn errors(&self) -> &[TagRuleError] {
        &self.errors
    }

    /// Every tag id named by a rule that compiled, deduplicated. Used by
    /// `tagsy retag --check` to report ids that match no known tag.
    pub fn referenced_tags(&self) -> Vec<TagId> {
        let mut tags: Vec<TagId> = Vec::new();
        for matcher in &self.matchers {
            for tag_id in &matcher.tags {
                if !tags.contains(tag_id) {
                    tags.push(*tag_id);
                }
            }
        }
        tags
    }

    /// True when no rule can ever match, so callers can skip work entirely.
    pub fn is_empty(&self) -> bool {
        self.matchers.is_empty()
    }

    /// How many rules compiled and are live. Excludes rules that failed to
    /// compile; those are counted by [`Self::errors`].
    pub fn len(&self) -> usize {
        self.matchers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(pattern: &str, tags: Vec<TagId>) -> TagRule {
        TagRule {
            pattern: pattern.to_owned(),
            tags,
        }
    }

    #[test]
    fn matching_rule_yields_its_tags() {
        let tag_id = TagId::new();
        let rules = CompiledTagRules::compile(&[rule(r"\.md$", vec![tag_id])]);

        assert_eq!(rules.tags_for(&LogicalPath::new("notes/todo.md")), vec![
            tag_id
        ]);
        assert!(rules.errors().is_empty());
    }

    #[test]
    fn non_matching_rule_yields_nothing() {
        let rules = CompiledTagRules::compile(&[rule(r"\.md$", vec![TagId::new()])]);

        assert!(
            rules
                .tags_for(&LogicalPath::new("notes/todo.txt"))
                .is_empty()
        );
        // Anchored at the end: a path merely *containing* `.md` must not match,
        // or `photo.mdx` and `a.md.bak` would be swept up too.
        assert!(rules.tags_for(&LogicalPath::new("a.md.bak")).is_empty());
    }

    /// Patterns match the full logical path, not just the basename — that is
    /// what makes a location-based rule (`^photos/`) expressible at all.
    #[test]
    fn pattern_matches_against_the_full_logical_path() {
        let tag_id = TagId::new();
        let rules = CompiledTagRules::compile(&[rule("^photos/", vec![tag_id])]);

        assert_eq!(
            rules.tags_for(&LogicalPath::new("photos/holiday/cat.jpg")),
            vec![tag_id]
        );
        // The same basename elsewhere must not match, which it would under
        // basename-only semantics.
        assert!(
            rules
                .tags_for(&LogicalPath::new("archive/photos/cat.jpg"))
                .is_empty()
        );
    }

    /// Every matching rule contributes; this is a union, not first-match-wins.
    #[test]
    fn all_matching_rules_contribute_their_tags() {
        let markdown = TagId::new();
        let notes = TagId::new();
        let rules = CompiledTagRules::compile(&[
            rule(r"\.md$", vec![markdown]),
            rule("^notes/", vec![notes]),
        ]);

        assert_eq!(rules.tags_for(&LogicalPath::new("notes/todo.md")), vec![
            markdown, notes
        ]);
    }

    /// Two rules naming the same tag yield it once: a duplicate would be
    /// announced twice to every peer.
    #[test]
    fn overlapping_rules_deduplicate_their_tags() {
        let tag_id = TagId::new();
        let rules = CompiledTagRules::compile(&[
            rule(r"\.md$", vec![tag_id]),
            rule("^notes/", vec![tag_id, tag_id]),
        ]);

        assert_eq!(rules.tags_for(&LogicalPath::new("notes/todo.md")), vec![
            tag_id
        ]);
    }

    /// An invalid pattern disables only itself: compilation still succeeds, the
    /// surviving rules still match, and the failure is retained for reporting.
    /// This is the behavior that lets the daemon start with a broken rule.
    #[test]
    fn invalid_pattern_disables_only_its_own_rule() {
        let good = TagId::new();
        let bad = TagId::new();
        let rules =
            CompiledTagRules::compile(&[rule("*.md", vec![bad]), rule(r"\.md$", vec![good])]);

        assert_eq!(
            rules.tags_for(&LogicalPath::new("notes/todo.md")),
            vec![good],
            "the valid rule must still apply"
        );

        assert_eq!(rules.errors().len(), 1);
        assert_eq!(rules.errors()[0].index, 0, "reports the offending index");
        assert_eq!(rules.errors()[0].pattern, "*.md");
        assert!(!rules.is_empty(), "one rule still compiled");
    }

    #[test]
    fn no_rules_is_empty() {
        let rules = CompiledTagRules::compile(&[]);
        assert!(rules.is_empty());
        assert!(rules.errors().is_empty());
        assert!(rules.referenced_tags().is_empty());
    }

    /// Only compiled rules contribute referenced tags — a tag named solely by a
    /// broken rule can never be applied, so reporting it as "unknown" would be
    /// a confusing second symptom of the same single fault.
    #[test]
    fn referenced_tags_covers_compiled_rules_only() {
        let good = TagId::new();
        let bad = TagId::new();
        let rules =
            CompiledTagRules::compile(&[rule("*.md", vec![bad]), rule(r"\.md$", vec![good])]);

        assert_eq!(rules.referenced_tags(), vec![good]);
    }
}
