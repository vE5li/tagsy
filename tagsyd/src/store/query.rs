//! Search: the query vocabulary (`QueryTerm` / `TextPattern`) and the two
//! functions that evaluate it.
//!
//! Pure composition over the other modules' primitives — this module issues no
//! SQL of its own.

use std::collections::BTreeSet;

use regex::{Regex, RegexBuilder};
use tagsy_api::{DeletedRule, SubtagRule};
use tagsy_core::{FileId, TagId};

use super::CatalogStore;
use super::types::DatabaseError;

/// A single clause of a file search (see [`CatalogStore::file_ids_for_query`]).
///
/// Terms are combined conjunctively: a file matches a query only if it matches
/// *every* term. Each term corresponds to one parsed chunk from the API layer
/// (`api::chunk::Chunk`) after its tag references have been resolved to
/// [`TagId`] sets.
///
/// A single tag token can match a *set* of tags (every tag whose name contains
/// the token substring or whose id starts with it), so the tag-bearing terms
/// carry a resolved [`TagId`] set rather than a single id; the set is expanded
/// from the user's syntax in a higher layer (the daemon's API), keeping this
/// database primitive free of name/prefix-resolution concerns. An empty set
/// matches no tag (so `HasTag([])` matches no file and `NotTag([])` excludes
/// nothing).
///
/// The `Any`/`NotAny` variants correspond to a *prefix-less* chunk: the user
/// wrote `foo`, and it should match anything that could reasonably relate to
/// `foo` — its logical path contains `foo`, *or* it carries/subtags a tag
/// resolved from `foo`, *or* its own id starts with `foo` (a pasted short id).
/// These variants carry the raw substring, the resolved tag set, and the
/// resolved file-id set so every side of the disjunction can be evaluated. The
/// file-id set is only meaningful on the file side (a tag is never matched by a
/// file's id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryTerm {
    /// Must carry *at least one* tag in this set.
    HasTag(Vec<TagId>),
    /// Must carry *none* of the tags in this set.
    NotTag(Vec<TagId>),
    /// The logical path of the file or name of the tag must match the pattern.
    NameMatches(TextPattern),
    /// The logical path of the file or name of the tag must *not* match the
    /// pattern.
    NotNameMatches(TextPattern),
    /// The logical path of the file must match the pattern.
    LogicalMatches(TextPattern),
    /// The logical path of the file must *not* match the pattern.
    NotLogicalMatches(TextPattern),
    /// The file's id must be in this set (a resolved id-prefix). Backs the
    /// `/i` prefix — files by id only. Purely a file-side filter: on the tag
    /// side it matches no tag (an id-prefix set built from `files_v2` says
    /// nothing about a tag), so `tag_ids_for_query` treats it like
    /// [`QueryTerm::LogicalMatches`]. An empty set matches no file.
    FileIdMatches(Vec<FileId>),
    /// The file's id must *not* be in this set. Negation of
    /// [`QueryTerm::FileIdMatches`]; an empty set excludes nothing.
    NotFileIdMatches(Vec<FileId>),
    /// Matches on *any* of text, tag, or file id (union across all axes). The
    /// [`TextPattern`] is the text side; the [`Vec<TagId>`] is the resolved tag
    /// set for the same token; the [`Vec<FileId>`] is the resolved file-id set
    /// (empty on the tag side, since a file id never identifies a tag). An
    /// empty tag or id set here does **not** mean "matches nothing" — the text
    /// side still stands.
    ///
    /// The tag set is read differently on each side: on the file side it means
    /// "files carrying any of these tags" (like `HasTag`); on the tag side it
    /// means "any of these tags *or* their subtags" — the resolved tags
    /// themselves are included so a bare id-prefix token surfaces the tag it
    /// names, not merely that tag's children.
    AnyMatch(TextPattern, Vec<TagId>, Vec<FileId>),
    /// Negation of [`QueryTerm::AnyMatch`]: must match *none* of the text, any
    /// tag in the set, or any file id in the set.
    NotAnyMatch(TextPattern, Vec<TagId>, Vec<FileId>),
}

/// How the text half of a [`QueryTerm`] should be interpreted.
///
/// Which one a chunk produces is decided purely by how the user delimited it
/// (see the `chunk` module in `api.rs`): a bare or `"`-quoted payload is a
/// [`Substring`](Self::Substring), a `%`-delimited one is a
/// [`Regex`](Self::Regex).
///
/// The pattern is carried as a [`String`], not a compiled `Regex`, so that
/// `QueryTerm` stays `Clone + PartialEq + Eq` (a compiled regex is none of
/// those) and so the parsed query remains a plain value that is cheap to
/// inspect and compare in tests. Compilation happens once per query inside
/// [`CatalogStore::file_ids_for_query`] / [`CatalogStore::tag_ids_for_query`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextPattern {
    /// A literal substring, matched case-insensitively.
    Substring(String),
    /// A regular expression, matched case-insensitively by default. The
    /// default is inverted rather than fixed: an author who wants case to
    /// matter writes `(?-i)` at the front of the pattern.
    ///
    /// Case-insensitive is the default because everything else in search is,
    /// and a `%Cat%` that quietly behaved differently from `Cat` would be
    /// impossible to predict.
    Regex(String),
}

impl TextPattern {
    /// Prepare this pattern for repeated matching.
    ///
    /// Done once per query rather than per candidate: compiling a regex costs
    /// far more than running one, and search is re-issued on every keystroke
    /// (the UI debounces at 200 ms).
    pub(super) fn compile(&self) -> CompiledPattern {
        match self {
            TextPattern::Substring(needle) => CompiledPattern::Substring(needle.to_lowercase()),
            TextPattern::Regex(pattern) => {
                match RegexBuilder::new(pattern).case_insensitive(true).build() {
                    Ok(regex) => CompiledPattern::Regex(regex),
                    // A pattern that does not compile matches nothing.
                    //
                    // Silently dropping the *term* instead would widen the
                    // result set, so a half-typed `%foo(%` would flash the
                    // user's entire library on screen. Matching nothing keeps
                    // a broken term restrictive, which is both safer and what
                    // an empty result set already communicates. Unlike the
                    // lexer's other recoveries this is not invisible: the user
                    // typed `%`, so they know they asked for a regex.
                    Err(error) => {
                        log::debug!("query regex {pattern:?} did not compile: {error}");
                        CompiledPattern::Never
                    }
                }
            }
        }
    }
}

/// A [`TextPattern`] prepared for matching. See [`TextPattern::compile`].
pub(super) enum CompiledPattern {
    /// Pre-lowercased needle, tested against the pre-lowercased haystack.
    Substring(String),
    Regex(Regex),
    /// A regex that failed to compile.
    Never,
}

impl CompiledPattern {
    /// Test both spellings of one haystack.
    ///
    /// Callers already hold a lowercased copy of every candidate's text (built
    /// once per query), so substring matching can use it directly instead of
    /// allocating per comparison. Regex matching deliberately uses the
    /// *original* text: the case-insensitivity is baked into the compiled
    /// regex, so feeding it a lowercased haystack would silently defeat a
    /// `(?-i)` opt-out.
    pub(super) fn is_match(&self, original: &str, lowercased: &str) -> bool {
        match self {
            CompiledPattern::Substring(needle) => lowercased.contains(needle.as_str()),
            CompiledPattern::Regex(regex) => regex.is_match(original),
            CompiledPattern::Never => false,
        }
    }
}

impl CatalogStore {
    /// Search files by a conjunction of [`QueryTerm`]s.
    ///
    /// A file is returned only if it satisfies every term:
    /// - [`QueryTerm::HasTag`]: it carries *at least one* tag in the set
    ///   (subtag traversal governed by `subtag_rule`).
    /// - [`QueryTerm::NotTag`]: it carries *none* of the tags in the set (same
    ///   traversal).
    /// - [`QueryTerm::LogicalMatches`] / [`QueryTerm::NotLogicalMatches`]: its
    ///   logical path matches / does not match the [`TextPattern`],
    ///   case-insensitively either way.
    /// - [`QueryTerm::FileIdMatches`] / [`QueryTerm::NotFileIdMatches`]: its id
    ///   is / is not in the resolved id-prefix set.
    /// - [`QueryTerm::AnyMatch`] / [`QueryTerm::NotAnyMatch`]: its logical path
    ///   matches the pattern **or** it carries any tag in the set **or** its id
    ///   is in the resolved id set — the "prefix-less" chunk semantics.
    ///
    /// An empty term list matches every file; an empty tag set inside a term
    /// matches no tag (so `HasTag([])` matches nothing and `NotTag([])`
    /// excludes nothing). For [`QueryTerm::AnyMatch`] the text side
    /// still stands when the tag set is empty. Composes
    /// [`Self::file_ids_for_tag`] and [`Self::get_all_files`]; no new SQL.
    ///
    /// `deleted_rule` is passed through to [`Self::get_all_files`] so that
    /// tombstoned files can participate in the candidate pool under
    /// [`DeletedRule::Include`]. Relationship traversal (`entries_v1`)
    /// stays live-only regardless — only the file's own row visibility is
    /// affected. Callers filter the returned ids by their `deleted` flag if
    /// they want an only-deleted view.
    pub fn file_ids_for_query(
        &self,
        terms: &[QueryTerm],
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<impl IntoIterator<Item = FileId>, DatabaseError> {
        // The set of files carrying at least one tag in `tag_ids` (the union of
        // each tag's files). An empty set yields no files.
        let files_for_any_tag = |tag_ids: &[TagId]| -> Result<BTreeSet<FileId>, DatabaseError> {
            let mut union = BTreeSet::new();
            for tag_id in tag_ids {
                union.extend(self.file_ids_for_tag(*tag_id, subtag_rule)?);
            }
            Ok(union)
        };

        // Seed the candidate set. If there is at least one positive tag-only
        // term (`HasTag`), start from the intersection of those (each
        // contributing the union of its matched tags' files); otherwise start
        // from all files. `AnyMatch` is *not* a seed — its substring half can
        // match files that carry no tags at all, so we always need every file
        // in the candidate pool for the filter pass below.
        let positive: Vec<&[TagId]> = terms
            .iter()
            .filter_map(|term| match term {
                QueryTerm::HasTag(tag_ids) => Some(tag_ids.as_slice()),
                _ => None,
            })
            .collect();

        let mut candidates: BTreeSet<FileId> = if let Some((first, rest)) = positive.split_first() {
            let mut set = files_for_any_tag(first)?;
            for tag_ids in rest {
                let next = files_for_any_tag(tag_ids)?;
                set.retain(|file_id| next.contains(file_id));
            }
            // A `HasTag` seed comes from `entries_v1` (always live) and does
            // not consult `files_v2.deleted`, so it can include ids for
            // tombstoned files. We do NOT drop them here — `ApiService::search`
            // joins each id through `file_info_from_id` (which honors the
            // requested `deleted_rule`) and skips misses. This preserves the
            // long-standing tolerance for files referenced from `entries_v1`
            // without a matching `file_versions` row yet.
            set
        } else {
            self.get_all_files(deleted_rule)?
                .into_iter()
                .map(|file| file.file_id)
                .collect()
        };

        // Subtract every negative tag term (the union of its matched tags' files).
        for term in terms {
            if let QueryTerm::NotTag(tag_ids) = term {
                let excluded = files_for_any_tag(tag_ids)?;
                candidates.retain(|file_id| !excluded.contains(file_id));
            }
        }

        // Membership-only terms (`FileIdMatches` / `NotFileIdMatches`) filter
        // by the resolved id set directly, no path lookup required. Apply them
        // before the text pass so a pure `/i` query need not build the path map.
        for term in terms {
            match term {
                QueryTerm::FileIdMatches(file_ids) => {
                    let allowed: BTreeSet<FileId> = file_ids.iter().copied().collect();
                    candidates.retain(|file_id| allowed.contains(file_id));
                }
                QueryTerm::NotFileIdMatches(file_ids) => {
                    let excluded: BTreeSet<FileId> = file_ids.iter().copied().collect();
                    candidates.retain(|file_id| !excluded.contains(file_id));
                }
                _ => {}
            }
        }

        // Text-bearing terms (`NameMatches`, `LogicalMatches`, their negations,
        // `AnyMatch`, `NotAnyMatch`) need each candidate's logical path *and*,
        // for the `Any` variants, the union of files-carrying-any-of-the-tag-set
        // together with the resolved file-id set (both sides of the id/tag/text
        // disjunction). Build the lookups once, then apply every text term as
        // one retain pass — cheaper than rebuilding per term and keeps the
        // semantics obvious.
        let has_text_term = terms.iter().any(|term| {
            matches!(
                term,
                QueryTerm::NameMatches(_)
                    | QueryTerm::NotNameMatches(_)
                    | QueryTerm::LogicalMatches(_)
                    | QueryTerm::NotLogicalMatches(_)
                    | QueryTerm::AnyMatch(..)
                    | QueryTerm::NotAnyMatch(..),
            )
        });

        if has_text_term {
            // Both spellings of each path: the original for regex matching and
            // a lowercased copy for substring matching. See
            // `CompiledPattern::is_match` for why the regex side must not see
            // the lowercased form.
            let paths: std::collections::BTreeMap<FileId, (String, String)> = self
                .get_all_files(deleted_rule)?
                .into_iter()
                .map(|file| {
                    let path = file.logical_path.as_str().to_owned();
                    let lowercased = path.to_lowercase();
                    (file.file_id, (path, lowercased))
                })
                .collect();

            // Compile every pattern, and precompute the file set for every
            // `Any` variant, before entering the retain closure: neither a
            // regex build nor a DB hit belongs inside a per-candidate loop.
            let mut patterns: Vec<(CompiledPattern, bool)> = Vec::new();
            let mut any_tag_sets: Vec<(CompiledPattern, BTreeSet<FileId>, bool)> = Vec::new();
            for term in terms {
                match term {
                    QueryTerm::NameMatches(pattern) | QueryTerm::LogicalMatches(pattern) => {
                        patterns.push((pattern.compile(), false));
                    }
                    QueryTerm::NotNameMatches(pattern) | QueryTerm::NotLogicalMatches(pattern) => {
                        patterns.push((pattern.compile(), true));
                    }
                    QueryTerm::AnyMatch(pattern, tag_ids, file_ids) => {
                        // The `Any` membership side is the union of files
                        // carrying any matched tag and the resolved file-id set.
                        let mut member_files = files_for_any_tag(tag_ids)?;
                        member_files.extend(file_ids.iter().copied());
                        any_tag_sets.push((pattern.compile(), member_files, false));
                    }
                    QueryTerm::NotAnyMatch(pattern, tag_ids, file_ids) => {
                        let mut member_files = files_for_any_tag(tag_ids)?;
                        member_files.extend(file_ids.iter().copied());
                        any_tag_sets.push((pattern.compile(), member_files, true));
                    }
                    _ => continue,
                }
            }

            candidates.retain(|file_id| {
                let Some((path, lowercased)) = paths.get(file_id) else {
                    return false;
                };
                for (pattern, negated) in &patterns {
                    if pattern.is_match(path, lowercased) == *negated {
                        return false;
                    }
                }
                for (pattern, member_files, negated) in &any_tag_sets {
                    let hit = pattern.is_match(path, lowercased) || member_files.contains(file_id);
                    if hit == *negated {
                        return false;
                    }
                }
                true
            });
        }

        Ok(candidates)
    }

    /// Search *tags* by the same conjunction of [`QueryTerm`]s used for files,
    /// mirroring [`Self::file_ids_for_query`] onto the tag hierarchy:
    ///
    /// - [`QueryTerm::HasTag`]: the tag must be a subtag of *at least one* tag
    ///   in the set (subtag traversal governed by `subtag_rule`) — the tag
    ///   analogue of "a file carries this tag".
    /// - [`QueryTerm::NotTag`]: the tag must *not* be a subtag of any tag in
    ///   the set.
    /// - [`QueryTerm::NameMatches`] / [`QueryTerm::NotNameMatches`]: the tag's
    ///   name matches / does not match the [`TextPattern`], compared
    ///   case-insensitively.
    /// - [`QueryTerm::LogicalMatches`] / [`QueryTerm::NotLogicalMatches`] and
    ///   [`QueryTerm::FileIdMatches`] / [`QueryTerm::NotFileIdMatches`]: these
    ///   are file-only axes (a tag has neither a logical path nor a file id),
    ///   so any of them empties the tag result.
    /// - [`QueryTerm::AnyMatch`] / [`QueryTerm::NotAnyMatch`]: the tag's name
    ///   matches the pattern **or** the tag *is* one of the tags in the set
    ///   **or** it is a subtag of one — the tag analogue of the file-side `Any`
    ///   semantics. Including the resolved tags *themselves* (not just their
    ///   subtags) is what lets a bare id-prefix token surface the very tag it
    ///   names; `HasTag` (`/t` / `/T`) deliberately stays subtags-only. The
    ///   `Any` file-id set is ignored here, since a file id never identifies a
    ///   tag.
    ///
    /// An empty term list matches every tag; an empty tag set inside a term
    /// matches no tag. For [`QueryTerm::AnyMatch`] the text side still
    /// stands when the tag set is empty. Composes [`Self::subtag_ids_for_tag`]
    /// and [`Self::get_all_tags`]; no new SQL.
    ///
    /// `deleted_rule` is passed through to [`Self::get_all_tags`] so that
    /// tombstoned tags can participate in the candidate pool under
    /// [`DeletedRule::Include`]. Relationship traversal (`entries_v1`)
    /// stays live-only regardless — only the tag's own row visibility is
    /// affected. Callers filter the returned ids by their `deleted` flag if
    /// they want an only-deleted view.
    pub fn tag_ids_for_query(
        &self,
        terms: &[QueryTerm],
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<impl IntoIterator<Item = TagId>, DatabaseError> {
        // The set of tags that are a subtag of at least one tag in `tag_ids`
        // (the union of each tag's subtags). An empty set yields no tags.
        let subtags_of_any = |tag_ids: &[TagId]| -> Result<BTreeSet<TagId>, DatabaseError> {
            let mut union = BTreeSet::new();
            for tag_id in tag_ids {
                union.extend(self.subtag_ids_for_tag(*tag_id, subtag_rule)?);
            }
            Ok(union)
        };

        // Seed on `HasTag` only, same reasoning as `file_ids_for_query`:
        // `AnyMatch` can match on the name side without any tag membership, so
        // we can't use it to narrow the seed.
        let positive: Vec<&[TagId]> = terms
            .iter()
            .filter_map(|term| match term {
                QueryTerm::HasTag(tag_ids) => Some(tag_ids.as_slice()),
                _ => None,
            })
            .collect();

        let mut candidates: BTreeSet<TagId> = if let Some((first, rest)) = positive.split_first() {
            let mut set = subtags_of_any(first)?;
            for tag_ids in rest {
                let next = subtags_of_any(tag_ids)?;
                set.retain(|tag_id| next.contains(tag_id));
            }
            // Subtag traversal comes from `entries_v1` (always live) and does
            // not consult `tags_v1.deleted`, so it can include ids for
            // tombstoned tags. We do NOT drop them here — `ApiService::search`
            // joins each id through `tag_from_id` (which honors the requested
            // `deleted_rule`) and skips misses, matching the tolerance in
            // `file_ids_for_query`.
            set
        } else {
            self.get_all_tags(deleted_rule)?
                .into_iter()
                .map(|tag| tag.id)
                .collect()
        };

        for term in terms {
            match term {
                // Logical-path and file-id terms are file-only axes: a tag has
                // neither a logical path nor a file id, so any such term (in
                // either polarity) makes the tag result empty — the same rule
                // as the logical case, extended to `/i`.
                QueryTerm::LogicalMatches(..)
                | QueryTerm::NotLogicalMatches(..)
                | QueryTerm::FileIdMatches(..)
                | QueryTerm::NotFileIdMatches(..) => {
                    candidates.clear();
                    return Ok(candidates);
                }
                QueryTerm::NotTag(tag_ids) => {
                    let excluded = subtags_of_any(tag_ids)?;
                    candidates.retain(|tag_id| !excluded.contains(tag_id));
                }
                _ => {}
            }
        }

        let has_text_term = terms.iter().any(|term| {
            matches!(
                term,
                QueryTerm::NameMatches(_)
                    | QueryTerm::NotNameMatches(_)
                    | QueryTerm::AnyMatch(..)
                    | QueryTerm::NotAnyMatch(..),
            )
        });

        if has_text_term {
            // Original + lowercased, for the same reason as in
            // `file_ids_for_query`.
            let names: std::collections::BTreeMap<TagId, (String, String)> = self
                .get_all_tags(deleted_rule)?
                .into_iter()
                .map(|tag| {
                    let lowercased = tag.name.to_lowercase();
                    (tag.id, (tag.name, lowercased))
                })
                .collect();

            let mut patterns: Vec<(CompiledPattern, bool)> = Vec::new();
            let mut any_tag_sets: Vec<(CompiledPattern, BTreeSet<TagId>, bool)> = Vec::new();
            for term in terms {
                match term {
                    QueryTerm::NameMatches(pattern) => patterns.push((pattern.compile(), false)),
                    QueryTerm::NotNameMatches(pattern) => {
                        patterns.push((pattern.compile(), true));
                    }
                    // The file-id set is ignored on the tag side: a file id
                    // never identifies a tag, so only the name and tag-set
                    // halves of an `Any` token can match here.
                    //
                    // Membership on the tag side is the resolved tags
                    // *themselves* together with their subtags — not subtags
                    // alone. A bare token can resolve a tag by *id* (e.g. a
                    // pasted short id), and the user's intent is to find that
                    // tag, which its name pattern won't match; folding the
                    // resolved ids in makes the tag surface itself. (For `/t` /
                    // `/T`, which mean "things carrying this tag", the
                    // subtags-only `HasTag` semantics are deliberately left
                    // unchanged.)
                    QueryTerm::AnyMatch(pattern, tag_ids, _file_ids) => {
                        let mut members = subtags_of_any(tag_ids)?;
                        members.extend(tag_ids.iter().copied());
                        any_tag_sets.push((pattern.compile(), members, false));
                    }
                    QueryTerm::NotAnyMatch(pattern, tag_ids, _file_ids) => {
                        let mut members = subtags_of_any(tag_ids)?;
                        members.extend(tag_ids.iter().copied());
                        any_tag_sets.push((pattern.compile(), members, true));
                    }
                    _ => continue,
                }
            }

            candidates.retain(|tag_id| {
                let Some((name, lowercased)) = names.get(tag_id) else {
                    return false;
                };
                for (pattern, negated) in &patterns {
                    if pattern.is_match(name, lowercased) == *negated {
                        return false;
                    }
                }
                for (pattern, subtagged, negated) in &any_tag_sets {
                    let hit = pattern.is_match(name, lowercased) || subtagged.contains(tag_id);
                    if hit == *negated {
                        return false;
                    }
                }
                true
            });
        }

        Ok(candidates)
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::LogicalPath;

    use super::*;
    use crate::clock::now_millis;
    use crate::store::fixtures::{dot_style, memory_db};

    /// Build a catalog of files at the given logical paths, returning the ids
    /// in the same order.
    fn files_at(database: &mut CatalogStore, paths: &[&str]) -> Vec<FileId> {
        paths
            .iter()
            .map(|path| {
                let file_id = FileId::new();
                database
                    .add_file(file_id, &LogicalPath::new(*path), 0)
                    .unwrap();
                database
                    .record_version(file_id, "hash", "local", 1)
                    .unwrap();
                file_id
            })
            .collect()
    }

    fn matching_files(database: &CatalogStore, terms: &[QueryTerm]) -> BTreeSet<FileId> {
        database
            .file_ids_for_query(terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect()
    }

    fn regex(pattern: &str) -> TextPattern {
        TextPattern::Regex(pattern.to_owned())
    }

    #[test]
    fn regex_term_matches_against_the_full_logical_path() {
        let mut database = memory_db();
        let files = files_at(&mut database, &[
            "photos/holiday/cat.jpg",
            "archive/photos/cat.jpg",
            "notes/todo.md",
        ]);

        let terms = vec![QueryTerm::LogicalMatches(regex("^photos/"))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[0]]),
            "anchoring must bind to the whole path, not the basename"
        );
    }

    /// Slashes in a pattern need no escaping — the reason the delimiter is `%`
    /// rather than the conventional `/`.
    #[test]
    fn regex_term_may_contain_slashes() {
        let mut database = memory_db();
        let files = files_at(&mut database, &[
            "photos/2024/raw/a.dng",
            "photos/2024/a.jpg",
        ]);

        let terms = vec![QueryTerm::LogicalMatches(regex(r"^photos/.*/raw/.*\.dng$"))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[0]])
        );
    }

    /// Regexes are case-insensitive by default, matching every other text term.
    #[test]
    fn regex_term_is_case_insensitive_by_default() {
        let mut database = memory_db();
        let files = files_at(&mut database, &["Photos/Cat.JPG"]);

        let terms = vec![QueryTerm::LogicalMatches(regex(r"^photos/cat\.jpg$"))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[0]])
        );
    }

    /// ...and `(?-i)` opts back out. This is the case that breaks if the
    /// matcher is handed a pre-lowercased haystack.
    #[test]
    fn regex_term_case_sensitivity_can_be_opted_into() {
        let mut database = memory_db();
        let files = files_at(&mut database, &["Photos/Cat.JPG", "photos/cat.jpg"]);

        let terms = vec![QueryTerm::LogicalMatches(regex(r"(?-i)^Photos/"))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[0]]),
            "(?-i) must see the original casing"
        );
    }

    /// A pattern that does not compile matches nothing, rather than being
    /// dropped (which would *widen* the result set).
    #[test]
    fn invalid_regex_term_matches_nothing() {
        let mut database = memory_db();
        files_at(&mut database, &["notes/todo.md", "notes/todo.txt"]);

        let terms = vec![QueryTerm::LogicalMatches(regex("*.md"))];
        assert!(matching_files(&database, &terms).is_empty());
    }

    #[test]
    fn negated_regex_term_excludes_matches() {
        let mut database = memory_db();
        let files = files_at(&mut database, &["notes/todo.md", "notes/todo.txt"]);

        let terms = vec![QueryTerm::NotLogicalMatches(regex(r"\.md$"))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[1]])
        );
    }

    /// A substring term keeps meaning exactly what it did: regex metacharacters
    /// in it are literal.
    #[test]
    fn substring_term_treats_metacharacters_literally() {
        let mut database = memory_db();
        let files = files_at(&mut database, &["notes/todo.md", "notes/todoXmd"]);

        let terms = vec![QueryTerm::LogicalMatches(TextPattern::Substring(
            "todo.md".to_owned(),
        ))];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([files[0]]),
            "`.` must not behave as a wildcard in a substring term"
        );
    }

    /// `/t %...%` resolves tags by regex over their names.
    #[test]
    fn regex_token_resolves_tags_by_name() {
        let database = memory_db();
        let wip = TagId::new();
        let done = TagId::new();
        database
            .add_tag(wip, "wip-draft", &dot_style("red"), 1)
            .unwrap();
        database
            .add_tag(done, "done", &dot_style("red"), 1)
            .unwrap();

        let matched = database
            .tag_ids_matching_pattern(&regex("^wip-"), DeletedRule::Exclude)
            .unwrap();
        assert_eq!(matched, vec![wip]);
    }

    /// Tag *ids* stay out of regex resolution: a pattern that would match a
    /// hex id prefix must not sweep tags in by id.
    #[test]
    fn regex_token_does_not_resolve_tags_by_id() {
        let database = memory_db();
        let tag_id = TagId::new();
        database
            .add_tag(tag_id, "unrelated", &dot_style("red"), 1)
            .unwrap();

        // A pattern matching the tag's own id prefix, which the substring path
        // *would* resolve.
        let id = tag_id.to_string();
        let matched = database
            .tag_ids_matching_pattern(&regex(&format!("^{}", &id[..6])), DeletedRule::Exclude)
            .unwrap();
        assert!(matched.is_empty(), "ids are not a regex surface");

        // The substring path still resolves that same prefix, proving the test
        // is comparing the two paths rather than a typo.
        let matched = database
            .tag_ids_matching_pattern(
                &TextPattern::Substring(id[..6].to_owned()),
                DeletedRule::Exclude,
            )
            .unwrap();
        assert_eq!(matched, vec![tag_id]);
    }

    #[test]
    fn file_ids_for_query_positive_term_unions_all_matching_tags() {
        let database = memory_db();
        // Two distinct tags both containing the substring 'foo'.
        let foo = TagId::new();
        let foobar = TagId::new();
        database.add_tag(foo, "foo", &dot_style("red"), 1).unwrap();
        database
            .add_tag(foobar, "foobar", &dot_style("red"), 1)
            .unwrap();

        // file_a carries `foo`, file_b carries `foobar`, file_c carries neither.
        let file_a = FileId::new();
        let file_b = FileId::new();
        let file_c = FileId::new();
        for (id, path) in [(file_a, "a"), (file_b, "b"), (file_c, "c")] {
            database.add_file(id, &LogicalPath::new(path), 0).unwrap();
        }
        database.tag_file(foo, file_a, 1).unwrap();
        database.tag_file(foobar, file_b, 1).unwrap();

        // `$foo` should match files carrying either tag (union), not require both.
        let terms = vec![QueryTerm::HasTag(vec![foo, foobar])];
        let matched: BTreeSet<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(matched, BTreeSet::from([file_a, file_b]));
    }

    #[test]
    fn file_ids_for_query_empty_positive_set_matches_nothing() {
        let database = memory_db();
        let file = FileId::new();
        database.add_file(file, &LogicalPath::new("a"), 0).unwrap();

        // A `$foo` term that matched no tag (empty set) matches no file.
        let terms = vec![QueryTerm::HasTag(vec![])];
        let matched: Vec<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();

        assert!(matched.is_empty());
    }

    #[test]
    fn file_ids_for_query_negative_term_excludes_union_of_matching_tags() {
        let mut database = memory_db();
        let foo = TagId::new();
        let foobar = TagId::new();
        database.add_tag(foo, "foo", &dot_style("red"), 1).unwrap();
        database
            .add_tag(foobar, "foobar", &dot_style("red"), 1)
            .unwrap();

        let file_a = FileId::new();
        let file_b = FileId::new();
        let file_c = FileId::new();
        for (id, path) in [(file_a, "a"), (file_b, "b"), (file_c, "c")] {
            database.add_file(id, &LogicalPath::new(path), 0).unwrap();
            // A negative-only query seeds from all files, which requires each to
            // have a recorded version to appear in the listing.
            database.record_version(id, "hash", "test", 1).unwrap();
        }
        database.tag_file(foo, file_a, 1).unwrap();
        database.tag_file(foobar, file_b, 1).unwrap();

        // `!foo` excludes files carrying either matching tag, leaving only file_c.
        let terms = vec![QueryTerm::NotTag(vec![foo, foobar])];
        let matched: BTreeSet<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(matched, BTreeSet::from([file_c]));
    }

    #[test]
    fn file_ids_for_query_include_returns_tombstoned_candidates() {
        // A `HasTag` search under `DeletedRule::Include` must surface files
        // whose row is tombstoned in `files_v2` (they still have live
        // `entries_v1` rows pointing at them). The caller is expected to
        // post-filter by `FileInfo::deleted` to keep only the tombstoned
        // ones; the evaluator does not do that step itself.
        let mut database = memory_db();
        let tag_id = TagId::new();
        database
            .add_tag(tag_id, "photos", &dot_style("red"), 10)
            .unwrap();

        let live = FileId::new();
        let dead = FileId::new();
        database
            .add_file(live, &LogicalPath::new("live.jpg"), 0)
            .unwrap();
        database
            .add_file(dead, &LogicalPath::new("dead.jpg"), 0)
            .unwrap();
        database.record_version(live, "h1", "local", 1).unwrap();
        database.record_version(dead, "h2", "local", 1).unwrap();
        database.tag_file(tag_id, live, 20).unwrap();
        database.tag_file(tag_id, dead, 20).unwrap();

        let deleted_at = now_millis() + 10_000;
        assert!(database.remove_file(dead, deleted_at).unwrap());

        let terms = vec![QueryTerm::HasTag(vec![tag_id])];

        // Exclude does not drop the tombstoned candidate at the evaluator
        // layer either; the "hide deleted" filtering happens downstream in
        // `ApiService::search` (via `file_info_from_id` returning MissingFile).
        // What we assert here is that both ids reach the caller and the
        // downstream filter has enough information to distinguish them.
        let candidates: BTreeSet<FileId> = database
            .file_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Include)
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(candidates, BTreeSet::from([live, dead]));

        // Joining under `Include` returns both, tagged with `deleted`.
        let live_info = database
            .file_info_from_id(live, DeletedRule::Include)
            .unwrap();
        let dead_info = database
            .file_info_from_id(dead, DeletedRule::Include)
            .unwrap();
        assert!(!live_info.deleted);
        assert!(dead_info.deleted);
    }

    /// A `FileIdMatches` term (the `/i` prefix) keeps only files whose id is in
    /// the resolved set.
    #[test]
    fn file_ids_for_query_file_id_term_filters_by_id() {
        use crate::store::fixtures::file_id_from_hex;
        let mut database = memory_db();
        let wanted = file_id_from_hex("abcd000000000000000000000000000a");
        let other = file_id_from_hex("ffff000000000000000000000000000f");
        for (id, path) in [(wanted, "a"), (other, "b")] {
            database.add_file(id, &LogicalPath::new(path), 0).unwrap();
            database.record_version(id, "hash", "local", 1).unwrap();
        }

        let terms = vec![QueryTerm::FileIdMatches(vec![wanted])];
        assert_eq!(matching_files(&database, &terms), BTreeSet::from([wanted]));

        // An empty resolved set matches no file.
        let terms = vec![QueryTerm::FileIdMatches(vec![])];
        assert!(matching_files(&database, &terms).is_empty());
    }

    /// `NotFileIdMatches` excludes the resolved ids and keeps the rest.
    #[test]
    fn file_ids_for_query_negated_file_id_term_excludes_id() {
        use crate::store::fixtures::file_id_from_hex;
        let mut database = memory_db();
        let excluded = file_id_from_hex("abcd000000000000000000000000000a");
        let kept = file_id_from_hex("ffff000000000000000000000000000f");
        for (id, path) in [(excluded, "a"), (kept, "b")] {
            database.add_file(id, &LogicalPath::new(path), 0).unwrap();
            database.record_version(id, "hash", "local", 1).unwrap();
        }

        let terms = vec![QueryTerm::NotFileIdMatches(vec![excluded])];
        assert_eq!(matching_files(&database, &terms), BTreeSet::from([kept]));
    }

    /// The bare-token `AnyMatch` unions its file-id set into the membership
    /// side: a file whose *path* doesn't match the substring but whose *id* is
    /// in the resolved set still matches.
    #[test]
    fn file_ids_for_query_any_match_unions_file_id_set() {
        use crate::store::fixtures::file_id_from_hex;
        let mut database = memory_db();
        // `by_id`'s path shares nothing with the search text; `by_path` does.
        let by_id = file_id_from_hex("abcd000000000000000000000000000a");
        let by_path = file_id_from_hex("ffff000000000000000000000000000f");
        database
            .add_file(by_id, &LogicalPath::new("unrelated"), 0)
            .unwrap();
        database.record_version(by_id, "hash", "local", 1).unwrap();
        database
            .add_file(by_path, &LogicalPath::new("abcd-name"), 0)
            .unwrap();
        database
            .record_version(by_path, "hash", "local", 1)
            .unwrap();

        // Text "abcd" matches `by_path`'s name; the file-id set pulls in
        // `by_id`. Both should match under the union.
        let terms = vec![QueryTerm::AnyMatch(
            TextPattern::Substring("abcd".to_owned()),
            vec![],
            vec![by_id],
        )];
        assert_eq!(
            matching_files(&database, &terms),
            BTreeSet::from([by_id, by_path])
        );
    }

    /// The `Any` file-id set is ignored on the tag side: a token that resolves
    /// a file id but whose text matches no tag name yields no tags.
    #[test]
    fn tag_ids_for_query_any_match_ignores_file_id_set() {
        use crate::store::fixtures::file_id_from_hex;
        let database = memory_db();
        let tag_id = TagId::new();
        database
            .add_tag(tag_id, "photos", &dot_style("red"), 1)
            .unwrap();

        let some_file = file_id_from_hex("abcd000000000000000000000000000a");
        let terms = vec![QueryTerm::AnyMatch(
            TextPattern::Substring("no-such-name".to_owned()),
            vec![],
            vec![some_file],
        )];
        let matched: BTreeSet<TagId> = database
            .tag_ids_for_query(&terms, SubtagRule::Exclude, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();
        assert!(matched.is_empty());
    }

    /// A file-id term empties the tag result, exactly like a logical-path term:
    /// tags have no file id, so `/i` is a file-only axis.
    #[test]
    fn tag_ids_for_query_file_id_term_yields_no_tags() {
        use crate::store::fixtures::file_id_from_hex;
        let database = memory_db();
        let tag_id = TagId::new();
        database
            .add_tag(tag_id, "photos", &dot_style("red"), 1)
            .unwrap();

        let some_file = file_id_from_hex("abcd000000000000000000000000000a");
        for term in [
            QueryTerm::FileIdMatches(vec![some_file]),
            QueryTerm::NotFileIdMatches(vec![some_file]),
        ] {
            let matched: BTreeSet<TagId> = database
                .tag_ids_for_query(&[term], SubtagRule::Exclude, DeletedRule::Exclude)
                .unwrap()
                .into_iter()
                .collect();
            assert!(matched.is_empty());
        }
    }

    /// A bare token that resolved a tag by id must surface the *tag itself* on
    /// the tag side — not just its subtags. The bare token's pattern is the hex
    /// id (which won't match the tag's name), so the tag only appears because
    /// the resolved id is folded into the `Any` membership set.
    #[test]
    fn tag_ids_for_query_any_match_surfaces_the_resolved_tag_itself() {
        use crate::store::fixtures::tag_id_from_hex;
        let database = memory_db();
        let parent = tag_id_from_hex("abcd000000000000000000000000000a");
        let child = TagId::new();
        let unrelated = TagId::new();
        database
            .add_tag(parent, "work", &dot_style("red"), 1)
            .unwrap();
        database
            .add_tag(child, "urgent", &dot_style("red"), 1)
            .unwrap();
        database
            .add_tag(unrelated, "leisure", &dot_style("red"), 1)
            .unwrap();
        // `urgent` is a subtag of `work`.
        database.tag_tag(parent, child, 1).unwrap();

        // A bare token whose payload is `parent`'s id prefix. Its text side is
        // the hex string (matches no name); the tag set is the resolved parent.
        let terms = vec![QueryTerm::AnyMatch(
            TextPattern::Substring("abcd".to_owned()),
            vec![parent],
            vec![],
        )];
        let matched: BTreeSet<TagId> = database
            .tag_ids_for_query(&terms, SubtagRule::Include, DeletedRule::Exclude)
            .unwrap()
            .into_iter()
            .collect();

        // The resolved tag itself *and* its subtag are returned; the unrelated
        // tag is not.
        assert!(
            matched.contains(&parent),
            "the tag named by the id must appear"
        );
        assert!(matched.contains(&child), "its subtags still appear too");
        assert!(!matched.contains(&unrelated));
    }
}
