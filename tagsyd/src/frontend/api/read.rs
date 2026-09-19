//! Read half of the API: resolution, lookup, traversal and search.
//!
//! Every method here opens its own short-lived read-only [`CatalogStore`]
//! handle (see [`ApiService::open_read`]) and drops it before returning; a
//! `&CatalogStore` is never held across an `.await`.

use tagsy_core::{FileId, FileInfo, FileKind, LogicalPath, TagId, classify_extension};
use tokio::sync::oneshot;

use super::{
    ApiError, ApiService, RetagSummary, SearchResults, StorageStats, TagRuleReport, token,
};
use crate::configuration::SyncDirectory;
use crate::store::{
    CatalogStore, DatabaseError, DeletedRule, QueryTerm, SubtagRule, Tag, TextPattern,
};
use crate::sync_directories::SyncDirectoryCommand;

impl ApiService {
    /// Resolve a user-supplied `term` to a single [`FileId`].
    ///
    /// `term` may be a full id, any id **prefix** (of any length — the short id
    /// shown in listings is only a *display* hint, it has no special standing
    /// in resolution), or a name/path. Resolution mirrors the `/e` entity
    /// query axis: a file matches if its logical path contains `term` as a
    /// substring **or** its id starts with `term`, never by tag membership.
    ///
    /// Two tiers, exact first:
    /// 1. **Exact path**: if `term` equals one file's logical path exactly,
    ///    that file wins even when it is also a substring of others (so
    ///    `report.txt` resolves cleanly next to `report.txt.bak`).
    /// 2. **`/e` union**: otherwise the path-substring ∪ id-prefix set must
    ///    contain **exactly one** file.
    ///
    /// Returns [`ApiError::UnknownId`] if nothing matches and
    /// [`ApiError::AmbiguousId`] (carrying the original `term`) if more than
    /// one file matches.
    ///
    /// `deleted_rule` governs whether tombstoned files participate; operational
    /// lookups pass [`DeletedRule::Exclude`], while the restore path passes
    /// [`DeletedRule::Include`] so a deleted file can still be named.
    pub fn resolve_file_id(
        &self,
        term: &str,
        deleted_rule: DeletedRule,
    ) -> Result<FileId, ApiError> {
        let database = self.open_read()?;
        resolve_file_id(&database, term, deleted_rule)
    }

    /// Classify a file's logical `name` into its [`FileKind`] from the
    /// extension alone — the shared, authoritative, byte-free
    /// classification. Pure: no DB handle, no I/O. Used for files not yet
    /// in the catalog (where no [`tagsy_core::FileInfo::kind`] exists), so
    /// both agree from the same name.
    pub fn classify(&self, name: &str) -> Result<FileKind, ApiError> {
        Ok(classify_extension(&LogicalPath::new(name).extension()))
    }

    /// Resolve a user-supplied `term` to a single [`TagId`]. The tag
    /// counterpart of [`resolve_file_id`](Self::resolve_file_id).
    ///
    /// `term` may be a full id, any id **prefix** (of any length — the short id
    /// is a display hint only), or a tag name. Resolution mirrors the `/e`
    /// entity query axis: a tag matches if its name contains `term` as a
    /// substring **or** its id starts with `term`, never by subtag membership.
    ///
    /// Two tiers, exact first:
    /// 1. **Exact name**: if `term` equals one tag's name exactly, that tag
    ///    wins even when it is a substring of others (so `photo` resolves
    ///    cleanly next to `photography`).
    /// 2. **`/e` union**: otherwise the name-substring ∪ id-prefix set must
    ///    contain **exactly one** tag.
    ///
    /// Returns [`ApiError::UnknownId`] if nothing matches and
    /// [`ApiError::AmbiguousId`] (carrying the original `term`) if more than
    /// one tag matches. See [`resolve_file_id`](Self::resolve_file_id) for the
    /// `deleted_rule` semantics.
    pub fn resolve_tag_id(&self, term: &str, deleted_rule: DeletedRule) -> Result<TagId, ApiError> {
        let database = self.open_read()?;
        resolve_tag_id(&database, term, deleted_rule)
    }

    /// List the tags applied to `file_id`. `subtag_rule` controls whether the
    /// tag hierarchy is walked. Backed by `CatalogStore::tag_ids_for_file`.
    pub fn tags_for_file(
        &self,
        file_id: FileId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        let database = self.open_read()?;
        Ok(database
            .tag_ids_for_file(file_id, subtag_rule)?
            .into_iter()
            .collect())
    }

    /// Run a free-form query and return both the matching files and tags.
    ///
    /// The query is a whitespace-separated list of *tokens*, combined
    /// conjunctively (a result must satisfy every token). Each token is
    /// optionally prefixed by `!` (negation) and/or a kind prefix:
    ///
    /// - `/t foo` — require the tag(s) resolved from `foo` by name **or** id
    ///   prefix. A file matches if it carries any such tag; a tag matches if it
    ///   is a subtag of any.
    /// - `/T foo` — like `/t`, but resolves tags by id prefix **only** (no name
    ///   matching).
    /// - `/i foo` — require the file(s) whose id starts with `foo`. File-only:
    ///   the tag result is empty when this token is present.
    /// - `/h foo` — require the file(s) whose latest content hash starts with
    ///   `foo`. File-only, like `/i`.
    /// - `/n foo` — case-insensitive substring against the logical path or tag
    ///   name.
    /// - `/l foo` — case-insensitive substring against the file's logical path
    ///   (or the tag's name on the tag side).
    /// - `foo` (no prefix) — matches on *any* axis: logical/name substring OR
    ///   tag membership OR the file/tag's own id prefix. This is the "just find
    ///   anything that looks like `foo`" token.
    /// - `!` in front of any of the above inverts the filter.
    ///
    /// Tokens with whitespace can be quoted: `/t "foo bar"`.
    ///
    /// Parsing is forgiving — malformed tokens are silently dropped so a
    /// half-typed query in a search box still returns results (see
    /// [`token`] for the full grammar and recovery rules). Tag tokens are
    /// resolved to [`TagId`]s here so clients pass the raw string through; an
    /// empty query matches everything. `subtag_rule` controls hierarchy
    /// traversal for the tag terms.
    ///
    /// Returns full [`FileInfo`]/[`Tag`] rows (not bare ids): the daemon joins
    /// each matched id to its row here, over just the result set, so callers
    /// render directly without a second whole-store listing. Backed by
    /// `CatalogStore::file_ids_for_query`/`tag_ids_for_query` plus
    /// `file_info_from_id`/`tag_from_id`.
    ///
    /// `deleted_rule` toggles between the standard live-only view
    /// ([`DeletedRule::Exclude`]) and the "search deleted rows"
    /// view ([`DeletedRule::Include`]). Under `Include`, this method
    /// widens tag-token resolution *and* the candidate pool to include
    /// tombstoned rows, then post-filters the joined `FileInfo`/`Tag` results
    /// to keep only the ones whose `deleted` flag is set — an *only deleted*
    /// result. This lets the UI expose "show deleted" as a toggle without
    /// requiring a separate query grammar. Tag-hierarchy walks and the
    /// file↔tag relationship table stay live-only regardless, since users
    /// searching for deleted files/tags want files whose row itself was
    /// tombstoned, not files that were merely untagged.
    pub fn search(
        &self,
        query: &str,
        subtag_rule: SubtagRule,
        deleted_rule: DeletedRule,
    ) -> Result<SearchResults, ApiError> {
        let database = self.open_read()?;
        let terms = Self::parse_query(&database, query, deleted_rule)?;

        // A matched id may not resolve to a full listable row: `file_ids_for_query`
        // draws file ids from the tag `entries` table, which can reference a file
        // that has no `file_versions` row yet (tagged before its content
        // materialized). Such a file is not listable, so skip it rather than
        // failing the whole query with `UnknownId`. Same tolerance for tags.
        let mut files = Vec::new();
        for file_id in database.file_ids_for_query(&terms, subtag_rule, deleted_rule)? {
            match database.file_info_from_id(file_id, deleted_rule) {
                Ok(file) => {
                    // Under `Include` we want only the tombstoned files; the
                    // live ones are handled by the standard `Exclude` path.
                    if deleted_rule == DeletedRule::Include && !file.deleted {
                        continue;
                    }
                    files.push(file);
                }
                Err(DatabaseError::MissingFile) => {}
                Err(other) => return Err(other.into()),
            }
        }

        let mut tags = Vec::new();
        for tag_id in database.tag_ids_for_query(&terms, subtag_rule, deleted_rule)? {
            match database.tag_from_id(tag_id, deleted_rule) {
                Ok(tag) => {
                    if deleted_rule == DeletedRule::Include && !tag.deleted {
                        continue;
                    }
                    tags.push(tag);
                }
                Err(DatabaseError::MissingTag) => {}
                Err(other) => return Err(other.into()),
            }
        }

        Ok(SearchResults { files, tags })
    }

    /// Get a single file's [`FileInfo`] by id, or [`ApiError::UnknownId`] if no
    /// such file exists. The by-id read that replaces scanning a full listing
    /// (used by `tagsy edit`/`download` to find one file's metadata). Backed
    /// by `CatalogStore::file_info_from_id`.
    ///
    /// `deleted_rule` governs tombstone visibility: `Exclude` treats a
    /// tombstoned file as `UnknownId` (the standard behavior for pickers and
    /// operational lookups); `Include` returns it with `FileInfo::deleted =
    /// true`, so a detail screen opened from a "search deleted" result can
    /// still render its metadata.
    pub fn get_file(
        &self,
        file_id: FileId,
        deleted_rule: DeletedRule,
    ) -> Result<FileInfo, ApiError> {
        let database = self.open_read()?;
        Ok(database.file_info_from_id(file_id, deleted_rule)?)
    }

    /// Get a single tag by id, or [`ApiError::UnknownId`] if no such tag
    /// exists. Backed by `CatalogStore::tag_from_id`. See
    /// [`Self::get_file`] for the `deleted_rule` semantics.
    pub fn get_tag(&self, tag_id: TagId, deleted_rule: DeletedRule) -> Result<Tag, ApiError> {
        let database = self.open_read()?;
        Ok(database.tag_from_id(tag_id, deleted_rule)?)
    }

    /// Parse a free-form query string into resolved [`QueryTerm`]s.
    ///
    /// Two stages: [`token::lex_query`] tokenises the string into [`Token`]s
    /// (pure, no DB access — see the [`token`] module docs for the grammar and
    /// error-recovery contract), then this function resolves each token into
    /// one [`QueryTerm`], expanding tag references via
    /// [`CatalogStore::tag_ids_matching_pattern`] (name-or-id, for `/t`, `/e`,
    /// and a bare token), [`CatalogStore::tag_ids_matching_id_prefix`] (id
    /// only, for `/T`), [`CatalogStore::file_ids_matching_id_prefix`] (id only,
    /// for `/i`, `/e`, and the id half of a bare token), and
    /// [`CatalogStore::file_ids_matching_content_hash_prefix`] (for `/h`).
    ///
    /// The lexer stage is forgiving: it silently drops malformed tokens (see
    /// its module docs). The only fallible step here is the tag/file-id
    /// resolution, which can surface a real database error; that is propagated
    /// as-is.
    ///
    /// `deleted_rule` is forwarded to
    /// [`CatalogStore::tag_ids_matching_pattern`] so a search that wants to
    /// see deleted rows can still resolve tokens that only match tombstoned
    /// tags.
    ///
    /// [`Token`]: token::Token
    fn parse_query(
        database: &CatalogStore,
        query: &str,
        deleted_rule: DeletedRule,
    ) -> Result<Vec<QueryTerm>, ApiError> {
        use token::{TokenKind, lex_query};

        let mut terms = Vec::new();
        for token in lex_query(query) {
            // The delimiter the user chose decides how the text half is
            // interpreted, independently of the kind prefix.
            let pattern = if token.regex {
                TextPattern::Regex(token.text)
            } else {
                TextPattern::Substring(token.text)
            };

            // Resolved before the match so the pattern can be moved into the
            // term afterwards. Only the id/tag-bearing kinds need each lookup,
            // and these are the only fallible steps here.
            //
            // `/t` and a bare token resolve tags by name-or-id
            // (`tag_ids_matching_pattern`); `/T` resolves tags by id only. Id
            // resolution runs against the raw payload text, never a regex — ids
            // are opaque hex — so it is skipped for a regex payload (a `%...%`
            // token then has empty id sets, and only its text side stands).
            let tag_ids = match token.kind {
                // `/e` resolves tags the same way a bare token does — name
                // substring **or** id prefix — because on the tag side "the
                // entity's own identity" *is* its name-or-id. The difference
                // from `Any` is only in how the resolved set is used
                // downstream (no subtag expansion).
                TokenKind::Tag | TokenKind::Any | TokenKind::Entity => {
                    database.tag_ids_matching_pattern(&pattern, deleted_rule)?
                }
                TokenKind::TagId => match &pattern {
                    TextPattern::Substring(text) => {
                        database.tag_ids_matching_id_prefix(text, deleted_rule)?
                    }
                    TextPattern::Regex(_) => Vec::new(),
                },
                _ => Vec::new(),
            };
            // File-id resolution serves `/i` (by id), `/h` (by content hash),
            // the id half of a bare token, and the id half of `/e`. All resolve
            // a *set of file ids* and never a regex — ids and hashes are opaque
            // hex.
            let file_ids = match (token.kind, &pattern) {
                (
                    TokenKind::FileId | TokenKind::Any | TokenKind::Entity,
                    TextPattern::Substring(text),
                ) => database.file_ids_matching_id_prefix(text, deleted_rule)?,
                (TokenKind::ContentHash, TextPattern::Substring(text)) => {
                    database.file_ids_matching_content_hash_prefix(text, deleted_rule)?
                }
                _ => Vec::new(),
            };

            let term = match (token.kind, token.negated) {
                // `/T` (tag by id only) resolves into the same HasTag/NotTag
                // terms as `/t`; the difference lived entirely in resolution.
                (TokenKind::Tag | TokenKind::TagId, false) => QueryTerm::HasTag(tag_ids),
                (TokenKind::Tag | TokenKind::TagId, true) => QueryTerm::NotTag(tag_ids),
                // `/i` and `/h` both resolve to a file-id set, so they share the
                // `FileIdMatches`/`NotFileIdMatches` terms; the difference lived
                // entirely in how `file_ids` above was resolved.
                (TokenKind::FileId | TokenKind::ContentHash, false) => {
                    QueryTerm::FileIdMatches(file_ids)
                }
                (TokenKind::FileId | TokenKind::ContentHash, true) => {
                    QueryTerm::NotFileIdMatches(file_ids)
                }
                (TokenKind::Name, false) => QueryTerm::NameMatches(pattern),
                (TokenKind::Name, true) => QueryTerm::NotNameMatches(pattern),
                (TokenKind::Logical, false) => QueryTerm::LogicalMatches(pattern),
                (TokenKind::Logical, true) => QueryTerm::NotLogicalMatches(pattern),
                (TokenKind::Any, false) => QueryTerm::AnyMatch(pattern, tag_ids, file_ids),
                (TokenKind::Any, true) => QueryTerm::NotAnyMatch(pattern, tag_ids, file_ids),
                // `/e`: name/path **or** id, never tag membership. The
                // resolved sets are the same as a bare token's; the
                // membership-free semantics live in the evaluator.
                (TokenKind::Entity, false) => QueryTerm::EntityMatches(pattern, tag_ids, file_ids),
                (TokenKind::Entity, true) => {
                    QueryTerm::NotEntityMatches(pattern, tag_ids, file_ids)
                }
            };
            terms.push(term);
        }
        Ok(terms)
    }

    /// List the subtags of `tag_id` (its children in the tag hierarchy).
    /// `subtag_rule` controls whether the hierarchy is walked transitively.
    /// Backed by `CatalogStore::subtag_ids_for_tag`.
    pub fn subtags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        let database = self.open_read()?;
        Ok(database
            .subtag_ids_for_tag(tag_id, subtag_rule)?
            .into_iter()
            .collect())
    }

    /// List the tags applied to `tag_id` (the tags it is a subtag of) — the tag
    /// analogue of [`tags_for_file`](Self::tags_for_file). `subtag_rule`
    /// controls whether the hierarchy is walked transitively. Backed by
    /// `CatalogStore::tag_ids_for_subtag`.
    pub fn tags_for_tag(
        &self,
        tag_id: TagId,
        subtag_rule: SubtagRule,
    ) -> Result<Vec<TagId>, ApiError> {
        let database = self.open_read()?;
        Ok(database
            .tag_ids_for_subtag(tag_id, subtag_rule)?
            .into_iter()
            .collect())
    }

    /// Report how much data this device stores locally versus how much the
    /// whole catalog holds. See [`StorageStats`].
    ///
    /// This is async because the "stored locally" half lives in the per-sync-
    /// directory indexes owned by the directory-manager actor: we ask it for
    /// the set of materialized file ids (mirroring
    /// [`ApiService::local_path_for_file`]), then price that set — and the
    /// whole catalog — against the catalog's latest-version sizes over a fresh
    /// read handle. Both totals exclude tombstoned files.
    pub async fn storage_stats(&self) -> Result<StorageStats, ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.command_sender
            .send(SyncDirectoryCommand::LocalFileIds { respond_to })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;
        let local_ids = response
            .await
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;

        let local_ids: Vec<FileId> = local_ids.into_iter().collect();
        let database = self.open_read()?;
        let (total_bytes, total_files) = database.total_catalog_size(DeletedRule::Exclude)?;
        let (local_bytes, local_files) =
            database.size_of_files(&local_ids, DeletedRule::Exclude)?;

        Ok(StorageStats {
            local_bytes,
            total_bytes,
            local_files,
            total_files,
        })
    }

    /// Snapshot the sync directories this device is currently serving, each
    /// carrying its absolute path and
    /// [`SyncType`](crate::configuration::SyncType).
    ///
    /// Async because the authoritative set lives in the sync-directory actor,
    /// not on this handle: we round-trip a
    /// [`SyncDirectoryCommand::ListDirectories`] over a oneshot (mirroring
    /// [`ApiService::storage_stats`]). The result reflects live actor state
    /// — directories whose setup failed at startup are already excluded —
    /// rather than the possibly-stale startup configuration. The backup
    /// builder uses it to derive per-directory DB paths and to record
    /// each directory in the archive manifest.
    pub async fn sync_directories(&self) -> Result<Vec<SyncDirectory>, ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.command_sender
            .send(SyncDirectoryCommand::ListDirectories { respond_to })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;
        response
            .await
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))
    }

    /// Re-apply the configured tag rules to the files already in the catalog.
    ///
    /// Rules normally run once, when this device creates a file (see
    /// [`crate::configuration::TagRule`]), so adding or fixing a rule has no
    /// effect on anything that already exists. This is the escape hatch, and
    /// the reason a broken rule does not need to be a fatal startup error:
    /// whatever a rule failed to tag while it was missing or misspelled can be
    /// tagged afterwards.
    ///
    /// # Additive only
    ///
    /// Tags are only ever added, never removed — not even for a file that a
    /// rule *no longer* matches. Nothing records whether a given tag came from
    /// a rule or from a person, so "remove tags this rule would no longer
    /// assign" cannot be distinguished from "delete the user's manual
    /// tagging". Editing a regex must not be able to destroy data.
    ///
    /// # Why this is not a bulk database operation
    ///
    /// The work is a *read* here plus ordinary [`Change::FileTagged`] messages
    /// on the ingest bus — exactly what [`Self::tag_file`] produces. Two
    /// reasons. Iterating the catalog inside `handle_changes` (the sole DB
    /// writer) would stall every other ingestion for the duration, which on a
    /// large catalog means sync visibly freezes. And routing through the
    /// normal change pipeline means retagging inherits last-writer-wins
    /// semantics, peer propagation, and `plan_placement` — so a file
    /// that gains a tag actually gets copied into the `TagBased` directories
    /// that now want it — rather than reimplementing all three.
    ///
    /// The consequence is that the returned summary describes work
    /// *enqueued*, not yet applied. Tagging is idempotent, so a re-run after a
    /// partial application is safe and simply enqueues less.
    ///
    /// [`Change::FileTagged`]: tagsy_core::state::Change::FileTagged
    pub fn retag(&self, dry_run: bool) -> Result<RetagSummary, ApiError> {
        let mut summary = RetagSummary::default();

        // Read the whole plan under one handle, then release it before
        // enqueuing: a rule matching every file would otherwise hold a read
        // handle open across thousands of sends.
        let plan = {
            let database = self.open_read()?;

            // Tombstoned files are skipped: tagging a deleted file changes
            // nothing a user can see and would resurrect the relationship in
            // every peer's catalog for no reason.
            let files = database.get_all_files(DeletedRule::Exclude)?;
            summary.files_scanned = files.len();

            let mut plan: Vec<(FileId, TagId)> = Vec::new();
            for file in files {
                let wanted = self.tag_rules.tags_for(&file.logical_path);
                if wanted.is_empty() {
                    continue;
                }

                // Only read the file's current tags once we know a rule
                // matched; for a narrow rule this skips the query entirely on
                // almost every file.
                let existing: Vec<TagId> = database
                    .tag_ids_for_file(file.file_id, SubtagRule::Exclude)?
                    .into_iter()
                    .collect();

                let missing = wanted.into_iter().filter(|tag| !existing.contains(tag));
                let before = plan.len();
                plan.extend(missing.map(|tag_id| (file.file_id, tag_id)));
                if plan.len() > before {
                    summary.files_changed += 1;
                }
            }
            plan
        };

        summary.tags_applied = plan.len();
        if dry_run {
            return Ok(summary);
        }

        for (file_id, tag_id) in plan {
            self.tag_file(tag_id, file_id)?;
        }

        Ok(summary)
    }

    /// Diagnose the configured tag rules: which failed to compile, and which
    /// name a tag that does not exist.
    ///
    /// The tag check is deliberately made here rather than at startup.
    /// [`crate::configuration::Configuration::tags`] is a floor, not the set of
    /// all tags — a tag created through the UI or synced from a peer is equally
    /// real — so the only meaningful place to ask "does this tag exist?" is
    /// against the live database, on demand.
    pub fn tag_rule_report(&self) -> Result<TagRuleReport, ApiError> {
        let database = self.open_read()?;

        let mut unknown_tags = Vec::new();
        for tag_id in self.tag_rules.referenced_tags() {
            if !database.tag_exists(tag_id)? {
                unknown_tags.push(tag_id);
            }
        }

        Ok(TagRuleReport {
            active: self.tag_rules.len(),
            // Rendered here so the wire type does not have to carry
            // `regex::Error`, which is not serializable.
            invalid: self
                .tag_rules
                .errors()
                .iter()
                .map(ToString::to_string)
                .collect(),
            unknown_tags,
        })
    }

    /// Resolve `file_id` to the absolute on-disk path where its bytes currently
    /// live locally, or `None` if no sync directory holds it. Read-only.
    ///
    /// Used by `tagsy edit` to detect the "already local" case and open the
    /// real file in place (the watcher then propagates the save).
    pub async fn local_path_for_file(
        &self,
        file_id: FileId,
    ) -> Result<Option<std::path::PathBuf>, ApiError> {
        let (respond_to, response) = oneshot::channel();
        self.command_sender
            .send(SyncDirectoryCommand::LocalPath {
                file_id,
                respond_to,
            })
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))?;
        response
            .await
            .map_err(|_| ApiError::Internal("runtime is shutting down".to_owned()))
    }
}

/// Resolve a `term` to a single [`FileId`] against `database`. The core of
/// [`ApiService::resolve_file_id`] — a free function so it is testable against
/// a bare [`CatalogStore`] without standing up the full actor system. See the
/// method for the tiering and error semantics.
fn resolve_file_id(
    database: &CatalogStore,
    term: &str,
    deleted_rule: DeletedRule,
) -> Result<FileId, ApiError> {
    // Tier 1: exact logical-path match (live-only). A deleted file cannot be
    // found here, but the union tier below still catches it by exact
    // name-as-substring when `deleted_rule` is `Include`.
    if deleted_rule == DeletedRule::Exclude
        && let Ok(file_id) = database.file_id_from_logical_path(&LogicalPath::new(term))
    {
        return Ok(file_id);
    }

    // Tier 2: the `/e` union — path substring ∪ id prefix, no tag membership.
    // Build the same term `parse_query` would for `/e <term>`.
    let file_ids = database.file_ids_matching_id_prefix(term, deleted_rule)?;
    let terms = [QueryTerm::EntityMatches(
        TextPattern::Substring(term.to_owned()),
        Vec::new(),
        file_ids,
    )];
    let mut matches = database
        .file_ids_for_query(&terms, SubtagRule::Exclude, deleted_rule)?
        .into_iter();
    match (matches.next(), matches.next()) {
        (None, _) => Err(ApiError::UnknownId),
        (Some(file_id), None) => Ok(file_id),
        (Some(_), Some(_)) => Err(ApiError::AmbiguousId(term.to_owned())),
    }
}

/// Resolve a `term` to a single [`TagId`] against `database`. The tag
/// counterpart of [`resolve_file_id`]; the core of
/// [`ApiService::resolve_tag_id`].
fn resolve_tag_id(
    database: &CatalogStore,
    term: &str,
    deleted_rule: DeletedRule,
) -> Result<TagId, ApiError> {
    // Tier 1: exact name (live-only), for the same reason as the file side.
    if deleted_rule == DeletedRule::Exclude
        && let Ok(tag_id) = database.tag_id_from_name(term)
    {
        return Ok(tag_id);
    }

    // Tier 2: the `/e` union — name substring ∪ id prefix, no subtags.
    // `tag_ids_matching_pattern` already unions name-substring with id-prefix,
    // exactly the resolved set `parse_query` builds for `/e`.
    let tag_ids = database
        .tag_ids_matching_pattern(&TextPattern::Substring(term.to_owned()), deleted_rule)?;
    let terms = [QueryTerm::EntityMatches(
        TextPattern::Substring(term.to_owned()),
        tag_ids,
        Vec::new(),
    )];
    let mut matches = database
        .tag_ids_for_query(&terms, SubtagRule::Exclude, deleted_rule)?
        .into_iter();
    match (matches.next(), matches.next()) {
        (None, _) => Err(ApiError::UnknownId),
        (Some(tag_id), None) => Ok(tag_id),
        (Some(_), Some(_)) => Err(ApiError::AmbiguousId(term.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::{FileId, LogicalPath, TagId, TagStyle};

    use super::{ApiError, resolve_file_id, resolve_tag_id};
    use crate::clock::now_millis;
    use crate::store::{CatalogStore, DeletedRule};

    fn memory_db() -> CatalogStore {
        CatalogStore::initialize(":memory:").expect("open in-memory db")
    }

    fn dot_style(color: &str) -> TagStyle {
        TagStyle {
            dot_color: color.to_owned(),
            ..TagStyle::default()
        }
    }

    fn file_id_from_hex(hex: &str) -> FileId {
        FileId::from_string(hex).expect("valid hex uuid")
    }

    fn tag_id_from_hex(hex: &str) -> TagId {
        TagId::from_string(hex).expect("valid hex uuid")
    }

    fn add_file(database: &mut CatalogStore, id: FileId, path: &str) {
        database.add_file(id, &LogicalPath::new(path), 0).unwrap();
        database.record_version(id, "hash", "local", 1).unwrap();
    }

    /// Any id prefix resolves, of any length — the short id has no special
    /// standing. `1`, a mid-length prefix, and the full id all pick the same
    /// file as long as they stay unique.
    #[test]
    fn resolve_file_by_id_prefix_of_any_length() {
        let mut database = memory_db();
        let id = file_id_from_hex("1234abcd00000000000000000000000f");
        add_file(&mut database, id, "some/file.txt");

        for prefix in ["1", "1234", "1234abcd", "1234abcd00000000000000000000000f"] {
            assert_eq!(
                resolve_file_id(&database, prefix, DeletedRule::Exclude).unwrap(),
                id,
                "prefix {prefix} should resolve"
            );
        }
    }

    /// A hyphenated full id resolves (hyphens are stripped by
    /// `normalize_id_prefix`).
    #[test]
    fn resolve_file_by_hyphenated_id() {
        let mut database = memory_db();
        let id = file_id_from_hex("7f3a1b2c4d5e6f708192a3b4c5d6e7f8");
        add_file(&mut database, id, "a.txt");

        assert_eq!(
            resolve_file_id(
                &database,
                "7f3a1b2c-4d5e-6f70-8192-a3b4c5d6e7f8",
                DeletedRule::Exclude
            )
            .unwrap(),
            id
        );
    }

    /// A file resolves by a substring of its logical path.
    #[test]
    fn resolve_file_by_name_substring() {
        let mut database = memory_db();
        let id = file_id_from_hex("ffff000000000000000000000000000f");
        add_file(&mut database, id, "photos/holiday.jpg");

        assert_eq!(
            resolve_file_id(&database, "holiday", DeletedRule::Exclude).unwrap(),
            id
        );
    }

    /// An id prefix short enough to hit two files is ambiguous — expected once
    /// the catalog grows, and reported with the original term.
    #[test]
    fn resolve_file_ambiguous_prefix_reports_term() {
        let mut database = memory_db();
        add_file(
            &mut database,
            file_id_from_hex("abcd000000000000000000000000000a"),
            "a.txt",
        );
        add_file(
            &mut database,
            file_id_from_hex("abcd000000000000000000000000000b"),
            "b.txt",
        );

        assert!(matches!(
            resolve_file_id(&database, "abcd", DeletedRule::Exclude),
            Err(ApiError::AmbiguousId(term)) if term == "abcd"
        ));
    }

    #[test]
    fn resolve_file_no_match_is_unknown() {
        let mut database = memory_db();
        add_file(
            &mut database,
            file_id_from_hex("aaaa000000000000000000000000000a"),
            "a.txt",
        );

        assert!(matches!(
            resolve_file_id(&database, "no-such-thing", DeletedRule::Exclude),
            Err(ApiError::UnknownId)
        ));
    }

    /// The exact-path tier wins over a substring: `report.txt` resolves to the
    /// file with that exact path even though `report.txt.bak` also contains it.
    #[test]
    fn resolve_file_exact_path_beats_substring() {
        let mut database = memory_db();
        let exact = file_id_from_hex("1111000000000000000000000000001a");
        let superstring = file_id_from_hex("2222000000000000000000000000002b");
        add_file(&mut database, exact, "report.txt");
        add_file(&mut database, superstring, "report.txt.bak");

        assert_eq!(
            resolve_file_id(&database, "report.txt", DeletedRule::Exclude).unwrap(),
            exact,
            "the exact path must win over the substring superstring"
        );
    }

    /// A deleted file cannot be resolved under `Exclude`, but is found under
    /// `Include` (the restore path).
    #[test]
    fn resolve_file_deleted_only_under_include() {
        let mut database = memory_db();
        let id = file_id_from_hex("dead000000000000000000000000000d");
        add_file(&mut database, id, "gone.txt");
        assert!(database.remove_file(id, now_millis() + 10_000).unwrap());

        assert!(matches!(
            resolve_file_id(&database, "gone.txt", DeletedRule::Exclude),
            Err(ApiError::UnknownId)
        ));
        assert_eq!(
            resolve_file_id(&database, "gone.txt", DeletedRule::Include).unwrap(),
            id
        );
    }

    /// The tag exact-name tier wins over a substring: `photo` resolves to the
    /// tag named exactly `photo`, not the ambiguous {photo, photography} set.
    #[test]
    fn resolve_tag_exact_name_beats_substring() {
        let database = memory_db();
        let photo = tag_id_from_hex("1111000000000000000000000000001a");
        let photography = tag_id_from_hex("2222000000000000000000000000002b");
        database
            .add_tag(photo, "photo", &dot_style("red"), 1)
            .unwrap();
        database
            .add_tag(photography, "photography", &dot_style("red"), 1)
            .unwrap();

        assert_eq!(
            resolve_tag_id(&database, "photo", DeletedRule::Exclude).unwrap(),
            photo
        );
    }

    /// A tag resolves by id prefix of any length, and by name substring.
    #[test]
    fn resolve_tag_by_id_prefix_and_name() {
        let database = memory_db();
        let id = tag_id_from_hex("1234abcd00000000000000000000000f");
        database
            .add_tag(id, "important", &dot_style("red"), 1)
            .unwrap();

        assert_eq!(
            resolve_tag_id(&database, "12", DeletedRule::Exclude).unwrap(),
            id
        );
        assert_eq!(
            resolve_tag_id(&database, "portan", DeletedRule::Exclude).unwrap(),
            id
        );
    }

    /// A name substring matching two tags (neither exactly) is ambiguous.
    #[test]
    fn resolve_tag_ambiguous_substring_reports_term() {
        let database = memory_db();
        database
            .add_tag(TagId::new(), "draft-a", &dot_style("red"), 1)
            .unwrap();
        database
            .add_tag(TagId::new(), "draft-b", &dot_style("red"), 1)
            .unwrap();

        assert!(matches!(
            resolve_tag_id(&database, "draft", DeletedRule::Exclude),
            Err(ApiError::AmbiguousId(term)) if term == "draft"
        ));
    }
}
