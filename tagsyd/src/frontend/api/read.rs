//! Read half of the API: resolution, lookup, traversal and search.
//!
//! Every method here opens its own short-lived read-only [`CatalogStore`]
//! handle (see [`ApiService::open_read`]) and drops it before returning; a
//! `&CatalogStore` is never held across an `.await`.

use tagsy_core::{FileId, FileInfo, TagId};
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
    /// Resolve a full-or-short file id `prefix` (as displayed by `list_files`'s
    /// short ids, or a pasted full id) to a single [`FileId`]. Backed by
    /// `CatalogStore::resolve_file_id_prefix`.
    ///
    /// Returns [`ApiError::UnknownId`] if nothing matches and
    /// [`ApiError::AmbiguousId`] if more than one file matches.
    pub fn resolve_file_id(&self, prefix: &str) -> Result<FileId, ApiError> {
        let database = self.open_read()?;
        Ok(database.resolve_file_id_prefix(prefix)?)
    }

    /// Resolve a full-or-short tag id `prefix` (as displayed by `list_tags`'s
    /// short ids, or a pasted full id) to a single [`TagId`]. The tag
    /// counterpart of [`resolve_file_id`](Self::resolve_file_id). Backed by
    /// `CatalogStore::resolve_tag_id_prefix`.
    ///
    /// Returns [`ApiError::UnknownId`] if nothing matches and
    /// [`ApiError::AmbiguousId`] if more than one tag matches.
    pub fn resolve_tag_id(&self, prefix: &str) -> Result<TagId, ApiError> {
        let database = self.open_read()?;
        Ok(database.resolve_tag_id_prefix(prefix)?)
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
    /// - `/t foo` — require the tag(s) resolved from `foo`. A file matches if
    ///   it carries any such tag; a tag matches if it is a subtag of any.
    /// - `/l foo` — case-insensitive substring against the file's logical path
    ///   (or the tag's name on the tag side).
    /// - `/p foo` — reserved for physical-path search; currently a no-op.
    /// - `foo` (no prefix) — matches on *either* side: logical/name substring
    ///   OR tag membership. This is the "just find anything that looks like
    ///   `foo`" token.
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
    /// [`CatalogStore::tag_ids_matching_pattern`].
    ///
    /// Both stages are forgiving:
    /// - the lexer silently drops malformed tokens (see its module docs);
    /// - this resolver silently drops any [`TokenKind::Physical`] token, since
    ///   physical-path search is not wired up yet — the grammar accepts `/p` so
    ///   users see consistent parsing, but the filter is a no-op.
    ///
    /// The only remaining fallible step is `tag_ids_matching_pattern`, which
    /// can surface a real database error; that is propagated as-is.
    ///
    /// `deleted_rule` is forwarded to
    /// [`CatalogStore::tag_ids_matching_pattern`] so a search that wants to
    /// see deleted rows can still resolve tokens that only match tombstoned
    /// tags.
    ///
    /// [`Token`]: token::Token
    /// [`TokenKind::Physical`]: token::TokenKind::Physical
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
            // term afterwards. Only the tag-bearing kinds need it, and the
            // lookup is the one fallible step here.
            let tag_ids = match token.kind {
                TokenKind::Tag | TokenKind::Any => {
                    database.tag_ids_matching_pattern(&pattern, deleted_rule)?
                }
                _ => Vec::new(),
            };

            let term = match (token.kind, token.negated) {
                (TokenKind::Tag, false) => QueryTerm::HasTag(tag_ids),
                (TokenKind::Tag, true) => QueryTerm::NotTag(tag_ids),
                (TokenKind::Name, false) => QueryTerm::NameMatches(pattern),
                (TokenKind::Name, true) => QueryTerm::NotNameMatches(pattern),
                (TokenKind::Logical, false) => QueryTerm::LogicalMatches(pattern),
                (TokenKind::Logical, true) => QueryTerm::NotLogicalMatches(pattern),
                (TokenKind::Any, false) => QueryTerm::AnyMatch(pattern, tag_ids),
                (TokenKind::Any, true) => QueryTerm::NotAnyMatch(pattern, tag_ids),
                // `/p` is reserved but not yet supported — drop the token so
                // the rest of the query still works, matching the "forgiving
                // search box" contract.
                (TokenKind::Physical, _) => continue,
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
