//! The data types that cross the port: read-filter enums, the `Tag` row, the
//! editor rule, and every API result DTO.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tagsy_core::state::Change;
use tagsy_core::tag::MetadataFormat;
use tagsy_core::{FileId, FileInfo, TagId, TagStyle};

/// Whether a read walks the tag hierarchy transitively (`Include`) or looks at
/// only direct relationships (`Exclude`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubtagRule {
    Include,
    Exclude,
}

/// Governs whether soft-deleted (tombstoned) rows are visible to a read.
///
/// Applies to `files_v2.deleted` and `tags_v1.deleted`. Relationship-level
/// tombstones (`entries_v1.deleted`) are always filtered — a search for
/// "deleted files" is about files whose own row is tombstoned, not files that
/// were merely untagged.
///
/// - [`Exclude`](DeletedRule::Exclude): behave as before — every read hides
///   tombstoned rows (`WHERE ... deleted = 0`). This is what every non-search
///   caller wants.
/// - [`Include`](DeletedRule::Include): drop that filter so live *and*
///   tombstoned rows come back together. Search callers that want to show
///   deleted rows to the user use this, then post-filter by the returned
///   `deleted` flag to keep only the tombstoned ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeletedRule {
    Include,
    Exclude,
}

/// A tag as the UI sees it: id, name, its full visual style, optional metadata,
/// and its tombstone flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Tag {
    pub id: TagId,
    pub name: String,
    /// The tag's complete visual style. The old single `color` field is now
    /// `style.dot_color`, one peer property among ten.
    pub style: TagStyle,
    pub metadata: Option<MetadataFormat>,
    /// Whether the tag is soft-deleted (tombstoned). Always `false` when the
    /// row was fetched under [`DeletedRule::Exclude`] (the default). Under
    /// [`DeletedRule::Include`] this may be `true`, letting the caller
    /// distinguish live from tombstoned rows in a mixed result set.
    pub deleted: bool,
}

/// A rule mapping a search query to an external editor command.
///
/// Used by the desktop UI's "edit" action: when a file matches [`query`], its
/// bytes are handed to [`argv`] instead of the generic `$VISUAL`/`$EDITOR`
/// fallback. Rules are consulted in declaration order; the first match wins.
///
/// The daemon does not use these rules itself — it has no notion of external
/// processes — but stores them so every frontend on this device (and any
/// future non-Flutter client of the same daemon) sees the same set. The
/// Android app currently has no external-editor concept and simply ignores
/// them.
///
/// # Security
///
/// A rule is, by construction, "run this program" — arbitrary code execution
/// with the desktop app's privileges. That is the feature, not a flaw, but it
/// makes **write access to the config file equivalent to code execution**, so
/// the config should be owned by the user running the app and not
/// group/world-writable.
///
/// What the config explicitly is *not* is a place where untrusted data lands:
/// editor rules are read once at startup from the local config and are never
/// synced from peers, stored in the database, or mutated at runtime (there is
/// no setter anywhere in the API or control protocol). A malicious peer
/// therefore cannot introduce or alter a rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditorRule {
    /// The search query, in the same syntax the search box accepts (`/t
    /// favorite`, negation, regex, name/path substrings, …). A file matches
    /// the rule when it is in the query's results, so the full filtering
    /// grammar is available rather than a bare tag membership test.
    ///
    /// Keyed by query string rather than tag id on purpose — that is what
    /// buys the full grammar. It also inherits the query path's id stability:
    /// a `/t` term resolves a name to an id up front, so a rule survives a
    /// `rename_tag`, exactly as a tag-id key would have. Mirrors
    /// [`HomeSection::query`]; both run through the daemon's single query
    /// parser.
    ///
    /// To test whether a *specific* file matches, the launcher composes
    /// `/i <file-id> <query>` and asks whether the (at most one) result is
    /// non-empty. The `/i` term goes **first** so it stays a complete,
    /// well-formed token even if the operator's query ends in an unclosed
    /// `"` or `%` — otherwise the trailing id could be swallowed into an
    /// unterminated quote/regex.
    pub query: String,
    /// The editor command as an explicit `argv` vector, e.g.
    /// `["/run/current-system/sw/bin/gimp"]` or
    /// `["/usr/bin/code", "--wait"]`. The file path is appended as the final
    /// argument, and the vector is passed straight to `execvp` — **no shell is
    /// involved**, so quoting, globbing and metacharacters have no meaning
    /// here.
    ///
    /// This is a list rather than a single string on purpose. A string would
    /// have to be split into `argv` by the launcher, and every splitting rule
    /// is either too naive to express an argument containing a space or
    /// complex enough (quotes, escapes) to be worth getting subtly wrong. A
    /// list sidesteps the question: the operator states the argument
    /// boundaries directly.
    ///
    /// `argv[0]` **must be an absolute path**. See the Linux launcher
    /// (`app/lib/editor/linux_editor_launcher.dart`) for the rationale.
    ///
    /// The command must block until the user is done editing (e.g. `gimp`,
    /// `inkscape`, `code --wait`); one that forks and returns immediately
    /// (`xdg-open`, `nohup ...`) will make the UI think the edit finished as
    /// soon as the launch call returns.
    pub argv: Vec<String>,
}

/// A named saved search shown on the desktop UI's home screen.
///
/// Each section pairs a display `name` with a `query` in the same syntax the
/// search box accepts (`/t favorite`, negation, regex, name substrings, …), so
/// a section is just a saved search: the daemon remains the single query parser
/// and every existing filter works unchanged. Keyed by query string rather than
/// tag id on purpose — that is what buys the full filtering grammar instead of
/// a bare tag membership test.
///
/// Like [`EditorRule`], this is config-shaped but crosses the port (the UI
/// reads it over the backend), so it lives here in `tagsy-api`. It is read once
/// at startup, never mutated, and never synced from peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HomeSection {
    /// Human-readable heading rendered above this section's results.
    pub name: String,
    /// The search query, in the same syntax the search box accepts. Run through
    /// the daemon's normal query path, so all filtering (tag terms, negation,
    /// regex, name substrings) applies.
    pub query: String,
}

/// The result of a search: the files and tags matching a query.
///
/// Both lists are matched by the same conjunction of query terms; files by
/// their tags/logical path and tags by their place in the tag hierarchy / their
/// name. Full rows (not bare ids) are returned so callers can render results
/// without a second listing round-trip — the daemon does the id→row join once,
/// over just the matched set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResults {
    pub files: Vec<FileInfo>,
    pub tags: Vec<Tag>,
}

/// How much data this device holds on disk versus how much the whole catalog
/// ("the cloud") knows about.
///
/// Both totals price only the *latest* version of each file — this is the
/// current storage footprint, not the sum of every historical version — and
/// both exclude tombstoned files. `local_*` counts files materialized on this
/// device (present in some sync directory's index); `total_*` counts every live
/// file in the catalog. `local_bytes <= total_bytes` and
/// `local_files <= total_files` always hold.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    pub local_bytes: u64,
    pub total_bytes: u64,
    pub local_files: u64,
    pub total_files: u64,
}

/// The result of a completed `backup`: where the archive landed and how much it
/// covers.
///
/// `path` is the absolute path of the finished `*.tar.zst` in
/// `TAGSY_BACKUP_DIR` (the `.partial` staging name has already been renamed off
/// by the time this is returned). `bytes_written` and `file_count` describe the
/// **sync-directory contents** walked into the archive — the raw bytes seen
/// before compression, not the on-disk archive size, and not counting the two
/// databases or the manifest. They give the CLI a one-line "N files, M bytes"
/// confirmation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupOutcome {
    pub path: PathBuf,
    pub bytes_written: u64,
    pub file_count: u64,
}

/// Did an edit actually change the file?
///
/// `changed = false` means the post-edit bytes hashed to the file's current
/// recorded `content_hash`; either the editor produced no change, or the edit
/// happened in place and the filesystem watcher already published the same
/// content the daemon then saw at `finish_edit` time. `changed = true` means
/// the daemon streamed the new content to peers as a new version. The Dart UI
/// uses this to show a "no changes" hint vs. an "edited" confirmation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditOutcome {
    /// Whether the daemon published a new version from the edited bytes.
    pub changed: bool,
}

/// A live update delivered on the API event stream.
///
/// Delivery is **best-effort**, mirroring the in-process ingest bus. There is
/// no per-event replay or buffering. On (re)connection over IPC the transport
/// emits [`ApiEvent::Resynced`] first; the UI responds by re-fetching current
/// state via the read API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApiEvent {
    /// The stream (re)started; the UI should re-fetch current state. Produced
    /// by the transport layer on connect/reconnect, not by the change bus.
    Resynced,
    /// A change was applied to the store.
    Changed(Change),
    /// A file this connection was temporarily providing (an upload/edit) has
    /// been handed off (a peer completed pulling it); the client may release
    /// the local file. Produced by the control layer, not the change bus.
    ProviderReleased { file_id: FileId },
}

/// What a retag did, or (under `dry_run`) would do.
///
/// Counts describe work *enqueued* onto the ingest bus, not yet applied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetagSummary {
    /// Live (non-tombstoned) files examined.
    pub files_scanned: usize,
    /// Files that were missing at least one tag a rule assigns.
    pub files_changed: usize,
    /// Individual file→tag applications. At least `files_changed`, and more
    /// when a file was missing several tags.
    pub tags_applied: usize,
}

/// Diagnostics for the configured tag rules.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TagRuleReport {
    /// Rules that compiled and are being applied.
    pub active: usize,
    /// One rendered diagnostic per rule that failed to compile. Such a rule is
    /// disabled but never prevented the daemon from starting.
    pub invalid: Vec<String>,
    /// Tag ids named by a live rule that match no tag in the catalog. Usually
    /// a typo; harmless but inert, since the rule can only ever apply a tag
    /// nothing else refers to.
    pub unknown_tags: Vec<TagId>,
}
