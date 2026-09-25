//! The clap surface: the top-level [`Arguments`] and the [`Commands`]
//! subcommand enum. Parsing only — dispatch lives in [`crate::run`].

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// The tag-style flags shared by `create-tag` and `set-tag-style`. Every flag
/// is optional; a flag left unset leaves that property at its existing value
/// (for `set-tag-style`) or its default (for `create-tag`). This mirrors the
/// ten properties of `tagsy_core::TagStyle` — colors are `#RRGGBB[AA]` hex,
/// `--border-style` is `none|solid|dashed`, `--shape` is
/// `rounded|stadium|square|cut_corner`.
#[derive(Debug, Args)]
pub struct StyleArgs {
    /// Leading dot color (hex).
    #[arg(long)]
    pub dot_color: Option<String>,
    /// Pill fill color (hex); default transparent.
    #[arg(long)]
    pub background: Option<String>,
    /// Color the fill fades to, left→right (hex). Equal to background = no
    /// gradient.
    #[arg(long)]
    pub gradient: Option<String>,
    /// Text color (hex).
    #[arg(long)]
    pub foreground: Option<String>,
    /// Border color (hex); default transparent.
    #[arg(long)]
    pub border: Option<String>,
    /// Border stroke width.
    #[arg(long)]
    pub border_width: Option<f64>,
    /// Border stroke style: none, solid, or dashed.
    #[arg(long)]
    pub border_style: Option<String>,
    /// Pill shape: rounded, stadium, square, or cut_corner.
    #[arg(long)]
    pub shape: Option<String>,
    /// Draw a soft drop shadow.
    #[arg(long)]
    pub shadow: Option<bool>,
    /// Drop-shadow color (hex); used when --shadow is on.
    #[arg(long)]
    pub shadow_color: Option<String>,
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Arguments {
    /// Path to the daemon's control socket. Defaults to the fixed
    /// `/run/tagsy/tagsy.sock`; override only for non-standard launches.
    #[arg(long, global = true)]
    pub socket: Option<PathBuf>,
    /// Emit machine-readable JSON instead of human-friendly tables/text.
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Upload files' contents to the daemon, optionally tagging them.
    ///
    /// Each path may be a file or a directory; directories are walked
    /// recursively, uploading every regular file within (symlinks are
    /// skipped). Hidden entries (dotfiles, and anything inside a dotted
    /// directory) are skipped unless `--hidden` is given.
    #[command(visible_alias = "u")]
    Upload {
        /// Files or directories on disk to read and upload. Directories are
        /// walked recursively.
        #[arg(required = true)]
        paths: Vec<PathBuf>,
        /// Tags to apply to every uploaded file, each an id, id prefix, or
        /// name that resolves to a single tag.
        #[arg(long = "tag", value_name = "TAG_ID")]
        tags: Vec<String>,
        /// Keep the local files after uploading (by default each is deleted
        /// once its upload has succeeded).
        #[arg(long = "keep")]
        keep: bool,
        /// Include hidden entries (dotfiles, and files inside dotted
        /// directories) when walking directories.
        #[arg(long = "hidden")]
        hidden: bool,
        /// Confirm uploading a large batch (more than 100 files). Without
        /// this, such uploads are refused to prevent accidents; scripts can
        /// pass it unconditionally.
        #[arg(long = "many")]
        many: bool,
    },
    /// Create a tag; prints the new tag.
    ///
    /// Unset style flags take their defaults (see `StyleArgs`); the dot color
    /// defaults to `#F44336`, matching the Flutter app's palette so CLI- and
    /// app-created tags render identically.
    CreateTag {
        name: String,
        #[command(flatten)]
        style: StyleArgs,
    },
    /// Search files with a free-form query.
    ///
    /// The query is a whitespace-separated list of chunks combined
    /// conjunctively. Each chunk may be prefixed:
    ///
    /// - `/t foo` — require the tag(s) matching `foo` by name or id prefix
    /// - `/T foo` — require the tag(s) matching `foo` by id prefix only
    /// - `/i foo` — require the file(s) whose id starts with `foo`
    /// - `/h foo` — require the file(s) whose content hash starts with `foo`
    /// - `/l foo` — logical-path substring
    /// - `/e foo` — match an entity by its *own* identity: a file by
    ///   logical-path substring OR id prefix, a tag by name OR id prefix —
    ///   never by tag membership (the axis `tagsy` uses to resolve a name/id
    ///   argument to one file/tag)
    /// - `!` — invert the following chunk (e.g. `! /t foo`)
    /// - no prefix — match `foo` as a logical-path substring, a tag, OR the
    ///   file/tag's own id prefix
    ///
    /// Payloads can be written three ways: bare (`foo`), double-quoted to
    /// include whitespace (`"my file"`), or `%`-delimited to make the payload a
    /// regular expression (`%\.md$%`). Regexes are case-insensitive unless the
    /// pattern starts with `(?-i)`, need no escaping of `/`, and compose with
    /// every prefix — `/l %^photos/%`, `/t %^wip-%`, `! %\.tmp$%`.
    ///
    /// Malformed chunks are silently dropped; an invalid regex matches nothing.
    /// Examples:
    ///   `tagsy search '/t photos ! /t archived beach'`
    ///   `tagsy search '/l %^photos/\d{4}/% ! %\.tmp$%'`
    #[command(visible_alias = "s")]
    Search {
        /// The query terms; joined with spaces if given as multiple arguments.
        #[arg(trailing_var_arg = true, required = true)]
        query: Vec<String>,
        /// Also match files carrying any subtag of a `$tag`/`!tag` term,
        /// walking the hierarchy transitively.
        #[arg(long)]
        include_subtags: bool,
        /// Search soft-deleted (tombstoned) files and tags instead of live
        /// ones. Results contain *only* rows whose own tombstone is set;
        /// relationships (which tags a deleted file used to carry, etc.) are
        /// still walked live-only.
        #[arg(long)]
        deleted: bool,
    },
    /// Edit a file in `$EDITOR`, fetching it from a peer first if it is not
    /// present locally, and writing back any changes.
    #[command(visible_alias = "e")]
    Edit {
        /// The file to edit, given as an id, any id prefix, or a name/path
        /// that resolves to a single file.
        id: String,
    },
    /// Download a file into the current directory, fetching it from a peer
    /// first if it is not present locally.
    #[command(visible_alias = "d")]
    Download {
        /// The file to download, given as an id, any id prefix, or a name/path
        /// that resolves to a single file.
        id: String,
    },
    /// Delete a file.
    DeleteFile {
        /// The file to delete, given as an id, any id prefix, or a name/path
        /// that resolves to a single file.
        id: String,
    },
    /// Restore a soft-deleted file (best-effort; fails if no source still holds
    /// its bytes).
    RestoreFile {
        /// The deleted file to restore, given as an id, any id prefix, or a
        /// name/path that resolves to a single deleted file.
        id: String,
    },
    /// Delete a tag.
    DeleteTag {
        /// The tag to delete, given as an id, any id prefix, or a name that
        /// resolves to a single tag.
        tag_id: String,
    },
    /// Restore a soft-deleted tag.
    RestoreTag {
        /// The deleted tag to restore, given as an id, any id prefix, or a name
        /// that resolves to a single deleted tag.
        tag_id: String,
    },
    /// Apply one or more tags to an existing file.
    #[command(visible_alias = "t")]
    Tag {
        /// The file to tag, given as an id, any id prefix, or a name/path that
        /// resolves to a single file.
        id: String,
        /// One or more tags to apply, each an id, id prefix, or name that
        /// resolves to a single tag.
        #[arg(required = true)]
        tag_ids: Vec<String>,
    },
    /// Remove one or more tags from a file.
    #[command(visible_alias = "ut")]
    Untag {
        /// The file to untag, given as an id, any id prefix, or a name/path
        /// that resolves to a single file.
        id: String,
        /// One or more tags to remove, each an id, id prefix, or name that
        /// resolves to a single tag.
        #[arg(required = true)]
        tag_ids: Vec<String>,
    },
    /// List the tags applied to a file.
    TagsForFile {
        /// The file to inspect, given as an id, any id prefix, or a name/path
        /// that resolves to a single file.
        id: String,
        /// Also include tags reached through the tag hierarchy (the tags this
        /// file's tags are subtags of), walking transitively.
        #[arg(long)]
        include_subtags: bool,
    },
    /// Rename a tag.
    RenameTag {
        /// The tag to rename, given as an id, any id prefix, or a name that
        /// resolves to a single tag.
        tag_id: String,
        /// The tag's new name.
        name: String,
    },
    /// Change a tag's visual style. Fetches the tag's current style and
    /// overrides only the flags you pass, so e.g. `--border '#000000'` changes
    /// just the border and leaves the dot color, shape, etc. untouched. Dot
    /// color is one property (`--dot-color`), so this is also how you recolor.
    SetTagStyle {
        /// The tag to restyle, given as an id, any id prefix, or a name that
        /// resolves to a single tag.
        tag_id: String,
        #[command(flatten)]
        style: StyleArgs,
    },
    /// Move (rename) a file to a new logical path.
    #[command(visible_alias = "mv")]
    Move {
        /// The file to move, given as an id, any id prefix, or a name/path
        /// that resolves to a single file.
        id: String,
        /// The file's new logical path.
        path: String,
    },
    /// Make a tag a subtag of one or more parent tags.
    #[command(visible_alias = "tt")]
    TagTag {
        /// The child tag, given as an id, any id prefix, or a name that
        /// resolves to a single tag.
        child: String,
        /// One or more parent tags to nest the child under, each an id, id
        /// prefix, or name that resolves to a single tag.
        #[arg(required = true)]
        parents: Vec<String>,
    },
    /// Remove a tag as a subtag of one or more parent tags.
    #[command(visible_alias = "utt")]
    UntagTag {
        /// The child tag, given as an id, any id prefix, or a name that
        /// resolves to a single tag.
        child: String,
        /// One or more parent tags to detach the child from, each an id, id
        /// prefix, or name that resolves to a single tag.
        #[arg(required = true)]
        parents: Vec<String>,
    },
    /// List the subtags (children) of a tag.
    Subtags {
        /// The parent tag, given as an id, any id prefix, or a name that
        /// resolves to a single tag.
        tag_id: String,
        /// Walk the hierarchy transitively (include subtags of subtags).
        #[arg(long)]
        recursive: bool,
    },
    /// List the daemon's currently-active sync operations (connecting to peers,
    /// sending/receiving files, reconciling, ...).
    #[command(visible_alias = "ops")]
    ListOperations,
    /// List the peers the daemon currently holds a live connection with.
    ///
    /// A connection is state, not an operation, so it has its own command
    /// rather than appearing in `list-operations`.
    #[command(visible_alias = "peers")]
    ConnectedPeers,
    /// Show how much work the daemon currently has queued or in hand: each
    /// actor's inbox, filesystem events still debouncing, and byte transfers.
    /// "idle" means all of them are empty and the startup scan is done.
    Activity,
    /// Purge the daemon's cached file previews, forcing them to regenerate on
    /// demand. Useful after the set of previewable file types changes (e.g. new
    /// PDF/video support). Prints how many cached previews were removed.
    PurgePreviews,
    /// Permanently purge broken files: every file the catalog knows about whose
    /// content is missing from local storage.
    ///
    /// This is DESTRUCTIVE and IRREVERSIBLE. A purge strips the file's catalog
    /// entry, versions, and on-disk bytes, and propagates to every peer, which
    /// do the same — the id is remembered forever so the file can never come
    /// back, even from a peer that later reconnects holding a copy.
    ///
    /// Because "content missing locally" must be an authoritative verdict, this
    /// requires the daemon to have a Universal sync directory (one that is
    /// meant to hold every file's bytes); it refuses to run otherwise.
    /// Always run `--dry-run` first to see exactly what would be purged.
    PurgeBroken {
        /// Report which files would be purged without changing anything.
        #[arg(long)]
        dry_run: bool,
    },
    /// Permanently purge soft-deleted files: every file currently in the
    /// deleted (trashed) state.
    ///
    /// This is DESTRUCTIVE and IRREVERSIBLE. A normal delete is a reversible
    /// tombstone (it can be restored, or resurrected by a newer edit); a purge
    /// is not. Each purged file's catalog entry, versions, and on-disk bytes
    /// are stripped, the purge propagates to every peer, and the id is
    /// remembered forever so the file can never come back. Run `--dry-run`
    /// first to see exactly what would be purged.
    PurgeDeleted {
        /// Report which files would be purged without changing anything.
        #[arg(long)]
        dry_run: bool,
    },
    /// Delete duplicate files: live files with the same logical path and the
    /// same content.
    ///
    /// Of each set of duplicates, the file with the lowest id is kept, so every
    /// device picks the same one. Any tag carried by a deleted duplicate is
    /// added to the kept file, so no tagging is lost. Deletion is the
    /// normal, reversible soft delete (see `restore-file`), and propagates to
    /// every peer. Run `--dry-run` first to see exactly what would change.
    DeleteDuplicates {
        /// Report which files would be deleted without changing anything.
        #[arg(long)]
        dry_run: bool,
    },
    /// Bundle the entire tagsy state (both databases plus every sync
    /// directory's contents) into a single compressed archive in
    /// TAGSY_BACKUP_DIR. Prints where the archive landed.
    Backup,
    /// Re-apply the daemon's configured tag rules to files that already exist.
    ///
    /// Tag rules normally run once, when this device first creates a file, so
    /// adding or fixing a rule leaves everything already in the catalog
    /// untouched. This command catches those files up.
    ///
    /// Only ever *adds* tags. A file that a rule no longer matches keeps the
    /// tags it has.
    ///
    /// The daemon reads its configuration once at startup, so restart it
    /// before running this if you have just edited the rules.
    Retag {
        /// Report what would be tagged without changing anything.
        #[arg(long)]
        dry_run: bool,
        /// Only validate the rules — report invalid patterns and rule tags
        /// that match no known tag — without scanning or tagging any file.
        #[arg(long, conflicts_with = "dry_run")]
        check: bool,
    },
}
