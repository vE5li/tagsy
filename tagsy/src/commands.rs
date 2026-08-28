//! The clap surface: the top-level [`Arguments`] and the [`Commands`]
//! subcommand enum. Parsing only — dispatch lives in [`crate::run`].

use std::path::PathBuf;

use clap::{Parser, Subcommand};

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
        /// Tags to apply to every uploaded file, each a full id or any
        /// unambiguous short-id prefix of it.
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
    /// Create a tag; prints the newly-minted tag id.
    CreateTag {
        name: String,
        // Hex form (matches the Flutter app's palette, kTagColorPalette), so
        // CLI- and app-created tags render identically.
        #[arg(long, default_value = "#F44336")]
        color: String,
    },
    /// Search files with a free-form query.
    ///
    /// The query is a whitespace-separated list of chunks combined
    /// conjunctively. Each chunk may be prefixed:
    ///
    /// - `/t foo` — require the tag(s) matching `foo`
    /// - `/l foo` — logical-path substring
    /// - `!` — invert the following chunk (e.g. `! /t foo`)
    /// - no prefix — match `foo` as *either* a logical-path substring OR a tag
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
        /// The file to edit, given as a full id or any unambiguous short-id
        /// prefix of it.
        id: String,
    },
    /// Download a file into the current directory, fetching it from a peer
    /// first if it is not present locally.
    #[command(visible_alias = "d")]
    Download {
        /// The file to download, given as a full id or any unambiguous
        /// short-id prefix of it.
        id: String,
    },
    /// Delete a file.
    DeleteFile {
        /// The file to delete, given as a full id or any unambiguous short-id
        /// prefix of it.
        id: String,
    },
    /// Restore a soft-deleted file (best-effort; fails if no source still holds
    /// its bytes).
    RestoreFile {
        /// The deleted file to restore, given as a full id or any unambiguous
        /// short-id prefix of it.
        id: String,
    },
    /// Delete a tag.
    DeleteTag {
        /// The tag to delete (a full id or any unambiguous short-id prefix of
        /// it.
        tag_id: String,
    },
    /// Restore a soft-deleted tag.
    RestoreTag {
        /// The deleted tag to restore (a full id or any unambiguous short-id
        /// prefix of it.
        tag_id: String,
    },
    /// Apply one or more tags to an existing file.
    #[command(visible_alias = "t")]
    Tag {
        /// The file to tag, given as a full id or any unambiguous short-id
        /// prefix of it.
        id: String,
        /// One or more tags to apply, each a full id or any unambiguous
        /// short-id prefix of it.
        #[arg(required = true)]
        tag_ids: Vec<String>,
    },
    /// Remove one or more tags from a file.
    #[command(visible_alias = "ut")]
    Untag {
        /// The file to untag, given as a full id or any unambiguous short-id
        /// prefix of it.
        id: String,
        /// One or more tags to remove, each a full id or any unambiguous
        /// short-id prefix of it.
        #[arg(required = true)]
        tag_ids: Vec<String>,
    },
    /// List the tags applied to a file.
    TagsForFile {
        /// The file to inspect, given as a full id or any unambiguous short-id
        /// prefix of it.
        id: String,
        /// Also include tags reached through the tag hierarchy (the tags this
        /// file's tags are subtags of), walking transitively.
        #[arg(long)]
        include_subtags: bool,
    },
    /// Rename a tag.
    RenameTag {
        /// The tag to rename (a full id or any unambiguous short-id prefix of
        /// it.
        tag_id: String,
        /// The tag's new name.
        name: String,
    },
    /// Change a tag's color.
    SetTagColor {
        /// The tag to recolor (a full id or any unambiguous short-id prefix of
        /// it.
        tag_id: String,
        /// The tag's new color.
        color: String,
    },
    /// Move (rename) a file to a new logical path.
    #[command(visible_alias = "mv")]
    Move {
        /// The file to move, given as a full id or any unambiguous short-id
        /// prefix of it.
        id: String,
        /// The file's new logical path.
        path: String,
    },
    /// Make a tag a subtag of one or more parent tags.
    #[command(visible_alias = "tt")]
    TagTag {
        /// The child tag, given as a full id or any unambiguous short-id prefix
        /// of it.
        child: String,
        /// One or more parent tags to nest the child under, each a full id or
        /// any unambiguous short-id prefix of it.
        #[arg(required = true)]
        parents: Vec<String>,
    },
    /// Remove a tag as a subtag of one or more parent tags.
    #[command(visible_alias = "utt")]
    UntagTag {
        /// The child tag, given as a full id or any unambiguous short-id prefix
        /// of it.
        child: String,
        /// One or more parent tags to detach the child from, each a full id or
        /// any unambiguous short-id prefix of it.
        #[arg(required = true)]
        parents: Vec<String>,
    },
    /// List the subtags (children) of a tag.
    Subtags {
        /// The parent tag, given as a full id or any unambiguous short-id
        /// prefix of it.
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
    /// Purge the daemon's cached file previews, forcing them to regenerate on
    /// demand. Useful after the set of previewable file types changes (e.g. new
    /// PDF/video support). Prints how many cached previews were removed.
    PurgePreviews,
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
