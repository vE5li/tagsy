//! All terminal output.

use std::collections::HashMap;

use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Table};
use owo_colors::OwoColorize;
use serde::Serialize;
use serde_json::json;
use tagsy_api::{
    ConnectedPeer, Direction, DuplicateDeletionOutcome, Operation, OperationKind, OperationStatus,
    PurgeOutcome, Tag,
};
use tagsy_core::{FileId, FileInfo, FileKind, TagId};

/// How command results are rendered to stdout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Human-friendly tables and prose. Can include less information than
    /// the machine-readable output.
    Human,
    /// Machine-readable JSON (one value per command, pretty-printed). Can
    /// include more information than the human-readable output.
    Json,
}

/// The lowercase name of a [`FileKind`] for machine-readable output.
fn file_kind_name(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Image => "image",
        FileKind::Svg => "svg",
        FileKind::Pdf => "pdf",
        FileKind::Video => "video",
        FileKind::Markdown => "markdown",
        FileKind::Text => "text",
        FileKind::Other => "other",
    }
}

/// A serializable file row, shared by every command that prints files.
#[derive(Debug, Serialize)]
struct FileRow {
    id: FileId,
    path: String,
    version: i64,
    content_hash: String,
    /// The file's type, decided from its extension by the daemon's shared
    /// classifier (see [`FileKind`]). Rendered as a lowercase name.
    kind: &'static str,
    size: u64,
    tags: Vec<String>,
    deleted: bool,
}

impl FileRow {
    /// Build a row from a file's info and its tag names.
    fn new(file: &FileInfo, tags: Vec<String>) -> Self {
        Self {
            id: file.file_id,
            path: file.logical_path.to_string(),
            version: file.version_number,
            content_hash: file.content_hash.clone(),
            kind: file_kind_name(file.kind()),
            size: file.size,
            tags,
            deleted: file.deleted,
        }
    }
}

/// A serializable tag row, shared by every command that prints tags.
///
/// `style` carries the tag's full [`tagsy_api::TagStyle`] — every property with
/// equal weight, matching the human table's `Style` column. No single property
/// (dot color included) is promoted to its own field.
#[derive(Debug, Serialize)]
struct TagRow {
    id: TagId,
    name: String,
    style: tagsy_api::TagStyle,
    tags: Vec<String>,
    deleted: bool,
}

impl TagRow {
    /// Build a row from a tag and its applied-tag names.
    fn new(tag: &Tag, tags: Vec<String>) -> Self {
        Self {
            id: tag.id,
            name: tag.name.clone(),
            style: tag.style.clone(),
            tags,
            deleted: tag.deleted,
        }
    }
}

/// The [`FileRow`] for `file`, with its tag names from `tags_by_file` (none if
/// absent).
fn file_row(file: &FileInfo, tags_by_file: &HashMap<FileId, Vec<String>>) -> FileRow {
    FileRow::new(
        file,
        tags_by_file.get(&file.file_id).cloned().unwrap_or_default(),
    )
}

/// [`file_row`] for each of `files`.
fn file_rows(files: &[FileInfo], tags_by_file: &HashMap<FileId, Vec<String>>) -> Vec<FileRow> {
    files
        .iter()
        .map(|file| file_row(file, tags_by_file))
        .collect()
}

/// Print a serializable value as pretty JSON to stdout.
pub fn print_json(value: &impl Serialize) {
    match serde_json::to_string_pretty(value) {
        Ok(text) => println!("{text}"),
        Err(error) => eprintln!("{{\"error\":\"failed to serialize output: {error}\"}}"),
    }
}

/// Emit a one-shot scalar result: a human sentence in [`OutputMode::Human`], a
/// JSON value in [`OutputMode::Json`].
///
/// Only for commands whose result is not an entry — counts and status
/// (`Purged N previews`, `backup`, `activity`, `retag`). A command that
/// touches files or tags prints them with [`emit_files`] / [`emit_tags`]
/// instead, so every such command looks the same. Routing these here keeps the
/// two renderings adjacent so they can't drift.
///
/// `human` is computed by the caller (usually a `format!`); `json` is any
/// serializable value (typically a `serde_json::json!({..})`). Both are always
/// evaluated — these are cheap confirmation payloads, so the small waste of
/// building the unused side is not worth a closure or macro.
pub fn emit_scalar(output_mode: OutputMode, human: impl AsRef<str>, json: serde_json::Value) {
    match output_mode {
        OutputMode::Human => println!("{}", human.as_ref()),
        OutputMode::Json => print_json(&json),
    }
}

/// Emit the result of a purge command (`purge-broken` / `purge-deleted`): a
/// dry-run-vs-applied header over the shared [`file_table`] of the files
/// purged, or the equivalent JSON object with [`FileRow`]s. `noun` names the
/// class of files purged ("broken", "deleted") so the one renderer serves each
/// command without drift. `tags_by_file` supplies each file's tag names, as in
/// [`emit_files`].
pub fn emit_purge_outcome(
    output_mode: OutputMode,
    noun: &str,
    outcome: &PurgeOutcome,
    tags_by_file: &HashMap<FileId, Vec<String>>,
) {
    let files: Vec<FileInfo> = outcome
        .purged
        .iter()
        .map(|purged| purged.file.clone())
        .collect();
    let count = files.len();

    match output_mode {
        OutputMode::Human => {
            if count == 0 {
                if outcome.dry_run {
                    println!("No {noun} files found; nothing would be purged");
                } else {
                    println!("No {noun} files found; nothing purged");
                }
                return;
            }
            if outcome.dry_run {
                println!("{count} {noun} file(s) would be purged (dry run, nothing changed):");
            } else {
                println!("Permanently purged {count} {noun} file(s):");
            }
            println!("{}", file_table(&files, tags_by_file));
        }
        OutputMode::Json => print_json(&json!({
            "dry_run": outcome.dry_run,
            "count": count,
            "purged": file_rows(&files, tags_by_file),
        })),
    }
}

/// The tag names [`emit_duplicate_deletion_outcome`] shows: every member's
/// direct tags, and per group (in order) the names of the tags merged onto
/// the survivor.
pub struct DuplicateGroupTags {
    pub files: HashMap<FileId, Vec<String>>,
    pub merged: Vec<Vec<String>>,
}

/// Emit the result of `delete-duplicates`: the survivors and the deleted
/// duplicates, each as a shared [`file_table`], or the equivalent JSON object
/// with one entry per duplicate set. `count` is the number of files deleted.
///
/// A survivor's Tags column shows its tags as they stand. On a dry run the
/// merge has not happened yet, so the tags it would gain are appended with a
/// leading `+`.
pub fn emit_duplicate_deletion_outcome(
    output_mode: OutputMode,
    outcome: &DuplicateDeletionOutcome,
    tags: &DuplicateGroupTags,
) {
    let count: usize = outcome.groups.iter().map(|group| group.deleted.len()).sum();
    let merged = |index: usize| tags.merged.get(index).cloned().unwrap_or_default();

    match output_mode {
        OutputMode::Human => {
            if outcome.groups.is_empty() {
                if outcome.dry_run {
                    println!("No duplicate files found; nothing would be deleted");
                } else {
                    println!("No duplicate files found; nothing deleted");
                }
                return;
            }
            let sets = outcome.groups.len();
            if outcome.dry_run {
                println!(
                    "{count} duplicate file(s) in {sets} set(s) would be deleted (dry run, \
                     nothing changed):"
                );
            } else {
                println!("Deleted {count} duplicate file(s) in {sets} set(s):");
            }

            let kept: Vec<FileInfo> = outcome
                .groups
                .iter()
                .map(|group| group.kept.clone())
                .collect();
            let mut kept_tags = tags.files.clone();
            if outcome.dry_run {
                for (index, group) in outcome.groups.iter().enumerate() {
                    kept_tags
                        .entry(group.kept.file_id)
                        .or_default()
                        .extend(merged(index).into_iter().map(|name| format!("+{name}")));
                }
            }
            let deleted: Vec<FileInfo> = outcome
                .groups
                .iter()
                .flat_map(|group| group.deleted.iter().cloned())
                .collect();

            println!("Kept:");
            println!("{}", file_table(&kept, &kept_tags));
            println!("Deleted:");
            println!("{}", file_table(&deleted, &tags.files));
        }
        OutputMode::Json => {
            let groups: Vec<serde_json::Value> = outcome
                .groups
                .iter()
                .enumerate()
                .map(|(index, group)| {
                    json!({
                        "logical_path": group.logical_path.as_str(),
                        "content_hash": group.content_hash,
                        "kept": file_row(&group.kept, &tags.files),
                        "deleted": file_rows(&group.deleted, &tags.files),
                        "tags_merged": merged(index),
                    })
                })
                .collect();
            print_json(&json!({
                "dry_run": outcome.dry_run,
                "count": count,
                "groups": groups,
            }));
        }
    }
}

/// Number of leading characters needed to uniquely identify `target` among
/// `all` ids (jj-style short change ids).
fn unique_prefix_length(target: &str, all: &[String]) -> usize {
    for length in 1..=target.len() {
        let prefix = &target[..length];
        let collisions = all
            .iter()
            .filter(|other| other.as_str() != target && other.starts_with(prefix))
            .count();

        if collisions == 0 {
            return length;
        }
    }

    target.len()
}

/// Render an id with its unique prefix highlighted and the remainder
/// dimmed, mirroring how `jj` displays change ids.
fn highlight_id(id: &str, prefix_length: usize) -> String {
    let (unique, rest) = id.split_at(prefix_length.min(id.len()));
    format!("{}{}", unique.magenta().bold(), rest.bright_black())
}

/// Render a tag's full [`TagStyle`] as a compact `key=value` list for the
/// table.
///
/// Every one of the ten style properties is shown, in the same order and with
/// equal weight — the CLI does not single out the dot color (or any other
/// property) with its own column. Kept to a single cell so the table stays
/// legible; the machine-readable JSON path emits the same properties as a
/// structured object.
fn format_style(style: &tagsy_api::TagStyle) -> String {
    format!(
        "dot={} bg={} grad={} fg={} border={} width={} border_style={} shape={} shadow={} \
         shadow_color={}",
        style.dot_color,
        style.background,
        style.gradient,
        style.foreground,
        style.border,
        style.border_width,
        style.border_style.as_str(),
        style.shape.as_str(),
        style.shadow,
        style.shadow_color,
    )
}

/// The `Deleted` column of the file and tag tables: whether the entry is
/// tombstoned (soft-deleted, still restorable).
fn deleted_label(deleted: bool) -> &'static str {
    if deleted { "yes" } else { "no" }
}

/// The single tag table used by *every* command that prints tags — listings
/// (`search`, `tags-for-file`, `subtags`) and every tag mutation alike.
///
/// Short-id prefixes are highlighted the way `jj`/`git` show change ids.
/// The prefix length is computed against `tags`, so pass the full set
/// you intend to display; the highlighted prefix is a valid lookup key
/// for the tag commands.
///
/// The `Tags` column shows the tags applied to each tag (the tags it is a
/// subtag of), the tag analogue of the file table's per-file tags. The
/// `Deleted` column tells a tombstoned tag from a live one, as in the file
/// table.
/// `tags_by_tag` supplies those names; a tag absent from the map renders
/// with an empty column.
fn tag_table(tags: &[Tag], tags_by_tag: &HashMap<TagId, Vec<String>>) -> Table {
    let ids: Vec<String> = tags.iter().map(|tag| tag.id.to_string()).collect();

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["Tag id", "Name", "Style", "Tags", "Deleted"]);

    for tag in tags {
        let id = tag.id.to_string();
        let prefix_length = unique_prefix_length(&id, &ids);
        let tags_column = tags_by_tag
            .get(&tag.id)
            .map(|names| names.join(", "))
            .unwrap_or_default();

        table.add_row(vec![
            Cell::new(highlight_id(&id, prefix_length)),
            Cell::new(&tag.name),
            Cell::new(format_style(&tag.style)),
            // TODO: Store the ids instead of the names.
            Cell::new(tags_column),
            Cell::new(deleted_label(tag.deleted)),
        ]);
    }

    table
}

/// The short id shown here comes from the daemon-computed `short_id_length`
/// (the fewest leading hex chars unique against *all* files right now) — a
/// convenience handle to display, though any id prefix or a name resolves
/// equally well. `tags_by_file` supplies the human-readable tag names shown
/// per file; a file absent from the map renders with an empty tag column.
/// The `Deleted` column tells a tombstoned file (e.g. just deleted, or found
/// by `search --deleted`) from a live one.
fn file_table(files: &[FileInfo], tags_by_file: &HashMap<FileId, Vec<String>>) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            "File id", "Path", "Kind", "Version", "Size", "Tags", "Deleted",
        ]);

    for file in files {
        let id = file.file_id.to_string();
        let tags = tags_by_file
            .get(&file.file_id)
            .map(|names| names.join(", "))
            .unwrap_or_default();

        table.add_row(vec![
            Cell::new(highlight_id(&id, file.short_id_length)),
            Cell::new(&file.logical_path),
            Cell::new(file_kind_name(file.kind())),
            Cell::new(format!("v{}", file.version_number)),
            Cell::new(format!("{}b", file.size)),
            Cell::new(tags),
            Cell::new(deleted_label(file.deleted)),
        ]);
    }

    table
}

/// Emit a set of tags in the selected [`OutputMode`]: the shared
/// [`tag_table`] (or `(no tags)`) for humans, or a JSON array of
/// [`TagRow`]s for scripts.
pub fn emit_tags(output_mode: OutputMode, tags: &[Tag], tags_by_tag: &HashMap<TagId, Vec<String>>) {
    match output_mode {
        OutputMode::Human => {
            if tags.is_empty() {
                println!("(no tags)");
            } else {
                println!("{}", tag_table(tags, tags_by_tag));
            }
        }
        OutputMode::Json => {
            let rows: Vec<TagRow> = tags
                .iter()
                .map(|tag| TagRow::new(tag, tags_by_tag.get(&tag.id).cloned().unwrap_or_default()))
                .collect();

            print_json(&rows);
        }
    }
}

/// Emit a set of files in the selected [`OutputMode`]: the shared
/// [`file_table`] (or `(no files)`) for humans, or a JSON array of
/// [`FileRow`]s for scripts.
pub fn emit_files(
    output_mode: OutputMode,
    files: &[FileInfo],
    tags_by_file: &HashMap<FileId, Vec<String>>,
) {
    match output_mode {
        OutputMode::Human => {
            if files.is_empty() {
                println!("(no files)");
            } else {
                println!("{}", file_table(files, tags_by_file));
            }
        }
        OutputMode::Json => print_json(&file_rows(files, tags_by_file)),
    }
}

pub fn emit_tags_and_files(
    output_mode: OutputMode,
    tags: &[Tag],
    files: &[FileInfo],
    tags_by_tag: &HashMap<TagId, Vec<String>>,
    tags_by_file: &HashMap<FileId, Vec<String>>,
) {
    match output_mode {
        OutputMode::Human => {
            emit_tags(output_mode, tags, tags_by_tag);
            emit_files(output_mode, files, tags_by_file);
        }
        OutputMode::Json => {
            let tag_rows: Vec<TagRow> = tags
                .iter()
                .map(|tag| TagRow::new(tag, tags_by_tag.get(&tag.id).cloned().unwrap_or_default()))
                .collect();

            print_json(&json!({ "tags": tag_rows, "files": file_rows(files, tags_by_file) }));
        }
    }
}

pub fn emit_error(output_mode: OutputMode, message: &str) {
    match output_mode {
        OutputMode::Human => {
            eprintln!("{message}")
        }
        OutputMode::Json => print_json(&json!({
            "error": message,
        })),
    }
}

/// Human-readable label for an [`OperationKind`]: a short verb phrase for
/// the "Action" column of the operations table.
fn operation_kind_label(kind: &OperationKind) -> String {
    match kind {
        OperationKind::ConnectingToPeer { url, .. } => format!("Connecting ({url})"),
        OperationKind::ReceivingFile { .. } => "Receiving".to_owned(),
        OperationKind::Fetching { .. } => "Fetching".to_owned(),
        OperationKind::ReconcilingManifest { .. } => "Reconciling manifest".to_owned(),
        OperationKind::ReconcilingTags { .. } => "Reconciling tags".to_owned(),
        OperationKind::PlacingFile { .. } => "Placing file".to_owned(),
        OperationKind::ScanningSyncDirectories => "Scanning sync directories".to_owned(),
    }
}

/// The peer an operation involves, if any (its configured name).
fn operation_peer(kind: &OperationKind) -> Option<&str> {
    match kind {
        OperationKind::ConnectingToPeer { peer_name, .. }
        | OperationKind::ReceivingFile { peer_name, .. }
        | OperationKind::ReconcilingManifest { peer_name }
        | OperationKind::ReconcilingTags { peer_name } => Some(peer_name),
        OperationKind::Fetching { .. }
        | OperationKind::PlacingFile { .. }
        | OperationKind::ScanningSyncDirectories => None,
    }
}

/// The file an operation concerns, if any (its id string).
fn operation_file(kind: &OperationKind) -> Option<&str> {
    match kind {
        OperationKind::ReceivingFile { file_id, .. }
        | OperationKind::Fetching { file_id }
        | OperationKind::PlacingFile { file_id } => Some(file_id),
        OperationKind::ConnectingToPeer { .. }
        | OperationKind::ReconcilingManifest { .. }
        | OperationKind::ReconcilingTags { .. }
        | OperationKind::ScanningSyncDirectories => None,
    }
}

/// Human-readable label for an [`OperationStatus`], including a
/// `done/total` progress fragment for active operations that report
/// one.
fn operation_status_label(status: &OperationStatus) -> String {
    match status {
        OperationStatus::Active { progress: None } => "active".to_owned(),
        OperationStatus::Active {
            progress: Some(progress),
        } => match progress.total {
            Some(total) => format!("active ({}/{})", progress.done, total),
            None => format!("active ({})", progress.done),
        },
        OperationStatus::Completed => "completed".to_owned(),
        OperationStatus::Failed { reason } => format!("failed: {reason}"),
        OperationStatus::Aborted => "aborted".to_owned(),
    }
}

/// Build the operations table (see [`file_table`] for the shared pattern).
fn operation_table(operations: &[Operation]) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["Id", "Action", "Peer", "File", "Status"]);

    for operation in operations {
        table.add_row(vec![
            Cell::new(operation.id.as_u64()),
            Cell::new(operation_kind_label(&operation.kind)),
            Cell::new(operation_peer(&operation.kind).unwrap_or("")),
            Cell::new(operation_file(&operation.kind).unwrap_or("")),
            Cell::new(operation_status_label(&operation.status)),
        ]);
    }

    table
}

/// Emit the currently-active operations in the selected [`OutputMode`]: the
/// shared [`operation_table`] (or `(no operations)`) for humans, or the raw
/// [`Operation`]s as a JSON array for scripts (they already derive
/// `Serialize`).
pub fn emit_operations(output_mode: OutputMode, operations: &[Operation]) {
    match output_mode {
        OutputMode::Human => {
            if operations.is_empty() {
                println!("(no operations)");
            } else {
                println!("{}", operation_table(operations));
            }
        }
        OutputMode::Json => print_json(&operations),
    }
}

/// Build the connected-peers table.
fn connected_peers_table(peers: &[ConnectedPeer]) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["Peer", "Direction", "Public key"]);

    for peer in peers {
        let direction = match peer.direction {
            Direction::Outbound => "outbound",
            Direction::Inbound => "inbound",
        };
        table.add_row(vec![
            Cell::new(&peer.peer_name),
            Cell::new(direction),
            Cell::new(&peer.public_key),
        ]);
    }

    table
}

/// Emit the currently-connected peers in the selected [`OutputMode`].
///
/// A connection is *state*, not an operation, so it has its own command and
/// output rather than appearing among the operations.
pub fn emit_connected_peers(output_mode: OutputMode, peers: &[ConnectedPeer]) {
    match output_mode {
        OutputMode::Human => {
            if peers.is_empty() {
                println!("(no connected peers)");
            } else {
                println!("{}", connected_peers_table(peers));
            }
        }
        OutputMode::Json => print_json(&peers),
    }
}

/// Render tag-rule diagnostics for `retag --check`.
///
/// Both problem classes are reported as warnings rather than errors: neither
/// stops the daemon, and neither stops the *other* rules from working.
pub fn print_tag_rule_report(report: &tagsy_api::TagRuleReport) {
    println!(
        "{} tag rule{} active",
        report.active,
        if report.active == 1 { "" } else { "s" }
    );

    if report.invalid.is_empty() && report.unknown_tags.is_empty() {
        println!("No problems found");
        return;
    }

    if !report.invalid.is_empty() {
        println!("\nInvalid patterns (these rules are disabled):");
        for problem in &report.invalid {
            println!("  {problem}");
        }
    }

    if !report.unknown_tags.is_empty() {
        println!("\nRules name tags that do not exist (they will never be useful):");
        for tag_id in &report.unknown_tags {
            println!("  {}", tag_id.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use tagsy_api::{OperationKind, OperationStatus, Progress};

    use super::*;

    // ---- unique_prefix_length: jj-style shortest-unique-prefix ----

    #[test]
    fn unique_prefix_is_one_char_when_first_chars_differ() {
        let all = vec!["abc".to_owned(), "bcd".to_owned(), "cde".to_owned()];
        assert_eq!(unique_prefix_length("abc", &all), 1);
    }

    #[test]
    fn unique_prefix_grows_past_a_shared_run() {
        // "ab.." collides with "abd" until the 3rd char disambiguates.
        let all = vec!["abc".to_owned(), "abd".to_owned()];
        assert_eq!(unique_prefix_length("abc", &all), 3);
    }

    #[test]
    fn unique_prefix_ignores_the_target_itself() {
        // The target appearing in `all` must not count as a collision, or no
        // prefix would ever be unique.
        let all = vec!["abc".to_owned()];
        assert_eq!(unique_prefix_length("abc", &all), 1);
    }

    #[test]
    fn unique_prefix_is_full_length_when_target_is_a_prefix_of_another() {
        // "ab" is a prefix of "abc", so no prefix of "ab" is unique; it falls
        // back to the whole string.
        let all = vec!["ab".to_owned(), "abc".to_owned()];
        assert_eq!(unique_prefix_length("ab", &all), 2);
    }

    #[test]
    fn unique_prefix_of_a_lone_id_is_one() {
        assert_eq!(
            unique_prefix_length("deadbeef", &["deadbeef".to_owned()]),
            1
        );
    }

    // ---- table columns ----

    #[test]
    fn deleted_label_names_both_states() {
        assert_eq!(deleted_label(true), "yes");
        assert_eq!(deleted_label(false), "no");
    }

    #[test]
    fn tables_show_whether_each_entry_is_deleted() {
        let file = |deleted| FileInfo {
            file_id: FileId::new(),
            logical_path: tagsy_core::LogicalPath::new("a.txt"),
            content_hash: String::new(),
            version_number: 1,
            size: 1,
            short_id_length: 4,
            deleted,
            first_recorded_at: 0,
            latest_change_at: 0,
        };
        let table = file_table(&[file(true)], &HashMap::new());
        assert_eq!(table.header().unwrap().cell_count(), 7);
        let row = table.row(0).unwrap().cell_iter().last().unwrap().content();
        assert_eq!(row, "yes");

        let tag = Tag {
            id: TagId::new(),
            name: "work".to_owned(),
            style: tagsy_api::TagStyle::default(),
            metadata: None,
            deleted: false,
        };
        let table = tag_table(&[tag], &HashMap::new());
        let row = table.row(0).unwrap().cell_iter().last().unwrap().content();
        assert_eq!(row, "no");
    }

    // ---- row DTOs: field mapping ----

    #[test]
    fn file_row_maps_fields_and_carries_tags() {
        let file = FileInfo {
            file_id: FileId::new(),
            logical_path: tagsy_core::LogicalPath::new("photos/cat.jpg"),
            content_hash: "deadbeef".to_owned(),
            version_number: 3,
            size: 2048,
            short_id_length: 4,
            deleted: true,
            first_recorded_at: 0,
            latest_change_at: 0,
        };
        let row = FileRow::new(&file, vec!["photos".to_owned()]);

        assert_eq!(row.id, file.file_id);
        assert_eq!(row.path, "photos/cat.jpg");
        assert_eq!(row.version, 3);
        assert_eq!(row.content_hash, "deadbeef");
        assert_eq!(row.kind, "image");
        assert_eq!(row.size, 2048);
        assert_eq!(row.tags, vec!["photos".to_owned()]);
        assert!(row.deleted);
    }

    #[test]
    fn tag_row_maps_fields_and_carries_tags() {
        let tag = Tag {
            id: TagId::new(),
            name: "work".to_owned(),
            style: tagsy_api::TagStyle {
                dot_color: "#00FF00".to_owned(),
                ..tagsy_api::TagStyle::default()
            },
            metadata: None,
            deleted: false,
        };
        let row = TagRow::new(&tag, vec!["parent".to_owned()]);

        assert_eq!(row.id, tag.id);
        assert_eq!(row.name, "work");
        assert_eq!(row.style.dot_color, "#00FF00");
        assert_eq!(row.tags, vec!["parent".to_owned()]);
        assert!(!row.deleted);
    }

    // ---- operation labels: the kind/status → string mapping ----

    #[test]
    fn operation_kind_label_covers_each_variant() {
        assert_eq!(
            operation_kind_label(&OperationKind::ConnectingToPeer {
                peer_name: "B".to_owned(),
                url: "ws://b".to_owned(),
            }),
            "Connecting (ws://b)"
        );
        assert_eq!(
            operation_kind_label(&OperationKind::ReceivingFile {
                file_id: "f".to_owned(),
                peer_name: "B".to_owned(),
            }),
            "Receiving"
        );
        assert_eq!(
            operation_kind_label(&OperationKind::Fetching {
                file_id: "f".to_owned(),
            }),
            "Fetching"
        );
        assert_eq!(
            operation_kind_label(&OperationKind::PlacingFile {
                file_id: "f".to_owned(),
            }),
            "Placing file"
        );
    }

    #[test]
    fn operation_peer_and_file_are_disjoint_projections() {
        let receiving = OperationKind::ReceivingFile {
            file_id: "f1".to_owned(),
            peer_name: "B".to_owned(),
        };
        assert_eq!(operation_peer(&receiving), Some("B"));
        assert_eq!(operation_file(&receiving), Some("f1"));

        let fetching = OperationKind::Fetching {
            file_id: "f2".to_owned(),
        };
        assert_eq!(operation_peer(&fetching), None);
        assert_eq!(operation_file(&fetching), Some("f2"));

        let reconciling = OperationKind::ReconcilingTags {
            peer_name: "C".to_owned(),
        };
        assert_eq!(operation_peer(&reconciling), Some("C"));
        assert_eq!(operation_file(&reconciling), None);
    }

    #[test]
    fn operation_status_label_renders_progress_fragments() {
        assert_eq!(
            operation_status_label(&OperationStatus::Active { progress: None }),
            "active"
        );
        assert_eq!(
            operation_status_label(&OperationStatus::Active {
                progress: Some(Progress {
                    done: 3,
                    total: Some(10),
                }),
            }),
            "active (3/10)"
        );
        assert_eq!(
            operation_status_label(&OperationStatus::Active {
                progress: Some(Progress {
                    done: 3,
                    total: None,
                }),
            }),
            "active (3)"
        );
        assert_eq!(
            operation_status_label(&OperationStatus::Completed),
            "completed"
        );
        assert_eq!(
            operation_status_label(&OperationStatus::Failed {
                reason: "boom".to_owned(),
            }),
            "failed: boom"
        );
        assert_eq!(operation_status_label(&OperationStatus::Aborted), "aborted");
    }
}
