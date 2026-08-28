//! All terminal output.

use std::collections::HashMap;

use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Table};
use owo_colors::OwoColorize;
use serde::Serialize;
use serde_json::json;
use tagsy_api::{ConnectedPeer, Direction, Operation, OperationKind, OperationStatus, Tag};
use tagsy_core::{FileId, FileInfo, TagId};

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

/// A serializable file row, shared by every command that prints files.
#[derive(Debug, Serialize)]
struct FileRow {
    id: FileId,
    path: String,
    version: i64,
    content_hash: String,
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
            size: file.size,
            tags,
            deleted: file.deleted,
        }
    }
}

/// A serializable tag row, shared by every command that prints tags.
#[derive(Debug, Serialize)]
struct TagRow {
    id: TagId,
    name: String,
    color: String,
    tags: Vec<String>,
    deleted: bool,
}

impl TagRow {
    /// Build a row from a tag and its applied-tag names.
    fn new(tag: &Tag, tags: Vec<String>) -> Self {
        Self {
            id: tag.id,
            name: tag.name.clone(),
            color: tag.color.clone(),
            tags,
            deleted: tag.deleted,
        }
    }
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
/// This is the single home for the ~dozen command arms whose whole output is
/// "print a confirmation line, or the equivalent JSON object" (`Deleted file
/// …`, `Moved file …`, `Purged N previews`). Each used to inline the same
/// `match output_mode { Human => println!(..), Json => print_json(&json!(..))
/// }`; routing them here keeps the two renderings adjacent so they can't drift,
/// and leaves the dispatch arms as one line.
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

/// The single tag table used by *every* command that prints a set of tags
/// (`search`, `tags-for-file`, `subtags`).
///
/// Short-id prefixes are highlighted the way `jj`/`git` show change ids.
/// The prefix length is computed against `tags`, so pass the full set
/// you intend to display; the highlighted prefix is a valid lookup key
/// for the tag commands.
///
/// The `Tags` column shows the tags applied to each tag (the tags it is a
/// subtag of), the tag analogue of the file table's per-file tags.
/// `tags_by_tag` supplies those names; a tag absent from the map renders
/// with an empty column.
fn tag_table(tags: &[Tag], tags_by_tag: &HashMap<TagId, Vec<String>>) -> Table {
    let ids: Vec<String> = tags.iter().map(|tag| tag.id.to_string()).collect();

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["Tag id", "Name", "Color", "Tags"]);

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
            Cell::new(&tag.color),
            // TODO: Store the ids instead of the names.
            Cell::new(tags_column),
        ]);
    }

    table
}

/// The short-id prefix comes from the daemon-computed `short_id_length`
/// (unique against *all* files, so it is a valid global lookup key).
/// `tags_by_file` supplies the human-readable tag names shown per file;
/// a file absent from the map renders with an empty tag column.
fn file_table(files: &[FileInfo], tags_by_file: &HashMap<FileId, Vec<String>>) -> Table {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["File id", "Path", "Version", "Size", "Tags"]);

    for file in files {
        let id = file.file_id.to_string();
        let tags = tags_by_file
            .get(&file.file_id)
            .map(|names| names.join(", "))
            .unwrap_or_default();

        table.add_row(vec![
            Cell::new(highlight_id(&id, file.short_id_length)),
            Cell::new(&file.logical_path),
            Cell::new(format!("v{}", file.version_number)),
            Cell::new(format!("{}b", file.size)),
            Cell::new(tags),
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
        OutputMode::Json => {
            let rows: Vec<FileRow> = files
                .iter()
                .map(|file| {
                    FileRow::new(
                        file,
                        tags_by_file.get(&file.file_id).cloned().unwrap_or_default(),
                    )
                })
                .collect();

            print_json(&rows);
        }
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

            let file_rows: Vec<FileRow> = files
                .iter()
                .map(|file| {
                    FileRow::new(
                        file,
                        tags_by_file.get(&file.file_id).cloned().unwrap_or_default(),
                    )
                })
                .collect();

            print_json(&json!({ "tags": tag_rows, "files": file_rows }));
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
    }
}

/// The peer an operation involves, if any (its configured name).
fn operation_peer(kind: &OperationKind) -> Option<&str> {
    match kind {
        OperationKind::ConnectingToPeer { peer_name, .. }
        | OperationKind::ReceivingFile { peer_name, .. }
        | OperationKind::ReconcilingManifest { peer_name }
        | OperationKind::ReconcilingTags { peer_name } => Some(peer_name),
        OperationKind::Fetching { .. } | OperationKind::PlacingFile { .. } => None,
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
        | OperationKind::ReconcilingTags { .. } => None,
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
        assert_eq!(row.size, 2048);
        assert_eq!(row.tags, vec!["photos".to_owned()]);
        assert!(row.deleted);
    }

    #[test]
    fn tag_row_maps_fields_and_carries_tags() {
        let tag = Tag {
            id: TagId::new(),
            name: "work".to_owned(),
            color: "#00FF00".to_owned(),
            metadata: None,
            deleted: false,
        };
        let row = TagRow::new(&tag, vec!["parent".to_owned()]);

        assert_eq!(row.id, tag.id);
        assert_eq!(row.name, "work");
        assert_eq!(row.color, "#00FF00");
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
