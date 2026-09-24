//! The convergence oracle: what two daemons must agree on, rendered as sorted
//! text lines so a mismatch prints as a readable diff.
//!
//! There are two views of the same catalog, for two different comparisons:
//!
//! - [`CatalogState::exact`] — every replicated field, keyed by id, including
//!   LWW timestamps and tombstones. Two nodes of the *same run* must match it
//!   exactly: reconciliation carries stamps verbatim, and a tombstone that one
//!   node has and another lacks changes how a future stale write resolves.
//! - [`CatalogState::normalized`] — ids replaced by logical paths / tag names,
//!   timestamps and tombstones dropped. Two *different runs* of the same script
//!   (live vs. reconnect) mint different ids and clocks, but must still end in
//!   the same effective state.
//!
//! The disk is checked per node against that node's own catalog
//! ([`check_disk`]) rather than across nodes, because each node's directory
//! layout legitimately differs (Universal vs. TagBased).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use tagsy_core::state::{RelationshipKind, RelationshipManifestEntry};
use tagsy_core::{FileId, TagId};
use tagsyd::configuration::SyncType;
use tagsyd::store::{CatalogStore, DeletedRule, ManifestRow, Tag};

/// A set of rendered facts. Two snapshots are equal iff their line sets are.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Snapshot {
    lines: BTreeSet<String>,
}

impl Snapshot {
    fn push(&mut self, line: String) {
        self.lines.insert(line);
    }

    /// The rendered facts, in sort order.
    pub fn lines(&self) -> impl Iterator<Item = &str> {
        self.lines.iter().map(String::as_str)
    }

    /// A unified-style diff (`-` only in `self`, `+` only in `other`), or
    /// `None` if the snapshots are equal. Lines are merged in sort order, so
    /// the two sides of a changed fact (same id prefix) print adjacently.
    pub fn diff(&self, other: &Snapshot) -> Option<String> {
        let mut merged: Vec<(&String, char)> = self
            .lines
            .difference(&other.lines)
            .map(|line| (line, '-'))
            .chain(other.lines.difference(&self.lines).map(|line| (line, '+')))
            .collect();
        if merged.is_empty() {
            return None;
        }
        merged.sort();
        let mut out = String::new();
        for (line, sign) in merged {
            let _ = writeln!(out, "{sign} {line}");
        }
        Some(out)
    }
}

/// Everything the catalog replicates, read straight from a node's `main.db`.
pub struct CatalogState {
    files: Vec<ManifestRow>,
    /// Tag definitions with their LWW `modified_at`.
    tags: Vec<(Tag, i64)>,
    relationships: Vec<RelationshipManifestEntry>,
    purged: Vec<FileId>,
}

impl CatalogState {
    /// Read the catalog through its own store, exactly as the API's read path
    /// does. Safe against a running daemon (SQLite serializes; the busy
    /// timeout covers the writer).
    pub fn load(main_db: &Path) -> Self {
        let store = CatalogStore::initialize(main_db).expect("open catalog for snapshot");
        let files = store.manifest_entries().expect("read file manifest");
        let modified_at: BTreeMap<TagId, i64> = store
            .tag_manifest_entries()
            .expect("read tag manifest")
            .into_iter()
            .map(|entry| (entry.tag_id, entry.modified_at))
            .collect();
        let tags = store
            .get_all_tags(DeletedRule::Include)
            .expect("read tags")
            .into_iter()
            .map(|tag| {
                let at = modified_at.get(&tag.id).copied().unwrap_or_default();
                (tag, at)
            })
            .collect();
        let relationships = store
            .relationship_manifest_entries()
            .expect("read relationships");
        let purged = store.purged_ids().expect("read purged ids");
        Self {
            files,
            tags,
            relationships,
            purged,
        }
    }

    /// Every replicated field, keyed by id. See the module docs.
    pub fn exact(&self) -> Snapshot {
        let mut snapshot = Snapshot::default();
        for (id, history, latest_at, path, path_at, deleted, deleted_at, restored_at) in &self.files
        {
            snapshot.push(format!(
                "file {} path={} path_at={path_at} latest_at={latest_at} deleted={deleted} \
                 deleted_at={deleted_at} restored_at={restored_at} history={}",
                id.to_string(),
                path.as_str(),
                render_history(history),
            ));
        }
        for (tag, modified_at) in &self.tags {
            snapshot.push(format!(
                "tag {} name={:?} deleted={} modified_at={modified_at} style={:?}",
                tag.id.to_string(),
                tag.name,
                tag.deleted,
                tag.style,
            ));
        }
        for relationship in &self.relationships {
            snapshot.push(format!(
                "rel {} {} -> {} deleted={} at={}",
                kind_label(relationship.kind),
                relationship.tag_id.to_string(),
                relationship.target_id,
                relationship.deleted,
                relationship.modified_at,
            ));
        }
        for id in &self.purged {
            snapshot.push(format!("purged {}", id.to_string()));
        }
        snapshot
    }

    /// The effective state with ids and clocks abstracted away. See the module
    /// docs.
    pub fn normalized(&self) -> Snapshot {
        let paths = self.file_paths();
        let names: BTreeMap<String, &str> = self
            .tags
            .iter()
            .map(|(tag, _)| (tag.id.to_string(), tag.name.as_str()))
            .collect();
        let file_label = |id: &str| paths.get(id).map_or(format!("<file {id}>"), Clone::clone);
        let tag_label = |id: &str| {
            names
                .get(id)
                .map_or(format!("<tag {id}>"), |name| (*name).to_owned())
        };

        let mut snapshot = Snapshot::default();
        for (_, history, _, path, _, deleted, ..) in &self.files {
            snapshot.push(format!(
                "file {} deleted={deleted} history={}",
                path.as_str(),
                render_history(history),
            ));
        }
        for (tag, _) in &self.tags {
            snapshot.push(format!(
                "tag {:?} deleted={} style={:?}",
                tag.name, tag.deleted, tag.style
            ));
        }
        for relationship in self.relationships.iter().filter(|r| !r.deleted) {
            let target = match relationship.kind {
                RelationshipKind::File => file_label(&relationship.target_id),
                RelationshipKind::Tag => tag_label(&relationship.target_id),
            };
            snapshot.push(format!(
                "rel {} {} -> {target}",
                kind_label(relationship.kind),
                tag_label(&relationship.tag_id.to_string()),
            ));
        }
        snapshot.push(format!("purged count={}", self.purged.len()));
        snapshot
    }

    fn file_paths(&self) -> BTreeMap<String, String> {
        self.files
            .iter()
            .map(|row| (row.0.to_string(), row.3.as_str().to_owned()))
            .collect()
    }

    /// Live (non-tombstoned) files as `id -> (logical path, latest hash)`.
    fn live_files(&self) -> BTreeMap<String, (String, String)> {
        self.files
            .iter()
            .filter(|row| !row.5)
            .filter_map(|row| {
                let latest = row.1.last()?;
                Some((
                    row.0.to_string(),
                    (row.3.as_str().to_owned(), latest.1.clone()),
                ))
            })
            .collect()
    }

    /// Each file's live *direct* tags — the set TagBased placement tests
    /// against (`SubtagRule::Exclude` in `plan_placement`).
    fn direct_tags(&self) -> BTreeMap<String, BTreeSet<TagId>> {
        let mut tags: BTreeMap<String, BTreeSet<TagId>> = BTreeMap::new();
        for relationship in &self.relationships {
            if relationship.kind == RelationshipKind::File && !relationship.deleted {
                tags.entry(relationship.target_id.clone())
                    .or_default()
                    .insert(relationship.tag_id);
            }
        }
        tags
    }
}

/// Check one sync directory's disk contents against its node's catalog.
///
/// - Universal: exactly one file per live catalog file, named by its id,
///   holding the latest version's bytes.
/// - TagBased: exactly the live files whose direct tags include all of the
///   directory's tags, at their logical paths, holding the latest bytes.
///
/// Returns a diff of `expected` (`-`) against `actual` (`+`), or `None`.
pub fn check_disk(
    catalog: &CatalogState,
    directory: &Path,
    sync_type: &SyncType,
) -> Option<String> {
    let live = catalog.live_files();
    let mut expected = Snapshot::default();
    match sync_type {
        SyncType::Universal { .. } => {
            for (id, (_, hash)) in &live {
                expected.push(format!("{id} {}", short(hash)));
            }
        }
        SyncType::TagBased { tags } => {
            let direct = catalog.direct_tags();
            let empty = BTreeSet::new();
            for (id, (path, hash)) in &live {
                let file_tags = direct.get(id).unwrap_or(&empty);
                if tags.iter().all(|tag| file_tags.contains(tag)) {
                    expected.push(format!("{path} {}", short(hash)));
                }
            }
        }
    }
    expected.diff(&disk_contents(directory)).map(|diff| {
        format!(
            "{} ({sync_type:?}) disagrees with its catalog (- expected, + on disk):\n{diff}",
            directory.display()
        )
    })
}

/// `relative path -> short content hash` for every regular file under `root`.
pub fn disk_contents(root: &Path) -> Snapshot {
    let mut snapshot = Snapshot::default();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let relative = entry
            .path()
            .strip_prefix(root)
            .expect("walk stays under root");
        let bytes = std::fs::read(entry.path()).unwrap_or_default();
        let hash = blake3::hash(&bytes).to_hex().to_string();
        snapshot.push(format!("{} {}", relative.to_string_lossy(), short(&hash)));
    }
    snapshot
}

fn render_history(history: &[(i64, String, i64)]) -> String {
    let versions: Vec<String> = history
        .iter()
        .map(|(number, hash, size)| format!("{number}:{}:{size}", short(hash)))
        .collect();
    format!("[{}]", versions.join(","))
}

fn short(hash: &str) -> &str {
    hash.get(..12).unwrap_or(hash)
}

fn kind_label(kind: RelationshipKind) -> &'static str {
    match kind {
        RelationshipKind::File => "file",
        RelationshipKind::Tag => "tag",
    }
}
