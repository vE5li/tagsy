//! Reconciling a peer's tag manifest (definitions + relationships) against
//! ours.

use tagsy_core::state::{
    Change, ChangeOrigin, Frame, RelationshipManifestEntry, Sync as SyncMessage, TagManifestEntry,
};
use tagsy_core::{FileId, TagId};
use tokio::sync::mpsc::UnboundedSender;

use crate::catalog::messages::{CatalogCommand, Ingest};
use crate::store::CatalogStore;

/// Build our local tag manifest: lightweight definition entries (id +
/// `modified_at`) plus every relationship (with its soft-delete state). The
/// tag counterpart of [`crate::peer::plan::build_local_manifest`].
pub fn build_local_tag_manifest(
    database: &CatalogStore,
) -> Result<(Vec<TagManifestEntry>, Vec<RelationshipManifestEntry>), String> {
    let definitions = database
        .tag_manifest_entries()
        .map_err(|e| format!("tag_manifest_entries: {e:?}"))?;
    let relationships = database
        .relationship_manifest_entries()
        .map_err(|e| format!("relationship_manifest_entries: {e:?}"))?;
    Ok((definitions, relationships))
}

/// Split a full tag manifest into batches, each destined for its own
/// `Sync::TagManifest` frame, so a large tag set never approaches a single
/// WebSocket message's size limit.
///
/// A `TagManifest` frame carries two vecs (definitions + relationships). Both
/// are per-entry additive on the receiver (definitions drive a `TagRequest`,
/// relationships apply by last-writer-wins), so they can be batched
/// independently: this emits definition-only frames first, then
/// relationship-only frames (each `(defs, rels)` pair has exactly one non-empty
/// vec). The definitions-before-relationships order is a best-effort efficiency
/// hint, not a correctness requirement.
///
/// Empty inputs yield no frames. `batch_size` is clamped to at least 1.
pub fn batch_tag_manifest(
    definitions: Vec<TagManifestEntry>,
    relationships: Vec<RelationshipManifestEntry>,
    batch_size: usize,
) -> Vec<(Vec<TagManifestEntry>, Vec<RelationshipManifestEntry>)> {
    let batch_size = batch_size.max(1);
    let mut frames = Vec::new();
    for chunk in definitions.chunks(batch_size) {
        frames.push((chunk.to_vec(), Vec::new()));
    }
    for chunk in relationships.chunks(batch_size) {
        frames.push((Vec::new(), chunk.to_vec()));
    }
    frames
}

/// Reconcile a peer's tag manifest against ours.
///
/// - **Definitions**: for each tag whose `modified_at` is newer than ours (or
///   that we don't know), enqueue a `TagRequest`; the peer answers with a
///   `Change::TagAdded` carrying the full definition. Older/equal definitions
///   are skipped — the peer will request ours via the symmetric path.
/// - **Relationships**: applied directly. Each carries its whole state, so we
///   translate it into the matching relationship `Change` (tag/untag) stamped
///   with the peer's `modified_at` and hand it to the single DB-writer, which
///   enforces last-writer-wins. This routes through the same code path as a
///   live relationship change, keeping behavior uniform.
///
/// Pure of `.await` and holds no lock: takes `&CatalogStore` synchronously so
/// the caller's future stays `Send`.
pub fn plan_tag_sync(
    peer_name: &str,
    peer_public_key: &str,
    definitions: Vec<TagManifestEntry>,
    relationships: Vec<RelationshipManifestEntry>,
    database: &CatalogStore,
    outbound: &UnboundedSender<Frame>,
    change_sender: &UnboundedSender<CatalogCommand>,
) {
    log::info!(
        "Reconciling {} tag definitions and {} relationships from {peer_name}",
        definitions.len(),
        relationships.len()
    );

    for definition in definitions {
        let ours = match database.tag_modified_at(definition.tag_id) {
            Ok(value) => value,
            Err(error) => {
                log::error!(
                    "tag_modified_at failed for {}: {error:?}",
                    definition.tag_id.to_string()
                );
                continue;
            }
        };
        // Act when we don't know the tag, or the peer's is strictly newer.
        let need = match ours {
            None => true,
            Some(ours) => definition.modified_at > ours,
        };
        if !need {
            continue;
        }

        if definition.deleted {
            // The peer's newer state is a tombstone. Apply it directly through
            // the single DB writer (a tag delete bumps `modified_at`, so the
            // same LWW comparison decides delete-vs-edit). No `TagRequest`: there
            // is no definition to fetch for a deleted tag.
            if let Err(error) = change_sender.send(CatalogCommand::Change(
                Ingest::from_change(Change::TagRemoved {
                    tag_id: definition.tag_id,
                    modified_at: definition.modified_at,
                }),
                ChangeOrigin::Peer {
                    public_key: peer_public_key.to_owned(),
                },
            )) {
                log::error!("change_sender closed; cannot apply reconciled tag delete: {error}");
                return;
            }
        } else {
            // A live tag whose definition is newer than ours: request it.
            let frame = Frame::Sync(SyncMessage::TagRequest {
                tag_id: definition.tag_id,
            });
            if let Err(error) = outbound.send(frame) {
                log::warn!("Failed to enqueue TagRequest for {peer_name}: {error}");
                return;
            }
        }
    }

    for relationship in relationships {
        // LWW pre-check: skip anything not newer than what we hold. The DB layer
        // also enforces this, but skipping here avoids bus traffic for no-ops.
        let ours = match database.relationship_modified_at(
            relationship.tag_id,
            &relationship.target_id,
            relationship.kind,
        ) {
            Ok(value) => value,
            Err(error) => {
                log::error!("relationship_modified_at failed: {error:?}");
                continue;
            }
        };
        if let Some(ours) = ours
            && relationship.modified_at <= ours
        {
            continue;
        }

        let Some(change) = relationship_to_change(&relationship) else {
            log::warn!(
                "Skipping relationship with unparseable target_id {}",
                relationship.target_id
            );
            continue;
        };
        if let Err(error) = change_sender.send(CatalogCommand::Change(
            Ingest::from_change(change),
            ChangeOrigin::Peer {
                public_key: peer_public_key.to_owned(),
            },
        )) {
            log::error!("change_sender closed; cannot apply reconciled relationship: {error}");
            return;
        }
    }
}

/// Translate a reconciled relationship manifest entry into the equivalent
/// relationship `Change`, carrying the entry's `modified_at` so
/// last-writer-wins is preserved. Returns `None` if `target_id` doesn't parse
/// as the id kind.
fn relationship_to_change(entry: &RelationshipManifestEntry) -> Option<Change> {
    use tagsy_core::state::RelationshipKind;
    let change = match (entry.kind, entry.deleted) {
        (RelationshipKind::File, false) => Change::FileTagged {
            file_id: FileId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            metadata: None,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::File, true) => Change::FileUntagged {
            file_id: FileId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::Tag, false) => Change::TagTagged {
            taggee_id: TagId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            metadata: None,
            modified_at: entry.modified_at,
        },
        (RelationshipKind::Tag, true) => Change::TagUntagged {
            taggee_id: TagId::from_string(&entry.target_id)?,
            tag_id: entry.tag_id,
            modified_at: entry.modified_at,
        },
    };
    Some(change)
}

/// Answer a peer's `TagRequest` with the full tag definition as a
/// `Change::TagAdded`, or `TagNotFound` if we no longer hold the tag.
pub fn build_tag_request_response(
    peer_name: &str,
    tag_id: TagId,
    database: &CatalogStore,
) -> Frame {
    match database.tag_definition(tag_id) {
        Ok(Some((name, color, modified_at))) => Frame::Change(Change::TagAdded {
            tag_id,
            tag_name: name,
            color,
            metadata: None,
            modified_at,
        }),
        Ok(None) => {
            log::warn!(
                "TagRequest from {peer_name} for {} but we no longer hold it",
                tag_id.to_string()
            );
            Frame::Sync(SyncMessage::TagNotFound { tag_id })
        }
        Err(error) => {
            log::error!(
                "tag_definition failed for {} requested by {peer_name}: {error:?}",
                tag_id.to_string()
            );
            Frame::Sync(SyncMessage::TagNotFound { tag_id })
        }
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::state::RelationshipKind;

    use super::*;

    fn definition(n: u8) -> TagManifestEntry {
        TagManifestEntry {
            tag_id: TagId::new(),
            modified_at: n as i64,
            deleted: false,
        }
    }

    fn relationship(n: u8) -> RelationshipManifestEntry {
        RelationshipManifestEntry {
            tag_id: TagId::new(),
            target_id: FileId::new().to_string(),
            kind: RelationshipKind::File,
            modified_at: n as i64,
            deleted: false,
        }
    }

    /// Nothing to announce → no frames.
    #[test]
    fn batch_tag_manifest_empty_yields_no_frames() {
        assert!(batch_tag_manifest(Vec::new(), Vec::new(), 5000).is_empty());
    }

    /// Definitions and relationships are batched independently into
    /// single-kind frames (definitions first), splitting each by size.
    #[test]
    fn batch_tag_manifest_splits_each_kind_independently() {
        let defs: Vec<_> = (0..5u8).map(definition).collect();
        let rels: Vec<_> = (0..3u8).map(relationship).collect();

        let frames = batch_tag_manifest(defs, rels, 2);
        // ceil(5/2)=3 definition frames + ceil(3/2)=2 relationship frames.
        assert_eq!(frames.len(), 5);

        // Definition frames come first, each with only definitions.
        assert_eq!(frames[0].0.len(), 2);
        assert!(frames[0].1.is_empty());
        assert_eq!(frames[1].0.len(), 2);
        assert_eq!(frames[2].0.len(), 1);

        // Then relationship frames, each with only relationships.
        assert!(frames[3].0.is_empty());
        assert_eq!(frames[3].1.len(), 2);
        assert_eq!(frames[4].1.len(), 1);

        // Nothing lost across the split.
        let total_defs: usize = frames.iter().map(|(d, _)| d.len()).sum();
        let total_rels: usize = frames.iter().map(|(_, r)| r.len()).sum();
        assert_eq!(total_defs, 5);
        assert_eq!(total_rels, 3);
    }

    /// Only-definitions and only-relationships inputs each produce their own
    /// frames without a spurious empty frame for the missing kind.
    #[test]
    fn batch_tag_manifest_single_kind() {
        let defs_only = batch_tag_manifest(vec![definition(1)], Vec::new(), 5000);
        assert_eq!(defs_only.len(), 1);
        assert_eq!(defs_only[0].0.len(), 1);

        let rels_only = batch_tag_manifest(Vec::new(), vec![relationship(1)], 5000);
        assert_eq!(rels_only.len(), 1);
        assert_eq!(rels_only[0].1.len(), 1);
    }

    /// A zero batch size is clamped to 1.
    #[test]
    fn batch_tag_manifest_zero_size_is_clamped() {
        let frames = batch_tag_manifest(vec![definition(1), definition(2)], Vec::new(), 0);
        assert_eq!(frames.len(), 2);
    }
}
