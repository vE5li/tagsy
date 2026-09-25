//! The pure kernel behind `delete-duplicates`: find live files that share a
//! logical path and latest content hash, and plan which one survives and which
//! tags it must absorb.
//!
//! Split in two so only files that are actually duplicated have their tags
//! read: [`find_duplicate_sets`] groups the catalog listing, and
//! [`plan_duplicate_group`] turns one set plus its members' direct tags into a
//! [`DuplicatePlan`]. Neither touches the database; the thin
//! [`plan_duplicate_deletions`] feeds them from it.
//!
//! The survivor is always the member with the lowest [`FileId`]. That depends
//! only on replicated state, so two devices running the command concurrently
//! keep the same file — if they chose differently, their soft deletes could
//! between them remove every copy. Merging the deleted members' tags into the
//! survivor makes the choice otherwise immaterial.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use tagsy_core::{FileId, FileInfo, LogicalPath, TagId};

use crate::store::{CatalogStore, DatabaseError, DeletedRule, SubtagRule};

/// Two or more live files sharing a logical path and latest content hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DuplicateSet {
    pub logical_path: LogicalPath,
    pub content_hash: String,
    /// Every member, sorted ascending; the first is the survivor.
    pub file_ids: Vec<FileId>,
}

/// What to do with one [`DuplicateSet`]: which member survives, which are
/// deleted, and which tags the survivor absorbs. Ids only; the writer reports
/// the members themselves as a [`tagsy_api::DuplicateGroup`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DuplicatePlan {
    pub logical_path: LogicalPath,
    pub content_hash: String,
    pub kept: FileId,
    pub deleted: Vec<FileId>,
    pub tags_merged: Vec<TagId>,
}

/// Group `files` by `(logical_path, content_hash)`, keeping only groups of two
/// or more. The result is ordered by logical path, then content hash.
///
/// `files` is expected to hold only live files; any tombstoned entry is
/// skipped, since deleting it again would change nothing.
pub(crate) fn find_duplicate_sets(files: Vec<FileInfo>) -> Vec<DuplicateSet> {
    let mut groups: HashMap<(LogicalPath, String), Vec<FileId>> = HashMap::new();
    for file in files.into_iter().filter(|file| !file.deleted) {
        groups
            .entry((file.logical_path, file.content_hash))
            .or_default()
            .push(file.file_id);
    }

    let mut sets: Vec<DuplicateSet> = groups
        .into_iter()
        .filter(|(_, file_ids)| file_ids.len() > 1)
        .map(|((logical_path, content_hash), mut file_ids)| {
            file_ids.sort_unstable();
            DuplicateSet {
                logical_path,
                content_hash,
                file_ids,
            }
        })
        .collect();
    sets.sort_by(|left, right| {
        (left.logical_path.as_str(), &left.content_hash)
            .cmp(&(right.logical_path.as_str(), &right.content_hash))
    });
    sets
}

/// Read the catalog and plan every duplicate set: the thin DB layer over
/// [`find_duplicate_sets`] and [`plan_duplicate_group`]. Only members of a set
/// have their tags read.
pub(crate) fn plan_duplicate_deletions(
    database: &CatalogStore,
) -> Result<Vec<DuplicatePlan>, DatabaseError> {
    let sets = find_duplicate_sets(database.get_all_files(DeletedRule::Exclude)?);

    let mut tags = BTreeMap::new();
    for file_id in sets.iter().flat_map(|set| &set.file_ids) {
        let file_tags = database.tag_ids_for_file(*file_id, SubtagRule::Exclude)?;
        tags.insert(*file_id, file_tags.into_iter().collect());
    }

    Ok(sets
        .into_iter()
        .map(|set| plan_duplicate_group(set, &tags))
        .collect())
}

/// Plan one set: keep the lowest id, delete the rest, and merge onto the
/// survivor every tag a deleted member has that the survivor lacks.
///
/// `tags` maps each member to its direct tags; a member missing from it is
/// treated as untagged.
pub(crate) fn plan_duplicate_group(
    set: DuplicateSet,
    tags: &BTreeMap<FileId, BTreeSet<TagId>>,
) -> DuplicatePlan {
    let mut file_ids = set.file_ids.into_iter();
    let kept = file_ids
        .next()
        .expect("a duplicate set has at least two members");
    let deleted: Vec<FileId> = file_ids.collect();

    let empty = BTreeSet::new();
    let kept_tags = tags.get(&kept).unwrap_or(&empty);
    let tags_merged: Vec<TagId> = deleted
        .iter()
        .flat_map(|file_id| tags.get(file_id).unwrap_or(&empty))
        .filter(|tag_id| !kept_tags.contains(tag_id))
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    DuplicatePlan {
        logical_path: set.logical_path,
        content_hash: set.content_hash,
        kept,
        deleted,
        tags_merged,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(file_id: FileId, path: &str, hash: &str) -> FileInfo {
        FileInfo {
            file_id,
            logical_path: LogicalPath::new(path),
            content_hash: hash.to_owned(),
            version_number: 1,
            size: 1,
            short_id_length: 4,
            deleted: false,
            first_recorded_at: 0,
            latest_change_at: 0,
        }
    }

    /// `count` fresh ids, sorted ascending.
    fn sorted_ids(count: usize) -> Vec<FileId> {
        let mut ids: Vec<FileId> = (0..count).map(|_| FileId::new()).collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn no_duplicates_yields_nothing() {
        let files = vec![
            file(FileId::new(), "a.txt", "h1"),
            file(FileId::new(), "b.txt", "h2"),
        ];
        assert!(find_duplicate_sets(files).is_empty());
    }

    #[test]
    fn same_path_different_content_is_not_a_duplicate() {
        let files = vec![
            file(FileId::new(), "a.txt", "h1"),
            file(FileId::new(), "a.txt", "h2"),
        ];
        assert!(find_duplicate_sets(files).is_empty());
    }

    #[test]
    fn same_content_different_path_is_not_a_duplicate() {
        let files = vec![
            file(FileId::new(), "a.txt", "h1"),
            file(FileId::new(), "b.txt", "h1"),
        ];
        assert!(find_duplicate_sets(files).is_empty());
    }

    #[test]
    fn tombstoned_files_are_ignored() {
        let mut dead = file(FileId::new(), "a.txt", "h1");
        dead.deleted = true;
        let files = vec![file(FileId::new(), "a.txt", "h1"), dead];
        assert!(find_duplicate_sets(files).is_empty());
    }

    #[test]
    fn members_are_sorted_regardless_of_listing_order() {
        let ids = sorted_ids(3);
        let files = vec![
            file(ids[2], "a.txt", "h1"),
            file(ids[0], "a.txt", "h1"),
            file(ids[1], "a.txt", "h1"),
        ];
        assert_eq!(find_duplicate_sets(files), vec![DuplicateSet {
            logical_path: LogicalPath::new("a.txt"),
            content_hash: "h1".to_owned(),
            file_ids: ids,
        }]);
    }

    #[test]
    fn sets_are_ordered_by_path_then_hash() {
        let ids = sorted_ids(6);
        let files = vec![
            file(ids[0], "b.txt", "h1"),
            file(ids[1], "b.txt", "h1"),
            file(ids[2], "a.txt", "h2"),
            file(ids[3], "a.txt", "h2"),
            file(ids[4], "a.txt", "h1"),
            file(ids[5], "a.txt", "h1"),
        ];
        let keys: Vec<(String, String)> = find_duplicate_sets(files)
            .into_iter()
            .map(|set| (set.logical_path.as_str().to_owned(), set.content_hash))
            .collect();
        assert_eq!(keys, vec![
            ("a.txt".to_owned(), "h1".to_owned()),
            ("a.txt".to_owned(), "h2".to_owned()),
            ("b.txt".to_owned(), "h1".to_owned()),
        ]);
    }

    #[test]
    fn keeps_the_lowest_id_and_deletes_the_rest() {
        let ids = sorted_ids(3);
        let set = DuplicateSet {
            logical_path: LogicalPath::new("a.txt"),
            content_hash: "h1".to_owned(),
            file_ids: ids.clone(),
        };
        let group = plan_duplicate_group(set, &BTreeMap::new());
        assert_eq!(group.kept, ids[0]);
        assert_eq!(group.deleted, vec![ids[1], ids[2]]);
        assert!(group.tags_merged.is_empty());
    }

    #[test]
    fn merges_the_union_of_missing_tags_onto_the_survivor() {
        let ids = sorted_ids(3);
        let shared = TagId::new();
        let only_second = TagId::new();
        let only_third = TagId::new();
        let tags = BTreeMap::from([
            (ids[0], BTreeSet::from([shared])),
            (ids[1], BTreeSet::from([shared, only_second])),
            (ids[2], BTreeSet::from([only_second, only_third])),
        ]);
        let set = DuplicateSet {
            logical_path: LogicalPath::new("a.txt"),
            content_hash: "h1".to_owned(),
            file_ids: ids,
        };
        let group = plan_duplicate_group(set, &tags);
        let mut expected = vec![only_second, only_third];
        expected.sort_unstable();
        assert_eq!(group.tags_merged, expected);
    }
}
