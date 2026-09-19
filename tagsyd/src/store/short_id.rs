//! Short ids: the shortest unique prefix of an id, shown in listings so a user
//! knows the fewest characters that uniquely identify a file or tag *right
//! now*.
//!
//! This is a **display** concern only. Resolution (turning a term a user typed
//! back into an id) lives in `frontend/api/read.rs` and treats *any* id prefix
//! uniformly — the shortest-unique length computed here has no special
//! standing there. See [`normalize_id_prefix`] for the shared hex-normalization
//! the id-prefix matchers use.
//!
//! Generic over `(table, column)` so files and tags share one implementation.

use rusqlite::{Connection, OptionalExtension};
use tagsy_core::{FileId, TagId};

use super::CatalogStore;
use super::types::DatabaseError;

/// Number of leading characters two strings share.
///
/// Operates on `char`s; ids are ASCII hex so this is equivalent to bytes.
pub(super) fn common_prefix_length(a: &str, b: &str) -> usize {
    a.chars().zip(b.chars()).take_while(|(x, y)| x == y).count()
}

/// Normalize a user-supplied id or short-id into the canonical lowercase-hex
/// form used for prefix matching.
///
/// Accepts hyphenated UUIDs, full simple-hex ids, and short prefixes of either.
/// Hyphens are stripped (so a pasted full UUID resolves) and the result is
/// lowercased. Returns `None` if any remaining character is not a hex digit —
/// this both rejects junk early and guarantees the value is safe to splice into
/// a `LIKE` pattern (no wildcards).
pub fn normalize_id_prefix(input: &str) -> Option<String> {
    let cleaned: String = input
        .chars()
        .filter(|character| *character != '-')
        .collect();
    if cleaned.is_empty()
        || !cleaned
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return None;
    }
    Some(cleaned.to_ascii_lowercase())
}

/// Shortest prefix of `full` that no other value in `table.column` shares.
///
/// The trick that makes this scale: a value only ever needs to be
/// distinguished from its two lexicographic *neighbours* (the value
/// immediately before and after it, sorted). If a prefix separates you from
/// both neighbours, it separates you from everyone. So this is two indexed
/// range lookups against the column's primary-key index — O(log n) — not a
/// scan.
///
/// Ids are stored in canonical simple-hex form, so lexicographic ordering on
/// the stored strings is a clean hex ordering and prefixes never straddle a
/// separator. Returns the full length when `full` has no neighbours.
///
/// `table` / `column` are internal constants, never user input.
fn shortest_unique_prefix_length(
    connection: &Connection,
    table: &str,
    column: &str,
    full: &str,
) -> Result<usize, DatabaseError> {
    // Immediate lexicographic predecessor, if any.
    let predecessor: Option<String> = connection
        .query_row(
            &format!(
                "SELECT {column} FROM {table} WHERE {column} < ?1 ORDER BY {column} DESC LIMIT 1"
            ),
            [&full],
            |row| row.get(0),
        )
        .optional()?;

    // Immediate lexicographic successor, if any.
    let successor: Option<String> = connection
        .query_row(
            &format!(
                "SELECT {column} FROM {table} WHERE {column} > ?1 ORDER BY {column} ASC LIMIT 1"
            ),
            [&full],
            |row| row.get(0),
        )
        .optional()?;

    // The prefix must be one longer than the longest prefix we share with
    // either neighbour, so that it excludes both of them.
    let mut required = 0;
    for neighbour in [predecessor, successor].into_iter().flatten() {
        let shared = common_prefix_length(full, &neighbour);
        required = required.max(shared + 1);
    }

    Ok(required.clamp(1, full.len()))
}

impl CatalogStore {
    /// Compute the shortest unique prefix of `file_id` among **all** files in
    /// the database — the "short id" shown in listings, à la `jj`/`git`.
    ///
    /// The result is the fewest leading hex characters of `file_id` that no
    /// other file's id shares; see `shortest_unique_prefix_length` for why
    /// this costs two indexed lookups rather than a scan.
    ///
    /// Note: the returned length reflects the database *at call time*. It is
    /// not stored and not stable across concurrent inserts — a prefix that
    /// is unique now may become ambiguous if a colliding file is added
    /// later. That is the intended behavior (resolution re-checks
    /// uniqueness on use).
    ///
    /// Returns the full id length if the file has no neighbours (e.g. it is the
    /// only file). Returns `MissingFile` if `file_id` is not in `files`.
    pub fn shorten_file_id(&self, file_id: FileId) -> Result<usize, DatabaseError> {
        if !self.file_exists(file_id)? {
            return Err(DatabaseError::MissingFile);
        }

        shortest_unique_prefix_length(&self.connection, "files_v2", "id", &file_id.to_string())
    }

    /// Compute the shortest unique prefix of `tag_id` among **all** tags — the
    /// "short id" shown in listings. The tag counterpart of
    /// [`shorten_file_id`](Self::shorten_file_id); see it for the
    /// neighbour-based reasoning and the caveats about the length not being
    /// stable across concurrent inserts.
    ///
    /// Returns `MissingTag` if `tag_id` is not in `tags`.
    pub fn shorten_tag_id(&self, tag_id: TagId) -> Result<usize, DatabaseError> {
        if !self.tag_exists(tag_id)? {
            return Err(DatabaseError::MissingTag);
        }

        shortest_unique_prefix_length(&self.connection, "tags_v2", "id", &tag_id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use tagsy_core::LogicalPath;

    use super::*;
    use crate::store::DeletedRule;
    use crate::store::fixtures::{dot_style, file_id_from_hex, memory_db, tag_id_from_hex};

    #[test]
    fn shorten_file_id_single_file_needs_one_char() {
        let database = memory_db();
        let only = file_id_from_hex("00000000000000000000000000000001");
        database.add_file(only, &LogicalPath::new("a"), 0).unwrap();

        // No neighbours -> a single character already uniquely identifies it.
        assert_eq!(database.shorten_file_id(only).unwrap(), 1);
    }

    #[test]
    fn shorten_file_id_grows_prefix_to_disambiguate_neighbours() {
        let database = memory_db();
        // Three ids: two share the leading `abcd`, one is far away.
        let shared_a = file_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = file_id_from_hex("abcd000000000000000000000000000b");
        let far = file_id_from_hex("ffff000000000000000000000000000f");
        for (id, name) in [(shared_a, "a"), (shared_b, "b"), (far, "c")] {
            database.add_file(id, &LogicalPath::new(name), 0).unwrap();
        }

        // shared_a and shared_b agree on `abcd00...000` up to the final hex
        // char, so they must be distinguished at the last differing position.
        let len_a = database.shorten_file_id(shared_a).unwrap();
        let len_b = database.shorten_file_id(shared_b).unwrap();
        let a = shared_a.to_string();
        let b = shared_b.to_string();
        // The prefix of each must exclude the other.
        assert!(!b.starts_with(&a[..len_a]));
        assert!(!a.starts_with(&b[..len_b]));

        // The far id only needs one char (`f`), since neither neighbour shares
        // its first character.
        assert_eq!(database.shorten_file_id(far).unwrap(), 1);
    }

    #[test]
    fn shorten_file_id_missing_is_not_found() {
        let database = memory_db();
        assert!(matches!(
            database.shorten_file_id(FileId::new()),
            Err(DatabaseError::MissingFile)
        ));
    }

    #[test]
    fn get_all_files_reports_short_id_length() {
        let mut database = memory_db();
        let shared_a = file_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = file_id_from_hex("abcd000000000000000000000000000b");
        for (id, name) in [(shared_a, "a"), (shared_b, "b")] {
            database.add_file(id, &LogicalPath::new(name), 0).unwrap();
            database.record_version(id, "hash", "local", 1).unwrap();
        }

        let files = database.get_all_files(DeletedRule::Exclude).unwrap();
        // Both ids share all but the final character, so the short id must be
        // the full length to disambiguate.
        for info in &files {
            let full = info.file_id.to_string();
            assert_eq!(info.short_id_length, full.len());
        }
    }

    #[test]
    fn shorten_then_resolve_tag_roundtrips() {
        let database = memory_db();
        let shared_a = tag_id_from_hex("abcd000000000000000000000000000a");
        let shared_b = tag_id_from_hex("abcd000000000000000000000000000b");
        let far = tag_id_from_hex("ffff000000000000000000000000000f");
        for (id, name) in [(shared_a, "a"), (shared_b, "b"), (far, "c")] {
            database.add_tag(id, name, &dot_style("red"), 1).unwrap();
        }

        // Each tag's displayed short id length is at least enough to be unique;
        // the shared pair need the full length, the far tag needs one char.
        assert_eq!(database.shorten_tag_id(far).unwrap(), 1);
        let full_a = shared_a.to_string();
        let len_a = database.shorten_tag_id(shared_a).unwrap();
        assert!(!shared_b.to_string().starts_with(&full_a[..len_a]));
    }
}
