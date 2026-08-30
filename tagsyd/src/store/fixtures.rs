//! Test fixtures shared by every module in `store`.

use tagsy_core::{FileId, TagId, TagStyle};

use super::CatalogStore;

pub(super) fn memory_db() -> CatalogStore {
    CatalogStore::initialize(":memory:").expect("open in-memory db")
}

/// A default [`TagStyle`] carrying the given dot color. Tags historically keyed
/// on a single "color", which is now `style.dot_color`; this keeps the many
/// `add_tag(id, name, <color>, ts)` test call sites terse and preserves their
/// original intent.
pub(super) fn dot_style(color: &str) -> TagStyle {
    TagStyle {
        dot_color: color.to_owned(),
        ..TagStyle::default()
    }
}

/// Build a `FileId` from a 32-char hex string so tests can control the exact
/// prefix relationships between ids.
pub(super) fn file_id_from_hex(hex: &str) -> FileId {
    FileId::from_string(hex).expect("valid hex uuid")
}

/// Build a `TagId` from a 32-char hex string so tests can control the exact
/// prefix relationships between ids.
pub(super) fn tag_id_from_hex(hex: &str) -> TagId {
    TagId::from_string(hex).expect("valid hex uuid")
}
