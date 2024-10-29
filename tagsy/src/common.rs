//! Shared backend helpers used across the dispatch arms: flag→rule
//! translation, short-id resolution, and the tag-name *enrichment* layer —
//! the [`NameCache`] plus the `tags_by_file` / `tags_by_tag` fan-outs that turn
//! bare ids into the display names the tables show. Kept here (rather than in
//! [`crate::run`]) so the pure rendering in [`crate::output`] never has to make
//! a backend call.

use std::collections::HashMap;

use tagsy_api::{Backend, DeletedRule, SubtagRule, Tag};
use tagsy_core::{FileId, FileInfo, TagId};
use tagsy_ipc::IpcBackend;

/// Memoizes `TagId → name` lookups within a single command so a table with the
/// same tag on many rows resolves each name once. See [`resolve_tag_names`].
pub type NameCache = HashMap<TagId, String>;

/// Translate the `--include-subtags` (or `--recursive`) flag into a
/// [`SubtagRule`].
pub fn subtag_rule(include: bool) -> SubtagRule {
    match include {
        true => SubtagRule::Include,
        false => SubtagRule::Exclude,
    }
}

/// Translate the `--deleted` flag into a [`DeletedRule`]. `true` means
/// search-over-tombstones (`Include`, which returns *only* tombstoned rows
/// per `ApiService::search`'s semantics); `false` is the standard live-only
/// search.
pub fn deleted_rule(deleted: bool) -> DeletedRule {
    match deleted {
        true => DeletedRule::Include,
        false => DeletedRule::Exclude,
    }
}

/// Resolve `tag_ids` to display names, one `get_tag` per *distinct* id,
/// memoized in ``name_cache`` across calls so a tag seen on many files/tags is
/// fetched once. An id that no longer resolves (deleted) falls back to its
/// stringified form.
pub async fn resolve_tag_names(
    backend: &IpcBackend,
    name_cache: &mut NameCache,
    tag_ids: &[TagId],
) -> Result<Vec<String>, String> {
    let mut names = Vec::with_capacity(tag_ids.len());

    for tag_id in tag_ids {
        if let Some(name) = name_cache.get(tag_id) {
            names.push(name.clone());
            continue;
        }

        let name = match backend.get_tag(*tag_id, DeletedRule::Exclude).await {
            Ok(tag) => tag.name,
            Err(tagsy_api::ApiError::UnknownId) => tag_id.to_string(),
            Err(error) => return Err(error.to_string()),
        };

        name_cache.insert(*tag_id, name.clone());
        names.push(name);
    }

    Ok(names)
}

/// Materialize a set of tag ids into full [`Tag`] rows via `get_tag`, one
/// lookup per id. Ids that no longer resolve (deleted) are skipped.
pub async fn tags_from_ids(
    backend: &IpcBackend,
    tag_ids: impl IntoIterator<Item = TagId>,
) -> Result<Vec<Tag>, String> {
    let mut tags = Vec::new();

    for tag_id in tag_ids {
        match backend.get_tag(tag_id, DeletedRule::Exclude).await {
            Ok(tag) => tags.push(tag),
            Err(tagsy_api::ApiError::UnknownId) => continue,
            Err(error) => return Err(error.to_string()),
        }
    }

    Ok(tags)
}

/// Build the per-file tag-name lists shown in [`file_table`], one
/// `tags_for_file` lookup per file. Names are resolved on demand via
/// [`resolve_tag_names`], sharing `name_cache` so repeated tags cost one
/// lookup. `rule` controls whether the tag hierarchy is walked (see
/// `--include-subtags`).
pub async fn tags_by_file(
    backend: &IpcBackend,
    name_cache: &mut NameCache,
    files: &[FileInfo],
    rule: SubtagRule,
) -> Result<HashMap<FileId, Vec<String>>, String> {
    let mut map = HashMap::with_capacity(files.len());

    for file in files {
        let tag_ids = backend
            .tags_for_file(file.file_id, rule)
            .await
            .map_err(|error| error.to_string())?;

        let names = resolve_tag_names(backend, name_cache, &tag_ids).await?;
        map.insert(file.file_id, names);
    }

    Ok(map)
}

/// Build the per-tag applied-tag name lists shown in [`tag_table`], one
/// `tags_for_tag` lookup per tag. The tag analogue of [`tags_by_file`]; shares
/// the same `name_cache`. `rule` controls whether the tag hierarchy is walked.
pub async fn tags_by_tag(
    backend: &IpcBackend,
    name_cache: &mut NameCache,
    tags: &[Tag],
    rule: SubtagRule,
) -> Result<HashMap<TagId, Vec<String>>, String> {
    let mut map = HashMap::with_capacity(tags.len());

    for tag in tags {
        let applied_tag_ids = backend
            .tags_for_tag(tag.id, rule)
            .await
            .map_err(|error| error.to_string())?;

        let names = resolve_tag_names(backend, name_cache, &applied_tag_ids).await?;
        map.insert(tag.id, names);
    }

    Ok(map)
}

/// Resolve a user-supplied file id — a full id or any unambiguous short-id
/// prefix — to a full [`FileId`] via the daemon.
///
/// This is the single entry point every command that accepts a file id should
/// use, so short ids work uniformly everywhere. Resolution is done daemon-side
/// against all files, so uniqueness is re-checked at use time (a prefix that
/// was unique when displayed may since have become ambiguous).
pub async fn resolve_file_id(backend: &IpcBackend, input: &str) -> Result<FileId, String> {
    backend
        .resolve_file_id(input.to_owned())
        .await
        .map_err(|error| match error {
            tagsy_api::ApiError::UnknownId => format!("no file matches id '{input}'"),
            other => other.to_string(),
        })
}

/// Resolve a user-supplied tag id — a full id or any unambiguous short-id
/// prefix (as shown by `search`) — to a full [`TagId`] via the daemon.
///
/// The tag counterpart of [`resolve_file_id`]. Every command that accepts a tag
/// id should route through this so short ids work uniformly, and so uniqueness
/// is re-checked daemon-side at use time.
pub async fn resolve_tag_id(backend: &IpcBackend, input: &str) -> Result<TagId, String> {
    backend
        .resolve_tag_id(input.to_owned())
        .await
        .map_err(|error| match error {
            tagsy_api::ApiError::UnknownId => format!("no tag matches id '{input}'"),
            other => other.to_string(),
        })
}

/// Open `path` in the user's `$EDITOR` (falling back to `vi`), blocking until
/// it exits. A non-zero editor exit is treated as an abort.
pub fn open_in_editor(path: &std::path::Path) -> Result<(), String> {
    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_owned());
    let status = std::process::Command::new(&editor)
        .arg(path)
        .status()
        .map_err(|error| format!("failed to launch editor '{editor}': {error}"))?;

    if !status.success() {
        return Err(format!("editor '{editor}' exited without success"));
    }

    Ok(())
}
