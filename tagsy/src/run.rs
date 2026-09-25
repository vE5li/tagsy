//! Command dispatch: one arm per [`Commands`] variant, driving the daemon
//! [`Backend`] and rendering results through [`crate::output`].

use std::collections::HashMap;

use serde_json::json;
use tagsy_api::{
    Backend, BorderStyle, DeletedRule, DuplicateDeletionOutcome, PurgeOutcome, SubtagRule, Tag,
    TagShape, TagStyle,
};
use tagsy_core::FileInfo;
use tagsy_ipc::IpcBackend;

use crate::commands::{Commands, StyleArgs};
use crate::output::{
    DuplicateGroupTags, OutputMode, emit_connected_peers, emit_duplicate_deletion_outcome,
    emit_files, emit_operations, emit_purge_outcome, emit_scalar, emit_tags, emit_tags_and_files,
    print_json, print_tag_rule_report,
};
use crate::{common, upload};

/// Print `files` the way every command that shows files does: the shared file
/// table (or `FileRow` JSON), each row with its direct tags.
async fn show_files(
    backend: &IpcBackend,
    output_mode: OutputMode,
    files: &[FileInfo],
) -> Result<(), String> {
    let mut name_cache = common::NameCache::new();
    let file_tags =
        common::tags_by_file(backend, &mut name_cache, files, SubtagRule::Exclude).await?;
    emit_files(output_mode, files, &file_tags);
    Ok(())
}

/// Print `tags` the way every command that shows tags does: the shared tag
/// table (or `TagRow` JSON), each row with the tags applied to it.
async fn show_tags(
    backend: &IpcBackend,
    output_mode: OutputMode,
    tags: &[Tag],
) -> Result<(), String> {
    let mut name_cache = common::NameCache::new();
    let tag_tags = common::tags_by_tag(backend, &mut name_cache, tags, SubtagRule::Exclude).await?;
    emit_tags(output_mode, tags, &tag_tags);
    Ok(())
}

/// Print a purge's files. Their tags come with the outcome: once purged, a
/// file's tag relations are gone, so they cannot be looked up afterwards.
async fn show_purge_outcome(
    backend: &IpcBackend,
    output_mode: OutputMode,
    noun: &str,
    outcome: &PurgeOutcome,
) -> Result<(), String> {
    let mut name_cache = common::NameCache::new();
    let mut file_tags = HashMap::with_capacity(outcome.purged.len());
    for purged in &outcome.purged {
        let names = common::resolve_tag_names(backend, &mut name_cache, &purged.tags).await?;
        file_tags.insert(purged.file.file_id, names);
    }
    emit_purge_outcome(output_mode, noun, outcome, &file_tags);
    Ok(())
}

/// Print `delete-duplicates`' sets, every file with its direct tags as they
/// now stand, plus the names of the tags merged onto each survivor.
async fn show_duplicate_deletion_outcome(
    backend: &IpcBackend,
    output_mode: OutputMode,
    outcome: &DuplicateDeletionOutcome,
) -> Result<(), String> {
    let mut name_cache = common::NameCache::new();
    let files: Vec<FileInfo> = outcome
        .groups
        .iter()
        .flat_map(|group| std::iter::once(&group.kept).chain(&group.deleted))
        .cloned()
        .collect();
    let file_tags =
        common::tags_by_file(backend, &mut name_cache, &files, SubtagRule::Exclude).await?;
    let mut merged = Vec::with_capacity(outcome.groups.len());
    for group in &outcome.groups {
        merged.push(common::resolve_tag_names(backend, &mut name_cache, &group.tags_merged).await?);
    }
    emit_duplicate_deletion_outcome(output_mode, outcome, &DuplicateGroupTags {
        files: file_tags,
        merged,
    });
    Ok(())
}

/// A side note for the human at the terminal (a download's destination, an
/// edit that changed nothing). Goes to stderr in both output modes, so it
/// never breaks the uniform stdout output or a JSON consumer.
fn note(message: impl AsRef<str>) {
    eprintln!("{}", message.as_ref());
}

/// Apply the CLI's optional style flags on top of a base [`TagStyle`], leaving
/// any unspecified property untouched. `create-tag` passes
/// `TagStyle::default()` as the base; `set-tag-style` passes the tag's current
/// style so it edits in place. Unrecognized enum spellings fall back to the
/// default variant (same forgiving rule as the wire/SQL layer).
fn apply_style_args(mut base: TagStyle, args: &StyleArgs) -> TagStyle {
    if let Some(v) = &args.dot_color {
        base.dot_color = v.clone();
    }
    if let Some(v) = &args.background {
        base.background = v.clone();
    }
    if let Some(v) = &args.gradient {
        base.gradient = v.clone();
    }
    if let Some(v) = &args.foreground {
        base.foreground = v.clone();
    }
    if let Some(v) = &args.border {
        base.border = v.clone();
    }
    if let Some(v) = args.border_width {
        base.border_width = v;
    }
    if let Some(v) = &args.border_style {
        base.border_style = BorderStyle::from_str_or_default(v);
    }
    if let Some(v) = &args.shape {
        base.shape = TagShape::from_str_or_default(v);
    }
    if let Some(v) = args.shadow {
        base.shadow = v;
    }
    if let Some(v) = &args.shadow_color {
        base.shadow_color = v.clone();
    }
    base
}

pub async fn run(
    backend: &IpcBackend,
    command: Commands,
    output_mode: OutputMode,
) -> Result<(), String> {
    match command {
        Commands::Upload {
            paths,
            tags,
            keep,
            hidden,
            many,
        } => {
            // Expand the file/directory arguments into the flat list of files to
            // upload before touching the daemon, so the count guard below can
            // trip without any bytes having moved.
            let planned = upload::expand_paths(&paths, hidden)?;

            if planned.len() > upload::MANY_THRESHOLD && !many {
                return Err(format!(
                    "refusing to upload {} files; pass --many to confirm",
                    planned.len()
                ));
            }

            // Resolve each `--tag` argument (full id or short prefix) via the
            // daemon, so tagging on upload accepts short ids like every other
            // tag-id command. Resolved once and applied to every file.
            let mut resolved_tags = Vec::with_capacity(tags.len());
            for tag in &tags {
                resolved_tags
                    .push(common::resolve_tag_id(backend, tag, DeletedRule::Exclude).await?);
            }

            let mut files = Vec::with_capacity(planned.len());

            // Fail fast: on the first upload error we stop, leaving any
            // already-uploaded files in place.
            for item in &planned {
                // The daemon copies the file into its outbox before answering,
                // so the source may be deleted as soon as this returns. It
                // answers with the file as recorded.
                let file = backend
                    .upload_file(
                        item.disk_path.clone(),
                        item.path_name.clone(),
                        resolved_tags.clone(),
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                if !keep {
                    std::fs::remove_file(&item.disk_path).map_err(|error| {
                        format!(
                            "uploaded as file {}, but failed to delete {}: {error}",
                            file.file_id.to_string(),
                            item.disk_path.display()
                        )
                    })?;
                }

                files.push(file);
            }

            show_files(backend, output_mode, &files).await?;
        }
        Commands::CreateTag { name, style } => {
            let style = apply_style_args(TagStyle::default(), &style);
            let tag = backend
                .create_tag(name, style)
                .await
                .map_err(|error| error.to_string())?;
            show_tags(backend, output_mode, &[tag]).await?;
        }
        Commands::Search {
            query,
            include_subtags,
            deleted,
        } => {
            let query = query.join(" ");
            // The query returns full rows for exactly the matched set (files and
            // tags), so no whole-store listing is needed to render them.
            let result = backend
                .search(
                    query,
                    common::subtag_rule(include_subtags),
                    common::deleted_rule(deleted),
                )
                .await
                .map_err(|error| error.to_string())?;
            let files = result.files;
            let tags = result.tags;

            let mut name_cache = common::NameCache::new();
            // The Tags column shows each row's own direct tags, regardless of
            // how the search matched it.
            let file_tags =
                common::tags_by_file(backend, &mut name_cache, &files, SubtagRule::Exclude).await?;
            let tag_tags =
                common::tags_by_tag(backend, &mut name_cache, &tags, SubtagRule::Exclude).await?;

            emit_tags_and_files(output_mode, &tags, &files, &tag_tags, &file_tags);
        }
        // The `edit` flow — a thin driver over the daemon's stateless edit protocol.
        //
        // The daemon owns the whole workflow (local-path vs. peer-fetch decision,
        // extension-preserving naming, hashing, no-op detection, upload, and temp
        // cleanup). This CLI's job is only:
        //
        // 1. Ask the daemon to prepare an editable path (`begin_edit`).
        // 2. Launch `$EDITOR` on it, blocking until it exits.
        // 3. Hand the path back with `finish_edit` (uploads iff the bytes changed) on success, or
        //    `cancel_edit` on editor failure.
        //
        // A crash between (1) and (3) only leaks a temp file, which the daemon
        // bulk-wipes on next start.
        Commands::Edit { id } => {
            let file_id = common::resolve_file_id(backend, &id, DeletedRule::Exclude).await?;

            let path = match backend.begin_edit(file_id).await {
                Ok(path) => path,
                Err(tagsy_api::ApiError::UnknownId) => {
                    return Err(format!("unknown file id: {}", file_id.to_string()));
                }
                Err(error) => return Err(error.to_string()),
            };

            // Launch the editor. On failure, tell the daemon to clean up and return
            // the editor error to the user — we do not want a stale temp to linger
            // until the next daemon restart.
            if let Err(error) = common::open_in_editor(&path) {
                let _ = backend.cancel_edit(path).await;
                return Err(error);
            }

            let outcome = backend
                .finish_edit(file_id, path)
                .await
                .map_err(|error| error.to_string())?;

            if !outcome.changed {
                note("No changes");
            }
            show_files(backend, output_mode, &[outcome.file]).await?;
        }

        // Shares its start with the edit flow: locate the file's bytes — reading
        // the real file if it lives in a local sync directory, otherwise fetching
        // them from a peer — then, instead of editing, copy them into the current
        // directory.
        Commands::Download { id } => {
            let file_id = common::resolve_file_id(backend, &id, DeletedRule::Exclude).await?;

            // Pull the file's metadata once (a single by-id lookup): we need its content
            // hash to fetch (if it isn't local) and its logical path to pick a sensible
            // output filename.
            let file = match backend.get_file(file_id, DeletedRule::Exclude).await {
                Ok(file) => file,
                Err(tagsy_api::ApiError::UnknownId) => {
                    return Err(format!("unknown file id: {}", file_id.to_string()));
                }
                Err(error) => return Err(error.to_string()),
            };

            // Either the file already lives in a local sync directory (copy it out,
            // leaving the real file untouched) or we fetch it, which stages a
            // CLI-owned temp we can move into place.
            let local_path = backend
                .local_path_for_file(file_id)
                .await
                .map_err(|error| error.to_string())?;

            // Name the download after the file's logical path's final component, so a
            // nested `foo/bar/name.txt` lands as `name.txt`. Fall back to the file id
            // if the logical path has no usable component.
            let logical = file.logical_path.to_string();
            let file_name = logical
                .rsplit('/')
                .find(|segment| !segment.is_empty())
                .unwrap_or(&logical);

            let file_name = match file_name.is_empty() {
                true => file_id.to_string(),
                false => file_name.to_owned(),
            };

            if let Some(path) = local_path {
                std::fs::copy(&path, &file_name).map_err(|error| {
                    format!(
                        "failed to copy local file {} to {file_name}: {error}",
                        path.display()
                    )
                })?;
            } else {
                let temp_path = backend
                    .fetch_file(file_id, file.content_hash.clone())
                    .await
                    .map_err(|error| error.to_string())?;

                // Move the staged temp into place. A plain rename works when the
                // fetch temp dir and the destination are on the same filesystem;
                // only a *cross-filesystem* rename (`EXDEV`) needs the
                // copy-then-remove fallback. Any other rename failure (permission
                // denied, no space, ...) is a real error and is propagated as-is
                // rather than masked by a copy that would fail the same way.
                if let Err(rename_error) = std::fs::rename(&temp_path, &file_name) {
                    // EXDEV (errno 18 on Linux) is "cross-device link" — the one
                    // case a rename cannot handle but a copy can.
                    if rename_error.raw_os_error() != Some(EXDEV) {
                        let _ = std::fs::remove_file(&temp_path);
                        return Err(format!(
                            "failed to move downloaded file into {file_name}: {rename_error}"
                        ));
                    }

                    let copied = std::fs::copy(&temp_path, &file_name);
                    let _ = std::fs::remove_file(&temp_path);

                    copied.map_err(|error| {
                        format!(
                            "failed to move downloaded file into {file_name} across filesystems: \
                             {error}"
                        )
                    })?;
                }

                // The daemon staged the fetched bytes in a per-request subdirectory
                // (`<fetch_temp_dir>/<uuid>/<logical_basename>`). We just moved the
                // file out of it, so the subdir is now an empty leftover. Remove it
                // (best-effort — the daemon bulk-wipes `fetch_temp_dir` on next start
                // regardless).
                if let Some(parent) = temp_path.parent() {
                    let _ = std::fs::remove_dir(parent);
                }
            }

            note(format!("Downloaded to {file_name}"));
            show_files(backend, output_mode, &[file]).await?;
        }
        Commands::DeleteFile { id } => {
            let file_id = common::resolve_file_id(backend, &id, DeletedRule::Exclude).await?;
            let file = backend
                .delete_file(file_id)
                .await
                .map_err(|error| error.to_string())?;
            show_files(backend, output_mode, &[file]).await?;
        }
        Commands::RestoreFile { id } => {
            // The restore path names a *deleted* file, so resolution must see
            // tombstoned rows.
            let file_id = common::resolve_file_id(backend, &id, DeletedRule::Include).await?;
            let file = backend
                .restore_file(file_id)
                .await
                .map_err(|error| error.to_string())?;
            show_files(backend, output_mode, &[file]).await?;
        }
        Commands::DeleteTag { tag_id } => {
            let tag_id = common::resolve_tag_id(backend, &tag_id, DeletedRule::Exclude).await?;
            let tag = backend
                .delete_tag(tag_id)
                .await
                .map_err(|error| error.to_string())?;
            show_tags(backend, output_mode, &[tag]).await?;
        }
        Commands::RestoreTag { tag_id } => {
            // Same as RestoreFile: a deleted tag must be resolvable.
            let tag_id = common::resolve_tag_id(backend, &tag_id, DeletedRule::Include).await?;
            let tag = backend
                .restore_tag(tag_id)
                .await
                .map_err(|error| error.to_string())?;
            show_tags(backend, output_mode, &[tag]).await?;
        }
        Commands::Tag { id, tag_ids } => {
            let file_id = common::resolve_file_id(backend, &id, DeletedRule::Exclude).await?;

            // Each call answers with the file as it stands after that tag; the
            // last answer carries them all.
            let mut file = None;
            for tag in &tag_ids {
                let tag_id = common::resolve_tag_id(backend, tag, DeletedRule::Exclude).await?;
                file = Some(
                    backend
                        .tag_file(tag_id, file_id)
                        .await
                        .map_err(|error| error.to_string())?,
                );
            }

            let files: Vec<FileInfo> = file.into_iter().collect();
            show_files(backend, output_mode, &files).await?;
        }
        Commands::Untag { id, tag_ids } => {
            let file_id = common::resolve_file_id(backend, &id, DeletedRule::Exclude).await?;

            let mut file = None;
            for tag in &tag_ids {
                let tag_id = common::resolve_tag_id(backend, tag, DeletedRule::Exclude).await?;
                file = Some(
                    backend
                        .untag_file(tag_id, file_id)
                        .await
                        .map_err(|error| error.to_string())?,
                );
            }

            let files: Vec<FileInfo> = file.into_iter().collect();
            show_files(backend, output_mode, &files).await?;
        }
        Commands::TagsForFile {
            id,
            include_subtags,
        } => {
            let file_id = common::resolve_file_id(backend, &id, DeletedRule::Exclude).await?;
            let tag_ids = backend
                .tags_for_file(file_id, common::subtag_rule(include_subtags))
                .await
                .map_err(|error| error.to_string())?;
            let tags = common::tags_from_ids(backend, tag_ids).await?;
            let mut name_cache = common::NameCache::new();
            // The Tags column shows each tag's own direct tags, regardless of
            // how the command matched them.
            let tag_tags =
                common::tags_by_tag(backend, &mut name_cache, &tags, SubtagRule::Exclude).await?;

            emit_tags(output_mode, &tags, &tag_tags);
        }
        Commands::RenameTag { tag_id, name } => {
            let tag_id = common::resolve_tag_id(backend, &tag_id, DeletedRule::Exclude).await?;
            let tag = backend
                .rename_tag(tag_id, name)
                .await
                .map_err(|error| error.to_string())?;
            show_tags(backend, output_mode, &[tag]).await?;
        }
        Commands::SetTagStyle { tag_id, style } => {
            let tag_id = common::resolve_tag_id(backend, &tag_id, DeletedRule::Exclude).await?;

            // Fetch the current style so unspecified flags are preserved — a
            // restyle replaces the whole style, so we must send the merged value.
            let current = backend
                .get_tag(tag_id, DeletedRule::Exclude)
                .await
                .map_err(|error| error.to_string())?;
            let merged = apply_style_args(current.style, &style);

            let tag = backend
                .set_tag_style(tag_id, merged)
                .await
                .map_err(|error| error.to_string())?;
            show_tags(backend, output_mode, &[tag]).await?;
        }
        Commands::Move { id, path } => {
            let file_id = common::resolve_file_id(backend, &id, DeletedRule::Exclude).await?;
            let file = backend
                .move_file(file_id, path)
                .await
                .map_err(|error| error.to_string())?;
            show_files(backend, output_mode, &[file]).await?;
        }
        Commands::TagTag { child, parents } => {
            let child_id = common::resolve_tag_id(backend, &child, DeletedRule::Exclude).await?;

            // As with `tag`: the last answer shows the child with every parent.
            let mut tag = None;
            for parent in &parents {
                let parent_id =
                    common::resolve_tag_id(backend, parent, DeletedRule::Exclude).await?;
                tag = Some(
                    backend
                        .tag_tag(parent_id, child_id)
                        .await
                        .map_err(|error| error.to_string())?,
                );
            }

            let tags: Vec<Tag> = tag.into_iter().collect();
            show_tags(backend, output_mode, &tags).await?;
        }
        Commands::UntagTag { child, parents } => {
            let child_id = common::resolve_tag_id(backend, &child, DeletedRule::Exclude).await?;

            let mut tag = None;
            for parent in &parents {
                let parent_id =
                    common::resolve_tag_id(backend, parent, DeletedRule::Exclude).await?;
                tag = Some(
                    backend
                        .untag_tag(parent_id, child_id)
                        .await
                        .map_err(|error| error.to_string())?,
                );
            }

            let tags: Vec<Tag> = tag.into_iter().collect();
            show_tags(backend, output_mode, &tags).await?;
        }
        Commands::Subtags { tag_id, recursive } => {
            let tag_id = common::resolve_tag_id(backend, &tag_id, DeletedRule::Exclude).await?;
            let subtag_ids = backend
                .subtags_for_tag(tag_id, common::subtag_rule(recursive))
                .await
                .map_err(|error| error.to_string())?;
            let tags = common::tags_from_ids(backend, subtag_ids).await?;
            let mut name_cache = common::NameCache::new();
            // The Tags column shows each tag's own direct tags, regardless of
            // how the command matched them.
            let tag_tags =
                common::tags_by_tag(backend, &mut name_cache, &tags, SubtagRule::Exclude).await?;

            emit_tags(output_mode, &tags, &tag_tags);
        }
        Commands::ListOperations => {
            let operations = backend
                .list_operations()
                .await
                .map_err(|error| error.to_string())?;

            emit_operations(output_mode, &operations);
        }
        Commands::ConnectedPeers => {
            let peers = backend
                .connected_peers()
                .await
                .map_err(|error| error.to_string())?;

            emit_connected_peers(output_mode, &peers);
        }
        Commands::Activity => {
            let activity = backend
                .activity()
                .await
                .map_err(|error| error.to_string())?;

            let inbox = |inbox: &tagsy_api::InboxActivity| {
                format!(
                    "{} ({} queued, {} processed)",
                    if inbox.busy { "busy" } else { "idle" },
                    inbox.queued,
                    inbox.processed
                )
            };
            emit_scalar(
                output_mode,
                format!(
                    "{}\ncatalog:          {}\nsync directories: {}{}\npeer sessions:    {} busy, \
                     {} frames queued, {} processed\nfilesystem events debouncing: {}\npulls: {} \
                     running, {} queued",
                    if activity.is_idle() { "idle" } else { "busy" },
                    inbox(&activity.catalog),
                    inbox(&activity.sync_directories),
                    if activity.initial_scan_complete {
                        ""
                    } else {
                        " [startup scan running]"
                    },
                    activity.peer_sessions.busy,
                    activity.peer_sessions.outbound_queued,
                    activity.peer_sessions.processed,
                    activity.pending_filesystem_events,
                    activity.pulls_running,
                    activity.pulls_queued,
                ),
                json!({
                    "idle": activity.is_idle(),
                    "activity": activity,
                }),
            );
        }
        Commands::PurgePreviews => {
            let purged = backend
                .purge_previews()
                .await
                .map_err(|error| error.to_string())?;

            emit_scalar(
                output_mode,
                format!("Purged {purged} cached previews"),
                json!({ "purged": purged }),
            );
        }
        Commands::PurgeBroken { dry_run } => {
            let outcome = backend
                .purge_broken(dry_run)
                .await
                .map_err(|error| error.to_string())?;
            show_purge_outcome(backend, output_mode, "broken", &outcome).await?;
        }
        Commands::PurgeDeleted { dry_run } => {
            let outcome = backend
                .purge_deleted(dry_run)
                .await
                .map_err(|error| error.to_string())?;
            show_purge_outcome(backend, output_mode, "deleted", &outcome).await?;
        }
        Commands::DeleteDuplicates { dry_run } => {
            let outcome = backend
                .delete_duplicates(dry_run)
                .await
                .map_err(|error| error.to_string())?;
            show_duplicate_deletion_outcome(backend, output_mode, &outcome).await?;
        }
        Commands::Backup => {
            let outcome = backend.backup().await.map_err(|error| error.to_string())?;

            emit_scalar(
                output_mode,
                format!(
                    "Wrote backup to {} ({} files, {} bytes)",
                    outcome.path.display(),
                    outcome.file_count,
                    outcome.bytes_written,
                ),
                json!({
                    "path": outcome.path,
                    "file_count": outcome.file_count,
                    "bytes_written": outcome.bytes_written,
                }),
            );
        }
        Commands::Retag { dry_run, check } => {
            // Always fetch the diagnostics, even for a real run. A rule that
            // failed to compile is exactly the situation someone runs `retag`
            // to recover from, and silently retagging with it still broken
            // would look like the command simply did nothing.
            let report = backend
                .tag_rule_report()
                .await
                .map_err(|error| error.to_string())?;

            if check {
                match output_mode {
                    OutputMode::Human => print_tag_rule_report(&report),
                    OutputMode::Json => print_json(&json!({
                        "active": report.active,
                        "invalid": report.invalid,
                        "unknown_tags": report
                            .unknown_tags
                            .iter()
                            .map(|tag_id| tag_id.to_string())
                            .collect::<Vec<_>>(),
                    })),
                }
                return Ok(());
            }

            // Warnings go to stderr so they survive a `| jq` and do not
            // corrupt the JSON on stdout.
            for problem in &report.invalid {
                eprintln!("Warning: {problem}");
            }

            let summary = backend
                .retag(dry_run)
                .await
                .map_err(|error| error.to_string())?;

            let human = if summary.tags_applied == 0 {
                format!(
                    "Nothing to do: {} files scanned, all already carry the tags their rules \
                     assign",
                    summary.files_scanned
                )
            } else if dry_run {
                format!(
                    "Would apply {} tags across {} of {} files (dry run; nothing changed)",
                    summary.tags_applied, summary.files_changed, summary.files_scanned
                )
            } else {
                format!(
                    "Applied {} tags across {} of {} files",
                    summary.tags_applied, summary.files_changed, summary.files_scanned
                )
            };
            emit_scalar(
                output_mode,
                human,
                json!({
                    "dry_run": dry_run,
                    "files_scanned": summary.files_scanned,
                    "files_changed": summary.files_changed,
                    "tags_applied": summary.tags_applied,
                }),
            );
        }
    }

    Ok(())
}

/// `EXDEV` — "Invalid cross-device link" — is errno 18 on Linux. The one
/// `rename(2)` failure the download flow can recover from with a copy.
const EXDEV: i32 = 18;
