//! Command dispatch: one arm per [`Commands`] variant, driving the daemon
//! [`Backend`] and rendering results through [`crate::output`].

use std::collections::HashMap;

use serde_json::json;
use tagsy_api::{Backend, DeletedRule, SubtagRule, Tag};
use tagsy_core::FileInfo;
use tagsy_ipc::IpcBackend;

use crate::commands::Commands;
use crate::common;
use crate::output::{
    OutputMode, emit_files, emit_operations, emit_scalar, emit_tags, emit_tags_and_files,
    print_json, print_tag_rule_report,
};

pub async fn run(
    backend: &IpcBackend,
    command: Commands,
    output_mode: OutputMode,
) -> Result<(), String> {
    match command {
        Commands::Upload { path, tags, keep } => {
            let path_name = path
                .file_name()
                .ok_or_else(|| format!("{} has no file name", path.display()))?
                .to_string_lossy()
                .to_string();

            // Resolve each `--tag` argument (full id or short prefix) via the
            // daemon, so tagging on upload accepts short ids like every other
            // tag-id command.
            let mut resolved_tags = Vec::with_capacity(tags.len());
            for tag in &tags {
                resolved_tags.push(common::resolve_tag_id(backend, tag).await?);
            }

            // Serve the file to the daemon as a temporary chunk provider: no
            // bytes are read into memory here. This call blocks until the daemon
            // has handed the content off to the storing peer(s).
            let file_id = backend
                .upload_file(path.clone(), path_name.clone(), resolved_tags.clone())
                .await
                .map_err(|error| error.to_string())?;

            if !keep {
                std::fs::remove_file(&path).map_err(|error| {
                    format!(
                        "uploaded as file {}, but failed to delete {}: {error}",
                        file_id.to_string(),
                        path.display()
                    )
                })?;
            }

            // Render the full entry from locally-known data rather than fetching
            // it back (the metadata write is enqueued asynchronously and would
            // race). We know the id, logical path, applied tags, and that this is
            // the first version. The content hash is computed daemon-side and is
            // not known here, so it renders empty in JSON output.
            let file = FileInfo {
                file_id,
                logical_path: tagsy_core::LogicalPath::new(path_name),
                content_hash: String::new(),
                version_number: 1,
                // The size is computed daemon-side and is not known here.
                size: 0,
                // Only one id is known locally; highlight the whole id.
                short_id_length: file_id.to_string().len(),
                // A freshly-added file is live by construction.
                deleted: false,
                // Freshly added: its only version was recorded just now. The
                // authoritative timestamps are stamped daemon-side; approximate
                // with now for this optimistic local render.
                first_recorded_at: tagsy_core::clock::now_millis(),
                latest_change_at: tagsy_core::clock::now_millis(),
            };

            let mut name_cache = common::NameCache::new();
            let mut file_tags = HashMap::new();

            let tag_names =
                common::resolve_tag_names(backend, &mut name_cache, &resolved_tags).await?;
            file_tags.insert(file_id, tag_names);

            emit_files(output_mode, std::slice::from_ref(&file), &file_tags);
        }
        Commands::CreateTag { name, color } => {
            let tag_id = backend
                .create_tag(name.clone(), color.clone())
                .await
                .map_err(|error| error.to_string())?;

            // Persistence is async (the write is enqueued), so we can't fetch the
            // row back yet without racing the pipeline. Render the full entry from
            // what we just sent instead — the id is authoritative and the
            // name/color are exactly what the daemon will persist (the CLI's
            // default color matches the daemon's empty-color default). A fresh tag
            // has no applied tags, so that column is empty.
            let tag = Tag {
                id: tag_id,
                name,
                color,
                metadata: None,
                // A freshly-created tag is live by construction.
                deleted: false,
            };
            emit_tags(output_mode, std::slice::from_ref(&tag), &HashMap::new());
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
            let file_id = common::resolve_file_id(backend, &id).await?;

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

            match (output_mode, outcome.changed) {
                (OutputMode::Human, true) => println!("Edited file {}", file_id.to_string()),
                (OutputMode::Human, false) => println!("No changes"),
                (OutputMode::Json, changed) => {
                    print_json(&json!({ "id": file_id, "edited": changed }))
                }
            }
        }

        // Shares its start with the edit flow: locate the file's bytes — reading
        // the real file if it lives in a local sync directory, otherwise fetching
        // them from a peer — then, instead of editing, copy them into the current
        // directory.
        Commands::Download { id } => {
            let file_id = common::resolve_file_id(backend, &id).await?;

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
                    .fetch_file(file_id, file.content_hash)
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

            emit_scalar(
                output_mode,
                format!("Downloaded to {file_name}"),
                json!({ "id": file_id, "path": file_name }),
            );
        }
        Commands::DeleteFile { id } => {
            let file_id = common::resolve_file_id(backend, &id).await?;

            backend
                .delete_file(file_id)
                .await
                .map_err(|error| error.to_string())?;

            emit_scalar(
                output_mode,
                format!("Deleted file {}", file_id.to_string()),
                json!({ "deleted": file_id }),
            );
        }
        Commands::RestoreFile { id } => {
            let file_id = common::resolve_file_id(backend, &id).await?;

            backend
                .restore_file(file_id)
                .await
                .map_err(|error| error.to_string())?;

            emit_scalar(
                output_mode,
                format!("Restored file {}", file_id.to_string()),
                json!({ "restored": file_id }),
            );
        }
        Commands::DeleteTag { tag_id } => {
            let tag_id = common::resolve_tag_id(backend, &tag_id).await?;

            backend
                .delete_tag(tag_id)
                .await
                .map_err(|error| error.to_string())?;

            emit_scalar(
                output_mode,
                format!("Deleted tag {}", tag_id.to_string()),
                json!({ "deleted": tag_id }),
            );
        }
        Commands::RestoreTag { tag_id } => {
            let tag_id = common::resolve_tag_id(backend, &tag_id).await?;

            backend
                .restore_tag(tag_id)
                .await
                .map_err(|error| error.to_string())?;

            emit_scalar(
                output_mode,
                format!("Restored tag {}", tag_id.to_string()),
                json!({ "restored": tag_id }),
            );
        }
        Commands::Tag { id, tag_ids } => {
            let file_id = common::resolve_file_id(backend, &id).await?;

            let mut applied = Vec::new();
            for tag in &tag_ids {
                let tag_id = common::resolve_tag_id(backend, tag).await?;

                backend
                    .tag_file(tag_id, file_id)
                    .await
                    .map_err(|error| error.to_string())?;

                if output_mode == OutputMode::Human {
                    println!(
                        "Tagged file {} with tag {}",
                        file_id.to_string(),
                        tag_id.to_string()
                    );
                }

                applied.push(tag_id);
            }

            if output_mode == OutputMode::Json {
                print_json(&json!({ "file": file_id, "tagged": applied }));
            }
        }
        Commands::Untag { id, tag_ids } => {
            let file_id = common::resolve_file_id(backend, &id).await?;

            let mut removed = Vec::new();
            for tag in &tag_ids {
                let tag_id = common::resolve_tag_id(backend, tag).await?;

                backend
                    .untag_file(tag_id, file_id)
                    .await
                    .map_err(|error| error.to_string())?;

                if output_mode == OutputMode::Human {
                    println!(
                        "Removed tag {} from file {}",
                        tag_id.to_string(),
                        file_id.to_string()
                    );
                }

                removed.push(tag_id);
            }

            if output_mode == OutputMode::Json {
                print_json(&json!({ "file": file_id, "untagged": removed }));
            }
        }
        Commands::TagsForFile {
            id,
            include_subtags,
        } => {
            let file_id = common::resolve_file_id(backend, &id).await?;
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
            let tag_id = common::resolve_tag_id(backend, &tag_id).await?;

            backend
                .rename_tag(tag_id, name.clone())
                .await
                .map_err(|error| error.to_string())?;

            emit_scalar(
                output_mode,
                format!("Renamed tag {}", tag_id.to_string()),
                json!({ "id": tag_id, "name": name }),
            );
        }
        Commands::SetTagColor { tag_id, color } => {
            let tag_id = common::resolve_tag_id(backend, &tag_id).await?;

            backend
                .set_tag_color(tag_id, color.clone())
                .await
                .map_err(|error| error.to_string())?;

            emit_scalar(
                output_mode,
                format!("Recolored tag {}", tag_id.to_string()),
                json!({ "id": tag_id, "color": color }),
            );
        }
        Commands::Move { id, path } => {
            let file_id = common::resolve_file_id(backend, &id).await?;

            backend
                .move_file(file_id, path.clone())
                .await
                .map_err(|error| error.to_string())?;

            emit_scalar(
                output_mode,
                format!("Moved file {}", file_id.to_string()),
                json!({ "id": file_id, "path": path }),
            );
        }
        Commands::TagTag { child, parents } => {
            let child_id = common::resolve_tag_id(backend, &child).await?;

            let mut applied = Vec::new();
            for parent in &parents {
                let parent_id = common::resolve_tag_id(backend, parent).await?;

                backend
                    .tag_tag(parent_id, child_id)
                    .await
                    .map_err(|error| error.to_string())?;

                if output_mode == OutputMode::Human {
                    println!(
                        "Tagged tag {} with {}",
                        child_id.to_string(),
                        parent_id.to_string()
                    );
                }

                applied.push(parent_id);
            }

            if output_mode == OutputMode::Json {
                print_json(&json!({ "tag": child_id, "tagged": applied }));
            }
        }
        Commands::UntagTag { child, parents } => {
            let child_id = common::resolve_tag_id(backend, &child).await?;

            let mut removed = Vec::new();
            for parent in &parents {
                let parent_id = common::resolve_tag_id(backend, parent).await?;

                backend
                    .untag_tag(parent_id, child_id)
                    .await
                    .map_err(|error| error.to_string())?;

                if output_mode == OutputMode::Human {
                    println!(
                        "Removed tag {} from {}",
                        parent_id.to_string(),
                        child_id.to_string(),
                    );
                }

                removed.push(parent_id);
            }

            if output_mode == OutputMode::Json {
                print_json(&json!({ "tag": child_id, "untagged": removed }));
            }
        }
        Commands::Subtags { tag_id, recursive } => {
            let tag_id = common::resolve_tag_id(backend, &tag_id).await?;
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
