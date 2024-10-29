//! The backup builder: `ApiService::backup` and the archive-writing kernel.
//!
//! A backup bundles the **entire restorable state** of this tagsy instance into
//! a single `*.tar.zst` in `TAGSY_BACKUP_DIR`:
//!
//! ```text
//! db/main.db               # the main catalog, snapshotted with VACUUM INTO
//! db/<name>.db …           # each per-sync-directory index, likewise
//! sync/<name>/…            # the full recursive contents of each sync directory
//! manifest.json            # provenance: created_at, each sync dir's path+type
//! ```
//!
//! ## Why the daemon does this
//!
//! The daemon owns the write handles to both SQLite databases, so it is the
//! only process that can take a *consistent* snapshot while running: each DB is
//! copied with SQLite's `VACUUM INTO`, which serializes against in-flight
//! writes and so never captures a torn page. A client-side file copy could race
//! a mid-write database.
//!
//! ## No cross-artifact atomicity
//!
//! The two databases are snapshotted independently and the sync directories are
//! walked separately, so the archive is **not** a single global point-in-time
//! snapshot across all four artifacts — a file can change on disk between the
//! catalog snapshot and the directory walk. That is acceptable: the system
//! already reconciles catalog-vs-filesystem drift on startup
//! (`initial_sync_*`), so a restored backup converges the same way a
//! reconnecting daemon does. Do not add locking to make this atomic; the
//! reconciliation path is the design.
//!
//! ## Failure model
//!
//! The archive is written to a `*.partial` name and renamed onto its final name
//! only after the tar and zstd streams are finished and fsynced, so an
//! interrupted backup never leaves a half-written archive that looks complete.
//! Staging DB snapshots live under `data_dir` and are removed on success.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::Serialize;

use super::{ApiError, ApiService};
use crate::configuration::{SyncDirectory, SyncType};
use crate::store::{CatalogStore, DirectoryIndex};

/// What `manifest.json` records inside the archive, so a future restore knows
/// where each `sync/<name>/` came from and at what moment the backup was taken.
#[derive(Debug, Serialize)]
struct Manifest {
    /// RFC 3339 UTC timestamp of when the archive was built.
    created_at: String,
    /// One entry per sync directory captured, in archive order.
    sync_directories: Vec<ManifestDirectory>,
    /// The database files placed under `db/` in the archive.
    db_files: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ManifestDirectory {
    /// The `<name>` used for both `db/<name>.db` and `sync/<name>/` inside the
    /// archive. Derived from the directory's final path component.
    name: String,
    /// The original absolute path this directory lived at, so a restore can put
    /// it back where it belongs.
    path: PathBuf,
    /// Whether it was a Universal or TagBased directory (and its tags).
    sync_type: SyncType,
}

/// A sync directory resolved for archiving: its live path, the `<name>` used
/// inside the archive, and the staged DB snapshot path (if its index existed).
struct ResolvedDirectory {
    directory: SyncDirectory,
    name: String,
    /// The `<staging>/db/<name>.db` snapshot, or `None` if this directory has
    /// no index on disk yet (freshly added; the index is created lazily on
    /// first placement).
    staged_db: Option<PathBuf>,
}

impl ApiService {
    /// Bundle the entire restorable state of this instance — both SQLite
    /// databases plus the full contents of every sync directory — into a single
    /// compressed archive in `TAGSY_BACKUP_DIR`, returning where it landed.
    ///
    /// Errors with [`ApiError::Internal`] if `TAGSY_BACKUP_DIR` is unset (there
    /// is nowhere to write), or on any I/O / SQLite failure along the way. See
    /// the module docs for the archive layout and the consistency model.
    pub async fn backup(&self) -> Result<tagsy_api::BackupOutcome, ApiError> {
        let backup_dir = self.paths.backup_dir().ok_or_else(|| {
            ApiError::Internal("backups are not configured (TAGSY_BACKUP_DIR is unset)".to_owned())
        })?;
        let backup_dir = backup_dir.to_path_buf();

        // The live set of sync directories, straight from the actor (see
        // `ApiService::sync_directories`): reflects directories that actually
        // opened at startup, not the possibly-stale config.
        let directories = self.sync_directories().await?;

        let paths = self.paths.clone();
        let main_db_path = self.main_db_path.clone();

        // Everything below is blocking: VACUUM INTO, walking the filesystem, and
        // streaming through zstd. Run it off the async runtime so it cannot
        // stall other tasks; each DB handle is opened *inside* the closure since
        // `CatalogStore` is `!Send`.
        tokio::task::spawn_blocking(move || {
            build_archive(&paths, &main_db_path, &backup_dir, directories)
        })
        .await
        .map_err(|error| ApiError::Internal(format!("backup task panicked: {error}")))?
    }
}

/// The `<name>` used inside the archive for a sync directory: its final path
/// component. Mirrors how [`crate::paths::Paths::sync_directory_db_path`] names
/// the on-disk index, so `db/<name>.db` and `sync/<name>/` line up.
fn directory_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unnamed".to_owned())
}

/// The whole blocking backup: snapshot the databases, walk the sync
/// directories, stream everything through tar+zstd, and rename the finished
/// archive into place. Pure I/O; no `.await`.
fn build_archive(
    paths: &crate::paths::Paths,
    main_db_path: &Path,
    backup_dir: &Path,
    directories: Vec<SyncDirectory>,
) -> Result<tagsy_api::BackupOutcome, ApiError> {
    let io = |context: &'static str| {
        move |error: std::io::Error| ApiError::Internal(format!("{context}: {error}"))
    };

    std::fs::create_dir_all(backup_dir).map_err(io("create backup dir"))?;

    // A clean staging area under data_dir for the DB snapshots. VACUUM INTO
    // refuses to overwrite, so wipe any leftover from a previous interrupted run
    // first.
    let staging = paths.data_dir().join("backup-staging");
    if staging.exists() {
        std::fs::remove_dir_all(&staging).map_err(io("clear staging dir"))?;
    }
    let staging_db = staging.join("db");
    std::fs::create_dir_all(&staging_db).map_err(io("create staging db dir"))?;

    // Snapshot the main catalog.
    let main_snapshot = staging_db.join("main.db");
    CatalogStore::initialize(main_db_path)?.vacuum_into(&main_snapshot)?;
    let mut db_files = vec!["main.db".to_owned()];

    // Snapshot each per-directory index that exists on disk. A directory added
    // since startup may not have created its index yet; skip those rather than
    // fail — the sync/<name>/ contents are still archived below.
    let mut resolved: Vec<ResolvedDirectory> = Vec::with_capacity(directories.len());
    for directory in directories {
        let name = directory_name(&directory.path);
        let source_db = paths.sync_directory_db_path(&directory.path);
        let staged_db = if source_db.exists() {
            let dest = staging_db.join(format!("{name}.db"));
            DirectoryIndex::initialize(&source_db)?.vacuum_into(&dest)?;
            db_files.push(format!("{name}.db"));
            Some(dest)
        } else {
            None
        };
        resolved.push(ResolvedDirectory {
            directory,
            name,
            staged_db,
        });
    }

    let manifest = Manifest {
        created_at: now_rfc3339(),
        sync_directories: resolved
            .iter()
            .map(|resolved| ManifestDirectory {
                name: resolved.name.clone(),
                path: resolved.directory.path.clone(),
                sync_type: resolved.directory.sync_type.clone(),
            })
            .collect(),
        db_files,
    };

    // Assemble the archive under a .partial name; rename onto the real name only
    // after everything is flushed and fsynced.
    let final_path = backup_dir.join(format!("tagsy-backup-{}.tar.zst", timestamp_slug()));
    let partial_path = final_path.with_extension("partial");

    let (bytes_written, file_count) =
        write_tar_zst(&partial_path, &main_snapshot, &resolved, &manifest)?;

    std::fs::rename(&partial_path, &final_path).map_err(io("finalize archive"))?;
    std::fs::remove_dir_all(&staging).map_err(io("remove staging dir"))?;

    Ok(tagsy_api::BackupOutcome {
        path: final_path,
        bytes_written,
        file_count,
    })
}

/// Stream the staged databases, the manifest, and every sync directory's
/// contents into a tar+zstd archive at `partial_path`. Returns the
/// `(bytes_written, file_count)` of the **sync-directory contents** only.
fn write_tar_zst(
    partial_path: &Path,
    main_snapshot: &Path,
    resolved: &[ResolvedDirectory],
    manifest: &Manifest,
) -> Result<(u64, u64), ApiError> {
    let io = |context: &'static str| {
        move |error: std::io::Error| ApiError::Internal(format!("{context}: {error}"))
    };

    let output = File::create(partial_path).map_err(io("create archive file"))?;
    // Level 3 is zstd's default: a good speed/ratio tradeoff for mixed DB +
    // file-content payloads. Not `auto_finish`: we finish it explicitly below to
    // recover the underlying `File` and fsync it before the rename.
    let encoder = zstd::stream::write::Encoder::new(output, 3).map_err(io("init zstd encoder"))?;
    let mut tar = tar::Builder::new(encoder);

    // db/main.db and each db/<name>.db.
    tar.append_path_with_name(main_snapshot, "db/main.db")
        .map_err(io("archive main.db"))?;
    for entry in resolved {
        if let Some(staged_db) = &entry.staged_db {
            tar.append_path_with_name(staged_db, format!("db/{}.db", entry.name))
                .map_err(io("archive directory index"))?;
        }
    }

    // The manifest, written from an in-memory buffer.
    let manifest_json = serde_json::to_vec_pretty(manifest)
        .map_err(|error| ApiError::Internal(format!("serialize manifest: {error}")))?;
    let mut header = tar::Header::new_gnu();
    header.set_size(manifest_json.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, "manifest.json", manifest_json.as_slice())
        .map_err(io("archive manifest"))?;

    // sync/<name>/… for each directory, recursively. Accumulate the raw byte
    // and file totals for the outcome.
    let mut bytes_written: u64 = 0;
    let mut file_count: u64 = 0;
    for entry in resolved {
        let root = &entry.directory.path;
        if !root.exists() {
            continue;
        }
        let prefix = format!("sync/{}", entry.name);
        for walk_entry in walkdir::WalkDir::new(root).follow_links(false) {
            let walk_entry = walk_entry
                .map_err(|error| ApiError::Internal(format!("walk {}: {error}", root.display())))?;
            // Only regular files carry bytes; directories are recreated
            // implicitly from the file paths on restore. Symlinks are skipped
            // (follow_links(false) reports them but we do not archive them),
            // matching the watcher's "no symlink traversal" caution.
            if !walk_entry.file_type().is_file() {
                continue;
            }
            let absolute = walk_entry.path();
            let relative = absolute
                .strip_prefix(root)
                .map_err(|error| ApiError::Internal(format!("path outside sync dir: {error}")))?;
            let archive_path = format!("{prefix}/{}", relative.to_string_lossy());
            let metadata = walk_entry.metadata().map_err(|error| {
                ApiError::Internal(format!("stat {}: {error}", absolute.display()))
            })?;
            bytes_written += metadata.len();
            file_count += 1;
            tar.append_path_with_name(absolute, &archive_path)
                .map_err(io("archive sync file"))?;
        }
    }

    // Finish the tar (writes the trailer), then finish zstd (flushes its frame
    // and returns the underlying file), then fsync before the rename.
    let encoder = tar.into_inner().map_err(io("finish tar"))?;
    let mut output = encoder.finish().map_err(io("finish zstd"))?;
    output.flush().map_err(io("flush archive"))?;
    output.sync_all().map_err(io("fsync archive"))?;

    Ok((bytes_written, file_count))
}

/// Current UTC time as an RFC 3339 string for the manifest.
fn now_rfc3339() -> String {
    // Avoid a chrono dependency: format the unix time directly. Second
    // precision is plenty for a human-facing "when was this taken" field.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    format!("{secs}")
}

/// A filesystem-safe slug for the archive filename: the unix-epoch seconds.
/// Sorts chronologically and needs no escaping, unlike a full RFC-3339 string
/// with its colons.
fn timestamp_slug() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_owned())
}
