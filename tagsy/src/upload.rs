//! Path expansion for the `upload` command: turning the raw file/directory
//! arguments into the concrete list of files to hand to the backend, one at a
//! time. Pure filesystem logic — no backend calls — so it lives apart from the
//! dispatch in [`crate::run`] and the backend helpers in [`crate::common`].

use std::path::{Path, PathBuf};

/// A single file resolved for upload: where its bytes live on disk, paired with
/// the logical name it should carry in the catalog. For a file given directly
/// the name is its final component; for a file found by walking a directory
/// argument the name keeps the directory portion the user typed (e.g. an
/// argument `bar/` yields names like `bar/sub/baz.txt`), so the location
/// information is not lost.
pub struct PlannedUpload {
    pub disk_path: PathBuf,
    pub path_name: String,
}

/// Refuse batches larger than this unless the caller opts in with `--many`.
/// The guard is a courtesy against accidental interactive uploads (`tagsy u
/// ~/`); scripts pass `--many` and are never blocked.
pub const MANY_THRESHOLD: usize = 100;

/// Expand the raw `paths` arguments into the flat list of files to upload.
///
/// Each argument may be a file or a directory:
/// * a file is included directly (even if hidden — passing it is explicit
///   intent), named by its final component;
/// * a directory is walked recursively, collecting every regular file, named by
///   its path relative to the directory *with the argument's own final
///   component prepended*.
///
/// Symlinks are skipped entirely (neither followed nor uploaded). Hidden
/// entries — any component beginning with `.` — are skipped when walking, with
/// hidden directories pruned wholesale (so `.git/` is never descended), unless
/// `include_hidden` is set. A missing path, or one that is neither a regular
/// file nor a directory, is an error.
pub fn expand_paths(paths: &[PathBuf], include_hidden: bool) -> Result<Vec<PlannedUpload>, String> {
    let mut planned = Vec::new();

    for path in paths {
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|error| format!("cannot access {}: {error}", path.display()))?;

        let file_type = metadata.file_type();

        if file_type.is_symlink() {
            return Err(format!(
                "{} is a symlink; refusing to upload",
                path.display()
            ));
        }

        if file_type.is_file() {
            let name = path
                .file_name()
                .ok_or_else(|| format!("{} has no file name", path.display()))?
                .to_string_lossy()
                .to_string();
            planned.push(PlannedUpload {
                disk_path: path.clone(),
                path_name: name,
            });
        } else if file_type.is_dir() {
            // Names carry the directory argument's final component as a prefix
            // so the walked structure is preserved. A bare `.` or `/` has no
            // final component; fall back to an empty prefix in that case.
            let prefix = path.file_name().map(PathBuf::from).unwrap_or_default();
            walk_directory(path, &prefix, include_hidden, &mut planned)?;
        } else {
            return Err(format!(
                "{} is neither a regular file nor a directory",
                path.display()
            ));
        }
    }

    Ok(planned)
}

/// Recursively collect regular files under `dir`, assigning each a logical name
/// of `name_prefix` joined with its path relative to `dir`. `name_prefix` is
/// the running catalog-name prefix (the original directory argument's final
/// component, extended by each subdirectory as we descend).
fn walk_directory(
    dir: &Path,
    name_prefix: &Path,
    include_hidden: bool,
    planned: &mut Vec<PlannedUpload>,
) -> Result<(), String> {
    let entries = std::fs::read_dir(dir)
        .map_err(|error| format!("cannot read directory {}: {error}", dir.display()))?;

    for entry in entries {
        let entry =
            entry.map_err(|error| format!("cannot read entry in {}: {error}", dir.display()))?;
        let entry_path = entry.path();

        let name = entry.file_name();
        if !include_hidden && is_hidden(&name) {
            continue;
        }

        let file_type = entry
            .file_type()
            .map_err(|error| format!("cannot stat {}: {error}", entry_path.display()))?;

        // Skip symlinks entirely — neither followed nor uploaded — to avoid
        // loops and out-of-tree escapes.
        if file_type.is_symlink() {
            continue;
        }

        let child_name = name_prefix.join(&name);

        if file_type.is_dir() {
            walk_directory(&entry_path, &child_name, include_hidden, planned)?;
        } else if file_type.is_file() {
            planned.push(PlannedUpload {
                disk_path: entry_path,
                path_name: child_name.to_string_lossy().to_string(),
            });
        }
        // Anything else (sockets, fifos, ...) is silently skipped.
    }

    Ok(())
}

/// A directory entry is hidden when its name begins with a dot.
fn is_hidden(name: &std::ffi::OsStr) -> bool {
    name.to_string_lossy().starts_with('.')
}
