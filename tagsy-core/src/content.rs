//! Content primitives shared across the crate graph: the streaming file hash
//! and the chunk size the transfer and IPC layers agree on.
//!
//! These are pure content handling with no protocol content, so they live in
//! `tagsy-core` where both the daemon (`tagsyd`) and the IPC client
//! (`tagsy-ipc`) can reach them without depending on each other.

use std::path::{Path, PathBuf};

use tokio::io::AsyncReadExt;

/// The unit of transfer, in bytes: the size of one content chunk moved between
/// peers and read by the IPC provider protocol. Both the peer transfer
/// subsystem and the control-socket client agree on this number.
pub const CHUNK_SIZE: usize = 64 * 1024;

/// Buffer size for streaming a file while hashing. Independent of
/// [`CHUNK_SIZE`] (which is a wire concern); kept equal only by coincidence of
/// both being a reasonable 64 KiB.
const STREAM_CHUNK: usize = 64 * 1024;

/// An error while hashing a file's content by streaming it.
///
/// Deliberately narrow — the only failure is I/O against the source path.
#[derive(Debug, thiserror::Error)]
pub enum FileBytesError {
    /// An I/O error occurred against `path` (or an in-memory buffer when
    /// `path` is `None`).
    #[error("I/O error{}: {source}", match path {
        Some(p) => format!(" for {}", p.display()),
        None => String::new(),
    })]
    Io {
        path: Option<PathBuf>,
        #[source]
        source: std::io::Error,
    },
}

impl FileBytesError {
    /// Build an [`Io`](FileBytesError::Io) error against a known `path`.
    ///
    /// The overwhelmingly common shape — every file-backed read/write records
    /// which path failed. Collapses the repeated
    /// `FileBytesError::Io { path: Some(path.clone()), source }` literal to one
    /// call. Takes anything `AsRef<Path>` so a `&Path`, `&PathBuf` or `PathBuf`
    /// all work without the caller spelling the clone.
    pub fn io(path: impl AsRef<Path>, source: std::io::Error) -> Self {
        FileBytesError::Io {
            path: Some(path.as_ref().to_path_buf()),
            source,
        }
    }
}

/// Stream `path` once, returning both its BLAKE3 content hash (hex) and its
/// byte length.
///
/// The two values come from the same pass, so they always describe the same
/// bytes — unlike hashing and then stat-ing, which can disagree if the file is
/// rewritten in between. Callers that publish a `(content_hash, size)` pair to
/// peers depend on that: the size is what the receiver uses to know when a
/// transfer is complete, and the hash is what it verifies against.
pub async fn hash_and_len(path: &Path) -> Result<(String, u64), FileBytesError> {
    let io_error = |source| FileBytesError::io(path, source);

    let mut file = tokio::fs::File::open(path).await.map_err(io_error)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; STREAM_CHUNK];
    let mut size: u64 = 0;

    loop {
        let read = file.read(&mut buffer).await.map_err(io_error)?;
        if read == 0 {
            break;
        }
        size += read as u64;
        hasher.update(&buffer[..read]);
    }

    Ok((hasher.finalize().to_hex().to_string(), size))
}
