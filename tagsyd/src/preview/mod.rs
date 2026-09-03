//! Local preview generation.
//!
//! Turns a file's raw bytes into a small [`Preview`] — a low-resolution image,
//! a short text snippet, or [`Preview::None`] for anything we can't (or won't)
//! preview. This is the *producer* side of the preview feature; caching,
//! invalidation, and peer fetch live elsewhere (`store/previews.rs`,
//! `peer/relay/previews.rs`).
//!
//! This module owns the shared plumbing — the byte caps and the source reader —
//! and dispatches to one backend per kind based on the file's authoritative
//! [`FileKind`] (decided upstream from its extension by the shared
//! [`tagsy_core::classify_extension`], the same classifier every client
//! consults — there is no byte sniffing here). Each backend lives in its own
//! submodule with no shared state:
//!
//! - [`image`] — raster images, via the `image` crate.
//! - [`svg`] — vector images, rasterized via resvg then re-encoded as a raster
//!   preview through the [`image`] path.
//! - [`pdf`] — first-page render, via pdfium.
//! - [`video`] — one frame, via a pinned ffmpeg/ffprobe.
//! - [`text`] — a sanitized UTF-8 snippet.
//!
//! ## Determinism
//!
//! Previews are keyed by `(file_id, content_hash)` in the cache and on the
//! wire, and every peer runs this same generator. They are deterministic *in
//! kind* (the same bytes always yield an image, or always text, or always
//! none), but the exact encoded bytes of an image preview are **not** required
//! to be identical across peers — image-codec output can differ by library
//! version. Any valid preview of the requested content is acceptable, which is
//! why the peer-fetch layer takes the first responder and drops the rest.
//!
//! ## Blocking
//!
//! Decoding, resizing, and re-encoding an image is CPU-bound and must never run
//! on a Tokio worker thread. Call [`generate`] from inside
//! `tokio::task::spawn_blocking`; it is a plain synchronous function for
//! exactly that reason. Because it already runs on the blocking pool, it reads
//! its source ([`FileBytes`]) with synchronous `std::fs`, avoiding a second
//! async hop — and, crucially, letting the file-backed video path hand ffmpeg
//! the source's *own* on-disk path instead of copying it through memory into a
//! throwaway temp file.

mod image;
mod pdf;
mod svg;
mod text;
mod video;

use image::generate_image;
use pdf::generate_pdf;
pub use pdf::render_pdf_to_png;
use svg::generate_svg;
use tagsy_core::{FileKind, Preview};
use text::generate_text;
use video::generate_video;

use crate::file_bytes::FileBytes;

/// Longest edge, in pixels, of a generated image preview. Small on purpose: a
/// preview is a thumbnail hint, not a viewable image.
pub(super) const MAX_IMAGE_EDGE: u32 = 96;

/// Encoded-image preview size is bounded implicitly by [`MAX_IMAGE_EDGE`]; this
/// caps how many *source* bytes we are willing to decode so a hostile or
/// enormous image can't exhaust memory in the blocking task. Images larger than
/// this get no preview rather than risking an OOM.
pub(super) const MAX_IMAGE_SOURCE_BYTES: usize = 32 * 1024 * 1024;

/// Maximum length, in bytes, of a text preview snippet. Truncated on a UTF-8
/// character boundary, so the emitted `String` may be slightly shorter.
pub(super) const MAX_TEXT_BYTES: usize = 2048;

/// Generate a preview from a file's content, described by `source`.
///
/// `source` is a [`FileBytes`]: either bytes already in memory or a path to the
/// content on disk. Classification and the image/PDF/text generators read the
/// content into a bounded in-memory buffer (they must decode/snippet it), but
/// the *video* generator uses the source's on-disk path directly when it has
/// one — ffmpeg needs a seekable file, and a `FileToCopy`/`FileToMove` already
/// is one, so we avoid copying the (potentially large) video through memory
/// into a temp file. Only an in-memory video source is spilled to a temp file.
///
/// `extension` is the file's lowercase extension (no dot, e.g. `"jpg"`) taken
/// from its *logical* name, when known. It is used as a fallback (and a
/// tie-breaker) for type detection: the bytes on disk are content-addressed and
/// stored without an extension, and magic-byte sniffing occasionally misses
/// (unusual leading bytes, truncated reads), so the extension is a valuable
/// second signal.
///
/// Returns:
/// - `Some(preview)` — an authoritative result. `Some(Preview::None)` is the
///   cacheable "this content has no preview" (un-previewable type, or a decode
///   the generators declined); it is a *result*, not a failure.
/// - `None` — the source could not be read at all (e.g. the file vanished
///   between an earlier presence check and this call). The caller should treat
///   this as "no preview *this time*" and fall back to asking peers, rather
///   than caching a spurious negative.
///
/// The distinction matters: caching a read-race as `Preview::None` would mask a
/// preview a peer (or a later retry) could still produce.
///
/// Synchronous and CPU-bound — invoke via `spawn_blocking`.
///
/// `kind` is the file's authoritative [`FileKind`], decided upstream from its
/// logical name's extension by the shared [`classify_extension`] — the same
/// classifier every client consults. There is no byte sniffing here: the daemon
/// generates exactly the previews a client would ask about, and both agree from
/// the extension alone.
pub fn generate(source: &FileBytes, kind: FileKind) -> Option<Preview> {
    let preview = match kind {
        // Rasterizable content needs the whole (bounded) buffer in memory to
        // decode. The `kind` already disambiguates image vs. SVG vs. PDF, so
        // there is no re-classification pass.
        FileKind::Image | FileKind::Svg | FileKind::Pdf => {
            let bytes = match read_source_bounded(source, MAX_IMAGE_SOURCE_BYTES) {
                Ok(bytes) => bytes,
                Err(error) => {
                    log::debug!("preview: could not read source content: {error}");
                    return None;
                }
            };

            match kind {
                FileKind::Pdf => generate_pdf(&bytes).unwrap_or(Preview::None),
                FileKind::Svg => generate_svg(&bytes).unwrap_or(Preview::None),
                _ => generate_image(&bytes).unwrap_or(Preview::None),
            }
        }
        // Video prefers the source's own on-disk path (no copy); only an
        // in-memory source is spilled to a temp file inside `generate_video`.
        FileKind::Video => generate_video(source).unwrap_or(Preview::None),
        // Text (and markdown, which the daemon previews as plain text) needs
        // only a bounded leading window for the snippet.
        FileKind::Text | FileKind::Markdown => {
            let header = match read_source_bounded(source, MAX_TEXT_BYTES) {
                Ok(header) => header,
                Err(error) => {
                    log::debug!("preview: could not read source header: {error}");
                    return None;
                }
            };
            generate_text(&header)
        }
        FileKind::Other => Preview::None,
    };

    Some(preview)
}

/// Read up to `max_len` leading bytes of `source` into memory, synchronously.
///
/// In-memory sources are sliced; file-backed sources are read with `std::fs`
/// (this runs on the blocking pool, so blocking I/O is intended). Bounded so a
/// hostile or enormous file cannot exhaust memory.
fn read_source_bounded(source: &FileBytes, max_len: usize) -> std::io::Result<Vec<u8>> {
    use std::io::Read;

    match source.path() {
        // File-backed: read at most `max_len` bytes from the front.
        Some(path) => {
            let file = std::fs::File::open(path)?;
            let mut buffer = Vec::new();
            file.take(max_len as u64).read_to_end(&mut buffer)?;
            Ok(buffer)
        }
        // In-memory: slice the buffer we already hold.
        None => match source {
            FileBytes::InMemory(bytes) => Ok(bytes[..bytes.len().min(max_len)].to_vec()),
            // `path()` is `None` only for `InMemory`.
            _ => unreachable!("FileBytes::path() is None only for InMemory"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wrap raw bytes as an in-memory [`FileBytes`] source for the tests, which
    /// exercise the byte-decoding paths (image/PDF/text). The file-backed path
    /// differs only in *where* the bytes come from, which is covered by the
    /// read helper. Shared by the backend submodules' tests.
    pub(super) fn from_bytes(bytes: &[u8]) -> FileBytes {
        FileBytes::InMemory(bytes.to_vec())
    }
}
