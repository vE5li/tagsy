//! PDF first-page preview generation, via pdfium.
//!
//! ## Why a subprocess
//!
//! pdfium is a large C++ library bound into our address space. On a
//! malformed-but-parseable PDF it can hit an unhandled internal error (a
//! `std::bad_variant_access`, a bad scanline read) and call `abort()` — or
//! segfault outright — from deep inside its render path. Neither is a Rust
//! panic, so neither `Result`, `?`, nor `catch_unwind` can contain it: it takes
//! the **whole daemon** down. With `Restart=on-failure` and a cold preview
//! cache (a purge, a restore, a first sync), the daemon re-renders every PDF on
//! startup, so a single poison PDF becomes a permanent crash loop.
//!
//! So, exactly like the video path shells out to ffmpeg, PDF rendering runs in
//! a **short-lived child process** (`tagsyd render-pdf-preview`, reading the
//! PDF on stdin and writing a PNG on stdout). A pdfium crash then kills only
//! that child; the parent observes a failed subprocess and degrades to
//! [`Preview::None`], the same graceful outcome the load/render error arms
//! already produce. The in-process pdfium call survives only as
//! [`render_pdf_to_png`], reached **only** from inside the child.

use std::io::Cursor;

use tagsy_core::Preview;

use super::MAX_IMAGE_SOURCE_BYTES;

/// Render the first page of a PDF to a small PNG preview, isolating the
/// crash-prone pdfium render in a child process.
///
/// Spawns `tagsyd render-pdf-preview` (this same binary), pipes the PDF bytes
/// to its stdin, and reads the PNG back from its stdout. Returns `None`
/// (→ [`Preview::None`]) if the source is too large, the child cannot be
/// spawned, the child exits non-zero **or is killed by a signal** (a pdfium
/// `abort`/segv), or the emitted PNG cannot be decoded. The parent process is
/// never affected by a pdfium crash.
///
/// Runs synchronously (already on the blocking pool).
pub(super) fn generate_pdf(bytes: &[u8]) -> Option<Preview> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    if bytes.len() > MAX_IMAGE_SOURCE_BYTES {
        log::debug!(
            "preview: PDF source {} bytes exceeds cap {MAX_IMAGE_SOURCE_BYTES}; no preview",
            bytes.len()
        );
        return None;
    }

    // Re-invoke *this* binary's hidden rendering subcommand. `current_exe`
    // resolves the real executable (the Nix wrapper's `.tagsyd-wrapped`), which
    // has already inherited `TAGSY_PDFIUM_LIB_PATH` from our own environment, so
    // the child binds the same pinned pdfium.
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(error) => {
            log::debug!(
                "preview: could not resolve current_exe for PDF render: {error}; no preview"
            );
            return None;
        }
    };

    let render_start = std::time::Instant::now();
    let mut child = match Command::new(&exe)
        .arg("render-pdf-preview")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            log::debug!("preview: could not spawn PDF render subprocess: {error}; no preview");
            return None;
        }
    };

    // Feed the PDF to the child's stdin, then drop it to signal EOF. Take the
    // handle so the borrow ends before `wait_with_output` below.
    if let Some(mut stdin) = child.stdin.take()
        && let Err(error) = stdin.write_all(bytes)
    {
        // A broken pipe here usually means the child already died (e.g. a
        // pdfium crash before it drained stdin). Fall through to `wait` to
        // reap it and report the failure uniformly.
        log::debug!("preview: writing PDF to render subprocess failed: {error}");
        // `stdin` (moved into this block) drops at the end, closing the pipe.
    }

    let output = match child.wait_with_output() {
        Ok(output) => output,
        Err(error) => {
            log::debug!("preview: PDF render subprocess wait failed: {error}; no preview");
            return None;
        }
    };

    // A non-zero exit *or a signal death* (pdfium `abort`/segv) lands here: the
    // child crashed instead of us. Degrade gracefully.
    if !output.status.success() {
        log::debug!(
            "preview: PDF render subprocess did not succeed (status {:?}); no preview (a pdfium \
             crash on this document is contained to the child)",
            output.status
        );
        return None;
    }

    // Empty stdout is the child's authoritative "no preview" (unrenderable PDF).
    if output.stdout.is_empty() {
        log::debug!("preview: PDF render subprocess produced no image; no preview");
        return None;
    }

    // The child emitted a PNG; decode it here just to read its dimensions (and
    // confirm it is valid). It is already thumbnail-sized, so this is cheap.
    let decoded = image::ImageReader::new(Cursor::new(&output.stdout))
        .with_guessed_format()
        .ok()?
        .decode()
        .ok()?;
    let width = decoded.width();
    let height = decoded.height();

    log::debug!(
        "preview: PDF {} src bytes → {width}x{height} page-1 thumbnail via subprocess in {:?}",
        bytes.len(),
        render_start.elapsed()
    );

    Some(Preview::Image {
        bytes: output.stdout,
        width,
        height,
    })
}

/// Render the first page of a PDF to PNG bytes, **in-process**, via pdfium.
///
/// This is the crash-prone half: it calls into the linked pdfium C++ library,
/// which may `abort`/segfault on a malformed document. It is therefore reached
/// **only** from the `render-pdf-preview` child process (see
/// [`crate::render_pdf_preview_child`]), never on a daemon thread, so such a
/// crash is contained to that child.
///
/// Returns `Some(png)` on success, or `None` if pdfium is unavailable or the
/// document fails to load / has no pages / fails to render (the *recoverable*
/// failures; an unrecoverable pdfium crash never returns at all — it takes the
/// child down, which the parent reads as a failed subprocess).
pub fn render_pdf_to_png(bytes: &[u8]) -> Option<Vec<u8>> {
    use std::sync::OnceLock;

    use pdfium_render::prelude::{PdfRenderConfig, Pdfium};

    use super::MAX_IMAGE_EDGE;

    /// Lazily bind to the pdfium shared library, once for the process.
    ///
    /// Resolution order:
    /// 1. `TAGSY_PDFIUM_LIB_PATH` — a directory containing `libpdfium.so`, set
    ///    by the packaging wrapper (see `flake.nix`) so the pinned nixpkgs
    ///    build is used deterministically.
    /// 2. the system library (`bind_to_system_library`) as a fallback for dev.
    ///
    /// Returns `None` (logged once) if neither can be bound.
    fn pdfium() -> Option<&'static Pdfium> {
        static PDFIUM: OnceLock<Option<Pdfium>> = OnceLock::new();

        PDFIUM
            .get_or_init(|| {
                let bindings = match std::env::var("TAGSY_PDFIUM_LIB_PATH") {
                    Ok(dir) => {
                        Pdfium::bind_to_library(Pdfium::pdfium_platform_library_name_at_path(&dir))
                            .or_else(|error| {
                                log::warn!(
                                    "preview: TAGSY_PDFIUM_LIB_PATH set but binding failed \
                                     ({error:?}); trying system library"
                                );
                                Pdfium::bind_to_system_library()
                            })
                    }
                    Err(_) => Pdfium::bind_to_system_library(),
                };

                match bindings {
                    Ok(bindings) => Some(Pdfium::new(bindings)),
                    Err(error) => {
                        log::warn!(
                            "preview: could not bind to a pdfium library ({error:?}); PDF \
                             previews are disabled on this device"
                        );
                        None
                    }
                }
            })
            .as_ref()
    }

    let pdfium = pdfium()?;

    let document = match pdfium.load_pdf_from_byte_slice(bytes, None) {
        Ok(document) => document,
        Err(error) => {
            log::debug!("preview: failed to load PDF: {error:?}; no preview");
            return None;
        }
    };

    let pages = document.pages();
    let first_page = match pages.first() {
        Ok(page) => page,
        Err(error) => {
            log::debug!("preview: PDF has no first page: {error:?}; no preview");
            return None;
        }
    };

    // Render the first page directly at the thumbnail box, preserving aspect
    // ratio (pdfium fits within the given width/height). Rendering straight to
    // the small size avoids rasterizing a full-resolution page bitmap.
    let config = PdfRenderConfig::new()
        .set_target_width(MAX_IMAGE_EDGE as i32)
        .set_maximum_height(MAX_IMAGE_EDGE as i32);

    let rendered = match first_page.render_with_config(&config) {
        Ok(bitmap) => bitmap,
        Err(error) => {
            log::debug!("preview: failed to render PDF page: {error:?}; no preview");
            return None;
        }
    };

    let dynamic = match rendered.as_image() {
        Ok(image) => image,
        Err(error) => {
            log::debug!("preview: failed to convert rendered PDF page to image: {error:?}");
            return None;
        }
    };

    let mut encoded = Vec::new();
    dynamic
        .write_to(&mut Cursor::new(&mut encoded), image::ImageFormat::Png)
        .ok()?;

    Some(encoded)
}

#[cfg(test)]
mod tests {
    use tagsy_core::{FileKind, Preview};

    use super::super::tests::from_bytes;
    use super::super::{MAX_IMAGE_EDGE, generate};

    #[test]
    fn pdf_preview_renders_or_degrades_gracefully() {
        // A tiny one-page PDF. Generation now shells out to the
        // `render-pdf-preview` subcommand of *this* test binary, which has no
        // such subcommand — so `generate_pdf` sees a failed child and degrades
        // to `Preview::None`. The contract under test is "never crashes and
        // never panics", which holds regardless of pdfium availability.
        const ONE_PAGE_PDF: &[u8] = b"%PDF-1.1\n\
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n\
2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n\
3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]>>endobj\n\
trailer<</Root 1 0 R>>\n%%EOF";

        match generate(&from_bytes(ONE_PAGE_PDF), FileKind::Pdf) {
            Some(Preview::Image {
                bytes,
                width,
                height,
            }) => {
                assert!(!bytes.is_empty());
                assert!(width <= MAX_IMAGE_EDGE && height <= MAX_IMAGE_EDGE);
            }
            // pdfium unavailable, the subprocess subcommand absent (as in the
            // test binary), or this minimal PDF rejected — all fine; we only
            // require no panic.
            Some(Preview::None) => {}
            other => panic!("unexpected preview kind for PDF: {other:?}"),
        }
    }
}
