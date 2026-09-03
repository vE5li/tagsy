//! Vector-image (SVG) preview generation.
//!
//! SVG is resolution-independent, so there is no "source resolution" to
//! downscale from the way there is for a raster photo. We instead rasterize the
//! document *directly* at the preview's target scale: parse with `usvg`, pick a
//! scale that fits the document's intrinsic size into the [`MAX_IMAGE_EDGE`]
//! box, and render onto a `tiny-skia` pixmap of exactly that size. The pixmap's
//! RGBA pixels are then handed to the shared raster path (`generate_image`) —
//! not to re-scale (it is already at target size) but to reuse one PNG encoder
//! and emit the identical [`Preview::Image`] shape every other backend does, so
//! nothing downstream needs to know SVG is special.
//!
//! Runs fully in-process: `resvg` is memory-safe pure Rust, so — unlike the
//! pdfium PDF path — it needs no crash-isolation subprocess.

use resvg::{tiny_skia, usvg};
use tagsy_core::Preview;

use super::{MAX_IMAGE_EDGE, MAX_IMAGE_SOURCE_BYTES, generate_image};

/// Parse and rasterize an SVG into a small PNG preview.
///
/// Returns `None` (→ [`Preview::None`]) if the source is too large to parse
/// safely, if it is not valid SVG, or if any parse/render/encode step fails.
pub(super) fn generate_svg(bytes: &[u8]) -> Option<Preview> {
    if bytes.len() > MAX_IMAGE_SOURCE_BYTES {
        log::debug!(
            "preview: svg source {} bytes exceeds cap {MAX_IMAGE_SOURCE_BYTES}; no preview",
            bytes.len()
        );
        return None;
    }

    let parse_start = std::time::Instant::now();
    // Default options: 96 DPI, empty resource dir (external `href`s to local
    // files are not resolved — a preview must not read arbitrary paths off
    // disk), and the default font database. `<text>` renders with system fonts
    // when present; a document without embedded/available fonts simply omits
    // text, which is acceptable for a thumbnail.
    let tree = usvg::Tree::from_data(bytes, &usvg::Options::default()).ok()?;
    let parse_elapsed = parse_start.elapsed();

    // Intrinsic document size (from `width`/`height` or the `viewBox`), in
    // usvg's f32 user units. A zero/degenerate size cannot be rasterized.
    let size = tree.size();
    let (source_width, source_height) = (size.width(), size.height());
    if !(source_width > 0.0 && source_height > 0.0) {
        log::debug!(
            "preview: svg has non-positive size {source_width}x{source_height}; no preview"
        );
        return None;
    }

    // Scale to fit the longest edge into the box, never upscaling past the
    // intrinsic size (matches the raster `thumbnail` contract, which also never
    // upscales — a 10x10 icon stays 10x10 rather than being blown up and blurry).
    let longest_edge = source_width.max(source_height);
    let scale = (MAX_IMAGE_EDGE as f32 / longest_edge).min(1.0);

    // Round to whole pixels, clamped to at least 1 so a very thin document
    // (e.g. 500x1) still yields a valid, non-empty pixmap.
    let target_width = ((source_width * scale).round() as u32).max(1);
    let target_height = ((source_height * scale).round() as u32).max(1);

    let render_start = std::time::Instant::now();
    let mut pixmap = tiny_skia::Pixmap::new(target_width, target_height)?;
    resvg::render(
        &tree,
        tiny_skia::Transform::from_scale(scale, scale),
        &mut pixmap.as_mut(),
    );
    let render_elapsed = render_start.elapsed();

    // Wrap the rasterized RGBA8 pixels as an `image` buffer and hand them to the
    // shared raster path. It re-clamps to the box (a no-op here, already within
    // it) and runs the one PNG encoder, so an SVG preview is byte-shaped
    // identically to a PNG/JPEG one.
    let buffer = ::image::RgbaImage::from_raw(target_width, target_height, pixmap.take())?;
    let mut encoded = Vec::new();
    ::image::DynamicImage::ImageRgba8(buffer)
        .write_to(
            &mut std::io::Cursor::new(&mut encoded),
            ::image::ImageFormat::Png,
        )
        .ok()?;

    log::debug!(
        "preview: svg {} src bytes → {target_width}x{target_height} thumbnail: parse={:?} \
         render={:?}",
        bytes.len(),
        parse_elapsed,
        render_elapsed
    );

    // Re-encoded PNG bytes round-trip through the raster generator so the output
    // (downscale-if-needed + PNG) is produced by exactly one code path.
    generate_image(&encoded)
}

#[cfg(test)]
mod tests {
    use tagsy_core::{FileKind, Preview};

    use super::super::tests::from_bytes;
    use super::super::{MAX_IMAGE_EDGE, generate};

    /// A minimal, valid SVG: a wide (200x100) solid rectangle. Wider than tall,
    /// so the resulting thumbnail must preserve that landscape aspect ratio.
    const WIDE_SVG: &[u8] = br#"<svg xmlns="http://www.w3.org/2000/svg" width="200" height="100">
        <rect width="200" height="100" fill="rgb(10,20,30)"/>
    </svg>"#;

    #[test]
    fn svg_kind_becomes_image_preview() {
        match generate(&from_bytes(WIDE_SVG), FileKind::Svg) {
            Some(Preview::Image {
                bytes,
                width,
                height,
            }) => {
                assert!(!bytes.is_empty());
                assert!(width <= MAX_IMAGE_EDGE && height <= MAX_IMAGE_EDGE);
                // Aspect ratio preserved: the 200x100 document is landscape.
                assert!(width >= height, "expected landscape, got {width}x{height}");
            }
            other => panic!("expected image preview, got {other:?}"),
        }
    }

    /// An SVG preceded by an XML declaration (the common real-world shape) must
    /// still render, routed by its `.svg` extension.
    #[test]
    fn svg_with_xml_declaration_renders() {
        let with_decl = br#"<?xml version="1.0" encoding="UTF-8"?>
            <svg xmlns="http://www.w3.org/2000/svg" width="60" height="60">
                <circle cx="30" cy="30" r="30" fill="red"/>
            </svg>"#;
        assert!(matches!(
            generate(&from_bytes(with_decl), FileKind::Svg),
            Some(Preview::Image { .. })
        ));
    }

    /// Malformed SVG-ish bytes that fail to parse must yield an authoritative
    /// "no preview", not a panic or a spurious image.
    #[test]
    fn broken_svg_yields_no_preview() {
        let broken = b"<svg this is not valid xml at all <<< >>>";
        assert!(matches!(
            generate(&from_bytes(broken), FileKind::Svg),
            Some(Preview::None)
        ));
    }
}
