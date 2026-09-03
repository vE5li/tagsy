//! Raster-image preview generation.

use std::io::Cursor;

use image::metadata::Orientation;
use image::{DynamicImage, ImageDecoder, ImageReader};
use tagsy_core::Preview;

use super::{MAX_IMAGE_EDGE, MAX_IMAGE_SOURCE_BYTES};

/// Decode, downscale, and re-encode an image into a tiny PNG preview.
///
/// Returns `None` (→ [`Preview::None`]) if the source is too large to decode
/// safely or if any decode/encode step fails.
pub(super) fn generate_image(bytes: &[u8]) -> Option<Preview> {
    if bytes.len() > MAX_IMAGE_SOURCE_BYTES {
        log::debug!(
            "preview: image source {} bytes exceeds cap {MAX_IMAGE_SOURCE_BYTES}; no preview",
            bytes.len()
        );
        return None;
    }

    let reader = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .ok()?;

    // Decoding the *full* source image to a bitmap is by far the most expensive
    // step for a large photo (a few-MB JPEG can expand to tens of MB of pixels),
    // so time decode, resize, and encode separately.
    //
    // Go through the lower-level `ImageDecoder` path (rather than
    // `reader.decode()`) so we can read the EXIF `Orientation` tag *before*
    // consuming the decoder to build the `DynamicImage`. The `image` crate does
    // not apply orientation on decode — the pixels come out as stored — so a
    // phone photo shot in portrait (which is typically encoded landscape + an
    // `Orientation=6` tag) would otherwise be thumbnailed sideways. Only JPEG,
    // TIFF, and WebP carry an orientation tag; the other enabled formats
    // return `NoTransforms` and `apply_orientation` is a no-op.
    let decode_start = std::time::Instant::now();
    let mut decoder = reader.into_decoder().ok()?;
    // A missing or malformed orientation tag is not a decode failure — fall
    // back to `NoTransforms` and continue rather than dropping the preview.
    let orientation = decoder.orientation().unwrap_or(Orientation::NoTransforms);
    let mut decoded = DynamicImage::from_decoder(decoder).ok()?;
    decoded.apply_orientation(orientation);
    let decode_elapsed = decode_start.elapsed();

    // Downscale preserving aspect ratio; `thumbnail` uses a fast filter and
    // never upscales past the requested box.
    let resize_start = std::time::Instant::now();
    let thumbnail = decoded.thumbnail(MAX_IMAGE_EDGE, MAX_IMAGE_EDGE);
    let width = thumbnail.width();
    let height = thumbnail.height();
    let resize_elapsed = resize_start.elapsed();

    let encode_start = std::time::Instant::now();
    let mut encoded = Vec::new();
    thumbnail
        .write_to(&mut Cursor::new(&mut encoded), image::ImageFormat::Png)
        .ok()?;
    let encode_elapsed = encode_start.elapsed();

    log::debug!(
        "preview: image {} src bytes → {width}x{height} thumbnail: decode={:?} resize={:?} \
         encode={:?}",
        bytes.len(),
        decode_elapsed,
        resize_elapsed,
        encode_elapsed
    );

    Some(Preview::Image {
        bytes: encoded,
        width,
        height,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tagsy_core::{FileKind, Preview};

    use super::super::tests::from_bytes;
    use super::super::{MAX_IMAGE_EDGE, generate};

    #[test]
    fn small_png_becomes_image_preview() {
        // Encode a tiny solid image, then round-trip it through the generator.
        let mut source = Vec::new();
        let image = image::RgbImage::from_pixel(200, 120, image::Rgb([10, 20, 30]));
        image::DynamicImage::ImageRgb8(image)
            .write_to(&mut Cursor::new(&mut source), image::ImageFormat::Png)
            .unwrap();

        match generate(&from_bytes(&source), FileKind::Image) {
            Some(Preview::Image {
                bytes,
                width,
                height,
            }) => {
                assert!(!bytes.is_empty());
                assert!(width <= MAX_IMAGE_EDGE && height <= MAX_IMAGE_EDGE);
                // Aspect ratio preserved: wider than tall.
                assert!(width >= height);
            }
            other => panic!("expected image, got {other:?}"),
        }
    }

    /// Phone cameras store portrait photos as landscape *pixels* plus an EXIF
    /// `Orientation=6` tag ("rotate 90 CW to display"). The `image` crate does
    /// not apply orientation on decode, so a preview generator that skips the
    /// tag produces a sideways thumbnail — this test reproduces exactly that
    /// scenario and asserts the preview comes out portrait.
    #[test]
    fn jpeg_exif_orientation_is_applied() {
        // Encode a landscape (wider than tall) JPEG with plain `image`. The
        // resulting bytes have no EXIF; we splice one in below.
        let mut jpeg = Vec::new();
        image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(
            200,
            100,
            image::Rgb([200, 50, 50]),
        ))
        .write_to(&mut Cursor::new(&mut jpeg), image::ImageFormat::Jpeg)
        .unwrap();

        // The JPEG must start with SOI (`FFD8`); insert an APP1/EXIF segment
        // right after it, before any other marker. This is exactly how a
        // camera writes EXIF, and it is what `image`'s JPEG decoder reads to
        // populate `ImageDecoder::orientation`.
        assert_eq!(&jpeg[..2], &[0xFF, 0xD8], "expected JPEG SOI marker");

        // Minimal little-endian TIFF/EXIF payload declaring Orientation=6.
        // Layout (mirrors `metadata.rs`'s `locate_orientation_entry`):
        //   "Exif\0\0"                        6 bytes  (APP1 identifier)
        //   TIFF header  "II" 42 0            4 bytes  (little-endian magic)
        //   IFD0 offset  = 8                  4 bytes
        //   IFD0 entry count = 1              2 bytes
        //   Tag 0x0112 (Orientation)          2 bytes
        //   Format 3 (u16)                    2 bytes
        //   Count 1                           4 bytes
        //   Value 6 + u16 padding             4 bytes
        // Total: 28 bytes (next-IFD offset omitted; the decoder does not
        // require it for the single-IFD case).
        let exif_payload: [u8; 28] = [
            b'E', b'x', b'i', b'f', 0x00, 0x00, // APP1 identifier
            0x49, 0x49, 0x2A, 0x00, // TIFF little-endian magic
            0x08, 0x00, 0x00, 0x00, // IFD0 offset = 8
            0x01, 0x00, // 1 entry
            0x12, 0x01, // tag 0x0112 (Orientation)
            0x03, 0x00, // format 3 (u16)
            0x01, 0x00, 0x00, 0x00, // count = 1
            0x06, 0x00, // value = 6 (Rotate 90 CW)
            0x00,
            0x00, /* padding
                   * next-IFD offset intentionally omitted; `image` accepts this. */
        ];
        // APP1 segment length is (2 bytes for the length field itself + payload).
        let app1_len: u16 = 2 + exif_payload.len() as u16;
        let mut app1 = Vec::with_capacity(4 + exif_payload.len());
        app1.extend_from_slice(&[0xFF, 0xE1]); // APP1 marker
        app1.extend_from_slice(&app1_len.to_be_bytes());
        app1.extend_from_slice(&exif_payload);

        let mut with_exif = Vec::with_capacity(jpeg.len() + app1.len());
        with_exif.extend_from_slice(&jpeg[..2]); // SOI
        with_exif.extend_from_slice(&app1); // APP1/EXIF
        with_exif.extend_from_slice(&jpeg[2..]); // rest of the JPEG

        match generate(&from_bytes(&with_exif), FileKind::Image) {
            Some(Preview::Image { width, height, .. }) => {
                // Source pixels were 200x100 (landscape). With Orientation=6
                // applied, the displayed image is 100x200 (portrait), so the
                // thumbnail must be taller than it is wide.
                assert!(
                    height > width,
                    "orientation not applied: got {width}x{height}, expected portrait"
                );
                assert!(width <= MAX_IMAGE_EDGE && height <= MAX_IMAGE_EDGE);
            }
            other => panic!("expected image preview, got {other:?}"),
        }
    }
}
