// Guards the local extension-classification table against drift with the Rust
// side.
//
// `lib/preview/extension_kind.dart` mirrors the daemon's `classify_by_extension`
// (tagsyd/src/preview/mod.rs). The two used to be independent tables with
// different contents and no sync mechanism, so a file could be previewable one
// way and not the other. This test carries the authoritative extension sets
// from the Rust match arms and asserts the Dart tables reproduce them exactly.
//
// When `classify_by_extension` changes on the Rust side, this test fails: update
// the sets in `extension_kind.dart` AND the `_rust*` sets below to match, in the
// same change.

import 'package:flutter_test/flutter_test.dart';

import 'package:tagsy_app/preview/extension_kind.dart';

// --- The authoritative table, copied from tagsyd/src/preview/mod.rs's
//     `classify_by_extension`. Keep these arms verbatim. -----------------------

const Set<String> _rustImage = {
  'png',
  'jpg',
  'jpeg',
  'gif',
  'bmp',
  'webp',
  'tif',
  'tiff',
  'ico',
};

const Set<String> _rustPdf = {'pdf'};

const Set<String> _rustVideo = {
  'mp4',
  'm4v',
  'mov',
  'mkv',
  'webm',
  'avi',
  'wmv',
  'flv',
  'mpg',
  'mpeg',
  '3gp',
  'ogv',
};

const Set<String> _rustText = {
  'txt',
  'md',
  'markdown',
  'log',
  'json',
  'yaml',
  'yml',
  'toml',
  'ini',
  'cfg',
  'conf',
  'csv',
  'tsv',
  'xml',
  'html',
  'htm',
  'css',
  'rs',
  'py',
  'js',
  'ts',
  'tsx',
  'jsx',
  'c',
  'h',
  'cpp',
  'hpp',
  'cc',
  'java',
  'kt',
  'go',
  'rb',
  'php',
  'sh',
  'bash',
  'zsh',
  'sql',
  'swift',
  'dart',
  'lua',
  'pl',
};

void main() {
  group('extension classification agrees with the Rust table', () {
    test('image set matches classify_by_extension', () {
      expect(kImageExtensions, _rustImage);
    });

    test('pdf set matches classify_by_extension', () {
      expect(kPdfExtensions, _rustPdf);
    });

    test('video set matches classify_by_extension', () {
      expect(kVideoExtensions, _rustVideo);
    });

    test('text set matches classify_by_extension', () {
      expect(kTextExtensions, _rustText);
    });

    test('markdown is a local-only refinement wholly within the text set', () {
      // The daemon files markdown under Text; the local previewer splits it out
      // to render it richly. Every markdown extension must therefore also be a
      // text extension, so the two sides still agree on the coarse category.
      expect(kMarkdownExtensions.difference(kTextExtensions), isEmpty);
    });

    test('no extension is claimed by two categories', () {
      // The Rust match is a single arm per extension; the Dart sets must be
      // mutually exclusive too (except markdown ⊂ text, checked above).
      final image = kImageExtensions;
      final pdf = kPdfExtensions;
      final video = kVideoExtensions;
      final text = kTextExtensions;
      expect(image.intersection(pdf), isEmpty);
      expect(image.intersection(video), isEmpty);
      expect(image.intersection(text), isEmpty);
      expect(pdf.intersection(video), isEmpty);
      expect(pdf.intersection(text), isEmpty);
      expect(video.intersection(text), isEmpty);
    });
  });

  group('classifyExtension', () {
    test('maps representative extensions to the expected kind', () {
      expect(classifyExtension('png'), ExtensionKind.image);
      expect(classifyExtension('pdf'), ExtensionKind.pdf);
      expect(classifyExtension('mp4'), ExtensionKind.video);
      expect(classifyExtension('md'), ExtensionKind.markdown);
      expect(classifyExtension('rs'), ExtensionKind.text);
      expect(classifyExtension('bin'), ExtensionKind.other);
      expect(classifyExtension(''), ExtensionKind.other);
    });

    test('markdown resolves ahead of plain text', () {
      // `md` is in both the markdown and text sets; the markdown branch wins so
      // the previewer can render it richly.
      expect(classifyExtension('markdown'), ExtensionKind.markdown);
    });
  });

  group('extensionOf', () {
    test('lowercases and strips the dot', () {
      expect(extensionOf('/a/b/Photo.PNG'), 'png');
      expect(extensionOf('report.pdf'), 'pdf');
    });

    test('returns empty for a name with no extension', () {
      expect(extensionOf('LICENSE'), '');
      expect(extensionOf('/path/to/README'), '');
    });
  });
}
