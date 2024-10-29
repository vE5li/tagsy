// The extension -> preview-kind table for local files, kept deliberately in
// lockstep with the daemon's `classify_by_extension` (tagsyd/src/preview/mod.rs).
//
// Two independent extension tables used to exist: this local one (which decides
// how the file-detail / share-review inline previews render a file already on
// disk) and the daemon's remote one (which decides whether a peer can generate
// a preview for a file we *don't* hold). They disagreed — a file could be
// previewable one way and not the other — with no mechanism to keep them
// aligned.
//
// This module makes the local table mirror the Rust one exactly, using the same
// `Kind` categories, and `test/extension_kind_test.dart` pins the two together:
// it carries the authoritative extension set from `classify_by_extension` and
// fails if this table drifts from it. When the Rust table changes, update the
// sets below AND the expected set in that test.
//
// The one refinement the local side adds is [ExtensionKind.markdown], a subset
// of the Rust `Text` category: the daemon only cares that markdown is *text*,
// but the local previewer renders it richly (see file_preview.dart), so it
// needs to tell markdown apart from plain text. Every markdown extension is
// therefore also a member of the Rust `Text` set (asserted by the test).

/// What kind of preview a local file's extension warrants.
///
/// Mirrors the daemon's `preview::Kind` (`Image`/`Pdf`/`Video`/`Text`/`Other`),
/// plus a local-only [markdown] refinement of the text category.
enum ExtensionKind { image, pdf, video, text, markdown, other }

/// Image extensions the daemon's `image` crate can decode. Mirrors the `Image`
/// arm of `classify_by_extension`.
const Set<String> kImageExtensions = {
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

/// Mirrors the `Pdf` arm.
const Set<String> kPdfExtensions = {'pdf'};

/// Containers ffmpeg can pull a frame from. Mirrors the `Video` arm.
const Set<String> kVideoExtensions = {
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

/// Markdown extensions — a local-only refinement rendered richly by
/// [FilePreview]. Every entry is also part of [kTextExtensions] (the daemon
/// treats markdown as plain text); the reconciliation test asserts that
/// subset relationship.
const Set<String> kMarkdownExtensions = {'md', 'markdown'};

/// Text / code / markup extensions. Mirrors the `Text` arm of
/// `classify_by_extension` exactly (markdown included, since the daemon files
/// it under text).
const Set<String> kTextExtensions = {
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

/// The lowercase extension (no dot) of [path], or `''` if it has none.
String extensionOf(String path) {
  final dot = path.lastIndexOf('.');
  return dot == -1 ? '' : path.substring(dot + 1).toLowerCase();
}

/// Classify [extension] (lowercase, no dot) into an [ExtensionKind].
///
/// [markdown] is reported ahead of [text] so the caller can render it richly;
/// callers that don't care about the distinction can treat markdown as text.
ExtensionKind classifyExtension(String extension) {
  if (kImageExtensions.contains(extension)) return ExtensionKind.image;
  if (kPdfExtensions.contains(extension)) return ExtensionKind.pdf;
  if (kVideoExtensions.contains(extension)) return ExtensionKind.video;
  if (kMarkdownExtensions.contains(extension)) return ExtensionKind.markdown;
  if (kTextExtensions.contains(extension)) return ExtensionKind.text;
  return ExtensionKind.other;
}
