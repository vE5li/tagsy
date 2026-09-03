// How the app previews a file of a given kind.
//
// File-type classification itself is no longer done here: the daemon is the one
// authority, deciding a file's [FileKindEntry] from its extension alone (see
// `tagsy_core::classify_extension`). Catalog files carry that decision on
// `FileEntry.kind`; a file not yet in the catalog (a share-review candidate on
// disk) is classified by asking the daemon via `Tagsy.classify`. Either way the
// app receives a [FileKindEntry] and never re-derives it — so there is no local
// extension table to drift from the Rust one.
//
// This module maps that received kind to a [PreviewStrategy]: *what* the file
// is dictates which renderer to use, whether the full bytes are worth fetching,
// and what an empty state should say.

import '../rust/api.dart' show FileKindEntry;

/// How a file of a given [FileKindEntry] can be previewed. This is the single
/// decision that drives the whole preview UI.
enum PreviewStrategy {
  /// The app can render the file's real bytes inline (image / text / markdown).
  /// Worth fetching the full file: prefer local bytes, then a cached fetch, then
  /// tap-to-fetch, falling back to the daemon thumbnail only until they arrive.
  renderLocally,

  /// No local renderer, but the daemon can generate an image thumbnail from a
  /// holder (pdf / video). Show that thumbnail; fetching the full bytes would
  /// not render anything, so it is *not* tappable-to-load.
  thumbnailOnly,

  /// Genuinely not previewable (unknown / binary). Show a typed empty state
  /// (icon + name); no daemon round-trip, not tappable.
  none,
}

/// The [PreviewStrategy] for a file [kind].
///
/// SVG is rendered by the daemon into an image thumbnail (the app has no vector
/// renderer of its own), so it takes the [PreviewStrategy.thumbnailOnly] path
/// alongside pdf and video.
PreviewStrategy previewStrategyFor(FileKindEntry kind) => switch (kind) {
  FileKindEntry.image ||
  FileKindEntry.text ||
  FileKindEntry.markdown => PreviewStrategy.renderLocally,
  FileKindEntry.svg ||
  FileKindEntry.pdf ||
  FileKindEntry.video => PreviewStrategy.thumbnailOnly,
  FileKindEntry.other => PreviewStrategy.none,
};
