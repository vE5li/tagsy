// How the home screen renders the *files* section of active-search results.
//
// This only affects the live-query result surface — the config-defined home
// sections ([SectionsView]) are unaffected and always render as rows.
//
// The modes cycle in declaration order on a horizontal swipe (left = next,
// right = previous), and each has its own dedicated entry in the overflow menu.

/// The available file result view modes, in cycle order.
enum FileViewMode {
  /// The original text-only list: one [FileRow] per file (logical path).
  list,

  /// A grid of tiles, each a preview thumbnail with the name underneath.
  tile,

  /// One full-width tile per file: a large preview thumbnail, the name, and the
  /// file's tags below it.
  large,

  /// One full-width tile per file with a much taller preview. Tapping the
  /// preview fetches and shows the full-fidelity file inline (rather than the
  /// small daemon thumbnail); tapping the name opens the file detail screen.
  full;

  /// The next mode in cycle order, wrapping around (for a left swipe).
  FileViewMode get next =>
      FileViewMode.values[(index + 1) % FileViewMode.values.length];

  /// The previous mode in cycle order, wrapping around (for a right swipe).
  FileViewMode get previous =>
      FileViewMode.values[(index - 1 + FileViewMode.values.length) %
          FileViewMode.values.length];
}
