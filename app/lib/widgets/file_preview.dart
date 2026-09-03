import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter_markdown_plus/flutter_markdown_plus.dart';
import 'package:flutter_svg/flutter_svg.dart';

import '../rust/api.dart' show FileKindEntry;

/// Renders the real bytes of a **local** file, dispatched by its type.
///
/// This is the leaf renderer of the preview system: given a path to bytes on
/// disk and the daemon-decided [FileKindEntry], it draws them according to that
/// kind (image / svg / text / markdown). It performs no fetching, and —
/// crucially — no classification: the kind is decided by the daemon and handed
/// in. It knows nothing about the catalog — the [Preview] widget decides *when*
/// local bytes are available and hands them here. Types with no local renderer
/// (pdf / video / other) render a typed empty state rather than a broken view;
/// those types are shown as daemon thumbnails by [Preview], not here.
class FilePreview extends StatelessWidget {
  /// The shared maximum height for a bounded inline preview — used by the file
  /// detail screen and the full-tile search view so a tapped-open preview is
  /// the same size wherever it appears. Tall enough for the full-fidelity image
  /// to be genuinely useful without crowding out the surrounding content.
  static const double maxPreviewHeight = 960;

  /// A smaller max height for text/markdown previews (roughly a third of
  /// [maxPreviewHeight]). A wall of monospace text doesn't benefit from the
  /// full image height and would otherwise crowd out the surrounding content;
  /// the preview stays internally scrollable within this bound.
  static const double maxTextPreviewHeight = maxPreviewHeight / 3;

  /// The fixed height a text/markdown preview occupies in an aspect-sized tile.
  /// Text has no aspect ratio, so a fixed height keeps it compact and its list
  /// extent stable.
  static const double tileTextPreviewHeight = 180;

  final String path;

  /// The file's kind, decided by the daemon (never re-derived here).
  final FileKindEntry kind;
  final int textByteLimit;

  /// Whether text/markdown may scroll within its bounds. Off by default: in a
  /// tile a scrollable text view traps the list scroll, so tiles render a
  /// clipped snippet. The detail/share screens opt in to read long files.
  final bool scrollable;

  const FilePreview({
    super.key,
    required this.path,
    required this.kind,
    this.textByteLimit = 256 * 1024, // 256 KB
    this.scrollable = false,
  });

  @override
  Widget build(BuildContext context) {
    final file = File(path);

    switch (kind) {
      case FileKindEntry.image:
        // Full width, `BoxFit.contain` to preserve aspect. No pan/zoom (it
        // fights a surrounding scroll).
        return SizedBox(
          width: double.infinity,
          child: Image.file(
            file,
            fit: BoxFit.contain,
            // Fade in on decode so a swap over a thumbnail doesn't blank-flash.
            frameBuilder: (context, child, frame, wasSynchronouslyLoaded) {
              if (wasSynchronouslyLoaded) return child;
              return AnimatedOpacity(
                opacity: frame == null ? 0 : 1,
                duration: const Duration(milliseconds: 200),
                curve: Curves.easeOut,
                child: child,
              );
            },
          ),
        );

      case FileKindEntry.svg:
        // Full width, contained to preserve aspect — the vector counterpart of
        // the image arm. `flutter_svg` decodes and rasterizes at display size.
        return SizedBox(
          width: double.infinity,
          child: SvgPicture.file(file, fit: BoxFit.contain),
        );

      case FileKindEntry.markdown:
        return _AsyncText(
          loadKey: path,
          load: () => file.readAsString(),
          builder: (text) => scrollable
              ? Markdown(data: text, selectable: true)
              : _ClippedSnippet(child: MarkdownBody(data: text)),
        );

      case FileKindEntry.text:
        return _AsyncText(
          loadKey: path,
          load: () => _readTextHead(file, textByteLimit),
          builder: (text) => scrollable
              ? SingleChildScrollView(
                  padding: const EdgeInsets.all(12),
                  child: SelectableText(
                    text,
                    style: const TextStyle(
                      fontFamily: 'monospace',
                      fontSize: 13,
                    ),
                  ),
                )
              : _ClippedSnippet(
                  child: Text(
                    text,
                    style: const TextStyle(
                      fontFamily: 'monospace',
                      fontSize: 13,
                    ),
                  ),
                ),
        );

      // No local renderer; `Preview` shows pdf/video as daemon thumbnails and
      // doesn't route them here (this is the share-review / defensive path).
      case FileKindEntry.pdf:
      case FileKindEntry.video:
      case FileKindEntry.other:
        return PreviewEmptyState(kind: kind, name: nameOf(path));
    }
  }

  static Future<String> _readTextHead(File file, int limit) async {
    final len = await file.length();
    if (len <= limit) return file.readAsString();
    final raf = await file.open();
    try {
      final bytes = await raf.read(limit);
      return '${String.fromCharCodes(bytes)}\n\n… (truncated)';
    } finally {
      await raf.close();
    }
  }
}

/// The basename of [path] (the segment after the last separator).
String nameOf(String path) => path.split(Platform.pathSeparator).last;

/// A typed empty/placeholder tile for a file that can't be rendered inline
/// (or whose thumbnail is unavailable). The icon reflects the file's kind so
/// the user gets a meaningful hint (a document, a film reel, a generic file)
/// rather than a bare "no preview".
class PreviewEmptyState extends StatelessWidget {
  const PreviewEmptyState({
    super.key,
    required this.kind,
    required this.name,
    this.subtitle,
  });

  final FileKindEntry kind;
  final String name;

  /// An optional override for the secondary line; defaults to a kind-specific
  /// message.
  final String? subtitle;

  @override
  Widget build(BuildContext context) {
    return ListTile(
      leading: Icon(_iconFor(kind)),
      title: Text(name, maxLines: 1, overflow: TextOverflow.ellipsis),
      subtitle: Text(subtitle ?? _messageFor(kind)),
    );
  }

  static IconData _iconFor(FileKindEntry kind) => switch (kind) {
    FileKindEntry.pdf => Icons.picture_as_pdf_outlined,
    FileKindEntry.video => Icons.movie_outlined,
    FileKindEntry.image || FileKindEntry.svg => Icons.image_outlined,
    FileKindEntry.text ||
    FileKindEntry.markdown => Icons.description_outlined,
    FileKindEntry.other => Icons.insert_drive_file_outlined,
  };

  static String _messageFor(FileKindEntry kind) => switch (kind) {
    FileKindEntry.pdf => 'No preview available for this PDF.',
    FileKindEntry.video => 'No preview available for this video.',
    FileKindEntry.image ||
    FileKindEntry.svg => 'No preview available for this image.',
    FileKindEntry.text ||
    FileKindEntry.markdown => 'No preview available for this file.',
    FileKindEntry.other => 'This file type cannot be previewed.',
  };
}

/// A non-scrolling, non-interactive, top-aligned text snippet for tile previews:
/// lays out at natural height and clips the overflow. [IgnorePointer] so it
/// captures neither the tap-to-load nor the list scroll.
class _ClippedSnippet extends StatelessWidget {
  const _ClippedSnippet({required this.child});

  final Widget child;

  @override
  Widget build(BuildContext context) {
    return IgnorePointer(
      child: ClipRect(
        child: Padding(
          padding: const EdgeInsets.all(12),
          child: Align(
            alignment: Alignment.topLeft,
            child: OverflowBox(
              alignment: Alignment.topLeft,
              maxHeight: double.infinity,
              child: child,
            ),
          ),
        ),
      ),
    );
  }
}

/// Loads text off disk once and renders it via [builder]. Stateful so the read
/// future is created once and only re-created when [loadKey] (the path) changes;
/// otherwise every rebuild would re-read and flash the spinner.
class _AsyncText extends StatefulWidget {
  final Object loadKey;
  final Future<String> Function() load;
  final Widget Function(String text) builder;

  const _AsyncText({
    required this.loadKey,
    required this.load,
    required this.builder,
  });

  @override
  State<_AsyncText> createState() => _AsyncTextState();
}

class _AsyncTextState extends State<_AsyncText> {
  late Future<String> _future;

  @override
  void initState() {
    super.initState();
    _future = widget.load();
  }

  @override
  void didUpdateWidget(_AsyncText oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.loadKey != widget.loadKey) {
      _future = widget.load();
    }
  }

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<String>(
      future: _future,
      builder: (context, snap) {
        if (snap.connectionState != ConnectionState.done) {
          return const Center(child: CircularProgressIndicator());
        }
        if (snap.hasError) {
          return Center(child: Text('Failed to load: ${snap.error}'));
        }
        return widget.builder(snap.data ?? '');
      },
    );
  }
}
