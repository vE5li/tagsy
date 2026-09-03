import 'dart:async';
import 'dart:typed_data';

import 'package:flutter/material.dart';

import '../data/repository.dart';
import '../preview/extension_kind.dart';
import '../rust/api.dart' as tagsy;
import 'file_preview.dart';

/// The one preview widget every screen uses for a catalog file.
///
/// The whole design turns on a single decision made up front from the file's
/// *type* — the daemon's authoritative [tagsy.FileEntry.kind], not whatever the
/// daemon happens to return for the preview: [previewStrategyFor] maps that kind
/// into one of three strategies, and everything else follows.
///
/// - [PreviewStrategy.renderLocally] (image / text / markdown): the real bytes
///   are worth showing. We render them from the best available source, in order:
///   a local sync-directory copy, a cached previous fetch, or — on tap — a fresh
///   fetch. Until real bytes are on hand we show the daemon's small thumbnail
///   (for images) as a placeholder. These previews are *tappable* to load.
/// - [PreviewStrategy.thumbnailOnly] (pdf / video): no local renderer exists, so
///   we show the daemon's generated image thumbnail and nothing more. Fetching
///   the full bytes would render nothing, so these are **not** tappable.
/// - [PreviewStrategy.none] (everything else): not previewable; a typed empty
///   state is shown immediately with no daemon round-trip.
///
/// Reuse of a fetched full file is shared across the app via the repository's
/// fetched-file cache (keyed by content hash), so a file loaded full-res in one
/// tile shows full-res when its detail screen opens, and share/download reuse it.
class Preview extends StatefulWidget {
  const Preview({
    super.key,
    required this.repository,
    required this.file,
    this.sizeToAspect = false,
    this.allowTextScroll = false,
  });

  final TagsyRepository repository;

  /// The catalog file to preview. Its `path` drives the type decision; its
  /// `fileId`/`contentHash` drive lookups and fetches.
  final tagsy.FileEntry file;

  /// Whether text/markdown previews may scroll within their bounds (forwarded to
  /// [FilePreview.scrollable]). Off in tiles (a scrolling text view traps the
  /// list scroll); on for the dedicated detail-screen preview.
  final bool allowTextScroll;

  /// Whether to size the preview box to the image's aspect ratio, reserving a
  /// stable height synchronously from [_previewAspectCache]. Off by default:
  /// callers with their own fixed box (grid/large tiles) must not fight it with
  /// an inner `AspectRatio`. On for compact, scroll-stable previews (the
  /// full-tile list, the detail screen).
  final bool sizeToAspect;

  @override
  State<Preview> createState() => _PreviewState();
}

/// Process-wide cache of resolved image-preview aspect ratios, keyed by content
/// hash. Lets a [Preview] reserve its box height synchronously on (re)build,
/// before the thumbnail loads, so a scrolled-away tile doesn't collapse to a
/// loading tile and shove the surrounding [SliverList].
final Map<String, double> _previewAspectCache = {};

class _PreviewState extends State<Preview> {
  /// The strategy chosen from the file's type. Fixed for the widget's identity
  /// (a content change rebuilds via the key its callers set).
  late PreviewStrategy _strategy;

  /// The daemon thumbnail request, or null for [PreviewStrategy.none] (which
  /// needs no daemon round-trip).
  Future<tagsy.PreviewEntry>? _thumbnail;

  /// A local sync-directory path holding the file's bytes, if any.
  String? _localPath;

  /// A full-file path we've fetched (or reused from the cache) to render at full
  /// fidelity. Only meaningful for [PreviewStrategy.renderLocally].
  String? _fetchedPath;

  /// Whether a full-file fetch is currently in flight (peer round-trip).
  bool _fetching = false;

  /// A full-file fetch failure to surface; cleared on a fresh tap.
  Object? _fetchError;

  /// The last resolved image-thumbnail bytes, kept so a fetched full image can
  /// cross-fade in over the thumbnail rather than blanking while it decodes.
  Uint8List? _thumbnailBytes;

  /// The thumbnail's aspect ratio (width / height) when known, used to reserve
  /// a stable box height (see [_previewAspectCache]).
  double? _thumbnailAspect;

  bool get _renderLocally => _strategy == PreviewStrategy.renderLocally;

  /// Subscription to the repository's "full file fetched" broadcast, so a
  /// preview still showing the thumbnail swaps to the full render the moment
  /// another surface (a tile we tapped, then navigated away from) finishes
  /// fetching the same content.
  StreamSubscription<String>? _fetchedSub;

  @override
  void initState() {
    super.initState();
    _initForCurrentFile();
    _fetchedSub = widget.repository.fetchedFiles.listen(_onFileFetched);
  }

  @override
  void didUpdateWidget(Preview oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.file.fileId != widget.file.fileId ||
        oldWidget.file.contentHash != widget.file.contentHash ||
        oldWidget.file.path != widget.file.path) {
      _initForCurrentFile();
    }
  }

  @override
  void dispose() {
    _fetchedSub?.cancel();
    super.dispose();
  }

  /// A full file finished fetching somewhere. If it's *this* file's content and
  /// we're still on the thumbnail, pick up the now-cached full render.
  void _onFileFetched(String contentHash) {
    if (!mounted ||
        contentHash != widget.file.contentHash ||
        !_renderLocally ||
        _fetchedPath != null ||
        _localPath != null) {
      return;
    }
    final path = widget.repository.cachedFetchedPath(contentHash);
    if (path == null) return;
    setState(() => _fetchedPath = path);
  }

  /// (Re)initialize all per-file state from the current [Preview.file].
  void _initForCurrentFile() {
    _strategy = previewStrategyFor(widget.file.kind);
    _fetchError = null;
    _thumbnailBytes = null;
    _localPath = null;
    _fetchedPath = null;
    _thumbnailAspect = _previewAspectCache[widget.file.contentHash];

    // 'none' needs nothing from the daemon or disk.
    if (_strategy == PreviewStrategy.none) {
      _thumbnail = null;
      return;
    }

    _thumbnail = widget.repository.getPreview(fileId: widget.file.fileId);
    _recordThumbnailMetadata(_thumbnail!);

    if (_renderLocally) {
      // Reuse a full-res copy we already have on disk for this content, so we
      // show full fidelity immediately instead of the thumbnail.
      _fetchedPath = widget.repository.cachedFetchedPath(
        widget.file.contentHash,
      );
      _resolveLocalPath();
    }
  }

  /// Ask the daemon whether a local sync directory holds this file's bytes; if
  /// so, render from that path directly. Best-effort — absence (not synced here)
  /// is the normal case.
  Future<void> _resolveLocalPath() async {
    try {
      final path = await widget.repository.localPathForFile(
        fileId: widget.file.fileId,
      );
      if (!mounted || path == null) return;
      // A content change may have swapped the file out from under this async
      // call; ignore a stale result.
      if (previewStrategyFor(widget.file.kind) != PreviewStrategy.renderLocally) {
        return;
      }
      setState(() => _localPath = path);
    } catch (_) {
      // Fall back to the thumbnail / fetch path.
    }
  }

  /// Record the thumbnail's bytes and aspect ratio into state once [future]
  /// resolves. Done off the build path (not inside the FutureBuilder) so the
  /// `setState` triggers the rebuild that applies the reserved [AspectRatio].
  Future<void> _recordThumbnailMetadata(
    Future<tagsy.PreviewEntry> future,
  ) async {
    final tagsy.PreviewEntry preview;
    try {
      preview = await future;
    } catch (_) {
      return; // Errors surface via the FutureBuilder; nothing to record.
    }
    if (!mounted || !identical(_thumbnail, future)) return;
    if (preview.kind != tagsy.PreviewKind.image) return;
    final bytes = preview.imageBytes;
    if (bytes == null || bytes.isEmpty) return;
    final w = preview.width;
    final h = preview.height;
    final aspect = (w != null && h != null && w > 0 && h > 0) ? w / h : null;
    setState(() {
      _thumbnailBytes = bytes;
      if (aspect != null) {
        _thumbnailAspect = aspect;
        _previewAspectCache[widget.file.contentHash] = aspect;
      }
    });
  }

  /// Pull the full file to a daemon-owned temp path (from a peer if needed) and
  /// render it at full fidelity. The fetched path is owned and reused by the
  /// repository's fetched-file cache, so we don't delete it here.
  Future<void> _fetchFullFile() async {
    if (!_renderLocally ||
        _fetching ||
        _fetchedPath != null ||
        _localPath != null) {
      return;
    }
    setState(() {
      _fetching = true;
      _fetchError = null;
    });
    try {
      final path = await widget.repository.fetchFileCached(
        fileId: widget.file.fileId,
        expectedHash: widget.file.contentHash,
      );
      if (!mounted) return;
      setState(() {
        _fetchedPath = path;
        _fetching = false;
      });
    } catch (error) {
      if (!mounted) return;
      setState(() {
        _fetchError = error;
        _fetching = false;
      });
    }
  }

  /// Re-request the thumbnail after a transient failure.
  void _retry() {
    setState(() {
      _thumbnail = widget.repository.getPreview(fileId: widget.file.fileId);
    });
  }

  @override
  Widget build(BuildContext context) {
    final content = _buildContent(context);
    // A subtly lighter frame behind the preview so images/text/empty states
    // read as a distinct "preview" area against the tile/screen background.
    return PreviewFrame(child: _sized(content));
  }

  /// Reserve a stable box height for [sizeToAspect] callers: images to their
  /// cached aspect ratio, tile text to a fixed compact height (text has no
  /// aspect, and an unstable height lurches the list). Everything else — and all
  /// non-[sizeToAspect] callers — passes through untouched.
  Widget _sized(Widget content) {
    if (!widget.sizeToAspect) return content;
    final aspect = _thumbnailAspect;
    if (aspect != null) return AspectRatio(aspectRatio: aspect, child: content);
    final kind = widget.file.kind;
    final isText =
        kind == tagsy.FileKindEntry.text ||
        kind == tagsy.FileKindEntry.markdown;
    if (isText && !widget.allowTextScroll) {
      return SizedBox(height: FilePreview.tileTextPreviewHeight, child: content);
    }
    return content;
  }

  Widget _buildContent(BuildContext context) {
    // Not previewable: a typed empty state, no daemon round-trip.
    if (_strategy == PreviewStrategy.none) {
      return PreviewEmptyState(
        kind: widget.file.kind,
        name: nameOf(widget.file.path),
      );
    }

    // Locally renderable and we have real bytes: render them at full fidelity.
    if (_renderLocally) {
      final local = _localPath;
      if (local != null) {
        return FilePreview(
          path: local,
          kind: widget.file.kind,
          scrollable: widget.allowTextScroll,
        );
      }
      final fetched = _fetchedPath;
      if (fetched != null) return _buildFetched(fetched);
    }

    // Otherwise fall back to the daemon thumbnail (a placeholder for
    // renderLocally until bytes arrive; the only view for thumbnailOnly).
    return _buildThumbnail(context);
  }

  Widget _buildThumbnail(BuildContext context) {
    return FutureBuilder<tagsy.PreviewEntry>(
      future: _thumbnail,
      builder: (context, snapshot) {
        if (snapshot.connectionState != ConnectionState.done) {
          return _loadingTile();
        }
        if (snapshot.hasError) {
          return _errorTile(snapshot.error);
        }
        return _wrapTappable(context, _buildThumbnailData(snapshot.data!));
      },
    );
  }

  Widget _loadingTile() {
    // Only for the initial thumbnail request; a full-file fetch keeps the
    // thumbnail up with the corner spinner from `_wrapTappable` instead.
    return const _StatusTile(
      icon: Icons.image_outlined,
      title: 'Loading preview…',
      subtitle: 'Fetching a preview from a peer that holds this file.',
      trailing: SizedBox(
        width: 20,
        height: 20,
        child: CircularProgressIndicator(strokeWidth: 2),
      ),
    );
  }

  Widget _errorTile(Object? error) {
    // `UnknownId`: the file is gone (deleted while open). Permanent.
    if (error is tagsy.ApiError_UnknownId) {
      return const _StatusTile(
        icon: Icons.help_outline,
        title: 'File no longer exists',
        subtitle: 'It was deleted while this screen was open.',
      );
    }
    // `ContentUnavailable`: exists, but no reachable holder right now. Transient
    // — the daemon doesn't cache it, so a retry once a holder is online works.
    final transient = error is tagsy.ApiError_ContentUnavailable;
    return _StatusTile(
      icon: Icons.cloud_off_outlined,
      title: transient ? 'Preview unavailable' : 'Failed to load preview',
      subtitle: transient
          ? 'No device holding this file is reachable right now.'
          : '$error',
      trailing: IconButton(
        icon: const Icon(Icons.refresh),
        tooltip: 'Retry',
        onPressed: _retry,
      ),
    );
  }

  Widget _buildThumbnailData(tagsy.PreviewEntry preview) {
    switch (preview.kind) {
      case tagsy.PreviewKind.image:
        final bytes = preview.imageBytes;
        if (bytes == null || bytes.isEmpty) return _noThumbnail();
        // Centered so a box taller than the image's aspect (grid/large tiles)
        // centers the thumbnail like the full preview.
        return Center(
          child: SizedBox(
            width: double.infinity,
            child: Image.memory(bytes, fit: BoxFit.contain),
          ),
        );
      case tagsy.PreviewKind.text:
        // A short daemon snippet, shown until the full bytes arrive. Clipped and
        // non-interactive so it neither captures the tap-to-load nor scrolls.
        return IgnorePointer(
          child: ClipRect(
            child: Padding(
              padding: const EdgeInsets.all(12),
              child: Align(
                alignment: Alignment.topLeft,
                child: Text(
                  preview.text ?? '',
                  style: const TextStyle(
                    fontFamily: 'monospace',
                    fontSize: 13,
                  ),
                ),
              ),
            ),
          ),
        );
      case tagsy.PreviewKind.none:
        return _noThumbnail();
    }
  }

  /// The daemon has (authoritatively) no thumbnail for this content. Show a
  /// typed empty state keyed to the file's kind.
  Widget _noThumbnail() {
    return PreviewEmptyState(
      kind: widget.file.kind,
      name: nameOf(widget.file.path),
    );
  }

  /// Render the fetched full file, cross-fading it in over the still-painted
  /// thumbnail so the preview never blanks or resizes mid-swap.
  Widget _buildFetched(String path) {
    final thumbnail = _thumbnailBytes;
    final aspect = _thumbnailAspect;
    // Keep the thumbnail underneath only with a known aspect box to give
    // `StackFit.expand` bounds; otherwise swap directly.
    if (thumbnail == null || aspect == null) {
      return FilePreview(
        path: path,
        kind: widget.file.kind,
        scrollable: widget.allowTextScroll,
      );
    }

    final stack = Stack(
      fit: StackFit.expand,
      children: [
        Image.memory(thumbnail, fit: BoxFit.contain),
        FilePreview(
          path: path,
          kind: widget.file.kind,
          scrollable: widget.allowTextScroll,
        ),
      ],
    );
    // `build` reserves the aspect box when [sizeToAspect]; else wrap our own.
    if (widget.sizeToAspect) return stack;
    return AspectRatio(aspectRatio: aspect, child: stack);
  }

  /// Make a locally-renderable preview tappable to pull the full file (pdf/video
  /// aren't tappable — their bytes wouldn't render). A fetch in flight shows a
  /// corner spinner; a failure a corner error marker (tap again to retry).
  Widget _wrapTappable(BuildContext context, Widget child) {
    if (!_renderLocally) return child;

    final theme = Theme.of(context);
    Widget? corner;
    if (_fetching) {
      corner = const SizedBox(
        width: 18,
        height: 18,
        child: CircularProgressIndicator(strokeWidth: 2),
      );
    } else if (_fetchError != null) {
      final transient = _fetchError is tagsy.ApiError_ContentUnavailable;
      corner = Icon(
        transient ? Icons.cloud_off_outlined : Icons.error_outline,
        size: 18,
        color: theme.colorScheme.error,
      );
    }

    // A tap-only GestureDetector yields the arena to a surrounding scrollable's
    // drag, so it never fights the list scroll.
    return GestureDetector(
      behavior: HitTestBehavior.opaque,
      onTap: _fetching ? null : _fetchFullFile,
      child: Stack(
        children: [
          child,
          if (corner != null)
            Positioned(
              top: 8,
              right: 8,
              child: IgnorePointer(child: corner),
            ),
        ],
      ),
    );
  }
}

/// The preview's framed background — a surface slightly lighter (a low-opacity
/// surface tint) than the surrounding tile/screen, so a preview reads as a
/// distinct area. Used by [Preview] and by screens that render a [FilePreview]
/// directly (share-review) so the framing is consistent everywhere.
class PreviewFrame extends StatelessWidget {
  const PreviewFrame({super.key, required this.child});

  final Widget child;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return DecoratedBox(
      decoration: BoxDecoration(
        color: theme.colorScheme.surfaceContainerHighest.withValues(alpha: 0.4),
      ),
      child: child,
    );
  }
}

/// A compact status tile for the transient thumbnail states (loading, error).
class _StatusTile extends StatelessWidget {
  const _StatusTile({
    required this.icon,
    required this.title,
    required this.subtitle,
    this.trailing,
  });

  final IconData icon;
  final String title;
  final String subtitle;
  final Widget? trailing;

  @override
  Widget build(BuildContext context) {
    return ListTile(
      leading: Icon(icon),
      title: Text(title),
      subtitle: Text(subtitle),
      trailing: trailing,
    );
  }
}
