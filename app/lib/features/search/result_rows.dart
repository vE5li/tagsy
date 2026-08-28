// The individual rows the home screen's search results are built from: a
// section header, a tag row, a file row, and the one-off "create tag" row.
//
// Each interactive row takes a [FocusNode] owned by the enclosing
// [RovingFocusList] (see widgets/roving_focus_list.dart) so the keyboard's
// roving tab-stop can land on it, plus an `onActivate` callback the home screen
// wires to navigation (it keeps navigation on the state so it can restore focus
// to the row when the pushed detail screen pops).

import 'package:flutter/material.dart';

import '../../data/repository.dart';
import '../../rust/api.dart' as tagsy;
import '../../widgets/remote_preview.dart';
import '../../widgets/tag_chip.dart';
// SectionHeader moved to a shared widget (it's used well beyond search now);
// re-exported so existing `result_rows.dart` importers keep resolving it.
export '../../widgets/section_header.dart';

/// A single tag result row (color swatch + name), opening the tag detail on
/// activate.
class TagRow extends StatelessWidget {
  const TagRow({
    super.key,
    required this.tag,
    this.focusNode,
    required this.onActivate,
  });

  final tagsy.TagEntry tag;

  /// The stable focus node the enclosing [RovingFocusList] owns for this row's
  /// slot; attach it so the roving tab-stop can land here. Null in surfaces
  /// without roving navigation (e.g. the tile view's tag list).
  final FocusNode? focusNode;

  /// Invoked on tap or Enter. Navigation lives on the home screen so it can
  /// restore focus to this row's slot when the detail screen pops.
  final VoidCallback onActivate;

  @override
  Widget build(BuildContext context) {
    // Deleted rows only appear under the "show deleted" toggle; strike the
    // name through so the user can tell at a glance that a row is a
    // tombstone rather than a live tag.
    final titleStyle = tag.deleted
        ? const TextStyle(decoration: TextDecoration.lineThrough)
        : null;
    return ListTile(
      dense: true,
      visualDensity: VisualDensity.compact,
      minVerticalPadding: 0,
      focusNode: focusNode,
      leading: TagColorSwatch(color: tag.color),
      title: Text(tag.name, style: titleStyle),
      trailing: const Icon(Icons.chevron_right),
      onTap: onActivate,
    );
  }
}

/// A one-off row rendered under the Tags section when the current query looks
/// like a plausible tag name and no tag with that name (or any substring
/// match) exists yet. Tapping it creates the tag with the engine's default
/// color; the user can recolor via the tag detail screen.
class CreateTagRow extends StatelessWidget {
  const CreateTagRow({
    super.key,
    required this.name,
    required this.onCreate,
    this.focusNode,
  });

  final String name;
  final VoidCallback onCreate;

  /// See [TagRow.focusNode].
  final FocusNode? focusNode;

  @override
  Widget build(BuildContext context) {
    return ListTile(
      dense: true,
      visualDensity: VisualDensity.compact,
      minVerticalPadding: 0,
      focusNode: focusNode,
      leading: const Icon(Icons.add),
      title: Text('Create tag "$name"'),
      onTap: onCreate,
    );
  }
}

/// A single file result row (logical path), opening the file detail on
/// activate.
class FileRow extends StatelessWidget {
  const FileRow({
    super.key,
    required this.file,
    required this.focusNode,
    required this.onActivate,
  });

  final tagsy.FileEntry file;

  /// See [TagRow.focusNode].
  final FocusNode focusNode;

  /// See [TagRow.onActivate].
  final VoidCallback onActivate;

  @override
  Widget build(BuildContext context) {
    // See [TagRow] for why we strike deleted rows through.
    final titleStyle = file.deleted
        ? const TextStyle(decoration: TextDecoration.lineThrough)
        : null;
    return ListTile(
      dense: true,
      visualDensity: VisualDensity.compact,
      minVerticalPadding: 0,
      focusNode: focusNode,
      title: Text(file.path, style: titleStyle),
      trailing: const Icon(Icons.chevron_right),
      onTap: onActivate,
    );
  }
}

/// A single file result rendered as a grid tile: a preview thumbnail on top
/// with the logical name underneath. The tile view mode ([FileViewMode.tile])
/// on the home screen's search results uses these in a [GridView] in place of
/// [FileRow]s.
///
/// The thumbnail reuses [RemotePreview], which self-manages the per-file fetch
/// (image/text/none/loading/retry) keyed by `fileId` + `contentHash`. Tiles are
/// tappable but are not part of the list's roving arrow-key navigation.
class FileTile extends StatelessWidget {
  const FileTile({
    super.key,
    required this.file,
    required this.repository,
    required this.onActivate,
  });

  final tagsy.FileEntry file;

  /// The repository the embedded [RemotePreview] fetches the thumbnail through.
  final TagsyRepository repository;

  /// Invoked on tap; the home screen wires this to open the file detail.
  final VoidCallback onActivate;

  @override
  Widget build(BuildContext context) {
    // See [TagRow] for why we strike deleted rows through. The basename reads
    // better than the full logical path in a narrow tile; the full path is
    // still available on the detail screen.
    final titleStyle = file.deleted
        ? const TextStyle(decoration: TextDecoration.lineThrough)
        : null;
    final name = file.path.split('/').last;
    final theme = Theme.of(context);
    return Card(
      clipBehavior: Clip.antiAlias,
      margin: EdgeInsets.zero,
      child: InkWell(
        onTap: onActivate,
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Expanded(
              child: RemotePreview(
                repository: repository,
                fileId: file.fileId,
                contentHash: file.contentHash,
              ),
            ),
            Padding(
              padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 6),
              child: Text(
                name,
                maxLines: 2,
                overflow: TextOverflow.ellipsis,
                style: (theme.textTheme.bodySmall ?? const TextStyle()).merge(
                  titleStyle,
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

/// A single file result rendered as a large, full-width tile: a tall preview
/// thumbnail on top, the logical name, and the file's tags below it. Used by the
/// large view mode ([FileViewMode.large]) on the home screen's search results,
/// one per row. Like [FileTile], the thumbnail reuses [RemotePreview]; the tag
/// strip is fetched per-file by [FileTagStrip]. Tappable but outside the roving
/// arrow-key navigation.
class FileLargeTile extends StatelessWidget {
  const FileLargeTile({
    super.key,
    required this.file,
    required this.repository,
    required this.onActivate,
    required this.onOpenTag,
  });

  final tagsy.FileEntry file;

  /// The repository the embedded [RemotePreview] / [FileTagStrip] fetch through.
  final TagsyRepository repository;

  /// Invoked on tap; the home screen wires this to open the file detail.
  final VoidCallback onActivate;

  /// Invoked with a tag id when one of the file's tag chips is tapped; the home
  /// screen wires this to open the tag detail. Also what makes the chips render
  /// enabled (full-saturation), matching the detail screen's tappable chips.
  final ValueChanged<String> onOpenTag;

  @override
  Widget build(BuildContext context) {
    // See [TagRow] for why we strike deleted rows through. The full logical
    // path fits comfortably in a full-width tile, so show it whole here.
    final titleStyle = file.deleted
        ? const TextStyle(decoration: TextDecoration.lineThrough)
        : null;
    final theme = Theme.of(context);
    return Card(
      clipBehavior: Clip.antiAlias,
      margin: EdgeInsets.zero,
      child: InkWell(
        onTap: onActivate,
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            // A tall, full-width preview. Fixed height keeps every tile's
            // proportions consistent regardless of the thumbnail's aspect.
            SizedBox(
              height: 220,
              child: RemotePreview(
                repository: repository,
                fileId: file.fileId,
                contentHash: file.contentHash,
              ),
            ),
            Padding(
              padding: const EdgeInsets.fromLTRB(12, 10, 12, 12),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    file.path,
                    maxLines: 2,
                    overflow: TextOverflow.ellipsis,
                    style: (theme.textTheme.titleMedium ?? const TextStyle())
                        .merge(titleStyle),
                  ),
                  const SizedBox(height: 8),
                  FileTagStrip(
                    repository: repository,
                    fileId: file.fileId,
                    contentHash: file.contentHash,
                    onOpenTag: onOpenTag,
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}

/// Fetches and renders a file's applied tags as a wrap of [TagChip]s.
///
/// Self-manages the per-file lookup ([TagsyRepository.tagsForFile]), keyed by
/// `fileId` + `contentHash` so navigating between files (or a content change)
/// re-fetches rather than showing a stale set. While loading it reserves a
/// small blank strip so the tile doesn't jump; on error or no tags it renders
/// nothing. Tapping a chip opens that tag ([onOpenTag]); the chips carry no
/// remove affordance (tag editing lives on the file detail screen), but being
/// tappable also keeps them rendered enabled — matching the detail screen's
/// saturation rather than the greyed-out disabled look.
class FileTagStrip extends StatefulWidget {
  const FileTagStrip({
    super.key,
    required this.repository,
    required this.fileId,
    required this.contentHash,
    required this.onOpenTag,
  });

  final TagsyRepository repository;
  final String fileId;

  /// Part of the fetch identity (see class doc); not sent to the API.
  final String contentHash;

  /// Invoked with a tag id when its chip is tapped.
  final ValueChanged<String> onOpenTag;

  @override
  State<FileTagStrip> createState() => _FileTagStripState();
}

class _FileTagStripState extends State<FileTagStrip> {
  late Future<List<tagsy.TagEntry>> _future;

  /// Guards the change-stream loop so it stops when the widget is disposed.
  bool _watching = false;

  @override
  void initState() {
    super.initState();
    _future = widget.repository.tagsForFile(fileId: widget.fileId);
    _subscribeToChanges();
  }

  @override
  void didUpdateWidget(FileTagStrip oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.fileId != widget.fileId ||
        oldWidget.contentHash != widget.contentHash) {
      _reload();
    }
  }

  @override
  void dispose() {
    _watching = false;
    super.dispose();
  }

  /// Re-fetch this file's tags and rebuild. Used both on a fileId/hash change
  /// and when the change stream reports a relevant mutation.
  void _reload() {
    setState(() {
      _future = widget.repository.tagsForFile(fileId: widget.fileId);
    });
  }

  /// Subscribe to the live change stream and re-fetch when a mutation could
  /// alter this file's tags. Without this the strip would only ever show the
  /// tags present when the tile was first built (tagging a file doesn't change
  /// its content hash, so `didUpdateWidget` never re-fetches on its own).
  Future<void> _subscribeToChanges() async {
    _watching = true;
    try {
      final events = await widget.repository.subscribe();
      while (mounted && _watching) {
        final event = await events.next();
        if (event == null) break;
        if (!mounted || !_watching) break;
        if (_affectsThisFile(event)) _reload();
      }
    } catch (_) {
      // Transient stream hiccups are surfaced elsewhere; don't let one kill
      // the tile.
    }
  }

  /// Whether a change-stream event can alter this file's displayed tags.
  /// Mirrors the file detail screen's filter:
  /// - `FileTagChanged`: reload only when it is *this* file (a tag added/removed
  ///   from it).
  /// - `TagChanged` / `TagTagChanged`: a tag's name/color/hierarchy changed;
  ///   reload unconditionally (an over-approximation — we don't track which
  ///   tags this file carries here).
  /// - `Resynced`: reload; intervening changes may have been missed.
  /// - `FileChanged` / `ProviderReleased`: never affect the tag set.
  bool _affectsThisFile(tagsy.ApiEventDto event) => switch (event) {
    tagsy.ApiEventDto_Resynced() => true,
    tagsy.ApiEventDto_FileTagChanged(:final fileId) => fileId == widget.fileId,
    tagsy.ApiEventDto_TagChanged() => true,
    tagsy.ApiEventDto_TagTagChanged() => true,
    tagsy.ApiEventDto_FileChanged() => false,
    tagsy.ApiEventDto_ProviderReleased() => false,
  };

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<List<tagsy.TagEntry>>(
      future: _future,
      builder: (context, snapshot) {
        if (snapshot.connectionState != ConnectionState.done) {
          // Reserve a little height so the tile doesn't jump when tags land.
          return const SizedBox(height: 24);
        }
        // A tag lookup failure is not worth surfacing on a result tile; just
        // show nothing (the detail screen is authoritative for tags).
        final tags = snapshot.data ?? const <tagsy.TagEntry>[];
        if (tags.isEmpty) return const SizedBox.shrink();
        return Wrap(
          spacing: 6,
          runSpacing: 6,
          children: [
            for (final tag in tags)
              TagChip(
                tag: tag,
                onPressed: () => widget.onOpenTag(tag.tagId),
              ),
          ],
        );
      },
    );
  }
}
