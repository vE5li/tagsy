// Per-tag detail screen: shows every property of a single tag (id, name,
// color swatch), the tag's parent tags (tags applied to this tag), and its
// subtags (children). Tap the Name or Color row to edit; each of the two tag
// sections has an Add button and per-chip remove; the AppBar action deletes
// the tag itself. Live-updates on the change stream so rename / recolor /
// external deletions land immediately (the screen pops itself if the tag
// disappears underneath it).
//
// The screen is keyed by [tagId] rather than by a captured [TagEntry] so it
// always reflects the current state of the store on rebuild.

import 'dart:async';

import 'package:flutter/material.dart';

import '../data/repository.dart';
import '../rust/api.dart' as tagsy;
import '../session/session.dart';
import '../widgets/busy_icon_button.dart';
import '../widgets/property_tile.dart';
import '../widgets/tag_chip.dart';
import '../widgets/tag_picker_sheet.dart';
import '../widgets/tags_section.dart';
import '../widgets/text_prompt_dialog.dart';

class TagDetailScreen extends StatefulWidget {
  const TagDetailScreen({
    super.key,
    required this.session,
    required this.tagId,
  });

  final TagsySession session;
  final String tagId;

  @override
  State<TagDetailScreen> createState() => _TagDetailScreenState();
}

class _TagDetailScreenState extends State<TagDetailScreen> {
  tagsy.TagEntry? _tag;

  /// Direct parent tags (tags applied to this tag). String ids for the wire,
  /// resolved to entries in [_relatedTags] for rendering.
  List<String> _parentTagIds = [];

  /// Direct subtags (children of this tag).
  List<String> _subtagIds = [];

  /// Name/color lookup for every tag id that appears in either section
  /// above. Bounded by parents + subtags — never a whole-store listing.
  Map<String, tagsy.TagEntry> _relatedTags = {};

  bool _loading = true;
  String? _error;
  bool _deleted = false;
  bool _watching = false;
  bool _restoring = false;

  TagsyRepository get _repository => widget.session.repository;

  @override
  void initState() {
    super.initState();
    _load();
    _subscribeToChanges();
  }

  @override
  void dispose() {
    _watching = false;
    super.dispose();
  }

  Future<void> _subscribeToChanges() async {
    _watching = true;
    try {
      final events = await _repository.subscribe();
      while (mounted && _watching) {
        final event = await events.next();
        if (event == null) break;
        if (!mounted) break;
        if (_affectsThisTag(event)) await _load();
      }
    } catch (_) {
      // See HomeScreen._subscribeToChanges: intentionally swallowed here.
    }
  }

  /// Whether a change-stream event can alter what this screen shows: the tag
  /// itself, or its parent/subtag hierarchy. This view renders no files, so
  /// file-only and file-tag events never matter.
  ///
  /// - `Resynced`: reload; intervening changes may have been missed.
  /// - `TagChanged` / `TagTagChanged`: reload unconditionally. The view shows
  ///   related tags (parents, subtags) by row, and a hierarchy edge can touch
  ///   any tag, so rather than track the related-id set we over-approximate
  ///   (tag mutations are rare).
  /// - `FileChanged` / `FileTagChanged` / `ProviderReleased`: never relevant.
  bool _affectsThisTag(tagsy.ApiEventDto event) => switch (event) {
    tagsy.ApiEventDto_Resynced() => true,
    tagsy.ApiEventDto_TagChanged() => true,
    tagsy.ApiEventDto_TagTagChanged() => true,
    tagsy.ApiEventDto_FileChanged() => false,
    tagsy.ApiEventDto_FileTagChanged() => false,
    tagsy.ApiEventDto_ProviderReleased() => false,
  };

  Future<void> _load() async {
    try {
      // Direct parents/subtags only (Exclude = no hierarchy walk). Matches the
      // file detail, which also shows direct membership.
      //
      // For the tag itself we pass `Include` so a tombstoned tag opened from
      // the home screen's "show deleted" toggle still loads (with its
      // `deleted` flag set). Parents/subtags are always live-only — a
      // tombstoned tag can't participate in a live hierarchy edge.
      final tag = await _repository.getTagEntry(
        tagId: widget.tagId,
        deletedRule: tagsy.DeletedRule.include,
      );
      final parents = await _repository.tagIdsForTag(
        tagId: widget.tagId,
        subtagRule: tagsy.SubtagRule.exclude,
      );
      final subtags = await _repository.subtagIdsForTag(
        tagId: widget.tagId,
        subtagRule: tagsy.SubtagRule.exclude,
      );
      // Resolve every related tag by id. Bounded by parents.length +
      // subtags.length; avoids the whole-store listing that `runQuery('')`
      // would do.
      final relatedIds = {...parents, ...subtags};
      final relatedEntries = await Future.wait(
        relatedIds.map(
          (id) => _repository.getTagEntry(
            tagId: id,
            deletedRule: tagsy.DeletedRule.exclude,
          ),
        ),
      );
      if (!mounted) return;
      setState(() {
        _tag = tag;
        _parentTagIds = parents;
        _subtagIds = subtags;
        _relatedTags = {for (final t in relatedEntries) t.tagId: t};
        _loading = false;
        _error = null;
      });
    } catch (error) {
      if (!mounted) return;
      // `getTagEntry` rejects with `UnknownId` when the tag is gone; treat
      // that as "deleted underneath us" and pop back to the list. Other errors
      // (transport, etc.) surface normally.
      final isMissing = error is tagsy.ApiError_UnknownId;
      setState(() {
        if (isMissing) {
          _tag = null;
          _error = null;
          if (!_deleted) {
            _deleted = true;
            WidgetsBinding.instance.addPostFrameCallback((_) {
              if (mounted) Navigator.of(context).maybePop();
            });
          }
        } else {
          _error = '$error';
        }
        _loading = false;
      });
    }
  }

  Future<void> _renameTag() async {
    final tag = _tag;
    if (tag == null) return;
    final result = await showDialog<String>(
      context: context,
      builder: (_) => TextPromptDialog(
        title: 'Rename tag',
        label: 'Name',
        initial: tag.name,
      ),
    );
    if (result == null) return;
    final trimmed = result.trim();
    if (trimmed.isEmpty || trimmed == tag.name) return;
    try {
      await _repository.renameTag(tagId: tag.tagId, name: trimmed);
      // Live update flows in via the change stream.
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('Failed to rename tag: $error')));
    }
  }

  Future<void> _recolorTag() async {
    final tag = _tag;
    if (tag == null) return;
    final result = await showDialog<String>(
      context: context,
      builder: (_) => _RecolorTagDialog(initial: tag.color),
    );
    if (result == null || result == tag.color) return;
    try {
      await _repository.setTagColor(tagId: tag.tagId, color: result);
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('Failed to change color: $error')));
    }
  }

  Future<void> _addParent() async {
    final chosen = await TagPickerSheet.show(
      context: context,
      repository: _repository,
      title: 'Add tag',
      // Exclude self (a tag can't be its own parent) and existing parents.
      excludeIds: {widget.tagId, ..._parentTagIds},
    );
    if (chosen == null) return;
    try {
      // The chosen tag becomes a parent of this tag: parent = chosen, subtag = this.
      await _repository.tagTag(parentId: chosen.tagId, subtagId: widget.tagId);
      // The change stream drives _load().
    } catch (error) {
      _snack('Failed to add parent: $error');
    }
  }

  Future<void> _addSubtag() async {
    final chosen = await TagPickerSheet.show(
      context: context,
      repository: _repository,
      title: 'Add subtag',
      // Exclude self (a tag can't be its own subtag) and existing subtags.
      excludeIds: {widget.tagId, ..._subtagIds},
    );
    if (chosen == null) return;
    try {
      // The chosen tag becomes a subtag of this tag: parent = this, subtag = chosen.
      await _repository.tagTag(parentId: widget.tagId, subtagId: chosen.tagId);
    } catch (error) {
      _snack('Failed to add subtag: $error');
    }
  }

  Future<void> _removeParent(String parentId) async {
    try {
      await _repository.untagTag(parentId: parentId, subtagId: widget.tagId);
    } catch (error) {
      _snack('Failed to remove parent: $error');
    }
  }

  /// Push another [TagDetailScreen] for the given related tag. Used by the
  /// chips in the Tags / Subtags sections so the user can navigate the tag
  /// hierarchy without going back to search each time.
  void _openTag(String tagId) {
    Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) => TagDetailScreen(session: widget.session, tagId: tagId),
      ),
    );
  }

  Future<void> _removeSubtag(String subtagId) async {
    try {
      await _repository.untagTag(parentId: widget.tagId, subtagId: subtagId);
    } catch (error) {
      _snack('Failed to remove subtag: $error');
    }
  }

  void _snack(String message) {
    if (!mounted) return;
    ScaffoldMessenger.of(
      context,
    ).showSnackBar(SnackBar(content: Text(message)));
  }

  Future<void> _deleteTag() async {
    final tag = _tag;
    if (tag == null) return;
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (_) => AlertDialog(
        title: const Text('Confirmation'),
        content: Text('Delete "${tag.name}"?'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context, false),
            child: const Text('Cancel'),
          ),
          TextButton(
            onPressed: () => Navigator.pop(context, true),
            child: const Text('Delete'),
          ),
        ],
      ),
    );
    if (confirmed != true) return;
    try {
      _deleted = true;
      await _repository.deleteTag(tagId: tag.tagId);
      if (!mounted) return;
      Navigator.of(context).maybePop();
    } catch (error) {
      _deleted = false;
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('Failed to delete tag: $error')));
    }
  }

  /// Restore a soft-deleted tag. Unlike a file, a tag carries no content, so
  /// this always succeeds for a known tag (it re-announces the definition with
  /// a fresh timestamp, winning last-writer-wins over the delete). On success
  /// we reload so the view re-renders as live.
  Future<void> _restoreTag() async {
    final tag = _tag;
    if (tag == null) return;
    setState(() => _restoring = true);
    try {
      await _repository.restoreTag(tagId: tag.tagId);
      if (!mounted) return;
      _deleted = false;
      _snack('Restored "${tag.name}".');
      await _load();
    } catch (error) {
      if (!mounted) return;
      _snack('Failed to restore tag: $error');
    } finally {
      if (mounted) setState(() => _restoring = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    final tag = _tag;
    // See FileDetailScreen.build for the strikethrough / restore-swap
    // rationale; the two detail screens keep matching tombstone treatments.
    final titleStyle = tag?.deleted == true
        ? const TextStyle(decoration: TextDecoration.lineThrough)
        : null;
    return Scaffold(
      appBar: AppBar(
        title: Text(tag?.name ?? 'Tag', style: titleStyle),
        actions: [
          if (tag != null)
            (tag.deleted
                ? BusyIconButton(
                    busy: _restoring,
                    icon: Icons.restore_from_trash,
                    tooltip: 'Restore tag',
                    onPressed: _restoreTag,
                  )
                : IconButton(
                    icon: const Icon(Icons.delete_outline),
                    tooltip: 'Delete tag',
                    onPressed: _deleteTag,
                  )),
        ],
      ),
      body: _buildBody(),
    );
  }

  Widget _buildBody() {
    if (_loading) return const Center(child: CircularProgressIndicator());
    if (_error != null) return Center(child: Text('Error: $_error'));
    final tag = _tag;
    if (tag == null) {
      // Post-frame pop is queued; render a neutral state in the meantime.
      return const SizedBox.shrink();
    }
    return ListView(
      padding: const EdgeInsets.symmetric(vertical: 8),
      children: [
        PropertyTile(
          label: 'Name',
          value: tag.name,
          trailing: const Icon(Icons.edit_outlined, size: 20),
          onTap: _renameTag,
        ),
        PropertyTile(
          label: 'Color',
          value: tag.color,
          trailing: TagColorSwatch(color: tag.color),
          onTap: _recolorTag,
          monospace: true,
        ),
        const SizedBox(height: 12),
        TagsSection(
          title: 'Tags',
          tagIds: _parentTagIds,
          resolved: _relatedTags,
          onAdd: _addParent,
          onRemove: _removeParent,
          onTapTag: _openTag,
          emptyLabel: 'No tags.',
        ),
        const SizedBox(height: 16),
        TagsSection(
          title: 'Subtags',
          tagIds: _subtagIds,
          resolved: _relatedTags,
          onAdd: _addSubtag,
          onRemove: _removeSubtag,
          onTapTag: _openTag,
          emptyLabel: 'No subtags.',
        ),
        const SizedBox(height: 24),
        PropertyTile(
          label: 'Tag id',
          value: tag.tagId,
          monospace: true,
          dense: true,
        ),
      ],
    );
  }
}

/// Lets the user pick a new color from [kTagColorPalette]. Pops the chosen
/// `#RRGGBB` string, or `null` on cancel.
class _RecolorTagDialog extends StatefulWidget {
  const _RecolorTagDialog({required this.initial});

  final String initial;

  @override
  State<_RecolorTagDialog> createState() => _RecolorTagDialogState();
}

class _RecolorTagDialogState extends State<_RecolorTagDialog> {
  late final TextEditingController _controller = TextEditingController(
    text: widget.initial,
  );

  @override
  void initState() {
    super.initState();
    // Rebuild on every keystroke so the preview swatch, preset selection ring,
    // and Save-button enablement all track the live text value.
    _controller.addListener(() => setState(() {}));
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  /// Returns the normalized `#RRGGBB[AA]` form of the current input, or `null`
  /// if it doesn't parse. Accepts input with or without a leading `#` and
  /// treats it case-insensitively.
  String? get _normalized {
    var text = _controller.text.trim();
    if (text.startsWith('#')) text = text.substring(1);
    if (text.length != 6 && text.length != 8) return null;
    if (int.tryParse(text, radix: 16) == null) return null;
    return '#${text.toUpperCase()}';
  }

  @override
  Widget build(BuildContext context) {
    final normalized = _normalized;
    return AlertDialog(
      title: const Text('Tag color'),
      content: Column(
        mainAxisSize: MainAxisSize.min,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Expanded(
                child: TextField(
                  controller: _controller,
                  autofocus: true,
                  decoration: InputDecoration(
                    labelText: 'Hex color',
                    hintText: '#RRGGBB',
                    errorText: normalized == null && _controller.text.isNotEmpty
                        ? 'Expected #RRGGBB or #RRGGBBAA'
                        : null,
                  ),
                  onSubmitted: (_) {
                    if (normalized != null) Navigator.pop(context, normalized);
                  },
                ),
              ),
              const SizedBox(width: 12),
              // Live preview of whatever is currently typed. Falls back to grey
              // via [parseTagColor] when the input is invalid.
              TagColorSwatch(color: normalized ?? _controller.text),
            ],
          ),
          const SizedBox(height: 16),
          const Text('Presets'),
          const SizedBox(height: 8),
          Wrap(
            spacing: 8,
            runSpacing: 8,
            children: [
              for (final color in kTagColorPalette)
                GestureDetector(
                  onTap: () {
                    _controller.text = color;
                    _controller.selection = TextSelection.collapsed(
                      offset: color.length,
                    );
                  },
                  child: TagColorSwatch(
                    color: color,
                    selected:
                        normalized != null && color.toUpperCase() == normalized,
                  ),
                ),
            ],
          ),
        ],
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.pop(context),
          child: const Text('Cancel'),
        ),
        TextButton(
          onPressed: normalized == null
              ? null
              : () => Navigator.pop(context, normalized),
          child: const Text('Save'),
        ),
      ],
    );
  }
}
