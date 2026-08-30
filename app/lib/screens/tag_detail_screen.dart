// Per-tag detail screen: shows every property of a single tag (id, name, and
// its full visual style, edited inline), the tag's parent tags (tags applied to
// this tag), and its subtags (children). Tap the Name row to rename; the inline
// style editor persists each change immediately; each of the two tag sections
// has an Add button and per-chip remove; the AppBar action deletes the tag
// itself. Live-updates on the change stream so rename / restyle / external
// deletions land immediately (the screen pops itself if the tag disappears
// underneath it).
//
// The screen is keyed by [tagId] rather than by a captured [TagEntry] so it
// always reflects the current state of the store on rebuild.

import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter_colorpicker/flutter_colorpicker.dart';

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

  Future<void> _applyStyle(tagsy.TagStyleEntry style) async {
    final tag = _tag;
    if (tag == null) return;
    try {
      await _repository.setTagStyle(tagId: tag.tagId, style: style);
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('Failed to change style: $error')));
    }
  }

  /// A value key summarizing a style, used to reseat the inline editor's text
  /// controllers only when the style actually changes (a rebuild that leaves
  /// the style unchanged — e.g. a subtag edit — keeps the same key so the
  /// editor's fields don't lose focus mid-typing).
  String _styleKey(tagsy.TagStyleEntry s) =>
      '${s.dotColor}|${s.background}|${s.gradient}|${s.foreground}|${s.border}|'
      '${s.borderWidth}|${s.borderStyle}|${s.shape}|${s.shadow}|${s.shadowColor}';

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
        Padding(
          padding: const EdgeInsets.fromLTRB(16, 8, 16, 8),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                children: [
                  Text('Style', style: Theme.of(context).textTheme.titleMedium),
                  const Spacer(),
                  TagChip(tag: tag),
                ],
              ),
              const SizedBox(height: 8),
              // Keyed by the tag's current style so the field controllers
              // reseat if the style changes underneath us (e.g. a peer edit).
              _StyleEditor(
                key: ValueKey(_styleKey(tag.style)),
                style: tag.style,
                tagName: tag.name,
                onChanged: _applyStyle,
              ),
            ],
          ),
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

/// An inline tag-style editor. Edits every one of the ten style properties with
/// equal weight and persists each change immediately via [onChanged] — there is
/// no Save/Cancel step; the tag detail screen owns the persisted style and
/// rebuilds from the store.
///
/// Color fields persist when the user submits or leaves the field (not on every
/// keystroke) so a half-typed hex doesn't spam the daemon; the slider, dropdowns
/// and switches persist immediately.
class _StyleEditor extends StatefulWidget {
  const _StyleEditor({
    super.key,
    required this.style,
    required this.tagName,
    required this.onChanged,
  });

  final tagsy.TagStyleEntry style;

  /// The tag's name, shown in the color picker's live preview chip.
  final String tagName;

  final ValueChanged<tagsy.TagStyleEntry> onChanged;

  @override
  State<_StyleEditor> createState() => _StyleEditorState();
}

class _StyleEditorState extends State<_StyleEditor> {
  late final _dotColor = TextEditingController(text: widget.style.dotColor);
  late final _background = TextEditingController(text: widget.style.background);
  late final _gradient = TextEditingController(text: widget.style.gradient);
  late final _foreground = TextEditingController(text: widget.style.foreground);
  late final _border = TextEditingController(text: widget.style.border);
  late final _shadowColor =
      TextEditingController(text: widget.style.shadowColor);

  @override
  void dispose() {
    _dotColor.dispose();
    _background.dispose();
    _gradient.dispose();
    _foreground.dispose();
    _border.dispose();
    _shadowColor.dispose();
    super.dispose();
  }

  /// The style as currently shown in the editor: the color fields' live
  /// (normalized) text plus the enum/slider/switch state from [widget.style].
  /// This is the single source the preview and [_emit] both build on.
  tagsy.TagStyleEntry _liveStyle() => tagsy.TagStyleEntry(
    dotColor: _normalizeColor(_dotColor.text),
    background: _normalizeColor(_background.text),
    gradient: _normalizeColor(_gradient.text),
    foreground: _normalizeColor(_foreground.text),
    border: _normalizeColor(_border.text),
    borderWidth: widget.style.borderWidth,
    borderStyle: widget.style.borderStyle,
    shape: widget.style.shape,
    shadow: widget.style.shadow,
    shadowColor: _normalizeColor(_shadowColor.text),
  );

  /// Emit a style with `mutate` applied to the current live style, so a submit
  /// picks up whatever is typed alongside a same-instant enum/slider change.
  void _emit(tagsy.TagStyleEntry Function(tagsy.TagStyleEntry base) mutate) {
    widget.onChanged(mutate(_liveStyle()));
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        _colorField('Dot color', _dotColor,
            (base, hex) => _copyColor(base, dotColor: hex)),
        _colorField('Background', _background,
            (base, hex) => _copyColor(base, background: hex)),
        _colorField('Gradient (fades from background)', _gradient,
            (base, hex) => _copyColor(base, gradient: hex)),
        _colorField('Text', _foreground,
            (base, hex) => _copyColor(base, foreground: hex)),
        _colorField('Border', _border,
            (base, hex) => _copyColor(base, border: hex)),
        const SizedBox(height: 8),
        Row(
          children: [
            const Text('Border width'),
            Expanded(
              child: Slider(
                value: widget.style.borderWidth.clamp(0, 8),
                min: 0,
                max: 8,
                divisions: 16,
                label: widget.style.borderWidth.toStringAsFixed(1),
                onChanged: (v) => _emit((b) => _copyWith(b, borderWidth: v)),
              ),
            ),
          ],
        ),
        _enumField('Border style', kTagBorderStyles, widget.style.borderStyle,
            (v) => _emit((b) => _copyWith(b, borderStyle: v))),
        _enumField('Shape', kTagShapes, widget.style.shape,
            (v) => _emit((b) => _copyWith(b, shape: v))),
        SwitchListTile(
          contentPadding: EdgeInsets.zero,
          title: const Text('Shadow'),
          value: widget.style.shadow,
          onChanged: (v) => _emit((b) => _copyWith(b, shadow: v)),
        ),
        _colorField('Shadow color', _shadowColor,
            (base, hex) => _copyColor(base, shadowColor: hex)),
      ],
    );
  }

  /// A row: label, a hex text field, and a live swatch that opens a full color
  /// picker (wheel + alpha + hex). The hex field remains the authoritative
  /// manual-entry path; the picker is a convenience that writes back into it.
  /// Persists on submit or focus-loss rather than per keystroke.
  ///
  /// `withColor(base, hex)` places a chosen color into *this* field's property
  /// of a style, so the picker can preview the whole tag with just this color
  /// changed.
  Widget _colorField(
    String label,
    TextEditingController controller,
    tagsy.TagStyleEntry Function(tagsy.TagStyleEntry base, String hex) withColor,
  ) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 6),
      child: Focus(
        onFocusChange: (hasFocus) {
          if (!hasFocus) _emit((b) => b);
        },
        child: Row(
          children: [
            Expanded(
              child: TextField(
                controller: controller,
                decoration: InputDecoration(
                  labelText: label,
                  hintText: '#AARRGGBB or #RRGGBB',
                  isDense: true,
                ),
                onSubmitted: (_) => _emit((b) => b),
                // Rebuild so the swatch tracks the live text.
                onChanged: (_) => setState(() {}),
              ),
            ),
            const SizedBox(width: 4),
            // A generously-sized tap target around the swatch: the swatch stays
            // a compact 28px, but the InkWell fills a ~48px box (Material's
            // minimum touch target) so it's easy to hit. The trailing edit icon
            // makes the tap affordance obvious.
            Tooltip(
              message: 'Open color picker',
              child: InkWell(
                onTap: () => _pickColor(label, controller, withColor),
                borderRadius: BorderRadius.circular(8),
                child: SizedBox(
                  height: 48,
                  child: Padding(
                    padding: const EdgeInsets.symmetric(horizontal: 8),
                    child: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        TagColorSwatch(color: controller.text),
                        const SizedBox(width: 4),
                        Icon(
                          Icons.edit_outlined,
                          size: 16,
                          color: Theme.of(context).colorScheme.outline,
                        ),
                      ],
                    ),
                  ),
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }

  /// Open a wheel+alpha+hex color picker seeded from the field's current value,
  /// showing a live preview of the whole tag (with only this color changed)
  /// above the wheel. On confirm, write the chosen color back as `#AARRGGBB`
  /// and persist. The dialog is the visual assist; the text field stays the
  /// source of truth.
  Future<void> _pickColor(
    String label,
    TextEditingController controller,
    tagsy.TagStyleEntry Function(tagsy.TagStyleEntry base, String hex) withColor,
  ) async {
    var picked = parseTagColor(controller.text);
    // Snapshot the rest of the style now so the preview reflects the other
    // fields as they currently stand while only this color tracks the wheel.
    final baseStyle = _liveStyle();

    final confirmed = await showDialog<bool>(
      context: context,
      builder: (context) => StatefulBuilder(
        builder: (context, setDialogState) {
          final previewStyle = withColor(baseStyle, formatTagColor(picked));
          final previewTag = tagsy.TagEntry(
            tagId: 'preview',
            name: widget.tagName,
            style: previewStyle,
            deleted: false,
          );
          return AlertDialog(
            title: Text(label),
            content: SingleChildScrollView(
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  // Live preview of the tag as it would look if accepted.
                  Padding(
                    padding: const EdgeInsets.only(bottom: 12),
                    child: TagChip(tag: previewTag),
                  ),
                  ColorPicker(
                    pickerColor: picked,
                    onColorChanged: (color) =>
                        setDialogState(() => picked = color),
                    paletteType: PaletteType.hueWheel,
                    enableAlpha: true,
                    hexInputBar: true,
                    portraitOnly: true,
                  ),
                ],
              ),
            ),
            actions: [
              TextButton(
                onPressed: () => Navigator.pop(context, false),
                child: const Text('Cancel'),
              ),
              FilledButton(
                onPressed: () => Navigator.pop(context, true),
                child: const Text('Select'),
              ),
            ],
          );
        },
      ),
    );
    if (confirmed != true) return;
    controller.text = formatTagColor(picked);
    setState(() {}); // refresh the swatch
    _emit((b) => b); // persist
  }

  /// A labeled dropdown over a fixed set of enum names.
  Widget _enumField(
    String label,
    List<String> options,
    String value,
    ValueChanged<String> onChanged,
  ) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 6),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(label),
          DropdownButton<String>(
            value: options.contains(value) ? value : options.first,
            items: [
              for (final option in options)
                DropdownMenuItem(value: option, child: Text(option)),
            ],
            onChanged: (v) {
              if (v != null) onChanged(v);
            },
          ),
        ],
      ),
    );
  }

  /// Normalize free-form input to `#AARRGGBB` (or `#RRGGBB`) when it parses,
  /// else pass it through so the field keeps the user's text (swatch grey).
  String _normalizeColor(String input) {
    var text = input.trim();
    if (text.startsWith('#')) text = text.substring(1);
    if ((text.length == 6 || text.length == 8) &&
        int.tryParse(text, radix: 16) != null) {
      return '#${text.toUpperCase()}';
    }
    return input;
  }
}

/// Copy a [tagsy.TagStyleEntry] overriding the non-color (enum/scalar/bool)
/// fields; the color fields are always taken from the editor's live text via
/// `_emit`'s base.
tagsy.TagStyleEntry _copyWith(
  tagsy.TagStyleEntry base, {
  double? borderWidth,
  String? borderStyle,
  String? shape,
  bool? shadow,
}) {
  return tagsy.TagStyleEntry(
    dotColor: base.dotColor,
    background: base.background,
    gradient: base.gradient,
    foreground: base.foreground,
    border: base.border,
    borderWidth: borderWidth ?? base.borderWidth,
    borderStyle: borderStyle ?? base.borderStyle,
    shape: shape ?? base.shape,
    shadow: shadow ?? base.shadow,
    shadowColor: base.shadowColor,
  );
}

/// Copy a [tagsy.TagStyleEntry] overriding exactly one color field, leaving the
/// rest (colors, enums, scalars, bools) intact. Used by the color picker's live
/// preview to show the whole tag with just this color changed.
tagsy.TagStyleEntry _copyColor(
  tagsy.TagStyleEntry base, {
  String? dotColor,
  String? background,
  String? gradient,
  String? foreground,
  String? border,
  String? shadowColor,
}) {
  return tagsy.TagStyleEntry(
    dotColor: dotColor ?? base.dotColor,
    background: background ?? base.background,
    gradient: gradient ?? base.gradient,
    foreground: foreground ?? base.foreground,
    border: border ?? base.border,
    borderWidth: base.borderWidth,
    borderStyle: base.borderStyle,
    shape: base.shape,
    shadow: base.shadow,
    shadowColor: shadowColor ?? base.shadowColor,
  );
}
