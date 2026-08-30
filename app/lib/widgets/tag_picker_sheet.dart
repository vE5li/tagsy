// A modal bottom sheet for picking an existing tag or creating a new one inline.
//
// Replaces three copies of the same "scan the whole tag store, filter, show a
// ListView of tags in a sheet" flow — the file-detail "Add tag", the tag-detail
// parent/subtag picker, and the share-review tag picker — each of which carried
// the same whole-store-scan TODO. Inline tag creation (once unique to the
// share-review copy) is offered in every picker.

import 'package:flutter/material.dart';

import '../data/repository.dart';
import '../rust/api.dart' as tagsy;
import 'tag_chip.dart';
import 'text_prompt_dialog.dart';

/// A tag-picker modal bottom sheet.
///
/// Use [TagPickerSheet.show]: it lists every *live* tag (minus [excludeIds]),
/// lets the user tap one, and returns it — or `null` if the sheet was
/// dismissed. A "Create new tag" row prompts for a name and creates the tag
/// through the [TagsyRepository], returning the fresh entry. The tags most
/// recently chosen through the picker this session are surfaced as a "Recent"
/// quick-pick section at the top (in-memory only, reset on app restart).
class TagPickerSheet {
  const TagPickerSheet._();

  /// Ids of the tags most recently chosen (or created) through the picker,
  /// most-recent first. In-memory only — deliberately not persisted; it's a
  /// convenience for the current session, reset on app restart. Capped at
  /// [_recentCapacity]; surfaced as a "Recent" section at the top of the sheet.
  static final List<String> _recentIds = [];
  static const int _recentCapacity = 10;

  /// Record `tagId` as the most-recently-used tag, moving it to the front and
  /// dropping the oldest once over capacity. Called whenever the picker yields
  /// a tag (an existing pick or a fresh creation).
  static void _remember(String tagId) {
    _recentIds
      ..remove(tagId)
      ..insert(0, tagId);
    if (_recentIds.length > _recentCapacity) {
      _recentIds.removeRange(_recentCapacity, _recentIds.length);
    }
  }

  /// Show the picker and await the chosen (or newly-created) tag, or `null` on
  /// dismiss.
  ///
  /// [title] labels the sheet (e.g. `Add tag`, `Add subtag`). [excludeIds] are
  /// hidden from the list — typically the ids already applied/selected, plus
  /// (for the tag hierarchy) the subject tag itself. The sheet always offers
  /// inline tag creation via a [TextPromptDialog], and a search field filters
  /// the list by name.
  ///
  /// The whole-tag listing is run lazily here (on show), not on the calling
  /// screen's open; the search field then filters that in-memory list by
  /// case-insensitive name substring — no re-query per keystroke.
  static Future<tagsy.TagEntry?> show({
    required BuildContext context,
    required TagsyRepository repository,
    required String title,
    Set<String> excludeIds = const {},
  }) async {
    final tagsy.QueryEntries all;
    try {
      // Tag pickers only surface live tags — you can't apply a tombstoned tag
      // to anything.
      all = await repository.runQuery(
        query: '',
        subtagRule: tagsy.SubtagRule.include,
        deletedRule: tagsy.DeletedRule.exclude,
      );
    } catch (error) {
      if (context.mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(SnackBar(content: Text('Failed to load tags: $error')));
      }
      return null;
    }
    if (!context.mounted) return null;

    final available = all.tags
        .where((t) => !excludeIds.contains(t.tagId))
        .toList();

    // The recent tags to surface at the top, resolved against `available` (so
    // they inherit the same exclude/live filtering) and kept in most-recent
    // order. A remembered tag that's been deleted or is already applied simply
    // won't be found here — no stale rows.
    final byId = {for (final t in available) t.tagId: t};
    final recent = [for (final id in _recentIds) ?byId[id]];

    final chosen = await showModalBottomSheet<tagsy.TagEntry>(
      context: context,
      // The search field grows the sheet; let it use the full height and sit
      // above the keyboard rather than being clipped by it.
      isScrollControlled: true,
      builder: (sheetContext) => _TagPickerSheetBody(
        title: title,
        repository: repository,
        available: available,
        recent: recent,
      ),
    );
    if (chosen != null) _remember(chosen.tagId);
    return chosen;
  }

  /// Prompt for a tag name, create it with the default style, and return the
  /// fresh [tagsy.TagEntry] (or `null` on cancel / failure). The tag can be
  /// restyled afterward from its detail screen.
  static Future<tagsy.TagEntry?> _createTag(
    BuildContext context,
    TagsyRepository repository,
  ) async {
    final name = await showDialog<String>(
      context: context,
      builder: (_) => const TextPromptDialog(
        title: 'Create tag',
        label: 'Tag name',
        confirmLabel: 'Create',
      ),
    );
    final trimmed = name?.trim();
    if (trimmed == null || trimmed.isEmpty) return null;
    try {
      final tagId = await repository.createTag(
        name: trimmed,
        style: defaultTagStyle(),
      );
      return await repository.getTagEntry(
        tagId: tagId,
        deletedRule: tagsy.DeletedRule.exclude,
      );
    } catch (error) {
      if (context.mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(SnackBar(content: Text('Failed to create tag: $error')));
      }
      return null;
    }
  }
}

/// The sheet's stateful body: a search field over the pre-fetched [available]
/// tags, an optional "Recent" quick-pick section ([recent]), and the "Create
/// new tag" row. Filtering is a plain case-insensitive substring match on the
/// tag name, done in memory over the already-loaded lists — no re-query per
/// keystroke.
class _TagPickerSheetBody extends StatefulWidget {
  const _TagPickerSheetBody({
    required this.title,
    required this.repository,
    required this.available,
    required this.recent,
  });

  final String title;
  final TagsyRepository repository;
  final List<tagsy.TagEntry> available;

  /// The recently-used tags to surface at the top, most-recent first. Already
  /// resolved against [available] by the caller, so every entry here is also a
  /// selectable row; the main list below excludes them to avoid duplication.
  final List<tagsy.TagEntry> recent;

  @override
  State<_TagPickerSheetBody> createState() => _TagPickerSheetBodyState();
}

class _TagPickerSheetBodyState extends State<_TagPickerSheetBody> {
  final TextEditingController _search = TextEditingController();
  String _query = '';

  @override
  void dispose() {
    _search.dispose();
    super.dispose();
  }

  /// Case-insensitive name-substring filter used for both the recent and main
  /// lists.
  List<tagsy.TagEntry> _matching(Iterable<tagsy.TagEntry> tags) {
    final needle = _query.trim().toLowerCase();
    if (needle.isEmpty) return tags.toList();
    return tags
        .where((t) => t.name.toLowerCase().contains(needle))
        .toList();
  }

  /// A selectable tag row: color swatch + name, popping the sheet with the tag.
  Widget _tagTile(tagsy.TagEntry tag) => ListTile(
    visualDensity: VisualDensity.compact,
    leading: TagColorSwatch(color: tag.style.dotColor),
    title: Text(tag.name),
    onTap: () => Navigator.pop(context, tag),
  );

  @override
  Widget build(BuildContext context) {
    final recent = _matching(widget.recent);
    final recentIds = {for (final t in recent) t.tagId};
    // The main list excludes anything already shown under "Recent" so a tag
    // never appears twice.
    final matches = _matching(
      widget.available.where((t) => !recentIds.contains(t.tagId)),
    );
    return SafeArea(
      child: Padding(
        // Lift the sheet above the keyboard so the search field stays visible.
        padding: EdgeInsets.only(
          bottom: MediaQuery.of(context).viewInsets.bottom,
        ),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            ListTile(
              visualDensity: VisualDensity.compact,
              title: Text(
                widget.title,
                style: const TextStyle(fontWeight: FontWeight.bold),
              ),
            ),
            Padding(
              padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
              child: TextField(
                controller: _search,
                decoration: const InputDecoration(
                  prefixIcon: Icon(Icons.search),
                  hintText: 'Search tags',
                  border: OutlineInputBorder(),
                  isDense: true,
                ),
                onChanged: (value) => setState(() => _query = value),
              ),
            ),
            Flexible(
              child: ListView(
                shrinkWrap: true,
                children: [
                  ListTile(
                    leading: const Icon(Icons.add),
                    title: const Text('Create new tag'),
                    visualDensity: VisualDensity.compact,
                    onTap: () async {
                      // Capture the sheet's navigator before the async gap so
                      // we can dismiss it (returning the created tag) once
                      // creation resolves. Only pop on an actual creation —
                      // cancelling (or a failure) leaves the picker open.
                      final navigator = Navigator.of(context);
                      final created = await TagPickerSheet._createTag(
                        context,
                        widget.repository,
                      );
                      if (created != null) navigator.pop(created);
                    },
                  ),
                  if (recent.isNotEmpty) ...[
                    const _SectionLabel('Recent'),
                    for (final tag in recent) _tagTile(tag),
                    const _SectionLabel('All tags'),
                  ],
                  if (matches.isEmpty)
                    ListTile(
                      visualDensity: VisualDensity.compact,
                      title: Text(
                        _query.trim().isEmpty
                            ? 'No more tags to add.'
                            : 'No tags match “${_query.trim()}”.',
                      ),
                    )
                  else
                    for (final tag in matches) _tagTile(tag),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}

/// A small, muted group label separating the sheet's "Recent" and "All tags"
/// sections. Kept lightweight (not the app-wide SectionHeader) to match the
/// picker's dense list.
class _SectionLabel extends StatelessWidget {
  const _SectionLabel(this.text);

  final String text;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Padding(
      padding: const EdgeInsets.fromLTRB(16, 12, 16, 4),
      child: Text(
        text,
        style: theme.textTheme.labelSmall?.copyWith(
          color: theme.colorScheme.onSurfaceVariant,
          fontWeight: FontWeight.w600,
        ),
      ),
    );
  }
}
