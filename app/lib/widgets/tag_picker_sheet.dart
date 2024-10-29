// A modal bottom sheet for picking an existing tag (optionally creating a new
// one inline).
//
// Replaces three copies of the same "scan the whole tag store, filter, show a
// ListView of tags in a sheet" flow — the file-detail "Add tag", the tag-detail
// parent/subtag picker, and the share-review tag picker — each of which carried
// the same whole-store-scan TODO. The share-review copy was the richest (it
// alone offered inline tag creation); this generalises it behind an
// [allowCreate] flag.

import 'package:flutter/material.dart';

import '../data/repository.dart';
import '../rust/api.dart' as tagsy;
import 'tag_chip.dart';
import 'text_prompt_dialog.dart';

/// A tag-picker modal bottom sheet.
///
/// Use [TagPickerSheet.show]: it lists every *live* tag (minus [excludeIds]),
/// lets the user tap one, and returns it — or `null` if the sheet was
/// dismissed. With [allowCreate] set, a "Create new tag" row prompts for a name
/// and creates the tag through the [TagsyRepository], returning the fresh entry.
class TagPickerSheet {
  const TagPickerSheet._();

  /// Show the picker and await the chosen (or newly-created) tag, or `null` on
  /// dismiss.
  ///
  /// [title] labels the sheet (e.g. `Add tag`, `Add subtag`). [excludeIds] are
  /// hidden from the list — typically the ids already applied/selected, plus
  /// (for the tag hierarchy) the subject tag itself. When [allowCreate] is
  /// true, the sheet offers inline tag creation via a [TextPromptDialog].
  ///
  /// The whole-tag listing is run lazily here (on show), not on the calling
  /// screen's open — see the class doc's TODO note about revisiting this in
  /// favour of a search-as-you-type picker.
  static Future<tagsy.TagEntry?> show({
    required BuildContext context,
    required TagsyRepository repository,
    required String title,
    Set<String> excludeIds = const {},
    bool allowCreate = false,
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

    return showModalBottomSheet<tagsy.TagEntry>(
      context: context,
      builder: (sheetContext) => SafeArea(
        child: ListView(
          shrinkWrap: true,
          children: [
            ListTile(
              title: Text(
                title,
                style: const TextStyle(fontWeight: FontWeight.bold),
              ),
            ),
            if (allowCreate)
              ListTile(
                leading: const Icon(Icons.add),
                title: const Text('Create new tag'),
                onTap: () async {
                  // Capture the sheet's navigator before the async gap so we
                  // can dismiss it (returning the created tag) once creation
                  // resolves.
                  final navigator = Navigator.of(sheetContext);
                  final created = await _createTag(sheetContext, repository);
                  navigator.pop(created);
                },
              ),
            if (available.isEmpty)
              const ListTile(title: Text('No more tags to add.'))
            else
              for (final tag in available)
                ListTile(
                  leading: TagColorSwatch(color: tag.color),
                  title: Text(tag.name),
                  onTap: () => Navigator.pop(sheetContext, tag),
                ),
          ],
        ),
      ),
    );
  }

  /// Prompt for a tag name, create it, and return the fresh [tagsy.TagEntry]
  /// (or `null` on cancel / failure). The engine substitutes a default palette
  /// color for the empty color passed here.
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
      final tagId = await repository.createTag(name: trimmed, color: '');
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
