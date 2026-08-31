// A labelled group of tag chips with an "Add" affordance.
//
// The "Tags" block on the file-detail screen, the "Tags"/"Subtags" blocks on
// the tag-detail screen, and the "Tags" block on the share-review screen were
// three copies of the same layout (a bold header with a trailing Add button,
// then a `Wrap` of chips or an empty-state label). This is the one
// implementation; behaviour is parameterised through callbacks so the widget
// stays stateless and each screen keeps its own wiring.

import 'package:flutter/material.dart';

import '../rust/api.dart' as tagsy;
import 'section_header.dart';
import 'tag_chip.dart';

/// Renders a titled section of [TagChip]s.
///
/// [tagIds] gives the render order; [resolved] maps each id to its
/// [tagsy.TagEntry] for the chip's name/color. An id missing from [resolved]
/// falls back to a placeholder chip that still uses the standard [TagChip]
/// visual (default style, `?<short-id>` label) so the row's shape and
/// affordances stay consistent — crucially, [onRemove] still fires, since the
/// hierarchy edge referencing an unresolved id is a real edge the user can
/// (and often needs to) untag. Tapping the placeholder body is disabled: an
/// unresolved id has no detail to open.
///
/// Ids reach this state when a hierarchy edge references a tag whose
/// definition hasn't reconciled to this device yet (see
/// `tag_ids_for_subtag` / `subtag_ids_for_tag` in the daemon's `entries.rs`,
/// which deliberately admit such rows via `t.deleted IS NULL`).
///
/// The three interactions are optional so one widget covers every caller:
///   * [onAdd] — the header's Add button; the button is disabled when null.
///   * [onTapTag] — tapping a resolved chip body (e.g. to open its detail);
///     resolved chips are not tappable when null, placeholder chips are never
///     tappable.
///   * [onRemove] — the chip's trailing X (untag); no X is shown when null.
class TagsSection extends StatelessWidget {
  const TagsSection({
    super.key,
    required this.title,
    required this.tagIds,
    required this.resolved,
    required this.emptyLabel,
    this.onAdd,
    this.onTapTag,
    this.onRemove,
  });

  final String title;
  final List<String> tagIds;
  final Map<String, tagsy.TagEntry> resolved;
  final String emptyLabel;
  final VoidCallback? onAdd;
  final ValueChanged<String>? onTapTag;
  final ValueChanged<String>? onRemove;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          SectionHeader(
            title,
            padding: EdgeInsets.zero,
            trailing: TextButton.icon(
              icon: const Icon(Icons.add, size: 18),
              label: const Text('Add'),
              onPressed: onAdd,
            ),
          ),
          if (tagIds.isEmpty)
            Text(emptyLabel)
          else
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [for (final tagId in tagIds) _chipFor(tagId)],
            ),
        ],
      ),
    );
  }

  Widget _chipFor(String tagId) {
    final resolvedTag = resolved[tagId];
    final tag = resolvedTag ?? _placeholderEntry(tagId);
    final onTapTag = this.onTapTag;
    final onRemove = this.onRemove;
    return TagChip(
      tag: tag,
      // Placeholder chips are never tappable (there's no detail to open for
      // an unresolved id). Removal remains available so the user can untag
      // even when the definition hasn't reconciled.
      onPressed: resolvedTag == null || onTapTag == null
          ? null
          : () => onTapTag(tagId),
      onDeleted: onRemove == null ? null : () => onRemove(tagId),
    );
  }

  /// Build a stand-in [tagsy.TagEntry] for an unresolved tag id: the default
  /// style plus a short label so the chip renders with the standard visual.
  /// The label is `?<first-8-of-id>` — a short id has proven enough to
  /// disambiguate across the store (see the daemon's short-id machinery), so
  /// eight characters plus the leading `?` marker is enough for the user to
  /// spot which edge they're removing.
  static tagsy.TagEntry _placeholderEntry(String tagId) {
    final short = tagId.length > 8 ? tagId.substring(0, 8) : tagId;
    return tagsy.TagEntry(
      tagId: tagId,
      name: '?$short',
      style: defaultTagStyle(),
      deleted: false,
    );
  }
}
