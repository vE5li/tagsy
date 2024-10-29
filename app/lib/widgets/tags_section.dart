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
import 'tag_chip.dart';

/// Renders a titled section of [TagChip]s.
///
/// [tagIds] gives the render order; [resolved] maps each id to its
/// [tagsy.TagEntry] for the chip's name/color. An id missing from [resolved]
/// (a transient load race) falls back to a monospace chip showing the raw id,
/// so the row stays meaningful.
///
/// The three interactions are optional so one widget covers every caller:
///   * [onAdd] — the header's Add button; the button is disabled when null.
///   * [onTapTag] — tapping a chip body (e.g. to open its detail); chips are
///     not tappable when null.
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
    final theme = Theme.of(context);
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Text(
                title,
                style: theme.textTheme.labelMedium?.copyWith(
                  color: theme.colorScheme.onSurfaceVariant,
                  fontWeight: FontWeight.bold,
                ),
              ),
              const Spacer(),
              TextButton.icon(
                icon: const Icon(Icons.add, size: 18),
                label: const Text('Add'),
                onPressed: onAdd,
              ),
            ],
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
    final tag = resolved[tagId];
    if (tag == null) {
      // Tag not resolved (e.g. a race between the caller's load steps). Show
      // the raw id so the row is still meaningful.
      return Chip(
        label: Text(tagId, style: const TextStyle(fontFamily: 'monospace')),
      );
    }
    final onTapTag = this.onTapTag;
    final onRemove = this.onRemove;
    return TagChip(
      tag: tag,
      onPressed: onTapTag == null ? null : () => onTapTag(tagId),
      onDeleted: onRemove == null ? null : () => onRemove(tagId),
    );
  }
}
