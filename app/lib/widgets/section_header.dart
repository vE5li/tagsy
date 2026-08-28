// A non-interactive section label used across the app: the "Tags" / "Files"
// dividers in search results, the "Tags" / "Subtags" headers on the detail
// screens, the "Preview" header, and so on. This is the single source of truth
// for section-header styling — accent color + a slightly heavier weight — so
// every heading looks the same.

import 'package:flutter/material.dart';

/// A styled section heading.
///
/// [trailing] renders an optional widget on the right of the label (e.g. an
/// "Add" button); when present the label and trailing sit on one row with the
/// label pushed left. [padding] defaults to the search-list spacing but callers
/// laying the header out inside their own padded column can override it.
class SectionHeader extends StatelessWidget {
  const SectionHeader(
    this.label, {
    super.key,
    this.trailing,
    this.padding = const EdgeInsets.fromLTRB(16, 12, 16, 4),
  });

  final String label;
  final Widget? trailing;
  final EdgeInsetsGeometry padding;

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    // A long label (e.g. a shared file's name) ellipsizes rather than
    // overflowing the row.
    final text = Text(
      label,
      style: theme.textTheme.labelMedium?.copyWith(
        color: theme.colorScheme.primary,
        fontWeight: FontWeight.w600,
      ),
      overflow: TextOverflow.ellipsis,
    );
    final trailing = this.trailing;
    return Padding(
      padding: padding,
      // With a trailing widget the label takes the remaining width (so it can
      // ellipsize and the trailing stays put); without one it sizes to content.
      child: trailing == null
          ? text
          : Row(children: [Expanded(child: text), const SizedBox(width: 8), trailing]),
    );
  }
}
