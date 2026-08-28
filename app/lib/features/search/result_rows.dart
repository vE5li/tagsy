// The individual rows the home screen's search results are built from: a
// section header, a tag row, a file row, and the one-off "create tag" row.
//
// Each interactive row takes a [FocusNode] owned by the enclosing
// [RovingFocusList] (see widgets/roving_focus_list.dart) so the keyboard's
// roving tab-stop can land on it, plus an `onActivate` callback the home screen
// wires to navigation (it keeps navigation on the state so it can restore focus
// to the row when the pushed detail screen pops).

import 'package:flutter/material.dart';

import '../../rust/api.dart' as tagsy;
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
    required this.focusNode,
    required this.onActivate,
  });

  final tagsy.TagEntry tag;

  /// The stable focus node the enclosing [RovingFocusList] owns for this row's
  /// slot; attach it so the roving tab-stop can land here.
  final FocusNode focusNode;

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
    required this.focusNode,
  });

  final String name;
  final VoidCallback onCreate;

  /// See [TagRow.focusNode].
  final FocusNode focusNode;

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
