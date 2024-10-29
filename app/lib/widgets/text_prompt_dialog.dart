// A single-field prompt dialog: one [TextField] that pops the entered string on
// submit (Enter or the confirm button) and `null` on cancel.
//
// Replaces three near-verbatim dialogs that each wrapped a lone TextField in an
// AlertDialog and popped its text — the file-rename, tag-rename, and tag-create
// prompts. Callers filter empty / unchanged input themselves (the dialog pops
// the raw text). Anything richer than a single field (e.g. the tag recolor
// dialog, with its palette + live swatch) is out of scope and stays bespoke.

import 'package:flutter/material.dart';

/// Prompts the user for a single line of text.
///
/// Show it with [showDialog] and await the result: the entered string on
/// confirm, or `null` if the user cancelled (tapped Cancel or dismissed the
/// barrier). The field autofocuses and submits on Enter.
class TextPromptDialog extends StatefulWidget {
  const TextPromptDialog({
    super.key,
    required this.title,
    required this.label,
    this.initial = '',
    this.hintText,
    this.confirmLabel = 'Save',
  });

  /// Dialog title, e.g. `Rename file`.
  final String title;

  /// The field's floating label, e.g. `Logical path`.
  final String label;

  /// Prefilled text; the field opens with this selected-for-overwrite value.
  /// Empty for a create-style prompt.
  final String initial;

  /// Optional placeholder shown when the field is empty.
  final String? hintText;

  /// The confirm button's text, e.g. `Save` or `Create`.
  final String confirmLabel;

  @override
  State<TextPromptDialog> createState() => _TextPromptDialogState();
}

class _TextPromptDialogState extends State<TextPromptDialog> {
  late final TextEditingController _controller = TextEditingController(
    text: widget.initial,
  );

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  void _submit() => Navigator.pop(context, _controller.text);

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: Text(widget.title),
      content: TextField(
        controller: _controller,
        autofocus: true,
        decoration: InputDecoration(
          labelText: widget.label,
          hintText: widget.hintText,
        ),
        onSubmitted: (_) => _submit(),
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.pop(context),
          child: const Text('Cancel'),
        ),
        TextButton(onPressed: _submit, child: Text(widget.confirmLabel)),
      ],
    );
  }
}
