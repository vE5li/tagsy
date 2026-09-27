// New file: compose a file from scratch — its name, its initial text content,
// and the tags to apply — then create it. Reached from the home screen's
// "New file" button.
//
// Uploads go through the daemon's outbox, which takes a *path* (see
// AGENTS.md, "Uploads go through the outbox"). So on confirm we write the
// typed content to a private temp file, hand that path to `uploadFile`, and
// delete the temp once the call returns: by then the daemon holds its own
// copy. On success the screen replaces itself with the new file's detail
// screen, so backing out of that returns to wherever "New file" was pressed.

import 'dart:io';

import 'package:flutter/material.dart';

import '../data/repository.dart';
import '../format/format.dart';
import '../rust/api.dart' as tagsy;
import '../session/session.dart';
import '../widgets/tag_picker_sheet.dart';
import '../widgets/tags_section.dart';
import 'file_detail_screen.dart';

class NewFileScreen extends StatefulWidget {
  const NewFileScreen({super.key, required this.session});

  final TagsySession session;

  @override
  State<NewFileScreen> createState() => _NewFileScreenState();
}

class _NewFileScreenState extends State<NewFileScreen> {
  TagsyRepository get _repository => widget.session.repository;

  final TextEditingController _name = TextEditingController();
  final TextEditingController _content = TextEditingController();

  /// Tags to apply to the new file, keyed by string id (for chip rendering and
  /// de-duping against the picker).
  final Map<String, tagsy.TagEntry> _selected = {};

  bool _creating = false;

  @override
  void initState() {
    super.initState();
    // Rebuild so the Create button enables/disables with the name field.
    _name.addListener(() => setState(() {}));
  }

  @override
  void dispose() {
    _name.dispose();
    _content.dispose();
    super.dispose();
  }

  bool get _canCreate => !_creating && _name.text.trim().isNotEmpty;

  /// Whether leaving now would throw away something the user entered.
  bool get _hasInput =>
      _name.text.trim().isNotEmpty ||
      _content.text.isNotEmpty ||
      _selected.isNotEmpty;

  Future<void> _addTag() async {
    final chosen = await TagPickerSheet.show(
      context: context,
      repository: _repository,
      title: 'Add tag',
      excludeIds: _selected.keys.toSet(),
    );
    if (chosen == null) return;
    setState(() => _selected[chosen.tagId] = chosen);
  }

  void _removeTag(String tagId) {
    setState(() => _selected.remove(tagId));
  }

  Future<void> _create() async {
    final name = _name.text.trim();
    if (name.isEmpty || _creating) return;
    setState(() => _creating = true);
    Directory? temp;
    try {
      // The temp file carries the logical basename so nothing downstream has
      // to guess the type from an anonymous path.
      temp = await Directory.systemTemp.createTemp('tagsy-new-');
      final source = File('${temp.path}/${nameFor(name)}');
      await source.writeAsString(_content.text, flush: true);
      final fileId = await _repository.uploadFile(
        path: source.path,
        pathName: name,
        tags: _selected.keys.toList(),
      );
      // `uploadFile` returns once the catalog has recorded the file, so the
      // entry is readable right away.
      final file = await _repository.getFileEntry(
        fileId: fileId,
        deletedRule: tagsy.DeletedRule.exclude,
      );
      if (!mounted) return;
      Navigator.of(context).pushReplacement(
        MaterialPageRoute(
          builder: (_) => FileDetailScreen(session: widget.session, file: file),
        ),
      );
    } catch (error) {
      if (!mounted) return;
      setState(() => _creating = false);
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('Failed to create file: $error')));
    } finally {
      // The daemon copied the bytes into its outbox; ours are no longer needed.
      try {
        await temp?.delete(recursive: true);
      } catch (_) {
        // Best-effort: it's a temp dir.
      }
    }
  }

  /// Ask before discarding entered input on back navigation.
  Future<void> _confirmDiscard() async {
    final discard = await showDialog<bool>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Discard new file?'),
        content: const Text('The file has not been created yet.'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context, false),
            child: const Text('Keep editing'),
          ),
          TextButton(
            onPressed: () => Navigator.pop(context, true),
            child: const Text('Discard'),
          ),
        ],
      ),
    );
    if (discard == true && mounted) Navigator.of(context).pop();
  }

  @override
  Widget build(BuildContext context) {
    return PopScope(
      canPop: !_creating && !_hasInput,
      onPopInvokedWithResult: (didPop, _) {
        if (didPop || _creating) return;
        _confirmDiscard();
      },
      child: Scaffold(
        appBar: AppBar(title: const Text('New file')),
        body: ListView(
          padding: const EdgeInsets.symmetric(vertical: 16),
          children: [
            Padding(
              padding: const EdgeInsets.symmetric(horizontal: 16),
              child: TextField(
                controller: _name,
                autofocus: true,
                enabled: !_creating,
                textInputAction: TextInputAction.next,
                decoration: const InputDecoration(
                  labelText: 'File name',
                  hintText: 'notes.md',
                  border: OutlineInputBorder(),
                ),
              ),
            ),
            const SizedBox(height: 16),
            Padding(
              padding: const EdgeInsets.symmetric(horizontal: 16),
              child: TextField(
                controller: _content,
                enabled: !_creating,
                minLines: 8,
                maxLines: null,
                keyboardType: TextInputType.multiline,
                decoration: const InputDecoration(
                  labelText: 'Content',
                  alignLabelWithHint: true,
                  border: OutlineInputBorder(),
                ),
              ),
            ),
            const SizedBox(height: 16),
            TagsSection(
              title: 'Tags',
              tagIds: _selected.keys.toList(),
              resolved: _selected,
              emptyLabel: 'No tags selected.',
              onAdd: _creating ? null : _addTag,
              onRemove: _creating ? null : _removeTag,
            ),
            const SizedBox(height: 24),
            Padding(
              padding: const EdgeInsets.symmetric(horizontal: 16),
              child: FilledButton.icon(
                icon: _creating
                    ? const SizedBox(
                        width: 18,
                        height: 18,
                        child: CircularProgressIndicator(strokeWidth: 2),
                      )
                    : const Icon(Icons.note_add_outlined),
                label: Text(_creating ? 'Creating…' : 'Create'),
                onPressed: _canCreate ? _create : null,
              ),
            ),
          ],
        ),
      ),
    );
  }
}
