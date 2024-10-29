// Share-review: the interstitial shown when files are shared into tagsy via
// the Android share sheet. Instead of uploading immediately, this screen lets
// the user attach tags to the incoming file(s) first, then uploads them with
// those tags applied. It mirrors the file detail screen's top preview so the
// user can see what they're about to ingest.
//
// One preview is shown per shared file (each capped in height like the detail
// screen). The chosen tags apply to *all* files in the batch — the common case
// is sharing a handful of related files that want the same tags. The user can
// also create a brand-new tag inline from the picker.

import 'dart:io';

import 'package:flutter/material.dart';

import '../data/repository.dart';
import '../format/format.dart';
import '../rust/api.dart' as tagsy;
import '../session/session.dart';
import '../widgets/file_preview.dart';
import '../widgets/tag_picker_sheet.dart';
import '../widgets/tags_section.dart';

class ShareReviewScreen extends StatefulWidget {
  const ShareReviewScreen({
    super.key,
    required this.session,
    required this.paths,
  });

  final TagsySession session;

  /// Absolute on-disk paths of the shared files to review and upload. Never
  /// empty (the share handler drops empty batches before navigating).
  final List<String> paths;

  @override
  State<ShareReviewScreen> createState() => _ShareReviewScreenState();
}

class _ShareReviewScreenState extends State<ShareReviewScreen> {
  TagsyRepository get _repository => widget.session.repository;

  /// Tags the user has picked to apply to the whole batch, keyed by string id
  /// so we can render chips and de-dupe against the picker.
  final Map<String, tagsy.TagEntry> _selected = {};

  bool _uploading = false;

  Future<void> _addTag() async {
    final chosen = await TagPickerSheet.show(
      context: context,
      repository: _repository,
      title: 'Add tag',
      excludeIds: _selected.keys.toSet(),
      allowCreate: true,
    );
    if (chosen == null) return;
    setState(() => _selected[chosen.tagId] = chosen);
  }

  void _removeTag(String tagId) {
    setState(() => _selected.remove(tagId));
  }

  Future<void> _upload() async {
    setState(() => _uploading = true);
    // Apply the selected tags (by string id) to every file in the batch. The
    // bridge resolves the ids per call, so the same list is safely reused
    // across uploads — unlike opaque TagId handles, which are consumed on use.
    final tagIds = _selected.keys.toList();

    var uploaded = 0;
    for (final path in widget.paths) {
      try {
        await _repository.uploadFile(
          path: path,
          pathName: nameFor(path),
          tags: tagIds,
        );
        uploaded++;
      } catch (error) {
        _snack('Failed to upload $path: $error');
      }
    }
    if (!mounted) return;
    if (uploaded > 0) {
      _snack('Uploaded $uploaded file${uploaded == 1 ? '' : 's'} to tagsy');
    }
    Navigator.of(context).maybePop();
  }

  void _snack(String message) {
    if (!mounted) return;
    ScaffoldMessenger.of(
      context,
    ).showSnackBar(SnackBar(content: Text(message)));
  }

  @override
  Widget build(BuildContext context) {
    final multiple = widget.paths.length > 1;
    return Scaffold(
      appBar: AppBar(
        title: Text(
          multiple ? 'Share ${widget.paths.length} files' : 'Share file',
        ),
      ),
      body: ListView(
        padding: const EdgeInsets.symmetric(vertical: 8),
        children: [
          for (final path in widget.paths) _buildPreview(context, path),
          const SizedBox(height: 16),
          TagsSection(
            title: 'Tags',
            tagIds: _selected.keys.toList(),
            resolved: _selected,
            emptyLabel: 'No tags selected.',
            onAdd: _uploading ? null : _addTag,
            // Chips are not tappable here (nothing to navigate to during
            // review); the trailing X removes, disabled during upload.
            onRemove: _uploading ? null : _removeTag,
          ),
          const SizedBox(height: 24),
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 16),
            child: FilledButton.icon(
              icon: _uploading
                  ? const SizedBox(
                      width: 18,
                      height: 18,
                      child: CircularProgressIndicator(strokeWidth: 2),
                    )
                  : const Icon(Icons.cloud_upload_outlined),
              label: Text(_uploading ? 'Uploading…' : 'Upload'),
              onPressed: _uploading ? null : _upload,
            ),
          ),
        ],
      ),
    );
  }

  /// A shared file's inline preview, capped in height, mirroring the file
  /// detail screen's top preview. When more than one file is shared, each is
  /// labelled with its name so the user can tell them apart.
  Widget _buildPreview(BuildContext context, String path) {
    final theme = Theme.of(context);
    final header = Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 4),
      child: Text(
        widget.paths.length > 1 ? nameFor(path) : 'Preview',
        style: theme.textTheme.labelMedium?.copyWith(
          color: theme.colorScheme.onSurfaceVariant,
          fontWeight: FontWeight.bold,
        ),
        overflow: TextOverflow.ellipsis,
      ),
    );
    final body = File(path).existsSync()
        ? ConstrainedBox(
            constraints: const BoxConstraints(maxHeight: 360),
            child: FilePreview(path: path),
          )
        : const ListTile(
            leading: Icon(Icons.error_outline),
            title: Text('File not available'),
            subtitle: Text('The shared file could not be read.'),
          );
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [header, body],
    );
  }
}
