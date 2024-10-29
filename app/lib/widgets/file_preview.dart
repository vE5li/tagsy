import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter_markdown_plus/flutter_markdown_plus.dart';

import '../preview/extension_kind.dart';

/// Minimal file previewer for local files. Dispatches by extension, using the
/// shared [classifyExtension] table (kept in lockstep with the daemon's
/// `classify_by_extension`):
/// - Images: streamed via Image.file
/// - Markdown: full read into MarkdownBody
/// - Text/code: first [textByteLimit] bytes into SelectableText
/// - Pdf/Video/Other: fallback tile (no local pdf/video renderer here; a peer
///   still generates a thumbnail for these via the remote preview path)
class FilePreview extends StatelessWidget {
  final String path;
  final int textByteLimit;

  const FilePreview({
    super.key,
    required this.path,
    this.textByteLimit = 256 * 1024, // 256 KB
  });

  @override
  Widget build(BuildContext context) {
    final file = File(path);
    final ext = extensionOf(path);

    switch (classifyExtension(ext)) {
      case ExtensionKind.image:
        // Center the image (like the remote preview) rather than pinning it
        // top-left, which is InteractiveViewer's default child alignment.
        return InteractiveViewer(
          alignment: Alignment.center,
          child: Center(child: Image.file(file, fit: BoxFit.contain)),
        );

      case ExtensionKind.markdown:
        return _AsyncText(
          load: () => file.readAsString(),
          builder: (text) => Markdown(data: text, selectable: true),
        );

      case ExtensionKind.text:
        return _AsyncText(
          load: () => _readTextHead(file, textByteLimit),
          builder: (text) => SingleChildScrollView(
            padding: const EdgeInsets.all(12),
            child: SelectableText(
              text,
              style: const TextStyle(fontFamily: 'monospace', fontSize: 13),
            ),
          ),
        );

      // No local renderer for PDF or video (a peer can still generate a
      // thumbnail for these — see RemotePreview), and Other is un-previewable.
      case ExtensionKind.pdf:
      case ExtensionKind.video:
      case ExtensionKind.other:
        return ListTile(
          leading: const Icon(Icons.insert_drive_file_outlined),
          title: Text(path.split(Platform.pathSeparator).last),
          subtitle: Text('No preview for .$ext'),
        );
    }
  }

  static Future<String> _readTextHead(File file, int limit) async {
    final len = await file.length();
    if (len <= limit) return file.readAsString();
    final raf = await file.open();
    try {
      final bytes = await raf.read(limit);
      return '${String.fromCharCodes(bytes)}\n\n… (truncated)';
    } finally {
      await raf.close();
    }
  }
}

class _AsyncText extends StatelessWidget {
  final Future<String> Function() load;
  final Widget Function(String text) builder;

  const _AsyncText({required this.load, required this.builder});

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<String>(
      future: load(),
      builder: (context, snap) {
        if (snap.connectionState != ConnectionState.done) {
          return const Center(child: CircularProgressIndicator());
        }
        if (snap.hasError) {
          return Center(child: Text('Failed to load: ${snap.error}'));
        }
        return builder(snap.data ?? '');
      },
    );
  }
}
