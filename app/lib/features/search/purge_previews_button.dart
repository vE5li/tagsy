// AppBar action that purges the daemon's cached file previews, forcing them to
// regenerate on demand. Useful after the set of previewable file types changes
// (e.g. new PDF/video support). Disabled while no session is attached and while
// a purge is in flight.

import 'package:flutter/material.dart';

import '../../session/session.dart';

/// The home AppBar's "purge cached previews" button.
class PurgePreviewsButton extends StatefulWidget {
  const PurgePreviewsButton({super.key, required this.session});

  final TagsySession? session;

  @override
  State<PurgePreviewsButton> createState() => _PurgePreviewsButtonState();
}

class _PurgePreviewsButtonState extends State<PurgePreviewsButton> {
  bool _purging = false;

  Future<void> _purge() async {
    final session = widget.session;
    if (session == null || _purging) return;

    setState(() => _purging = true);
    try {
      final purged = await session.repository.purgePreviews();
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('Purged $purged cached previews')));
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Failed to purge previews: $error')),
      );
    } finally {
      if (mounted) setState(() => _purging = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return IconButton(
      icon: _purging
          ? const SizedBox(
              width: 18,
              height: 18,
              child: CircularProgressIndicator(strokeWidth: 2),
            )
          : const Icon(Icons.image_not_supported_outlined),
      tooltip: 'Purge cached previews',
      onPressed: widget.session == null || _purging ? null : _purge,
    );
  }
}
