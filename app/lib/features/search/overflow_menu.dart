// The home AppBar's overflow menu: a single three-dot button that hosts the
// low-frequency actions (deleted-search toggle, the file result view-mode
// selectors, purge cached previews, copy public key) so they don't clutter the
// AppBar. Sync status — peers, operations, activity — lives in the AppBar's
// status button instead (`features/status/`).

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../../session/session.dart';
import 'view_mode.dart';

class OverflowMenu extends StatefulWidget {
  const OverflowMenu({
    super.key,
    required this.session,
    required this.publicKey,
    required this.showDeleted,
    required this.onToggleShowDeleted,
    required this.fileViewMode,
    required this.onSelectViewMode,
  });

  final TagsySession? session;

  /// Non-null on Android, where the app owns the identity and can expose the
  /// public key; null on Linux (daemon owns the key). Drives whether the
  /// "Copy public key" item is shown at all.
  final String? publicKey;

  /// Current state of the search-deleted toggle. Controls the icon and label
  /// used for that menu item.
  final bool showDeleted;

  /// Invoked when the deleted-search toggle item is picked.
  final VoidCallback onToggleShowDeleted;

  /// The active file result view mode. Each mode has its own dedicated menu
  /// item; the active one is marked with a check.
  final FileViewMode fileViewMode;

  /// Invoked with the mode whose menu item was picked.
  final ValueChanged<FileViewMode> onSelectViewMode;

  @override
  State<OverflowMenu> createState() => _OverflowMenuState();
}

/// Menu-item identifiers. Kept as a private enum so the switch in `onSelected`
/// is exhaustive.
enum _MenuAction {
  toggleDeleted,
  viewModeList,
  viewModeTile,
  viewModeLarge,
  viewModeFull,
  purgePreviews,
  copyPublicKey,
}

class _OverflowMenuState extends State<OverflowMenu> {
  /// True while a preview purge is in flight, so the menu item shows a spinner
  /// and can't be re-invoked.
  bool _purging = false;

  Future<void> _purgePreviews() async {
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

  Future<void> _copyPublicKey() async {
    final key = widget.publicKey;
    if (key == null) return;
    await Clipboard.setData(ClipboardData(text: key));
  }

  void _onSelected(_MenuAction action) {
    switch (action) {
      case _MenuAction.toggleDeleted:
        widget.onToggleShowDeleted();
      case _MenuAction.viewModeList:
        widget.onSelectViewMode(FileViewMode.list);
      case _MenuAction.viewModeTile:
        widget.onSelectViewMode(FileViewMode.tile);
      case _MenuAction.viewModeLarge:
        widget.onSelectViewMode(FileViewMode.large);
      case _MenuAction.viewModeFull:
        widget.onSelectViewMode(FileViewMode.full);
      case _MenuAction.purgePreviews:
        _purgePreviews();
      case _MenuAction.copyPublicKey:
        _copyPublicKey();
    }
  }

  @override
  Widget build(BuildContext context) {
    final session = widget.session;
    final publicKey = widget.publicKey;

    return PopupMenuButton<_MenuAction>(
      tooltip: 'More',
      icon: const Icon(Icons.more_vert),
      onSelected: _onSelected,
      itemBuilder: (context) => [
        PopupMenuItem<_MenuAction>(
          value: _MenuAction.toggleDeleted,
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: Icon(
              widget.showDeleted ? Icons.delete : Icons.delete_outline,
            ),
            title: Text(
              widget.showDeleted
                  ? 'Showing deleted — tap to search live'
                  : 'Search deleted files and tags',
            ),
          ),
        ),
        PopupMenuItem<_MenuAction>(
          value: _MenuAction.viewModeList,
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: const Icon(Icons.view_list_outlined),
            title: const Text('View as list'),
            trailing: widget.fileViewMode == FileViewMode.list
                ? const Icon(Icons.check)
                : null,
          ),
        ),
        PopupMenuItem<_MenuAction>(
          value: _MenuAction.viewModeTile,
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: const Icon(Icons.grid_view_outlined),
            title: const Text('View as tiles'),
            trailing: widget.fileViewMode == FileViewMode.tile
                ? const Icon(Icons.check)
                : null,
          ),
        ),
        PopupMenuItem<_MenuAction>(
          value: _MenuAction.viewModeLarge,
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: const Icon(Icons.view_agenda_outlined),
            title: const Text('View as large tiles'),
            trailing: widget.fileViewMode == FileViewMode.large
                ? const Icon(Icons.check)
                : null,
          ),
        ),
        PopupMenuItem<_MenuAction>(
          value: _MenuAction.viewModeFull,
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: const Icon(Icons.fullscreen),
            title: const Text('View as full tiles'),
            trailing: widget.fileViewMode == FileViewMode.full
                ? const Icon(Icons.check)
                : null,
          ),
        ),
        PopupMenuItem<_MenuAction>(
          value: _MenuAction.purgePreviews,
          enabled: session != null && !_purging,
          child: ListTile(
            contentPadding: EdgeInsets.zero,
            leading: _purging
                ? const SizedBox(
                    width: 18,
                    height: 18,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  )
                : const Icon(Icons.image_not_supported_outlined),
            title: const Text('Purge cached previews'),
          ),
        ),
        if (publicKey != null)
          const PopupMenuItem<_MenuAction>(
            value: _MenuAction.copyPublicKey,
            child: ListTile(
              contentPadding: EdgeInsets.zero,
              leading: Icon(Icons.copy),
              title: Text('Copy public key'),
            ),
          ),
      ],
    );
  }
}
