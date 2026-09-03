// The home AppBar's overflow menu: a single three-dot button that hosts the
// low-frequency actions (Operations, deleted-search toggle, the file result
// view-mode selectors, purge cached previews, copy public key) so they don't
// clutter the AppBar. The storage indicator stays inline because it's a passive
// readout, not an action.
//
// The Operations item still needs its live active-count badge, so this widget
// itself subscribes to the operations stream — the badge renders on the
// three-dot menu button whenever real sync work is in flight (transfers,
// reconciliation, fetches, connect attempts). Steady-state peer connections are
// NOT operations any more — they are connection *state*, shown by the AppBar's
// connection indicator — so the badge is no longer permanently lit whenever a
// peer is connected. It is now a genuine "something is happening" signal.

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../../rust/api.dart' as tagsy;
import '../../screens/operations_screen.dart';
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
  operations,
  toggleDeleted,
  viewModeList,
  viewModeTile,
  viewModeLarge,
  viewModeFull,
  purgePreviews,
  copyPublicKey,
}

class _OverflowMenuState extends State<OverflowMenu> {
  /// Currently-active operations, keyed by id. Peer connections are no longer
  /// operations, so this is now purely real sync work (see [_countsAsWork]).
  final Map<BigInt, tagsy.OperationEntry> _working = {};

  /// True while a preview purge is in flight, so the menu item shows a spinner
  /// and can't be re-invoked.
  bool _purging = false;

  bool _watching = false;

  @override
  void initState() {
    super.initState();
    if (widget.session != null) _watch();
  }

  @override
  void didUpdateWidget(covariant OverflowMenu oldWidget) {
    super.didUpdateWidget(oldWidget);
    // The session arrives asynchronously after connect; start watching once it
    // first appears.
    if (oldWidget.session == null && widget.session != null) _watch();
  }

  @override
  void dispose() {
    _watching = false;
    super.dispose();
  }

  /// Whether an operation should count toward the badge: any active operation.
  /// (All operations are now genuine work — connections moved out of this
  /// stream — so no kind special-casing is needed here.)
  static bool _countsAsWork(tagsy.OperationEntry op) {
    return op.status is tagsy.OperationStatusDto_Active;
  }

  void _apply(tagsy.OperationEntry op) {
    if (_countsAsWork(op)) {
      _working[op.id] = op;
    } else {
      _working.remove(op.id);
    }
  }

  Future<void> _watch() async {
    final session = widget.session;
    if (session == null || _watching) return;
    _watching = true;
    try {
      // Seed from a snapshot so an already-in-flight transfer is counted
      // immediately, then apply live updates on top.
      final snapshot = await session.repository.listOperations();
      if (!mounted) return;
      setState(() {
        _working.clear();
        for (final op in snapshot) {
          _apply(op);
        }
      });

      final updates = await session.repository.subscribeOperations();
      while (mounted && _watching) {
        final update = await updates.next();
        if (update == null) break;
        if (!mounted) break;
        switch (update) {
          case tagsy.OperationUpdateDto_Resynced():
            final refreshed = await session.repository.listOperations();
            if (!mounted) break;
            setState(() {
              _working.clear();
              for (final op in refreshed) {
                _apply(op);
              }
            });
          case tagsy.OperationUpdateDto_Started(:final operation):
            setState(() => _apply(operation));
          case tagsy.OperationUpdateDto_Updated(:final operation):
            setState(() => _apply(operation));
        }
      }
    } catch (_) {
      // Transient stream hiccups are surfaced elsewhere; don't kill the menu.
    }
  }

  void _openOperations() {
    final session = widget.session;
    if (session == null) return;
    FocusManager.instance.primaryFocus?.unfocus();
    Navigator.push(
      context,
      MaterialPageRoute(builder: (_) => OperationsScreen(session: session)),
    );
  }

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
      case _MenuAction.operations:
        _openOperations();
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
    final activeCount = _working.length;
    final publicKey = widget.publicKey;

    final button = PopupMenuButton<_MenuAction>(
      tooltip: 'More',
      icon: const Icon(Icons.more_vert),
      onSelected: _onSelected,
      itemBuilder: (context) => [
        PopupMenuItem<_MenuAction>(
          value: _MenuAction.operations,
          enabled: session != null,
          child: _OperationsItem(activeCount: activeCount),
        ),
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

    if (activeCount == 0) return button;

    // Overlay a small count badge on the three-dot button so the
    // active-operations indicator remains visible without opening the menu —
    // preserving what the standalone OperationsButton used to show.
    return Stack(
      alignment: Alignment.center,
      children: [
        button,
        Positioned(
          top: 8,
          right: 6,
          child: IgnorePointer(
            child: Container(
              padding: const EdgeInsets.symmetric(horizontal: 5, vertical: 1),
              decoration: BoxDecoration(
                color: Theme.of(context).colorScheme.error,
                borderRadius: BorderRadius.circular(8),
              ),
              constraints: const BoxConstraints(minWidth: 16),
              child: Text(
                '$activeCount',
                textAlign: TextAlign.center,
                style: TextStyle(
                  color: Theme.of(context).colorScheme.onError,
                  fontSize: 10,
                  fontWeight: FontWeight.bold,
                ),
              ),
            ),
          ),
        ),
      ],
    );
  }
}

/// The "Operations" menu-item body, with an inline active-count badge next to
/// the label so users see how many operations are running before opening the
/// operations screen.
class _OperationsItem extends StatelessWidget {
  const _OperationsItem({required this.activeCount});

  final int activeCount;

  @override
  Widget build(BuildContext context) {
    return ListTile(
      contentPadding: EdgeInsets.zero,
      leading: const Icon(Icons.sync),
      title: const Text('Operations'),
      trailing: activeCount == 0
          ? null
          : Container(
              padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
              decoration: BoxDecoration(
                color: Theme.of(context).colorScheme.error,
                borderRadius: BorderRadius.circular(10),
              ),
              constraints: const BoxConstraints(minWidth: 20),
              child: Text(
                '$activeCount',
                textAlign: TextAlign.center,
                style: TextStyle(
                  color: Theme.of(context).colorScheme.onError,
                  fontSize: 11,
                  fontWeight: FontWeight.bold,
                ),
              ),
            ),
    );
  }
}
