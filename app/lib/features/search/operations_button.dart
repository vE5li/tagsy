// AppBar action that opens the [OperationsScreen] (the live view of what the
// daemon is doing). Always shown; disabled until a session is available.
//
// It watches the operation stream itself so it can render a live badge with
// the number of operations currently active. This includes steady-state
// `peer_connected_*` rows, so the badge doubles as a connected-peer count when
// nothing else is in flight.

import 'package:flutter/material.dart';

import '../../rust/api.dart' as tagsy;
import '../../screens/operations_screen.dart';
import '../../session/session.dart';

/// The home AppBar's operations button, with a live active-operation-count
/// badge; opens [OperationsScreen].
class OperationsButton extends StatefulWidget {
  const OperationsButton({super.key, required this.session});

  final TagsySession? session;

  @override
  State<OperationsButton> createState() => _OperationsButtonState();
}

class _OperationsButtonState extends State<OperationsButton> {
  /// Currently-active operations, keyed by id. Includes steady-state
  /// peer-connection rows (see [_countsAsWork]).
  final Map<BigInt, tagsy.OperationEntry> _working = {};

  bool _watching = false;

  @override
  void initState() {
    super.initState();
    if (widget.session != null) _watch();
  }

  @override
  void didUpdateWidget(covariant OperationsButton oldWidget) {
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

  /// Whether an operation should count toward the badge: any active operation
  /// (including steady-state peer connections).
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
      // Transient stream hiccups are surfaced elsewhere; don't kill the button.
    }
  }

  @override
  Widget build(BuildContext context) {
    final session = widget.session;
    final count = _working.length;
    final button = IconButton(
      icon: const Icon(Icons.sync),
      tooltip: 'Operations',
      onPressed: session == null
          ? null
          : () {
              FocusManager.instance.primaryFocus?.unfocus();
              Navigator.push(
                context,
                MaterialPageRoute(
                  builder: (_) => OperationsScreen(session: session),
                ),
              );
            },
    );

    if (count == 0) return button;

    // Overlay a small count badge on the top-right of the icon.
    return Stack(
      alignment: Alignment.center,
      children: [
        button,
        Positioned(
          top: 8,
          right: 6,
          child: Container(
            padding: const EdgeInsets.symmetric(horizontal: 5, vertical: 1),
            decoration: BoxDecoration(
              color: Theme.of(context).colorScheme.error,
              borderRadius: BorderRadius.circular(8),
            ),
            constraints: const BoxConstraints(minWidth: 16),
            child: Text(
              '$count',
              textAlign: TextAlign.center,
              style: TextStyle(
                color: Theme.of(context).colorScheme.onError,
                fontSize: 10,
                fontWeight: FontWeight.bold,
              ),
            ),
          ),
        ),
      ],
    );
  }
}
