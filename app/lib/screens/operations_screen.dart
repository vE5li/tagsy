// Operations screen: a live list of what the daemon is currently doing —
// connecting to peers, sending/receiving files, reconciling manifests, placing
// files, etc. This is the UI surface for the sync-operation stream.
//
// It seeds from a `listOperations()` snapshot, then applies live
// `OperationUpdateDto`s to an in-memory map keyed by operation id: `started`
// inserts, `updated` replaces (progress or terminal outcome), and `resynced`
// re-snapshots. Terminal operations (completed/failed/aborted) still arrive as
// `updated` events; we keep them visible briefly so the user can see the
// outcome rather than having rows silently vanish.
//
// Read-only for now — no abort/cancel action.

import 'package:flutter/material.dart';

import '../data/repository.dart';
import '../format/operation_labels.dart';
import '../rust/api.dart' as tagsy;
import '../session/session.dart';

class OperationsScreen extends StatefulWidget {
  const OperationsScreen({super.key, required this.session});

  final TagsySession session;

  @override
  State<OperationsScreen> createState() => _OperationsScreenState();
}

class _OperationsScreenState extends State<OperationsScreen> {
  /// Current operations, keyed by their stable id so live updates replace the
  /// matching row in place. Rendered sorted by `startedAt` (newest first).
  final Map<BigInt, tagsy.OperationEntry> _operations = {};

  bool _loading = true;
  String? _error;
  bool _watching = false;

  TagsyRepository get _repository => widget.session.repository;

  @override
  void initState() {
    super.initState();
    _load();
    _watch();
  }

  @override
  void dispose() {
    _watching = false;
    super.dispose();
  }

  /// Snapshot the currently-active operations for the initial paint (and after
  /// a `resynced`).
  Future<void> _load() async {
    try {
      final operations = await _repository.listOperations();
      if (!mounted) return;
      setState(() {
        _operations
          ..clear()
          ..addEntries(operations.map((op) => MapEntry(op.id, op)));
        _loading = false;
        _error = null;
      });
    } catch (error) {
      if (!mounted) return;
      setState(() {
        _error = '$error';
        _loading = false;
      });
    }
  }

  /// Consume the live operation stream, mutating the in-memory map. Mirrors the
  /// change-stream watch loop used by the other screens.
  Future<void> _watch() async {
    _watching = true;
    try {
      final updates = await _repository.subscribeOperations();
      while (mounted && _watching) {
        final update = await updates.next();
        if (update == null) break;
        if (!mounted) break;
        switch (update) {
          case tagsy.OperationUpdateDto_Resynced():
            await _load();
          case tagsy.OperationUpdateDto_Started(:final operation):
            setState(() => _operations[operation.id] = operation);
          case tagsy.OperationUpdateDto_Updated(:final operation):
            setState(() => _operations[operation.id] = operation);
        }
      }
    } catch (_) {
      // Transient stream hiccups are surfaced elsewhere; don't kill the screen.
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Operations'),
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            tooltip: 'Refresh',
            onPressed: _load,
          ),
        ],
      ),
      body: SafeArea(child: _buildBody()),
    );
  }

  Widget _buildBody() {
    if (_loading) {
      return const Center(child: CircularProgressIndicator());
    }
    if (_error != null) {
      return Center(child: Text('Error: $_error'));
    }
    if (_operations.isEmpty) {
      return const Center(child: Text('No active operations'));
    }

    final operations = _operations.values.toList()
      ..sort((a, b) => b.startedAt.compareTo(a.startedAt));

    return ListView.builder(
      itemCount: operations.length,
      itemBuilder: (_, index) => _OperationRow(operation: operations[index]),
    );
  }
}

class _OperationRow extends StatelessWidget {
  const _OperationRow({required this.operation});

  final tagsy.OperationEntry operation;

  @override
  Widget build(BuildContext context) {
    return ListTile(
      dense: true,
      leading: Icon(iconForOperationKind(operation.kind)),
      title: Text(labelForOperationKind(operation.kind)),
      subtitle: Text(_subtitle(operation)),
      trailing: _trailing(operation),
    );
  }

  /// Subtitle: the peer and/or file the operation concerns, plus its status.
  String _subtitle(tagsy.OperationEntry operation) {
    final parts = <String>[];
    if (operation.peerName != null) {
      parts.add('peer: ${operation.peerName}');
    }
    if (operation.fileId != null) {
      final id = operation.fileId!;
      final short = id.length > 12 ? id.substring(0, 12) : id;
      parts.add('file: $short');
    }
    parts.add(_statusLabel(operation.status));
    return parts.join('  ·  ');
  }

  /// Status label, including a `done/total` fragment when progress is reported.
  String _statusLabel(tagsy.OperationStatusDto status) {
    switch (status) {
      case tagsy.OperationStatusDto_Active():
        final done = operation.progressDone;
        if (done == null) return 'active';
        final total = operation.progressTotal;
        return total == null ? 'active ($done)' : 'active ($done/$total)';
      case tagsy.OperationStatusDto_Completed():
        return 'completed';
      case tagsy.OperationStatusDto_Failed(:final reason):
        return 'failed: $reason';
      case tagsy.OperationStatusDto_Aborted():
        return 'aborted';
    }
  }

  /// A progress indicator for active operations that report byte/entry
  /// progress; nothing otherwise.
  Widget? _trailing(tagsy.OperationEntry operation) {
    if (operation.status is! tagsy.OperationStatusDto_Active) return null;
    final done = operation.progressDone;
    final total = operation.progressTotal;
    if (done == null) return null;
    final value = (total != null && total > BigInt.zero)
        ? done.toDouble() / total.toDouble()
        : null;
    return SizedBox(
      width: 24,
      height: 24,
      child: CircularProgressIndicator(strokeWidth: 2, value: value),
    );
  }
}
