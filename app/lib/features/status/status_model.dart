// The single live model behind the AppBar status button: peer connections,
// sync operations, and daemon activity, watched once and shared by the button
// and its sheet.
//
// Three sources feed it:
//   - the connection snapshot + stream (who is connected),
//   - the operation snapshot + stream (what the daemon is doing, with progress),
//   - `activity()`, polled every [pollInterval] (is anything still queued or in
//     flight — work that is not an operation, like applying a manifest's
//     changes or a filesystem event waiting out its debounce).
//
// "Busy" is set as soon as a sample is not idle or an operation runs, but only
// cleared after two consecutive idle samples: a single idle sample can race a
// message in flight (see `tagsy_api::activity`), and the indicator should not
// flicker between two bursts of the same sync.

import 'dart:async';

import 'package:flutter/foundation.dart';

import '../../data/status_source.dart';
import '../../rust/api.dart' as tagsy;

/// Machine kind string (see `flatten_kind` in tagsy-bridge) of a connect
/// attempt. Shown as the peer state ("Connecting…"), not as sync work.
const String kConnectingKind = 'connecting_to_peer';

class StatusModel extends ChangeNotifier {
  StatusModel(
    this._source, {
    this.pollInterval = const Duration(seconds: 1),
    this.recentLimit = 10,
  });

  final StatusSource _source;

  /// How often [tagsy.ActivityEntry] is sampled.
  final Duration pollInterval;

  /// How many finished operations are kept for the "recent" list.
  final int recentLimit;

  final Map<String, tagsy.ConnectedPeerDto> _peers = {};
  final Map<BigInt, tagsy.OperationEntry> _active = {};
  final List<tagsy.OperationEntry> _recent = [];
  tagsy.ActivityEntry? _activity;
  bool _activityBusy = false;
  int _idleSamples = 0;

  bool _running = false;
  Timer? _pollTimer;

  /// Connected peers, sorted by name.
  List<tagsy.ConnectedPeerDto> get peers =>
      _peers.values.toList()..sort((a, b) => a.peerName.compareTo(b.peerName));

  /// Whether a connect attempt is in flight.
  bool get connecting => _active.values.any((op) => op.kind == kConnectingKind);

  /// Active sync operations (connect attempts excluded), newest first.
  List<tagsy.OperationEntry> get activeOperations =>
      _active.values.where((op) => op.kind != kConnectingKind).toList()
        ..sort((a, b) => b.startedAt.compareTo(a.startedAt));

  /// Recently finished operations (connect attempts excluded), newest first.
  List<tagsy.OperationEntry> get recentOperations => List.unmodifiable(_recent);

  /// The latest activity sample, or `null` before the first one arrives.
  tagsy.ActivityEntry? get activity => _activity;

  /// Whether the daemon is doing anything: an operation runs, or activity has
  /// not yet been idle for two samples in a row.
  bool get busy => _activityBusy || activeOperations.isNotEmpty;

  /// Start watching. Idempotent.
  void start() {
    if (_running) return;
    _running = true;
    unawaited(_watchConnections());
    unawaited(_watchOperations());
    unawaited(_poll());
  }

  @override
  void dispose() {
    _running = false;
    _pollTimer?.cancel();
    super.dispose();
  }

  void _notify() {
    if (_running) notifyListeners();
  }

  Future<void> _watchConnections() async {
    try {
      await _snapshotPeers();
      final next = await _source.connectionUpdates();
      while (_running) {
        final update = await next();
        if (update == null || !_running) break;
        switch (update) {
          case tagsy.ConnectionUpdateDto_Resynced():
            await _snapshotPeers();
          case tagsy.ConnectionUpdateDto_Connected(:final peer):
            _peers[peer.publicKey] = peer;
            _notify();
          case tagsy.ConnectionUpdateDto_Disconnected(:final publicKey):
            _peers.remove(publicKey);
            _notify();
        }
      }
    } catch (_) {
      // A dropped stream leaves the last known state; the next app start
      // re-snapshots.
    }
  }

  Future<void> _snapshotPeers() async {
    final snapshot = await _source.connectedPeers();
    _peers
      ..clear()
      ..addEntries(snapshot.map((peer) => MapEntry(peer.publicKey, peer)));
    _notify();
  }

  Future<void> _watchOperations() async {
    try {
      await _snapshotOperations();
      final next = await _source.operationUpdates();
      while (_running) {
        final update = await next();
        if (update == null || !_running) break;
        switch (update) {
          case tagsy.OperationUpdateDto_Resynced():
            await _snapshotOperations();
          case tagsy.OperationUpdateDto_Started(:final operation):
          case tagsy.OperationUpdateDto_Updated(:final operation):
            _applyOperation(operation);
            _notify();
        }
      }
    } catch (_) {
      // See `_watchConnections`.
    }
  }

  Future<void> _snapshotOperations() async {
    final snapshot = await _source.listOperations();
    _active.clear();
    snapshot.forEach(_applyOperation);
    _notify();
  }

  void _applyOperation(tagsy.OperationEntry operation) {
    if (operation.status is tagsy.OperationStatusDto_Active) {
      _active[operation.id] = operation;
      return;
    }
    _active.remove(operation.id);
    if (operation.kind == kConnectingKind) return;
    _recent
      ..removeWhere((op) => op.id == operation.id)
      ..insert(0, operation);
    if (_recent.length > recentLimit) {
      _recent.removeRange(recentLimit, _recent.length);
    }
  }

  Future<void> _poll() async {
    if (!_running) return;
    try {
      final sample = await _source.activity();
      if (!_running) return;
      final wasBusy = _activityBusy;
      if (sample.idle) {
        _idleSamples += 1;
        if (_idleSamples >= 2) _activityBusy = false;
      } else {
        _idleSamples = 0;
        _activityBusy = true;
      }
      if (sample != _activity || wasBusy != _activityBusy) {
        _activity = sample;
        _notify();
      }
    } catch (_) {
      // Keep the last sample; try again next tick.
    }
    if (_running) _pollTimer = Timer(pollInterval, _poll);
  }
}
