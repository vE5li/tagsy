// The home AppBar's peer-connection indicator: a calm, always-present readout
// of link health, distinct from the overflow menu's "work in flight" badge.
//
// A peer connection is *state*, not an operation (see the daemon's
// `connections` module and `tagsy-bridge`'s `ConnectedPeerDto`), so it has its
// own snapshot + stream — `connectedPeers()` / `subscribeConnections()` — that
// this widget consumes. It renders one of three states:
//
//   - idle      (no peers, no attempt): muted link-off icon
//   - connecting (a connect attempt in flight): amber sync icon
//   - connected  (>=1 peer): green link icon
//
// The state is conveyed by icon + color alone (no count badge); tapping the
// indicator opens a sheet with the peer list for the details.
//
// The "connecting" state is derived from the *operation* stream: a connect
// attempt is genuinely an operation (`connecting_to_peer`), so we watch that
// stream too and light the amber state while any such op is active. That is the
// one remaining place the UI reads an operation kind string; it is guarded by
// `test/operation_labels_test.dart`'s bridge-kind set.
//
// Tapping the indicator opens a sheet listing the connected peers.

import 'package:flutter/material.dart';

import '../../rust/api.dart' as tagsy;
import '../../session/session.dart';

/// Machine kind string (see `flatten_kind` in tagsy-bridge) for an outbound
/// connect attempt. Kept in one place so the dependency is easy to find.
const String _kConnectingKind = 'connecting_to_peer';

class ConnectionIndicator extends StatefulWidget {
  const ConnectionIndicator({super.key, required this.session});

  final TagsySession? session;

  @override
  State<ConnectionIndicator> createState() => _ConnectionIndicatorState();
}

class _ConnectionIndicatorState extends State<ConnectionIndicator> {
  /// Currently-connected peers, keyed by public key so a `Disconnected` event
  /// drops the matching row and a re-`Connected` replaces it in place.
  final Map<String, tagsy.ConnectedPeerDto> _peers = {};

  /// Ids of active `connecting_to_peer` operations, so the indicator shows the
  /// amber "connecting" state while any attempt is in flight.
  final Set<BigInt> _connecting = {};

  bool _watching = false;

  @override
  void initState() {
    super.initState();
    if (widget.session != null) _watch();
  }

  @override
  void didUpdateWidget(covariant ConnectionIndicator oldWidget) {
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

  Future<void> _watch() async {
    final session = widget.session;
    if (session == null || _watching) return;
    _watching = true;
    // Two independent streams feed this widget; run both loops concurrently.
    _watchConnections(session);
    _watchConnectingOperations(session);
  }

  Future<void> _watchConnections(TagsySession session) async {
    try {
      // Seed from a snapshot so an already-live link shows immediately, then
      // apply connect/disconnect edges on top.
      final snapshot = await session.repository.connectedPeers();
      if (!mounted) return;
      setState(() {
        _peers
          ..clear()
          ..addEntries(snapshot.map((p) => MapEntry(p.publicKey, p)));
      });

      final updates = await session.repository.subscribeConnections();
      while (mounted && _watching) {
        final update = await updates.next();
        if (update == null) break;
        if (!mounted) break;
        switch (update) {
          case tagsy.ConnectionUpdateDto_Resynced():
            final refreshed = await session.repository.connectedPeers();
            if (!mounted) break;
            setState(() {
              _peers
                ..clear()
                ..addEntries(refreshed.map((p) => MapEntry(p.publicKey, p)));
            });
          case tagsy.ConnectionUpdateDto_Connected(:final peer):
            setState(() => _peers[peer.publicKey] = peer);
          case tagsy.ConnectionUpdateDto_Disconnected(:final publicKey):
            setState(() => _peers.remove(publicKey));
        }
      }
    } catch (_) {
      // Transient stream hiccups are surfaced elsewhere; don't kill the widget.
    }
  }

  Future<void> _watchConnectingOperations(TagsySession session) async {
    try {
      final snapshot = await session.repository.listOperations();
      if (!mounted) return;
      setState(() {
        _connecting
          ..clear()
          ..addAll(snapshot.where(_isActiveConnecting).map((op) => op.id));
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
              _connecting
                ..clear()
                ..addAll(
                  refreshed.where(_isActiveConnecting).map((op) => op.id),
                );
            });
          case tagsy.OperationUpdateDto_Started(:final operation):
            setState(() => _applyOperation(operation));
          case tagsy.OperationUpdateDto_Updated(:final operation):
            setState(() => _applyOperation(operation));
        }
      }
    } catch (_) {
      // Ignore: the connection stream alone still drives the connected state.
    }
  }

  /// Whether an operation is an in-flight connect *attempt*.
  static bool _isActiveConnecting(tagsy.OperationEntry op) =>
      op.kind == _kConnectingKind &&
      op.status is tagsy.OperationStatusDto_Active;

  /// Track or drop `op` from the in-flight connect-attempt set.
  void _applyOperation(tagsy.OperationEntry op) {
    if (_isActiveConnecting(op)) {
      _connecting.add(op.id);
    } else {
      _connecting.remove(op.id);
    }
  }

  void _showPeers() {
    final peers = _peers.values.toList()
      ..sort((a, b) => a.peerName.compareTo(b.peerName));
    FocusManager.instance.primaryFocus?.unfocus();
    showModalBottomSheet<void>(
      context: context,
      builder: (context) => SafeArea(
        child: peers.isEmpty
            ? const ListTile(
                leading: Icon(Icons.link_off),
                title: Text('No peers connected'),
              )
            : ListView(
                shrinkWrap: true,
                children: [
                  for (final peer in peers)
                    ListTile(
                      leading: Icon(
                        peer.direction ==
                                tagsy.ConnectionDirectionDto.outbound
                            ? Icons.call_made
                            : Icons.call_received,
                      ),
                      title: Text(peer.peerName),
                      subtitle: Text(
                        peer.direction ==
                                tagsy.ConnectionDirectionDto.outbound
                            ? 'outbound'
                            : 'inbound',
                      ),
                    ),
                ],
              ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    if (widget.session == null) return const SizedBox.shrink();

    final count = _peers.length;
    final connecting = _connecting.isNotEmpty;
    final theme = Theme.of(context);

    final (IconData icon, Color color, String tooltip) = count > 0
        ? (
            Icons.link,
            Colors.green,
            count == 1 ? '1 peer connected' : '$count peers connected',
          )
        : connecting
        ? (Icons.sync, Colors.amber.shade700, 'Connecting…')
        : (
            Icons.link_off,
            theme.disabledColor,
            'No peers connected',
          );

    return IconButton(
      tooltip: tooltip,
      onPressed: _showPeers,
      icon: Icon(icon, color: color),
    );
  }
}
