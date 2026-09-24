// The home AppBar's status button: the one place for peer connections, sync
// operations, and daemon activity.
//
// The icon shows the peer state — green link (connected), amber sync
// (connecting), muted link-off (alone). A thin ring spins around it while the
// daemon is busy (see [StatusModel.busy]), and a small badge counts the sync
// operations in flight. Tapping it opens a live sheet with the details
// ([StatusPanel]).

import 'package:flutter/material.dart';

import '../../data/status_source.dart';
import 'status_model.dart';
import 'status_panel.dart';

class StatusIndicator extends StatefulWidget {
  const StatusIndicator({super.key, required this.source});

  /// What to watch; `null` until the backend has connected.
  final StatusSource? source;

  @override
  State<StatusIndicator> createState() => _StatusIndicatorState();
}

class _StatusIndicatorState extends State<StatusIndicator> {
  StatusModel? _model;

  @override
  void initState() {
    super.initState();
    _attach();
  }

  @override
  void didUpdateWidget(covariant StatusIndicator oldWidget) {
    super.didUpdateWidget(oldWidget);
    // The backend arrives asynchronously after bootstrap.
    if (oldWidget.source != widget.source) {
      _model?.dispose();
      _model = null;
      _attach();
    }
  }

  void _attach() {
    final source = widget.source;
    if (source == null) return;
    _model = StatusModel(source)..start();
  }

  @override
  void dispose() {
    _model?.dispose();
    super.dispose();
  }

  void _openSheet(StatusModel model) {
    FocusManager.instance.primaryFocus?.unfocus();
    showModalBottomSheet<void>(
      context: context,
      isScrollControlled: true,
      showDragHandle: true,
      builder: (context) => DraggableScrollableSheet(
        expand: false,
        initialChildSize: 0.5,
        minChildSize: 0.3,
        maxChildSize: 0.9,
        builder: (context, scrollController) =>
            StatusPanel(model: model, scrollController: scrollController),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final model = _model;
    if (model == null) return const SizedBox.shrink();
    return ListenableBuilder(
      listenable: model,
      builder: (context, _) {
        final theme = Theme.of(context);
        final peerCount = model.peers.length;
        final (IconData icon, Color color, String peerText) = peerCount > 0
            ? (
                Icons.link,
                Colors.green,
                peerCount == 1
                    ? '1 peer connected'
                    : '$peerCount peers connected',
              )
            : model.connecting
            ? (Icons.sync, Colors.amber.shade700, 'Connecting…')
            : (Icons.link_off, theme.disabledColor, 'No peers connected');
        final running = model.activeOperations.length;
        final tooltip = model.busy
            ? '$peerText · syncing'
            : '$peerText · up to date';

        return IconButton(
          tooltip: tooltip,
          onPressed: () => _openSheet(model),
          icon: SizedBox(
            width: 32,
            height: 32,
            child: Stack(
              alignment: Alignment.center,
              clipBehavior: Clip.none,
              children: [
                if (model.busy)
                  SizedBox.expand(
                    child: CircularProgressIndicator(
                      key: const ValueKey('status-busy'),
                      strokeWidth: 2,
                      color: color,
                    ),
                  ),
                Icon(icon, color: color, size: 20),
                if (running > 0)
                  Positioned(top: -4, right: -6, child: _Badge(count: running)),
              ],
            ),
          ),
        );
      },
    );
  }
}

class _Badge extends StatelessWidget {
  const _Badge({required this.count});

  final int count;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    return IgnorePointer(
      child: Container(
        padding: const EdgeInsets.symmetric(horizontal: 4, vertical: 1),
        constraints: const BoxConstraints(minWidth: 16),
        decoration: BoxDecoration(
          color: scheme.primary,
          borderRadius: BorderRadius.circular(8),
        ),
        child: Text(
          '$count',
          textAlign: TextAlign.center,
          style: TextStyle(
            color: scheme.onPrimary,
            fontSize: 10,
            fontWeight: FontWeight.bold,
          ),
        ),
      ),
    );
  }
}
