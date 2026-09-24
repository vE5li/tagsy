// The body of the status sheet: peers, activity, and operations in one live
// view. Rebuilds on every [StatusModel] change, so progress bars move and rows
// appear/disappear while the sheet is open.

import 'package:flutter/material.dart';

import '../../format/format.dart';
import '../../format/operation_labels.dart';
import '../../rust/api.dart' as tagsy;
import '../../widgets/section_header.dart';
import 'status_model.dart';

class StatusPanel extends StatelessWidget {
  const StatusPanel({super.key, required this.model, this.scrollController});

  final StatusModel model;
  final ScrollController? scrollController;

  @override
  Widget build(BuildContext context) {
    return ListenableBuilder(
      listenable: model,
      builder: (context, _) {
        final active = model.activeOperations;
        final recent = model.recentOperations;
        return ListView(
          controller: scrollController,
          padding: const EdgeInsets.only(bottom: 16),
          children: [
            const SectionHeader('Peers'),
            ..._peerRows(context),
            const SectionHeader('Activity'),
            ..._activityRows(context),
            const SectionHeader('Operations'),
            if (active.isEmpty)
              const ListTile(dense: true, title: Text('Nothing running')),
            for (final operation in active) OperationTile(operation: operation),
            if (recent.isNotEmpty) ...[
              const SectionHeader('Recently finished'),
              for (final operation in recent)
                OperationTile(operation: operation),
            ],
          ],
        );
      },
    );
  }

  List<Widget> _peerRows(BuildContext context) {
    final peers = model.peers;
    if (peers.isEmpty) {
      return [
        ListTile(
          dense: true,
          leading: Icon(model.connecting ? Icons.sync : Icons.link_off),
          title: Text(model.connecting ? 'Connecting…' : 'No peers connected'),
        ),
      ];
    }
    return [
      for (final peer in peers)
        ListTile(
          dense: true,
          leading: Icon(
            peer.direction == tagsy.ConnectionDirectionDto.outbound
                ? Icons.call_made
                : Icons.call_received,
            color: Colors.green,
          ),
          title: Text(peer.peerName),
          subtitle: Text(
            '${peer.direction == tagsy.ConnectionDirectionDto.outbound ? 'outbound' : 'inbound'}'
            '  ·  connected ${_since(peer.since)}',
          ),
        ),
    ];
  }

  List<Widget> _activityRows(BuildContext context) {
    final activity = model.activity;
    final busy = model.busy;
    final headline = ListTile(
      dense: true,
      leading: busy
          ? const SizedBox(
              width: 24,
              height: 24,
              child: Padding(
                padding: EdgeInsets.all(3),
                child: CircularProgressIndicator(strokeWidth: 2),
              ),
            )
          : const Icon(Icons.check_circle_outline, color: Colors.green),
      title: Text(busy ? 'Syncing…' : 'Up to date'),
    );
    if (activity == null) return [headline];
    return [
      headline,
      for (final detail in activityDetails(activity)) _detail(detail),
    ];
  }

  Widget _detail(String text) => ListTile(
    dense: true,
    visualDensity: VisualDensity.compact,
    contentPadding: const EdgeInsets.only(left: 72, right: 16),
    title: Text(text),
  );
}

/// The human-readable parts of an activity sample that are not idle — what
/// the daemon is still working through. Empty when everything is idle.
List<String> activityDetails(tagsy.ActivityEntry activity) {
  final details = <String>[];
  if (!activity.initialScanComplete) {
    details.add('Scanning sync directories');
  }
  final catalog = activity.catalog;
  if (catalog.busy || catalog.queued > 0) {
    details.add(_queued('Applying catalog changes', catalog.queued));
  }
  final directories = activity.syncDirectories;
  if (directories.busy || directories.queued > 0) {
    details.add(_queued('Updating sync directories', directories.queued));
  }
  if (activity.pendingFilesystemEvents > 0) {
    final count = activity.pendingFilesystemEvents;
    details.add('$count file change${count == 1 ? '' : 's'} settling');
  }
  if (activity.pullsRunning > 0 || activity.pullsQueued > 0) {
    details.add(
      'Transfers: ${activity.pullsRunning} running, ${activity.pullsQueued} queued',
    );
  }
  final sessions = activity.peerSessions;
  if (sessions.busy > 0 || sessions.outboundQueued > 0) {
    details.add(_queued('Exchanging with peers', sessions.outboundQueued));
  }
  return details;
}

String _queued(String label, int queued) =>
    queued > 0 ? '$label ($queued queued)' : label;

/// "just now" / "5 min ago" / "3 h ago" / "2 d ago" for a wall-clock
/// millisecond timestamp.
String _since(int millis) {
  final elapsed = DateTime.now().difference(
    DateTime.fromMillisecondsSinceEpoch(millis),
  );
  if (elapsed.inMinutes < 1) return 'just now';
  if (elapsed.inHours < 1) return '${elapsed.inMinutes} min ago';
  if (elapsed.inDays < 1) return '${elapsed.inHours} h ago';
  return '${elapsed.inDays} d ago';
}

/// Operation kinds whose progress counts bytes; the startup scan counts files.
const Set<String> _byteKinds = {'receiving_file', 'fetching', 'placing_file'};

/// One operation: icon and label by kind, the peer/file it concerns, its
/// status, and a progress bar while it runs.
class OperationTile extends StatelessWidget {
  const OperationTile({super.key, required this.operation});

  final tagsy.OperationEntry operation;

  @override
  Widget build(BuildContext context) {
    final status = operation.status;
    final progress = _progress();
    return ListTile(
      dense: true,
      leading: Icon(iconForOperationKind(operation.kind)),
      title: Text(labelForOperationKind(operation.kind)),
      trailing: switch (status) {
        tagsy.OperationStatusDto_Completed() => const Icon(
          Icons.check,
          color: Colors.green,
        ),
        tagsy.OperationStatusDto_Failed() => Icon(
          Icons.error_outline,
          color: Theme.of(context).colorScheme.error,
        ),
        tagsy.OperationStatusDto_Aborted() => const Icon(Icons.block),
        tagsy.OperationStatusDto_Active() => null,
      },
      subtitle: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(_subtitle()),
          if (status is tagsy.OperationStatusDto_Active &&
              operation.progressDone != null)
            Padding(
              padding: const EdgeInsets.only(top: 4),
              child: LinearProgressIndicator(value: progress),
            ),
        ],
      ),
    );
  }

  /// `done / total`, or `null` (indeterminate) when the total is unknown.
  double? _progress() {
    final done = operation.progressDone;
    final total = operation.progressTotal;
    if (done == null || total == null || total == BigInt.zero) return null;
    return (done.toDouble() / total.toDouble()).clamp(0.0, 1.0);
  }

  /// Progress in the operation's unit: sizes for transfers, a file count for
  /// the scan.
  String _amount(BigInt done, BigInt? total) {
    if (_byteKinds.contains(operation.kind)) {
      final doneText = formatSize(done.toInt());
      return total == null
          ? doneText
          : '$doneText / ${formatSize(total.toInt())}';
    }
    final noun = operation.kind == 'scanning_sync_directories' ? ' files' : '';
    return total == null ? '$done$noun' : '$done / $total$noun';
  }

  String _subtitle() {
    final parts = <String>[];
    final peer = operation.peerName;
    if (peer != null) parts.add(peer);
    final file = operation.fileId;
    if (file != null) {
      parts.add('file ${file.length > 8 ? file.substring(0, 8) : file}');
    }
    switch (operation.status) {
      case tagsy.OperationStatusDto_Active():
        final done = operation.progressDone;
        if (done != null) parts.add(_amount(done, operation.progressTotal));
      case tagsy.OperationStatusDto_Completed():
        parts.add('completed');
      case tagsy.OperationStatusDto_Failed(:final reason):
        parts.add('failed: $reason');
      case tagsy.OperationStatusDto_Aborted():
        parts.add('aborted');
    }
    return parts.join('  ·  ');
  }
}
