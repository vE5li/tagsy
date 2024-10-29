// AppBar indicator showing `<local>/<total>` storage: how much data this
// device holds on disk versus how much the whole catalog knows about. Both
// figures price the latest version of each live file. Refreshes on connect and
// on every catalog change event; renders nothing until the first fetch lands.

import 'package:flutter/material.dart';

import '../../format/format.dart';
import '../../rust/api.dart' as tagsy;
import '../../screens/storage_stats_screen.dart';
import '../../session/session.dart';

/// The home AppBar's tappable storage indicator; opens [StorageStatsScreen].
class StorageStatsIndicator extends StatefulWidget {
  const StorageStatsIndicator({super.key, required this.session});

  final TagsySession? session;

  @override
  State<StorageStatsIndicator> createState() => _StorageStatsIndicatorState();
}

class _StorageStatsIndicatorState extends State<StorageStatsIndicator> {
  tagsy.StorageStatsEntry? _stats;
  bool _watching = false;

  @override
  void initState() {
    super.initState();
    if (widget.session != null) _subscribeToChanges();
  }

  @override
  void didUpdateWidget(covariant StorageStatsIndicator oldWidget) {
    super.didUpdateWidget(oldWidget);
    // The session arrives asynchronously after connect; start watching once it
    // first appears.
    if (oldWidget.session == null && widget.session != null)
      _subscribeToChanges();
  }

  @override
  void dispose() {
    _watching = false;
    super.dispose();
  }

  Future<void> _refresh() async {
    final session = widget.session;
    if (session == null) return;
    try {
      final stats = await session.repository.storageStats();
      if (!mounted) return;
      setState(() => _stats = stats);
    } catch (_) {
      // Transient failures keep the last shown value; the change stream will
      // trigger another refresh soon.
    }
  }

  Future<void> _subscribeToChanges() async {
    final session = widget.session;
    if (session == null || _watching) return;
    _watching = true;
    // Paint an initial value, then re-fetch only when stored bytes can have
    // moved (see `_affectsStorage`).
    await _refresh();
    try {
      final events = await session.repository.subscribe();
      while (mounted && _watching) {
        final event = await events.next();
        if (event == null) break;
        if (!mounted) break;
        if (_affectsStorage(event)) await _refresh();
      }
    } catch (_) {
      // Stream errors are surfaced elsewhere (bootstrap) — ignore here so a
      // transient hiccup doesn't kill the indicator.
    }
  }

  /// Storage totals move only when file *bytes* are added, changed, deleted or
  /// restored — every one of which arrives as `FileChanged`. Tag edits never
  /// change stored size. `Resynced` reloads (a missed change may have moved the
  /// totals). Mirrors `StorageStatsScreen._affectsStorage`.
  bool _affectsStorage(tagsy.ApiEventDto event) => switch (event) {
    tagsy.ApiEventDto_Resynced() => true,
    tagsy.ApiEventDto_FileChanged() => true,
    tagsy.ApiEventDto_TagChanged() => false,
    tagsy.ApiEventDto_FileTagChanged() => false,
    tagsy.ApiEventDto_TagTagChanged() => false,
    tagsy.ApiEventDto_ProviderReleased() => false,
  };

  @override
  Widget build(BuildContext context) {
    final stats = _stats;
    final session = widget.session;
    if (stats == null || session == null) return const SizedBox.shrink();
    final local = formatSize(stats.localBytes.toInt());
    final total = formatSize(stats.totalBytes.toInt());
    return InkWell(
      onTap: () {
        FocusManager.instance.primaryFocus?.unfocus();
        Navigator.push(
          context,
          MaterialPageRoute(
            builder: (_) => StorageStatsScreen(session: session),
          ),
        );
      },
      child: Center(
        child: Padding(
          padding: const EdgeInsets.symmetric(horizontal: 12),
          child: Text(
            '$local / $total',
            style: Theme.of(context).textTheme.bodyMedium,
          ),
        ),
      ),
    );
  }
}
