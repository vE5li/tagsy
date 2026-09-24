// Everything the status indicator watches, as one narrow interface.
//
// [TagsyRepository] implements it against the Rust bridge. It exists so the
// status model and its widgets can be driven by a plain Dart fake in tests,
// without the bridge's opaque subscription handles: a live stream is exposed
// as its `next()` function.

import '../rust/api.dart' as tagsy;

/// Pull the next update from a live stream; `null` once the stream has ended.
typedef NextUpdate<T> = Future<T?> Function();

abstract interface class StatusSource {
  /// Snapshot of the peers we hold a live session with.
  Future<List<tagsy.ConnectedPeerDto>> connectedPeers();

  /// Live connect/disconnect updates.
  Future<NextUpdate<tagsy.ConnectionUpdateDto>> connectionUpdates();

  /// Snapshot of the currently-active sync operations.
  Future<List<tagsy.OperationEntry>> listOperations();

  /// Live operation updates (started / progress / terminal).
  Future<NextUpdate<tagsy.OperationUpdateDto>> operationUpdates();

  /// A sample of the daemon's actor activity. Snapshot only: poll it.
  Future<tagsy.ActivityEntry> activity();
}
