// The single source for how a sync operation's machine `kind` string maps to
// an icon and a human label.
//
// The daemon's `OperationKind` is flattened to a stable string at the FFI
// boundary (`flatten_kind` in tagsy-bridge/src/api.rs); the operations screen
// then switches on that string to pick an icon and label. Those were two
// parallel `switch`es that silently fell through to a generic
// icon/raw-string if a new kind was added on the Rust side. This one table
// replaces both, and `test/operation_labels_test.dart` asserts it covers every
// kind the bridge can emit — so a drift becomes a failing test, not a silent
// `Icons.pending`.

import 'package:flutter/material.dart';

/// Icon + human label for one operation kind.
typedef OperationLabel = (IconData icon, String label);

/// Maps each machine `kind` string (see `flatten_kind` in the bridge) to its
/// icon and display label.
///
/// The keys MUST stay in sync with the strings `flatten_kind` produces; the
/// test enumerates the expected set and fails if this map drifts from it.
const Map<String, OperationLabel> kOperationLabels = {
  'connecting_to_peer': (Icons.sync, 'Connecting to peer'),
  'peer_connected_outbound': (Icons.link, 'Connected (outbound)'),
  'peer_connected_inbound': (Icons.link, 'Connected (inbound)'),
  'receiving_file': (Icons.download, 'Receiving file'),
  'fetching': (Icons.cloud_download, 'Fetching file'),
  'reconciling_manifest': (Icons.compare_arrows, 'Reconciling manifest'),
  'reconciling_tags': (Icons.compare_arrows, 'Reconciling tags'),
  'placing_file': (Icons.place, 'Placing file'),
};

/// The icon for [kind], or [Icons.pending] for an unknown kind (a kind the
/// bridge added that this table hasn't caught up with — the test guards against
/// this in CI, but the UI degrades gracefully at runtime).
IconData iconForOperationKind(String kind) =>
    kOperationLabels[kind]?.$1 ?? Icons.pending;

/// The human label for [kind], falling back to the raw [kind] string when
/// unknown (see [iconForOperationKind]).
String labelForOperationKind(String kind) => kOperationLabels[kind]?.$2 ?? kind;
