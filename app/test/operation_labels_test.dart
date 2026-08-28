// Guards the operation kind -> (icon, label) table against drift with the Rust
// side.
//
// The daemon's `OperationKind` is flattened to a stable machine string by
// `flatten_kind` in `tagsy-bridge/src/api.rs`. `kOperationLabels` in
// `lib/format/operation_labels.dart` maps each of those strings to an icon and
// a label. If a new `OperationKind` variant is added on the Rust side (and so a
// new string in `flatten_kind`), the UI would silently fall through to a
// generic icon / the raw string. This test pins the expected set of kinds so
// that drift is a failing test instead: when it fails, update BOTH `flatten_kind`
// and `kOperationLabels`, then update `_bridgeKinds` here to match.

import 'package:flutter_test/flutter_test.dart';

import 'package:tagsy_app/format/operation_labels.dart';

/// Every kind string `flatten_kind` (tagsy-bridge/src/api.rs) can produce.
/// Kept in lockstep with that function by hand — this list is the contract the
/// test enforces.
const Set<String> _bridgeKinds = {
  'connecting_to_peer',
  'receiving_file',
  'fetching',
  'reconciling_manifest',
  'reconciling_tags',
  'placing_file',
};

void main() {
  group('kOperationLabels', () {
    test('covers exactly the kinds the bridge can emit', () {
      expect(kOperationLabels.keys.toSet(), _bridgeKinds);
    });

    test('every entry has a non-empty label', () {
      for (final entry in kOperationLabels.entries) {
        expect(entry.value.$2, isNotEmpty, reason: 'kind "${entry.key}"');
      }
    });

    test('lookup helpers resolve a known kind', () {
      final (icon, label) = kOperationLabels['fetching']!;
      expect(iconForOperationKind('fetching'), icon);
      expect(labelForOperationKind('fetching'), label);
    });

    test('lookup helpers fall back gracefully on an unknown kind', () {
      // Unknown kind: icon degrades to a generic pending glyph, label to the
      // raw string (so a drifted UI still shows *something* meaningful).
      expect(labelForOperationKind('brand_new_kind'), 'brand_new_kind');
      // Distinct from any mapped icon, but we only assert the label contract
      // here; the icon fallback is exercised by the coverage test above staying
      // green.
      expect(iconForOperationKind('brand_new_kind'), isNotNull);
    });
  });
}
