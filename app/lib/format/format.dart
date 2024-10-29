// Shared, dependency-free display/path formatters used across screens, kept
// here so every surface renders the same value the same way (e.g. the file
// detail screen and the top-bar storage indicator both show byte sizes
// identically, and every share/download path derivation agrees on the logical
// name).

import 'dart:io';

/// Format a byte count as a human-readable size (binary units: KiB, MiB, …).
/// Bytes are shown as a plain count; larger sizes use one decimal place.
String formatSize(int bytes) {
  if (bytes < 1024) {
    return '$bytes B';
  }
  const units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  var value = bytes / 1024;
  var unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit++;
  }
  return '${value.toStringAsFixed(1)} ${units[unit]}';
}

/// Derive a display/logical name from a path: its last `/`-separated segment.
/// Matches the engine's ingestion-boundary convention (a nested `foo/bar.png`
/// becomes just `bar.png`).
String nameFor(String path) => path.split('/').last;

/// Build a destination path in [dir] for [name] that does not collide with an
/// existing file, inserting ` (n)` before the extension as needed
/// (`report.pdf` -> `report (2).pdf`).
String uniqueDestination(String dir, String name) {
  if (!File('$dir/$name').existsSync()) return '$dir/$name';
  final dot = name.lastIndexOf('.');
  final stem = dot <= 0 ? name : name.substring(0, dot);
  final ext = dot <= 0 ? '' : name.substring(dot);
  for (var n = 2; ; n++) {
    final candidate = '$dir/$stem ($n)$ext';
    if (!File(candidate).existsSync()) return candidate;
  }
}
