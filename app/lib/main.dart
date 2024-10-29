// Shared entrypoint for BOTH the tagsy Android and Linux desktop apps.
//
// The two apps differ only in how they connect to the backend (Android starts
// an in-process engine; Linux attaches to a daemon over IPC). That difference
// is isolated in bootstrap/*_bootstrap.dart behind the TagsyBootstrap
// contract, and everything else — the whole UI — is shared (app.dart,
// screens/).
//
// The backend is chosen at build time with a compile-time define:
//
//   flutter run --dart-define=TAGSY_BACKEND=android   # (default)
//   flutter run --dart-define=TAGSY_BACKEND=linux -d linux
//
// The flake's run-android / run-linux apps pass the right value.

import 'package:flutter/material.dart';

import 'app.dart';
import 'bootstrap/bootstrap.dart';
import 'bootstrap/android_bootstrap.dart' as android;
import 'bootstrap/linux_bootstrap.dart' as linux;

// Both bootstraps are imported unconditionally even though only one runs per
// build. This is deliberate and cheap:
//
//   * `_backend` is a compile-time `const`, so `_selectBootstrap`'s switch has
//     one reachable arm per build. The other `createBootstrap()` factory — and
//     everything only it references (e.g. Android's `share_review_screen`,
//     Linux's `.so` loader) — is unreachable Dart and is dropped by the
//     release AOT tree-shaker. The dead branch costs no app-code size.
//   * The one residual is native *plugin registration*: `receive_sharing_intent`
//     has a Linux stub that the generated plugin registrant links regardless of
//     `_backend`. It is a no-op stub, a few KB, and cannot be conditionally
//     excluded from Dart — plugin selection is a build-system (pubspec platform
//     support) concern, not a Dart-import one.
//
// Conditional imports can't help here: Dart only keys them on `dart.library.*`,
// and both Android and Linux desktop are `dart.library.io`, so there is no
// library predicate that distinguishes the two backends. Truly compiling only
// one bootstrap would require split entrypoints (main_android.dart /
// main_linux.dart) wired through the flake's run targets — not worth it for a
// no-op stub. Leave both imports.

/// Backend id baked in at build time; defaults to Android.
const String _backend = String.fromEnvironment(
  'TAGSY_BACKEND',
  defaultValue: 'android',
);

TagsyBootstrap _selectBootstrap() {
  switch (_backend) {
    case 'linux':
      return linux.createBootstrap();
    case 'android':
      return android.createBootstrap();
    default:
      throw StateError(
        'Unknown TAGSY_BACKEND "$_backend" '
        '(expected "android" or "linux").',
      );
  }
}

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  // RustLib.init() is done inside each bootstrap's connect(), because Linux
  // needs a custom library loader and Android uses the default.
  runApp(TagsyAppRoot(bootstrap: _selectBootstrap()));
}
