// Linux desktop launcher: spawn the editor as a child process and await
// its exit.
//
// The editor to spawn is resolved by consulting the daemon-configured
// query-based rules (first rule whose `query` matches the file wins). Each
// rule's match is decided by the daemon, not here: we compose
// `/i <file-id> <rule.query>` and run it through the normal query path, and
// the rule matches when that yields a (single) result. The `/i` term goes
// first so it stays a complete token even if the operator's query ends in an
// unclosed `"` or `%`.
// There is deliberately **no `$VISUAL`/`$EDITOR` fallback**: those env vars
// name terminal editors (`vi`, `nvim`, `nano`), and running one from a GUI
// process with no controlling TTY hangs forever — the child neither errors
// out nor produces output, so `Process.run` never returns. The CLI's
// `open_in_editor` uses that env-var chain safely because it runs inside a
// terminal already; the desktop app cannot. If no rule matches, we surface a
// clear error instead of silently invoking something that will misbehave.
//
// The rule's `argv` is passed straight to `Process.run` with the file path
// appended as the final argument. The command must run in the foreground and
// exit when the user is done editing — the process exit is our "editing
// finished" signal. A backgrounding launcher (`xdg-open`, `nohup`,
// `gtk-launch`) would return immediately and confuse that signal; those are
// not supported.
//
// ---------------------------------------------------------------------------
// Security model
// ---------------------------------------------------------------------------
//
// An editor rule is arbitrary code execution by design — "run this program" is
// the whole feature. The trust anchor is therefore **write access to the
// daemon's config file**, and nothing else: rules are read once at startup
// from local config, are never synced from peers or persisted to the database,
// and have no runtime setter. The three properties below keep that the *only*
// way to influence what runs.
//
// 1. No shell, ever. `runInShell: false` means Dart goes straight to `execvp`
//    with an argv array. `;`, `$(...)`, backticks and globs have no meaning,
//    and the file path — which is peer-influenced data, since a peer chooses
//    the filename it advertises — is passed as one discrete argv element that
//    can never be re-parsed as syntax.
//
// 2. `argv[0]` must be absolute. A bare name like `gimp` would be resolved
//    through `PATH` inherited from this process, so anyone able to prepend a
//    directory to `PATH` could substitute the editor. Requiring an absolute
//    path removes the lookup entirely.
//
//    This is a deliberate ergonomics trade-off, and it is a cheap one on
//    NixOS, the target platform: executables live at immutable `/nix/store`
//    paths surfaced through stable symlink trees
//    (`/run/current-system/sw/bin/gimp`, `~/.nix-profile/bin/inkscape`), so an
//    absolute path is both the natural thing to write and stable across
//    rebuilds. On a distro where binaries move between `/bin` and `/usr/bin`
//    this rule would be more annoying; here it costs essentially nothing. If
//    this ever needs relaxing, resolve bare names against a *hardcoded* PATH
//    rather than the inherited one — do not simply drop the check.
//
// 3. The child gets a scrubbed environment. See [_childEnvironment].
//
// Note also that the file path can never be parsed as an option flag by the
// editor: we always pass it through `.absolute`, so it begins with `/` even if
// the peer-chosen filename begins with `-`. (tagsy-core deliberately permits
// leading-dash filenames, since they are legal on Linux — see
// `validate_relative_path` — so this normalization is what actually closes the
// argument-injection case, not a restriction upstream.)

import 'dart:io';

import 'package:flutter/foundation.dart' show visibleForTesting;

import '../rust/api.dart' as tagsy;
import 'editor_launcher.dart';

class LinuxEditorLauncher implements EditorLauncher {
  /// [runMatches] answers "does this composed query yield any file?" — it is
  /// the seam to the daemon's query path (see [EditorLauncher.launchAndWait]),
  /// kept as a callback so [resolveArgv] can be unit-tested without a live
  /// bridge. Bootstrap wires it to the repository's `runQuery`.
  LinuxEditorLauncher({required this.runMatches});

  final Future<bool> Function(String query) runMatches;

  @override
  Future<void> launchAndWait({
    required String path,
    required String logicalName,
    required String fileId,
    required List<tagsy.EditorRuleEntry> rules,
  }) async {
    final argv = await resolveArgv(
      fileId: fileId,
      rules: rules,
      runMatches: runMatches,
    );

    // Convention matches the CLI's `open_in_editor` (tagsy/src/main.rs):
    // path is the last argument. Users writing rules can rely on it.
    //
    // `.absolute` is belt-and-braces: the daemon already returns an absolute
    // path, but if that ever changed, a relative path here would be resolved
    // against the *app's* working directory rather than the user's, silently
    // editing the wrong file.
    final args = [...argv.skip(1), File(path).absolute.path];

    final ProcessResult result;
    try {
      result = await Process.run(
        argv[0],
        args,
        runInShell: false,
        includeParentEnvironment: false,
        environment: _childEnvironment(),
      );
    } on ProcessException catch (error) {
      throw EditorLaunchException(
        'failed to launch editor "${argv[0]}": ${error.message}',
      );
    }

    if (result.exitCode != 0) {
      // Nonzero is treated as an abort — the CLI does the same. Include a
      // slice of stderr so the user can see why (e.g. GIMP printing a
      // display error). Trim to keep the snackbar short.
      final stderr = result.stderr.toString().trim();
      final tail = stderr.length > 200
          ? '${stderr.substring(0, 200)}…'
          : stderr;
      throw EditorLaunchException(
        'editor "${argv[0]}" exited with code ${result.exitCode}'
        '${tail.isEmpty ? '' : ': $tail'}',
      );
    }
  }

  /// Walk `rules` in declaration order; the first rule whose `query` matches
  /// the file `fileId` wins. A rule matches when `runMatches` returns true for
  /// the composed query `/i <fileId> <rule.query>` — the `/i` term is placed
  /// first so it stays a well-formed token even if `rule.query` ends in an
  /// unclosed `"` or `%`. Throws [EditorLaunchException] if no rule matches —
  /// see the file-level doc for why there is no `$VISUAL`/`$EDITOR` fallback.
  ///
  /// A *matching* rule whose `argv` is unusable (empty, or a non-absolute
  /// `argv[0]`) is rejected loudly rather than skipped. Skipping would
  /// silently fall through to a *different* editor than the operator
  /// configured, which is both confusing and, for a rule that fails the
  /// absolute-path check, exactly the case where staying quiet is least
  /// appropriate. The `argv` is validated only once a rule has matched, so a
  /// malformed rule that does not apply to this file stays inert.
  @visibleForTesting
  static Future<List<String>> resolveArgv({
    required String fileId,
    required List<tagsy.EditorRuleEntry> rules,
    required Future<bool> Function(String query) runMatches,
  }) async {
    for (final rule in rules) {
      // `/i <fileId>` first: a complete token that a trailing unclosed quote
      // or regex in `rule.query` cannot swallow.
      if (!await runMatches('/i $fileId ${rule.query}')) continue;

      final argv = rule.argv;
      if (argv.isEmpty || argv.first.isEmpty) {
        throw EditorLaunchException(
          'editor rule for query "${rule.query}" has an empty argv: give it '
          'at least the absolute path of the editor, e.g. '
          '["/run/current-system/sw/bin/gimp"]',
        );
      }
      if (!argv.first.startsWith('/')) {
        throw EditorLaunchException(
          'editor rule for query "${rule.query}" must use an absolute path '
          'for argv[0], got "${argv.first}". A bare name would be resolved '
          'via the inherited PATH, which is not a trustworthy lookup.',
        );
      }
      return argv;
    }

    throw const EditorLaunchException(
      'no editor rule matched: add an `editor_rules` entry in the daemon '
      'config whose query matches this file',
    );
  }

  /// The environment handed to the editor.
  ///
  /// Built as an **allowlist** rather than inheriting ours wholesale, so that
  /// credentials that happen to sit in the app's environment — `SSH_AUTH_SOCK`
  /// (an agent socket is a signing oracle), `AWS_*`, `GITHUB_TOKEN`, and
  /// whatever else the launching shell exported — are not handed to a
  /// long-running GUI process that has no use for them.
  ///
  /// The list is deliberately generous about display/session/theming vars,
  /// because the failure mode of omitting one is a confusing GUI breakage
  /// (missing icons, wrong theme, "cannot open display") rather than an
  /// obvious error. `XDG_DATA_DIRS` in particular matters on NixOS, where icon
  /// themes and GSettings schemas are found through it. If an editor
  /// misbehaves in a way that smells environmental, add the variable here —
  /// that is the intended maintenance path, not switching back to full
  /// inheritance.
  ///
  /// `PATH` is forwarded because editors shell out to helpers (GIMP plug-ins,
  /// `code`'s node runtime). It does *not* weaken the `argv[0]` rule above:
  /// that is resolved by us, before this environment is ever consulted.
  static Map<String, String> _childEnvironment() {
    const forwarded = [
      // Core session identity.
      'HOME', 'USER', 'LOGNAME', 'SHELL', 'PATH', 'TMPDIR',
      // Locale — omitting these garbles non-ASCII filenames in the editor's UI.
      'LANG', 'LANGUAGE', 'LC_ALL', 'LC_CTYPE', 'LC_MESSAGES',
      // Display server.
      'DISPLAY', 'XAUTHORITY', 'WAYLAND_DISPLAY', 'XDG_SESSION_TYPE',
      // Desktop integration: portals, theming, icon and schema lookup.
      'DBUS_SESSION_BUS_ADDRESS', 'XDG_RUNTIME_DIR', 'XDG_DATA_DIRS',
      'XDG_CONFIG_DIRS', 'XDG_DATA_HOME', 'XDG_CONFIG_HOME', 'XDG_CACHE_HOME',
      'XDG_CURRENT_DESKTOP', 'DESKTOP_SESSION',
      // Toolkit rendering knobs; wrong-looking UI if dropped.
      'GDK_BACKEND', 'QT_QPA_PLATFORM', 'GTK_THEME', 'QT_STYLE_OVERRIDE',
      // NixOS-specific lookup paths for GTK/GDK asset bundles.
      'GDK_PIXBUF_MODULE_FILE', 'GSETTINGS_SCHEMA_DIR', 'FONTCONFIG_FILE',
      'GIO_EXTRA_MODULES', 'LOCALE_ARCHIVE',
    ];

    final parent = Platform.environment;
    // `?parent[name]` drops the entry entirely when the variable is unset,
    // rather than forwarding an empty string — some toolkits treat "set but
    // empty" differently from "unset" (an empty DISPLAY is not the same as no
    // DISPLAY).
    return {for (final name in forwarded) name: ?parent[name]};
  }
}
