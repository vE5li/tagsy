// Tests for the Linux editor launcher's rule resolution.
//
// These cover the resolution/guard logic only, not the actual spawn: the
// interesting behavior is *which* argv gets chosen and which rules are
// refused, and that is what the security properties described in
// `linux_editor_launcher.dart` rest on. Spawning a real process would test
// `Process.run` rather than anything of ours.
//
// Rule matching is delegated to the daemon's query path in production; here
// we substitute a `runMatches` stub that records the composed queries it is
// asked and answers from a fixed set of "matching" queries. That lets us
// assert both the match outcome and the exact query composition (`/i <id>`
// first) without a live bridge.

import 'package:flutter_test/flutter_test.dart';

import 'package:tagsy_app/editor/editor_launcher.dart';
import 'package:tagsy_app/editor/linux_editor_launcher.dart';
import 'package:tagsy_app/rust/api.dart' as tagsy;

tagsy.EditorRuleEntry _rule(String query, List<String> argv) =>
    tagsy.EditorRuleEntry(query: query, argv: argv);

/// A `runMatches` stub: matches whenever the composed query contains one of
/// [matching] as a substring, and records every query it was asked.
class _Matcher {
  _Matcher(this.matching);

  final Set<String> matching;
  final List<String> asked = [];

  Future<bool> call(String query) async {
    asked.add(query);
    return matching.any(query.contains);
  }
}

void main() {
  group('LinuxEditorLauncher.resolveArgv', () {
    test('returns the argv of the first rule whose query matches', () async {
      final argv = await LinuxEditorLauncher.resolveArgv(
        fileId: 'file-1',
        rules: [
          _rule('/t images', const ['/usr/bin/gimp']),
          _rule('/t code', const ['/usr/bin/code', '--wait']),
        ],
        runMatches: _Matcher({'/t code'}).call,
      );
      expect(argv, const ['/usr/bin/code', '--wait']);
    });

    test('composes `/i <fileId>` first, ahead of the rule query', () async {
      final matcher = _Matcher({'/t code'});
      await LinuxEditorLauncher.resolveArgv(
        fileId: 'file-1',
        rules: [
          _rule('/t code', const ['/usr/bin/code']),
        ],
        runMatches: matcher.call,
      );
      // The `/i` term leads so a trailing unclosed quote/regex in the rule
      // query cannot swallow it.
      expect(matcher.asked, ['/i file-1 /t code']);
    });

    test('declaration order wins when several rules match', () async {
      final argv = await LinuxEditorLauncher.resolveArgv(
        fileId: 'file-1',
        rules: [
          _rule('/t a', const ['/usr/bin/first']),
          _rule('/t b', const ['/usr/bin/second']),
        ],
        // Both match; the first in declaration order must win.
        runMatches: _Matcher({'/t a', '/t b'}).call,
      );
      expect(argv, const ['/usr/bin/first']);
    });

    test('multi-argument argv is preserved verbatim, including spaces', () async {
      // The whole point of taking a list instead of a string: an argument
      // containing a space stays one argument.
      final argv = await LinuxEditorLauncher.resolveArgv(
        fileId: 'file-1',
        rules: [
          _rule('/t a', const ['/opt/my editor/bin/edit', '--flag=a b']),
        ],
        runMatches: _Matcher({'/t a'}).call,
      );
      expect(argv, const ['/opt/my editor/bin/edit', '--flag=a b']);
    });

    test('throws when no rule matches', () async {
      await expectLater(
        LinuxEditorLauncher.resolveArgv(
          fileId: 'file-1',
          rules: [
            _rule('/t a', const ['/usr/bin/gimp']),
          ],
          runMatches: _Matcher({}).call,
        ),
        throwsA(isA<EditorLaunchException>()),
      );
    });

    test('throws when there are no rules at all', () async {
      await expectLater(
        LinuxEditorLauncher.resolveArgv(
          fileId: 'file-1',
          rules: const [],
          runMatches: _Matcher({'anything'}).call,
        ),
        throwsA(isA<EditorLaunchException>()),
      );
    });

    test('rejects a relative argv[0] rather than trusting PATH', () async {
      // The security guard: a bare name would be resolved through the
      // inherited PATH, so anyone able to prepend a directory to it could
      // substitute the editor.
      await expectLater(
        LinuxEditorLauncher.resolveArgv(
          fileId: 'file-1',
          rules: [
            _rule('/t a', const ['gimp']),
          ],
          runMatches: _Matcher({'/t a'}).call,
        ),
        throwsA(
          isA<EditorLaunchException>().having(
            (error) => error.message,
            'message',
            contains('absolute path'),
          ),
        ),
      );
    });

    test('rejects an empty argv', () async {
      await expectLater(
        LinuxEditorLauncher.resolveArgv(
          fileId: 'file-1',
          rules: [_rule('/t a', const [])],
          runMatches: _Matcher({'/t a'}).call,
        ),
        throwsA(isA<EditorLaunchException>()),
      );
    });

    test(
      'a malformed matching rule fails loudly instead of falling through',
      () async {
        // Silently skipping to the next rule would launch a *different* editor
        // than the operator configured — least acceptable exactly when the rule
        // failed the absolute-path check.
        await expectLater(
          LinuxEditorLauncher.resolveArgv(
            fileId: 'file-1',
            rules: [
              _rule('/t a', const ['relative-editor']),
              _rule('/t b', const ['/usr/bin/fallback']),
            ],
            runMatches: _Matcher({'/t a', '/t b'}).call,
          ),
          throwsA(isA<EditorLaunchException>()),
        );
      },
    );

    test('non-matching malformed rules are ignored', () async {
      // The guard applies to the rule that actually matched, not to every
      // rule in the config.
      final argv = await LinuxEditorLauncher.resolveArgv(
        fileId: 'file-1',
        rules: [
          _rule('/t a', const ['relative-editor']),
          _rule('/t b', const ['/usr/bin/ok']),
        ],
        runMatches: _Matcher({'/t b'}).call,
      );
      expect(argv, const ['/usr/bin/ok']);
    });
  });
}
