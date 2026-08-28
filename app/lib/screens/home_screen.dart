// Shared home screen: a live search bar that renders returned tags at the top
// and returned files immediately below. Both open the corresponding detail
// screen on tap; when a non-empty tag-name-shaped query resolves to zero tags,
// a "Create tag" affordance appears in the tags section so tag creation
// remains reachable without a dedicated management screen.
//
// The screen intentionally does NOT fetch anything on load: an empty
// `runQuery` scans the entire store, which is a real performance hazard as the
// store grows. Results only appear once the user types.
//
// Identical on every platform; the AppBar exposes an (Android-only)
// copy-public-key action that renders only when the session carries a key
// (absent on Linux, where the daemon owns the identity). No platform imports
// here.

import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

import '../features/search/overflow_menu.dart';
import '../features/search/result_rows.dart';
import '../features/search/search_field.dart';
import '../features/search/sections_view.dart';
import '../features/search/storage_stats_indicator.dart';
import '../rust/api.dart' as tagsy;
import '../session/session.dart';
import '../widgets/roving_focus_list.dart';
import 'file_detail_screen.dart';
import 'tag_detail_screen.dart';

class HomeScreen extends StatefulWidget {
  const HomeScreen({
    super.key,
    required this.session,
    this.bootError,
    this.onRetry,
  });

  final TagsySession? session;

  /// Non-null when bootstrap (connecting to the backend) failed. With
  /// [session] still null, this drives an error surface + retry instead of a
  /// perpetual "Connecting…".
  final Object? bootError;

  /// Invoked from the boot-error surface to re-attempt the connection.
  final VoidCallback? onRetry;

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  final TextEditingController _query = TextEditingController();

  /// Owned so we can programmatically drop focus when navigating to a detail
  /// screen. Without this, Flutter's automatic focus restoration can re-focus
  /// the search field when the detail route is popped, which re-opens the
  /// soft keyboard on mobile — a jarring UX papercut every time the user
  /// backs out of a detail view.
  final FocusNode _queryFocus = FocusNode();

  /// Debounce timer for keystrokes -> `runQuery` calls. Kept short so results
  /// feel live but the daemon isn't hit on every character.
  Timer? _debounce;

  /// Monotonic counter used to discard stale results if a slower query resolves
  /// after a newer one has already been dispatched.
  int _queryEpoch = 0;

  /// Latest result to render. Null until the user runs a query.
  tagsy.QueryEntries? _results;
  String? _error;
  bool _loading = false;

  /// When true, the search runs against soft-deleted (tombstoned) files and
  /// tags instead of live ones — see [tagsy.DeletedRule]. Toggled by the
  /// small button next to the search field. Off by default: the standard
  /// search only ever shows live rows.
  bool _showDeleted = false;

  /// Change-stream watcher: re-runs the *current* query whenever the underlying
  /// data changes so the results stay accurate. Deliberately does nothing when
  /// the user has not typed a query yet — we never synthesise an empty query.
  ///
  /// TODO(perf): this refetches on every change event, which is coarse. For
  /// large stores we should either debounce the change-driven refetches or
  /// filter which events actually need a re-query (e.g. only re-run on tag /
  /// file mutations, not on transport heartbeats). Revisit when the redesign
  /// stabilises.
  bool _watching = false;

  /// Drives keyboard focus over the result rows (arrow-key navigation, Enter
  /// jump-to-first, focus restore after a detail route pops). The focus-node
  /// bookkeeping itself lives inside [RovingFocusList].
  final RovingFocusListController _rows = RovingFocusListController();

  @override
  void initState() {
    super.initState();
    _query.addListener(_onQueryChanged);
    if (widget.session != null) _subscribeToChanges();
    // Global Ctrl+F -> focus the search field. We hook `HardwareKeyboard`
    // directly (rather than using Shortcuts/Actions) because the focus tree
    // above HomeScreen doesn't route to a Shortcuts widget placed inside it:
    // shortcut resolution walks up from the currently focused node, so a
    // Shortcuts widget higher than the focus never sees the event. The raw
    // handler runs regardless of focus location, which is exactly what a
    // "global" accelerator wants.
    HardwareKeyboard.instance.addHandler(_handleGlobalKey);
  }

  bool _handleGlobalKey(KeyEvent event) {
    if (event is! KeyDownEvent) return false;
    if (event.logicalKey != LogicalKeyboardKey.keyF) return false;
    if (!HardwareKeyboard.instance.isControlPressed) return false;
    _queryFocus.requestFocus();
    return true; // consume so the browser/OS Ctrl+F is fully suppressed
  }

  @override
  void didUpdateWidget(covariant HomeScreen old) {
    super.didUpdateWidget(old);
    if (old.session == null && widget.session != null) _subscribeToChanges();
  }

  @override
  void dispose() {
    _watching = false;
    _debounce?.cancel();
    HardwareKeyboard.instance.removeHandler(_handleGlobalKey);
    _query.removeListener(_onQueryChanged);
    _query.dispose();
    _queryFocus.dispose();
    _rows.dispose();
    super.dispose();
  }

  void _onQueryChanged() {
    _debounce?.cancel();
    _debounce = Timer(const Duration(milliseconds: 200), _runQuery);
    // Rebuild for the clear button in the search bar suffix.
    setState(() {});
  }

  Future<void> _subscribeToChanges() async {
    final session = widget.session;
    if (session == null || _watching) return;
    _watching = true;
    try {
      final events = await session.repository.subscribe();
      while (mounted && _watching) {
        final event = await events.next();
        if (event == null) break;
        if (!mounted) break;
        // The search results span every file and tag, so any catalog change
        // can alter them — re-run on all of them. The one event that never
        // affects search is `ProviderReleased` (a byte-staging handoff, not a
        // catalog mutation), so skip it. On `Resynced` we may have missed
        // changes, so re-run too.
        final relevant = switch (event) {
          tagsy.ApiEventDto_ProviderReleased() => false,
          _ => true,
        };
        // Only re-run if the user has actually issued a query. We must never
        // fabricate an empty-query listing here (see class doc).
        if (relevant && _results != null) await _runQuery();
      }
    } catch (_) {
      // Stream errors are surfaced elsewhere (bootstrap) — ignore here so a
      // transient hiccup doesn't kill the screen.
    }
  }

  Future<void> _runQuery() async {
    final session = widget.session;
    if (session == null) return;
    final epoch = ++_queryEpoch;
    // An empty (or whitespace-only) query means "no search" — reset back to the
    // null state so the home sections reappear, and skip the round-trip. Never
    // run an empty `runQuery`: it would scan the entire store (see class doc).
    if (_query.text.trim().isEmpty) {
      setState(() {
        _results = null;
        _error = null;
        _loading = false;
      });
      return;
    }
    setState(() => _loading = true);
    try {
      final result = await session.repository.runQuery(
        query: _query.text,
        subtagRule: tagsy.SubtagRule.include,
        deletedRule: _showDeleted
            ? tagsy.DeletedRule.include
            : tagsy.DeletedRule.exclude,
      );
      if (!mounted || epoch != _queryEpoch) return;
      setState(() {
        _results = result;
        _error = null;
        _loading = false;
      });
    } catch (error) {
      if (!mounted || epoch != _queryEpoch) return;
      // Mid-typing tag tokens (`$fo`) legitimately fail to resolve; treat those
      // as "no matches" so the UI doesn't flash red at every keystroke. Other
      // errors (transport, etc.) still surface.
      final looksLikeUnresolved =
          error is tagsy.ApiError_UnknownId ||
          error is tagsy.ApiError_AmbiguousId;
      setState(() {
        if (looksLikeUnresolved) {
          _results = const tagsy.QueryEntries(files: [], tags: []);
          _error = null;
        } else {
          _error = '$error';
        }
        _loading = false;
      });
    }
  }

  /// If the current query text is a plausible bare tag name (non-empty, no
  /// whitespace, no query sigils) and the search returned zero tags, returns
  /// that name so the results view can offer to create it. Otherwise returns
  /// null and no "create" affordance is shown.
  ///
  /// The affordance is suppressed while [_showDeleted] is on: in that mode
  /// the empty result set means "no *deleted* tag matches", not "no tag by
  /// this name exists", so offering to create one would be misleading.
  String? get _createCandidate {
    if (_showDeleted) return null;
    final text = _query.text.trim();
    if (text.isEmpty) return null;
    if (text.contains(RegExp(r'[\s$!]'))) return null;
    final results = _results;
    if (results == null) return null;
    if (results.tags.isNotEmpty) return null;
    return text;
  }

  /// Handle Enter in the search field.
  ///
  /// If the results list contains exactly one entry (across tags, the
  /// create-tag affordance, and files combined) we activate it directly —
  /// there's no ambiguity, and this preserves the fast "type + Enter to
  /// open" flow for common cases like resolving a query down to a single
  /// tag or offering to create a fresh tag name. Otherwise (two or more
  /// entries, or none) we hand focus to the first row instead, so the user
  /// can arrow-key their way to the desired result without tabbing past the
  /// AppBar actions.
  ///
  /// Flushes any pending debounced query first so Enter works even when the
  /// user types and immediately hits Enter, before the 200 ms debounce has
  /// fired.
  Future<void> _handleSubmit() async {
    final session = widget.session;
    if (session == null) return;
    if (_debounce?.isActive ?? false) {
      _debounce!.cancel();
      await _runQuery();
    }
    if (!mounted) return;
    final results = _results;
    if (results == null) return;
    final candidate = _createCandidate;
    final total =
        results.tags.length +
        (candidate != null ? 1 : 0) +
        results.files.length;
    if (total == 1) {
      if (results.tags.length == 1) {
        // Sole result is a tag; open it and restore focus to row 0 on
        // return so a subsequent Enter re-opens the same tag.
        await _openTag(results.tags.first, restoreIndex: 0);
      } else if (candidate != null) {
        await _createTag(candidate);
      } else {
        await _openFile(results.files.first, restoreIndex: 0);
      }
      return;
    }
    // Zero or 2+ results: move keyboard focus onto the first row (if any)
    // so arrow keys traverse the list.
    if (total >= 2 && _rows.rowCount > 0) {
      _rows.focusFirstRow();
    }
  }

  /// Push the tag detail screen and, on return, put keyboard focus back on
  /// the row the user came from so keyboard navigation resumes where it
  /// left off.
  Future<void> _openTag(tagsy.TagEntry tag, {required int restoreIndex}) async {
    // Drop focus before pushing so Flutter's automatic focus restoration
    // doesn't re-focus the search field (which would also re-open the soft
    // keyboard on mobile). We put focus back explicitly on return.
    FocusManager.instance.primaryFocus?.unfocus();
    await Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) =>
            TagDetailScreen(session: widget.session!, tagId: tag.tagId),
      ),
    );
    if (!mounted) return;
    _rows.restoreRow(restoreIndex);
  }

  /// See [_openTag].
  Future<void> _openFile(
    tagsy.FileEntry file, {
    required int restoreIndex,
  }) async {
    FocusManager.instance.primaryFocus?.unfocus();
    await Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) => FileDetailScreen(session: widget.session!, file: file),
      ),
    );
    if (!mounted) return;
    _rows.restoreRow(restoreIndex);
  }

  Future<void> _createTag(String name) async {
    final session = widget.session;
    if (session == null) return;
    try {
      // Pass an empty color; the engine substitutes its default palette entry
      // (see tagsyd::api::create_tag). The user can recolor via the tag
      // detail screen.
      await session.repository.createTag(name: name, color: '');
      // The change stream will re-run the current query and the new tag will
      // appear in the results (matching `name` as a substring).
    } catch (error) {
      if (!mounted) return;
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(SnackBar(content: Text('Failed to create tag: $error')));
    }
  }

  @override
  Widget build(BuildContext context) {
    final publicKey = widget.session?.publicKey;
    return Scaffold(
      appBar: AppBar(
        title: const Text('Tagsy'),
        actions: [
          StorageStatsIndicator(session: widget.session),
          OverflowMenu(
            session: widget.session,
            publicKey: publicKey,
            showDeleted: _showDeleted,
            onToggleShowDeleted: () {
              setState(() => _showDeleted = !_showDeleted);
              // Re-run immediately if a query is already active so the mode
              // change is visible without waiting for a keystroke.
              if (_results != null) _runQuery();
            },
          ),
        ],
      ),
      body: SafeArea(
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Padding(
              padding: const EdgeInsets.all(16),
              child: SearchField(
                controller: _query,
                focusNode: _queryFocus,
                loading: _loading,
                onSubmitted: _handleSubmit,
              ),
            ),
            Expanded(child: _buildResults()),
          ],
        ),
      ),
    );
  }

  Widget _buildResults() {
    final session = widget.session;
    if (session == null) {
      final bootError = widget.bootError;
      if (bootError == null) {
        return const Center(child: Text('Connecting…'));
      }
      return Center(
        child: Padding(
          padding: const EdgeInsets.symmetric(horizontal: 32),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              const Icon(Icons.cloud_off, size: 48),
              const SizedBox(height: 16),
              const Text(
                'Could not connect to the tagsy backend.',
                textAlign: TextAlign.center,
              ),
              const SizedBox(height: 8),
              Text(
                '$bootError',
                textAlign: TextAlign.center,
                style: Theme.of(context).textTheme.bodySmall,
              ),
              if (widget.onRetry != null) ...[
                const SizedBox(height: 16),
                FilledButton.tonal(
                  onPressed: widget.onRetry,
                  child: const Text('Retry'),
                ),
              ],
            ],
          ),
        ),
      );
    }
    if (_error != null) {
      return Center(child: Text('Error: $_error'));
    }
    final results = _results;
    if (results == null) {
      // No query has run yet. Instead of an eager full-store listing, render
      // the config-defined home sections (named saved searches). Each runs its
      // own bounded query; [SectionsView] owns that fetching. When no sections
      // are configured it renders nothing, leaving the surface blank.
      return SectionsView(
        session: session,
        controller: _rows,
        onOpenTag: (tag, index) => _openTag(tag, restoreIndex: index),
        onOpenFile: (file, index) => _openFile(file, restoreIndex: index),
        onExitTop: _queryFocus.requestFocus,
      );
    }
    final createCandidate = _createCandidate;
    final hasTags = results.tags.isNotEmpty;
    final hasFiles = results.files.isNotEmpty;
    if (!hasTags && !hasFiles && createCandidate == null) {
      return const Center(child: Text('No matches.'));
    }
    // Describe the list as headers + rows in render order (tags → create-tag →
    // files); [RovingFocusList] owns the per-row focus nodes and the arrow-key
    // navigation. `restoreIndex` is the row's position among *focusable* rows
    // only (headers don't count), matching the index RovingFocusList assigns —
    // the tap and Enter handlers pass it back to `_openTag` / `_openFile` so
    // focus resumes on the right row after a detail route pops.
    var rowIndex = 0;
    final items = <RovingFocusItem>[];
    if (hasTags || createCandidate != null) {
      items.add(const RovingFocusItem.header(SectionHeader('Tags')));
      for (final tag in results.tags) {
        final index = rowIndex++;
        items.add(
          RovingFocusItem.row(
            (focusNode) => TagRow(
              tag: tag,
              focusNode: focusNode,
              onActivate: () => _openTag(tag, restoreIndex: index),
            ),
          ),
        );
      }
      if (createCandidate != null) {
        // The create-tag row doesn't push a route, so nothing to restore
        // focus to; `_createTag` returns and the results list mutates.
        rowIndex++;
        items.add(
          RovingFocusItem.row(
            (focusNode) => CreateTagRow(
              name: createCandidate,
              onCreate: () => _createTag(createCandidate),
              focusNode: focusNode,
            ),
          ),
        );
      }
    }
    if (hasFiles) {
      items.add(const RovingFocusItem.header(SectionHeader('Files')));
      for (final file in results.files) {
        final index = rowIndex++;
        items.add(
          RovingFocusItem.row(
            (focusNode) => FileRow(
              file: file,
              focusNode: focusNode,
              onActivate: () => _openFile(file, restoreIndex: index),
            ),
          ),
        );
      }
    }
    return RovingFocusList(
      items: items,
      controller: _rows,
      // ArrowUp off the top row and Escape both return to the search field so
      // the user can keep typing without Shift-Tabbing past anything.
      onExitTop: _queryFocus.requestFocus,
    );
  }
}
