// Shared home screen: a live search bar that renders returned tags at the top
// and returned files immediately below. Both open the corresponding detail
// screen on tap. Tag creation lives in the tag picker (reachable from any
// file/tag detail screen and the share-review flow), not here.
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

import '../features/search/connection_indicator.dart';
import '../features/search/overflow_menu.dart';
import '../features/search/result_rows.dart';
import '../features/search/search_field.dart';
import '../features/search/sections_view.dart';
import '../features/search/storage_stats_indicator.dart';
import '../features/search/view_mode.dart';
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

  /// How the *files* section of active-search results is rendered. Only affects
  /// the live-query result surface, not the config-defined home sections
  /// ([SectionsView], which always renders rows). Ephemeral (resets on
  /// restart), mirroring [_showDeleted]. Cycled by a horizontal swipe over the
  /// results and set directly from the overflow menu.
  FileViewMode _fileViewMode = FileViewMode.full;

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

  /// Handle Enter in the search field.
  ///
  /// If the results list contains exactly one entry (across tags and files
  /// combined) we activate it directly — there's no ambiguity, and this
  /// preserves the fast "type + Enter to open" flow for common cases like
  /// resolving a query down to a single tag. Otherwise (two or more entries,
  /// or none) we hand focus to the first row instead, so the user can
  /// arrow-key their way to the desired result without tabbing past the
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
    final total = results.tags.length + results.files.length;
    if (total == 1) {
      if (results.tags.length == 1) {
        // Sole result is a tag; open it and restore focus to row 0 on
        // return so a subsequent Enter re-opens the same tag.
        await _openTag(results.tags.first, restoreIndex: 0);
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

  /// Open a tag by id (from a large-tile tag chip, which only carries the id).
  /// Unlike [_openTag] there's no roving-focus row to restore — tile chips are
  /// outside the keyboard navigation.
  Future<void> _openTagById(String tagId) async {
    FocusManager.instance.primaryFocus?.unfocus();
    await Navigator.push(
      context,
      MaterialPageRoute(
        builder: (_) => TagDetailScreen(session: widget.session!, tagId: tagId),
      ),
    );
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

  @override
  Widget build(BuildContext context) {
    final publicKey = widget.session?.publicKey;
    return Scaffold(
      appBar: AppBar(
        title: const Text('Tagsy'),
        actions: [
          StorageStatsIndicator(session: widget.session),
          ConnectionIndicator(session: widget.session),
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
            fileViewMode: _fileViewMode,
            onSelectViewMode: (mode) =>
                setState(() => _fileViewMode = mode),
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
    final hasTags = results.tags.isNotEmpty;
    final hasFiles = results.files.isNotEmpty;
    if (!hasTags && !hasFiles) {
      return const Center(child: Text('No matches.'));
    }
    // A horizontal swipe cycles the file view mode (left = next, right = prev).
    // Wraps whichever surface the current mode builds. `onHorizontalDragEnd`
    // reads the fling velocity so it doesn't fight vertical scrolling.
    return switch (_fileViewMode) {
      FileViewMode.list => _buildListResults(results, hasTags, hasFiles),
      FileViewMode.tile || FileViewMode.large || FileViewMode.full =>
        _buildTileResults(results, hasTags, hasFiles),
    };
  }

  /// The original text-list result surface: tags → files, all as focusable rows
  /// in a [RovingFocusList] (owns per-row focus + arrow-key nav).
  /// `restoreIndex` is the row's position among *focusable* rows only (headers
  /// don't count), matching the index RovingFocusList assigns — the tap and
  /// Enter handlers pass it back to `_openTag` / `_openFile` so focus resumes on
  /// the right row after a detail route pops.
  Widget _buildListResults(
    tagsy.QueryEntries results,
    bool hasTags,
    bool hasFiles,
  ) {
    var rowIndex = 0;
    final items = <RovingFocusItem>[];
    if (hasTags) {
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

  /// The tile result surfaces (shared by [FileViewMode.tile], [.large] and
  /// [.full]): tags stay as rows at the top, files render below as either a grid
  /// of small thumbnails ([FileTile]), one full-width tile per file with tags
  /// ([FileLargeTile]), or one very large tap-to-load-inline tile per file
  /// ([FileFullTile]). Composed as a [CustomScrollView] since the sections have
  /// different layouts. Tiles are tappable but not part of the roving arrow-key
  /// navigation, so `restoreRow` doesn't apply here — passing index 0 just parks
  /// focus back near the top on return.
  Widget _buildTileResults(
    tagsy.QueryEntries results,
    bool hasTags,
    bool hasFiles,
  ) {
    final session = widget.session!;
    final slivers = <Widget>[];
    if (hasTags) {
      final tagChildren = <Widget>[const SectionHeader('Tags')];
      for (final tag in results.tags) {
        tagChildren.add(
          TagRow(
            tag: tag,
            onActivate: () => _openTag(tag, restoreIndex: 0),
          ),
        );
      }
      slivers.add(SliverList(delegate: SliverChildListDelegate(tagChildren)));
    }
    if (hasFiles) {
      slivers.add(
        const SliverToBoxAdapter(child: SectionHeader('Files')),
      );
      if (_fileViewMode == FileViewMode.large ||
          _fileViewMode == FileViewMode.full) {
        // One full-width tile per file, stacked; each shows a large preview,
        // the name, and the file's tags. The `full` mode uses a taller,
        // tap-to-load-inline preview ([FileFullTile]); `large` uses the
        // fixed-height thumbnail tile ([FileLargeTile]).
        final full = _fileViewMode == FileViewMode.full;
        slivers.add(
          SliverPadding(
            padding: const EdgeInsets.all(8),
            sliver: SliverList(
              delegate: SliverChildBuilderDelegate((context, i) {
                final file = results.files[i];
                final key = ValueKey('${file.fileId}:${file.contentHash}');
                return Padding(
                  padding: const EdgeInsets.only(bottom: 8),
                  child: full
                      ? FileFullTile(
                          key: key,
                          file: file,
                          repository: session.repository,
                          onActivate: () => _openFile(file, restoreIndex: 0),
                          onOpenTag: _openTagById,
                        )
                      : FileLargeTile(
                          key: key,
                          file: file,
                          repository: session.repository,
                          onActivate: () => _openFile(file, restoreIndex: 0),
                          onOpenTag: _openTagById,
                        ),
                );
              }, childCount: results.files.length),
            ),
          ),
        );
      } else {
        slivers.add(
          SliverPadding(
            padding: const EdgeInsets.all(8),
            sliver: SliverGrid(
              gridDelegate:
                  const SliverGridDelegateWithMaxCrossAxisExtent(
                    maxCrossAxisExtent: 180,
                    mainAxisSpacing: 8,
                    crossAxisSpacing: 8,
                    childAspectRatio: 0.85,
                  ),
              delegate: SliverChildBuilderDelegate((context, i) {
                final file = results.files[i];
                return FileTile(
                  key: ValueKey('${file.fileId}:${file.contentHash}'),
                  file: file,
                  repository: session.repository,
                  onActivate: () => _openFile(file, restoreIndex: 0),
                );
              }, childCount: results.files.length),
            ),
          ),
        );
      }
    }
    return CustomScrollView(slivers: slivers);
  }
}
