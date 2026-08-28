// The home screen's default view: the config-defined "home sections" rendered
// when the search box is empty.
//
// Each section is a named saved search (see the daemon's `HomeSection` /
// `home_sections` config). This widget loads the section list once, runs each
// section's query through the same `runQuery` path the live search uses — so
// every filter (tag terms, negation, regex, name substrings) applies — and
// renders each section's tags and files under its heading in a single
// [RovingFocusList] so arrow-key navigation flows across section boundaries.
//
// It owns its own fetch/loading/refresh state (deliberately kept off the
// already-large home screen state), and re-runs all section queries whenever
// the change stream reports a catalog mutation so the sections stay accurate.
//
// Navigation is handed back to the home screen via callbacks: the home screen
// owns route pushing so it can restore keyboard focus to the originating row
// when a detail screen pops. Each callback receives the row's index among the
// focusable rows in this view (headers excluded), matching the index
// [RovingFocusList] assigns.

import 'dart:async';

import 'package:flutter/material.dart';

import '../../rust/api.dart' as tagsy;
import '../../session/session.dart';
import '../../widgets/roving_focus_list.dart';
import 'result_rows.dart';

/// A loaded section: the config heading plus the query's results.
class _LoadedSection {
  const _LoadedSection(this.name, this.results);

  final String name;
  final tagsy.QueryEntries results;

  bool get isEmpty => results.tags.isEmpty && results.files.isEmpty;
}

class SectionsView extends StatefulWidget {
  const SectionsView({
    super.key,
    required this.session,
    required this.controller,
    required this.onOpenTag,
    required this.onOpenFile,
    required this.onExitTop,
  });

  final TagsySession session;

  /// Drives keyboard focus over the section rows (shared with the home screen
  /// so Enter-from-search-field and focus-restore-after-a-route-pop both reach
  /// the same list).
  final RovingFocusListController controller;

  /// Push the tag detail; [restoreIndex] is the row's slot among focusable
  /// rows so focus resumes there when the detail screen pops.
  final void Function(tagsy.TagEntry tag, int restoreIndex) onOpenTag;

  /// See [onOpenTag].
  final void Function(tagsy.FileEntry file, int restoreIndex) onOpenFile;

  /// Invoked on ArrowUp off the top row / Escape — the home screen returns
  /// focus to the search field.
  final VoidCallback onExitTop;

  @override
  State<SectionsView> createState() => _SectionsViewState();
}

class _SectionsViewState extends State<SectionsView> {
  /// Loaded sections in config order, or null until the first load completes.
  List<_LoadedSection>? _sections;
  String? _error;

  /// Monotonic counter to discard a slower reload that resolves after a newer
  /// one (mirrors the home screen's `_queryEpoch`).
  int _loadEpoch = 0;

  /// Change-stream watcher guard; see [_subscribeToChanges].
  bool _watching = false;

  @override
  void initState() {
    super.initState();
    _load();
    _subscribeToChanges();
  }

  @override
  void dispose() {
    _watching = false;
    super.dispose();
  }

  /// Fetch the configured sections and run every section's query. A section's
  /// query is run with the same rules the standard (non-deleted) live search
  /// uses: subtags included, tombstones excluded.
  Future<void> _load() async {
    final repository = widget.session.repository;
    final epoch = ++_loadEpoch;
    try {
      final configured = await repository.homeSections();
      final loaded = <_LoadedSection>[];
      for (final section in configured) {
        // A single malformed section query shouldn't blank the whole home
        // screen: treat its failure as an empty result, like the live search
        // does for mid-typing tokens.
        tagsy.QueryEntries results;
        try {
          results = await repository.runQuery(
            query: section.query,
            subtagRule: tagsy.SubtagRule.include,
            deletedRule: tagsy.DeletedRule.exclude,
          );
        } catch (_) {
          results = const tagsy.QueryEntries(files: [], tags: []);
        }
        loaded.add(_LoadedSection(section.name, results));
      }
      if (!mounted || epoch != _loadEpoch) return;
      setState(() {
        _sections = loaded;
        _error = null;
      });
    } catch (error) {
      if (!mounted || epoch != _loadEpoch) return;
      setState(() => _error = '$error');
    }
  }

  /// Re-run all section queries when the catalog changes so the sections stay
  /// accurate. Mirrors the home screen's change subscription; the section list
  /// itself is config and never changes at runtime, so only the queries are
  /// re-run (via [_load], which re-reads the — unchanged — list too, harmless).
  Future<void> _subscribeToChanges() async {
    if (_watching) return;
    _watching = true;
    try {
      final events = await widget.session.repository.subscribe();
      while (mounted && _watching) {
        final event = await events.next();
        if (event == null) break;
        if (!mounted) break;
        // `ProviderReleased` is a byte-staging handoff, not a catalog mutation,
        // so it can't change any section's results — skip it (matches the home
        // screen's live-search subscription).
        final relevant = switch (event) {
          tagsy.ApiEventDto_ProviderReleased() => false,
          _ => true,
        };
        if (relevant) await _load();
      }
    } catch (_) {
      // Transient stream errors are surfaced by bootstrap; ignore here so a
      // hiccup doesn't kill the view.
    }
  }

  @override
  Widget build(BuildContext context) {
    if (_error != null) {
      return Center(child: Text('Error: $_error'));
    }
    final sections = _sections;
    if (sections == null) {
      return const Center(child: CircularProgressIndicator());
    }
    // Render only sections that matched something; an empty section adds a bare
    // heading with no rows, which reads as a glitch rather than information.
    final nonEmpty = sections.where((s) => !s.isEmpty).toList();
    if (nonEmpty.isEmpty) {
      // Sections are configured but none matched (e.g. an empty store). Leave
      // the surface blank rather than showing a wall of empty headings.
      return const SizedBox.shrink();
    }

    // Describe every section as headers + rows in render order, tracking the
    // running index among *focusable* rows so navigation callbacks can restore
    // focus to the right slot after a detail route pops.
    var rowIndex = 0;
    final items = <RovingFocusItem>[];
    for (final section in nonEmpty) {
      items.add(RovingFocusItem.header(SectionHeader(section.name)));
      for (final tag in section.results.tags) {
        final index = rowIndex++;
        items.add(
          RovingFocusItem.row(
            (focusNode) => TagRow(
              tag: tag,
              focusNode: focusNode,
              onActivate: () => widget.onOpenTag(tag, index),
            ),
          ),
        );
      }
      for (final file in section.results.files) {
        final index = rowIndex++;
        items.add(
          RovingFocusItem.row(
            (focusNode) => FileRow(
              file: file,
              focusNode: focusNode,
              onActivate: () => widget.onOpenFile(file, index),
            ),
          ),
        );
      }
    }

    return RovingFocusList(
      items: items,
      controller: widget.controller,
      onExitTop: widget.onExitTop,
    );
  }
}
