// A keyboard-navigable list with "roving focus": exactly one row holds the
// tab-stop, and Up/Down move it between rows.
//
// Extracted from the home screen, whose search results were a flat list of
// interactive rows (tags, a create-tag affordance, files) interleaved with
// non-interactive section headers, wrapped in ~120 lines of bespoke focus
// bookkeeping: a stable, never-shrunk pool of [FocusNode]s (so a rebuild driven
// by the change stream mid-navigation can't dispose the focused node out from
// under the user), arrow-key handlers that clamp at the ends, Escape / Up-off-
// the-top handoff back to a search field, ensure-visible scrolling, and
// focus restoration after returning from a pushed detail route.
//
// This widget owns all of that. The caller describes the list as a sequence of
// [RovingFocusItem]s — headers (non-focusable) and rows (each built with the
// [FocusNode] this widget hands it) — and drives programmatic focus (first row,
// or a specific row after a route pop) through a [RovingFocusListController].

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

/// One entry in a [RovingFocusList]: either a non-focusable [header] (e.g. a
/// section title) or a focusable [row].
class RovingFocusItem {
  /// A non-interactive item that is skipped by keyboard navigation.
  const RovingFocusItem.header(Widget this.header) : rowBuilder = null;

  /// An interactive row. [builder] receives the stable [FocusNode] this widget
  /// owns for the row's slot; attach it to the row's [ListTile]/[Focus] so the
  /// roving tab-stop lands here.
  RovingFocusItem.row(Widget Function(FocusNode focusNode) builder)
    : header = null,
      rowBuilder = builder;

  final Widget? header;
  final Widget Function(FocusNode focusNode)? rowBuilder;

  bool get isRow => rowBuilder != null;
}

/// Programmatic control over a [RovingFocusList]'s focus.
///
/// Hand one to [RovingFocusList.controller] and call [focusFirstRow] (e.g. on
/// Enter in a search field to jump into the results) or [restoreRow] (e.g. after
/// a pushed detail route pops, to resume where the user was). [rowCount] is the
/// number of focusable rows in the current build.
class RovingFocusListController extends ChangeNotifier {
  _RovingFocusListState? _state;

  void _attach(_RovingFocusListState state) => _state = state;
  void _detach(_RovingFocusListState state) {
    if (identical(_state, state)) _state = null;
  }

  /// Number of focusable rows bound in the most recent build.
  int get rowCount => _state?._activeRowCount ?? 0;

  /// Move keyboard focus to the first row, if any.
  void focusFirstRow() => _state?._focusRow(0);

  /// Put keyboard focus back on row [index], clamped to the current range;
  /// falls back to [RovingFocusList.onExitTop] when there are no rows.
  void restoreRow(int index) => _state?._restoreRow(index);
}

/// A [ListView] whose focusable rows share a single roving tab-stop.
class RovingFocusList extends StatefulWidget {
  const RovingFocusList({
    super.key,
    required this.items,
    required this.onExitTop,
    this.controller,
  });

  /// The list contents in render order: headers and rows interleaved.
  final List<RovingFocusItem> items;

  /// Invoked when the user presses ArrowUp from the first row or Escape — the
  /// caller typically returns focus to the search field above the list.
  final VoidCallback onExitTop;

  /// Optional handle for programmatic focus (first row / restore after a route
  /// pop).
  final RovingFocusListController? controller;

  @override
  State<RovingFocusList> createState() => _RovingFocusListState();
}

class _RovingFocusListState extends State<RovingFocusList> {
  /// Stable pool of focus nodes for the focusable rows, in render order. Grows
  /// lazily as the list returns more rows and is never shrunk — extra nodes
  /// just don't get attached — so a rebuild triggered mid-navigation can't
  /// dispose the currently-focused node out from under the user (which used to
  /// cause focus to snap back to a previous row on Up).
  ///
  /// All entries are disposed in [dispose]; unused entries are cheap.
  final List<FocusNode> _rowFocus = [];

  /// Number of row focus nodes actually bound to a visible row in the most
  /// recent build. Used to clamp navigation.
  int _activeRowCount = 0;

  @override
  void initState() {
    super.initState();
    widget.controller?._attach(this);
  }

  @override
  void didUpdateWidget(covariant RovingFocusList oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (!identical(oldWidget.controller, widget.controller)) {
      oldWidget.controller?._detach(this);
      widget.controller?._attach(this);
    }
  }

  @override
  void dispose() {
    widget.controller?._detach(this);
    for (final node in _rowFocus) {
      node.dispose();
    }
    super.dispose();
  }

  /// Ensure `_rowFocus` has at least [count] entries, creating new nodes on
  /// demand. Never shrinks — see the field docstring.
  void _ensureCapacity(int count) {
    while (_rowFocus.length < count) {
      _rowFocus.add(FocusNode(debugLabel: 'row${_rowFocus.length}'));
    }
  }

  /// Index of the currently-focused row within `_rowFocus`, or -1 if none. We
  /// match by primary focus rather than `hasFocus` because parent focus scopes
  /// report `hasFocus == true` on ancestors too.
  int _focusedRowIndex() {
    final primary = FocusManager.instance.primaryFocus;
    if (primary == null) return -1;
    for (var i = 0; i < _activeRowCount; i++) {
      if (identical(_rowFocus[i], primary)) return i;
    }
    return -1;
  }

  /// Focus row [index] (clamped) and scroll it into view.
  void _focusRow(
    int index, [
    ScrollPositionAlignmentPolicy policy =
        ScrollPositionAlignmentPolicy.explicit,
  ]) {
    if (_activeRowCount == 0) return;
    final clamped = index.clamp(0, _activeRowCount - 1);
    _rowFocus[clamped].requestFocus();
    _ensureVisible(clamped, policy);
  }

  /// ArrowDown: move to the next row, clamped at the last visible row. No
  /// wraparound — reaching the bottom just stays put.
  void _focusNextRow() {
    if (_activeRowCount == 0) return;
    final current = _focusedRowIndex();
    // If focus somehow drifted off-list, fall back to the first row.
    final next = current < 0 ? 0 : (current + 1).clamp(0, _activeRowCount - 1);
    _focusRow(next, ScrollPositionAlignmentPolicy.keepVisibleAtEnd);
  }

  /// ArrowUp: move to the previous row. From row 0, hand off via [onExitTop]
  /// (the caller returns focus to the search field so the user can keep typing
  /// without Shift-Tabbing through anything).
  void _focusPreviousRow() {
    if (_activeRowCount == 0) return;
    final current = _focusedRowIndex();
    if (current <= 0) {
      widget.onExitTop();
      return;
    }
    _focusRow(current - 1, ScrollPositionAlignmentPolicy.keepVisibleAtStart);
  }

  /// Best-effort: put keyboard focus back on `_rowFocus[index]`. If the rows
  /// have shrunk while we were away, clamp to the last visible row; if there
  /// are none, hand off via [onExitTop].
  void _restoreRow(int index) {
    if (_activeRowCount == 0) {
      widget.onExitTop();
      return;
    }
    _rowFocus[index.clamp(0, _activeRowCount - 1)].requestFocus();
  }

  /// Scroll `_rowFocus[index]` into view. Uses a post-frame callback so this
  /// works both when the row is already laid out (arrow-key navigation) and
  /// when the tree is mid-rebuild.
  void _ensureVisible(int index, ScrollPositionAlignmentPolicy policy) {
    final node = _rowFocus[index];
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (!mounted) return;
      final ctx = node.context;
      if (ctx == null) return;
      Scrollable.ensureVisible(
        ctx,
        alignment: 0.0,
        alignmentPolicy: policy,
        duration: const Duration(milliseconds: 150),
      );
    });
  }

  @override
  Widget build(BuildContext context) {
    final rowCount = widget.items.where((i) => i.isRow).length;
    _ensureCapacity(rowCount);
    _activeRowCount = rowCount;

    var rowIndex = 0;
    final children = <Widget>[
      for (final item in widget.items)
        if (item.isRow)
          item.rowBuilder!(_rowFocus[rowIndex++])
        else
          item.header!,
    ];

    return CallbackShortcuts(
      bindings: {
        const SingleActivator(LogicalKeyboardKey.arrowDown): _focusNextRow,
        const SingleActivator(LogicalKeyboardKey.arrowUp): _focusPreviousRow,
        const SingleActivator(LogicalKeyboardKey.escape): widget.onExitTop,
      },
      child: ListView(children: children),
    );
  }
}
