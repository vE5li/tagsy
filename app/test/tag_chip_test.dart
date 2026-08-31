// Render tests for [TagChip].
//
// These exist because a static `dart analyze` can't catch the failure mode this
// widget has hit twice: a layout-time "unbounded constraints" throw (from
// stretching a cross-axis that a `Wrap` leaves unbounded) that blanks the
// screen at runtime while compiling cleanly. Each test pumps a real chip inside
// a `Wrap` — the exact parent `FileTagStrip`/`TagsSection` use — and fails if
// layout throws or the delete X isn't hittable across its full height.

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:tagsy_app/rust/api.dart' as tagsy;
import 'package:tagsy_app/widgets/tag_chip.dart';

tagsy.TagEntry _tag({
  String name = 'example',
  String shape = 'stadium',
  String borderStyle = 'solid',
  bool shadow = false,
}) {
  return tagsy.TagEntry(
    tagId: 't1',
    name: name,
    deleted: false,
    style: tagsy.TagStyleEntry(
      dotColor: '#FF0000',
      background: '#FFEEEE',
      gradient: '#EEEEFF',
      foreground: '#000000',
      border: '#FF000000',
      borderWidth: 1.5,
      borderStyle: borderStyle,
      shape: shape,
      shadow: shadow,
      shadowColor: '#80000000',
    ),
  );
}

Future<void> _pumpInWrap(WidgetTester tester, Widget child) {
  return tester.pumpWidget(
    MaterialApp(
      home: Scaffold(
        // A left-aligned, top-anchored Wrap reproduces the real parent: it
        // hands children unbounded space on both axes, which is what exposed
        // the bad stretch. If layout throws, pumpWidget rethrows here.
        body: Align(
          alignment: Alignment.topLeft,
          child: Wrap(children: [child]),
        ),
      ),
    ),
  );
}

void main() {
  testWidgets('renders inside a Wrap with a delete button', (tester) async {
    var deleted = 0;
    await _pumpInWrap(
      tester,
      TagChip(tag: _tag(), onPressed: () {}, onDeleted: () => deleted++),
    );
    expect(tester.takeException(), isNull);
    expect(find.text('example'), findsOneWidget);
    expect(find.byIcon(Icons.close), findsOneWidget);
  });

  testWidgets('renders inside a Wrap without a delete button', (tester) async {
    await _pumpInWrap(tester, TagChip(tag: _tag(), onPressed: () {}));
    expect(tester.takeException(), isNull);
    expect(find.byIcon(Icons.close), findsNothing);
  });

  // The delete cell must own the pill's full height: a tap near the very top
  // and very bottom of the X's column should still untag, not open the tag.
  testWidgets('delete target spans the full chip height', (tester) async {
    var deleted = 0;
    var opened = 0;
    await _pumpInWrap(
      tester,
      TagChip(
        tag: _tag(name: 'tall-enough'),
        onPressed: () => opened++,
        onDeleted: () => deleted++,
      ),
    );
    expect(tester.takeException(), isNull);

    final chipRect = tester.getRect(find.byType(TagChip));
    final xCenterX = tester.getCenter(find.byIcon(Icons.close)).dx;

    // Just inside the top and bottom edges of the pill, at the X's column.
    await tester.tapAt(Offset(xCenterX, chipRect.top + 1));
    await tester.tapAt(Offset(xCenterX, chipRect.bottom - 1));
    await tester.pump();

    expect(deleted, 2, reason: 'top/bottom of the X column should untag');
    expect(opened, 0, reason: 'those taps must not open the tag');
  });

  // Exercise every shape/border/shadow permutation through layout — the dashed
  // and shadow branches use CustomPaint, a different subtree than the Material
  // branch, and must also survive the Wrap's unbounded constraints.
  testWidgets('all style permutations lay out', (tester) async {
    for (final shape in kTagShapes) {
      for (final border in kTagBorderStyles) {
        for (final shadow in [false, true]) {
          await _pumpInWrap(
            tester,
            TagChip(
              tag: _tag(shape: shape, borderStyle: border, shadow: shadow),
              onPressed: () {},
              onDeleted: () {},
            ),
          );
          expect(
            tester.takeException(),
            isNull,
            reason: 'shape=$shape border=$border shadow=$shadow threw',
          );
        }
      }
    }
  });
}
