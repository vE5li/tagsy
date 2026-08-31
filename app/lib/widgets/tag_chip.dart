// Shared widgets for rendering tags and their full visual style.
//
// A tag's style is the ten peer properties carried by [tagsy.TagStyleEntry]
// (dot color, fill, gradient, foreground, border, border width/style, shape,
// shadow, outline). Every property is a concrete stored value — nothing is
// derived here — so this renderer just reads and paints, matching every other
// frontend byte-for-byte.

import 'dart:math' as math;

import 'package:flutter/material.dart';

import '../rust/api.dart' as tagsy;

/// The pill shapes, keyed by the wire/SQL name the core stores
/// (`rounded|stadium|square|cut_corner`).
const List<String> kTagShapes = ['rounded', 'stadium', 'square', 'cut_corner'];

/// The border styles, keyed by the wire/SQL name (`none|solid|dashed`).
const List<String> kTagBorderStyles = ['none', 'solid', 'dashed'];

/// Parse a `#RRGGBB` or `#AARRGGBB` string into a [Color]. Falls back to
/// transparent for anything unparseable — an unset/garbage color should paint
/// nothing rather than a jarring fallback swatch (the dot swatch below opts
/// into grey instead for visibility).
///
/// The 8-digit form is **alpha-first** (`AARRGGBB`), matching Flutter's own
/// `Color(0xAARRGGBB)` and [formatTagColor]. This is the canonical stored
/// format across the app; 6-digit input is promoted to fully opaque.
Color parseTagColor(String value) {
  var hex = value.trim();
  if (hex.startsWith('#')) hex = hex.substring(1);
  if (hex.length == 6) hex = 'FF$hex';
  final parsed = int.tryParse(hex, radix: 16);
  if (parsed == null) return Colors.transparent;
  return Color(parsed);
}

/// Format a [Color] as an uppercase `#AARRGGBB` string — the inverse of
/// [parseTagColor], and the canonical form persisted for every style color.
String formatTagColor(Color color) {
  int channel(double c) => (c * 255).round().clamp(0, 255);
  final a = channel(color.a);
  final r = channel(color.r);
  final g = channel(color.g);
  final b = channel(color.b);
  final value = (a << 24) | (r << 16) | (g << 8) | b;
  return '#${value.toRadixString(16).padLeft(8, '0').toUpperCase()}';
}

/// A round color swatch, optionally showing a selection ring. Used by the style
/// editor and pickers to preview a single color. Unparseable input shows grey
/// (rather than transparent) so the swatch is always visible.
class TagColorSwatch extends StatelessWidget {
  const TagColorSwatch({super.key, required this.color, this.selected = false});

  final String color;
  final bool selected;

  @override
  Widget build(BuildContext context) {
    final parsed = parseTagColor(color);
    return Container(
      width: 28,
      height: 28,
      decoration: BoxDecoration(
        color: parsed == Colors.transparent ? Colors.grey : parsed,
        shape: BoxShape.circle,
        border: selected
            ? Border.all(
                color: Theme.of(context).colorScheme.onSurface,
                width: 3,
              )
            : Border.all(color: Colors.black26),
      ),
    );
  }
}

/// A styled tag pill honoring the tag's full [tagsy.TagStyleEntry].
///
/// [onPressed] fires when the pill body is tapped (e.g. to open the tag
/// detail); [onDeleted] fires when the trailing X is tapped (untag). Either or
/// both can be null.
class TagChip extends StatelessWidget {
  const TagChip({super.key, required this.tag, this.onPressed, this.onDeleted});

  final tagsy.TagEntry tag;
  final VoidCallback? onPressed;
  final VoidCallback? onDeleted;

  @override
  Widget build(BuildContext context) {
    final style = tag.style;
    final background = parseTagColor(style.background);
    final gradientColor = parseTagColor(style.gradient);
    final foreground = parseTagColor(style.foreground);
    final borderColor = parseTagColor(style.border);
    final dotColor = parseTagColor(style.dotColor);

    // A gradient only applies when the background is a real (non-transparent)
    // fill and the gradient stop differs from it — matching the core's rule.
    final hasGradient =
        background != Colors.transparent && gradientColor != background;
    final gradient = hasGradient
        ? LinearGradient(
            colors: [background, gradientColor],
            begin: Alignment.centerLeft,
            end: Alignment.centerRight,
          )
        : null;

    final fillColor = background;
    final showBorder =
        style.borderStyle != 'none' && borderColor != Colors.transparent;

    final label = DefaultTextStyle.merge(
      style: TextStyle(
        color: foreground,
        fontWeight: FontWeight.w600,
        fontSize: 13,
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          if (dotColor != Colors.transparent) ...[
            Container(
              width: 10,
              height: 10,
              decoration: BoxDecoration(
                color: dotColor,
                shape: BoxShape.circle,
              ),
            ),
            const SizedBox(width: 6),
          ],
          Text(tag.name),
          if (onDeleted != null) ...[
            const SizedBox(width: 4),
            InkWell(
              onTap: onDeleted,
              child: Icon(Icons.close, size: 14, color: foreground),
            ),
          ],
        ],
      ),
    );

    final content = Padding(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 5),
      child: label,
    );

    // Dashed borders need a painter (Flutter's ShapeBorder can't dash a
    // stroke); everything else uses a Material shape.
    final dashed = style.borderStyle == 'dashed';
    Widget pill;
    if (dashed) {
      pill = CustomPaint(
        painter: _DashedPillPainter(
          shape: style.shape,
          fillColor: fillColor,
          gradient: gradient,
          borderColor: showBorder ? borderColor : null,
          borderWidth: style.borderWidth,
        ),
        child: content,
      );
    } else {
      pill = Material(
        color: fillColor,
        shape: _shapeBorder(
          style.shape,
          showBorder
              ? BorderSide(color: borderColor, width: style.borderWidth)
              : BorderSide.none,
        ),
        clipBehavior: Clip.antiAlias,
        child: DecoratedBox(
          decoration: BoxDecoration(gradient: gradient),
          child: content,
        ),
      );
    }

    if (style.shadow) {
      pill = CustomPaint(
        painter: _ShapeShadowPainter(
          shape: style.shape,
          color: parseTagColor(style.shadowColor),
          blurRadius: 4,
          offset: const Offset(0, 3),
        ),
        child: pill,
      );
    }

    return InkWell(
      onTap: onPressed,
      borderRadius: BorderRadius.circular(20),
      child: pill,
    );
  }
}

/// Build a [ShapeBorder] for a tag shape name, with the given side.
ShapeBorder _shapeBorder(String shape, BorderSide side) {
  switch (shape) {
    case 'rounded':
      return RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(8),
        side: side,
      );
    case 'square':
      return RoundedRectangleBorder(
        borderRadius: BorderRadius.zero,
        side: side,
      );
    case 'cut_corner':
      return BeveledRectangleBorder(
        borderRadius: const BorderRadius.all(Radius.circular(10)),
        side: side,
      );
    case 'stadium':
    default:
      return StadiumBorder(side: side);
  }
}

/// Paints a filled pill with a dashed border following the tag's shape (the one
/// case Flutter's ShapeBorder can't express). Reuses each shape's own
/// [ShapeBorder.getOuterPath] so the dashes trace the exact outline.
class _DashedPillPainter extends CustomPainter {
  _DashedPillPainter({
    required this.shape,
    required this.fillColor,
    required this.gradient,
    required this.borderColor,
    required this.borderWidth,
  });

  final String shape;
  final Color fillColor;
  final Gradient? gradient;
  final Color? borderColor;
  final double borderWidth;

  @override
  void paint(Canvas canvas, Size size) {
    final rect = Offset.zero & size;
    final path = _shapeBorder(shape, BorderSide.none).getOuterPath(rect);

    if (gradient != null) {
      canvas.drawPath(path, Paint()..shader = gradient!.createShader(rect));
    } else if (fillColor != Colors.transparent) {
      canvas.drawPath(path, Paint()..color = fillColor);
    }

    if (borderColor != null) {
      final stroke = Paint()
        ..style = PaintingStyle.stroke
        ..strokeWidth = borderWidth
        ..color = borderColor!;
      _drawDashed(canvas, path, stroke);
    }
  }

  void _drawDashed(Canvas canvas, Path source, Paint paint) {
    const dash = 5.0;
    const gap = 3.0;
    for (final metric in source.computeMetrics()) {
      double distance = 0;
      while (distance < metric.length) {
        final next = distance + dash;
        canvas.drawPath(
          metric.extractPath(distance, math.min(next, metric.length)),
          paint,
        );
        distance = next + gap;
      }
    }
  }

  @override
  bool shouldRepaint(_DashedPillPainter old) =>
      old.shape != shape ||
      old.fillColor != fillColor ||
      old.borderColor != borderColor ||
      old.borderWidth != borderWidth;
}

/// Paints a blurred drop shadow that follows the tag's own shape, rather than a
/// fixed rounded rectangle. Reuses each shape's [ShapeBorder.getOuterPath] — the
/// same outline the fill and border trace — so the shadow matches exactly.
class _ShapeShadowPainter extends CustomPainter {
  _ShapeShadowPainter({
    required this.shape,
    required this.color,
    required this.blurRadius,
    required this.offset,
  });

  final String shape;
  final Color color;
  final double blurRadius;
  final Offset offset;

  @override
  void paint(Canvas canvas, Size size) {
    final rect = Offset.zero & size;
    final path = _shapeBorder(shape, BorderSide.none).getOuterPath(rect);
    final paint = Paint()
      ..color = color
      ..maskFilter = MaskFilter.blur(BlurStyle.normal, blurRadius);
    canvas.drawPath(path.shift(offset), paint);
  }

  @override
  bool shouldRepaint(_ShapeShadowPainter old) =>
      old.shape != shape ||
      old.color != color ||
      old.blurRadius != blurRadius ||
      old.offset != offset;
}

/// A default [tagsy.TagStyleEntry] with the given dot color, matching the core's
/// `TagStyle::default()`. Used when creating a tag with just a chosen dot color.
tagsy.TagStyleEntry defaultTagStyle({String dotColor = '#000000'}) {
  return tagsy.TagStyleEntry(
    dotColor: dotColor,
    background: '#FFFFFF',
    gradient: '#FFFFFF',
    foreground: '#000000',
    border: '#00000000',
    borderWidth: 1.5,
    borderStyle: 'solid',
    shape: 'stadium',
    shadow: false,
    shadowColor: '#80000000',
  );
}
