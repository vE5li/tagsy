// An [IconButton] that swaps its icon for a spinner while an action is in
// flight, and disables itself for the duration.
//
// The file-detail AppBar has four actions (restore, download, edit, share) that
// each repeated the same `busy ? SizedBox(CircularProgressIndicator) : Icon`
// body plus a `busy ? null : handler` gate. This captures that one pattern.

import 'package:flutter/material.dart';

/// An icon button with a built-in busy state.
///
/// While [busy] is true it shows a small [CircularProgressIndicator] in place
/// of [icon] and ignores taps; otherwise it behaves as a normal [IconButton]
/// calling [onPressed]. A null [onPressed] disables the button independently of
/// [busy] (e.g. no session yet).
class BusyIconButton extends StatelessWidget {
  const BusyIconButton({
    super.key,
    required this.busy,
    required this.icon,
    required this.tooltip,
    required this.onPressed,
  });

  /// Whether the action is currently running. Shows the spinner and blocks
  /// taps.
  final bool busy;

  /// The icon shown when not [busy].
  final IconData icon;

  final String tooltip;

  /// Tapped when idle. Null disables the button regardless of [busy].
  final VoidCallback? onPressed;

  @override
  Widget build(BuildContext context) {
    return IconButton(
      icon: busy
          ? const SizedBox(
              width: 20,
              height: 20,
              child: CircularProgressIndicator(strokeWidth: 2),
            )
          : Icon(icon),
      tooltip: tooltip,
      onPressed: busy ? null : onPressed,
    );
  }
}
