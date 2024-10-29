// The search text field at the top of the home screen.
//
// Named `SearchField` rather than `SearchBar` to avoid colliding with the
// Material `SearchBar` widget.

import 'package:flutter/material.dart';

/// The home screen's query input: a text field with a search prefix icon and a
/// suffix that shows a spinner while a query is in flight or a clear button
/// otherwise.
class SearchField extends StatelessWidget {
  const SearchField({
    super.key,
    required this.controller,
    required this.focusNode,
    required this.loading,
    required this.onSubmitted,
  });

  final TextEditingController controller;
  final FocusNode focusNode;
  final bool loading;

  /// Invoked when the user presses Enter in the field. Wired to the home
  /// screen's submit handler, which either activates the sole result or hands
  /// focus to the first result row, so keyboard users don't have to tab past
  /// the AppBar actions to reach the list.
  final Future<void> Function() onSubmitted;

  @override
  Widget build(BuildContext context) {
    return TextField(
      controller: controller,
      focusNode: focusNode,
      onSubmitted: (_) => onSubmitted(),
      decoration: InputDecoration(
        prefixIcon: const Icon(Icons.search),
        hintText: 'Search files and tags',
        border: const OutlineInputBorder(),
        suffixIcon: loading
            ? const Padding(
                padding: EdgeInsets.all(12),
                child: SizedBox(
                  width: 16,
                  height: 16,
                  child: CircularProgressIndicator(strokeWidth: 2),
                ),
              )
            : (controller.text.isEmpty
                  ? null
                  : IconButton(
                      icon: const Icon(Icons.clear),
                      tooltip: 'Clear',
                      onPressed: () => controller.clear(),
                    )),
      ),
    );
  }
}
