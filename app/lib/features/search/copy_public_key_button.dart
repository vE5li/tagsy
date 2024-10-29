// Android-only: AppBar action that copies this device's public key to the
// clipboard. Rendered by the home screen only when the session carries a key.

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';

/// The home AppBar's "copy public key" button (Android only).
class CopyPublicKeyButton extends StatelessWidget {
  const CopyPublicKeyButton({super.key, required this.publicKey});

  final String publicKey;

  @override
  Widget build(BuildContext context) {
    return IconButton(
      icon: const Icon(Icons.copy),
      tooltip: 'Copy public key',
      onPressed: () async {
        await Clipboard.setData(ClipboardData(text: publicKey));
      },
    );
  }
}
