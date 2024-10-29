// The connected backend, as the shared UI sees it.
//
// Every screen imports this type: it carries the [TagsyRepository] the UI
// drives plus the handful of platform-provided extras (public key, downloads
// dir, editor launcher). It lived under `bootstrap/` originally, but the
// bootstrap layer is a platform-selection detail the screens must not depend
// on — they only need the *result* of a bootstrap. Keeping [TagsySession] in
// its own leaf module lets a screen import it without pulling in the Android /
// Linux bootstrap machinery.

import '../data/repository.dart';
import '../editor/editor_launcher.dart';

/// A connected backend, ready for the shared UI to drive.
class TagsySession {
  /// The repository every screen calls into (search/create/tag/upload/...).
  final TagsyRepository repository;

  /// This device's base64 public key, or `null` when the platform has no local
  /// identity to show (Linux, where the daemon owns the identity).
  final String? publicKey;

  /// Absolute path of the device's public Downloads directory, or `null` when
  /// the platform has no such concept exposed to the app (Linux/desktop). Used
  /// by the file detail screen's mobile-only "download" button; its nullness
  /// doubles as the mobile-only gate, like [publicKey].
  final String? downloadsDir;

  /// External-editor launcher for this platform, or `null` if editing files
  /// in an external app is not supported on this platform. Currently
  /// non-null on both Android (ACTION_EDIT via a FileProvider URI) and Linux
  /// (child process from a daemon-configured rule or $VISUAL/$EDITOR). The
  /// file detail screen's "Edit" button is hidden when this is null.
  final EditorLauncher? editorLauncher;

  const TagsySession({
    required this.repository,
    this.publicKey,
    this.downloadsDir,
    this.editorLauncher,
  });
}
