// The single seam between the shared UI and the generated Rust bridge.
//
// Every screen talks to a [TagsyRepository] instead of importing
// `rust/api.dart` and calling the opaque [tagsy.Tagsy] handle directly.
// Concentrating the ~40 bridge calls here means the widget tree names the
// operations it needs, not the FFI handle that happens to serve them: the
// bridge can be re-shaped (or faked in a test) without touching a screen.
//
// This is deliberately *thin*. It forwards each call one-for-one and returns
// the bridge's own DTOs (`FileEntry`, `TagEntry`, `QueryEntries`, …) and enums
// (`DeletedRule`, `SubtagRule`, `PreviewKind`) unchanged — those are plain
// data classes the UI reads directly, so re-wrapping them would buy nothing.
// The one thing it hides is the handle itself.

import '../rust/api.dart' as tagsy;

/// A connected backend as a set of named operations.
///
/// Wraps the live [tagsy.Tagsy] handle (in-process engine on Android, daemon
/// IPC on Linux — the repository does not care which). Held by [TagsySession]
/// and reached through it by every screen.
class TagsyRepository {
  TagsyRepository(this._client);

  final tagsy.Tagsy _client;

  // --- Search / lookup ------------------------------------------------------

  /// The files and tags matching a free-form `query` (`$tag`, `!tag`, name
  /// substrings), as flattened rows.
  Future<tagsy.QueryEntries> runQuery({
    required String query,
    required tagsy.SubtagRule subtagRule,
    required tagsy.DeletedRule deletedRule,
  }) => _client.runQuery(
    query: query,
    subtagRule: subtagRule,
    deletedRule: deletedRule,
  );

  /// A single file's flattened row by id string (full or short prefix).
  Future<tagsy.FileEntry> getFileEntry({
    required String fileId,
    required tagsy.DeletedRule deletedRule,
  }) => _client.getFileEntry(fileId: fileId, deletedRule: deletedRule);

  /// A single tag's flattened row by id string (full or short prefix).
  Future<tagsy.TagEntry> getTagEntry({
    required String tagId,
    required tagsy.DeletedRule deletedRule,
  }) => _client.getTagEntry(tagId: tagId, deletedRule: deletedRule);

  /// The tag ids applied to a file.
  Future<List<String>> tagIdsForFile({
    required String fileId,
    required tagsy.SubtagRule subtagRule,
  }) => _client.tagIdsForFile(fileId: fileId, subtagRule: subtagRule);

  /// The parent tag ids of a tag.
  Future<List<String>> tagIdsForTag({
    required String tagId,
    required tagsy.SubtagRule subtagRule,
  }) => _client.tagIdsForTag(tagId: tagId, subtagRule: subtagRule);

  /// The subtag (child) ids of a tag.
  Future<List<String>> subtagIdsForTag({
    required String tagId,
    required tagsy.SubtagRule subtagRule,
  }) => _client.subtagIdsForTag(tagId: tagId, subtagRule: subtagRule);

  /// Absolute on-disk path where a file's bytes live locally, or `null` if no
  /// local sync directory holds a copy.
  Future<String?> localPathForFile({required String fileId}) =>
      _client.localPathForFile(fileId: fileId);

  // --- Tags -----------------------------------------------------------------

  /// Create a tag; returns the freshly-minted id string.
  Future<String> createTag({required String name, required String color}) =>
      _client.createTag(name: name, color: color);

  /// Rename a tag.
  Future<void> renameTag({required String tagId, required String name}) =>
      _client.renameTag(tagId: tagId, name: name);

  /// Change a tag's color.
  Future<void> setTagColor({required String tagId, required String color}) =>
      _client.setTagColor(tagId: tagId, color: color);

  /// Delete a tag.
  Future<void> deleteTag({required String tagId}) =>
      _client.deleteTag(tagId: tagId);

  /// Restore a soft-deleted tag.
  Future<void> restoreTag({required String tagId}) =>
      _client.restoreTag(tagId: tagId);

  /// Apply a tag to a file.
  Future<void> tagFile({required String tagId, required String fileId}) =>
      _client.tagFile(tagId: tagId, fileId: fileId);

  /// Remove a tag from a file.
  Future<void> untagFile({required String tagId, required String fileId}) =>
      _client.untagFile(tagId: tagId, fileId: fileId);

  /// Make `subtagId` a subtag (child) of `parentId`.
  Future<void> tagTag({required String parentId, required String subtagId}) =>
      _client.tagTag(parentId: parentId, subtagId: subtagId);

  /// Remove `subtagId` as a subtag of `parentId`.
  Future<void> untagTag({required String parentId, required String subtagId}) =>
      _client.untagTag(parentId: parentId, subtagId: subtagId);

  // --- Files ----------------------------------------------------------------

  /// Upload a file from a path on disk; returns the freshly-minted id.
  Future<String> uploadFile({
    required String path,
    required String pathName,
    required List<String> tags,
  }) => _client.uploadFile(path: path, pathName: pathName, tags: tags);

  /// Move (rename) a file to a new logical path.
  Future<void> moveFile({
    required String fileId,
    required String logicalPath,
  }) => _client.moveFile(fileId: fileId, logicalPath: logicalPath);

  /// Delete a file.
  Future<void> deleteFile({required String fileId}) =>
      _client.deleteFile(fileId: fileId);

  /// Restore a soft-deleted file (best-effort).
  Future<void> restoreFile({required String fileId}) =>
      _client.restoreFile(fileId: fileId);

  /// Fetch a file's content on demand and return the path to a daemon-owned
  /// temp file holding the bytes (move semantics — the caller must consume it).
  Future<String> fetchFile({
    required String fileId,
    required String expectedHash,
  }) => _client.fetchFile(fileId: fileId, expectedHash: expectedHash);

  // --- Editing --------------------------------------------------------------

  /// Start an external edit; returns the on-disk path to hand to an editor.
  Future<String> beginEdit({required String fileId}) =>
      _client.beginEdit(fileId: fileId);

  /// Complete an external edit; `true` if a new version was published.
  Future<bool> finishEdit({required String fileId, required String path}) =>
      _client.finishEdit(fileId: fileId, path: path);

  /// Abort an external edit without publishing.
  Future<void> cancelEdit({required String path}) =>
      _client.cancelEdit(path: path);

  /// The daemon's configured external-editor rules.
  Future<List<tagsy.EditorRuleEntry>> editorRules() => _client.editorRules();

  // --- Previews -------------------------------------------------------------

  /// The preview for a file's current content.
  Future<tagsy.PreviewEntry> getPreview({required String fileId}) =>
      _client.getPreview(fileId: fileId);

  /// Purge the daemon's cached previews; returns how many were removed.
  Future<BigInt> purgePreviews() => _client.purgePreviews();

  // --- Stats ----------------------------------------------------------------

  /// Local-vs-total storage totals.
  Future<tagsy.StorageStatsEntry> storageStats() => _client.storageStats();

  // --- Streams --------------------------------------------------------------

  /// Subscribe to the live change stream. Poll the returned subscription with
  /// `next()`, which yields a readable [tagsy.ApiEventDto] so a screen can
  /// filter by the affected file/tag id instead of reloading on every change.
  Future<tagsy.EventSubscription> subscribe() => _client.subscribe();

  /// Snapshot every currently-active sync operation.
  Future<List<tagsy.OperationEntry>> listOperations() =>
      _client.listOperations();

  /// Subscribe to the live sync-operation stream.
  Future<tagsy.OperationSubscription> subscribeOperations() =>
      _client.subscribeOperations();
}
