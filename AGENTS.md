# AGENTS.md

## Project

Tagsy is a file synchronization system with tag-based organization. The
repo is a Cargo workspace (`tagsy-core`, `tagsyd`, `tagsy`,
`tagsy-bridge`) plus a Flutter app under `app/`.

## Version control

This repo uses [`jj`](https://jj-vcs.github.io/jj/). Use the `jj` CLI for all
version-control operations; do not invoke `git` directly.

## Build / run

Use the helper apps defined in `flake.nix` instead of raw `cargo` / `flutter`
invocations:

- `nix run .#codegen` — regenerate the Dart↔Rust bindings.
- `nix run .#run-android` — codegen + native `.so` + launch on Android.
- `nix run .#launch-android` — fast path, no rebuild.
- `nix run .#run-android-clean` — uninstall first (wipes local data), then run.
- `nix run .#build-native-android` — just cross-compile the native `.so`.
- `nix run .#run-linux` — codegen + launch the Linux desktop app.
- `nix run .#launch-linux` — fast path, no codegen.

See `flake.nix` for the full list and required env vars (e.g.
`TAGSY_CONFIG`, `TAGSY_DEVICE`). `TAGSY_BACKUP_DIR` is where `tagsy backup`
writes archives; unlike `TAGSY_DATA_DIR` it may be unset, in which case backup
is unavailable.

## Architecture

`tagsyd` is a message-passing actor system with a ports-and-adapters frontend.
**The module boundaries are the actor boundaries.** There are four state-owning
actors, each with exactly one inbox:

| Actor | Owns | Inbox |
|---|---|---|
| `CatalogWriter` (`catalog/`) | the only `&mut CatalogStore` | `CatalogCommand` |
| `SyncDirectories` (`sync_directories/`) | the filesystem + per-directory indexes | `SyncDirectoryCommand` |
| peer session tasks (`peer/`) | one socket each | `Frame` / `PeerCommand` |
| relay engines (`peer/relay/`) | the waiter tables | (shared `Arc`) |

The central invariant of the whole system: **only `CatalogWriter` writes the
main catalog.** Aligning modules with actors is what makes that checkable from
`pub` markers rather than by convention. An inbox is named after its actor
(`CatalogCommand` / `SyncDirectoryCommand` / `PeerCommand`) so a channel's type
tells you who is on the other end.

The **catalog** is the authoritative index of what exists — files, tags, the
graph, versions, previews — kept independently of the content-addressed bytes it
describes. `CatalogWriter` (actor) / `CatalogStore` (persistence) /
`CatalogCommand` (inbox) share the `Catalog` stem, each distinguished by a role
suffix.

The secondary axis is **pure vs. I/O-driving**: the pure kernels (reconcile,
query lexer, tag rules, path validation, LWW semantics) are named as their own
modules and hold essentially all the tests; dispatch and I/O layers are thin.

### The mirrored API surface

The API surface is deliberately expressed six times over: `ApiService` →
`ControlRequest` → `dispatch` → `ControlResponse` → `IpcBackend` (client) →
`Backend` forward, plus the CLI and bridge. Each level is meant to be
independently readable. **Do not** collapse it into a code-generating macro.
Hand-written forwarding is the price: keep every `impl Backend for AnyBackend`
method in trait-declaration order, forwarding to its own same-named method — the
compiler checks presence and types but not that identically-typed forwards reach
the right target.

## Naming conventions

- **American spelling everywhere**, in identifiers and prose alike (`initialize`,
  `normalize`, `serialize`). A reader should never have to try both spellings
  when grepping.
- **Name types after the pattern's responsibility, not the pattern.** Avoid
  `-Manager`, `-Service` (when it means nothing), `Rich-`/`Any-`-as-filler.
  Plural-of-type (`Operations`) reads like `Vec<Operation>`; use `-Registry` for
  a live registry.
- **Match a module's name to the type it exports** (`configuration/` exports
  `Configuration`, not an abbreviated `config/`).
- **"Chunk" always means a 64 KiB byte range** across the transfer stack; the
  query lexer's unit is a `Token`.
- **"Placement"** is the one word for content-to-directory routing (not
  "target"/"dispatch"). **"Plan" vs "apply"**: a `plan_*` function computes a
  delta and is pure (no `.await`); an `apply_*` function mutates.
- A handle-taking method suffixes the *handle* variant, never the common path:
  `move_file(String)` and `move_file_by_id(FileId)`.

## DTO layering

Each boundary a value crosses has a suffix that tells you what is safe to change:

| Layer | Suffix | Example |
|---|---|---|
| core domain / wire | none, or `-Info` | `FileInfo`, `Tag`, `Preview` |
| FFI DTO | `-Entry` | `FileEntry`, `TagEntry`, `OperationEntry` |
| CLI presentation | `-Row` | `FileRow`, `TagRow` |

This layering is deliberate; do not collapse it. The bridge speaks Dart's types:
`String` in, DTOs out, no Rust handles except genuinely long-lived resources
(`Tagsy`, subscriptions).

## Crate graph

Dependencies point one way: `tagsy-core` (ids, paths, wire protocol) ←
`tagsy-api` (the port: `Backend` trait, `ApiError`, every DTO crossing it) ←
`tagsy-ipc` (protocol + codec + `IpcBackend` client) ← `tagsy` (CLI). `tagsyd`
depends on all three and holds the server half. **The CLI must not depend on
`tagsyd`** — that keeps "the CLI cannot reach behind the daemon's back" a
compiler-enforced fact and keeps the preview-generation stack out of the CLI
build.

## Content-addressed transfers

All byte movement is **one mechanism**, and the reason it collapses to one is a
single idea: **a chunk is pure content, not a message.** Its identity is
`(file_id, content_hash, offset)`, and because the whole-file hash pins the exact
byte sequence, that key denotes one bit-identical range on every peer that holds
the content. Everything else follows from taking that seriously —
point-to-point pull, relayed fetch, and the "who has it?" probe are not separate
features but the same request seen from different distances. When you're tempted
to add a transfer-session, a correlation id, a retry counter, or a discovery
message, that's the signal you've stopped treating chunks as content; find the
version of the change that doesn't.

The receive path deliberately has **no retries and no re-flooding.** This isn't
an omission — it falls out of the transport (WebSocket-over-TCP has no silent
loss, so every failure is *structural*: the content is gone, superseded, or the
link is dead) and out of content-addressing (a fresh holder is already tried on
the *next* chunk, so multi-source needs no retry). Recovery is **external**: a
newer version announcement or a reconnect→reconcile, never an in-transfer retry
loop. Keep new failure handling on that side of the line — the transfer itself
only ever fails fast, guarded by one liveness timeout so it can't hang forever.

Integrity is checked **once, end-to-end**, by the receiver against the full-file
hash — never per-chunk and never on relays (relays hold no bytes and verify
nothing). This rests on a deliberate trust assumption: all peers are the **same
user, mutually authenticated at handshake**, so a bad chunk is a bug to catch at
final verification, not an attack to attribute. Don't add per-chunk hashing or
blame machinery; if the trust model ever changes, that's the design decision to
revisit, not a local patch.

## Reconciliation is additive, per-entry, and idempotent

Manifest reconciliation (`peer/plan.rs`, `peer/plan_tags.rs`) decides each entry
against the local DB alone — no cross-entry state, and **absence never implies
anything** (deletes/restores are explicit LWW-stamped flags carried *on* the
entry, never inferred from a file being missing). Every applied change is
last-writer-wins, so re-applying it is a no-op. Three properties follow, and
must be preserved:

- **Splittable**: a manifest can be sent in any number of frames grouped any
  way — the connection path batches it (`manifest_batch_size` /
  `tag_manifest_batch_size`) to stay under the WebSocket size ceiling. Never add
  a "complete set" assumption to a manifest handler.
- **Additive**: a frame only ever *adds* knowledge; a peer that never mentions a
  file simply says nothing about it.
- **Idempotent**: a partial/duplicated exchange (dropped link, reconnect) just
  converges on the next full exchange; there is no in-session retry or "did I
  get everything?" bookkeeping.

The same discipline as the transfer stack: when tempted to add sequencing, a
completeness check, or a retry, find the version of the change that doesn't.

## Backup

`tagsy backup` produces a single `*.tar.zst` archive of the entire restorable
state — both SQLite databases plus the full byte contents of every sync
directory — into `TAGSY_BACKUP_DIR`. It flows through the full mirrored API
surface like any other operation (`Backend` → `ControlRequest` → `dispatch` →
`ApiService::backup` → `ControlResponse` → IPC → CLI). **The daemon is the only
process that can take a consistent snapshot** while running, because it owns the
write handles to both databases; a client-side file copy could race a mid-write
DB.

Databases are snapshotted with SQLite's `VACUUM INTO`, which yields a
transactionally consistent copy from a live connection (never a torn page)
without quiescing the daemon — plain file copies are rejected for the live path.
The archive **excludes** the identity private key and the configuration file on
purpose: a backup is a movable artifact and must not bundle a secret, and config
is operator-managed. The transient `fetch-temp` scratch directory is excluded
too.

The two databases are snapshotted independently and each sync directory is
walked separately, so an archive is **not** a single global point-in-time
snapshot across all artifacts. This is acceptable by design: startup
reconciliation (`initial_sync_*`) already resolves catalog-vs-filesystem drift,
so a backup only needs each piece to be internally consistent, not mutually
atomic. Don't add cross-artifact locking to "fix" this.

The DB snapshots are plain SQLite files at whatever schema version they were
taken; a restored archive is walked forward on startup by the same
`store/schema.rs` migration chain (see *Database schema versioning*), so no
version stamp lives in the archive itself.

## Database schema versioning

There are two SQLite databases, both owned by `tagsyd/src/store/`:

- the **main catalog** (`CatalogStore`) — `files_v2`, `tags_v2`,
  `entries_v1`, `file_versions_v1`, `previews_v1`.
- a **per-sync-directory index** (`DirectoryIndex`) — one database per sync
  directory, holding a single `files_v1` table. Unrelated to the main
  catalog's former `files_v1`.

Every table name carries a version suffix. All `CREATE TABLE` statements and
all migrations live in `store/schema.rs`, for both databases — that one file
is the entire schema. The SQL that *reads and writes* each table lives in the
module that owns it:

| Table | Owning module |
|---|---|
| `files_v2` | `store/files.rs` |
| `tags_v2` | `store/tags.rs` |
| `entries_v1` | `store/entries.rs` |
| `file_versions_v1` | `store/versions.rs` |
| `previews_v1` | `store/previews.rs` |
| `files_v1` (per-directory) | `store/directory_index.rs` |

Three modules also touch a table they don't own — check them too when
versioning one:

- `store/entries.rs` `LEFT JOIN`s `tags_v2` in its three tag-returning
  traversals, to drop tombstoned tags from a walk.
- `store/files.rs` `JOIN`s `file_versions_v1` to resolve each file's latest
  version.
- `store/short_id.rs` names `files_v2` and `tags_v2` as string arguments
  rather than in SQL (see step 6 below).

`store/query.rs` issues no SQL at all; it composes the primitives above.

Migrations are free functions in `store::schema` taking `&Connection`, called
in sequence from `CatalogStore::initialize` and `DirectoryIndex::initialize`.
**Two exist today:**

- `migrate_files_to_v2` — main catalog `files_v1` → `files_v2`, adding the
  `restored_at` clock.
- `migrate_tags_to_v2` — main catalog `tags_v1` → `tags_v2`, adding the nine
  tag-style columns (`background`, `gradient`, `foreground`, `border`,
  `border_width`, `border_style`, `shape`, `shadow`, `shadow_color`) and
  renaming the old `color` column to `dot_color`. A tag's style is these ten
  properties together (the single `tagsy_core::TagStyle`); each is a concrete
  stored value with a default — nothing is derived at render time, so every
  frontend renders identically. The old `color` becomes `dot_color`; the rest
  take defaults, reproducing a migrated tag's previous dot-only look.

Every other table is still at its first version and has no migration function.

Because both databases share `schema.rs`, the per-directory creator is named
`create_directory_files_v1` to keep it distinct from the main catalog's
`files_v1` that `migrate_files_to_v2` walks forward. Keep that prefix if you
version that table.

### Adding a new schema version

When the schema needs to change again — steps 1–4 all happen in
`store/schema.rs`:

1. Rename `create_<table>_vN` to `create_<table>_v<N+1>` and adjust the
   `CREATE TABLE` statement inside it.
2. Don't modify any existing `migrate_*_to_vN` function. They are frozen so
   that any backup at version `N` can still be restored on a newer build and
   walked forward through every intermediate version.
3. For each table whose schema changes, add a `migrate_<table>_to_v<N+1>`
   function alongside the existing ones. It should:
   - Do nothing if `<table>_vN` doesn't exist.
   - Otherwise create `<table>_v<N+1>`, `INSERT INTO <table>_v<N+1> SELECT ...
     FROM <table>_vN` (with whatever column translation the schema change
     requires), then `DROP TABLE <table>_vN`.
4. Call the new function from the relevant `initialize` — `CatalogStore` or
   `DirectoryIndex` — **after** the N-1 migration and **before** the
   `create_*` calls.
5. Update every SQL literal that references the changed table from
   `<table>_vN` to `<table>_v<N+1>`. Start with the owning module from the
   table above, then grep the old suffix across `tagsyd/src/store/` to
   confirm you got them all; it should survive only inside the frozen
   `migrate_*_to_vN` functions.
6. If `files_v2` or `tags_v2` changed, update the four hardcoded table names
   in `store/short_id.rs` — two `resolve_id_prefix` calls and two
   `shortest_unique_prefix_length` calls. They take the table name as a
   string argument, so the grep in step 5 still finds them.

This chain lets a restored v1 backup on a v3 build migrate v1 → v2 → v3
on startup, permanently.
