//! The outbox: daemon-owned copies of uploaded content, kept until some other
//! holder has it.
//!
//! An upload or edit through the API (CLI, UI) first copies the file into the
//! outbox — hashing while copying — and only then is announced. From that
//! moment the daemon holds the bytes itself: the caller may delete its source,
//! the daemon may restart, the peers that want the file may connect whenever
//! they like. The outbox serves chunk requests like any other local source,
//! and local placement copies from it.
//!
//! An entry is removed once the content is held elsewhere — in a local sync
//! directory or on a peer — or is no longer needed (purged, or superseded by a
//! newer version); see [`run_release`].
//!
//! The outbox is a plain directory under the data dir, with the filesystem as
//! its only state: an entry is the file `<file_id>.<content_hash>`, written as
//! a `.partial` file, synced, and renamed into place, so a crash never leaves
//! a truncated entry under a real name. Leftover partials are swept on
//! startup ([`Outbox::prepare`]).

use std::path::{Path, PathBuf};

use tagsy_core::FileId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Buffer size for the streaming copy.
const COPY_BUFFER: usize = 64 * 1024;
const PARTIAL_SUFFIX: &str = ".partial";

#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error("failed to read upload source {}: {source}", path.display())]
    Source {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write outbox entry {}: {source}", path.display())]
    Entry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Handle on the outbox directory. Cheap to clone.
#[derive(Debug, Clone)]
pub struct Outbox {
    directory: PathBuf,
}

impl Outbox {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Ensure the directory exists and drop partial copies an interrupted
    /// ingest left behind. Complete entries are kept: they may be the only
    /// copy of their content.
    pub async fn prepare(&self) -> std::io::Result<()> {
        tokio::fs::create_dir_all(&self.directory).await?;
        let mut entries = tokio::fs::read_dir(&self.directory).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry
                .file_name()
                .to_string_lossy()
                .ends_with(PARTIAL_SUFFIX)
            {
                let _ = tokio::fs::remove_file(entry.path()).await;
            }
        }
        Ok(())
    }

    fn entry_path(&self, file_id: FileId, content_hash: &str) -> PathBuf {
        self.directory
            .join(format!("{}.{content_hash}", file_id.to_string()))
    }

    /// Copy `source` into the outbox as a version of `file_id`, returning its
    /// content hash and size. The entry is synced to disk before this returns,
    /// so the caller may delete `source` straight away.
    pub async fn ingest(
        &self,
        source: &Path,
        file_id: FileId,
    ) -> Result<(String, u64), OutboxError> {
        let source_error = |error| OutboxError::Source {
            path: source.to_path_buf(),
            source: error,
        };
        let partial = self
            .directory
            .join(format!("{}{PARTIAL_SUFFIX}", uuid::Uuid::new_v4()));
        let entry_error = |error| OutboxError::Entry {
            path: partial.clone(),
            source: error,
        };

        let mut input = tokio::fs::File::open(source).await.map_err(source_error)?;
        let mut output = tokio::fs::File::create(&partial)
            .await
            .map_err(entry_error)?;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0u8; COPY_BUFFER];
        let mut size = 0u64;
        let copied = async {
            loop {
                let read = input.read(&mut buffer).await.map_err(source_error)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                output
                    .write_all(&buffer[..read])
                    .await
                    .map_err(entry_error)?;
                size += read as u64;
            }
            output.sync_all().await.map_err(entry_error)?;
            Ok::<_, OutboxError>(())
        }
        .await;
        if let Err(error) = copied {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(error);
        }

        let content_hash = hasher.finalize().to_hex().to_string();
        let entry = self.entry_path(file_id, &content_hash);
        tokio::fs::rename(&partial, &entry)
            .await
            .map_err(|error| OutboxError::Entry {
                path: entry.clone(),
                source: error,
            })?;
        // Make the rename itself durable.
        if let Ok(directory) = tokio::fs::File::open(&self.directory).await {
            let _ = directory.sync_all().await;
        }
        Ok((content_hash, size))
    }

    /// The entry holding `content_hash` of `file_id`, if the outbox has it.
    pub fn get(&self, file_id: FileId, content_hash: &str) -> Option<PathBuf> {
        let path = self.entry_path(file_id, content_hash);
        path.is_file().then_some(path)
    }

    pub async fn remove(&self, file_id: FileId, content_hash: &str) {
        let path = self.entry_path(file_id, content_hash);
        if let Err(error) = tokio::fs::remove_file(&path).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            log::warn!("Failed to remove outbox entry {}: {error}", path.display());
        }
    }

    /// Every complete entry, as `(file_id, content_hash)`.
    pub async fn entries(&self) -> Vec<(FileId, String)> {
        let mut out = Vec::new();
        let Ok(mut entries) = tokio::fs::read_dir(&self.directory).await else {
            return out;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some((id, hash)) = name.split_once('.')
                && !hash.ends_with(PARTIAL_SUFFIX)
                && let Some(file_id) = FileId::from_string(id)
            {
                out.push((file_id, hash.to_owned()));
            }
        }
        out
    }
}

/// What the catalog says about an outbox entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatalogVerdict {
    /// The content is no longer needed anywhere: drop the entry.
    Obsolete,
    /// Still the file's current content (or not yet recorded): keep it until
    /// another holder has it.
    Needed,
}

/// Decide an entry from the catalog alone. Purged or deleted → obsolete: a
/// delete removes a file's bytes everywhere (short of a `keep_deleted_files`
/// vault), and the outbox only bridges "not yet held elsewhere", it is no
/// recycle bin. A version *newer* than the entry's in the history → obsolete:
/// sync only ever wants the latest content. An entry whose hash is not in the
/// history yet is still being announced (the ingest raced ahead of the
/// catalog), and an unknown file may be one whose catalog commit was lost —
/// both are kept.
fn catalog_verdict(
    database: &crate::store::CatalogStore,
    file_id: FileId,
    content_hash: &str,
) -> CatalogVerdict {
    if database.is_purged(file_id).unwrap_or(false) {
        return CatalogVerdict::Obsolete;
    }
    if matches!(database.file_deletion_state(file_id), Ok(Some(state)) if state.deleted) {
        return CatalogVerdict::Obsolete;
    }
    let Ok(history) = database.version_history(file_id) else {
        return CatalogVerdict::Needed;
    };
    let recorded = history.iter().any(|(_, hash, _)| hash == content_hash);
    let latest = history.last().map(|(_, hash, _)| hash.as_str());
    if recorded && latest != Some(content_hash) {
        CatalogVerdict::Obsolete
    } else {
        CatalogVerdict::Needed
    }
}

/// Drop every outbox entry that is obsolete or held elsewhere, once.
async fn release_pass(
    outbox: &Outbox,
    relay: &crate::peer::relay::ChunkRelay,
    command_sender: &tokio::sync::mpsc::UnboundedSender<
        crate::sync_directories::SyncDirectoryCommand,
    >,
    main_db_path: &Path,
) {
    let entries = outbox.entries().await;
    if entries.is_empty() {
        return;
    }
    for (file_id, content_hash) in entries {
        let verdict = match crate::store::CatalogStore::initialize(main_db_path) {
            Ok(database) => catalog_verdict(&database, file_id, &content_hash),
            Err(error) => {
                log::warn!("Outbox release: cannot read the catalog ({error:?}); keeping entries");
                return;
            }
        };
        let reason = if verdict == CatalogVerdict::Obsolete {
            Some("no longer needed")
        } else if crate::peer::fetch::read_local_if_hash_matches(
            command_sender,
            file_id,
            &content_hash,
        )
        .await
        .is_some()
        {
            Some("held in a local sync directory")
        } else if crate::peer::fetch::probe_availability(relay, file_id, content_hash.clone()).await
        {
            Some("held by a peer")
        } else {
            None
        };
        if let Some(reason) = reason {
            log::debug!(
                "Outbox: releasing {} [{}]: {reason}",
                file_id.to_string(),
                content_hash.get(..8).unwrap_or(&content_hash)
            );
            outbox.remove(file_id, &content_hash).await;
        }
    }
}

/// Keep releasing outbox entries: once at startup, whenever a peer connects
/// (it may hold, or just have pulled, our uploads), and every `interval` —
/// which covers a peer that pulls an upload while connected. Recovery on the
/// same footing as the connect-time missing-content sweep: no per-entry
/// retries or acknowledgements, just the next pass.
pub(crate) async fn run_release(
    outbox: Outbox,
    relay: crate::peer::relay::ChunkRelay,
    command_sender: tokio::sync::mpsc::UnboundedSender<
        crate::sync_directories::SyncDirectoryCommand,
    >,
    main_db_path: PathBuf,
    connections: crate::connections::Connections,
    interval: std::time::Duration,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let mut connection_events = connections.subscribe();
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = ticks.tick() => {}
            event = connection_events.recv() => match event {
                Ok(tagsy_api::ConnectionEvent::Connected(_))
                | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Ok(_) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            },
        }
        release_pass(&outbox, &relay, &command_sender, &main_db_path).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "tagsy-outbox-test-{label}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn ingest_copies_hashes_and_lists_the_entry() {
        let source_dir = temp_dir("source");
        let source = source_dir.join("upload.bin");
        let bytes: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&source, &bytes).unwrap();

        let outbox = Outbox::new(temp_dir("outbox"));
        outbox.prepare().await.unwrap();
        let file_id = FileId::new();
        let (hash, size) = outbox.ingest(&source, file_id).await.unwrap();

        assert_eq!(hash, blake3::hash(&bytes).to_hex().to_string());
        assert_eq!(size, bytes.len() as u64);
        // The source is no longer needed.
        std::fs::remove_file(&source).unwrap();
        let entry = outbox.get(file_id, &hash).expect("entry exists");
        assert_eq!(std::fs::read(entry).unwrap(), bytes);
        assert_eq!(outbox.entries().await, vec![(file_id, hash.clone())]);

        outbox.remove(file_id, &hash).await;
        assert!(outbox.get(file_id, &hash).is_none());
        assert!(outbox.entries().await.is_empty());
    }

    #[test]
    fn catalog_verdict_keeps_current_and_pending_content() {
        let mut database = crate::store::CatalogStore::initialize(":memory:").unwrap();
        let file_id = FileId::new();
        database
            .add_file(file_id, &tagsy_core::LogicalPath::new("a"), 0)
            .unwrap();
        database.record_version(file_id, "v1", "local", 1).unwrap();

        // The current version is needed; an unrecorded one is still pending.
        assert_eq!(
            catalog_verdict(&database, file_id, "v1"),
            CatalogVerdict::Needed
        );
        assert_eq!(
            catalog_verdict(&database, file_id, "v2"),
            CatalogVerdict::Needed
        );
        // An unknown file is kept (its catalog commit may have been lost).
        assert_eq!(
            catalog_verdict(&database, FileId::new(), "x"),
            CatalogVerdict::Needed
        );

        // Once a newer version is recorded, the old content is obsolete.
        database.record_version(file_id, "v2", "local", 1).unwrap();
        assert_eq!(
            catalog_verdict(&database, file_id, "v1"),
            CatalogVerdict::Obsolete
        );
        assert_eq!(
            catalog_verdict(&database, file_id, "v2"),
            CatalogVerdict::Needed
        );

        // A deleted file needs nothing, nor does a purged one.
        database.remove_file(file_id, i64::MAX).unwrap();
        assert_eq!(
            catalog_verdict(&database, file_id, "v2"),
            CatalogVerdict::Obsolete
        );
        database.record_purge(file_id).unwrap();
        assert_eq!(
            catalog_verdict(&database, file_id, "v2"),
            CatalogVerdict::Obsolete
        );
    }

    #[tokio::test]
    async fn prepare_sweeps_partials_but_keeps_entries() {
        let outbox = Outbox::new(temp_dir("prepare"));
        outbox.prepare().await.unwrap();
        let source = outbox.directory().join("..").join("src.txt");
        std::fs::write(&source, b"keep me").unwrap();
        let file_id = FileId::new();
        let (hash, _) = outbox.ingest(&source, file_id).await.unwrap();
        std::fs::write(outbox.directory().join("stale.partial"), b"x").unwrap();

        outbox.prepare().await.unwrap();
        assert!(!outbox.directory().join("stale.partial").exists());
        assert!(outbox.get(file_id, &hash).is_some());
    }
}
