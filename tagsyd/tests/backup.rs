//! End-to-end coverage for `ApiService::backup`: build an archive over a temp
//! data dir + a temp sync directory, then assert the `*.tar.zst` exists, is
//! non-empty, untars, and contains the two databases, the sync files, and the
//! manifest.
//!
//! The backup path asks the sync-directory actor for the live directory set, so
//! the test spawns a tiny stand-in that answers `ListDirectories` off the
//! command channel — the only actor message `backup` sends. Running the full
//! `SyncDirectories` loop would add nothing this test observes.

use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use tagsyd::configuration::{
    CompiledTagRules, Configuration, PreviewGenerationPolicy, RuntimeConfiguration, SyncDirectory,
    SyncType,
};
use tagsyd::frontend::api::ApiService;
use tagsyd::operations::Operations;
use tagsyd::paths::Paths;
use tagsyd::peer::relay::ChunkRelay;
use tagsyd::store::{CatalogStore, DirectoryIndex};
use tagsyd::sync_directories::SyncDirectoryCommand;
use tokio::sync::RwLock;

/// A unique temp directory for a test, created eagerly.
fn temp_dir(label: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let base = std::env::temp_dir().join(format!(
        "tagsy-backup-test-{}-{}-{}",
        label,
        std::process::id(),
        unique
    ));
    std::fs::create_dir_all(&base).unwrap();
    base
}

#[tokio::test]
async fn backup_bundles_databases_sync_files_and_manifest() {
    let data_dir = temp_dir("data");
    let backup_dir = temp_dir("backups");
    let sync_dir = temp_dir("sync");

    // A couple of files (one nested) in the sync directory.
    std::fs::write(sync_dir.join("alpha.txt"), b"alpha-bytes").unwrap();
    std::fs::create_dir_all(sync_dir.join("nested")).unwrap();
    std::fs::write(sync_dir.join("nested/beta.txt"), b"beta-bytes-longer").unwrap();

    // Seed the main catalog and this sync directory's index so both databases
    // exist on disk for the snapshot step.
    let main_db_path = data_dir.join("main.db");
    CatalogStore::initialize(&main_db_path).expect("open main db");
    let sync_name = sync_dir.file_name().unwrap().to_string_lossy().into_owned();
    DirectoryIndex::initialize(data_dir.join(format!("{sync_name}.db")))
        .expect("open directory index");

    // Build an ApiService with a backup dir configured.
    let configuration = Configuration {
        sync_directories: vec![SyncDirectory {
            path: sync_dir.clone(),
            sync_type: SyncType::Universal {
                keep_deleted_files: false,
            },
        }],
        listen_port: None,
        peers: Vec::new(),
        tags: Vec::new(),
        preview_generation_policy: PreviewGenerationPolicy::Never,
        editor_rules: Vec::new(),
        tag_rules: Vec::new(),
    };
    let runtime_configuration = Arc::new(RwLock::new(RuntimeConfiguration::new(&configuration)));
    let compiled = Arc::new(CompiledTagRules::compile(&configuration.tag_rules));

    let (change_sender, _change_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (command_sender, mut command_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (event_sender, _event_receiver) = tokio::sync::broadcast::channel(64);
    let pending_fetches = ChunkRelay::new(runtime_configuration.clone());

    let api = ApiService::new(
        main_db_path,
        change_sender,
        command_sender,
        event_sender,
        pending_fetches,
        data_dir.join("fetch-temp"),
        Operations::new(),
        Vec::new(),
        compiled,
        Paths::new(
            data_dir.clone(),
            Some(backup_dir.clone()),
            data_dir.join("identity.key"),
        ),
    );

    // Stand-in for the sync-directory actor: answer the one `ListDirectories`
    // the backup sends with the configured directory.
    let directories = configuration.sync_directories.clone();
    let responder = tokio::spawn(async move {
        if let Some(SyncDirectoryCommand::ListDirectories { respond_to }) =
            command_receiver.recv().await
        {
            let _ = respond_to.send(directories);
        }
    });

    let outcome = api.backup().await.expect("backup succeeds");
    responder.await.unwrap();

    // The two sync files are accounted for in the outcome.
    assert_eq!(outcome.file_count, 2, "both sync files counted");
    assert_eq!(
        outcome.bytes_written,
        (b"alpha-bytes".len() + b"beta-bytes-longer".len()) as u64,
        "raw sync-content bytes summed"
    );

    // The archive exists, is non-empty, and lives in the backup dir.
    let metadata = std::fs::metadata(&outcome.path).expect("archive exists");
    assert!(metadata.len() > 0, "archive is non-empty");
    assert!(
        outcome.path.starts_with(&backup_dir),
        "archive landed in TAGSY_BACKUP_DIR"
    );
    assert_eq!(
        outcome.path.extension().and_then(|e| e.to_str()),
        Some("zst"),
        "archive is a .tar.zst (no lingering .partial)"
    );

    // Untar it and confirm the expected members are present.
    let file = std::fs::File::open(&outcome.path).unwrap();
    let decoder = zstd::stream::read::Decoder::new(file).unwrap();
    let mut archive = tar::Archive::new(decoder);

    let mut entries: Vec<String> = Vec::new();
    let mut manifest_json = String::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        if path == "manifest.json" {
            entry.read_to_string(&mut manifest_json).unwrap();
        }
        entries.push(path);
    }

    assert!(
        entries.contains(&"db/main.db".to_owned()),
        "main db archived"
    );
    assert!(
        entries.contains(&format!("db/{sync_name}.db")),
        "directory index archived"
    );
    assert!(
        entries.contains(&format!("sync/{sync_name}/alpha.txt")),
        "top-level sync file archived: got {entries:?}"
    );
    assert!(
        entries.contains(&format!("sync/{sync_name}/nested/beta.txt")),
        "nested sync file archived: got {entries:?}"
    );
    assert!(
        entries.contains(&"manifest.json".to_owned()),
        "manifest archived"
    );

    // The manifest records the directory's original path and type.
    assert!(
        manifest_json.contains(&sync_dir.to_string_lossy().into_owned()),
        "manifest records the sync dir's absolute path"
    );
    assert!(
        manifest_json.contains("Universal"),
        "manifest records the sync type"
    );

    // Staging was cleaned up.
    assert!(
        !data_dir.join("backup-staging").exists(),
        "staging dir removed on success"
    );
}

#[tokio::test]
async fn backup_without_backup_dir_errors() {
    let data_dir = temp_dir("nodir-data");
    let main_db_path = data_dir.join("main.db");
    CatalogStore::initialize(&main_db_path).expect("open main db");

    let configuration = Configuration {
        sync_directories: Vec::new(),
        listen_port: None,
        peers: Vec::new(),
        tags: Vec::new(),
        preview_generation_policy: PreviewGenerationPolicy::Never,
        editor_rules: Vec::new(),
        tag_rules: Vec::new(),
    };
    let runtime_configuration = Arc::new(RwLock::new(RuntimeConfiguration::new(&configuration)));
    let compiled = Arc::new(CompiledTagRules::compile(&configuration.tag_rules));

    let (change_sender, _change_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (command_sender, _command_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (event_sender, _event_receiver) = tokio::sync::broadcast::channel(64);
    let pending_fetches = ChunkRelay::new(runtime_configuration.clone());

    let api = ApiService::new(
        main_db_path,
        change_sender,
        command_sender,
        event_sender,
        pending_fetches,
        data_dir.join("fetch-temp"),
        Operations::new(),
        Vec::new(),
        compiled,
        // No backup dir configured.
        Paths::new(
            data_dir.clone(),
            None::<PathBuf>,
            data_dir.join("identity.key"),
        ),
    );

    let error = api
        .backup()
        .await
        .expect_err("backup must fail with no dir");
    assert!(
        matches!(error, tagsyd::frontend::api::ApiError::Internal(_)),
        "unset backup dir surfaces an Internal error: {error:?}"
    );
}
