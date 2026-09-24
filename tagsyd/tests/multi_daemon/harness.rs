//! A cluster of real `tagsyd` daemons in one test process.
//!
//! Each node is a full daemon started through [`tagsyd::run`] — real SQLite
//! databases, real sync directories with a live inotify watcher, real
//! WebSocket peer links over loopback — on its **own runtime thread**. One
//! runtime per node is the closest in-process stand-in for separate processes:
//! a node's blocking SQLite work can't starve another node's tasks, and
//! stopping a node drops its runtime, killing its sockets exactly as a process
//! exit would.
//!
//! The peer graph must be a tree (see AGENTS.md); build it with
//! [`Cluster::link`] as a star or a line.

use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tagsy_api::{ActivityInfo, Backend, OperationKind};
use tagsy_core::{FileId, TagId};
use tagsyd::configuration::{
    Configuration, Peer, PreviewGenerationPolicy, SyncDirectory, SyncType, TagDeclaration,
};
use tagsyd::paths::Paths;
use tagsyd::peer::handshake::Identity;
use tagsyd::transport::InProcessBackend;

use crate::snapshot::{self, CatalogState, Snapshot};

/// How long [`Cluster::settle`] waits before failing.
pub const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long every node must stay idle, with no message handled anywhere, for
/// the cluster to count as settled. Covers frames in flight on loopback and
/// the brief gaps between one actor finishing and the next picking up.
const QUIET_WINDOW: Duration = Duration::from_millis(500);
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Short debounce so filesystem-driven tests don't wait out the 500 ms default.
const TEST_DEBOUNCE_MS: u64 = 100;
/// Fast redial so reconnect tests don't wait out the 5 s default.
const TEST_RECONNECT_INTERVAL_MS: u64 = 100;

/// Handle to a node in a [`Cluster`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeId(usize);

/// One sync directory of a node.
#[derive(Debug, Clone)]
pub struct DirectorySpec {
    /// Directory basename; also names its index database, so it must be unique
    /// within the node.
    pub label: String,
    pub sync_type: SyncType,
}

impl DirectorySpec {
    pub fn universal(label: &str) -> Self {
        Self {
            label: label.to_owned(),
            sync_type: SyncType::Universal {
                keep_deleted_files: false,
            },
        }
    }

    pub fn tag_based(label: &str, tags: &[TagId]) -> Self {
        Self {
            label: label.to_owned(),
            sync_type: SyncType::TagBased {
                tags: tags.to_vec(),
            },
        }
    }
}

type ConfigurationHook = Box<dyn Fn(&mut Configuration) + Send + Sync>;

struct Node {
    name: String,
    root: PathBuf,
    public_key: String,
    port: u16,
    directories: Vec<DirectorySpec>,
    hooks: Vec<ConfigurationHook>,
    running: Option<RunningNode>,
}

struct RunningNode {
    backend: InProcessBackend,
    shutdown: tagsyd::ShutdownSignal,
    thread: std::thread::JoinHandle<()>,
}

impl Node {
    fn data_dir(&self) -> PathBuf {
        self.root.join("data")
    }

    fn identity_path(&self) -> PathBuf {
        self.root.join("identity.key")
    }

    fn directory_path(&self, label: &str) -> PathBuf {
        self.root.join("directories").join(label)
    }

    fn scratch_dir(&self) -> PathBuf {
        self.root.join("scratch")
    }
}

/// A set of daemons plus the links between them.
pub struct Cluster {
    root: PathBuf,
    nodes: Vec<Node>,
    /// `(dialer, listener)` pairs.
    links: Vec<(NodeId, NodeId)>,
    tags: Vec<TagDeclaration>,
}

impl Cluster {
    pub fn new() -> Self {
        let _ = env_logger::builder().is_test(true).try_init();

        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "tagsy-multi-daemon-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("create cluster root");
        Self {
            root,
            nodes: Vec::new(),
            links: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// Declare a tag in every node's configuration, so TagBased directories
    /// can reference it from the first boot.
    pub fn declare_tag(&mut self, name: &str) -> TagId {
        let id = TagId::new();
        self.tags.push(TagDeclaration {
            id,
            name: name.to_owned(),
            color: String::new(),
        });
        id
    }

    pub fn add_node(&mut self, name: &str, directories: Vec<DirectorySpec>) -> NodeId {
        let root = self.root.join(name);
        std::fs::create_dir_all(root.join("data")).expect("create node data dir");
        std::fs::create_dir_all(root.join("scratch")).expect("create node scratch dir");

        let identity = Identity::generate();
        identity
            .save(&root.join("identity.key"))
            .expect("save node identity");

        self.nodes.push(Node {
            name: name.to_owned(),
            root,
            public_key: identity.public_key(),
            port: free_port(),
            directories,
            hooks: Vec::new(),
            running: None,
        });
        NodeId(self.nodes.len() - 1)
    }

    /// Link two nodes: `dialer` connects out to `listener`, mirroring a phone
    /// that dials the central server. Keep the resulting graph a tree.
    pub fn link(&mut self, dialer: NodeId, listener: NodeId) {
        self.links.push((dialer, listener));
    }

    /// Adjust a node's configuration before each start (e.g. tiny manifest
    /// batch sizes).
    pub fn configure(
        &mut self,
        node: NodeId,
        hook: impl Fn(&mut Configuration) + Send + Sync + 'static,
    ) {
        self.nodes[node.0].hooks.push(Box::new(hook));
    }

    fn node(&self, node: NodeId) -> &Node {
        &self.nodes[node.0]
    }

    pub fn name(&self, node: NodeId) -> &str {
        &self.node(node).name
    }

    fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        (0..self.nodes.len()).map(NodeId)
    }

    fn running_ids(&self) -> Vec<NodeId> {
        self.node_ids()
            .filter(|&id| self.node(id).running.is_some())
            .collect()
    }

    fn configuration(&self, id: NodeId) -> Configuration {
        let node = self.node(id);
        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);

        let mut peers = Vec::new();
        let mut listens = false;
        for &(dialer, listener) in &self.links {
            if dialer == id {
                let other = self.node(listener);
                peers.push(Peer {
                    address: Some((loopback, other.port)),
                    name: other.name.clone(),
                    public_key: other.public_key.clone(),
                });
            } else if listener == id {
                listens = true;
                let other = self.node(dialer);
                peers.push(Peer {
                    address: None,
                    name: other.name.clone(),
                    public_key: other.public_key.clone(),
                });
            }
        }

        let mut configuration = Configuration {
            sync_directories: node
                .directories
                .iter()
                .map(|directory| SyncDirectory {
                    path: node.directory_path(&directory.label),
                    sync_type: directory.sync_type.clone(),
                    respect_gitignore: false,
                    debounce_ms: TEST_DEBOUNCE_MS,
                })
                .collect(),
            listen_port: listens.then_some(node.port),
            peers,
            tags: self.tags.clone(),
            preview_generation_policy: PreviewGenerationPolicy::Never,
            max_concurrent_pulls: tagsyd::configuration::default_max_concurrent_pulls(),
            max_concurrent_preview_generations:
                tagsyd::configuration::default_max_concurrent_preview_generations(),
            manifest_batch_size: tagsyd::configuration::default_manifest_batch_size(),
            tag_manifest_batch_size: tagsyd::configuration::default_tag_manifest_batch_size(),
            purge_manifest_batch_size: tagsyd::configuration::default_purge_manifest_batch_size(),
            reconnect_interval_ms: TEST_RECONNECT_INTERVAL_MS,
            editor_rules: Vec::new(),
            tag_rules: Vec::new(),
            home_sections: Vec::new(),
        };
        for hook in &node.hooks {
            hook(&mut configuration);
        }
        configuration
    }

    /// Boot a node's daemon on its own runtime thread. Returns once startup
    /// (identity, catalog, listener bind) has succeeded; the startup scan and
    /// peer connections proceed asynchronously — use [`Self::settle`] /
    /// [`Self::wait_connected`].
    pub async fn start(&mut self, id: NodeId) {
        assert!(
            self.node(id).running.is_none(),
            "{} is already running",
            self.name(id)
        );
        let configuration = self.configuration(id);
        let node = self.node(id);
        let paths = Paths::new(node.data_dir(), None::<PathBuf>, node.identity_path());
        let shutdown = tagsyd::ShutdownSignal::new();
        let name = node.name.clone();

        let (api_tx, api_rx) = tokio::sync::oneshot::channel();
        let thread = {
            let shutdown = shutdown.clone();
            std::thread::Builder::new()
                .name(format!("node-{name}"))
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build()
                        .expect("build node runtime");
                    runtime.block_on(async move {
                        match tagsyd::run(configuration, paths, shutdown).await {
                            Ok((api, driver)) => {
                                let _ = api_tx.send(Ok(api));
                                if let Err(error) = driver.await {
                                    log::error!("node {name}: driver failed: {error}");
                                }
                            }
                            Err(error) => {
                                let _ = api_tx.send(Err(error.to_string()));
                            }
                        }
                    });
                    // Dropping the runtime here aborts every remaining task
                    // (inbound sessions, queued pulls): a process exit.
                })
                .expect("spawn node thread")
        };

        let api = api_rx
            .await
            .expect("node thread reported startup")
            .unwrap_or_else(|error| panic!("{} failed to start: {error}", self.name(id)));
        self.nodes[id.0].running = Some(RunningNode {
            backend: InProcessBackend::new(api),
            shutdown,
            thread,
        });
    }

    /// Start every node, wait for every link, and settle — the starting point
    /// for a scenario. Settling first matters: a fresh connection queues
    /// reconcile work (manifests, the missing-content sweep) that would
    /// otherwise race the scenario's first operations and make the outcome
    /// timing-dependent.
    pub async fn start_connected(&mut self) {
        self.start_all().await;
        self.wait_all_connected().await;
        self.settle().await;
    }

    pub async fn start_all(&mut self) {
        for id in self.node_ids().collect::<Vec<_>>() {
            if self.node(id).running.is_none() {
                self.start(id).await;
            }
        }
    }

    /// Shut a node down and wait for its runtime to exit. Its data and
    /// directories stay, so [`Self::start`] brings the same device back.
    pub async fn stop(&mut self, id: NodeId) {
        let running = self.nodes[id.0]
            .running
            .take()
            .unwrap_or_else(|| panic!("{} is not running", self.name(id)));
        running.shutdown.shutdown();
        drop(running.backend);
        tokio::task::spawn_blocking(move || running.thread.join())
            .await
            .expect("join task")
            .expect("node thread panicked");
    }

    pub async fn restart(&mut self, id: NodeId) {
        self.stop(id).await;
        self.start(id).await;
    }

    pub fn backend(&self, id: NodeId) -> &InProcessBackend {
        &self
            .node(id)
            .running
            .as_ref()
            .unwrap_or_else(|| panic!("{} is not running", self.name(id)))
            .backend
    }

    /// Wait until `a` reports a live session with `b` and vice versa.
    pub async fn wait_connected(&self, a: NodeId, b: NodeId) {
        let deadline = Instant::now() + SETTLE_TIMEOUT;
        loop {
            if self.is_connected(a, b).await && self.is_connected(b, a).await {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{} and {} never connected",
                self.name(a),
                self.name(b)
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn is_connected(&self, from: NodeId, to: NodeId) -> bool {
        let key = &self.node(to).public_key;
        self.backend(from)
            .connected_peers()
            .await
            .expect("connected_peers")
            .iter()
            .any(|peer| &peer.public_key == key)
    }

    /// Wait for every link between two running nodes to be up.
    pub async fn wait_all_connected(&self) {
        for &(a, b) in &self.links {
            if self.node(a).running.is_some() && self.node(b).running.is_some() {
                self.wait_connected(a, b).await;
            }
        }
    }

    /// Wait until every running node is quiet: idle by
    /// [`ActivityInfo::is_idle`], no operation in progress (other than a
    /// redial toward a stopped peer), and no message handled anywhere for
    /// [`QUIET_WINDOW`].
    pub async fn settle(&self) {
        let deadline = Instant::now() + SETTLE_TIMEOUT;
        let mut quiet_since: Option<(Instant, Vec<(u64, u64)>)> = None;
        loop {
            let mut idle = true;
            let mut fingerprint = Vec::new();
            let mut report = String::new();
            for id in self.running_ids() {
                let backend = self.backend(id);
                let activity = backend.activity().await.expect("activity");
                let busy_operations: Vec<OperationKind> = backend
                    .list_operations()
                    .await
                    .expect("list_operations")
                    .into_iter()
                    .map(|operation| operation.kind)
                    .filter(|kind| !matches!(kind, OperationKind::ConnectingToPeer { .. }))
                    .collect();
                idle &= activity.is_idle() && busy_operations.is_empty();
                fingerprint.push((
                    activity.catalog.processed,
                    activity.sync_directories.processed,
                ));
                report.push_str(&format!(
                    "  {}: {} operations={busy_operations:?}\n",
                    self.name(id),
                    describe(&activity)
                ));
            }

            let now = Instant::now();
            match (&quiet_since, idle) {
                (_, false) => quiet_since = None,
                (Some((since, previous)), true) if *previous == fingerprint => {
                    if now.duration_since(*since) >= QUIET_WINDOW {
                        return;
                    }
                }
                (_, true) => quiet_since = Some((now, fingerprint)),
            }

            assert!(
                now < deadline,
                "cluster did not settle within {SETTLE_TIMEOUT:?}:\n{report}"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Read a node's catalog (works whether or not the node is running).
    pub fn catalog(&self, id: NodeId) -> CatalogState {
        CatalogState::load(&self.node(id).data_dir().join("main.db"))
    }

    /// Assert every running node holds the identical catalog and that each
    /// node's disk matches its own catalog. Call after [`Self::settle`].
    pub fn assert_converged(&self) {
        let running = self.running_ids();
        let mut failures = Vec::new();

        let catalogs: Vec<(NodeId, CatalogState)> =
            running.iter().map(|&id| (id, self.catalog(id))).collect();
        if let Some(((first, reference), rest)) =
            catalogs.split_first().map(|(f, r)| ((f.0, &f.1), r))
        {
            let reference_snapshot = reference.exact();
            for (id, catalog) in rest {
                if let Some(diff) = reference_snapshot.diff(&catalog.exact()) {
                    failures.push(format!(
                        "catalog of {} (-) differs from {} (+):\n{diff}",
                        self.name(first),
                        self.name(*id)
                    ));
                }
            }
        }

        for (id, catalog) in &catalogs {
            let node = self.node(*id);
            for directory in &node.directories {
                if let Some(diff) = snapshot::check_disk(
                    catalog,
                    &node.directory_path(&directory.label),
                    &directory.sync_type,
                ) {
                    failures.push(format!("{}: {diff}", node.name));
                }
            }
        }

        assert!(
            failures.is_empty(),
            "cluster has not converged (data kept at {}):\n\n{}",
            self.root.display(),
            failures.join("\n")
        );
    }

    /// The run-independent view of the whole cluster: each node's normalized
    /// catalog plus its normalized disk contents. Two runs of the same script
    /// must produce equal results.
    pub fn normalized(&self) -> Vec<(String, Snapshot)> {
        let mut out = Vec::new();
        for id in self.running_ids() {
            let node = self.node(id);
            out.push((
                format!("{} catalog", node.name),
                self.catalog(id).normalized(),
            ));
            for directory in &node.directories {
                // Universal directories name files by id, which differs per
                // run; their placement is covered by `check_disk`.
                if matches!(directory.sync_type, SyncType::TagBased { .. }) {
                    out.push((
                        format!("{} dir {}", node.name, directory.label),
                        snapshot::disk_contents(&node.directory_path(&directory.label)),
                    ));
                }
            }
        }
        out
    }

    /// Path of a file inside one of a node's sync directories.
    pub fn directory_path(&self, id: NodeId, label: &str) -> PathBuf {
        self.node(id).directory_path(label)
    }

    /// Write `bytes` into a node's sync directory, as a user (or another
    /// program) would. The watcher picks it up.
    pub fn write_file(&self, id: NodeId, label: &str, relative: &str, bytes: &[u8]) {
        write_creating_parents(&self.directory_path(id, label).join(relative), bytes);
    }

    /// Remove a file from a node's sync directory, as a user would.
    pub fn remove_file(&self, id: NodeId, label: &str, relative: &str) {
        std::fs::remove_file(self.directory_path(id, label).join(relative))
            .expect("remove file from sync directory");
    }

    /// Upload `bytes` under `logical_path` through the node's API (the path the
    /// CLI / UI take). The source stays in the node's scratch dir so peers can
    /// pull it on demand.
    pub async fn upload(
        &self,
        id: NodeId,
        logical_path: &str,
        bytes: &[u8],
        tags: Vec<TagId>,
    ) -> FileId {
        let source = self.scratch_file(id, bytes);
        self.backend(id)
            .upload_file(source, logical_path.to_owned(), tags)
            .await
            .expect("upload_file")
    }

    /// Replace a file's content through the node's API.
    pub async fn edit(&self, id: NodeId, file_id: FileId, bytes: &[u8]) {
        let source = self.scratch_file(id, bytes);
        self.backend(id)
            .edit_file(file_id, source)
            .await
            .expect("edit_file");
    }

    fn scratch_file(&self, id: NodeId, bytes: &[u8]) -> PathBuf {
        let path = self
            .node(id)
            .scratch_dir()
            .join(uuid::Uuid::new_v4().to_string());
        write_creating_parents(&path, bytes);
        path
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(running) = node.running.take() {
                running.shutdown.shutdown();
                drop(running.backend);
                let _ = running.thread.join();
            }
        }
        if std::thread::panicking() {
            eprintln!(
                "multi-daemon test failed; data kept at {}",
                self.root.display()
            );
        } else {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

fn describe(activity: &ActivityInfo) -> String {
    format!(
        "catalog(busy={} queued={} processed={}) sync_directories(busy={} queued={} processed={}) \
         fs_pending={} scan_done={} pulls(queued={} running={})",
        activity.catalog.busy,
        activity.catalog.queued,
        activity.catalog.processed,
        activity.sync_directories.busy,
        activity.sync_directories.queued,
        activity.sync_directories.processed,
        activity.pending_filesystem_events,
        activity.initial_scan_complete,
        activity.pulls_queued,
        activity.pulls_running,
    )
}

/// Reserve a free loopback port. The daemon binds it moments later; the gap
/// is racy in principle but loopback ephemeral ports are not reused that fast.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("reserve a free port")
        .port()
}

fn write_creating_parents(path: &Path, bytes: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create parent directories");
    }
    std::fs::write(path, bytes).expect("write file");
}
