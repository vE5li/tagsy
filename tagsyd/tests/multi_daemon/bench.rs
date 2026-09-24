//! A scale benchmark of the real deployment shape: a central node holding a
//! large catalog in a Universal directory, and a phone with one TagBased
//! directory that dials it.
//!
//! Ignored by default. Run it in release mode:
//!
//! ```sh
//! cargo test --release -p tagsyd --test multi_daemon scale_benchmark -- --ignored --nocapture
//! ```
//!
//! Tunables (environment variables):
//!
//! - `TAGSY_BENCH_FILES` — catalog size (default 15000)
//! - `TAGSY_BENCH_LIVE_ADD` — files added while connected (default 1000)
//! - `TAGSY_BENCH_MAX_SIZE` — largest generated file in bytes (default 65536;
//!   sizes are log-uniform from 256 bytes)
//! - `TAGSY_BENCH_SEED` — generator seed (default 1)
//! - `TAGSY_BENCH_OUT` — also write the results as JSON to this path
//!
//! Each phase is timed from its trigger to the moment the cluster went quiet
//! ([`Cluster::settle_within`]), and reports how many messages each actor
//! handled — which is what points at a hot path. The run ends by checking the
//! cluster converged, so the benchmark doubles as a correctness check at scale.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use tagsy_api::{Backend, DeletedRule, SubtagRule};
use tagsy_core::{FileId, TagId};

use crate::harness::{Cluster, DirectorySpec, NodeId};

/// Generous: a slow phase should be reported, not abort the run.
const PHASE_TIMEOUT: Duration = Duration::from_secs(3600);
/// Tags every generated file draws from, besides the phone's.
const TOPICS: usize = 20;
/// Share of files carrying the phone's tag (so placed on the phone).
const PHONE_SHARE: f64 = 0.10;

struct Config {
    files: usize,
    live_add: usize,
    max_size: usize,
    seed: u64,
    out: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        let number = |name: &str, default: usize| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        };
        Self {
            files: number("TAGSY_BENCH_FILES", 15_000),
            live_add: number("TAGSY_BENCH_LIVE_ADD", 1_000),
            max_size: number("TAGSY_BENCH_MAX_SIZE", 65_536),
            seed: number("TAGSY_BENCH_SEED", 1) as u64,
            out: std::env::var("TAGSY_BENCH_OUT").ok(),
        }
    }
}

/// splitmix64: tiny, deterministic, good enough to generate test data.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn fill(&mut self, buffer: &mut [u8]) {
        for chunk in buffer.chunks_mut(8) {
            let bytes = self.next().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

/// A plausible logical path for the `index`-th generated file.
fn generated_path(rng: &mut Rng, prefix: &str, index: usize) -> String {
    match rng.below(4) {
        0 | 1 => format!(
            "{prefix}photos/{}/{:02}/img_{index:06}.jpg",
            2015 + rng.below(10),
            1 + rng.below(12)
        ),
        2 => format!(
            "{prefix}documents/topic-{:02}/doc_{index:06}.pdf",
            rng.below(TOPICS)
        ),
        _ => format!("{prefix}notes/note_{index:06}.md"),
    }
}

/// Bytes for a file: log-uniform size in `256..=max_size`, random content.
fn generated_bytes(rng: &mut Rng, max_size: usize) -> Vec<u8> {
    let (low, high) = (256f64.ln(), (max_size.max(257) as f64).ln());
    let size = (low + (high - low) * rng.unit()).exp() as usize;
    let mut bytes = vec![0u8; size];
    rng.fill(&mut bytes);
    bytes
}

/// Messages each running node's actors have handled so far:
/// node → (catalog, sync directories, peer sessions).
async fn counters(cluster: &Cluster, nodes: &[NodeId]) -> BTreeMap<String, [u64; 3]> {
    let mut out = BTreeMap::new();
    for &node in nodes.iter().filter(|&&node| cluster.is_running(node)) {
        if let Ok(activity) = cluster.backend(node).activity().await {
            out.insert(cluster.name(node).to_owned(), [
                activity.catalog.processed,
                activity.sync_directories.processed,
                activity.peer_sessions.processed,
            ]);
        }
    }
    out
}

struct Phase {
    name: String,
    seconds: f64,
    /// node → messages handled during the phase (catalog, dirs, sessions).
    messages: BTreeMap<String, [u64; 3]>,
}

#[derive(Default)]
struct Report {
    phases: Vec<Phase>,
    reads: Vec<(String, f64)>,
}

impl Report {
    fn phase(
        &mut self,
        name: &str,
        started: Instant,
        ended: Instant,
        before: &BTreeMap<String, [u64; 3]>,
        after: &BTreeMap<String, [u64; 3]>,
    ) {
        let messages = after
            .iter()
            .map(|(node, counts)| {
                let base = before.get(node).copied().unwrap_or([0; 3]);
                // A restarted node's counters start over.
                let delta = if counts.iter().zip(base).all(|(now, then)| *now >= then) {
                    [
                        counts[0] - base[0],
                        counts[1] - base[1],
                        counts[2] - base[2],
                    ]
                } else {
                    *counts
                };
                (node.clone(), delta)
            })
            .collect();
        let seconds = ended.saturating_duration_since(started).as_secs_f64();
        eprintln!("  {name}: {seconds:.2}s");
        self.phases.push(Phase {
            name: name.to_owned(),
            seconds,
            messages,
        });
    }

    fn read(&mut self, name: &str, seconds: f64) {
        eprintln!("  {name}: {:.1}ms", seconds * 1000.0);
        self.reads.push((name.to_owned(), seconds));
    }

    fn print(&self, config: &Config) {
        eprintln!(
            "\n== scale benchmark: {} files, {} added live, sizes 256..={} bytes, seed {}",
            config.files, config.live_add, config.max_size, config.seed
        );
        eprintln!(
            "{:<52} {:>9}   messages handled per node (catalog / dirs / sessions)",
            "phase", "seconds"
        );
        for phase in &self.phases {
            let messages: Vec<String> = phase
                .messages
                .iter()
                .map(|(node, [catalog, dirs, sessions])| {
                    format!("{node} {catalog}/{dirs}/{sessions}")
                })
                .collect();
            eprintln!(
                "{:<52} {:>9.2}   {}",
                phase.name,
                phase.seconds,
                messages.join("  ")
            );
        }
        eprintln!("\n{:<52} {:>9}", "read", "ms");
        for (name, seconds) in &self.reads {
            eprintln!("{name:<52} {:>9.1}", seconds * 1000.0);
        }
    }

    fn json(&self, config: &Config) -> serde_json::Value {
        serde_json::json!({
            "files": config.files,
            "live_add": config.live_add,
            "max_size": config.max_size,
            "seed": config.seed,
            "phases": self.phases.iter().map(|phase| serde_json::json!({
                "name": phase.name,
                "seconds": phase.seconds,
                "messages": phase.messages,
            })).collect::<Vec<_>>(),
            "reads_ms": self.reads.iter()
                .map(|(name, seconds)| (name.clone(), serde_json::json!(seconds * 1000.0)))
                .collect::<serde_json::Map<_, _>>(),
        })
    }
}

/// Wall time of one async read, in seconds.
async fn time<T>(future: impl std::future::Future<Output = T>) -> (T, f64) {
    let started = Instant::now();
    let value = future.await;
    (value, started.elapsed().as_secs_f64())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark; run with --release -- --ignored --nocapture (see bench.rs)"]
async fn scale_benchmark() {
    let config = Config::from_env();
    let mut rng = Rng(config.seed);
    let mut report = Report::default();

    let mut cluster = Cluster::new();
    let phone_tag = cluster.declare_tag("phone");
    let topics: Vec<TagId> = (0..TOPICS)
        .map(|index| cluster.declare_tag(&format!("topic-{index:02}")))
        .collect();
    let central = cluster.add_node("central", vec![DirectorySpec::universal("store")]);
    let phone = cluster.add_node("phone", vec![DirectorySpec::tag_based("phone", &[
        phone_tag,
    ])]);
    cluster.link(phone, central);
    let both = [central, phone];

    eprintln!("generating {} files ...", config.files);
    let mut total_bytes = 0usize;
    for index in 0..config.files {
        let path = generated_path(&mut rng, "", index);
        let bytes = generated_bytes(&mut rng, config.max_size);
        total_bytes += bytes.len();
        cluster.write_file(central, "store", &path, &bytes);
    }
    eprintln!("  {:.1} MiB", total_bytes as f64 / (1024.0 * 1024.0));
    eprintln!("running phases ...");

    // 1. Import: the startup scan ingests every untracked file.
    let started = Instant::now();
    cluster.start(central).await;
    let ended = cluster.settle_within(PHASE_TIMEOUT).await;
    let after = counters(&cluster, &both).await;
    report.phase(
        "import: startup scan ingests N untracked files",
        started,
        ended,
        &BTreeMap::new(),
        &after,
    );

    // 2. Bulk tagging through the API.
    let files = tagsyd::store::CatalogStore::initialize(cluster.main_db_path(central))
        .expect("open central catalog")
        .get_all_files(DeletedRule::Exclude)
        .expect("list central files");
    assert_eq!(
        files.len(),
        config.files,
        "every generated file was imported"
    );
    let mut ids: Vec<FileId> = files.iter().map(|file| file.file_id).collect();
    ids.sort();
    let before = counters(&cluster, &both).await;
    let started = Instant::now();
    let mut relationships = 0usize;
    for &file_id in &ids {
        for _ in 0..rng.below(4) {
            let topic = topics[rng.below(TOPICS)];
            cluster
                .backend(central)
                .tag_file(topic, file_id)
                .await
                .expect("tag_file");
            relationships += 1;
        }
        if rng.unit() < PHONE_SHARE {
            cluster
                .backend(central)
                .tag_file(phone_tag, file_id)
                .await
                .expect("tag_file");
            relationships += 1;
        }
    }
    let ended = cluster.settle_within(PHASE_TIMEOUT).await;
    let after = counters(&cluster, &both).await;
    report.phase(
        &format!("tagging: {relationships} tag_file calls"),
        started,
        ended,
        &before,
        &after,
    );

    // 3. Cold restart with everything tracked.
    cluster.stop(central).await;
    let started = Instant::now();
    cluster.start(central).await;
    let ended = cluster.settle_within(PHASE_TIMEOUT).await;
    let after = counters(&cluster, &both).await;
    report.phase(
        "cold restart: startup scan of N tracked files",
        started,
        ended,
        &BTreeMap::new(),
        &after,
    );

    // 4. First sync of an empty phone.
    let before = counters(&cluster, &both).await;
    let started = Instant::now();
    cluster.start(phone).await;
    cluster.wait_connected(central, phone).await;
    let ended = cluster.settle_within(PHASE_TIMEOUT).await;
    let after = counters(&cluster, &both).await;
    report.phase(
        "first sync: empty phone connects",
        started,
        ended,
        &before,
        &after,
    );

    // 5. Reconnect with nothing to do.
    cluster.stop(phone).await;
    let before = counters(&cluster, &both).await;
    let started = Instant::now();
    cluster.start(phone).await;
    cluster.wait_connected(central, phone).await;
    let ended = cluster.settle_within(PHASE_TIMEOUT).await;
    let after = counters(&cluster, &both).await;
    report.phase(
        "reconnect: nothing changed",
        started,
        ended,
        &before,
        &after,
    );

    // 6. Files added while connected.
    let before = counters(&cluster, &both).await;
    let started = Instant::now();
    for index in 0..config.live_add {
        let path = generated_path(&mut rng, "live/", index);
        let bytes = generated_bytes(&mut rng, config.max_size);
        cluster.write_file(central, "store", &path, &bytes);
    }
    let ended = cluster.settle_within(PHASE_TIMEOUT).await;
    let after = counters(&cluster, &both).await;
    report.phase(
        &format!("live add: {} files dropped on central", config.live_add),
        started,
        ended,
        &before,
        &after,
    );

    // 7. Reads against a catalog of this size.
    for (label, node) in [("central", central), ("phone", phone)] {
        let backend = cluster.backend(node);
        let (results, seconds) =
            time(backend.search(String::new(), SubtagRule::Include, DeletedRule::Exclude)).await;
        let count = results.expect("search").files.len();
        report.read(&format!("{label}: search \"\" ({count} files)"), seconds);
        let (results, seconds) = time(backend.search(
            "topic-03".to_owned(),
            SubtagRule::Include,
            DeletedRule::Exclude,
        ))
        .await;
        let count = results.expect("search").files.len();
        report.read(
            &format!("{label}: search \"topic-03\" ({count} files)"),
            seconds,
        );
        let (_, seconds) = time(backend.storage_stats()).await;
        report.read(&format!("{label}: storage_stats"), seconds);
        let sample: Vec<FileId> = ids
            .iter()
            .step_by((ids.len() / 100).max(1))
            .copied()
            .collect();
        let (_, seconds) = time(async {
            for &file_id in &sample {
                backend
                    .get_file(file_id, DeletedRule::Exclude)
                    .await
                    .expect("get_file");
            }
        })
        .await;
        report.read(
            &format!("{label}: get_file (mean of {})", sample.len()),
            seconds / sample.len() as f64,
        );
    }

    report.print(&config);
    if let Some(path) = &config.out {
        let json = serde_json::to_string_pretty(&report.json(&config)).expect("serialize report");
        std::fs::write(path, json).expect("write benchmark report");
        eprintln!("\nwrote {path}");
    }

    eprintln!("\nchecking convergence ...");
    cluster.assert_converged();
}
