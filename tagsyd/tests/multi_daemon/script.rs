//! Scripted scenarios run under two delivery modes, which must agree.
//!
//! A [`Scenario`] is a topology plus a list of [`Step`]s. [`check`] runs it
//! twice on fresh clusters:
//!
//! - **Live** — every node connected throughout. Whenever the acting node
//!   changes, the cluster settles first, so each change has propagated before
//!   another node acts on it (the sequential, fully-connected baseline).
//! - **Reconnect** — the scenario's `offline` nodes are stopped before the
//!   script and started after it, so everything they learn arrives through
//!   connect-time reconciliation instead of the live change stream.
//!
//! Each run must converge ([`Cluster::assert_converged`]); the two runs'
//! [`Cluster::normalized`] states must then be equal. The live run is also
//! restarted wholesale and must come back unchanged: reconnecting an already
//! converged cluster is a no-op.
//!
//! Steps name files and tags by *label*; each run resolves labels to its own
//! freshly minted ids.

use std::collections::BTreeMap;
use std::time::Duration;

use tagsy_api::{Backend, DeletedRule};
use tagsy_core::{FileId, TagId, TagStyle};
use tagsyd::configuration::Configuration;

use crate::harness::{Cluster, DirectorySpec, NodeId};
use crate::snapshot::Snapshot;

/// Gap between steps, so two consecutive changes to one entity never share a
/// last-writer-wins millisecond.
const STEP_GAP: Duration = Duration::from_millis(5);

/// Nodes and declared tags of a built topology, by role name.
pub struct Roles {
    nodes: BTreeMap<&'static str, NodeId>,
    tags: BTreeMap<String, TagId>,
}

impl Roles {
    fn node(&self, role: &str) -> NodeId {
        *self
            .nodes
            .get(role)
            .unwrap_or_else(|| panic!("unknown role {role:?}"))
    }
}

/// A central node holding everything in a Universal directory, and a phone
/// with one TagBased directory (tag `phone`) that dials it — the real
/// deployment's shape.
pub fn hub_and_spoke(cluster: &mut Cluster) -> Roles {
    let phone_tag = cluster.declare_tag("phone");
    let central = cluster.add_node("central", vec![DirectorySpec::universal("store")]);
    let phone = cluster.add_node("phone", vec![DirectorySpec::tag_based("phone", &[
        phone_tag,
    ])]);
    cluster.link(phone, central);
    Roles {
        nodes: [("central", central), ("phone", phone)].into(),
        tags: [("phone".to_owned(), phone_tag)].into(),
    }
}

/// [`hub_and_spoke`], but central keeps deleted files' bytes
/// (`keep_deleted_files`), so deleted files stay restorable.
pub fn hub_with_vault(cluster: &mut Cluster) -> Roles {
    let phone_tag = cluster.declare_tag("phone");
    let central = cluster.add_node("central", vec![DirectorySpec::vault("store")]);
    let phone = cluster.add_node("phone", vec![DirectorySpec::tag_based("phone", &[
        phone_tag,
    ])]);
    cluster.link(phone, central);
    Roles {
        nodes: [("central", central), ("phone", phone)].into(),
        tags: [("phone".to_owned(), phone_tag)].into(),
    }
}

/// A central hub with two phones (tags `a` and `b`). The phones only reach
/// each other through central, so stopping central partitions them.
pub fn two_phones(cluster: &mut Cluster) -> Roles {
    let tag_a = cluster.declare_tag("a");
    let tag_b = cluster.declare_tag("b");
    let central = cluster.add_node("central", vec![DirectorySpec::universal("store")]);
    let phone_a = cluster.add_node("phone_a", vec![DirectorySpec::tag_based("a", &[tag_a])]);
    let phone_b = cluster.add_node("phone_b", vec![DirectorySpec::tag_based("b", &[tag_b])]);
    cluster.link(phone_a, central);
    cluster.link(phone_b, central);
    Roles {
        nodes: [
            ("central", central),
            ("phone_a", phone_a),
            ("phone_b", phone_b),
        ]
        .into(),
        tags: [("a".to_owned(), tag_a), ("b".to_owned(), tag_b)].into(),
    }
}

/// `archive` (Universal) — `relay` (no sync directories: holds no bytes) —
/// `phone` (TagBased, tag `phone`). Everything between the ends crosses the
/// relay.
pub fn relay_line(cluster: &mut Cluster) -> Roles {
    let phone_tag = cluster.declare_tag("phone");
    let archive = cluster.add_node("archive", vec![DirectorySpec::universal("store")]);
    let relay = cluster.add_node("relay", Vec::new());
    let phone = cluster.add_node("phone", vec![DirectorySpec::tag_based("phone", &[
        phone_tag,
    ])]);
    cluster.link(relay, archive);
    cluster.link(phone, relay);
    Roles {
        nodes: [("archive", archive), ("relay", relay), ("phone", phone)].into(),
        tags: [("phone".to_owned(), phone_tag)].into(),
    }
}

/// One scripted action, performed on the node playing role `on`.
#[derive(Debug, Clone)]
pub enum Step {
    /// API upload of a new file (the CLI / UI path).
    Upload {
        on: &'static str,
        file: &'static str,
        path: &'static str,
        bytes: Vec<u8>,
        tags: Vec<&'static str>,
    },
    /// Create a new file in a sync directory, as a user would.
    Write {
        on: &'static str,
        file: &'static str,
        dir: &'static str,
        path: &'static str,
        bytes: Vec<u8>,
    },
    /// Overwrite an existing file in a sync directory.
    Overwrite {
        on: &'static str,
        dir: &'static str,
        path: &'static str,
        bytes: Vec<u8>,
    },
    /// Rename a file within a sync directory.
    Rename {
        on: &'static str,
        dir: &'static str,
        from: &'static str,
        to: &'static str,
    },
    /// Delete a file from a sync directory.
    Remove {
        on: &'static str,
        dir: &'static str,
        path: &'static str,
    },
    Edit {
        on: &'static str,
        file: &'static str,
        bytes: Vec<u8>,
    },
    Delete {
        on: &'static str,
        file: &'static str,
    },
    Restore {
        on: &'static str,
        file: &'static str,
    },
    Move {
        on: &'static str,
        file: &'static str,
        to: &'static str,
    },
    CreateTag {
        on: &'static str,
        tag: &'static str,
    },
    RenameTag {
        on: &'static str,
        tag: &'static str,
        to: &'static str,
    },
    RecolorTag {
        on: &'static str,
        tag: &'static str,
        dot_color: &'static str,
    },
    DeleteTag {
        on: &'static str,
        tag: &'static str,
    },
    RestoreTag {
        on: &'static str,
        tag: &'static str,
    },
    TagFile {
        on: &'static str,
        tag: &'static str,
        file: &'static str,
    },
    UntagFile {
        on: &'static str,
        tag: &'static str,
        file: &'static str,
    },
    TagTag {
        on: &'static str,
        parent: &'static str,
        child: &'static str,
    },
    UntagTag {
        on: &'static str,
        parent: &'static str,
        child: &'static str,
    },
    PurgeDeleted {
        on: &'static str,
    },
}

impl Step {
    fn actor(&self) -> &'static str {
        match self {
            Step::Upload { on, .. }
            | Step::Write { on, .. }
            | Step::Overwrite { on, .. }
            | Step::Rename { on, .. }
            | Step::Remove { on, .. }
            | Step::Edit { on, .. }
            | Step::Delete { on, .. }
            | Step::Restore { on, .. }
            | Step::Move { on, .. }
            | Step::CreateTag { on, .. }
            | Step::RenameTag { on, .. }
            | Step::RecolorTag { on, .. }
            | Step::DeleteTag { on, .. }
            | Step::RestoreTag { on, .. }
            | Step::TagFile { on, .. }
            | Step::UntagFile { on, .. }
            | Step::TagTag { on, .. }
            | Step::UntagTag { on, .. }
            | Step::PurgeDeleted { on } => on,
        }
    }
}

/// A topology, a script, and which roles are offline in the reconnect run.
pub struct Scenario {
    pub topology: fn(&mut Cluster) -> Roles,
    /// Performed connected and settled, before anyone goes offline, in both
    /// runs: the state the script starts from.
    pub setup: Vec<Step>,
    pub steps: Vec<Step>,
    /// Roles stopped for the script's duration in the reconnect run. Must not
    /// include any role a step acts on.
    pub offline: Vec<&'static str>,
    /// Applied to every node's configuration (e.g. tiny manifest batches).
    pub configure: Option<fn(&mut Configuration)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Live,
    Reconnect,
}

/// Run `scenario` live and via reconnect; assert both converge to the same
/// state, and that the live result survives a full restart unchanged.
pub async fn check(scenario: Scenario) {
    for step in &scenario.steps {
        assert!(
            !scenario.offline.contains(&step.actor()),
            "step {step:?} acts on a role that is offline in the reconnect run"
        );
    }
    let live = run(&scenario, Mode::Live).await;
    let reconnect = run(&scenario, Mode::Reconnect).await;
    assert_same_state("live", &live, "reconnect", &reconnect);
}

async fn run(scenario: &Scenario, mode: Mode) -> Vec<(String, Snapshot)> {
    let mut cluster = Cluster::new();
    let roles = (scenario.topology)(&mut cluster);
    if let Some(configure) = scenario.configure {
        for &node in roles.nodes.values() {
            cluster.configure(node, configure);
        }
    }
    cluster.start_connected().await;

    let mut context = Context {
        files: BTreeMap::new(),
        pending: BTreeMap::new(),
        tags: roles.tags.clone(),
    };
    perform_all(&cluster, &roles, &mut context, &scenario.setup, true).await;
    cluster.settle().await;

    if mode == Mode::Reconnect {
        for role in &scenario.offline {
            cluster.stop(roles.node(role)).await;
        }
    }

    perform_all(
        &cluster,
        &roles,
        &mut context,
        &scenario.steps,
        mode == Mode::Live,
    )
    .await;

    if mode == Mode::Reconnect {
        // Let the online nodes finish ingesting the script (watcher events
        // are still debouncing right after the last step), so the reconnect
        // sees the changes as made-while-offline rather than racing them.
        cluster.settle().await;
        for role in &scenario.offline {
            cluster.start(roles.node(role)).await;
        }
        cluster.wait_all_connected().await;
    }
    cluster.settle().await;
    eprintln!("[{mode:?} run] checking convergence");
    cluster.assert_converged();
    let result = cluster.normalized();

    if mode == Mode::Live {
        // Reconnecting a converged cluster must change nothing.
        for &node in roles.nodes.values() {
            cluster.stop(node).await;
        }
        cluster.start_connected().await;
        cluster.assert_converged();
        assert_same_state(
            "live",
            &result,
            "live after full restart",
            &cluster.normalized(),
        );
    }
    result
}

/// Perform `steps` in order. With `settle_between_actors`, the cluster settles
/// whenever the acting node changes, so each change reaches the others before
/// they act.
async fn perform_all(
    cluster: &Cluster,
    roles: &Roles,
    context: &mut Context,
    steps: &[Step],
    settle_between_actors: bool,
) {
    let mut previous_actor = None;
    for step in steps {
        if settle_between_actors && previous_actor.is_some_and(|actor| actor != step.actor()) {
            cluster.settle().await;
        }
        previous_actor = Some(step.actor());
        perform(cluster, roles, context, step).await;
        tokio::time::sleep(STEP_GAP).await;
    }
}

/// Per-run label → id resolution.
struct Context {
    files: BTreeMap<&'static str, FileId>,
    /// Files created on disk, whose id is only known once ingested:
    /// label → (node, path at creation).
    pending: BTreeMap<&'static str, (NodeId, &'static str)>,
    tags: BTreeMap<String, TagId>,
}

impl Context {
    async fn file(&mut self, cluster: &Cluster, label: &'static str) -> FileId {
        if let Some(id) = self.files.get(label) {
            return *id;
        }
        let (node, path) = *self
            .pending
            .get(label)
            .unwrap_or_else(|| panic!("unknown file label {label:?}"));
        cluster.settle().await;
        let id = cluster
            .backend(node)
            .resolve_file_id(path.to_owned(), DeletedRule::Include)
            .await
            .unwrap_or_else(|error| {
                panic!("file {label:?} ({path}) was never cataloged: {error:?}")
            });
        self.files.insert(label, id);
        self.pending.remove(label);
        id
    }

    fn tag(&self, label: &str) -> TagId {
        *self
            .tags
            .get(label)
            .unwrap_or_else(|| panic!("unknown tag label {label:?}"))
    }
}

async fn perform(cluster: &Cluster, roles: &Roles, context: &mut Context, step: &Step) {
    let node = roles.node(step.actor());
    let backend = cluster.backend(node);
    match step {
        Step::Upload {
            file,
            path,
            bytes,
            tags,
            ..
        } => {
            let tags = tags.iter().map(|tag| context.tag(tag)).collect();
            let id = cluster.upload(node, path, bytes, tags).await;
            context.files.insert(file, id);
        }
        Step::Write {
            file,
            dir,
            path,
            bytes,
            ..
        } => {
            cluster.write_file(node, dir, path, bytes);
            context.pending.insert(file, (node, path));
        }
        Step::Overwrite {
            dir, path, bytes, ..
        } => cluster.write_file(node, dir, path, bytes),
        Step::Rename { dir, from, to, .. } => {
            let base = cluster.directory_path(node, dir);
            let target = base.join(to);
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent).expect("create rename target parent");
            }
            std::fs::rename(base.join(from), target).expect("rename in sync directory");
        }
        Step::Remove { dir, path, .. } => cluster.remove_file(node, dir, path),
        Step::Edit { file, bytes, .. } => {
            let id = context.file(cluster, file).await;
            cluster.edit(node, id, bytes).await;
        }
        Step::Delete { file, .. } => {
            let id = context.file(cluster, file).await;
            backend.delete_file(id).await.or_fail(step);
        }
        Step::Restore { file, .. } => {
            let id = context.file(cluster, file).await;
            backend.restore_file(id).await.or_fail(step);
        }
        Step::Move { file, to, .. } => {
            let id = context.file(cluster, file).await;
            backend.move_file(id, (*to).to_owned()).await.or_fail(step);
        }
        Step::CreateTag { tag, .. } => {
            let id = backend
                .create_tag((*tag).to_owned(), TagStyle::default())
                .await
                .or_fail(step);
            context.tags.insert((*tag).to_owned(), id);
        }
        Step::RenameTag { tag, to, .. } => backend
            .rename_tag(context.tag(tag), (*to).to_owned())
            .await
            .or_fail(step),
        Step::RecolorTag { tag, dot_color, .. } => {
            let style = TagStyle {
                dot_color: (*dot_color).to_owned(),
                ..TagStyle::default()
            };
            backend
                .set_tag_style(context.tag(tag), style)
                .await
                .or_fail(step);
        }
        Step::DeleteTag { tag, .. } => backend.delete_tag(context.tag(tag)).await.or_fail(step),
        Step::RestoreTag { tag, .. } => backend.restore_tag(context.tag(tag)).await.or_fail(step),
        Step::TagFile { tag, file, .. } => {
            let id = context.file(cluster, file).await;
            backend.tag_file(context.tag(tag), id).await.or_fail(step);
        }
        Step::UntagFile { tag, file, .. } => {
            let id = context.file(cluster, file).await;
            backend.untag_file(context.tag(tag), id).await.or_fail(step);
        }
        Step::TagTag { parent, child, .. } => backend
            .tag_tag(context.tag(parent), context.tag(child))
            .await
            .or_fail(step),
        Step::UntagTag { parent, child, .. } => backend
            .untag_tag(context.tag(parent), context.tag(child))
            .await
            .or_fail(step),
        Step::PurgeDeleted { .. } => {
            backend.purge_deleted(false).await.or_fail(step);
        }
    }
}

/// Unwrap an API result, naming the step that failed.
trait OrFail<T> {
    fn or_fail(self, step: &Step) -> T;
}

impl<T> OrFail<T> for Result<T, tagsy_api::ApiError> {
    fn or_fail(self, step: &Step) -> T {
        self.unwrap_or_else(|error| panic!("step {step:?} failed: {error:?}"))
    }
}

/// Assert two [`Cluster::normalized`] results match, printing per-part diffs.
pub fn assert_same_state(
    a_label: &str,
    a: &[(String, Snapshot)],
    b_label: &str,
    b: &[(String, Snapshot)],
) {
    let mut failures = Vec::new();
    let names: std::collections::BTreeSet<&String> =
        a.iter().chain(b.iter()).map(|(name, _)| name).collect();
    for name in names {
        let find = |side: &[(String, Snapshot)]| {
            side.iter()
                .find(|(n, _)| n == name)
                .map(|(_, s)| s.clone())
                .unwrap_or_default()
        };
        if let Some(diff) = find(a).diff(&find(b)) {
            failures.push(format!("{name} (- {a_label}, + {b_label}):\n{diff}"));
        }
    }
    assert!(
        failures.is_empty(),
        "states differ:\n\n{}",
        failures.join("\n")
    );
}
