//! Live-vs-reconnect scenarios. Each test is one [`Scenario`] checked by
//! [`script::check`]: the script must end in the same converged state whether
//! the other nodes watched it happen live or learned it on reconnect.
//!
//! Scenarios are small and single-purpose so a failure names the operation
//! that diverged. Most come in two directions — the phone acting while central
//! is offline, and central acting while the phone is offline — because the
//! two sides take different code paths (TagBased vs. Universal placement,
//! dialer vs. listener).

use crate::script::{self, Scenario, Step, hub_and_spoke, hub_with_vault, relay_line, two_phones};

fn bytes(text: &str) -> Vec<u8> {
    text.as_bytes().to_vec()
}

fn upload(on: &'static str, file: &'static str, path: &'static str, tags: &[&'static str]) -> Step {
    Step::Upload {
        on,
        file,
        path,
        bytes: bytes(&format!("{file} @ {path}")),
        tags: tags.to_vec(),
    }
}

fn write(on: &'static str, file: &'static str, dir: &'static str, path: &'static str) -> Step {
    Step::Write {
        on,
        file,
        dir,
        path,
        bytes: bytes(&format!("{file} written to {dir}/{path}")),
    }
}

fn edit(on: &'static str, file: &'static str, text: &str) -> Step {
    Step::Edit {
        on,
        file,
        bytes: bytes(text),
    }
}

fn scenario(
    topology: fn(&mut crate::harness::Cluster) -> script::Roles,
    setup: Vec<Step>,
    steps: Vec<Step>,
    offline: &[&'static str],
) -> Scenario {
    Scenario {
        topology,
        setup,
        steps,
        offline: offline.to_vec(),
        configure: None,
    }
}

// ---- Creating files ------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phone_creates_files() {
    script::check(scenario(
        hub_and_spoke,
        vec![],
        vec![
            upload("phone", "tagged", "notes/tagged.txt", &["phone"]),
            upload("phone", "plain", "plain.txt", &[]),
            write("phone", "written", "phone", "photos/cat.jpg"),
        ],
        &["central"],
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn central_creates_files() {
    script::check(scenario(
        hub_and_spoke,
        vec![],
        vec![
            upload("central", "tagged", "docs/for-phone.txt", &["phone"]),
            upload("central", "plain", "docs/archive-only.txt", &[]),
            write("central", "dropped", "store", "dropped.txt"),
        ],
        &["phone"],
    ))
    .await;
}

// ---- Modifying files -----------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phone_modifies_files() {
    script::check(scenario(
        hub_and_spoke,
        vec![
            upload("phone", "api", "api.txt", &["phone"]),
            write("phone", "disk", "phone", "disk.txt"),
        ],
        vec![
            edit("phone", "api", "api, second version"),
            Step::Overwrite {
                on: "phone",
                dir: "phone",
                path: "disk.txt",
                bytes: bytes("disk, second version"),
            },
        ],
        &["central"],
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn central_modifies_files() {
    script::check(scenario(
        hub_and_spoke,
        vec![
            upload("central", "tagged", "tagged.txt", &["phone"]),
            upload("central", "plain", "plain.txt", &[]),
        ],
        vec![
            edit("central", "tagged", "tagged, second version"),
            edit("central", "plain", "plain, second version"),
        ],
        &["phone"],
    ))
    .await;
}

// ---- Deleting and restoring ---------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phone_deletes_files() {
    script::check(scenario(
        hub_and_spoke,
        vec![
            upload("phone", "api", "api.txt", &["phone"]),
            write("phone", "disk", "phone", "disk.txt"),
        ],
        vec![
            Step::Delete {
                on: "phone",
                file: "api",
            },
            Step::Remove {
                on: "phone",
                dir: "phone",
                path: "disk.txt",
            },
        ],
        &["central"],
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn central_deletes_files() {
    script::check(scenario(
        hub_and_spoke,
        vec![
            upload("central", "tagged", "tagged.txt", &["phone"]),
            upload("central", "plain", "plain.txt", &[]),
        ],
        vec![
            Step::Delete {
                on: "central",
                file: "tagged",
            },
            Step::Delete {
                on: "central",
                file: "plain",
            },
        ],
        &["phone"],
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn central_restores_deleted_file() {
    script::check(scenario(
        hub_with_vault,
        vec![
            upload("central", "doc", "doc.txt", &["phone"]),
            Step::Delete {
                on: "central",
                file: "doc",
            },
        ],
        vec![Step::Restore {
            on: "central",
            file: "doc",
        }],
        &["phone"],
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn central_purges_deleted_file() {
    script::check(scenario(
        hub_and_spoke,
        vec![
            upload("central", "doomed", "doomed.txt", &["phone"]),
            upload("central", "kept", "kept.txt", &["phone"]),
            Step::Delete {
                on: "central",
                file: "doomed",
            },
        ],
        vec![Step::PurgeDeleted { on: "central" }],
        &["phone"],
    ))
    .await;
}

// ---- Moving files --------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phone_moves_files() {
    script::check(scenario(
        hub_and_spoke,
        vec![
            upload("phone", "api", "old/api.txt", &["phone"]),
            write("phone", "disk", "phone", "disk.txt"),
        ],
        vec![
            Step::Move {
                on: "phone",
                file: "api",
                to: "new/api.txt",
            },
            Step::Rename {
                on: "phone",
                dir: "phone",
                from: "disk.txt",
                to: "renamed/disk.txt",
            },
        ],
        &["central"],
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn central_moves_file() {
    script::check(scenario(
        hub_and_spoke,
        vec![upload("central", "doc", "old/doc.txt", &["phone"])],
        vec![Step::Move {
            on: "central",
            file: "doc",
            to: "new/doc.txt",
        }],
        &["phone"],
    ))
    .await;
}

// ---- Tags ----------------------------------------------------------------

/// The whole tag lifecycle, performed on `on` while `offline` is away.
fn tag_lifecycle(on: &'static str, offline: &'static str) -> Scenario {
    scenario(
        hub_and_spoke,
        vec![upload("central", "doc", "doc.txt", &[]), Step::CreateTag {
            on: "central",
            tag: "old",
        }],
        vec![
            Step::CreateTag { on, tag: "work" },
            Step::CreateTag { on, tag: "urgent" },
            Step::TagFile {
                on,
                tag: "work",
                file: "doc",
            },
            Step::TagFile {
                on,
                tag: "urgent",
                file: "doc",
            },
            Step::TagTag {
                on,
                parent: "work",
                child: "urgent",
            },
            Step::RenameTag {
                on,
                tag: "work",
                to: "job",
            },
            Step::RecolorTag {
                on,
                tag: "urgent",
                dot_color: "#ff0000",
            },
            Step::UntagFile {
                on,
                tag: "urgent",
                file: "doc",
            },
            Step::DeleteTag { on, tag: "old" },
        ],
        &[offline],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phone_manages_tags() {
    script::check(tag_lifecycle("phone", "central")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn central_manages_tags() {
    script::check(tag_lifecycle("central", "phone")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn central_untags_tag_from_tag() {
    script::check(scenario(
        hub_and_spoke,
        vec![
            Step::CreateTag {
                on: "central",
                tag: "parent",
            },
            Step::CreateTag {
                on: "central",
                tag: "child",
            },
            Step::TagTag {
                on: "central",
                parent: "parent",
                child: "child",
            },
        ],
        vec![
            Step::UntagTag {
                on: "central",
                parent: "parent",
                child: "child",
            },
            Step::RestoreTag {
                on: "central",
                tag: "child",
            },
        ],
        &["phone"],
    ))
    .await;
}

// ---- Tag-driven placement -------------------------------------------------

/// Tagging moves files into the phone's directory, untagging moves them out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn central_retags_files_for_phone() {
    script::check(scenario(
        hub_and_spoke,
        vec![
            upload("central", "joins", "joins.txt", &[]),
            upload("central", "leaves", "leaves.txt", &["phone"]),
        ],
        vec![
            Step::TagFile {
                on: "central",
                tag: "phone",
                file: "joins",
            },
            Step::UntagFile {
                on: "central",
                tag: "phone",
                file: "leaves",
            },
        ],
        &["phone"],
    ))
    .await;
}

/// The phone tags a file whose bytes only central holds — while central is
/// away, so the placement can only complete after reconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phone_retags_files_for_itself() {
    script::check(scenario(
        hub_and_spoke,
        vec![
            upload("central", "joins", "joins.txt", &[]),
            upload("central", "leaves", "leaves.txt", &["phone"]),
        ],
        vec![
            Step::TagFile {
                on: "phone",
                tag: "phone",
                file: "joins",
            },
            Step::UntagFile {
                on: "phone",
                tag: "phone",
                file: "leaves",
            },
        ],
        &["central"],
    ))
    .await;
}

// ---- Conflicts between two phones (central offline partitions them) ------

fn two_phone_conflict(setup: Vec<Step>, steps: Vec<Step>) -> Scenario {
    scenario(two_phones, setup, steps, &["central"])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phones_edit_same_file() {
    script::check(two_phone_conflict(
        vec![upload("central", "doc", "doc.txt", &["a", "b"])],
        vec![
            edit("phone_a", "doc", "version from phone a"),
            edit("phone_b", "doc", "version from phone b"),
        ],
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phone_deletes_while_other_edits() {
    script::check(two_phone_conflict(
        vec![upload("central", "doc", "doc.txt", &["a", "b"])],
        vec![
            Step::Delete {
                on: "phone_a",
                file: "doc",
            },
            edit("phone_b", "doc", "edited after the delete"),
        ],
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phones_move_same_file() {
    script::check(two_phone_conflict(
        vec![upload("central", "doc", "doc.txt", &["a", "b"])],
        vec![
            Step::Move {
                on: "phone_a",
                file: "doc",
                to: "from-a/doc.txt",
            },
            Step::Move {
                on: "phone_b",
                file: "doc",
                to: "from-b/doc.txt",
            },
        ],
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phones_rename_same_tag() {
    script::check(two_phone_conflict(
        vec![Step::CreateTag {
            on: "central",
            tag: "shared",
        }],
        vec![
            Step::RenameTag {
                on: "phone_a",
                tag: "shared",
                to: "renamed-by-a",
            },
            Step::RenameTag {
                on: "phone_b",
                tag: "shared",
                to: "renamed-by-b",
            },
        ],
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phones_tag_and_untag_same_file() {
    script::check(two_phone_conflict(
        vec![upload("central", "doc", "doc.txt", &[]), Step::CreateTag {
            on: "central",
            tag: "t",
        }],
        vec![
            Step::TagFile {
                on: "phone_a",
                tag: "t",
                file: "doc",
            },
            Step::UntagFile {
                on: "phone_b",
                tag: "t",
                file: "doc",
            },
        ],
    ))
    .await;
}

// ---- Relaying through a node that holds no bytes -------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_to_phone_through_relay() {
    script::check(scenario(
        relay_line,
        vec![],
        vec![
            upload("archive", "tagged", "tagged.txt", &["phone"]),
            upload("archive", "plain", "plain.txt", &[]),
        ],
        &["phone"],
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phone_to_archive_through_relay() {
    script::check(scenario(
        relay_line,
        vec![],
        vec![
            upload("phone", "tagged", "tagged.txt", &["phone"]),
            write("phone", "written", "phone", "written.txt"),
        ],
        &["archive"],
    ))
    .await;
}

// ---- Manifests split into many frames ------------------------------------

/// Everything at once, with every manifest split into single-entry frames.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tiny_manifest_batches() {
    let mut scenario = scenario(
        hub_and_spoke,
        vec![upload("central", "existing", "existing.txt", &["phone"])],
        vec![
            upload("central", "a", "a.txt", &["phone"]),
            upload("central", "b", "b.txt", &[]),
            upload("central", "c", "c.txt", &["phone"]),
            edit("central", "existing", "existing, edited"),
            Step::CreateTag {
                on: "central",
                tag: "x",
            },
            Step::CreateTag {
                on: "central",
                tag: "y",
            },
            Step::TagFile {
                on: "central",
                tag: "x",
                file: "a",
            },
            Step::TagFile {
                on: "central",
                tag: "y",
                file: "b",
            },
            Step::Delete {
                on: "central",
                file: "c",
            },
            Step::PurgeDeleted { on: "central" },
        ],
        &["phone"],
    );
    scenario.configure = Some(|configuration| {
        configuration.manifest_batch_size = 1;
        configuration.tag_manifest_batch_size = 1;
        configuration.purge_manifest_batch_size = 1;
    });
    script::check(scenario).await;
}
