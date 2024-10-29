//! End-to-end coverage for creation-time tag rules, driving the real
//! [`CatalogWriter::run`] loop.
//!
//! These tests are deliberately not unit tests of the matcher (that lives in
//! `configuration::tests`). What needs pinning down here is *where* rules run:
//! on the two local creation paths and nowhere else. That boundary is a
//! property of the call sites, so it can only be observed by feeding real
//! messages onto the ingest bus and watching what comes out.
//!
//! Moved out of `lib.rs` in restructure 4.1: it boots the whole stack through
//! the crate's public surface (`CatalogWriter`, `ApiService`, the config
//! types), so it is an integration test rather than a unit test.

use std::sync::Arc;
use std::time::Duration;

use tagsy_core::state::{Change, ChangeOrigin};
use tagsy_core::{FileId, LogicalPath, TagId};
use tagsyd::catalog::CatalogWriter;
use tagsyd::catalog::messages::{CatalogCommand, ContentChange, Ingest};
use tagsyd::clock;
use tagsyd::configuration::{
    CompiledTagRules, Configuration, PreviewGenerationPolicy, RuntimeConfiguration, TagRule,
};
use tagsyd::frontend::api::ApiService;
use tagsyd::operations::Operations;
use tagsyd::peer::relay::{ChunkRelay, PreviewRelay};
use tagsyd::store::CatalogStore;
use tagsyd::sync_directories::SyncDirectoryCommand;
use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

/// How long to wait for a change to surface on the event stream. Generous:
/// a slow machine must not produce a flaky pass, and the negative tests
/// spend this in full.
const SETTLE: Duration = Duration::from_millis(500);

/// A running `handle_changes` over a scratch catalog, with no sync
/// directories and no peers.
///
/// Both are absent on purpose: with no sync directory nothing is
/// materialized to disk and with no peer nothing is forwarded, which
/// strips the loop down to the catalog write and the event publication —
/// exactly the two effects these tests care about. Every change
/// `handle_changes` applies is still published to `events`, so the event
/// stream is a faithful view of what was decided.
///
/// The catalog is a temp *file* rather than `:memory:` because the bundled
/// [`ApiService`] opens its own read handle by path, exactly as it does in
/// production; an in-memory database would not be shared between the two.
struct Harness {
    changes: UnboundedSender<CatalogCommand>,
    events: tokio::sync::broadcast::Receiver<Change>,
    api: ApiService,
    shutdown: CancellationToken,
    data_dir: std::path::PathBuf,
    /// Held only to keep the channel open. The sync-directory manager is
    /// never started, and dropping this would make `handle_changes`'s sends
    /// fail rather than simply go nowhere.
    _commands: tokio::sync::mpsc::UnboundedReceiver<SyncDirectoryCommand>,
}

impl Harness {
    fn new(tag_rules: Vec<TagRule>) -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let data_dir = std::env::temp_dir().join(format!(
            "tagsy-tag-rule-test-{}-{}",
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&data_dir).expect("create test data dir");
        let main_db_path = data_dir.join("main.db");

        let configuration = Configuration {
            sync_directories: Vec::new(),
            listen_port: None,
            peers: Vec::new(),
            tags: Vec::new(),
            preview_generation_policy: PreviewGenerationPolicy::Never,
            editor_rules: Vec::new(),
            tag_rules,
        };
        let compiled = Arc::new(CompiledTagRules::compile(&configuration.tag_rules));
        let runtime_configuration =
            Arc::new(RwLock::new(RuntimeConfiguration::new(&configuration)));
        let database = CatalogStore::initialize(&main_db_path).expect("open test db");

        let (change_sender, change_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (event_sender, event_receiver) = tokio::sync::broadcast::channel(64);
        let shutdown = CancellationToken::new();
        let pending_fetches = ChunkRelay::new(runtime_configuration.clone());
        let operations = Operations::new();

        let api = ApiService::new(
            main_db_path,
            change_sender.clone(),
            command_sender.clone(),
            event_sender.clone(),
            pending_fetches.clone(),
            data_dir.join("fetch-temp"),
            operations.clone(),
            Vec::new(),
            compiled.clone(),
            tagsyd::paths::Paths::new(
                data_dir.clone(),
                None::<std::path::PathBuf>,
                data_dir.join("identity.key"),
            ),
        );

        let catalog = CatalogWriter {
            configuration,
            tag_rules: compiled,
            runtime_configuration: runtime_configuration.clone(),
            pending_fetches,
            pending_previews: PreviewRelay::new(runtime_configuration),
            database,
            change_sender: change_sender.clone(),
            command_sender,
            event_sender,
            operations,
            shutdown: shutdown.clone(),
        };
        tokio::spawn(catalog.run(change_receiver));

        Self {
            changes: change_sender,
            events: event_receiver,
            api,
            shutdown,
            data_dir,
            _commands: command_receiver,
        }
    }

    /// Upload a file and wait until it is in the catalog, returning its id
    /// and the tags the announcement carried.
    async fn upload(&mut self, path: &str, tags: Vec<TagId>) -> (FileId, Vec<TagId>) {
        let file_id = FileId::new();
        self.send(CatalogCommand::AnnounceProvided {
            file_id,
            logical_path: Some(LogicalPath::new(path)),
            content_hash: format!("hash-{path}"),
            size: 1,
            tags,
        });
        let tags = self
            .expect("the upload announcement", |change| {
                added_tags(change, file_id)
            })
            .await;
        (file_id, tags)
    }

    fn send(&self, message: CatalogCommand) {
        self.changes.send(message).expect("ingest bus is alive");
    }

    /// Wait for the first published change satisfying `predicate`, or panic
    /// once `SETTLE` elapses.
    async fn expect<T>(&mut self, what: &str, predicate: impl Fn(&Change) -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + SETTLE;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let change = tokio::time::timeout(remaining, self.events.recv())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
                .expect("event stream stayed open");
            if let Some(value) = predicate(&change) {
                return value;
            }
        }
    }

    /// Assert no published change satisfies `predicate` within `SETTLE`.
    async fn expect_none(&mut self, what: &str, predicate: impl Fn(&Change) -> bool) {
        let deadline = tokio::time::Instant::now() + SETTLE;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return;
            }
            match tokio::time::timeout(remaining, self.events.recv()).await {
                // Elapsed without a disqualifying change: the assertion holds.
                Err(_) => return,
                Ok(Ok(change)) => assert!(!predicate(&change), "unexpected {what}: {change:?}"),
                Ok(Err(_)) => return,
            }
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn markdown_rule(tag_id: TagId) -> Vec<TagRule> {
    vec![TagRule {
        pattern: r"\.md$".to_owned(),
        tags: vec![tag_id],
    }]
}

/// The tags carried by the `FileMetadataAdded` published for `file_id`.
fn added_tags(change: &Change, file_id: FileId) -> Option<Vec<TagId>> {
    match change {
        Change::FileMetadataAdded {
            file_id: got, tags, ..
        } if *got == file_id => Some(tags.clone()),
        _ => None,
    }
}

/// Hook 1: a client upload (`ApiService::upload_file`) whose logical path
/// matches gets the rule's tag, and it is carried on the announcement so peers
/// learn of it too.
#[tokio::test]
async fn upload_applies_a_matching_rule() {
    let tag_id = TagId::new();
    let mut harness = Harness::new(markdown_rule(tag_id));
    let file_id = FileId::new();

    harness.send(CatalogCommand::AnnounceProvided {
        file_id,
        logical_path: Some(LogicalPath::new("notes/todo.md")),
        content_hash: "hash".to_owned(),
        size: 1,
        tags: Vec::new(),
    });

    let tags = harness
        .expect("the upload announcement", |change| {
            added_tags(change, file_id)
        })
        .await;
    assert_eq!(tags, vec![tag_id]);
}

/// A non-matching upload is left exactly as the caller specified.
#[tokio::test]
async fn upload_without_a_match_is_untouched() {
    let mut harness = Harness::new(markdown_rule(TagId::new()));
    let file_id = FileId::new();

    harness.send(CatalogCommand::AnnounceProvided {
        file_id,
        logical_path: Some(LogicalPath::new("notes/todo.txt")),
        content_hash: "hash".to_owned(),
        size: 1,
        tags: Vec::new(),
    });

    let tags = harness
        .expect("the upload announcement", |change| {
            added_tags(change, file_id)
        })
        .await;
    assert!(tags.is_empty());
}

/// Rule tags are merged with the caller's, never substituted for them.
#[tokio::test]
async fn upload_merges_rule_tags_with_caller_tags() {
    let rule_tag = TagId::new();
    let caller_tag = TagId::new();
    let mut harness = Harness::new(markdown_rule(rule_tag));
    let file_id = FileId::new();

    harness.send(CatalogCommand::AnnounceProvided {
        file_id,
        logical_path: Some(LogicalPath::new("notes/todo.md")),
        content_hash: "hash".to_owned(),
        size: 1,
        tags: vec![caller_tag],
    });

    let tags = harness
        .expect("the upload announcement", |change| {
            added_tags(change, file_id)
        })
        .await;
    assert_eq!(tags, vec![caller_tag, rule_tag]);
}

/// A caller-supplied tag that a rule would also assign appears once.
#[tokio::test]
async fn upload_does_not_duplicate_an_already_supplied_tag() {
    let tag_id = TagId::new();
    let mut harness = Harness::new(markdown_rule(tag_id));
    let file_id = FileId::new();

    harness.send(CatalogCommand::AnnounceProvided {
        file_id,
        logical_path: Some(LogicalPath::new("notes/todo.md")),
        content_hash: "hash".to_owned(),
        size: 1,
        tags: vec![tag_id],
    });

    let tags = harness
        .expect("the upload announcement", |change| {
            added_tags(change, file_id)
        })
        .await;
    assert_eq!(tags, vec![tag_id]);
}

/// Hook 2: a file appearing in a local sync directory is a creation too,
/// and rules apply to it on the same terms.
#[tokio::test]
async fn local_file_added_applies_a_matching_rule() {
    let tag_id = TagId::new();
    let mut harness = Harness::new(markdown_rule(tag_id));
    let file_id = FileId::new();

    harness.send(CatalogCommand::Change(
        Ingest::Content(ContentChange::FileAdded {
            file_id,
            logical_path: LogicalPath::new("notes/todo.md"),
            content: tagsyd::file_bytes::FileBytes::InMemory(b"x".to_vec()),
            content_hash: "hash".to_owned(),
            size: 1,
            tags: Vec::new(),
        }),
        ChangeOrigin::Local {
            directory_path: std::path::PathBuf::new(),
        },
    ));

    let tags = harness
        .expect("the local ingestion announcement", |change| {
            added_tags(change, file_id)
        })
        .await;
    assert_eq!(tags, vec![tag_id]);
}

/// The central negative case: rules run only on the device that creates a
/// file. A peer-originated add already carries whatever tags its origin's
/// rules assigned, so re-applying ours would let two devices with different
/// rule sets disagree about the same file forever.
#[tokio::test]
async fn peer_file_added_does_not_apply_rules() {
    let tag_id = TagId::new();
    let mut harness = Harness::new(markdown_rule(tag_id));
    let file_id = FileId::new();

    harness.send(CatalogCommand::Change(
        Ingest::Content(ContentChange::FileAdded {
            file_id,
            logical_path: LogicalPath::new("notes/todo.md"),
            content: tagsyd::file_bytes::FileBytes::InMemory(b"x".to_vec()),
            content_hash: "hash".to_owned(),
            size: 1,
            tags: Vec::new(),
        }),
        ChangeOrigin::Peer {
            public_key: "a-peer".to_owned(),
        },
    ));

    let tags = harness
        .expect("the inbound announcement", |change| {
            added_tags(change, file_id)
        })
        .await;
    assert!(
        tags.is_empty(),
        "a peer's file must not be re-tagged by our rules"
    );
}

/// The other central negative case, and the one the feature was scoped
/// around: renaming a file into a matching path does *not* tag it. Rules
/// are a creation-time default; once a file exists its tags belong to the
/// user. See `TagRule` for why re-running on move has no correct answer.
#[tokio::test]
async fn moving_a_file_into_a_matching_path_does_not_apply_rules() {
    let tag_id = TagId::new();
    let mut harness = Harness::new(markdown_rule(tag_id));
    let file_id = FileId::new();

    // Create it under a name no rule matches.
    harness.send(CatalogCommand::AnnounceProvided {
        file_id,
        logical_path: Some(LogicalPath::new("notes/todo.txt")),
        content_hash: "hash".to_owned(),
        size: 1,
        tags: Vec::new(),
    });
    let tags = harness
        .expect("the upload announcement", |change| {
            added_tags(change, file_id)
        })
        .await;
    assert!(tags.is_empty(), "precondition: created untagged");

    // Rename it onto a path the rule matches.
    harness.send(CatalogCommand::Change(
        Ingest::from_change(Change::FileMoved {
            file_id,
            logical_path: LogicalPath::new("notes/todo.md"),
            modified_at: clock::now_millis(),
        }),
        ChangeOrigin::Local {
            directory_path: std::path::PathBuf::new(),
        },
    ));

    harness
        .expect_none(
            "tagging triggered by a move",
            |change| matches!(change, Change::FileTagged { file_id: got, .. } if *got == file_id),
        )
        .await;
}

/// Replacing a file's content is not a creation either, so it cannot pick
/// up tags — even when the file's path matches a rule.
#[tokio::test]
async fn editing_content_does_not_apply_rules() {
    let tag_id = TagId::new();
    let mut harness = Harness::new(markdown_rule(tag_id));
    let file_id = FileId::new();

    harness.send(CatalogCommand::AnnounceProvided {
        file_id,
        logical_path: Some(LogicalPath::new("notes/todo.md")),
        content_hash: "hash".to_owned(),
        size: 1,
        tags: Vec::new(),
    });
    harness
        .expect("the upload announcement", |change| {
            added_tags(change, file_id)
        })
        .await;

    // A content-only republication (`ApiService::edit_file`): no logical path.
    harness.send(CatalogCommand::AnnounceProvided {
        file_id,
        logical_path: None,
        content_hash: "hash2".to_owned(),
        size: 2,
        tags: Vec::new(),
    });

    harness
        .expect_none(
            "tagging triggered by an edit",
            |change| matches!(change, Change::FileTagged { file_id: got, .. } if *got == file_id),
        )
        .await;
}

/// `retag` is the recovery path for the "rules do not run on move"
/// restriction: a file renamed into a matching path stays untagged until
/// the operator asks for it, and then gets tagged.
#[tokio::test]
async fn retag_catches_up_a_file_a_rule_now_matches() {
    let tag_id = TagId::new();
    let mut harness = Harness::new(markdown_rule(tag_id));

    let (file_id, tags) = harness.upload("notes/todo.txt", Vec::new()).await;
    assert!(tags.is_empty(), "precondition: created untagged");

    harness.send(CatalogCommand::Change(
        Ingest::from_change(Change::FileMoved {
            file_id,
            logical_path: LogicalPath::new("notes/todo.md"),
            modified_at: clock::now_millis(),
        }),
        ChangeOrigin::Local {
            directory_path: std::path::PathBuf::new(),
        },
    ));
    harness
        .expect("the move to be applied", |change| {
            matches!(change, Change::FileMoved { file_id: got, .. } if *got == file_id)
                .then_some(())
        })
        .await;

    let summary = harness.api.retag(false).expect("retag succeeds");
    assert_eq!(summary.files_scanned, 1);
    assert_eq!(summary.files_changed, 1);
    assert_eq!(summary.tags_applied, 1);

    let applied = harness
        .expect("the retagging", |change| match change {
            Change::FileTagged {
                file_id: got,
                tag_id,
                ..
            } if *got == file_id => Some(*tag_id),
            _ => None,
        })
        .await;
    assert_eq!(applied, tag_id);
}

/// A file that already carries the tag is not re-enqueued, so a second run
/// is a no-op. Without this, `retag` on a large catalog would flood the
/// bus (and every peer) with redundant changes on every invocation.
#[tokio::test]
async fn retag_is_idempotent() {
    let tag_id = TagId::new();
    let mut harness = Harness::new(markdown_rule(tag_id));

    // Created with a matching name, so the creation-time rule already
    // applied the tag.
    let (_file_id, tags) = harness.upload("notes/todo.md", Vec::new()).await;
    assert_eq!(tags, vec![tag_id]);

    let summary = harness.api.retag(false).expect("retag succeeds");
    assert_eq!(summary.files_scanned, 1);
    assert_eq!(
        summary.tags_applied, 0,
        "the tag is already applied; nothing to do"
    );
    assert_eq!(summary.files_changed, 0);
}

/// A dry run reports the same plan but enqueues nothing.
#[tokio::test]
async fn retag_dry_run_changes_nothing() {
    let tag_id = TagId::new();
    let mut harness = Harness::new(markdown_rule(tag_id));

    let (file_id, _) = harness.upload("notes/todo.txt", Vec::new()).await;
    harness.send(CatalogCommand::Change(
        Ingest::from_change(Change::FileMoved {
            file_id,
            logical_path: LogicalPath::new("notes/todo.md"),
            modified_at: clock::now_millis(),
        }),
        ChangeOrigin::Local {
            directory_path: std::path::PathBuf::new(),
        },
    ));
    harness
        .expect("the move to be applied", |change| {
            matches!(change, Change::FileMoved { file_id: got, .. } if *got == file_id)
                .then_some(())
        })
        .await;

    let summary = harness.api.retag(true).expect("dry run succeeds");
    assert_eq!(summary.tags_applied, 1, "the plan is still reported");

    harness
        .expect_none("tagging during a dry run", |change| {
            matches!(change, Change::FileTagged { .. })
        })
        .await;

    // And the plan is still there to be applied for real afterwards.
    let summary = harness.api.retag(false).expect("retag succeeds");
    assert_eq!(summary.tags_applied, 1);
}

/// `retag` never removes a tag, not even one no rule would assign. Nothing
/// distinguishes a rule-applied tag from a hand-applied one, so removal
/// could not be done without risking the user's own tagging.
#[tokio::test]
async fn retag_never_removes_tags() {
    let rule_tag = TagId::new();
    let manual_tag = TagId::new();
    let mut harness = Harness::new(markdown_rule(rule_tag));

    // Carries a tag no rule mentions, on a path no rule matches.
    let (_file_id, tags) = harness.upload("notes/todo.txt", vec![manual_tag]).await;
    assert_eq!(tags, vec![manual_tag]);

    let summary = harness.api.retag(false).expect("retag succeeds");
    assert_eq!(summary.tags_applied, 0);

    harness
        .expect_none("any untagging", |change| {
            matches!(change, Change::FileUntagged { .. })
        })
        .await;
}

/// A tombstoned file is skipped: tagging it would change nothing visible
/// and would resurrect the relationship in every peer's catalog.
#[tokio::test]
async fn retag_skips_deleted_files() {
    let tag_id = TagId::new();
    let mut harness = Harness::new(markdown_rule(tag_id));

    let (file_id, _) = harness.upload("notes/todo.txt", Vec::new()).await;
    harness.send(CatalogCommand::Change(
        Ingest::from_change(Change::FileMoved {
            file_id,
            logical_path: LogicalPath::new("notes/todo.md"),
            modified_at: clock::now_millis(),
        }),
        ChangeOrigin::Local {
            directory_path: std::path::PathBuf::new(),
        },
    ));
    harness.api.delete_file(file_id).expect("delete enqueued");
    harness
        .expect("the deletion to be applied", |change| {
            matches!(change, Change::FileDeleted { file_id: got, .. } if *got == file_id)
                .then_some(())
        })
        .await;

    let summary = harness.api.retag(false).expect("retag succeeds");
    assert_eq!(summary.files_scanned, 0);
    assert_eq!(summary.tags_applied, 0);
}

/// The report distinguishes the two independent faults: a pattern that
/// does not compile, and a tag id that names nothing.
#[tokio::test]
async fn tag_rule_report_lists_invalid_patterns_and_unknown_tags() {
    let unknown_tag = TagId::new();
    let harness = Harness::new(vec![
        TagRule {
            pattern: "*.md".to_owned(),
            tags: vec![TagId::new()],
        },
        TagRule {
            pattern: r"\.md$".to_owned(),
            tags: vec![unknown_tag],
        },
    ]);

    let report = harness.api.tag_rule_report().expect("report succeeds");
    assert_eq!(report.active, 1, "only the valid rule is live");
    assert_eq!(report.invalid.len(), 1);
    assert!(
        report.invalid[0].contains("*.md"),
        "the diagnostic names the offending pattern: {}",
        report.invalid[0]
    );
    assert_eq!(
        report.unknown_tags,
        vec![unknown_tag],
        "no tag with this id has ever been created"
    );
}

/// A broken rule does not stop the daemon, and does not stop its siblings
/// from working. This is the availability property `CompiledTagRules`
/// documents, observed end to end.
#[tokio::test]
async fn a_broken_rule_does_not_disable_the_others() {
    let tag_id = TagId::new();
    let mut harness = Harness::new(vec![
        TagRule {
            pattern: "*.md".to_owned(),
            tags: vec![TagId::new()],
        },
        TagRule {
            pattern: r"\.md$".to_owned(),
            tags: vec![tag_id],
        },
    ]);
    let file_id = FileId::new();

    harness.send(CatalogCommand::AnnounceProvided {
        file_id,
        logical_path: Some(LogicalPath::new("notes/todo.md")),
        content_hash: "hash".to_owned(),
        size: 1,
        tags: Vec::new(),
    });

    let tags = harness
        .expect("the upload announcement", |change| {
            added_tags(change, file_id)
        })
        .await;
    assert_eq!(tags, vec![tag_id]);
}
