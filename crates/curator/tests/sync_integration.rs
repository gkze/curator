//! Integration tests for sync operations.
//!
//! These tests ensure that sync operations complete within reasonable timeouts
//! and don't hang due to deadlocks, spin loops, or other concurrency issues.
//!
//! Key scenarios tested:
//! - Streaming sync completes with various data sizes
//! - Channel closure properly signals task termination
//! - Select loops don't spin when streams exhaust
//! - Persist task flushes final batches correctly

#![cfg(all(feature = "sqlite", feature = "migrate", feature = "github"))]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use curator::connect_and_migrate;
use curator::entity::code_repository::ActiveModel;
use curator::entity::code_visibility::CodeVisibility;
use curator::entity::instance::{ActiveModel as InstanceActiveModel, Entity as Instance};
use curator::entity::platform_type::PlatformType;
use curator::repository;
use curator::sync::{
    PERSIST_BATCH_SIZE, PersistTaskResult, SyncProgress, await_persist_task, create_model_channel,
    spawn_persist_task,
};
use sea_orm::{ConnectionTrait, EntityTrait, Set};
use tokio::sync::mpsc;
use uuid::Uuid;

/// Maximum time any sync operation should take in tests.
/// If exceeded, there's likely a hang/deadlock.
const SYNC_TIMEOUT: Duration = Duration::from_secs(10);

/// Shorter timeout for operations that should be nearly instant.
const FAST_TIMEOUT: Duration = Duration::from_secs(2);

/// Test instance ID used for all sync tests
fn test_instance_id() -> Uuid {
    Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
}

/// Create an in-memory SQLite database with migrations applied and a test instance.
///
/// Note: Uses unique test host (not github.com) to avoid conflicts
/// with the well-known instances seeded by migrations.
async fn setup_test_db() -> sea_orm::DatabaseConnection {
    let db = connect_and_migrate("sqlite::memory:")
        .await
        .expect("Failed to create test database");

    // Create a test instance for the foreign key constraint
    let now = Utc::now();
    let instance = InstanceActiveModel {
        id: Set(test_instance_id()),
        name: Set("test-github".to_string()),
        platform_type: Set(PlatformType::GitHub),
        host: Set("test-github.example.com".to_string()),
        oauth_client_id: Set(None),
        oauth_flow: Set("auto".to_string()),
        created_at: Set(now.fixed_offset()),
    };

    Instance::insert(instance)
        .exec(&db)
        .await
        .expect("Failed to create test instance");

    db
}

/// Generate a deterministic platform_id from owner/name.
fn platform_id_from_name(owner: &str, name: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    owner.hash(&mut hasher);
    name.hash(&mut hasher);
    hasher.finish() as i64
}

/// Create a test ActiveModel for persistence tests.
fn create_test_model(owner: &str, name: &str) -> ActiveModel {
    let now = Utc::now();
    ActiveModel {
        id: Set(Uuid::new_v4()),
        instance_id: Set(test_instance_id()), // Use the test instance
        platform_id: Set(platform_id_from_name(owner, name)),
        owner: Set(owner.to_string()),
        name: Set(name.to_string()),
        description: Set(Some(format!("Test repo {}/{}", owner, name))),
        default_branch: Set("main".to_string()),
        topics: Set(serde_json::json!(["test"])),
        primary_language: Set(Some("Rust".to_string())),
        license_spdx: Set(Some("MIT".to_string())),
        homepage: Set(None),
        visibility: Set(CodeVisibility::Public),
        is_fork: Set(false),
        is_mirror: Set(false),
        is_archived: Set(false),
        is_template: Set(false),
        is_empty: Set(false),
        stars: Set(Some(100)),
        forks: Set(Some(10)),
        open_issues: Set(Some(5)),
        watchers: Set(Some(50)),
        size_kb: Set(Some(1024)),
        has_issues: Set(true),
        has_wiki: Set(true),
        has_pull_requests: Set(true),
        created_at: Set(Some(now.fixed_offset())),
        updated_at: Set(Some(now.fixed_offset())),
        pushed_at: Set(Some(now.fixed_offset())),
        platform_metadata: Set(serde_json::json!({})),
        synced_at: Set(now.fixed_offset()),
        etag: Set(None),
    }
}

// ─── Persist Task Completion Tests ─────────────────────────────────────────────
// These tests ensure the persist task properly terminates when channels close.

/// Test that the persist task completes when the sender is dropped.
/// This catches bugs where channels don't close properly.
#[tokio::test]
async fn test_persist_task_completes_on_sender_drop() {
    let db = Arc::new(setup_test_db().await);
    let (tx, rx) = mpsc::channel::<ActiveModel>(100);

    let (handle, _counter) = spawn_persist_task(db, rx, None, None);

    // Send a few models
    for i in 0..5 {
        tx.send(create_test_model("test-owner", &format!("repo-{}", i)))
            .await
            .unwrap();
    }

    // Drop sender to signal completion
    drop(tx);

    // Task should complete within timeout
    let result = tokio::time::timeout(FAST_TIMEOUT, await_persist_task(handle)).await;

    assert!(
        result.is_ok(),
        "Persist task should complete when sender is dropped, not hang"
    );

    let persist_result = result.unwrap();
    assert_eq!(persist_result.saved_count, 5);
    assert!(!persist_result.has_errors());
}

/// Test that the persist task completes immediately with empty channel.
/// Ensures no unnecessary waiting when there's no work.
#[tokio::test]
async fn test_persist_task_completes_immediately_when_empty() {
    let db = Arc::new(setup_test_db().await);
    let (tx, rx) = mpsc::channel::<ActiveModel>(100);

    let (handle, _counter) = spawn_persist_task(db, rx, None, None);

    // Drop sender immediately without sending anything
    drop(tx);

    // Should complete almost instantly
    let result = tokio::time::timeout(Duration::from_millis(500), await_persist_task(handle)).await;

    assert!(
        result.is_ok(),
        "Persist task should complete immediately with empty channel"
    );

    let persist_result = result.unwrap();
    assert_eq!(persist_result.saved_count, 0);
}

#[tokio::test]
async fn test_persist_task_preserves_repository_id_across_rename_batches() {
    let db = Arc::new(setup_test_db().await);
    let instance_id = test_instance_id();

    let mut original_alchemy = create_test_model("alchemy-run", "alchemy");
    original_alchemy.platform_id = Set(917_974_798);
    let mut original_alchemy_effect = create_test_model("alchemy-run", "alchemy-effect");
    original_alchemy_effect.platform_id = Set(1_081_394_458);
    repository::bulk_upsert(&db, vec![original_alchemy, original_alchemy_effect])
        .await
        .unwrap();

    let original_alchemy_id = repository::find_by_platform_id(&db, instance_id, 917_974_798)
        .await
        .unwrap()
        .unwrap()
        .id;

    let (tx, rx) = mpsc::channel::<ActiveModel>(PERSIST_BATCH_SIZE + 1);
    let (handle, _counter) = spawn_persist_task(Arc::clone(&db), rx, None, None);

    // Fill the first persistence batch with the successor before sending the
    // displaced repository in the next batch.
    let mut renamed_alchemy_effect = create_test_model("alchemy-run", "alchemy");
    renamed_alchemy_effect.platform_id = Set(1_081_394_458);
    tx.send(renamed_alchemy_effect).await.unwrap();
    for i in 1..PERSIST_BATCH_SIZE {
        let mut filler = create_test_model("filler", &format!("repo-{i}"));
        filler.platform_id = Set(2_000_000_000 + i as i64);
        tx.send(filler).await.unwrap();
    }

    let mut renamed_alchemy = create_test_model("alchemy-run", "alchemy-async");
    renamed_alchemy.platform_id = Set(917_974_798);
    tx.send(renamed_alchemy).await.unwrap();
    drop(tx);

    let result = await_persist_task(handle).await;
    assert!(!result.has_errors());

    let renamed_alchemy = repository::find_by_platform_id(&db, instance_id, 917_974_798)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renamed_alchemy.id, original_alchemy_id);
    assert_eq!(renamed_alchemy.name, "alchemy-async");
}

#[tokio::test]
async fn test_failed_final_rename_batch_rolls_back_without_losing_existing_rows() {
    let db = Arc::new(setup_test_db().await);
    let instance_id = test_instance_id();

    let mut original_alchemy = create_test_model("alchemy-run", "alchemy");
    original_alchemy.platform_id = Set(917_974_798);
    let mut original_alchemy_effect = create_test_model("alchemy-run", "alchemy-effect");
    original_alchemy_effect.platform_id = Set(1_081_394_458);
    repository::bulk_upsert(&db, vec![original_alchemy, original_alchemy_effect])
        .await
        .unwrap();

    let original_alchemy = repository::find_by_platform_id(&db, instance_id, 917_974_798)
        .await
        .unwrap()
        .unwrap();
    let original_alchemy_effect = repository::find_by_platform_id(&db, instance_id, 1_081_394_458)
        .await
        .unwrap()
        .unwrap();

    db.execute_unprepared(
        "CREATE TRIGGER fail_alchemy_async \
         BEFORE UPDATE ON code_repositories \
         WHEN NEW.name = 'alchemy-async' \
         BEGIN SELECT RAISE(FAIL, 'forced rename failure'); END",
    )
    .await
    .unwrap();

    let (tx, rx) = mpsc::channel::<ActiveModel>(PERSIST_BATCH_SIZE + 1);
    let (handle, _counter) = spawn_persist_task(Arc::clone(&db), rx, None, None);

    let mut renamed_alchemy_effect = create_test_model("alchemy-run", "alchemy");
    renamed_alchemy_effect.platform_id = Set(1_081_394_458);
    tx.send(renamed_alchemy_effect).await.unwrap();
    for i in 1..PERSIST_BATCH_SIZE {
        let mut filler = create_test_model("filler", &format!("rollback-{i}"));
        filler.platform_id = Set(3_000_000_000 + i as i64);
        tx.send(filler).await.unwrap();
    }

    let mut renamed_alchemy = create_test_model("alchemy-run", "alchemy-async");
    renamed_alchemy.platform_id = Set(917_974_798);
    tx.send(renamed_alchemy).await.unwrap();
    drop(tx);

    let result = await_persist_task(handle).await;
    assert!(result.has_errors());
    assert_eq!(result.errors.len(), 2);

    let alchemy = repository::find_by_platform_id(&db, instance_id, 917_974_798)
        .await
        .unwrap()
        .unwrap();
    let alchemy_effect = repository::find_by_platform_id(&db, instance_id, 1_081_394_458)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alchemy.id, original_alchemy.id);
    assert_eq!(alchemy.name, "alchemy");
    assert_eq!(alchemy_effect.id, original_alchemy_effect.id);
    assert_eq!(alchemy_effect.name, "alchemy-effect");

    let repos = repository::find_all_by_instance(&db, instance_id)
        .await
        .unwrap();
    assert!(
        repos
            .iter()
            .all(|repo| !repo.owner.starts_with("__curator_reconcile__/"))
    );
}

#[tokio::test]
async fn test_aborted_persist_task_leaves_deferred_rename_rows_untouched() {
    let db = Arc::new(setup_test_db().await);
    let instance_id = test_instance_id();

    let mut original_alchemy = create_test_model("alchemy-run", "alchemy");
    original_alchemy.platform_id = Set(917_974_798);
    let mut original_alchemy_effect = create_test_model("alchemy-run", "alchemy-effect");
    original_alchemy_effect.platform_id = Set(1_081_394_458);
    repository::bulk_upsert(&db, vec![original_alchemy, original_alchemy_effect])
        .await
        .unwrap();

    let original_alchemy = repository::find_by_platform_id(&db, instance_id, 917_974_798)
        .await
        .unwrap()
        .unwrap();
    let original_alchemy_effect = repository::find_by_platform_id(&db, instance_id, 1_081_394_458)
        .await
        .unwrap()
        .unwrap();

    let (tx, rx) = mpsc::channel::<ActiveModel>(PERSIST_BATCH_SIZE);
    let (handle, saved_counter) = spawn_persist_task(Arc::clone(&db), rx, None, None);

    let mut renamed_alchemy_effect = create_test_model("alchemy-run", "alchemy");
    renamed_alchemy_effect.platform_id = Set(1_081_394_458);
    tx.send(renamed_alchemy_effect).await.unwrap();
    for i in 1..PERSIST_BATCH_SIZE {
        let mut filler = create_test_model("filler", &format!("abort-{i}"));
        filler.platform_id = Set(4_000_000_000 + i as i64);
        tx.send(filler).await.unwrap();
    }

    tokio::time::timeout(FAST_TIMEOUT, async {
        while saved_counter.load(Ordering::Relaxed) < PERSIST_BATCH_SIZE - 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ready rows should persist before cancellation");

    handle.abort();
    drop(tx);
    let result = await_persist_task(handle).await;
    assert_eq!(result.panic_info.as_deref(), Some("Task was cancelled"));

    let alchemy = repository::find_by_platform_id(&db, instance_id, 917_974_798)
        .await
        .unwrap()
        .unwrap();
    let alchemy_effect = repository::find_by_platform_id(&db, instance_id, 1_081_394_458)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alchemy.id, original_alchemy.id);
    assert_eq!(alchemy.name, "alchemy");
    assert_eq!(alchemy_effect.id, original_alchemy_effect.id);
    assert_eq!(alchemy_effect.name, "alchemy-effect");

    let repos = repository::find_all_by_instance(&db, instance_id)
        .await
        .unwrap();
    assert!(
        repos
            .iter()
            .all(|repo| !repo.owner.starts_with("__curator_reconcile__/"))
    );
}

#[tokio::test]
async fn test_persist_task_preserves_rows_when_rename_input_is_incomplete() {
    let db = Arc::new(setup_test_db().await);
    let instance_id = test_instance_id();

    let mut displaced = create_test_model("alchemy-run", "alchemy");
    displaced.platform_id = Set(917_974_798);
    let mut successor = create_test_model("alchemy-run", "alchemy-effect");
    successor.platform_id = Set(1_081_394_458);
    repository::bulk_upsert(&db, vec![displaced, successor])
        .await
        .unwrap();

    let original_displaced = repository::find_by_platform_id(&db, instance_id, 917_974_798)
        .await
        .unwrap()
        .unwrap();
    let original_successor = repository::find_by_platform_id(&db, instance_id, 1_081_394_458)
        .await
        .unwrap()
        .unwrap();

    let (tx, rx) = mpsc::channel::<ActiveModel>(1);
    let (handle, _counter) = spawn_persist_task(Arc::clone(&db), rx, None, None);

    let mut renamed_successor = create_test_model("alchemy-run", "alchemy");
    renamed_successor.platform_id = Set(1_081_394_458);
    tx.send(renamed_successor).await.unwrap();
    drop(tx);

    let result = await_persist_task(handle).await;
    assert!(result.has_errors());
    assert_eq!(result.errors.len(), 1);
    assert!(result.errors[0].2.contains("incomplete repository rename"));

    let repos = repository::find_all_by_instance_and_owner(&db, instance_id, "alchemy-run")
        .await
        .unwrap();
    assert_eq!(repos.len(), 2);
    let displaced = repository::find_by_platform_id(&db, instance_id, 917_974_798)
        .await
        .unwrap()
        .unwrap();
    let successor = repository::find_by_platform_id(&db, instance_id, 1_081_394_458)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(displaced.id, original_displaced.id);
    assert_eq!(displaced.name, "alchemy");
    assert_eq!(successor.id, original_successor.id);
    assert_eq!(successor.name, "alchemy-effect");
    assert!(
        repos
            .iter()
            .all(|repo| !repo.owner.starts_with("__curator_reconcile__/"))
    );
}

#[tokio::test]
async fn test_known_incomplete_final_component_skips_reconciliation_backoff() {
    let db = Arc::new(setup_test_db().await);

    let mut displaced = create_test_model("alchemy-run", "alchemy");
    displaced.platform_id = Set(917_974_798);
    let mut successor = create_test_model("alchemy-run", "alchemy-effect");
    successor.platform_id = Set(1_081_394_458);
    repository::bulk_upsert(&db, vec![displaced, successor])
        .await
        .expect("original repositories should persist");

    let mut incomplete_rename = create_test_model("alchemy-run", "alchemy");
    incomplete_rename.platform_id = Set(1_081_394_458);

    let (tx, rx) = mpsc::channel::<ActiveModel>(1);
    let (handle, _counter) = spawn_persist_task(Arc::clone(&db), rx, None, None);
    tx.send(incomplete_rename)
        .await
        .expect("incomplete rename should send");
    drop(tx);

    let result = tokio::time::timeout(Duration::from_millis(500), await_persist_task(handle))
        .await
        .expect("known incomplete input should not wait through reconciliation backoff");
    assert_eq!(result.saved_count, 0);
    assert_eq!(result.errors.len(), 1);
    assert!(result.errors[0].2.contains("incomplete repository rename"));
}

#[tokio::test]
async fn test_final_flush_persists_valid_rows_beside_an_incomplete_rename() {
    let db = Arc::new(setup_test_db().await);
    let instance_id = test_instance_id();

    let mut displaced = create_test_model("alchemy-run", "alchemy");
    displaced.platform_id = Set(917_974_798);
    let mut successor = create_test_model("alchemy-run", "alchemy-effect");
    successor.platform_id = Set(1_081_394_458);
    repository::bulk_upsert(&db, vec![displaced, successor])
        .await
        .expect("original repositories should persist");

    let mut incomplete_rename = create_test_model("alchemy-run", "alchemy");
    incomplete_rename.platform_id = Set(1_081_394_458);
    let mut unrelated = create_test_model("unrelated", "valid");
    unrelated.platform_id = Set(6_000_000_000);

    let (tx, rx) = mpsc::channel::<ActiveModel>(2);
    let (handle, _counter) = spawn_persist_task(Arc::clone(&db), rx, None, None);
    tx.send(incomplete_rename)
        .await
        .expect("incomplete rename should send");
    tx.send(unrelated)
        .await
        .expect("unrelated repository should send");
    drop(tx);

    let result = await_persist_task(handle).await;
    assert_eq!(result.saved_count, 1);
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].0, "alchemy-run");
    assert_eq!(result.errors[0].1, "alchemy");
    assert!(result.errors[0].2.contains("incomplete repository rename"));

    let unrelated = repository::find_by_platform_id(&db, instance_id, 6_000_000_000)
        .await
        .expect("unrelated lookup should succeed")
        .expect("unrelated valid repository should persist");
    assert_eq!(unrelated.owner, "unrelated");
    assert_eq!(unrelated.name, "valid");
}

#[tokio::test]
async fn test_persist_task_bounds_writes_for_many_independent_rename_pairs() {
    const PAIR_COUNT: usize = PERSIST_BATCH_SIZE + 1;

    let db = Arc::new(setup_test_db().await);
    let mut original_models = Vec::with_capacity(PAIR_COUNT * 2);
    let mut renamed_successor_models = Vec::with_capacity(PAIR_COUNT);
    let mut renamed_displaced_models = Vec::with_capacity(PAIR_COUNT);

    for index in 0..PAIR_COUNT {
        let displaced_platform_id = 5_000_000_000 + (index as i64 * 2);
        let successor_platform_id = displaced_platform_id + 1;

        let mut displaced = create_test_model("bounded", &format!("old-{index}"));
        displaced.platform_id = Set(displaced_platform_id);
        original_models.push(displaced);

        let mut successor = create_test_model("bounded", &format!("successor-{index}"));
        successor.platform_id = Set(successor_platform_id);
        original_models.push(successor);

        let mut renamed_successor = create_test_model("bounded", &format!("old-{index}"));
        renamed_successor.platform_id = Set(successor_platform_id);
        renamed_successor_models.push(renamed_successor);

        let mut renamed_displaced = create_test_model("bounded", &format!("renamed-{index}"));
        renamed_displaced.platform_id = Set(displaced_platform_id);
        renamed_displaced_models.push(renamed_displaced);
    }

    for seed_batch in original_models.chunks(100) {
        repository::bulk_upsert(&db, seed_batch.to_vec())
            .await
            .expect("seed batch should persist");
    }

    let persisted_batch_sizes = Arc::new(Mutex::new(Vec::new()));
    let callback_batch_sizes = Arc::clone(&persisted_batch_sizes);
    let callback: Arc<curator::sync::ProgressCallback> =
        Arc::new(Box::new(move |event: SyncProgress| {
            if let SyncProgress::PersistingBatch { count, .. } = event {
                callback_batch_sizes
                    .lock()
                    .expect("batch sizes mutex should lock")
                    .push(count);
            }
        }));

    let (tx, rx) = mpsc::channel::<ActiveModel>(PERSIST_BATCH_SIZE);
    let (handle, _counter) = spawn_persist_task(Arc::clone(&db), rx, None, Some(callback));

    // Successors arrive first, so they are deferred until their displaced
    // repositories arrive in later persistence batches.
    for model in renamed_successor_models {
        tx.send(model).await.expect("successor should send");
    }
    for model in renamed_displaced_models {
        tx.send(model).await.expect("displaced repo should send");
    }
    drop(tx);

    let result = tokio::time::timeout(SYNC_TIMEOUT, await_persist_task(handle))
        .await
        .expect("persist task should not exceed the timeout");
    assert!(!result.has_errors(), "{:?}", result.errors);
    assert_eq!(result.saved_count, PAIR_COUNT * 2);

    let persisted_batch_sizes = persisted_batch_sizes
        .lock()
        .expect("batch sizes mutex should lock");
    assert!(
        persisted_batch_sizes
            .iter()
            .all(|count| *count <= PERSIST_BATCH_SIZE),
        "every database write should honor the persistence batch limit: {persisted_batch_sizes:?}"
    );
}

// ─── Stress Tests ──────────────────────────────────────────────────────────────
// These tests verify behavior under load to catch deadlocks and spin loops.

/// Test that persist task handles large batches without hanging.
/// This stress tests the chunks_timeout logic.
#[tokio::test]
async fn test_persist_task_handles_large_batch() {
    let db = Arc::new(setup_test_db().await);
    let (tx, rx) = mpsc::channel::<ActiveModel>(1000);

    let (handle, _counter) = spawn_persist_task(db, rx, None, None);

    // Send many models (more than one batch)
    let count = 250;
    for i in 0..count {
        tx.send(create_test_model("stress-test", &format!("repo-{}", i)))
            .await
            .unwrap();
    }

    drop(tx);

    let result = tokio::time::timeout(SYNC_TIMEOUT, await_persist_task(handle)).await;

    assert!(
        result.is_ok(),
        "Persist task should handle {} items without hanging",
        count
    );

    let persist_result = result.unwrap();
    assert_eq!(persist_result.saved_count, count);
}

/// Test that concurrent sends don't cause deadlock.
#[tokio::test]
async fn test_persist_task_concurrent_sends() {
    let db = Arc::new(setup_test_db().await);
    let (tx, rx) = mpsc::channel::<ActiveModel>(50); // Smaller buffer to increase contention

    let (handle, _counter) = spawn_persist_task(db, rx, None, None);

    // Spawn multiple senders concurrently
    let mut send_handles = Vec::new();
    for batch in 0..5 {
        let tx_clone = tx.clone();
        let handle = tokio::spawn(async move {
            for i in 0..20 {
                let _ = tx_clone
                    .send(create_test_model(
                        &format!("batch-{}", batch),
                        &format!("repo-{}", i),
                    ))
                    .await;
            }
        });
        send_handles.push(handle);
    }

    // Wait for all senders
    for h in send_handles {
        h.await.unwrap();
    }

    // Drop original sender
    drop(tx);

    let result = tokio::time::timeout(SYNC_TIMEOUT, await_persist_task(handle)).await;

    assert!(
        result.is_ok(),
        "Persist task should handle concurrent sends without deadlock"
    );

    let persist_result = result.unwrap();
    assert_eq!(persist_result.saved_count, 100); // 5 batches * 20 items
}

// ─── Progress Callback Tests ───────────────────────────────────────────────────
// These tests verify progress reporting doesn't cause issues.

/// Test that progress callbacks are invoked and don't cause issues.
#[tokio::test]
async fn test_persist_task_with_progress_callback() {
    let db = Arc::new(setup_test_db().await);
    let (tx, rx) = mpsc::channel::<ActiveModel>(100);

    let progress_count = Arc::new(AtomicUsize::new(0));
    let progress_count_clone = Arc::clone(&progress_count);

    let callback: Arc<curator::sync::ProgressCallback> =
        Arc::new(Box::new(move |event: SyncProgress| {
            if matches!(event, SyncProgress::Persisted { .. }) {
                progress_count_clone.fetch_add(1, Ordering::Relaxed);
            }
        }));

    let (handle, _counter) = spawn_persist_task(db, rx, None, Some(callback));

    // Send models
    for i in 0..10 {
        tx.send(create_test_model("callback-test", &format!("repo-{}", i)))
            .await
            .unwrap();
    }

    drop(tx);

    let result = tokio::time::timeout(FAST_TIMEOUT, await_persist_task(handle)).await;

    assert!(result.is_ok(), "Persist task with callback should complete");
    assert_eq!(
        progress_count.load(Ordering::Relaxed),
        10,
        "Should receive progress for each persisted item"
    );
}

// ─── Helper Function Tests ─────────────────────────────────────────────────────

/// Test the model channel helper function.
#[test]
fn test_create_model_channel() {
    let (tx, _rx) = create_model_channel();
    assert!(!tx.is_closed());
}

/// Test PersistTaskResult error tracking.
#[test]
fn test_persist_task_result_error_tracking() {
    let mut result = PersistTaskResult::default();
    assert!(!result.has_errors());
    assert_eq!(result.failed_count(), 0);

    result
        .errors
        .push(("owner".to_string(), "repo".to_string(), "error".to_string()));
    assert!(result.has_errors());
    assert_eq!(result.failed_count(), 1);

    result.panic_info = Some("panic".to_string());
    assert_eq!(result.failed_count(), 2);
}
