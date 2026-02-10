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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use curator::connect_and_migrate;
use curator::entity::code_repository::ActiveModel;
use curator::entity::code_visibility::CodeVisibility;
use curator::entity::instance::{ActiveModel as InstanceActiveModel, Entity as Instance};
use curator::entity::platform_type::PlatformType;
use curator::sync::{
    PersistTaskResult, SyncProgress, await_persist_task, create_model_channel, spawn_persist_task,
};
use sea_orm::{EntityTrait, Set};
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
