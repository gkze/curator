//! Integration tests for repository operations.
//!
//! These tests require the `sqlite` and `migrate` features to be enabled
//! and use an in-memory SQLite database.

#![cfg(all(feature = "sqlite", feature = "migrate"))]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use chrono::Utc;
use curator::connect_and_migrate;
use curator::entity::code_repository::ActiveModel;
use curator::entity::code_visibility::CodeVisibility;
use curator::entity::instance::{ActiveModel as InstanceActiveModel, Entity as Instance};
use curator::entity::platform_type::PlatformType;
use curator::repository::{self, find_all_by_instance_and_owner};
use sea_orm::{DatabaseConnection, EntityTrait, Set};
use uuid::Uuid;

/// A test instance ID to use consistently across tests
fn test_instance_id() -> Uuid {
    // Using a fixed UUID for consistent test behavior
    Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
}

/// A second test instance ID for multi-instance tests
fn test_instance_id_2() -> Uuid {
    Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()
}

/// Generate a deterministic platform_id from owner/name.
/// This ensures the same repo always gets the same platform_id,
/// which is required since bulk_upsert uses (instance_id, platform_id) as conflict key.
fn platform_id_from_name(owner: &str, name: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    owner.hash(&mut hasher);
    name.hash(&mut hasher);
    hasher.finish() as i64
}

/// Create an in-memory SQLite database with migrations applied.
async fn setup_test_db() -> DatabaseConnection {
    connect_and_migrate("sqlite::memory:")
        .await
        .expect("Failed to create test database")
}

/// Create test instances in the database.
/// This is required because code_repository has a foreign key to instances.
///
/// Note: Uses unique test hosts (not github.com/gitlab.com) to avoid conflicts
/// with the well-known instances seeded by migrations.
async fn create_test_instances(db: &DatabaseConnection) {
    let now = Utc::now();

    let instance_1 = InstanceActiveModel {
        id: Set(test_instance_id()),
        name: Set("test-github".to_string()),
        platform_type: Set(PlatformType::GitHub),
        host: Set("test-github.example.com".to_string()),
        oauth_client_id: Set(None),
        oauth_flow: Set("auto".to_string()),
        created_at: Set(now.fixed_offset()),
    };

    let instance_2 = InstanceActiveModel {
        id: Set(test_instance_id_2()),
        name: Set("test-gitlab".to_string()),
        platform_type: Set(PlatformType::GitLab),
        host: Set("test-gitlab.example.com".to_string()),
        oauth_client_id: Set(None),
        oauth_flow: Set("auto".to_string()),
        created_at: Set(now.fixed_offset()),
    };

    Instance::insert_many([instance_1, instance_2])
        .exec(db)
        .await
        .expect("Failed to create test instances");
}

/// Create a test ActiveModel with the given owner and name.
fn create_test_model(
    instance_id: Uuid,
    owner: &str,
    name: &str,
    updated_at: chrono::DateTime<Utc>,
) -> ActiveModel {
    ActiveModel {
        id: Set(Uuid::new_v4()),
        instance_id: Set(instance_id),
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
        created_at: Set(Some(updated_at.fixed_offset())),
        updated_at: Set(Some(updated_at.fixed_offset())),
        pushed_at: Set(Some(updated_at.fixed_offset())),
        platform_metadata: Set(serde_json::json!({})),
        synced_at: Set(Utc::now().fixed_offset()),
        etag: Set(None),
    }
}

// ─── find_all_by_instance_and_owner Tests ────────────────────────────────────

#[tokio::test]
async fn test_find_all_by_instance_and_owner_empty() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let instance_id = test_instance_id();

    let result = find_all_by_instance_and_owner(&db, instance_id, "nonexistent")
        .await
        .unwrap();

    assert!(result.is_empty());
}

#[tokio::test]
async fn test_find_all_by_instance_and_owner_returns_matching() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let instance_id = test_instance_id();

    // Insert repos for two different owners
    let models = vec![
        create_test_model(instance_id, "org-a", "repo-1", now),
        create_test_model(instance_id, "org-a", "repo-2", now),
        create_test_model(instance_id, "org-b", "repo-1", now),
    ];

    repository::bulk_upsert(&db, models).await.unwrap();

    // Query for org-a
    let result = find_all_by_instance_and_owner(&db, instance_id, "org-a")
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    assert!(result.iter().all(|r| r.owner == "org-a"));

    // Query for org-b
    let result = find_all_by_instance_and_owner(&db, instance_id, "org-b")
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].owner, "org-b");
}

#[tokio::test]
async fn test_find_all_by_instance_and_owner_filters_by_instance() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let instance_1 = test_instance_id();
    let instance_2 = test_instance_id_2();

    // Insert repo on instance 1
    let model_1 = create_test_model(instance_1, "my-org", "repo-1", now);
    repository::bulk_upsert(&db, vec![model_1]).await.unwrap();

    // Insert repo on instance 2 with same owner
    let model_2 = create_test_model(instance_2, "my-org", "repo-2", now);
    repository::bulk_upsert(&db, vec![model_2]).await.unwrap();

    // Query instance 1 repos only
    let result = find_all_by_instance_and_owner(&db, instance_1, "my-org")
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].name, "repo-1");
    assert_eq!(result[0].instance_id, instance_1);
}

#[tokio::test]
async fn test_find_all_by_instance_and_owner_sorted_by_name() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let instance_id = test_instance_id();

    // Insert repos in non-alphabetical order
    let models = vec![
        create_test_model(instance_id, "my-org", "zebra", now),
        create_test_model(instance_id, "my-org", "alpha", now),
        create_test_model(instance_id, "my-org", "middle", now),
    ];

    repository::bulk_upsert(&db, models).await.unwrap();

    let result = find_all_by_instance_and_owner(&db, instance_id, "my-org")
        .await
        .unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].name, "alpha");
    assert_eq!(result[1].name, "middle");
    assert_eq!(result[2].name, "zebra");
}

// ─── bulk_upsert Conditional Update Tests ────────────────────────────────────

#[tokio::test]
async fn test_bulk_upsert_inserts_new_repos() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let instance_id = test_instance_id();

    let models = vec![
        create_test_model(instance_id, "test-org", "repo-1", now),
        create_test_model(instance_id, "test-org", "repo-2", now),
    ];

    let rows_affected = repository::bulk_upsert(&db, models).await.unwrap();

    // Both repos are new, so both should be inserted
    assert_eq!(rows_affected, 2);

    // Verify they exist
    let result = find_all_by_instance_and_owner(&db, instance_id, "test-org")
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
}

#[tokio::test]
async fn test_bulk_upsert_skips_unchanged_repos() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let instance_id = test_instance_id();

    // First insert
    let models = vec![create_test_model(instance_id, "test-org", "repo-1", now)];
    let first_insert = repository::bulk_upsert(&db, models).await.unwrap();
    assert_eq!(first_insert, 1);

    // Second insert with SAME updated_at - should skip update
    let models = vec![create_test_model(instance_id, "test-org", "repo-1", now)];
    let second_insert = repository::bulk_upsert(&db, models).await.unwrap();

    // The row already exists with same updated_at, so no update should happen
    assert_eq!(
        second_insert, 0,
        "Should skip update when updated_at unchanged"
    );
}

#[tokio::test]
async fn test_bulk_upsert_updates_changed_repos() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let later = now + chrono::Duration::hours(1);
    let instance_id = test_instance_id();

    // First insert
    let models = vec![create_test_model(instance_id, "test-org", "repo-1", now)];
    repository::bulk_upsert(&db, models).await.unwrap();

    // Second insert with DIFFERENT updated_at - should update
    let models = vec![create_test_model(instance_id, "test-org", "repo-1", later)];
    let rows_affected = repository::bulk_upsert(&db, models).await.unwrap();

    assert_eq!(rows_affected, 1, "Should update when updated_at changed");

    // Verify the update was applied
    let result = find_all_by_instance_and_owner(&db, instance_id, "test-org")
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].updated_at.unwrap().timestamp(), later.timestamp());
}

#[tokio::test]
async fn test_bulk_upsert_reconciles_repository_rename_chain() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let later = now + chrono::Duration::hours(1);
    let instance_id = test_instance_id();

    // GitHub repository 917974798 was originally named "alchemy", while
    // 1081394458 was named "alchemy-effect".
    let mut original_alchemy = create_test_model(instance_id, "alchemy-run", "alchemy", now);
    original_alchemy.platform_id = Set(917_974_798);
    let mut original_alchemy_effect =
        create_test_model(instance_id, "alchemy-run", "alchemy-effect", now);
    original_alchemy_effect.platform_id = Set(1_081_394_458);
    repository::bulk_upsert(&db, vec![original_alchemy, original_alchemy_effect])
        .await
        .unwrap();

    let original_alchemy_id = repository::find_by_platform_id(&db, instance_id, 917_974_798)
        .await
        .unwrap()
        .unwrap()
        .id;
    let original_alchemy_effect_id =
        repository::find_by_platform_id(&db, instance_id, 1_081_394_458)
            .await
            .unwrap()
            .unwrap()
            .id;

    // The first repository was renamed to "alchemy-async" and the second took
    // over the now-available "alchemy" name.
    let mut renamed_alchemy = create_test_model(instance_id, "alchemy-run", "alchemy-async", later);
    renamed_alchemy.platform_id = Set(917_974_798);
    let mut renamed_alchemy_effect =
        create_test_model(instance_id, "alchemy-run", "alchemy", later);
    renamed_alchemy_effect.platform_id = Set(1_081_394_458);

    let rows_affected = repository::bulk_upsert(&db, vec![renamed_alchemy_effect, renamed_alchemy])
        .await
        .expect("rename chain should not violate the owner/name constraint");

    assert_eq!(rows_affected, 2);

    let repos = find_all_by_instance_and_owner(&db, instance_id, "alchemy-run")
        .await
        .unwrap();
    assert_eq!(repos.len(), 2);
    assert!(
        repos
            .iter()
            .any(|repo| repo.name == "alchemy" && repo.platform_id == 1_081_394_458)
    );
    assert!(
        repos
            .iter()
            .any(|repo| repo.name == "alchemy-async" && repo.platform_id == 917_974_798)
    );

    let renamed_alchemy = repository::find_by_platform_id(&db, instance_id, 917_974_798)
        .await
        .unwrap()
        .unwrap();
    let renamed_alchemy_effect = repository::find_by_platform_id(&db, instance_id, 1_081_394_458)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renamed_alchemy.id, original_alchemy_id);
    assert_eq!(renamed_alchemy_effect.id, original_alchemy_effect_id);
}

#[tokio::test]
async fn test_bulk_upsert_rejects_incomplete_rename_without_deleting_existing_rows() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let later = now + chrono::Duration::hours(1);
    let instance_id = test_instance_id();

    let mut displaced = create_test_model(instance_id, "alchemy-run", "alchemy", now);
    displaced.platform_id = Set(917_974_798);
    let mut successor = create_test_model(instance_id, "alchemy-run", "alchemy-effect", now);
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

    let mut renamed_successor = create_test_model(instance_id, "alchemy-run", "alchemy", later);
    renamed_successor.platform_id = Set(1_081_394_458);
    let error = repository::bulk_upsert(&db, vec![renamed_successor])
        .await
        .expect_err("an incomplete rename must not delete the displaced repository");
    assert!(error.to_string().contains("incomplete repository rename"));

    let repos = find_all_by_instance_and_owner(&db, instance_id, "alchemy-run")
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
async fn test_bulk_upsert_persists_rename_when_updated_at_is_unchanged() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let instance_id = test_instance_id();

    let mut original = create_test_model(instance_id, "test-org", "old-name", now);
    original.platform_id = Set(42);
    repository::bulk_upsert(&db, vec![original]).await.unwrap();

    let mut renamed = create_test_model(instance_id, "test-org", "new-name", now);
    renamed.platform_id = Set(42);
    let rows_affected = repository::bulk_upsert(&db, vec![renamed])
        .await
        .expect("rename should be persisted even when updated_at is unchanged");

    assert_eq!(rows_affected, 1);
    let repo = repository::find_by_platform_id(&db, instance_id, 42)
        .await
        .unwrap()
        .expect("renamed repository should exist");
    assert_eq!(repo.name, "new-name");
}

#[tokio::test]
async fn test_bulk_upsert_deduplicates_redirected_aliases_by_platform_id() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let instance_id = test_instance_id();

    let alias_id = Uuid::new_v4();
    let mut redirected_alias = create_test_model(instance_id, "test-org", "old-name", now);
    redirected_alias.id = Set(alias_id);
    redirected_alias.platform_id = Set(42);

    let canonical_id = Uuid::new_v4();
    let mut canonical = create_test_model(instance_id, "test-org", "new-name", now);
    canonical.id = Set(canonical_id);
    canonical.platform_id = Set(42);

    let rows_affected = repository::bulk_upsert(&db, vec![redirected_alias, canonical])
        .await
        .expect("redirected aliases should be collapsed before the database write");

    assert_eq!(rows_affected, 1);
    let repo = repository::find_by_platform_id(&db, instance_id, 42)
        .await
        .unwrap()
        .expect("canonical repository should exist");
    assert_eq!(repo.id, canonical_id);
    assert_ne!(repo.id, alias_id);
    assert_eq!(repo.name, "new-name");
}

#[tokio::test]
async fn test_bulk_upsert_mixed_new_and_unchanged() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let instance_id = test_instance_id();

    // First insert repo-1
    let models = vec![create_test_model(instance_id, "test-org", "repo-1", now)];
    repository::bulk_upsert(&db, models).await.unwrap();

    // Insert batch with: repo-1 (unchanged) + repo-2 (new)
    let models = vec![
        create_test_model(instance_id, "test-org", "repo-1", now), // unchanged
        create_test_model(instance_id, "test-org", "repo-2", now), // new
    ];
    let rows_affected = repository::bulk_upsert(&db, models).await.unwrap();

    // Only repo-2 should be inserted; repo-1 should be skipped
    assert_eq!(rows_affected, 1, "Should only count the new repo");

    // Verify both exist
    let result = find_all_by_instance_and_owner(&db, instance_id, "test-org")
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
}

#[tokio::test]
async fn test_bulk_upsert_updates_null_updated_at() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let instance_id = test_instance_id();

    // First insert with NULL updated_at
    let mut model = create_test_model(instance_id, "test-org", "repo-1", now);
    model.updated_at = Set(None);
    repository::bulk_upsert(&db, vec![model]).await.unwrap();

    // Second insert with actual updated_at - should update (NULL is always considered "changed")
    let model = create_test_model(instance_id, "test-org", "repo-1", now);
    let rows_affected = repository::bulk_upsert(&db, vec![model]).await.unwrap();

    assert_eq!(
        rows_affected, 1,
        "Should update when existing updated_at is NULL"
    );
}

#[tokio::test]
async fn test_bulk_upsert_empty_vec_returns_zero() {
    let db = setup_test_db().await;

    let rows_affected = repository::bulk_upsert(&db, vec![]).await.unwrap();

    assert_eq!(rows_affected, 0);
}

// ─── Edge Cases ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_bulk_upsert_large_batch() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let instance_id = test_instance_id();

    // Create a large batch of repos
    let models: Vec<ActiveModel> = (0..100)
        .map(|i| create_test_model(instance_id, "large-org", &format!("repo-{:03}", i), now))
        .collect();

    let rows_affected = repository::bulk_upsert(&db, models).await.unwrap();

    assert_eq!(rows_affected, 100);

    let result = find_all_by_instance_and_owner(&db, instance_id, "large-org")
        .await
        .unwrap();
    assert_eq!(result.len(), 100);
}

#[tokio::test]
async fn test_bulk_upsert_handles_special_characters_in_names() {
    let db = setup_test_db().await;
    create_test_instances(&db).await;
    let now = Utc::now();
    let instance_id = test_instance_id();

    let models = vec![
        create_test_model(instance_id, "my-org", "repo-with-dashes", now),
        create_test_model(instance_id, "my_org", "repo_with_underscores", now),
        create_test_model(instance_id, "MyOrg", "RepoWithCaps", now),
    ];

    let rows_affected = repository::bulk_upsert(&db, models).await.unwrap();
    assert_eq!(rows_affected, 3);
}
