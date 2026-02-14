use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use uuid::Uuid;

use crate::entity::code_repository::{ActiveModel, Column, Entity as CodeRepository, Model};
use sea_orm::ActiveValue;

use super::errors::{RepositoryError, Result};

// ─── Single Record Operations ────────────────────────────────────────────────

/// Insert a new repository.
///
/// # Errors
/// Returns `RepositoryError::Database` if the insert fails (e.g., duplicate natural key).
pub async fn insert(db: &DatabaseConnection, model: ActiveModel) -> Result<Model> {
    model.insert(db).await.map_err(RepositoryError::from)
}

/// Find a repository by its UUID.
pub async fn find_by_id(db: &DatabaseConnection, id: Uuid) -> Result<Option<Model>> {
    CodeRepository::find_by_id(id)
        .one(db)
        .await
        .map_err(RepositoryError::from)
}

/// Find a repository by its natural key (instance_id + owner + name).
pub async fn find_by_natural_key(
    db: &DatabaseConnection,
    instance_id: Uuid,
    owner: &str,
    name: &str,
) -> Result<Option<Model>> {
    CodeRepository::find()
        .filter(Column::InstanceId.eq(instance_id))
        .filter(Column::Owner.eq(owner))
        .filter(Column::Name.eq(name))
        .one(db)
        .await
        .map_err(RepositoryError::from)
}

/// Find a repository by instance and platform_id (numeric ID from the platform).
pub async fn find_by_platform_id(
    db: &DatabaseConnection,
    instance_id: Uuid,
    platform_id: i64,
) -> Result<Option<Model>> {
    CodeRepository::find()
        .filter(Column::InstanceId.eq(instance_id))
        .filter(Column::PlatformId.eq(platform_id))
        .one(db)
        .await
        .map_err(RepositoryError::from)
}

/// Update an existing repository.
///
/// # Errors
/// Returns `RepositoryError::Database` if the update fails.
pub async fn update(db: &DatabaseConnection, model: ActiveModel) -> Result<Model> {
    model.update(db).await.map_err(RepositoryError::from)
}

/// Insert or update a repository by its natural key (instance_id + owner + name).
///
/// If a repository with the same instance_id, owner, and name exists, it will be updated.
/// Otherwise, a new repository will be inserted.
pub async fn upsert(db: &DatabaseConnection, model: ActiveModel) -> Result<Model> {
    // Extract natural key from the active model
    let instance_id = required_active_value("instance_id", &model.instance_id)?;
    let owner = required_active_value("owner", &model.owner)?;
    let name = required_active_value("name", &model.name)?;

    // Check if exists
    let existing = find_by_natural_key(db, instance_id, &owner, &name).await?;

    match existing {
        Some(existing) => {
            // Update: set the ID from existing record
            let mut update_model = model;
            update_model.id = Set(existing.id);
            update_model.update(db).await.map_err(RepositoryError::from)
        }
        None => {
            // Insert: ensure ID is set
            let mut insert_model = model;
            if insert_model.id.is_not_set() {
                insert_model.id = Set(Uuid::new_v4());
            }
            insert_model.insert(db).await.map_err(RepositoryError::from)
        }
    }
}

fn required_active_value<T: Clone + Into<sea_orm::Value>>(
    field: &str,
    value: &ActiveValue<T>,
) -> Result<T> {
    match value {
        ActiveValue::Set(value) | ActiveValue::Unchanged(value) => Ok(value.clone()),
        ActiveValue::NotSet => Err(RepositoryError::InvalidInput {
            message: format!("Missing required field: {}", field),
        }),
    }
}

/// Delete a repository by its UUID.
///
/// Returns the number of rows deleted (0 or 1).
pub async fn delete(db: &DatabaseConnection, id: Uuid) -> Result<u64> {
    let result = CodeRepository::delete_by_id(id).exec(db).await?;
    Ok(result.rows_affected)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult, Set};

    use crate::entity::code_visibility::CodeVisibility;

    use super::*;

    fn model(
        id: Uuid,
        instance_id: Uuid,
        owner: &str,
        name: &str,
        platform_id: i64,
        description: Option<&str>,
    ) -> Model {
        Model {
            id,
            instance_id,
            platform_id,
            owner: owner.to_string(),
            name: name.to_string(),
            description: description.map(ToString::to_string),
            default_branch: "main".to_string(),
            topics: serde_json::json!([]),
            primary_language: None,
            license_spdx: None,
            homepage: None,
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_mirror: false,
            is_archived: false,
            is_template: false,
            is_empty: false,
            stars: None,
            forks: None,
            open_issues: None,
            watchers: None,
            size_kb: None,
            has_issues: true,
            has_wiki: true,
            has_pull_requests: true,
            created_at: None,
            updated_at: None,
            pushed_at: None,
            platform_metadata: serde_json::json!({}),
            synced_at: Utc::now().fixed_offset(),
            etag: None,
        }
    }

    fn active_model(
        instance_id: Uuid,
        owner: &str,
        name: &str,
        platform_id: i64,
        description: Option<&str>,
    ) -> ActiveModel {
        let now = Utc::now().fixed_offset();
        ActiveModel {
            id: Set(Uuid::new_v4()),
            instance_id: Set(instance_id),
            platform_id: Set(platform_id),
            owner: Set(owner.to_string()),
            name: Set(name.to_string()),
            description: Set(description.map(ToString::to_string)),
            default_branch: Set("main".to_string()),
            topics: Set(serde_json::json!([])),
            primary_language: Set(None),
            license_spdx: Set(None),
            homepage: Set(None),
            visibility: Set(CodeVisibility::Public),
            is_fork: Set(false),
            is_mirror: Set(false),
            is_archived: Set(false),
            is_template: Set(false),
            is_empty: Set(false),
            stars: Set(None),
            forks: Set(None),
            open_issues: Set(None),
            watchers: Set(None),
            size_kb: Set(None),
            has_issues: Set(true),
            has_wiki: Set(true),
            has_pull_requests: Set(true),
            created_at: Set(Some(now)),
            updated_at: Set(Some(now)),
            pushed_at: Set(Some(now)),
            platform_metadata: Set(serde_json::json!({})),
            synced_at: Set(now),
            etag: Set(None),
        }
    }

    #[test]
    fn required_active_value_accepts_set_and_unchanged_and_rejects_not_set() {
        let set = sea_orm::ActiveValue::Set("abc".to_string());
        assert_eq!(required_active_value("field", &set).unwrap(), "abc");

        let unchanged = sea_orm::ActiveValue::Unchanged("def".to_string());
        assert_eq!(required_active_value("field", &unchanged).unwrap(), "def");

        let not_set: sea_orm::ActiveValue<String> = sea_orm::ActiveValue::NotSet;
        let err = required_active_value("missing", &not_set).expect_err("should fail");
        match err {
            RepositoryError::InvalidInput { message } => {
                assert!(message.contains("missing"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn insert_returns_inserted_model() {
        let instance_id = Uuid::new_v4();
        let expected = model(
            Uuid::new_v4(),
            instance_id,
            "org",
            "repo",
            123,
            Some("desc"),
        );

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_exec_results([MockExecResult {
                rows_affected: 1,
                last_insert_id: 0,
            }])
            .append_query_results([vec![expected.clone()]])
            .into_connection();

        let saved = insert(
            &db,
            active_model(instance_id, "org", "repo", 123, Some("desc")),
        )
        .await
        .expect("insert should succeed");
        assert_eq!(saved, expected);
    }

    #[tokio::test]
    async fn update_returns_updated_model() {
        let instance_id = Uuid::new_v4();
        let expected = model(
            Uuid::new_v4(),
            instance_id,
            "org",
            "repo",
            123,
            Some("updated"),
        );
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_exec_results([MockExecResult {
                rows_affected: 1,
                last_insert_id: 0,
            }])
            .append_query_results([vec![expected.clone()]])
            .into_connection();

        let saved = update(
            &db,
            active_model(instance_id, "org", "repo", 123, Some("updated")),
        )
        .await
        .expect("update should succeed");
        assert_eq!(saved, expected);
    }

    #[tokio::test]
    async fn find_by_id_returns_some_when_row_exists() {
        let instance_id = Uuid::new_v4();
        let expected = model(Uuid::new_v4(), instance_id, "org", "repo", 1, None);
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([vec![expected.clone()]])
            .into_connection();

        let found = find_by_id(&db, expected.id)
            .await
            .expect("lookup should succeed")
            .expect("expected row");
        assert_eq!(found, expected);
    }

    #[tokio::test]
    async fn find_by_id_returns_none_when_missing() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([Vec::<Model>::new()])
            .into_connection();

        let found = find_by_id(&db, Uuid::new_v4())
            .await
            .expect("lookup should succeed");
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn upsert_inserts_when_missing_and_id_is_unset() {
        let instance_id = Uuid::new_v4();
        let inserted = model(Uuid::new_v4(), instance_id, "org", "repo", 7, Some("first"));

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            // find_by_natural_key -> None
            .append_query_results([Vec::<Model>::new()])
            // insert -> exec
            .append_exec_results([MockExecResult {
                rows_affected: 1,
                last_insert_id: 0,
            }])
            // insert -> returning model
            .append_query_results([vec![inserted.clone()]])
            .into_connection();

        let mut item = active_model(instance_id, "org", "repo", 7, Some("first"));
        item.id = sea_orm::ActiveValue::NotSet;

        let saved = upsert(&db, item).await.expect("upsert should insert");
        assert_eq!(saved, inserted);
    }

    #[tokio::test]
    async fn upsert_updates_when_existing_record_is_present() {
        let instance_id = Uuid::new_v4();
        let existing_id = Uuid::new_v4();
        let existing = model(existing_id, instance_id, "org", "repo", 7, Some("old"));
        let updated = model(existing_id, instance_id, "org", "repo", 9, Some("new"));

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            // find_by_natural_key -> Some(existing)
            .append_query_results([vec![existing.clone()]])
            // update -> exec
            .append_exec_results([MockExecResult {
                rows_affected: 1,
                last_insert_id: 0,
            }])
            // update -> returning model
            .append_query_results([vec![updated.clone()]])
            .into_connection();

        let mut item = active_model(instance_id, "org", "repo", 9, Some("new"));
        item.id = sea_orm::ActiveValue::NotSet;
        let saved = upsert(&db, item).await.expect("upsert should update");

        assert_eq!(saved, updated);
        assert_eq!(saved.id, existing_id);
    }

    #[tokio::test]
    async fn find_by_platform_id_returns_expected_row() {
        let instance_id = Uuid::new_v4();
        let expected = model(Uuid::new_v4(), instance_id, "org", "repo", 42, None);
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([vec![expected.clone()]])
            .into_connection();

        let found = find_by_platform_id(&db, instance_id, 42)
            .await
            .expect("lookup should succeed")
            .expect("expected row");
        assert_eq!(found, expected);
    }

    #[tokio::test]
    async fn delete_returns_rows_affected() {
        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_exec_results([MockExecResult {
                rows_affected: 1,
                last_insert_id: 0,
            }])
            .into_connection();

        let deleted = delete(&db, Uuid::new_v4()).await.expect("delete succeeds");
        assert_eq!(deleted, 1);
    }
}

#[cfg(all(test, feature = "sqlite", feature = "migrate"))]
mod integration_tests {
    use chrono::Utc;
    use sea_orm::{EntityTrait, Set};

    use crate::connect_and_migrate;
    use crate::entity::code_visibility::CodeVisibility;
    use crate::entity::instance::{ActiveModel as InstanceActiveModel, Entity as Instance};
    use crate::entity::platform_type::PlatformType;

    use super::*;

    fn test_instance_id() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000111").expect("valid uuid")
    }

    async fn setup_db() -> DatabaseConnection {
        let db = connect_and_migrate("sqlite::memory:")
            .await
            .expect("test db should migrate");

        let now = Utc::now().fixed_offset();
        let instance = InstanceActiveModel {
            id: Set(test_instance_id()),
            name: Set("single-test".to_string()),
            platform_type: Set(PlatformType::GitHub),
            host: Set("single-test.example.com".to_string()),
            oauth_client_id: Set(None),
            oauth_flow: Set("auto".to_string()),
            created_at: Set(now),
        };
        Instance::insert(instance)
            .exec(&db)
            .await
            .expect("instance should insert");

        db
    }

    fn model(owner: &str, name: &str, description: Option<&str>) -> ActiveModel {
        let now = Utc::now().fixed_offset();
        ActiveModel {
            id: Set(Uuid::new_v4()),
            instance_id: Set(test_instance_id()),
            platform_id: Set(1001),
            owner: Set(owner.to_string()),
            name: Set(name.to_string()),
            description: Set(description.map(|s| s.to_string())),
            default_branch: Set("main".to_string()),
            topics: Set(serde_json::json!([])),
            primary_language: Set(None),
            license_spdx: Set(None),
            homepage: Set(None),
            visibility: Set(CodeVisibility::Public),
            is_fork: Set(false),
            is_mirror: Set(false),
            is_archived: Set(false),
            is_template: Set(false),
            is_empty: Set(false),
            stars: Set(None),
            forks: Set(None),
            open_issues: Set(None),
            watchers: Set(None),
            size_kb: Set(None),
            has_issues: Set(true),
            has_wiki: Set(true),
            has_pull_requests: Set(true),
            created_at: Set(Some(now)),
            updated_at: Set(Some(now)),
            pushed_at: Set(Some(now)),
            platform_metadata: Set(serde_json::json!({})),
            synced_at: Set(now),
            etag: Set(None),
        }
    }

    #[tokio::test]
    async fn test_upsert_rejects_missing_required_field() {
        let db = setup_db().await;
        let mut item = model("owner", "repo", None);
        item.owner = ActiveValue::NotSet;

        let err = upsert(&db, item).await.expect_err("upsert should fail");
        match err {
            RepositoryError::InvalidInput { message } => {
                assert!(message.contains("owner"));
            }
            other => panic!("expected invalid input error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_upsert_inserts_when_missing_even_with_unset_id() {
        let db = setup_db().await;
        let mut item = model("owner", "repo", Some("first"));
        item.id = ActiveValue::NotSet;

        let saved = upsert(&db, item).await.expect("upsert should insert");

        assert_eq!(saved.owner, "owner");
        assert_eq!(saved.name, "repo");
        assert_ne!(saved.id, Uuid::nil());
    }

    #[tokio::test]
    async fn test_upsert_updates_existing_record_in_place() {
        let db = setup_db().await;
        let first = upsert(&db, model("owner", "repo", Some("first")))
            .await
            .expect("first upsert should insert");

        let mut second = model("owner", "repo", Some("updated"));
        second.id = ActiveValue::NotSet;
        second.platform_id = Set(2002);
        let updated = upsert(&db, second)
            .await
            .expect("second upsert should update");

        assert_eq!(updated.id, first.id);
        assert_eq!(updated.description.as_deref(), Some("updated"));
        assert_eq!(updated.platform_id, 2002);

        let found = find_by_natural_key(&db, test_instance_id(), "owner", "repo")
            .await
            .expect("lookup should succeed")
            .expect("repo should exist");
        assert_eq!(found.id, first.id);
        assert_eq!(found.description.as_deref(), Some("updated"));
    }
}
