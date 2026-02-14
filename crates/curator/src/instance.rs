//! Instance management service.
//!
//! Provides functions to find, create, and manage platform instances.

use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use uuid::Uuid;

use crate::entity::instance::{self, ActiveModel, Entity, Model, well_known};
use crate::entity::platform_type::PlatformType;

/// Error type for instance operations.
#[derive(Debug, thiserror::Error)]
pub enum InstanceError {
    #[error("Database error: {0}")]
    Database(#[from] sea_orm::DbErr),

    #[error("Instance not found: {0}")]
    NotFound(String),
}

/// Find an instance by its unique name.
pub async fn find_by_name(
    db: &DatabaseConnection,
    name: &str,
) -> Result<Option<Model>, InstanceError> {
    Entity::find()
        .filter(instance::Column::Name.eq(name))
        .one(db)
        .await
        .map_err(InstanceError::Database)
}

/// Find an instance by platform type and host.
pub async fn find_by_host(
    db: &DatabaseConnection,
    platform_type: PlatformType,
    host: &str,
) -> Result<Option<Model>, InstanceError> {
    Entity::find()
        .filter(instance::Column::PlatformType.eq(platform_type))
        .filter(instance::Column::Host.eq(host))
        .one(db)
        .await
        .map_err(InstanceError::Database)
}

/// Find an instance by ID.
pub async fn find_by_id(db: &DatabaseConnection, id: Uuid) -> Result<Option<Model>, InstanceError> {
    Entity::find_by_id(id)
        .one(db)
        .await
        .map_err(InstanceError::Database)
}

/// Get or create an instance by platform type and host.
///
/// If an instance with the given platform type and host exists, returns it.
/// Otherwise, creates a new instance with a generated name based on the host.
pub async fn get_or_create(
    db: &DatabaseConnection,
    platform_type: PlatformType,
    host: &str,
) -> Result<Model, InstanceError> {
    // Try to find existing instance
    if let Some(instance) = find_by_host(db, platform_type, host).await? {
        return Ok(instance);
    }

    // Generate a name from the host
    let name = generate_instance_name(platform_type, host);

    // Create new instance
    let instance = ActiveModel {
        id: Set(Uuid::new_v4()),
        name: Set(name),
        platform_type: Set(platform_type),
        host: Set(host.to_string()),
        oauth_client_id: Set(None),
        oauth_flow: Set("auto".to_string()),
        created_at: Set(chrono::Utc::now().fixed_offset()),
    };

    let model = instance.insert(db).await?;
    Ok(model)
}

/// Get or create a well-known instance (github.com, gitlab.com, codeberg.org).
///
/// Uses the canonical names for well-known instances:
/// - "github" for github.com
/// - "gitlab" for gitlab.com  
/// - "codeberg" for codeberg.org
pub async fn get_or_create_well_known(
    db: &DatabaseConnection,
    platform_type: PlatformType,
    host: &str,
) -> Result<Model, InstanceError> {
    // Check if this is a well-known instance using the centralized lookup
    if let Some(wk) = well_known::by_platform_and_host(platform_type, host) {
        // For well-known instances, try to find by name first
        if let Some(instance) = find_by_name(db, wk.name).await? {
            return Ok(instance);
        }

        // Create with canonical name
        let instance = ActiveModel {
            id: Set(Uuid::new_v4()),
            name: Set(wk.name.to_string()),
            platform_type: Set(platform_type),
            host: Set(host.to_string()),
            oauth_client_id: Set(wk.oauth_client_id.map(ToString::to_string)),
            oauth_flow: Set("auto".to_string()),
            created_at: Set(chrono::Utc::now().fixed_offset()),
        };

        let model = instance.insert(db).await?;
        return Ok(model);
    }

    // Not a well-known instance, use the regular get_or_create
    get_or_create(db, platform_type, host).await
}

/// Generate an instance name from the platform type and host.
fn generate_instance_name(platform_type: PlatformType, host: &str) -> String {
    // Check if this is a well-known instance - use canonical name
    if let Some(wk) = well_known::by_platform_and_host(platform_type, host) {
        return wk.name.to_string();
    }

    // Remove common TLDs and simplify for custom instances
    let simplified = host
        .trim_end_matches(".com")
        .trim_end_matches(".org")
        .trim_end_matches(".io")
        .replace('.', "-");

    // Add platform prefix for non-canonical hosts
    match platform_type {
        PlatformType::GitHub => format!("github-{}", simplified),
        PlatformType::GitLab => format!("gitlab-{}", simplified),
        PlatformType::Gitea => simplified,
    }
}

/// List all instances.
pub async fn list_all(db: &DatabaseConnection) -> Result<Vec<Model>, InstanceError> {
    Entity::find()
        .all(db)
        .await
        .map_err(InstanceError::Database)
}

/// List instances by platform type.
pub async fn list_by_platform_type(
    db: &DatabaseConnection,
    platform_type: PlatformType,
) -> Result<Vec<Model>, InstanceError> {
    Entity::find()
        .filter(instance::Column::PlatformType.eq(platform_type))
        .all(db)
        .await
        .map_err(InstanceError::Database)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult};

    fn instance_model(
        id: Uuid,
        name: &str,
        platform_type: PlatformType,
        host: &str,
        oauth_client_id: Option<&str>,
        oauth_flow: &str,
    ) -> Model {
        Model {
            id,
            name: name.to_string(),
            platform_type,
            host: host.to_string(),
            oauth_client_id: oauth_client_id.map(ToString::to_string),
            oauth_flow: oauth_flow.to_string(),
            created_at: chrono::Utc::now().fixed_offset(),
        }
    }

    #[test]
    fn test_generate_instance_name() {
        // Well-known instances
        assert_eq!(
            generate_instance_name(PlatformType::GitHub, "github.com"),
            "github"
        );
        assert_eq!(
            generate_instance_name(PlatformType::GitLab, "gitlab.com"),
            "gitlab"
        );
        assert_eq!(
            generate_instance_name(PlatformType::Gitea, "codeberg.org"),
            "codeberg"
        );

        // Self-hosted instances
        assert_eq!(
            generate_instance_name(PlatformType::GitHub, "github.mycompany.com"),
            "github-github-mycompany"
        );
        assert_eq!(
            generate_instance_name(PlatformType::GitLab, "gitlab.mycompany.com"),
            "gitlab-gitlab-mycompany"
        );
        assert_eq!(
            generate_instance_name(PlatformType::Gitea, "git.mycompany.com"),
            "git-mycompany"
        );
    }

    #[tokio::test]
    async fn finders_return_expected_models() {
        let id = Uuid::new_v4();
        let model = instance_model(id, "my", PlatformType::GitHub, "ghe.local", None, "auto");

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([vec![model.clone()]])
            .append_query_results([vec![model.clone()]])
            .append_query_results([vec![model.clone()]])
            .into_connection();

        let got = find_by_name(&db, "my")
            .await
            .expect("find_by_name should succeed")
            .expect("expected row");
        assert_eq!(got, model);

        let got = find_by_host(&db, PlatformType::GitHub, "ghe.local")
            .await
            .expect("find_by_host should succeed")
            .expect("expected row");
        assert_eq!(got, model);

        let got = find_by_id(&db, id)
            .await
            .expect("find_by_id should succeed")
            .expect("expected row");
        assert_eq!(got, model);
    }

    #[tokio::test]
    async fn list_functions_return_vectors() {
        let a = instance_model(
            Uuid::new_v4(),
            "a",
            PlatformType::GitHub,
            "a.example",
            None,
            "auto",
        );
        let b = instance_model(
            Uuid::new_v4(),
            "b",
            PlatformType::GitLab,
            "b.example",
            None,
            "auto",
        );

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([vec![a.clone(), b.clone()]])
            .append_query_results([vec![a.clone()]])
            .into_connection();

        let all = list_all(&db).await.expect("list_all should succeed");
        assert_eq!(all, vec![a.clone(), b]);

        let github = list_by_platform_type(&db, PlatformType::GitHub)
            .await
            .expect("list_by_platform_type should succeed");
        assert_eq!(github, vec![a]);
    }

    #[tokio::test]
    async fn get_or_create_returns_existing_when_present() {
        let id = Uuid::new_v4();
        let existing = instance_model(
            id,
            "existing",
            PlatformType::GitLab,
            "gitlab.example.com",
            None,
            "auto",
        );

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([vec![existing.clone()]])
            .into_connection();

        let got = get_or_create(&db, PlatformType::GitLab, "gitlab.example.com")
            .await
            .expect("get_or_create should succeed");
        assert_eq!(got, existing);
    }

    #[tokio::test]
    async fn get_or_create_inserts_when_missing() {
        let host = "gitlab.mycompany.com";
        let expected_name = generate_instance_name(PlatformType::GitLab, host);
        let inserted = instance_model(
            Uuid::new_v4(),
            &expected_name,
            PlatformType::GitLab,
            host,
            None,
            "auto",
        );

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            // find_by_host -> None
            .append_query_results([Vec::<Model>::new()])
            // insert -> exec
            .append_exec_results([MockExecResult {
                rows_affected: 1,
                last_insert_id: 0,
            }])
            // insert -> returning model
            .append_query_results([vec![inserted.clone()]])
            .into_connection();

        let got = get_or_create(&db, PlatformType::GitLab, host)
            .await
            .expect("get_or_create should insert");
        assert_eq!(got, inserted);
    }

    #[tokio::test]
    async fn get_or_create_well_known_prefers_existing_by_name() {
        let existing = instance_model(
            Uuid::new_v4(),
            "github",
            PlatformType::GitHub,
            "github.com",
            Some("Ov23liN0721EfoUpRrLl"),
            "auto",
        );

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            .append_query_results([vec![existing.clone()]])
            .into_connection();

        let got = get_or_create_well_known(&db, PlatformType::GitHub, "github.com")
            .await
            .expect("should succeed");
        assert_eq!(got, existing);
    }

    #[tokio::test]
    async fn get_or_create_well_known_inserts_canonical_when_missing() {
        let inserted = instance_model(
            Uuid::new_v4(),
            "codeberg",
            PlatformType::Gitea,
            "codeberg.org",
            Some("dfe120ce-2440-4f13-8bb0-9ba5620542a7"),
            "auto",
        );

        let db = MockDatabase::new(DatabaseBackend::Sqlite)
            // find_by_name -> None
            .append_query_results([Vec::<Model>::new()])
            // insert -> exec
            .append_exec_results([MockExecResult {
                rows_affected: 1,
                last_insert_id: 0,
            }])
            // insert -> returning model
            .append_query_results([vec![inserted.clone()]])
            .into_connection();

        let got = get_or_create_well_known(&db, PlatformType::Gitea, "codeberg.org")
            .await
            .expect("should succeed");
        assert_eq!(got, inserted);
    }
}
