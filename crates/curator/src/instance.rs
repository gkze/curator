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
}
