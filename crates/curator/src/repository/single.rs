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
