use sea_orm::DbErr;
use thiserror::Error;
use uuid::Uuid;

/// Errors that can occur during repository operations.
#[derive(Debug, Error)]
pub enum RepositoryError {
    /// Database error from sea-orm.
    #[error("Database error: {0}")]
    Database(#[from] DbErr),

    /// Repository not found.
    #[error("Repository not found: {context}")]
    NotFound { context: String },

    /// Duplicate repository (natural key conflict).
    #[error("Repository already exists: {instance_id}/{owner}/{name}")]
    Duplicate {
        instance_id: Uuid,
        owner: String,
        name: String,
    },

    /// Invalid input data.
    #[error("Invalid input: {message}")]
    InvalidInput { message: String },

    /// Bulk operation partially failed.
    #[error("Bulk operation failed: {succeeded} succeeded, {failed} failed")]
    PartialFailure { succeeded: usize, failed: usize },
}

impl RepositoryError {
    /// Create a NotFound error for a UUID lookup.
    pub fn not_found_by_id(id: Uuid) -> Self {
        Self::NotFound {
            context: format!("id={}", id),
        }
    }

    /// Create a NotFound error for a natural key lookup.
    pub fn not_found_by_key(instance_id: Uuid, owner: &str, name: &str) -> Self {
        Self::NotFound {
            context: format!("{}/{}/{}", instance_id, owner, name),
        }
    }

    /// Create a NotFound error for a platform_id lookup.
    pub fn not_found_by_platform_id(instance_id: Uuid, platform_id: i64) -> Self {
        Self::NotFound {
            context: format!("{} platform_id={}", instance_id, platform_id),
        }
    }
}

/// Result type alias for repository operations.
pub type Result<T> = std::result::Result<T, RepositoryError>;
