use sea_orm::DbErr;
use thiserror::Error;
use uuid::Uuid;

use crate::entity::code_platform::CodePlatform;

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
    #[error("Repository already exists: {platform}/{owner}/{name}")]
    Duplicate {
        platform: CodePlatform,
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
    pub fn not_found_by_key(platform: CodePlatform, owner: &str, name: &str) -> Self {
        Self::NotFound {
            context: format!("{:?}/{}/{}", platform, owner, name),
        }
    }

    /// Create a NotFound error for a platform_id lookup.
    pub fn not_found_by_platform_id(platform: CodePlatform, platform_id: i64) -> Self {
        Self::NotFound {
            context: format!("{:?} platform_id={}", platform, platform_id),
        }
    }
}

/// Result type alias for repository operations.
pub type Result<T> = std::result::Result<T, RepositoryError>;
