//! Gitea API client for repository operations.
//!
//! This module provides functions for interacting with Gitea-based forges
//! (Gitea, Forgejo, Codeberg), including listing organization repositories,
//! starring repos, and converting Gitea data to curator entities.
//!
//! # Module Structure
//!
//! - [`error`] - Error types for Gitea API operations
//! - [`types`] - Data structures and progress events
//! - [`client`] - Client creation and management
//! - [`convert`] - Model conversion to curator entities
//!
//! # Sync Operations
//!
//! For syncing repositories, use the unified sync engine in [`crate::sync`]:
//!
//! ```ignore
//! use curator::gitea::GiteaClient;
//! use curator::entity::code_platform::CodePlatform;
//! use curator::sync::{sync_namespace_streaming, SyncOptions};
//!
//! // For Codeberg
//! let client = GiteaClient::new("https://codeberg.org", "token", CodePlatform::Codeberg)?;
//!
//! // For self-hosted Gitea/Forgejo
//! let client = GiteaClient::new("https://git.example.com", "token", CodePlatform::Gitea)?;
//!
//! let options = SyncOptions::default();
//! let result = sync_namespace_streaming(&client, "my-org", &options, None, tx, None).await?;
//! println!("Synced {} repositories", result.matched);
//! ```

mod client;
mod convert;
mod error;
mod types;

// Re-export error types
pub use error::GiteaError;

// Re-export types (for backwards compatibility, though most are now in crate::sync)
pub use types::{GiteaOrg, GiteaRepo, GiteaUser, ProgressCallback, SyncProgress};

// Re-export client
pub use client::{CODEBERG_HOST, GiteaClient, create_client};

// Re-export platform types used here
pub use crate::platform::{OrgInfo, RateLimitInfo};

// Re-export model conversion
pub use convert::{platform_repo_to_active_model, to_code_repository, to_platform_repo};

#[cfg(test)]
mod tests {
    // Integration tests that span multiple submodules can go here
}
