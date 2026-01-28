//! GitLab API client for repository operations.
//!
//! This module provides functions for interacting with GitLab's API,
//! including listing group projects, starring repos, and
//! converting GitLab data to curator entities.
//!
//! # Module Structure
//!
//! - [`error`] - Error types for GitLab API operations
//! - [`types`] - Data structures and progress events
//! - [`client`] - Client creation and management
//! - [`repo`] - Repository operations (listing, filtering, starring)
//! - [`convert`] - Model conversion to curator entities
//!
//! # Sync Operations
//!
//! For syncing repositories, use the unified sync engine in [`crate::sync`]:
//!
//! ```ignore
//! use curator::gitlab::GitLabClient;
//! use curator::sync::{sync_namespace_streaming, SyncOptions, PlatformOptions};
//!
//! let client = GitLabClient::new(&host, &token).await?;
//! let options = SyncOptions {
//!     platform_options: PlatformOptions { include_subgroups: true },
//!     ..Default::default()
//! };
//! let result = sync_namespace_streaming(&client, "group-name", &options, None, tx, None).await?;
//! ```

pub(crate) mod api;
mod client;
mod convert;
mod error;
pub mod oauth;
mod repo;
mod types;

// Re-export error types
pub use error::GitLabError;

// Re-export types (for backwards compatibility, though most are now in crate::sync)
pub use types::{
    ForkedFrom, GitLabGroup, GitLabNamespace, GitLabProject, ProgressCallback, SyncProgress,
};

// Re-export client
pub use client::{GitLabClient, create_client};

// Re-export platform types
pub use crate::platform::{OrgInfo, RateLimitInfo};

// Re-export repo operations
pub use repo::{filter_by_activity, list_group_projects, star_project, star_project_with_retry};

// Re-export model conversion
pub use convert::{platform_repo_to_active_model, to_code_repository, to_platform_repo};

#[cfg(test)]
mod tests {
    // Integration tests that span multiple submodules can go here
}
