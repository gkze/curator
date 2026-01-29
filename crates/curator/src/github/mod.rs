//! GitHub API client for repository operations.
//!
//! This module provides functions for interacting with GitHub's API,
//! including listing organization repositories, starring repos, and
//! converting GitHub data to curator entities.
//!
//! # Module Structure
//!
//! - [`error`] - Error types for GitHub API operations
//! - [`types`] - Data structures and progress events
//! - [`client`] - Client creation and rate limit management
//! - [`repo`] - Repository operations (listing, filtering, starring)
//! - [`convert`] - Model conversion to curator entities
//!
//! # Sync Operations
//!
//! For syncing repositories, use the unified sync engine in [`crate::sync`]:
//!
//! ```ignore
//! use curator::github::GitHubClient;
//! use curator::sync::{sync_namespace_streaming, SyncOptions};
//!
//! let client = GitHubClient::new(&token)?;
//! let result = sync_namespace_streaming(&client, "org-name", &options, None, tx, None).await?;
//! ```

mod client;
mod convert;
mod error;
pub mod oauth;
mod pagination;
mod repo;
mod types;

// Re-export error types
pub use error::GitHubError;

// Re-export types (for backwards compatibility, though most are now in crate::sync)
pub use types::{
    GitHubRateLimitResponse, GitHubRateLimits, ProgressCallback, RateLimitResource, RepoIdentity,
    SyncProgress,
};

// Re-export platform types
pub use crate::platform::{OrgInfo, RateLimitInfo};

// Re-export constants
pub use types::{DEFAULT_CONCURRENCY, DEFAULT_PAGE_FETCH_CONCURRENCY, DEFAULT_STAR_CONCURRENCY};

// Re-export client types and functions
pub use client::{
    GitHubClient, LinkPagination, check_rate_limit, create_client, extract_etag, get_org_info,
    get_rate_limit, get_rate_limits, parse_link_header,
};

// Re-export shared platform types used for conditional fetching
pub use crate::platform::{CacheStats, FetchResult, PaginationInfo};

// Re-export repo operations
pub use repo::{
    filter_by_activity, is_repo_starred, list_org_repos, star_repo, star_repo_with_retry,
};

// Re-export model conversion
pub use convert::{platform_repo_to_active_model, to_code_repository, to_platform_repo};

#[cfg(test)]
mod tests {
    // Integration tests that span multiple submodules can go here
}
