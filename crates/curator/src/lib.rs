//! Curator - A multi-platform repository tracker.
//!
//! This library provides a unified interface for tracking repositories across
//! GitHub, GitLab, Codeberg, and other Gitea-based forges.
//!
//! # Features
//!
//! - `migrate` - Enables database migration support. When enabled, you can use
//!   [`connect_and_migrate`] to automatically run migrations on connection.
//!
//! # Example
//!
//! ```ignore
//! use curator::{connect_and_migrate, repository, CodePlatform};
//!
//! let db = connect_and_migrate("sqlite://curator.db?mode=rwc").await?;
//!
//! // Count repositories by platform
//! let github_count = repository::count_by_platform(&db, CodePlatform::GitHub).await?;
//!
//! // Find stale repositories for re-sync
//! let stale = repository::find_stale(&db, one_hour_ago, 100).await?;
//! ```

pub mod api_cache;
pub mod db;
pub mod entity;
pub mod platform;
pub mod repository;
pub mod sync;

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub mod retry;

#[cfg(feature = "github")]
pub mod github;

#[cfg(feature = "gitlab")]
pub mod gitlab;

#[cfg(feature = "gitea")]
pub mod gitea;

#[cfg(feature = "migrate")]
pub mod migration;

pub use db::connect;
#[cfg(feature = "migrate")]
pub use db::connect_and_migrate;
pub use entity::prelude::*;
pub use platform::{
    ApiRateLimiter, PlatformClient, PlatformError, PlatformRepo,
    RateLimitInfo as PlatformRateLimitInfo, RateLimitedClient, rate_limits, strip_null_values,
};
pub use repository::RepositoryError;
