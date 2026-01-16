//! Shared sync infrastructure for all platforms.
//!
//! This module provides platform-agnostic types and utilities for syncing
//! repositories from code hosting platforms like GitHub, GitLab, and Codeberg.
//!
//! # Module Structure
//!
//! - [`types`] - Core types: `SyncResult`, `SyncOptions`, constants
//! - [`progress`] - Progress reporting: `SyncProgress`, `ProgressCallback`, `emit()`
//! - [`engine`] - Unified sync engine: `sync_namespace()`, `sync_namespaces_streaming()`
//!
//! # Example
//!
//! ```ignore
//! use curator::sync::{SyncOptions, SyncProgress, emit, sync_namespace};
//! use curator::platform::{PlatformClient, ApiRateLimiter, rate_limits};
//!
//! async fn sync<C: PlatformClient + Clone + 'static>(client: &C) {
//!     let options = SyncOptions::default();
//!     let limiter = ApiRateLimiter::new(rate_limits::GITHUB_DEFAULT_RPS);
//!     let (result, models) = sync_namespace(client, "my-org", &options, Some(&limiter), None).await?;
//!     println!("Synced {} repos", result.matched);
//! }
//! ```

pub mod engine;
mod progress;
mod types;

// Re-export types
pub use types::{
    NamespaceSyncResult, NamespaceSyncResultStreaming, PlatformOptions, StarResult, StarringStats,
    SyncOptions, SyncResult,
};

// Re-export constants
pub use types::{
    DEFAULT_CONCURRENCY, DEFAULT_PAGE_FETCH_CONCURRENCY, DEFAULT_STAR_CONCURRENCY,
    INITIAL_BACKOFF_MS, MAX_BACKOFF_MS, MAX_STAR_RETRIES,
};

// Re-export progress types
pub use progress::{ProgressCallback, SyncProgress, emit};

// Re-export engine functions for convenience
pub use engine::{
    filter_by_activity, sync_namespace, sync_namespace_streaming, sync_namespaces,
    sync_namespaces_streaming, sync_starred_streaming, sync_user, sync_user_streaming,
    sync_users_streaming,
};
