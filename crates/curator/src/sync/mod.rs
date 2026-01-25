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
//! - [`context`] - Builder pattern for sync operations: `SyncContext`
//! - [`persist_task`] - Background persistence task with batching and retry
//!
//! # Example (Classic API)
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
//!
//! # Example (Builder API)
//!
//! ```ignore
//! use curator::sync::{SyncContext, SyncOptions};
//! use curator::platform::ApiRateLimiter;
//!
//! async fn sync_with_context(client: GitHubClient, db: Arc<DatabaseConnection>) {
//!     let ctx = SyncContext::builder()
//!         .client(client)
//!         .options(SyncOptions::default())
//!         .rate_limiter(ApiRateLimiter::new(5))
//!         .database(db)
//!         .build()
//!         .unwrap();
//!
//!     let result = ctx.sync_namespace_streaming("rust-lang").await?;
//!     println!("Synced {} repos, saved {}", result.sync.matched, result.persist.saved_count);
//! }
//! ```

pub mod context;
pub mod engine;
pub mod persist_task;
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

// Re-export context types
pub use context::{SyncContext, SyncContextBuilder, SyncContextError, SyncStreamingResult};

// Re-export persist task types
pub use persist_task::{
    MODEL_CHANNEL_BUFFER_SIZE, PERSIST_BATCH_SIZE, PERSIST_FLUSH_TIMEOUT, PERSIST_RETRY_ATTEMPTS,
    PERSIST_RETRY_BACKOFF_MS, PersistTaskResult, await_persist_task, create_model_channel,
    display_persist_errors, spawn_persist_task,
};
