//! Shared sync types and constants for all platforms.
//!
//! This module contains platform-agnostic types used across GitHub, GitLab,
//! and future platform implementations.

use chrono::Duration;

/// Strategy for syncing repositories.
///
/// This determines how the sync engine fetches repository data from the platform.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SyncStrategy {
    /// Full sync: Fetch all repositories from the platform, regardless of
    /// whether they've changed since the last sync. This is the default
    /// and most thorough approach.
    #[default]
    Full,

    /// Incremental sync: Only fetch repositories that have been updated
    /// on the platform since the last sync. This is faster but requires
    /// existing sync data in the database to compare against.
    ///
    /// When no existing data is found, falls back to Full sync behavior.
    Incremental,
}

/// Default number of concurrent API requests for fetching.
pub const DEFAULT_CONCURRENCY: usize = 20;

/// Default concurrency for starring operations.
pub const DEFAULT_STAR_CONCURRENCY: usize = 20;

/// Default concurrency for page fetching operations.
/// Limited to 2 to avoid hitting secondary rate limits.
pub const DEFAULT_PAGE_FETCH_CONCURRENCY: usize = 2;

/// Maximum backoff delay in milliseconds when rate limited.
pub const MAX_BACKOFF_MS: u64 = 60_000;

/// Initial backoff delay in milliseconds.
pub const INITIAL_BACKOFF_MS: u64 = 1_000;

/// Maximum retries for a single starring operation.
pub const MAX_STAR_RETRIES: u32 = 5;

/// Result of a sync operation.
///
/// This type should not be silently ignored as it contains error information
/// and important statistics about the sync operation.
#[derive(Debug, Default)]
#[must_use = "SyncResult may contain errors that should be checked"]
pub struct SyncResult {
    /// Number of repositories processed.
    pub processed: usize,
    /// Number of repositories that matched the activity filter.
    pub matched: usize,
    /// Number of repositories starred.
    pub starred: usize,
    /// Number of repositories saved to database.
    pub saved: usize,
    /// Number of repositories skipped (already starred, etc.).
    pub skipped: usize,
    /// Number of repositories pruned (unstarred due to inactivity).
    pub pruned: usize,
    /// List of pruned repositories (owner, name) for database cleanup.
    pub pruned_repos: Vec<(String, String)>,
    /// Errors encountered (non-fatal).
    pub errors: Vec<String>,
}

/// Platform-specific sync options.
#[derive(Debug, Clone, Default)]
pub struct PlatformOptions {
    /// GitLab: Include projects from subgroups.
    pub include_subgroups: bool,
}

/// Options for syncing a namespace's repositories.
#[derive(Debug, Clone)]
pub struct SyncOptions {
    /// Only include repos with activity within this duration.
    pub active_within: Duration,
    /// Whether to star matching repositories.
    pub star: bool,
    /// Dry run mode - don't actually star or save.
    pub dry_run: bool,
    /// Maximum concurrent API requests.
    pub concurrency: usize,
    /// Platform-specific options.
    pub platform_options: PlatformOptions,
    /// Whether to prune (unstar) inactive repositories during starred sync.
    pub prune: bool,
    /// Sync strategy: Full or Incremental.
    pub strategy: SyncStrategy,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            active_within: Duration::days(60), // 2 months
            star: true,
            dry_run: false,
            concurrency: DEFAULT_CONCURRENCY,
            platform_options: PlatformOptions::default(),
            prune: true, // Default to pruning inactive starred repos
            strategy: SyncStrategy::default(),
        }
    }
}

/// Result of syncing a single namespace.
///
/// This type should not be silently ignored as it may contain an error
/// indicating the namespace sync failed entirely.
#[derive(Debug)]
#[must_use = "NamespaceSyncResult may contain errors that should be checked"]
pub struct NamespaceSyncResult {
    /// The namespace name/path.
    pub namespace: String,
    /// The sync result.
    pub result: SyncResult,
    /// The repository models ready for saving.
    pub models: Vec<crate::entity::code_repository::ActiveModel>,
    /// Error if the namespace sync failed entirely.
    pub error: Option<String>,
}

/// Result of syncing a single namespace (streaming version).
///
/// This version doesn't include models since they're sent via channel.
/// This type should not be silently ignored as it may contain an error
/// indicating the namespace sync failed entirely.
#[derive(Debug)]
#[must_use = "NamespaceSyncResultStreaming may contain errors that should be checked"]
pub struct NamespaceSyncResultStreaming {
    /// The namespace name/path.
    pub namespace: String,
    /// The sync result.
    pub result: SyncResult,
    /// Error if the namespace sync failed entirely.
    pub error: Option<String>,
}

/// Result of processing a single repository for starring.
#[derive(Debug)]
pub enum StarResult {
    /// Repository was successfully starred.
    Starred,
    /// Repository was already starred.
    AlreadyStarred,
    /// Failed to star with error message.
    Error(String),
}

/// Result of a batch starring operation.
///
/// This type should not be silently ignored as it contains error information
/// about failed starring operations.
#[derive(Debug, Default)]
#[must_use = "StarringStats may contain errors that should be checked"]
pub struct StarringStats {
    /// Number of repositories successfully starred.
    pub starred: usize,
    /// Number of repositories skipped (already starred).
    pub skipped: usize,
    /// Errors encountered during starring.
    pub errors: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_strategy_default() {
        let strategy = SyncStrategy::default();
        assert_eq!(strategy, SyncStrategy::Full);
    }

    #[test]
    fn test_sync_options_default() {
        let options = SyncOptions::default();

        assert_eq!(options.active_within, Duration::days(60));
        assert!(options.star);
        assert!(!options.dry_run);
        assert_eq!(options.concurrency, DEFAULT_CONCURRENCY);
        assert!(!options.platform_options.include_subgroups);
        assert_eq!(options.strategy, SyncStrategy::Full);
    }

    #[test]
    fn test_sync_result_default() {
        let result = SyncResult::default();

        assert_eq!(result.processed, 0);
        assert_eq!(result.matched, 0);
        assert_eq!(result.starred, 0);
        assert_eq!(result.saved, 0);
        assert_eq!(result.skipped, 0);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_starring_stats_default() {
        let stats = StarringStats::default();

        assert_eq!(stats.starred, 0);
        assert_eq!(stats.skipped, 0);
        assert!(stats.errors.is_empty());
    }
}
