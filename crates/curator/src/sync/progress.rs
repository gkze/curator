//! Progress reporting types for sync operations.
//!
//! This module provides a unified progress event system used across all
//! platform implementations to report sync progress to the UI.

/// Progress events emitted during sync operations.
///
/// This enum uses platform-agnostic terminology:
/// - "namespace" instead of "org" (GitHub) or "group" (GitLab)
/// - "repository" instead of "repo" (GitHub) or "project" (GitLab)
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SyncProgress {
    /// Starting to fetch repositories for a namespace.
    FetchingRepos {
        /// The namespace being synced (org/group name).
        namespace: String,
        /// Total repositories in the namespace (if known from metadata).
        total_repos: Option<usize>,
        /// Expected number of pages (if total is known).
        expected_pages: Option<u32>,
    },

    /// Fetched a page of repositories.
    FetchedPage {
        /// The namespace this page belongs to.
        namespace: String,
        /// Page number (1-indexed).
        page: u32,
        /// Number of repos on this page.
        count: usize,
        /// Running total of repos fetched so far.
        total_so_far: usize,
        /// Expected total pages (if known).
        expected_pages: Option<u32>,
    },

    /// Finished fetching all repositories.
    FetchComplete {
        /// The namespace that finished fetching.
        namespace: String,
        /// Total number of repositories fetched.
        total: usize,
    },

    /// Starting activity filter.
    FilteringByActivity {
        /// The namespace being filtered.
        namespace: String,
        /// Number of days to filter by.
        days: i64,
    },

    /// Activity filter complete.
    FilterComplete {
        /// The namespace that finished filtering.
        namespace: String,
        /// Number of repos that matched the filter.
        matched: usize,
        /// Total repos before filtering.
        total: usize,
    },

    /// Filtered a page (streaming mode) - running count of matched repos.
    FilteredPage {
        /// Running count of matched repos.
        matched_so_far: usize,
        /// Running count of total repos processed so far.
        processed_so_far: usize,
    },

    /// Starting to star repositories.
    StarringRepos {
        /// Number of repos to star.
        count: usize,
        /// Concurrency level for starring.
        concurrency: usize,
        /// Whether this is a dry run.
        dry_run: bool,
    },

    /// Starred (or checked) a single repository.
    StarredRepo {
        /// Repository owner.
        owner: String,
        /// Repository name.
        name: String,
        /// True if the repo was already starred.
        already_starred: bool,
    },

    /// Failed to star a repository.
    StarError {
        /// Repository owner.
        owner: String,
        /// Repository name.
        name: String,
        /// Error message.
        error: String,
    },

    /// Starring phase complete.
    StarringComplete {
        /// Number of repos newly starred.
        starred: usize,
        /// Number of repos already starred.
        already_starred: usize,
        /// Number of errors.
        errors: usize,
    },

    /// Converting to database models.
    ConvertingModels,

    /// Models ready for persistence.
    ModelsReady {
        /// Number of models ready.
        count: usize,
    },

    /// Flushing a batch of models to the database.
    PersistingBatch {
        /// Number of models in the batch.
        count: usize,
        /// Whether this is the final batch after the channel closed.
        final_batch: bool,
    },

    /// A repository was persisted to DB.
    Persisted {
        /// Repository owner.
        owner: String,
        /// Repository name.
        name: String,
    },

    /// Failed to persist a repository.
    PersistError {
        /// Repository owner.
        owner: String,
        /// Repository name.
        name: String,
        /// Error message.
        error: String,
    },

    /// Syncing multiple namespaces.
    SyncingNamespaces {
        /// Number of namespaces to sync.
        count: usize,
    },

    /// Completed syncing multiple namespaces.
    SyncNamespacesComplete {
        /// Number of successful syncs.
        successful: usize,
        /// Number of failed syncs.
        failed: usize,
    },

    /// Warning message (non-fatal).
    Warning {
        /// Warning message.
        message: String,
    },

    /// Rate limited, backing off before retry.
    RateLimitBackoff {
        /// Repository owner (if applicable).
        owner: String,
        /// Repository name (if applicable).
        name: String,
        /// Time to wait before retry (ms).
        retry_after_ms: u64,
        /// Current attempt number.
        attempt: u32,
    },

    /// Page fetch rate limited, retrying.
    PageFetchRetry {
        /// Page number being retried.
        page: u32,
        /// Time to wait before retry (ms).
        retry_after_ms: u64,
        /// Current attempt number.
        attempt: u32,
    },

    /// Starting to unstar (prune) inactive repositories.
    PruningRepos {
        /// Number of repos to prune.
        count: usize,
        /// Whether this is a dry run.
        dry_run: bool,
    },

    /// Unstarred (pruned) a single repository.
    PrunedRepo {
        /// Repository owner.
        owner: String,
        /// Repository name.
        name: String,
    },

    /// Failed to prune a repository.
    PruneError {
        /// Repository owner.
        owner: String,
        /// Repository name.
        name: String,
        /// Error message.
        error: String,
    },

    /// Pruning phase complete.
    PruningComplete {
        /// Number of repos pruned.
        pruned: usize,
        /// Number of errors.
        errors: usize,
    },

    /// Cache hit - data loaded from local cache instead of API.
    ///
    /// This is emitted when an ETag check returns 304 Not Modified,
    /// indicating that the API data hasn't changed since the last sync.
    CacheHit {
        /// The namespace/endpoint that was cached (e.g., "starred", "rust-lang").
        namespace: String,
        /// Number of items loaded from cache.
        cached_count: usize,
    },
}

/// Callback for progress updates during sync operations.
pub type ProgressCallback = Box<dyn Fn(SyncProgress) + Send + Sync>;

/// Emit a progress event if a callback is provided.
///
/// This is a convenience function to avoid repetitive `if let Some(cb) = ...` patterns.
///
/// # Example
///
/// ```ignore
/// use curator::sync::{emit, SyncProgress, ProgressCallback};
///
/// fn my_sync(on_progress: Option<&ProgressCallback>) {
///     emit(on_progress, SyncProgress::FetchComplete { total: 42 });
/// }
/// ```
#[inline]
pub fn emit(on_progress: Option<&ProgressCallback>, event: SyncProgress) {
    if let Some(cb) = on_progress {
        cb(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_emit_with_callback() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&count);

        let callback: ProgressCallback = Box::new(move |_event| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });

        emit(
            Some(&callback),
            SyncProgress::FetchComplete {
                namespace: "test".to_string(),
                total: 10,
            },
        );
        emit(
            Some(&callback),
            SyncProgress::FilterComplete {
                namespace: "test".to_string(),
                matched: 5,
                total: 10,
            },
        );

        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_emit_without_callback() {
        // Should not panic when callback is None
        emit(
            None,
            SyncProgress::FetchComplete {
                namespace: "test".to_string(),
                total: 10,
            },
        );
    }

    #[test]
    fn test_sync_progress_debug() {
        let event = SyncProgress::StarredRepo {
            owner: "rust-lang".to_string(),
            name: "rust".to_string(),
            already_starred: false,
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("rust-lang"));
        assert!(debug_str.contains("rust"));
    }

    #[test]
    fn test_sync_progress_fetching_repos() {
        let event = SyncProgress::FetchingRepos {
            namespace: "rust-lang".to_string(),
            total_repos: Some(100),
            expected_pages: Some(2),
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("FetchingRepos"));
        assert!(debug_str.contains("rust-lang"));
    }

    #[test]
    fn test_sync_progress_fetched_page() {
        let event = SyncProgress::FetchedPage {
            namespace: "test".to_string(),
            page: 1,
            count: 50,
            total_so_far: 50,
            expected_pages: Some(3),
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("FetchedPage"));
        assert!(debug_str.contains("50"));
    }

    #[test]
    fn test_sync_progress_filtered_page_with_processed() {
        let event = SyncProgress::FilteredPage {
            matched_so_far: 25,
            processed_so_far: 100,
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("FilteredPage"));
        assert!(debug_str.contains("25"));
        assert!(debug_str.contains("100"));
    }

    #[test]
    fn test_sync_progress_pruning_repos() {
        let event = SyncProgress::PruningRepos {
            count: 10,
            dry_run: false,
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("PruningRepos"));
        assert!(debug_str.contains("10"));
    }

    #[test]
    fn test_sync_progress_pruned_repo() {
        let event = SyncProgress::PrunedRepo {
            owner: "old-owner".to_string(),
            name: "inactive-repo".to_string(),
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("PrunedRepo"));
        assert!(debug_str.contains("old-owner"));
        assert!(debug_str.contains("inactive-repo"));
    }

    #[test]
    fn test_sync_progress_prune_error() {
        let event = SyncProgress::PruneError {
            owner: "failing".to_string(),
            name: "repo".to_string(),
            error: "API rate limited".to_string(),
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("PruneError"));
        assert!(debug_str.contains("failing"));
        assert!(debug_str.contains("API rate limited"));
    }

    #[test]
    fn test_sync_progress_pruning_complete() {
        let event = SyncProgress::PruningComplete {
            pruned: 8,
            errors: 2,
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("PruningComplete"));
        assert!(debug_str.contains("8"));
        assert!(debug_str.contains("2"));
    }

    #[test]
    fn test_sync_progress_star_error() {
        let event = SyncProgress::StarError {
            owner: "test".to_string(),
            name: "repo".to_string(),
            error: "403 Forbidden".to_string(),
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("StarError"));
        assert!(debug_str.contains("403 Forbidden"));
    }

    #[test]
    fn test_sync_progress_rate_limit_backoff() {
        let event = SyncProgress::RateLimitBackoff {
            owner: "test".to_string(),
            name: "repo".to_string(),
            retry_after_ms: 5000,
            attempt: 2,
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("RateLimitBackoff"));
        assert!(debug_str.contains("5000"));
        assert!(debug_str.contains("2"));
    }

    #[test]
    fn test_sync_progress_page_fetch_retry() {
        let event = SyncProgress::PageFetchRetry {
            page: 3,
            retry_after_ms: 10000,
            attempt: 1,
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("PageFetchRetry"));
        assert!(debug_str.contains("10000"));
    }

    #[test]
    fn test_sync_progress_warning() {
        let event = SyncProgress::Warning {
            message: "Channel closed early".to_string(),
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Warning"));
        assert!(debug_str.contains("Channel closed"));
    }

    #[test]
    fn test_sync_progress_persist_error() {
        let event = SyncProgress::PersistError {
            owner: "test".to_string(),
            name: "repo".to_string(),
            error: "Database connection lost".to_string(),
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("PersistError"));
        assert!(debug_str.contains("Database connection"));
    }

    #[test]
    fn test_sync_progress_clone() {
        let event = SyncProgress::FetchComplete {
            namespace: "test".to_string(),
            total: 42,
        };
        let cloned = event.clone();

        if let SyncProgress::FetchComplete { total, .. } = cloned {
            assert_eq!(total, 42);
        } else {
            panic!("Clone produced wrong variant");
        }
    }

    #[test]
    fn test_emit_multiple_events() {
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let events_clone = Arc::clone(&events);

        let callback: ProgressCallback = Box::new(move |event| {
            events_clone.lock().unwrap().push(format!("{:?}", event));
        });

        emit(
            Some(&callback),
            SyncProgress::FetchingRepos {
                namespace: "org".to_string(),
                total_repos: None,
                expected_pages: None,
            },
        );
        emit(
            Some(&callback),
            SyncProgress::FetchedPage {
                namespace: "org".to_string(),
                page: 1,
                count: 100,
                total_so_far: 100,
                expected_pages: None,
            },
        );
        emit(
            Some(&callback),
            SyncProgress::FetchComplete {
                namespace: "org".to_string(),
                total: 100,
            },
        );

        let recorded = events.lock().unwrap();
        assert_eq!(recorded.len(), 3);
        assert!(recorded[0].contains("FetchingRepos"));
        assert!(recorded[1].contains("FetchedPage"));
        assert!(recorded[2].contains("FetchComplete"));
    }

    #[test]
    fn test_sync_progress_all_variants_constructable() {
        // Verify all variants can be constructed
        let _events: Vec<SyncProgress> = vec![
            SyncProgress::FetchingRepos {
                namespace: "ns".to_string(),
                total_repos: Some(10),
                expected_pages: Some(1),
            },
            SyncProgress::FetchedPage {
                namespace: "ns".to_string(),
                page: 1,
                count: 10,
                total_so_far: 10,
                expected_pages: Some(1),
            },
            SyncProgress::FetchComplete {
                namespace: "ns".to_string(),
                total: 10,
            },
            SyncProgress::FilteringByActivity {
                namespace: "ns".to_string(),
                days: 60,
            },
            SyncProgress::FilterComplete {
                namespace: "ns".to_string(),
                matched: 5,
                total: 10,
            },
            SyncProgress::FilteredPage {
                matched_so_far: 5,
                processed_so_far: 10,
            },
            SyncProgress::StarringRepos {
                count: 5,
                concurrency: 10,
                dry_run: false,
            },
            SyncProgress::StarredRepo {
                owner: "o".to_string(),
                name: "n".to_string(),
                already_starred: false,
            },
            SyncProgress::StarError {
                owner: "o".to_string(),
                name: "n".to_string(),
                error: "e".to_string(),
            },
            SyncProgress::StarringComplete {
                starred: 4,
                already_starred: 1,
                errors: 0,
            },
            SyncProgress::ConvertingModels,
            SyncProgress::ModelsReady { count: 5 },
            SyncProgress::PersistingBatch {
                count: 10,
                final_batch: false,
            },
            SyncProgress::Persisted {
                owner: "o".to_string(),
                name: "n".to_string(),
            },
            SyncProgress::PersistError {
                owner: "o".to_string(),
                name: "n".to_string(),
                error: "e".to_string(),
            },
            SyncProgress::SyncingNamespaces { count: 3 },
            SyncProgress::SyncNamespacesComplete {
                successful: 2,
                failed: 1,
            },
            SyncProgress::Warning {
                message: "warn".to_string(),
            },
            SyncProgress::RateLimitBackoff {
                owner: "o".to_string(),
                name: "n".to_string(),
                retry_after_ms: 1000,
                attempt: 1,
            },
            SyncProgress::PageFetchRetry {
                page: 1,
                retry_after_ms: 1000,
                attempt: 1,
            },
            SyncProgress::PruningRepos {
                count: 2,
                dry_run: false,
            },
            SyncProgress::PrunedRepo {
                owner: "o".to_string(),
                name: "n".to_string(),
            },
            SyncProgress::PruneError {
                owner: "o".to_string(),
                name: "n".to_string(),
                error: "e".to_string(),
            },
            SyncProgress::PruningComplete {
                pruned: 2,
                errors: 0,
            },
            SyncProgress::CacheHit {
                namespace: "starred".to_string(),
                cached_count: 1000,
            },
        ];
        // If this compiles, all variants are constructable
        assert!(_events.len() > 20);
    }
}
