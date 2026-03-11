use curator::sync::SyncProgress;

/// Logging reporter using tracing for structured output.
pub struct LoggingReporter;

impl LoggingReporter {
    pub fn new() -> Self {
        Self
    }

    pub fn handle(&self, event: SyncProgress) {
        match event {
            SyncProgress::FetchingRepos {
                namespace,
                total_repos,
                expected_pages,
            } => {
                tracing::info!(
                    namespace = %namespace,
                    total_repos = ?total_repos,
                    expected_pages = ?expected_pages,
                    "Fetching repositories"
                );
            }

            SyncProgress::FetchedPage {
                namespace,
                page,
                count,
                total_so_far,
                expected_pages,
            } => {
                tracing::debug!(namespace = %namespace, page, count, total_so_far, expected_pages = ?expected_pages, "Fetched page");
            }

            SyncProgress::FetchComplete { namespace, total } => {
                tracing::info!(namespace = %namespace, total, "Fetch complete");
            }

            SyncProgress::FilteringByActivity { namespace, days } => {
                tracing::debug!(namespace = %namespace, days, "Filtering by activity");
            }

            SyncProgress::FilterComplete {
                namespace,
                matched,
                total,
            } => {
                tracing::info!(namespace = %namespace, matched, total, "Filtered by activity");
            }

            SyncProgress::FilteredPage {
                matched_so_far,
                processed_so_far,
            } => {
                tracing::debug!(matched_so_far, processed_so_far, "Filtered page");
            }

            SyncProgress::StarringRepos {
                count,
                concurrency,
                dry_run,
            } => {
                tracing::info!(count, concurrency, dry_run, "Starring repositories");
            }

            SyncProgress::StarredRepo {
                owner,
                name,
                already_starred,
            } => {
                if already_starred {
                    tracing::debug!(repo = %format!("{}/{}", owner, name), "Already starred");
                } else {
                    tracing::info!(repo = %format!("{}/{}", owner, name), "Starred");
                }
            }

            SyncProgress::StarError { owner, name, error } => {
                tracing::warn!(repo = %format!("{}/{}", owner, name), error = %error, "Failed to star");
            }

            SyncProgress::StarringComplete {
                starred,
                already_starred,
                errors,
            } => {
                tracing::info!(starred, already_starred, errors, "Starring complete");
            }

            SyncProgress::ConvertingModels => {
                tracing::debug!("Converting to database models");
            }

            SyncProgress::ModelsReady { count } => {
                tracing::debug!(count, "Models ready for saving");
            }

            SyncProgress::PersistingBatch { count, final_batch } => {
                if final_batch {
                    tracing::info!(count, "Flushing final persistence batch");
                } else {
                    tracing::debug!(count, "Flushing persistence batch");
                }
            }

            SyncProgress::Persisted { owner, name } => {
                tracing::debug!(repo = %format!("{}/{}", owner, name), "Saved to database");
            }

            SyncProgress::PersistError { owner, name, error } => {
                tracing::error!(repo = %format!("{}/{}", owner, name), error = %error, "Failed to save");
            }

            SyncProgress::SyncingNamespaces { count } => {
                tracing::info!(count, "Syncing organizations");
            }

            SyncProgress::SyncNamespacesComplete { successful, failed } => {
                tracing::info!(successful, failed, "Sync complete");
            }

            SyncProgress::Warning { message } => {
                tracing::warn!(message = %message, "Warning");
            }

            SyncProgress::RateLimitBackoff {
                owner,
                name,
                retry_after_ms,
                attempt,
            } => {
                tracing::warn!(
                    repo = %format!("{}/{}", owner, name),
                    retry_after_ms,
                    attempt,
                    "Rate limited, backing off"
                );
            }

            SyncProgress::PruningRepos { count, dry_run } => {
                tracing::info!(count, dry_run, "Pruning inactive repositories");
            }

            SyncProgress::PrunedRepo { owner, name } => {
                tracing::debug!(repo = %format!("{}/{}", owner, name), "Pruned");
            }

            SyncProgress::PruneError { owner, name, error } => {
                tracing::warn!(repo = %format!("{}/{}", owner, name), error = %error, "Failed to prune");
            }

            SyncProgress::PruningComplete { pruned, errors } => {
                tracing::info!(pruned, errors, "Pruning complete");
            }

            SyncProgress::CacheHit {
                namespace,
                cached_count,
            } => {
                tracing::info!(
                    namespace = %namespace,
                    cached_count,
                    "Cache hit - loaded from local cache"
                );
            }

            other => {
                tracing::debug!(event = ?other, "Unhandled sync progress event");
            }
        }
    }
}

impl Default for LoggingReporter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logging_reporter_handles_all_sync_progress_variants() {
        let reporter = LoggingReporter::new();

        let events = vec![
            SyncProgress::FetchingRepos {
                namespace: "org/repo".to_string(),
                total_repos: Some(10),
                expected_pages: Some(2),
            },
            SyncProgress::FetchedPage {
                namespace: "org/repo".to_string(),
                page: 1,
                count: 5,
                total_so_far: 5,
                expected_pages: Some(2),
            },
            SyncProgress::FetchComplete {
                namespace: "org/repo".to_string(),
                total: 10,
            },
            SyncProgress::FilteringByActivity {
                namespace: "org/repo".to_string(),
                days: 30,
            },
            SyncProgress::FilterComplete {
                namespace: "org/repo".to_string(),
                matched: 4,
                total: 10,
            },
            SyncProgress::FilteredPage {
                matched_so_far: 4,
                processed_so_far: 10,
            },
            SyncProgress::StarringRepos {
                count: 4,
                concurrency: 2,
                dry_run: false,
            },
            SyncProgress::StarredRepo {
                owner: "org".to_string(),
                name: "repo".to_string(),
                already_starred: false,
            },
            SyncProgress::StarredRepo {
                owner: "org".to_string(),
                name: "repo2".to_string(),
                already_starred: true,
            },
            SyncProgress::StarError {
                owner: "org".to_string(),
                name: "repo3".to_string(),
                error: "boom".to_string(),
            },
            SyncProgress::StarringComplete {
                starred: 1,
                already_starred: 1,
                errors: 1,
            },
            SyncProgress::ConvertingModels,
            SyncProgress::ModelsReady { count: 3 },
            SyncProgress::PersistingBatch {
                count: 2,
                final_batch: false,
            },
            SyncProgress::PersistingBatch {
                count: 1,
                final_batch: true,
            },
            SyncProgress::Persisted {
                owner: "org".to_string(),
                name: "repo".to_string(),
            },
            SyncProgress::PersistError {
                owner: "org".to_string(),
                name: "repo".to_string(),
                error: "save failed".to_string(),
            },
            SyncProgress::SyncingNamespaces { count: 2 },
            SyncProgress::SyncNamespacesComplete {
                successful: 1,
                failed: 1,
            },
            SyncProgress::Warning {
                message: "careful".to_string(),
            },
            SyncProgress::RateLimitBackoff {
                owner: "org".to_string(),
                name: "repo".to_string(),
                retry_after_ms: 1000,
                attempt: 2,
            },
            SyncProgress::PruningRepos {
                count: 3,
                dry_run: false,
            },
            SyncProgress::PrunedRepo {
                owner: "org".to_string(),
                name: "repo".to_string(),
            },
            SyncProgress::PruneError {
                owner: "org".to_string(),
                name: "repo".to_string(),
                error: "prune failed".to_string(),
            },
            SyncProgress::PruningComplete {
                pruned: 1,
                errors: 1,
            },
            SyncProgress::CacheHit {
                namespace: "org/repo".to_string(),
                cached_count: 4,
            },
            SyncProgress::PageFetchRetry {
                page: 2,
                retry_after_ms: 1000,
                attempt: 1,
            },
        ];

        for event in events {
            reporter.handle(event);
        }
    }

    #[test]
    fn logging_reporter_default_constructs() {
        let reporter = LoggingReporter;
        reporter.handle(SyncProgress::Warning {
            message: "default".to_string(),
        });
    }

    #[test]
    fn logging_reporter_handles_other_fallback_variant() {
        let reporter = LoggingReporter::new();
        reporter.handle(SyncProgress::PageFetchRetry {
            page: 3,
            retry_after_ms: 250,
            attempt: 2,
        });
    }
}
