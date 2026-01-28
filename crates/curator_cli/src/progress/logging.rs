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

            _ => {}
        }
    }
}

impl Default for LoggingReporter {
    fn default() -> Self {
        Self::new()
    }
}
