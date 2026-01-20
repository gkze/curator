use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use curator::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use curator::platform::{ApiRateLimiter, PlatformClient};
use curator::repository;
use curator::sync::{ProgressCallback, SyncProgress};
use sea_orm::DatabaseConnection;
use tokio::sync::mpsc;

use crate::shutdown::is_shutdown_requested;

/// Batch size for bulk database upserts.
/// Balances memory usage with database round-trip efficiency.
const PERSIST_BATCH_SIZE: usize = 100;

/// Number of retry attempts for database writes.
const PERSIST_RETRY_ATTEMPTS: u32 = 3;

/// Initial backoff delay in milliseconds for database write retries.
const PERSIST_RETRY_BACKOFF_MS: u64 = 100;

/// Result of a persist task, including saved count and any errors encountered.
#[derive(Debug, Default)]
pub(crate) struct PersistTaskResult {
    /// Number of repositories successfully saved to database.
    pub(crate) saved_count: usize,
    /// Accumulated errors: (owner, name, error_message).
    pub(crate) errors: Vec<(String, String, String)>,
    /// Panic message if the task panicked.
    pub(crate) panic_info: Option<String>,
}

impl PersistTaskResult {
    /// Check if there were any errors during persistence.
    pub(crate) fn has_errors(&self) -> bool {
        !self.errors.is_empty() || self.panic_info.is_some()
    }

    /// Get the total number of failed items.
    pub(crate) fn failed_count(&self) -> usize {
        self.errors.len() + usize::from(self.panic_info.is_some())
    }
}

/// Spawn a task that persists repository models from a channel using batch upserts.
///
/// Models are collected into batches and persisted using bulk upsert with ON CONFLICT,
/// which is significantly faster than individual upserts (1 query per batch vs 2n queries).
///
/// Features:
/// - Automatic retry with exponential backoff for transient database errors
/// - Accumulates all errors for reporting after sync completes
/// - Captures panics and includes them in the result
///
/// The task respects the global shutdown flag:
/// - On shutdown, it flushes any pending batch before exiting
/// - This ensures no data loss during graceful shutdown
///
/// Returns the task handle and a counter for tracking saved count in real-time.
pub(crate) fn spawn_persist_task(
    db: Arc<DatabaseConnection>,
    mut rx: mpsc::Receiver<CodeRepositoryActiveModel>,
    on_progress: Option<Arc<ProgressCallback>>,
) -> (tokio::task::JoinHandle<PersistTaskResult>, Arc<AtomicUsize>) {
    let saved_count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&saved_count);

    let handle = tokio::spawn(async move {
        let mut result = PersistTaskResult::default();
        let mut batch: Vec<CodeRepositoryActiveModel> = Vec::with_capacity(PERSIST_BATCH_SIZE);
        let mut batch_names: Vec<(String, String)> = Vec::with_capacity(PERSIST_BATCH_SIZE);

        // Macro to flush a batch with retry logic (avoids lifetime issues with closures)
        macro_rules! flush_batch {
            ($models:expr, $names:expr, $final_batch:expr) => {{
                let batch_size = $names.len();
                if batch_size > 0 {
                    if let Some(cb) = &on_progress {
                        cb(SyncProgress::PersistingBatch {
                            count: batch_size,
                            final_batch: $final_batch,
                        });
                    }
                }

                let flush_start = std::time::Instant::now();
                match repository::bulk_upsert_with_retry(
                    &db,
                    $models,
                    PERSIST_RETRY_ATTEMPTS,
                    PERSIST_RETRY_BACKOFF_MS,
                )
                .await
                {
                    Ok(rows_affected) => {
                        let elapsed = flush_start.elapsed();
                        tracing::debug!(
                            batch_size,
                            final_batch = $final_batch,
                            elapsed_ms = elapsed.as_millis(),
                            "Persisted batch"
                        );
                        let count = rows_affected as usize;
                        result.saved_count += count;
                        saved_count.fetch_add(count, Ordering::Relaxed);
                        // Report progress for actually persisted items
                        if let Some(cb) = &on_progress {
                            for (owner, name) in $names.into_iter().take(count) {
                                cb(SyncProgress::Persisted { owner, name });
                            }
                        }
                    }
                    Err(e) => {
                        let elapsed = flush_start.elapsed();
                        tracing::warn!(
                            batch_size,
                            final_batch = $final_batch,
                            elapsed_ms = elapsed.as_millis(),
                            error = %e,
                            "Failed to persist batch"
                        );
                        let error = e.to_string();
                        // Accumulate errors for later reporting
                        for (owner, name) in &$names {
                            result
                                .errors
                                .push((owner.clone(), name.clone(), error.clone()));
                        }
                        // Also emit progress events for real-time feedback
                        if let Some(cb) = &on_progress {
                            for (owner, name) in $names {
                                cb(SyncProgress::PersistError {
                                    owner,
                                    name,
                                    error: error.clone(),
                                });
                            }
                        }
                    }
                }
            }};
        }

        loop {
            // Check for shutdown - if requested, drain remaining items and exit
            if is_shutdown_requested() {
                // Drain any remaining items from the channel
                while let Ok(model) = rx.try_recv() {
                    // SAFETY: ActiveModel always has owner/name set by to_active_model()
                    let owner = model.owner.clone().unwrap();
                    let name = model.name.clone().unwrap();
                    batch_names.push((owner, name));
                    batch.push(model);
                }
                // Flush final batch
                if !batch.is_empty() {
                    let names = std::mem::take(&mut batch_names);
                    let models = std::mem::take(&mut batch);
                    flush_batch!(models, names, true);
                }
                break;
            }

            let model = rx.recv().await;
            let channel_closed = model.is_none();

            if channel_closed {
                tracing::debug!("Persist channel closed, flushing final batch");
            }

            // Add to batch if we received a model
            if let Some(model) = model {
                // SAFETY: ActiveModel always has owner/name set by to_active_model()
                let owner = model.owner.clone().unwrap();
                let name = model.name.clone().unwrap();
                tracing::trace!(owner = %owner, name = %name, batch_len = batch.len(), "Received model");
                batch_names.push((owner, name));
                batch.push(model);
            }

            // Flush when batch is full OR channel is closed
            let should_flush = batch.len() >= PERSIST_BATCH_SIZE || channel_closed;
            if should_flush && !batch.is_empty() {
                tracing::debug!(batch_len = batch.len(), channel_closed, "Flushing batch");
            }

            if should_flush && !batch.is_empty() {
                let names = std::mem::take(&mut batch_names);
                let models = std::mem::take(&mut batch);
                flush_batch!(models, names, channel_closed);
            }

            // Exit loop when channel is closed
            if channel_closed {
                break;
            }
        }

        result
    });

    (handle, counter)
}

/// Await the persist task handle and capture any panic information.
///
/// If the task panicked, returns a PersistTaskResult with panic_info set.
pub(crate) async fn await_persist_task(
    handle: tokio::task::JoinHandle<PersistTaskResult>,
) -> PersistTaskResult {
    match handle.await {
        Ok(result) => result,
        Err(e) => {
            // Task panicked or was cancelled
            let panic_info = if e.is_panic() {
                // Try to extract panic message
                let panic_payload = e.into_panic();
                if let Some(s) = panic_payload.downcast_ref::<&str>() {
                    Some((*s).to_string())
                } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                    Some(s.clone())
                } else {
                    Some("Unknown panic".to_string())
                }
            } else if e.is_cancelled() {
                Some("Task was cancelled".to_string())
            } else {
                Some(format!("Task failed: {}", e))
            };

            PersistTaskResult {
                saved_count: 0,
                errors: Vec::new(),
                panic_info,
            }
        }
    }
}

/// Display persist errors and panic info to the user.
///
/// This function outputs accumulated errors in a visible way so users
/// know exactly what failed to save to the database.
pub(crate) fn display_persist_errors(result: &PersistTaskResult, is_tty: bool) {
    if result.panic_info.is_some() || !result.errors.is_empty() {
        if is_tty {
            println!();
        }

        // Display panic info first (most severe)
        if let Some(ref panic) = result.panic_info {
            if is_tty {
                eprintln!("\x1b[1;31mPersist task crashed: {}\x1b[0m", panic);
                eprintln!("  Some repositories may not have been saved to the database.");
            } else {
                tracing::error!(panic = %panic, "Persist task crashed - some repos may not be saved");
            }
        }

        // Display individual errors (limited to first 10 to avoid flooding)
        if !result.errors.is_empty() {
            let total_errors = result.errors.len();
            let display_count = std::cmp::min(10, total_errors);

            if is_tty {
                eprintln!(
                    "\x1b[1;33mDatabase write errors ({} total):\x1b[0m",
                    total_errors
                );
                for (owner, name, error) in result.errors.iter().take(display_count) {
                    eprintln!("  - {}/{}: {}", owner, name, error);
                }
                if total_errors > display_count {
                    eprintln!("  ... and {} more errors", total_errors - display_count);
                }
            } else {
                for (owner, name, error) in result.errors.iter().take(display_count) {
                    tracing::error!(
                        repo = %format!("{}/{}", owner, name),
                        error = %error,
                        "Failed to save to database"
                    );
                }
                if total_errors > display_count {
                    tracing::error!(
                        additional_errors = total_errors - display_count,
                        "Additional database write errors occurred"
                    );
                }
            }
        }
    }
}

/// Create a rate limiter if rate limiting is enabled.
/// Returns None if no_rate_limit is true, Some(limiter) otherwise.
pub(crate) fn maybe_rate_limiter(no_rate_limit: bool, rps: u32) -> Option<ApiRateLimiter> {
    if no_rate_limit {
        None
    } else {
        Some(ApiRateLimiter::new(rps))
    }
}

/// Print a warning when rate limiting is disabled (TTY only).
pub(crate) fn warn_no_rate_limit(is_tty: bool) {
    if is_tty {
        eprintln!("Warning: Rate limiting disabled - you may experience API throttling\n");
    }
}

/// Display final rate limit status with a timeout to avoid hangs.
pub(crate) async fn display_final_rate_limit<C: PlatformClient>(
    client: &C,
    is_tty: bool,
    no_rate_limit: bool,
) {
    if no_rate_limit {
        return;
    }

    let rate_limit =
        tokio::time::timeout(std::time::Duration::from_secs(5), client.get_rate_limit()).await;

    match rate_limit {
        Ok(Ok(final_rate)) => {
            if is_tty {
                println!(
                    "\nRate limit after sync: {}/{} remaining",
                    final_rate.remaining, final_rate.limit
                );
            } else {
                tracing::info!(
                    remaining = final_rate.remaining,
                    limit = final_rate.limit,
                    "Rate limit after sync"
                );
            }
        }
        Ok(Err(error)) => {
            if is_tty {
                eprintln!("Warning: Failed to fetch rate limit after sync: {error}");
            } else {
                tracing::warn!(error = %error, "Failed to fetch rate limit after sync");
            }
        }
        Err(_) => {
            if is_tty {
                eprintln!("Warning: Timed out fetching rate limit after sync");
            } else {
                tracing::warn!("Timed out fetching rate limit after sync");
            }
        }
    }
}
