//! Robust persist task for streaming repository models to the database.
//!
//! This module provides a background task that receives repository models via a channel
//! and persists them in batches using bulk upserts. Key features:
//!
//! - **Time-based flushing**: Flushes partial batches after a timeout to prevent deadlocks
//! - **Automatic retry**: Retries transient database errors with exponential backoff
//! - **Graceful shutdown**: Processes current batch and exits cleanly on shutdown signal
//! - **Error accumulation**: Collects all errors for reporting after sync completes
//!
//! # Architecture
//!
//! The persist task is designed to work in a streaming pipeline:
//!
//! ```text
//! API Fetch → Filter/Process → model_tx channel → Persist Task → Database
//! ```
//!
//! The time-based flushing is critical for preventing deadlocks. Without it, the persist
//! task would wait indefinitely for a full batch, blocking the channel and causing
//! upstream tasks to block as well.
//!
//! # Example
//!
//! ```ignore
//! use curator::sync::persist_task::{spawn_persist_task, await_persist_task};
//! use tokio::sync::mpsc;
//!
//! let (tx, rx) = mpsc::channel(500);
//! let (handle, counter) = spawn_persist_task(db, rx, Some(progress_callback));
//!
//! // Send models through tx...
//! drop(tx); // Signal completion
//!
//! let result = await_persist_task(handle).await;
//! println!("Saved {} repos", result.saved_count);
//! ```

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use sea_orm::DatabaseConnection;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};

use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::repository;

use super::progress::{ProgressCallback, SyncProgress};

/// Batch size for bulk database upserts.
/// Balances memory usage with database round-trip efficiency.
pub const PERSIST_BATCH_SIZE: usize = 100;

/// Maximum time to wait before flushing a partial batch.
/// This prevents deadlocks when the upstream pipeline blocks waiting for channel capacity.
/// A partial batch blocking for too long can cause backpressure that blocks the fetch task,
/// which prevents channels from closing, which prevents the persist task from flushing.
pub const PERSIST_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// Number of retry attempts for database writes.
pub const PERSIST_RETRY_ATTEMPTS: u32 = 3;

/// Initial backoff delay in milliseconds for database write retries.
pub const PERSIST_RETRY_BACKOFF_MS: u64 = 100;

/// Channel buffer size for streaming repository models.
/// Larger buffers reduce backpressure likelihood. The time-based flushing in the persist
/// task is the primary deadlock prevention, but larger buffers help avoid the situation.
pub const MODEL_CHANNEL_BUFFER_SIZE: usize = 500;

/// Result of a persist task, including saved count and any errors encountered.
#[derive(Debug, Default)]
pub struct PersistTaskResult {
    /// Number of repositories successfully saved to database.
    pub saved_count: usize,
    /// Accumulated errors: (owner, name, error_message).
    pub errors: Vec<(String, String, String)>,
    /// Panic message if the task panicked.
    pub panic_info: Option<String>,
}

impl PersistTaskResult {
    /// Check if there were any errors during persistence.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty() || self.panic_info.is_some()
    }

    /// Get the total number of failed items.
    pub fn failed_count(&self) -> usize {
        self.errors.len() + usize::from(self.panic_info.is_some())
    }
}

/// Spawn a task that persists repository models from a channel using batch upserts.
///
/// Models are collected into batches using `chunks_timeout` which automatically handles:
/// - Collecting up to `PERSIST_BATCH_SIZE` items per batch
/// - Flushing after `PERSIST_FLUSH_TIMEOUT` if we have any pending items
/// - Ending when the channel is closed
///
/// This prevents deadlocks: if the upstream pipeline blocks waiting for channel capacity,
/// partial batches are flushed after the timeout to relieve backpressure.
///
/// Features:
/// - Automatic retry with exponential backoff for transient database errors
/// - Accumulates all errors for reporting after sync completes
/// - Captures panics and includes them in the result
///
/// The task respects the provided shutdown flag:
/// - On shutdown, it processes the current batch then exits
/// - This ensures no data loss during graceful shutdown
///
/// Returns the task handle and a counter for tracking saved count in real-time.
pub fn spawn_persist_task(
    db: Arc<DatabaseConnection>,
    rx: mpsc::Receiver<CodeRepositoryActiveModel>,
    shutdown_flag: Option<Arc<AtomicBool>>,
    on_progress: Option<Arc<ProgressCallback>>,
) -> (tokio::task::JoinHandle<PersistTaskResult>, Arc<AtomicUsize>) {
    let saved_count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&saved_count);

    let handle = tokio::spawn(async move {
        let mut result = PersistTaskResult::default();

        // Convert the receiver to a stream that yields batches.
        // chunks_timeout collects up to PERSIST_BATCH_SIZE items, OR flushes after
        // PERSIST_FLUSH_TIMEOUT if we have any pending items. This prevents deadlocks
        // by ensuring partial batches don't block forever waiting for more items.
        let stream = ReceiverStream::new(rx);
        let batched = stream.chunks_timeout(PERSIST_BATCH_SIZE, PERSIST_FLUSH_TIMEOUT);
        tokio::pin!(batched);

        while let Some(batch) = batched.next().await {
            // Check for shutdown between batches
            let shutdown_requested = shutdown_flag
                .as_ref()
                .is_some_and(|f| f.load(Ordering::Relaxed));

            // Extract names for progress reporting before consuming the batch
            let batch_names: Vec<(String, String)> = batch
                .iter()
                .map(|model| {
                    // SAFETY: ActiveModel always has owner/name set by to_active_model()
                    (model.owner.clone().unwrap(), model.name.clone().unwrap())
                })
                .collect();

            let batch_size = batch.len();
            let is_final = shutdown_requested;

            if batch_size > 0
                && let Some(cb) = &on_progress
            {
                cb(SyncProgress::PersistingBatch {
                    count: batch_size,
                    final_batch: is_final,
                });
            }

            tracing::debug!(batch_size, is_final, "Flushing batch");

            let flush_start = std::time::Instant::now();
            match repository::bulk_upsert_with_retry(
                &db,
                batch,
                PERSIST_RETRY_ATTEMPTS,
                PERSIST_RETRY_BACKOFF_MS,
            )
            .await
            {
                Ok(rows_affected) => {
                    let elapsed = flush_start.elapsed();
                    tracing::debug!(
                        batch_size,
                        final_batch = is_final,
                        elapsed_ms = elapsed.as_millis(),
                        "Persisted batch"
                    );
                    let count = rows_affected as usize;
                    result.saved_count += count;
                    saved_count.fetch_add(count, Ordering::Relaxed);
                    // Report progress for actually persisted items
                    if let Some(cb) = &on_progress {
                        for (owner, name) in batch_names.into_iter().take(count) {
                            cb(SyncProgress::Persisted { owner, name });
                        }
                    }
                }
                Err(e) => {
                    let elapsed = flush_start.elapsed();
                    tracing::warn!(
                        batch_size,
                        final_batch = is_final,
                        elapsed_ms = elapsed.as_millis(),
                        error = %e,
                        "Failed to persist batch"
                    );
                    let error = e.to_string();
                    // Accumulate errors for later reporting
                    for (owner, name) in &batch_names {
                        result
                            .errors
                            .push((owner.clone(), name.clone(), error.clone()));
                    }
                    // Also emit progress events for real-time feedback
                    if let Some(cb) = &on_progress {
                        for (owner, name) in batch_names {
                            cb(SyncProgress::PersistError {
                                owner,
                                name,
                                error: error.clone(),
                            });
                        }
                    }
                }
            }

            // Exit early if shutdown was requested
            if shutdown_requested {
                tracing::debug!("Shutdown requested, exiting persist task");
                break;
            }
        }

        tracing::debug!("Persist stream ended");
        result
    });

    (handle, counter)
}

/// Await the persist task handle and capture any panic information.
///
/// If the task panicked, returns a PersistTaskResult with panic_info set.
pub async fn await_persist_task(
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
pub fn display_persist_errors(result: &PersistTaskResult, is_tty: bool) {
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

/// Create a channel pair for streaming repository models.
///
/// Returns `(sender, receiver)` with the recommended buffer size.
pub fn create_model_channel() -> (
    mpsc::Sender<CodeRepositoryActiveModel>,
    mpsc::Receiver<CodeRepositoryActiveModel>,
) {
    mpsc::channel(MODEL_CHANNEL_BUFFER_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_persist_task_result_default() {
        let result = PersistTaskResult::default();
        assert_eq!(result.saved_count, 0);
        assert!(result.errors.is_empty());
        assert!(result.panic_info.is_none());
        assert!(!result.has_errors());
        assert_eq!(result.failed_count(), 0);
    }

    #[test]
    fn test_persist_task_result_with_errors() {
        let result = PersistTaskResult {
            saved_count: 10,
            errors: vec![
                (
                    "owner1".to_string(),
                    "repo1".to_string(),
                    "error1".to_string(),
                ),
                (
                    "owner2".to_string(),
                    "repo2".to_string(),
                    "error2".to_string(),
                ),
            ],
            panic_info: None,
        };
        assert!(result.has_errors());
        assert_eq!(result.failed_count(), 2);
    }

    #[test]
    fn test_persist_task_result_with_panic() {
        let result = PersistTaskResult {
            saved_count: 5,
            errors: vec![],
            panic_info: Some("task panicked".to_string()),
        };
        assert!(result.has_errors());
        assert_eq!(result.failed_count(), 1);
    }

    #[test]
    fn test_persist_task_result_with_errors_and_panic() {
        let result = PersistTaskResult {
            saved_count: 0,
            errors: vec![("o".to_string(), "r".to_string(), "e".to_string())],
            panic_info: Some("panic".to_string()),
        };
        assert!(result.has_errors());
        assert_eq!(result.failed_count(), 2);
    }

    #[test]
    fn test_constants() {
        assert_eq!(PERSIST_BATCH_SIZE, 100);
        assert_eq!(PERSIST_FLUSH_TIMEOUT, std::time::Duration::from_millis(500));
        assert_eq!(PERSIST_RETRY_ATTEMPTS, 3);
        assert_eq!(PERSIST_RETRY_BACKOFF_MS, 100);
        assert_eq!(MODEL_CHANNEL_BUFFER_SIZE, 500);
    }

    #[test]
    fn test_create_model_channel() {
        let (tx, _rx) = create_model_channel();
        // Verify the channel was created (tx is usable)
        assert!(!tx.is_closed());
    }
}
