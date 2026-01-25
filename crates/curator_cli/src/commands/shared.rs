//! Shared utilities for CLI command handlers.
//!
//! This module provides CLI-specific wrappers around the library's sync infrastructure,
//! injecting CLI concerns like the global shutdown flag.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize},
};

use curator::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use curator::platform::{ApiRateLimiter, PlatformClient};
use curator::sync::ProgressCallback;
use sea_orm::DatabaseConnection;
use tokio::sync::mpsc;

use crate::shutdown::SHUTDOWN_FLAG;

// Re-export types from the library for convenience
pub use curator::sync::{
    MODEL_CHANNEL_BUFFER_SIZE, PersistTaskResult, await_persist_task, display_persist_errors,
};

/// Spawn a persist task with the CLI's global shutdown flag.
///
/// This is a thin wrapper around the library's `spawn_persist_task` that injects
/// the CLI's global shutdown flag for graceful shutdown handling.
pub(crate) fn spawn_persist_task(
    db: Arc<DatabaseConnection>,
    rx: mpsc::Receiver<CodeRepositoryActiveModel>,
    on_progress: Option<Arc<ProgressCallback>>,
) -> (tokio::task::JoinHandle<PersistTaskResult>, Arc<AtomicUsize>) {
    // Get the global shutdown flag
    let shutdown_flag: Arc<AtomicBool> = Arc::clone(&SHUTDOWN_FLAG);

    curator::sync::spawn_persist_task(db, rx, Some(shutdown_flag), on_progress)
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
