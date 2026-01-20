//! Progress reporting for sync operations.
//!
//! This module provides two modes of progress reporting:
//! - Interactive mode (TTY): Animated progress bars using indicatif
//! - Logging mode (non-TTY): Structured logging using tracing
//!
//! Progress bars are organized as:
//! - Fetch bar(s): One per namespace, showing page fetching progress
//! - Star bar: Single bar for all starring operations
//! - Save bar: Single bar for database persistence

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
mod interactive;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
mod logging;

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use std::sync::Arc;

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use console::Term;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use curator::sync::{ProgressCallback, SyncProgress};

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub use interactive::InteractiveReporter;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub use logging::LoggingReporter;

/// Progress reporter that handles both interactive and logging modes.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub enum ProgressReporter {
    /// Interactive progress bars for TTY.
    Interactive(InteractiveReporter),
    /// Structured logging for non-TTY (CI, pipes).
    Logging(LoggingReporter),
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
impl ProgressReporter {
    /// Create a new progress reporter, auto-detecting TTY mode.
    pub fn new() -> Self {
        if Term::stdout().is_term() {
            Self::Interactive(InteractiveReporter::new())
        } else {
            Self::Logging(LoggingReporter::new())
        }
    }

    /// Create an interactive reporter (for testing or forcing TTY mode).
    #[allow(dead_code)]
    pub fn interactive() -> Self {
        Self::Interactive(InteractiveReporter::new())
    }

    /// Create a logging reporter (for testing or forcing non-TTY mode).
    #[allow(dead_code)]
    pub fn logging() -> Self {
        Self::Logging(LoggingReporter::new())
    }

    /// Handle a progress event.
    pub fn handle(&self, event: SyncProgress) {
        match self {
            Self::Interactive(r) => r.handle(event),
            Self::Logging(r) => r.handle(event),
        }
    }

    /// Convert to a ProgressCallback for the library.
    ///
    /// Works with the unified `SyncProgress` type from curator::sync.
    pub fn as_callback(self: &Arc<Self>) -> Arc<ProgressCallback> {
        let reporter = Arc::clone(self);
        Arc::new(Box::new(move |event| {
            reporter.handle(event);
        }))
    }

    /// Clear all progress bars (interactive mode only).
    #[allow(dead_code)]
    pub fn clear(&self) {
        if let Self::Interactive(r) = self {
            r.clear();
        }
    }

    /// Finish all progress bars (interactive mode only).
    pub fn finish(&self) {
        if let Self::Interactive(r) = self {
            r.finish();
        }
    }
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
impl Default for ProgressReporter {
    fn default() -> Self {
        Self::new()
    }
}
