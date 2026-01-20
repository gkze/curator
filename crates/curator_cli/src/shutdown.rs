use std::sync::atomic::{AtomicBool, Ordering};

use console::Term;

/// Global shutdown flag for graceful termination.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Check if shutdown has been requested.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[inline]
pub(crate) fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::Acquire)
}

/// Request shutdown.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[inline]
fn request_shutdown() {
    SHUTDOWN_REQUESTED.store(true, Ordering::Release);
}

/// Set up the Ctrl+C handler for graceful shutdown.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) fn setup_shutdown_handler() {
    tokio::spawn(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");

        let is_tty = Term::stdout().is_term();
        if is_tty {
            eprintln!("\n\nShutdown requested, finishing current operations...");
            eprintln!("Press Ctrl+C again to force quit.");
        } else {
            tracing::warn!("Shutdown requested, finishing current operations");
        }

        request_shutdown();

        // Wait for second Ctrl+C for force quit
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install second Ctrl+C handler");

        if is_tty {
            eprintln!("Force quit!");
        }
        std::process::exit(130);
    });
}
