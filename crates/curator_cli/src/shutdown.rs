use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use console::Term;

/// Global shutdown flag for graceful termination.
/// Uses Arc so it can be shared with spawned tasks in the library.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) static SHUTDOWN_FLAG: std::sync::LazyLock<Arc<AtomicBool>> =
    std::sync::LazyLock::new(|| Arc::new(AtomicBool::new(false)));

/// Check if shutdown has been requested.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[inline]
pub(crate) fn is_shutdown_requested() -> bool {
    SHUTDOWN_FLAG.load(Ordering::Acquire)
}

/// Request shutdown.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[inline]
fn request_shutdown() {
    SHUTDOWN_FLAG.store(true, Ordering::Release);
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
