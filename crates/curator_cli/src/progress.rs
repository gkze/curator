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
use std::collections::HashMap;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use std::sync::{Arc, Mutex};

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use console::Term;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use curator::sync::{ProgressCallback, SyncProgress};
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

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

/// State for tracking fetch progress per organization.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
struct FetchState {
    bar: ProgressBar,
    #[allow(dead_code)]
    total_repos: Option<usize>,
    #[allow(dead_code)]
    fetched: usize,
    #[allow(dead_code)]
    matched: usize,
    done: bool,
}

/// Consolidated progress state to avoid multiple mutex locks.
///
/// This struct groups all mutable progress state into a single unit,
/// ensuring consistent state updates and avoiding potential lock ordering issues.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
#[derive(Default)]
struct ProgressState {
    /// Fetch progress bars by org name.
    fetch_bars: HashMap<String, FetchState>,
    /// Single bar for filtering operations (streaming mode).
    filter_bar: Option<ProgressBar>,
    /// Single bar for starring operations.
    star_bar: Option<ProgressBar>,
    /// Total repos to star (accumulated from all orgs).
    star_total: usize,
    /// Separate bar for persistence.
    save_bar: Option<ProgressBar>,
    /// Final count of items to save (set by FilterComplete).
    save_total: usize,
    /// Whether filtering is complete (save total is final).
    filter_complete: bool,
}

/// Interactive progress reporter using indicatif.
///
/// Uses separate progress bars for each phase:
/// - Fetch bars: One per org, showing page fetching and filtering
/// - Star bar: Single bar for all starring operations
/// - Save bar: Single bar for database persistence
///
/// All mutable state is consolidated into a single `Mutex<ProgressState>`
/// to ensure consistent updates and avoid lock ordering issues.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub struct InteractiveReporter {
    multi: MultiProgress,
    /// Consolidated progress state under a single lock.
    state: Mutex<ProgressState>,
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
impl InteractiveReporter {
    /// Create a new interactive reporter.
    pub fn new() -> Self {
        Self {
            multi: MultiProgress::new(),
            state: Mutex::new(ProgressState::default()),
        }
    }

    /// Handle a progress event.
    ///
    /// All state access is done through a single lock on `self.state`,
    /// avoiding potential deadlocks from multiple lock acquisitions.
    pub fn handle(&self, event: SyncProgress) {
        // Acquire single lock for all state access
        let mut state = self.state.lock().unwrap();

        match event {
            SyncProgress::FetchingRepos {
                namespace,
                total_repos,
                expected_pages,
            } => {
                let pb = if let Some(pages) = expected_pages {
                    let bar = self.multi.add(ProgressBar::new(pages as u64));
                    bar.set_style(Self::bar_style());
                    bar
                } else {
                    let bar = self.multi.add(ProgressBar::new_spinner());
                    bar.set_style(Self::spinner_style());
                    bar.enable_steady_tick(std::time::Duration::from_millis(100));
                    bar
                };
                pb.set_prefix(format!("{:12}", namespace));
                let msg = match total_repos {
                    Some(total) => format!("Fetching {} repos...", total),
                    None => "Fetching repositories...".to_string(),
                };
                pb.set_message(msg);

                state.fetch_bars.insert(
                    namespace,
                    FetchState {
                        bar: pb,
                        total_repos,
                        fetched: 0,
                        matched: 0,
                        done: false,
                    },
                );
            }

            SyncProgress::FetchedPage {
                page,
                count: _,
                total_so_far,
                expected_pages: _,
            } => {
                // Find the org currently fetching (not done yet)
                for fetch_state in state.fetch_bars.values() {
                    if !fetch_state.done {
                        if let Some(len) = fetch_state.bar.length()
                            && page as u64 > len
                        {
                            fetch_state.bar.set_length(page as u64);
                        }
                        fetch_state.bar.set_position(page as u64);
                        fetch_state
                            .bar
                            .set_message(format!("Page {} ({} repos)", page, total_so_far));
                        break;
                    }
                }
            }

            SyncProgress::FetchComplete { total } => {
                for fetch_state in state.fetch_bars.values_mut() {
                    if !fetch_state.done {
                        fetch_state.fetched = total;
                        fetch_state
                            .bar
                            .set_message(format!("Fetched {} repos, filtering...", total));
                        break;
                    }
                }

                // Convert filter bar from spinner to progress bar now that we know the total
                if let Some(ref pb) = state.filter_bar {
                    pb.set_length(total as u64);
                    pb.set_style(Self::bar_style());
                    pb.disable_steady_tick();
                }
            }

            SyncProgress::FilteringByActivity { days } => {
                for fetch_state in state.fetch_bars.values() {
                    if !fetch_state.done {
                        fetch_state
                            .bar
                            .set_message(format!("Filtering (last {} days)...", days));
                        break;
                    }
                }
            }

            SyncProgress::FilterComplete { matched, total } => {
                // Finish fetch bar if it hasn't been finished yet
                for fetch_state in state.fetch_bars.values_mut() {
                    if !fetch_state.done {
                        fetch_state.matched = matched;
                        fetch_state.done = true;
                        fetch_state
                            .bar
                            .finish_with_message(format!("✓ {} repos fetched", total));
                        break;
                    }
                }

                // Finish filter bar if it exists (streaming mode)
                if let Some(ref pb) = state.filter_bar {
                    pb.finish_with_message(format!("✓ {}/{} active", matched, total));
                }

                // Set final save total and mark filtering as complete
                state.save_total = matched;
                state.filter_complete = true;

                // Update save bar to use progress bar style with final total
                if let Some(ref pb) = state.save_bar {
                    pb.set_length(matched as u64);
                    pb.set_style(Self::bar_style());
                    pb.disable_steady_tick();
                    // Force re-render to show progress bar (position may already be at length)
                    let pos = pb.position();
                    pb.set_message(format!("{}/{} saved", pos, matched));
                }
            }

            SyncProgress::FilteredPage {
                matched_so_far,
                processed_so_far,
            } => {
                // Create or update the filter bar for streaming sync (e.g., starred repos)
                if state.filter_bar.is_none() {
                    // Use a spinner since we don't know the total upfront
                    let pb = self.multi.add(ProgressBar::new_spinner());
                    pb.set_style(Self::filter_style());
                    pb.set_prefix(format!("{:12}", "Filtering"));
                    pb.enable_steady_tick(std::time::Duration::from_millis(100));
                    state.filter_bar = Some(pb);
                }

                if let Some(ref pb) = state.filter_bar {
                    // Update position if bar has a known length (converted to progress bar)
                    if pb.length().is_some() {
                        pb.set_position(processed_so_far as u64);
                    }
                    pb.set_message(format!("{}/{} active", matched_so_far, processed_so_far));
                }
            }

            SyncProgress::StarringRepos {
                count,
                concurrency: _,
                dry_run,
            } => {
                state.star_total += count;

                if state.star_bar.is_none() {
                    let pb = self.multi.add(ProgressBar::new(state.star_total as u64));
                    pb.set_style(Self::bar_style());
                    pb.set_prefix(format!("{:12}", "Starring"));
                    let action = if dry_run { "Checking" } else { "Starring" };
                    pb.set_message(format!("{}...", action));
                    state.star_bar = Some(pb);
                } else if let Some(ref pb) = state.star_bar {
                    pb.set_length(state.star_total as u64);
                }
            }

            SyncProgress::StarredRepo {
                owner,
                name,
                already_starred,
            } => {
                if let Some(ref pb) = state.star_bar {
                    pb.inc(1);
                    let symbol = if already_starred { "·" } else { "★" };
                    pb.set_message(format!("{} {}/{}", symbol, owner, name));
                }
            }

            SyncProgress::StarError { owner, name, error } => {
                if let Some(ref pb) = state.star_bar {
                    pb.inc(1);
                    pb.set_message(format!("✗ {}/{}: {}", owner, name, error));
                }
            }

            SyncProgress::StarringComplete {
                starred,
                already_starred,
                errors,
            } => {
                if let Some(ref pb) = state.star_bar {
                    let msg = if errors > 0 {
                        format!(
                            "✓ {} starred, {} skipped, {} errors",
                            starred, already_starred, errors
                        )
                    } else {
                        format!("✓ {} starred, {} skipped", starred, already_starred)
                    };
                    pb.finish_with_message(msg);
                }
            }

            SyncProgress::ConvertingModels => {
                // No-op for interactive mode
            }

            SyncProgress::ModelsReady { count } => {
                // Initialize save bar with known count (batch mode)
                if state.save_bar.is_none() {
                    let pb = self.multi.add(ProgressBar::new(count as u64));
                    pb.set_style(Self::bar_style());
                    pb.set_prefix(format!("{:12}", "Saving"));
                    pb.set_message("Saving to database...");
                    state.save_bar = Some(pb);
                    state.save_total = count;
                }
            }

            SyncProgress::Persisted { owner, name } => {
                // Create save bar on first Persisted event (streaming mode)
                if state.save_bar.is_none() {
                    // If filtering is complete, use progress bar with total
                    // Otherwise, use a spinner with counter
                    let pb = if state.filter_complete && state.save_total > 0 {
                        let bar = self.multi.add(ProgressBar::new(state.save_total as u64));
                        bar.set_style(Self::bar_style());
                        bar
                    } else {
                        let bar = self.multi.add(ProgressBar::new_spinner());
                        bar.set_style(Self::counter_style());
                        bar.enable_steady_tick(std::time::Duration::from_millis(100));
                        bar
                    };
                    pb.set_prefix(format!("{:12}", "Saving"));
                    pb.set_message("Saving to database...");
                    state.save_bar = Some(pb);
                }

                if let Some(ref pb) = state.save_bar {
                    pb.inc(1);

                    // If filter is now complete, switch to progress bar style
                    if state.filter_complete && state.save_total > 0 {
                        if pb.length() != Some(state.save_total as u64) {
                            pb.set_length(state.save_total as u64);
                            pb.set_style(Self::bar_style());
                            pb.disable_steady_tick();
                        }
                        pb.set_message(format!("{}/{}", owner, name));
                    } else {
                        // Show count without total
                        let pos = pb.position();
                        pb.set_message(format!("{} saved - {}/{}", pos, owner, name));
                    }
                }
            }

            SyncProgress::PersistError { owner, name, error } => {
                if let Some(ref pb) = state.save_bar {
                    pb.inc(1);
                    pb.set_message(format!("✗ {}/{}: {}", owner, name, error));
                }
            }

            SyncProgress::SyncingNamespaces { count } => {
                let _ = count;
            }

            SyncProgress::SyncNamespacesComplete { successful, failed } => {
                if let Some(ref pb) = state.save_bar {
                    let msg = if failed > 0 {
                        format!("✓ {} orgs done, {} failed", successful, failed)
                    } else {
                        format!("✓ {} orgs done", successful)
                    };
                    pb.finish_with_message(msg);
                }
            }

            SyncProgress::Warning { message } => {
                // Release lock before printing to avoid holding it during I/O
                drop(state);
                self.multi.println(format!("⚠ {}", message)).ok();
                // Early return - state already dropped, don't try to use it again
            }

            SyncProgress::RateLimitBackoff {
                owner,
                name,
                retry_after_ms,
                attempt,
            } => {
                if let Some(ref pb) = state.star_bar {
                    pb.set_message(format!(
                        "⏳ {}/{} rate limited, retry {} in {:.1}s",
                        owner,
                        name,
                        attempt,
                        retry_after_ms as f64 / 1000.0
                    ));
                }
            }

            SyncProgress::CacheHit {
                namespace,
                cached_count,
            } => {
                // Create a fetch bar that shows immediate completion from cache
                let pb = self.multi.add(ProgressBar::new(1));
                pb.set_style(Self::bar_style());
                pb.set_prefix(format!("{:12}", namespace));
                pb.set_position(1);
                pb.finish_with_message(format!("✓ {} repos (cached)", cached_count));

                state.fetch_bars.insert(
                    namespace,
                    FetchState {
                        bar: pb,
                        total_repos: Some(cached_count),
                        fetched: cached_count,
                        matched: cached_count,
                        done: true,
                    },
                );
            }

            _ => {}
        }
    }

    /// Clear all progress bars.
    #[allow(dead_code)]
    pub fn clear(&self) {
        self.multi.clear().ok();
    }

    /// Finish all progress bars.
    pub fn finish(&self) {
        let state = self.state.lock().unwrap();
        for fetch_state in state.fetch_bars.values() {
            if !fetch_state.bar.is_finished() {
                fetch_state.bar.finish();
            }
        }
        if let Some(ref pb) = state.filter_bar
            && !pb.is_finished()
        {
            pb.finish();
        }
        if let Some(ref pb) = state.star_bar
            && !pb.is_finished()
        {
            pb.finish();
        }
        if let Some(ref pb) = state.save_bar
            && !pb.is_finished()
        {
            pb.finish();
        }
    }

    fn spinner_style() -> ProgressStyle {
        ProgressStyle::default_spinner()
            .template("{prefix:.bold.cyan} {spinner:.green} {msg}")
            .expect("Invalid template")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn filter_style() -> ProgressStyle {
        ProgressStyle::default_spinner()
            .template("{prefix:.bold.cyan} {spinner:.yellow} {msg}")
            .expect("Invalid template")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn counter_style() -> ProgressStyle {
        ProgressStyle::default_spinner()
            .template("{prefix:.bold.cyan} {spinner:.green} {pos:>4} {msg}")
            .expect("Invalid template")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn bar_style() -> ProgressStyle {
        ProgressStyle::default_bar()
            .template("{prefix:.bold.cyan} [{bar:40.cyan/blue}] {pos:>3}/{len:3} {msg}")
            .expect("Invalid template")
            .progress_chars("█▓░")
    }
}

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
impl Default for InteractiveReporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Logging reporter using tracing for structured output.
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub struct LoggingReporter;

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
impl LoggingReporter {
    /// Create a new logging reporter.
    pub fn new() -> Self {
        Self
    }

    /// Handle a progress event.
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
                page,
                count,
                total_so_far,
                expected_pages,
            } => {
                tracing::debug!(page, count, total_so_far, expected_pages = ?expected_pages, "Fetched page");
            }

            SyncProgress::FetchComplete { total } => {
                tracing::info!(total, "Fetch complete");
            }

            SyncProgress::FilteringByActivity { days } => {
                tracing::debug!(days, "Filtering by activity");
            }

            SyncProgress::FilterComplete { matched, total } => {
                tracing::info!(matched, total, "Filtered by activity");
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

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
impl Default for LoggingReporter {
    fn default() -> Self {
        Self::new()
    }
}
