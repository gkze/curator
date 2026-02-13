use std::collections::HashMap;
use std::sync::Mutex;

use curator::sync::SyncProgress;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

/// State for tracking fetch progress per organization.
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
pub struct InteractiveReporter {
    multi: MultiProgress,
    state: Mutex<ProgressState>,
}

impl InteractiveReporter {
    pub fn new() -> Self {
        Self {
            multi: MultiProgress::new(),
            state: Mutex::new(ProgressState::default()),
        }
    }

    /// Create a reporter with a hidden draw target (no terminal output).
    /// Used in tests to prevent indicatif output from leaking into test output.
    #[cfg(test)]
    fn hidden() -> Self {
        Self {
            multi: MultiProgress::with_draw_target(indicatif::ProgressDrawTarget::hidden()),
            state: Mutex::new(ProgressState::default()),
        }
    }

    /// Create the save bar with proper styling.
    /// Positions it after the filter bar if one exists.
    fn create_save_bar(&self, state: &ProgressState) -> ProgressBar {
        let pb = if state.filter_complete && state.save_total > 0 {
            // Progress bar with known total
            ProgressBar::new(state.save_total as u64)
        } else {
            // Spinner with counter (total not known yet)
            let bar = ProgressBar::new_spinner();
            bar.enable_steady_tick(std::time::Duration::from_millis(100));
            bar
        };

        // Insert after filter bar to maintain correct visual order
        let pb = if let Some(ref filter_bar) = state.filter_bar {
            self.multi.insert_after(filter_bar, pb)
        } else {
            self.multi.add(pb)
        };

        if state.filter_complete && state.save_total > 0 {
            pb.set_style(Self::bar_style());
        } else {
            pb.set_style(Self::counter_style());
        }

        pb
    }

    /// Create the filter bar with proper styling.
    /// Positions it before the save bar if one already exists.
    fn create_filter_bar(&self, state: &ProgressState, use_spinner: bool) -> ProgressBar {
        let pb = if use_spinner {
            let bar = ProgressBar::new_spinner();
            bar.enable_steady_tick(std::time::Duration::from_millis(100));
            bar
        } else {
            ProgressBar::new_spinner()
        };

        // Insert before save bar if it exists, to maintain correct visual order
        let pb = if let Some(ref save_bar) = state.save_bar {
            self.multi.insert_before(save_bar, pb)
        } else {
            self.multi.add(pb)
        };

        if use_spinner {
            pb.set_style(Self::filter_style());
        }

        pb
    }

    pub fn handle(&self, event: SyncProgress) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());

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
                namespace,
                page,
                count: _,
                total_so_far,
                expected_pages: _,
            } => {
                if let Some(fetch_state) = state.fetch_bars.get(&namespace)
                    && !fetch_state.done
                {
                    if let Some(len) = fetch_state.bar.length()
                        && page as u64 > len
                    {
                        fetch_state.bar.set_length(page as u64);
                    }
                    fetch_state.bar.set_position(page as u64);
                    fetch_state
                        .bar
                        .set_message(format!("Page {} ({} repos)", page, total_so_far));
                }
            }

            SyncProgress::FetchComplete { namespace, total } => {
                if let Some(fetch_state) = state.fetch_bars.get_mut(&namespace)
                    && !fetch_state.done
                {
                    fetch_state.fetched = total;
                    fetch_state
                        .bar
                        .set_message(format!("Fetched {} repos, filtering...", total));
                }

                // Convert filter spinner to progress bar in-place (preserves position in MultiProgress)
                if let Some(ref pb) = state.filter_bar {
                    pb.set_length(total as u64);
                    pb.set_style(Self::bar_style());
                    pb.disable_steady_tick();
                }
            }

            SyncProgress::FilteringByActivity { namespace, days } => {
                if state.filter_bar.is_none() {
                    let pb = self.create_filter_bar(&state, true);
                    pb.set_prefix(format!("{:12}", "Filtering"));
                    state.filter_bar = Some(pb);
                }
                if let Some(fetch_state) = state.fetch_bars.get(&namespace)
                    && !fetch_state.done
                {
                    fetch_state
                        .bar
                        .set_message(format!("Filtering (last {} days)...", days));
                }
            }

            SyncProgress::FilterComplete {
                namespace,
                matched,
                total,
            } => {
                // Finish fetch bar
                if let Some(fetch_state) = state.fetch_bars.get_mut(&namespace)
                    && !fetch_state.done
                {
                    fetch_state.matched = matched;
                    fetch_state.done = true;
                    fetch_state
                        .bar
                        .finish_with_message(format!("✓ {} repos fetched", total));
                }

                // Finish filter bar
                if let Some(ref pb) = state.filter_bar {
                    pb.finish_with_message(format!("✓ {}/{} active", matched, total));
                }

                // Set final save total and mark filtering as complete
                state.save_total = matched;
                state.filter_complete = true;

                // Convert save spinner to progress bar in-place (preserves position in MultiProgress)
                if matched > 0
                    && let Some(ref pb) = state.save_bar
                {
                    pb.set_length(matched as u64);
                    pb.set_style(Self::bar_style());
                    pb.disable_steady_tick();
                    let pos = pb.position();
                    pb.set_message(format!("{}/{} saved", pos, matched));
                }
            }

            SyncProgress::FilteredPage {
                matched_so_far,
                processed_so_far,
            } => {
                if state.filter_bar.is_none() {
                    let fetch_total = state
                        .fetch_bars
                        .values()
                        .find(|fs| !fs.done)
                        .and_then(|fs| {
                            fs.total_repos
                                .or_else(|| fs.bar.length().map(|pages| (pages * 100) as usize))
                        })
                        .or_else(|| {
                            state
                                .fetch_bars
                                .values()
                                .find(|fs| fs.fetched > 0)
                                .map(|fs| fs.fetched)
                        });

                    let pb = if let Some(total) = fetch_total {
                        // Create progress bar with known total, insert before save bar if exists
                        let bar = ProgressBar::new(total as u64);
                        let bar = if let Some(ref save_bar) = state.save_bar {
                            self.multi.insert_before(save_bar, bar)
                        } else {
                            self.multi.add(bar)
                        };
                        bar.set_style(Self::bar_style());
                        bar
                    } else {
                        self.create_filter_bar(&state, true)
                    };
                    pb.set_prefix(format!("{:12}", "Filtering"));
                    state.filter_bar = Some(pb);
                }

                if let Some(ref pb) = state.filter_bar {
                    if let Some(len) = pb.length()
                        && len > 0
                        && processed_so_far as u64 > len
                    {
                        pb.set_length(processed_so_far as u64);
                    }
                    if pb.length().is_some() && pb.length() != Some(0) {
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

            SyncProgress::ConvertingModels => {}

            SyncProgress::ModelsReady { count } => {
                // Create save bar with known count
                if state.save_bar.is_none() && count > 0 {
                    // Insert after filter bar if it exists
                    let pb = ProgressBar::new(count as u64);
                    let pb = if let Some(ref filter_bar) = state.filter_bar {
                        self.multi.insert_after(filter_bar, pb)
                    } else {
                        self.multi.add(pb)
                    };
                    pb.set_style(Self::bar_style());
                    pb.set_prefix(format!("{:12}", "Saving"));
                    pb.set_message("Saving to database...");
                    state.save_bar = Some(pb);
                    state.save_total = count;
                }
            }

            SyncProgress::PersistingBatch { count, final_batch } => {
                // Create save bar on first batch if not exists
                if state.save_bar.is_none() {
                    let pb = self.create_save_bar(&state);
                    pb.set_prefix(format!("{:12}", "Saving"));
                    pb.set_message("Saving to database...");
                    state.save_bar = Some(pb);
                }

                if let Some(ref pb) = state.save_bar {
                    let message = if final_batch {
                        format!("Flushing final batch ({} repos)...", count)
                    } else {
                        format!("Flushing batch ({} repos)...", count)
                    };
                    pb.set_message(message);
                }
            }

            SyncProgress::Persisted { owner, name } => {
                // Create save bar on first persist if not exists
                if state.save_bar.is_none() {
                    let pb = self.create_save_bar(&state);
                    pb.set_prefix(format!("{:12}", "Saving"));
                    pb.set_message("Saving to database...");
                    state.save_bar = Some(pb);
                }

                if let Some(ref pb) = state.save_bar {
                    pb.inc(1);

                    // Convert spinner to progress bar in-place if filter is now complete
                    if state.filter_complete && state.save_total > 0 {
                        // Set length if not already set to the correct value
                        if pb.length() != Some(state.save_total as u64) {
                            pb.set_length(state.save_total as u64);
                            pb.set_style(Self::bar_style());
                            pb.disable_steady_tick();
                        }
                        pb.set_message(format!("{}/{}", owner, name));
                    } else {
                        pb.set_message(format!("saved - {}/{}", owner, name));
                    }
                }
            }

            SyncProgress::PersistError { owner, name, error } => {
                if let Some(ref pb) = state.save_bar {
                    pb.inc(1);
                    pb.set_message(format!("✗ {}/{}: {}", owner, name, error));
                }
            }

            SyncProgress::SyncingNamespaces { count: _ } => {}

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
                drop(state);
                self.multi.println(format!("⚠ {}", message)).ok();
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
                if let Some(fetch_state) = state.fetch_bars.get_mut(&namespace) {
                    fetch_state.total_repos = Some(cached_count);
                    fetch_state.fetched = cached_count;
                    fetch_state.matched = cached_count;
                    fetch_state.done = true;
                    fetch_state
                        .bar
                        .finish_with_message(format!("✓ {} repos (cached)", cached_count));
                } else {
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
            }

            _ => {}
        }
    }

    #[allow(dead_code)]
    pub fn clear(&self) {
        self.multi.clear().ok();
    }

    pub fn finish(&self) {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
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

impl Default for InteractiveReporter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filtered_page_creates_filter_bar_and_tracks_progress() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::FetchingRepos {
            namespace: "org".to_string(),
            total_repos: Some(50),
            expected_pages: None,
        });
        reporter.handle(SyncProgress::FilteredPage {
            matched_so_far: 2,
            processed_so_far: 10,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let filter_bar = state
            .filter_bar
            .as_ref()
            .expect("filter bar should exist after filtered page");

        assert_eq!(filter_bar.length(), Some(50));
        assert_eq!(filter_bar.position(), 10);
        assert_eq!(filter_bar.message().to_string(), "2/10 active");
    }

    #[test]
    fn fetch_complete_converts_existing_filter_spinner_to_progress_bar() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::FetchingRepos {
            namespace: "org".to_string(),
            total_repos: None,
            expected_pages: None,
        });
        reporter.handle(SyncProgress::FilteringByActivity {
            namespace: "org".to_string(),
            days: 30,
        });

        {
            let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
            let filter_bar = state
                .filter_bar
                .as_ref()
                .expect("filter bar should exist after filtering starts");
            assert_eq!(filter_bar.length(), None);
        }

        reporter.handle(SyncProgress::FetchComplete {
            namespace: "org".to_string(),
            total: 42,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let filter_bar = state
            .filter_bar
            .as_ref()
            .expect("filter bar should still exist");
        assert_eq!(filter_bar.length(), Some(42));
    }

    #[test]
    fn save_spinner_is_converted_in_place_after_filter_complete() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::PersistingBatch {
            count: 5,
            final_batch: false,
        });
        reporter.handle(SyncProgress::Persisted {
            owner: "owner".to_string(),
            name: "repo".to_string(),
        });

        {
            let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
            let save_bar = state
                .save_bar
                .as_ref()
                .expect("save bar should exist after first persist");
            assert_eq!(save_bar.length(), None);
            assert_eq!(save_bar.position(), 1);
        }

        reporter.handle(SyncProgress::FilterComplete {
            namespace: "org".to_string(),
            matched: 3,
            total: 10,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let save_bar = state
            .save_bar
            .as_ref()
            .expect("save bar should remain after filter complete");

        assert!(state.filter_complete);
        assert_eq!(state.save_total, 3);
        assert_eq!(save_bar.length(), Some(3));
        assert_eq!(save_bar.position(), 1);
        assert_eq!(save_bar.message().to_string(), "1/3 saved");
    }

    #[test]
    fn filtered_page_expands_filter_length_when_processed_exceeds_estimate() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::FetchingRepos {
            namespace: "org".to_string(),
            total_repos: None,
            expected_pages: Some(1),
        });

        reporter.handle(SyncProgress::FilteredPage {
            matched_so_far: 12,
            processed_so_far: 150,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let filter_bar = state
            .filter_bar
            .as_ref()
            .expect("filter bar should exist after filtered page");

        assert_eq!(filter_bar.length(), Some(150));
        assert_eq!(filter_bar.position(), 150);
        assert_eq!(filter_bar.message().to_string(), "12/150 active");
    }

    #[test]
    fn filtered_page_uses_fetched_total_when_initial_total_is_unknown() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::FetchingRepos {
            namespace: "org".to_string(),
            total_repos: None,
            expected_pages: None,
        });
        reporter.handle(SyncProgress::FetchComplete {
            namespace: "org".to_string(),
            total: 42,
        });
        reporter.handle(SyncProgress::FilteredPage {
            matched_so_far: 5,
            processed_so_far: 10,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let filter_bar = state
            .filter_bar
            .as_ref()
            .expect("filter bar should exist after filtered page");

        assert_eq!(filter_bar.length(), Some(42));
        assert_eq!(filter_bar.position(), 10);
        assert_eq!(filter_bar.message().to_string(), "5/10 active");
    }

    #[test]
    fn cache_hit_updates_existing_fetch_state() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::FetchingRepos {
            namespace: "org".to_string(),
            total_repos: None,
            expected_pages: None,
        });
        reporter.handle(SyncProgress::CacheHit {
            namespace: "org".to_string(),
            cached_count: 7,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let fetch_state = state
            .fetch_bars
            .get("org")
            .expect("fetch state should exist for org");

        assert_eq!(fetch_state.total_repos, Some(7));
        assert_eq!(fetch_state.fetched, 7);
        assert_eq!(fetch_state.matched, 7);
        assert!(fetch_state.done);
        assert!(fetch_state.bar.is_finished());
        assert_eq!(fetch_state.bar.message().to_string(), "✓ 7 repos (cached)");
    }

    #[test]
    fn filter_complete_with_zero_matches_keeps_save_spinner_open() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::PersistingBatch {
            count: 2,
            final_batch: false,
        });
        reporter.handle(SyncProgress::Persisted {
            owner: "owner".to_string(),
            name: "repo".to_string(),
        });
        reporter.handle(SyncProgress::FilterComplete {
            namespace: "org".to_string(),
            matched: 0,
            total: 10,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let save_bar = state
            .save_bar
            .as_ref()
            .expect("save bar should still exist");

        assert!(state.filter_complete);
        assert_eq!(state.save_total, 0);
        assert_eq!(save_bar.length(), None);
        assert_eq!(save_bar.position(), 1);
    }

    #[test]
    fn models_ready_with_zero_count_does_not_create_save_bar() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::ModelsReady { count: 0 });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(state.save_bar.is_none());
        assert_eq!(state.save_total, 0);
    }

    #[test]
    fn starring_repos_accumulates_total_and_resizes_existing_bar() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::StarringRepos {
            count: 2,
            concurrency: 1,
            dry_run: false,
        });
        reporter.handle(SyncProgress::StarringRepos {
            count: 3,
            concurrency: 1,
            dry_run: false,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let star_bar = state
            .star_bar
            .as_ref()
            .expect("star bar should exist after starring starts");

        assert_eq!(state.star_total, 5);
        assert_eq!(star_bar.length(), Some(5));
        assert_eq!(star_bar.message().to_string(), "Starring...");
    }

    #[test]
    fn persist_error_advances_save_progress_and_formats_message() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::PersistingBatch {
            count: 1,
            final_batch: false,
        });
        reporter.handle(SyncProgress::PersistError {
            owner: "rust-lang".to_string(),
            name: "rust".to_string(),
            error: "constraint violation".to_string(),
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let save_bar = state
            .save_bar
            .as_ref()
            .expect("save bar should exist after persisting starts");

        assert_eq!(save_bar.position(), 1);
        assert_eq!(
            save_bar.message().to_string(),
            "✗ rust-lang/rust: constraint violation"
        );
    }

    #[test]
    fn sync_namespaces_complete_formats_success_only_message() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::PersistingBatch {
            count: 1,
            final_batch: false,
        });
        reporter.handle(SyncProgress::SyncNamespacesComplete {
            successful: 4,
            failed: 0,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let save_bar = state.save_bar.as_ref().expect("save bar should exist");

        assert!(save_bar.is_finished());
        assert_eq!(save_bar.message().to_string(), "✓ 4 orgs done");
    }

    #[test]
    fn fetched_page_does_not_update_cached_fetch_state() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::FetchingRepos {
            namespace: "org".to_string(),
            total_repos: None,
            expected_pages: Some(2),
        });
        reporter.handle(SyncProgress::CacheHit {
            namespace: "org".to_string(),
            cached_count: 6,
        });

        let cached_position = {
            let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
            state
                .fetch_bars
                .get("org")
                .expect("fetch state should exist after cache hit")
                .bar
                .position()
        };

        reporter.handle(SyncProgress::FetchedPage {
            namespace: "org".to_string(),
            page: 10,
            count: 1,
            total_so_far: 100,
            expected_pages: Some(2),
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let fetch_state = state
            .fetch_bars
            .get("org")
            .expect("fetch state should exist");

        assert!(fetch_state.done);
        assert_eq!(fetch_state.bar.position(), cached_position);
        assert_eq!(fetch_state.bar.message().to_string(), "✓ 6 repos (cached)");
    }

    #[test]
    fn cache_hit_creates_finished_fetch_state_when_missing() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::CacheHit {
            namespace: "new-org".to_string(),
            cached_count: 9,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let fetch_state = state
            .fetch_bars
            .get("new-org")
            .expect("cache hit should create state when namespace is missing");

        assert!(fetch_state.done);
        assert_eq!(fetch_state.total_repos, Some(9));
        assert_eq!(fetch_state.bar.length(), Some(1));
        assert_eq!(fetch_state.bar.position(), 1);
        assert_eq!(fetch_state.bar.message().to_string(), "✓ 9 repos (cached)");
    }

    #[test]
    fn starring_repos_uses_checking_message_for_dry_run() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::StarringRepos {
            count: 1,
            concurrency: 1,
            dry_run: true,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let star_bar = state
            .star_bar
            .as_ref()
            .expect("star bar should exist after starring starts");

        assert_eq!(star_bar.message().to_string(), "Checking...");
    }

    #[test]
    fn create_bar_helpers_cover_spinner_and_known_total_paths() {
        let reporter = InteractiveReporter::hidden();

        let mut state = ProgressState {
            save_total: 4,
            filter_complete: true,
            ..Default::default()
        };

        let save_bar = reporter.create_save_bar(&state);
        assert_eq!(save_bar.length(), Some(4));

        state.save_bar = Some(save_bar.clone());
        let filter_bar = reporter.create_filter_bar(&state, false);
        assert_eq!(filter_bar.length(), None);
    }

    #[test]
    fn finish_marks_all_active_bars_finished() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::FetchingRepos {
            namespace: "org".to_string(),
            total_repos: Some(2),
            expected_pages: Some(1),
        });
        reporter.handle(SyncProgress::FilteringByActivity {
            namespace: "org".to_string(),
            days: 7,
        });
        reporter.handle(SyncProgress::StarringRepos {
            count: 1,
            concurrency: 1,
            dry_run: false,
        });
        reporter.handle(SyncProgress::PersistingBatch {
            count: 1,
            final_batch: false,
        });

        reporter.finish();

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let fetch = state.fetch_bars.get("org").expect("fetch bar should exist");
        assert!(fetch.bar.is_finished());
        assert!(
            state
                .filter_bar
                .as_ref()
                .expect("filter bar should exist")
                .is_finished()
        );
        assert!(
            state
                .star_bar
                .as_ref()
                .expect("star bar should exist")
                .is_finished()
        );
        assert!(
            state
                .save_bar
                .as_ref()
                .expect("save bar should exist")
                .is_finished()
        );
    }
}
