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
