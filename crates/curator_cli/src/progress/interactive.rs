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
    /// Final count of items expected to be saved once known.
    save_total: usize,
    /// Single bar for pruning inactive starred repositories.
    prune_bar: Option<ProgressBar>,
    /// Total repositories to prune (accumulated for multiple prune batches).
    prune_total: usize,
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
        let pb = if state.save_total > 0 {
            ProgressBar::new(state.save_total as u64)
        } else {
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

        if state.save_total > 0 {
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

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ProgressState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn set_prefix(pb: &ProgressBar, prefix: &str) {
        pb.set_prefix(format!("{prefix:12}"));
    }

    fn repo_label(owner: &str, name: &str) -> String {
        format!("{owner}/{name}")
    }

    fn retry_after_seconds(retry_after_ms: u64) -> String {
        let seconds = retry_after_ms / 1000;
        let tenths = (retry_after_ms % 1000) / 100;
        format!("{seconds}.{tenths}")
    }

    fn ensure_save_bar(&self, state: &mut ProgressState) {
        if state.save_bar.is_none() {
            let pb = self.create_save_bar(state);
            Self::set_prefix(&pb, "Saving");
            pb.set_message("Saving to database...");
            state.save_bar = Some(pb);
        }
    }

    fn infer_filter_total(state: &ProgressState) -> Option<usize> {
        state
            .fetch_bars
            .values()
            .find(|fetch_state| !fetch_state.done)
            .and_then(|fetch_state| {
                fetch_state.total_repos.or_else(|| {
                    fetch_state
                        .bar
                        .length()
                        .and_then(|pages| pages.checked_mul(100))
                        .and_then(|repos| usize::try_from(repos).ok())
                })
            })
            .or_else(|| {
                state
                    .fetch_bars
                    .values()
                    .find(|fetch_state| fetch_state.fetched > 0)
                    .map(|fetch_state| fetch_state.fetched)
            })
    }

    fn handle_fetch_event(&self, state: &mut ProgressState, event: SyncProgress) {
        match event {
            SyncProgress::FetchingRepos {
                namespace,
                total_repos,
                expected_pages,
            } => self.handle_fetching_repos(state, namespace, total_repos, expected_pages),
            SyncProgress::FetchedPage {
                namespace,
                page,
                count: _,
                total_so_far,
                expected_pages: _,
            } => Self::handle_fetched_page(state, &namespace, page, total_so_far),
            SyncProgress::FetchComplete { namespace, total } => {
                Self::handle_fetch_complete(state, &namespace, total);
            }
            SyncProgress::FilteringByActivity { namespace, days } => {
                self.handle_filtering_by_activity(state, &namespace, days);
            }
            SyncProgress::FilterComplete {
                namespace,
                matched,
                total,
            } => Self::handle_filter_complete(state, &namespace, matched, total),
            SyncProgress::FilteredPage {
                matched_so_far,
                processed_so_far,
            } => self.handle_filtered_page(state, matched_so_far, processed_so_far),
            SyncProgress::CacheHit {
                namespace,
                cached_count,
            } => self.handle_cache_hit(state, namespace, cached_count),
            other => unreachable!("unexpected fetch event: {other:?}"),
        }
    }

    fn handle_fetching_repos(
        &self,
        state: &mut ProgressState,
        namespace: String,
        total_repos: Option<usize>,
        expected_pages: Option<u32>,
    ) {
        let pb = if let Some(pages) = expected_pages {
            let bar = self.multi.add(ProgressBar::new(u64::from(pages)));
            bar.set_style(Self::bar_style());
            bar
        } else {
            let bar = self.multi.add(ProgressBar::new_spinner());
            bar.set_style(Self::spinner_style());
            bar.enable_steady_tick(std::time::Duration::from_millis(100));
            bar
        };

        Self::set_prefix(&pb, &namespace);
        let message = total_repos.map_or_else(
            || "Fetching repositories...".to_string(),
            |total| format!("Fetching {total} repos..."),
        );
        pb.set_message(message);

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

    fn handle_fetched_page(
        state: &mut ProgressState,
        namespace: &str,
        page: u32,
        total_so_far: usize,
    ) {
        if let Some(fetch_state) = state.fetch_bars.get(namespace)
            && !fetch_state.done
        {
            let page = u64::from(page);
            if let Some(length) = fetch_state.bar.length()
                && page > length
            {
                fetch_state.bar.set_length(page);
            }
            fetch_state.bar.set_position(page);
            fetch_state
                .bar
                .set_message(format!("Page {page} ({total_so_far} repos)"));
        }
    }

    fn handle_fetch_complete(state: &mut ProgressState, namespace: &str, total: usize) {
        if let Some(fetch_state) = state.fetch_bars.get_mut(namespace)
            && !fetch_state.done
        {
            fetch_state.fetched = total;
            fetch_state
                .bar
                .set_message(format!("Fetched {total} repos, filtering..."));
        }

        if let Some(pb) = state.filter_bar.as_ref() {
            pb.set_length(total as u64);
            pb.set_style(Self::bar_style());
            pb.disable_steady_tick();
        }
    }

    fn handle_filtering_by_activity(&self, state: &mut ProgressState, namespace: &str, days: i64) {
        if state.filter_bar.is_none() {
            let pb = self.create_filter_bar(state, true);
            Self::set_prefix(&pb, "Filtering");
            state.filter_bar = Some(pb);
        }

        if let Some(fetch_state) = state.fetch_bars.get(namespace)
            && !fetch_state.done
        {
            fetch_state
                .bar
                .set_message(format!("Filtering (last {days} days)..."));
        }
    }

    fn handle_filter_complete(
        state: &mut ProgressState,
        namespace: &str,
        matched: usize,
        total: usize,
    ) {
        if let Some(fetch_state) = state.fetch_bars.get_mut(namespace)
            && !fetch_state.done
        {
            fetch_state.matched = matched;
            fetch_state.done = true;
            fetch_state
                .bar
                .finish_with_message(format!("✓ {total} repos fetched"));
        }

        if let Some(pb) = state.filter_bar.as_ref() {
            pb.finish_with_message(format!("✓ {matched}/{total} active"));
        }
    }

    fn handle_filtered_page(
        &self,
        state: &mut ProgressState,
        matched_so_far: usize,
        processed_so_far: usize,
    ) {
        if state.filter_bar.is_none() {
            let pb = if let Some(total) = Self::infer_filter_total(state) {
                let bar = ProgressBar::new(total as u64);
                let bar = if let Some(save_bar) = state.save_bar.as_ref() {
                    self.multi.insert_before(save_bar, bar)
                } else {
                    self.multi.add(bar)
                };
                bar.set_style(Self::bar_style());
                bar
            } else {
                self.create_filter_bar(state, true)
            };
            Self::set_prefix(&pb, "Filtering");
            state.filter_bar = Some(pb);
        }

        if let Some(pb) = state.filter_bar.as_ref() {
            if let Some(length) = pb.length()
                && length > 0
                && processed_so_far as u64 > length
            {
                pb.set_length(processed_so_far as u64);
            }
            if matches!(pb.length(), Some(length) if length > 0) {
                pb.set_position(processed_so_far as u64);
            }
            pb.set_message(format!("{matched_so_far}/{processed_so_far} active"));
        }
    }

    fn handle_cache_hit(&self, state: &mut ProgressState, namespace: String, cached_count: usize) {
        if let Some(fetch_state) = state.fetch_bars.get_mut(&namespace) {
            fetch_state.total_repos = Some(cached_count);
            fetch_state.fetched = cached_count;
            fetch_state.matched = cached_count;
            fetch_state.done = true;
            fetch_state
                .bar
                .finish_with_message(format!("✓ {cached_count} repos (cached)"));
        } else {
            let pb = self.multi.add(ProgressBar::new(1));
            pb.set_style(Self::bar_style());
            Self::set_prefix(&pb, &namespace);
            pb.set_position(1);
            pb.finish_with_message(format!("✓ {cached_count} repos (cached)"));

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

    fn handle_star_event(&self, state: &mut ProgressState, event: SyncProgress) {
        match event {
            SyncProgress::StarringRepos {
                count,
                concurrency: _,
                dry_run,
            } => self.handle_starring_repos(state, count, dry_run),
            SyncProgress::StarredRepo {
                owner,
                name,
                already_starred,
            } => Self::handle_starred_repo(state, &owner, &name, already_starred),
            SyncProgress::StarError { owner, name, error } => {
                Self::handle_star_error(state, &owner, &name, &error);
            }
            SyncProgress::StarringComplete {
                starred,
                already_starred,
                errors,
            } => Self::handle_starring_complete(state, starred, already_starred, errors),
            SyncProgress::RateLimitBackoff {
                owner,
                name,
                retry_after_ms,
                attempt,
            } => Self::handle_rate_limit_backoff(state, &owner, &name, retry_after_ms, attempt),
            other => unreachable!("unexpected star event: {other:?}"),
        }
    }

    fn handle_starring_repos(&self, state: &mut ProgressState, count: usize, dry_run: bool) {
        state.star_total += count;

        if state.star_bar.is_none() {
            let pb = self.multi.add(ProgressBar::new(state.star_total as u64));
            pb.set_style(Self::bar_style());
            Self::set_prefix(&pb, "Starring");
            let action = if dry_run { "Checking" } else { "Starring" };
            pb.set_message(format!("{action}..."));
            state.star_bar = Some(pb);
        } else if let Some(pb) = state.star_bar.as_ref() {
            pb.set_length(state.star_total as u64);
        }
    }

    fn handle_starred_repo(
        state: &mut ProgressState,
        owner: &str,
        name: &str,
        already_starred: bool,
    ) {
        if let Some(pb) = state.star_bar.as_ref() {
            pb.inc(1);
            let symbol = if already_starred { "·" } else { "★" };
            pb.set_message(format!("{symbol} {owner}/{name}"));
        }
    }

    fn handle_star_error(state: &mut ProgressState, owner: &str, name: &str, error: &str) {
        if let Some(pb) = state.star_bar.as_ref() {
            pb.inc(1);
            pb.set_message(format!("✗ {owner}/{name}: {error}"));
        }
    }

    fn handle_starring_complete(
        state: &mut ProgressState,
        starred: usize,
        already_starred: usize,
        errors: usize,
    ) {
        if let Some(pb) = state.star_bar.as_ref() {
            let message = if errors > 0 {
                format!("✓ {starred} starred, {already_starred} skipped, {errors} errors")
            } else {
                format!("✓ {starred} starred, {already_starred} skipped")
            };
            pb.finish_with_message(message);
        }
    }

    fn handle_rate_limit_backoff(
        state: &mut ProgressState,
        owner: &str,
        name: &str,
        retry_after_ms: u64,
        attempt: u32,
    ) {
        if let Some(pb) = state.star_bar.as_ref() {
            let seconds = Self::retry_after_seconds(retry_after_ms);
            pb.set_message(format!(
                "⏳ {owner}/{name} rate limited, retry {attempt} in {seconds}s"
            ));
        }
    }

    fn handle_persistence_event(&self, state: &mut ProgressState, event: SyncProgress) {
        match event {
            SyncProgress::ConvertingModels | SyncProgress::SyncingNamespaces { count: _ } => {}
            SyncProgress::ModelsReady { count } => self.handle_models_ready(state, count),
            SyncProgress::PersistingBatch { count, final_batch } => {
                self.handle_persisting_batch(state, count, final_batch);
            }
            SyncProgress::Persisted { owner, name } => self.handle_persisted(state, &owner, &name),
            SyncProgress::PersistError { owner, name, error } => {
                Self::handle_persist_error(state, &owner, &name, &error);
            }
            SyncProgress::SyncNamespacesComplete { successful, failed } => {
                Self::handle_sync_namespaces_complete(state, successful, failed);
            }
            other => unreachable!("unexpected persistence event: {other:?}"),
        }
    }

    fn handle_models_ready(&self, state: &mut ProgressState, count: usize) {
        state.save_total = count;

        if let Some(pb) = state.save_bar.as_ref() {
            if count > 0 {
                pb.set_length(count as u64);
                pb.set_style(Self::bar_style());
                pb.disable_steady_tick();

                let position = pb.position();
                if position > 0 {
                    pb.set_message(format!("{position}/{count} saved"));
                } else {
                    pb.set_message("Saving to database...");
                }
            }
        } else if count > 0 {
            self.ensure_save_bar(state);
        }
    }

    fn handle_persisting_batch(&self, state: &mut ProgressState, count: usize, final_batch: bool) {
        self.ensure_save_bar(state);

        if let Some(pb) = state.save_bar.as_ref() {
            let message = if final_batch {
                format!("Flushing final batch ({count} repos)...")
            } else {
                format!("Flushing batch ({count} repos)...")
            };
            pb.set_message(message);
        }
    }

    fn handle_persisted(&self, state: &mut ProgressState, owner: &str, name: &str) {
        self.ensure_save_bar(state);

        if let Some(pb) = state.save_bar.as_ref() {
            pb.inc(1);

            if state.save_total > 0 {
                if pb.length() != Some(state.save_total as u64) {
                    pb.set_length(state.save_total as u64);
                    pb.set_style(Self::bar_style());
                    pb.disable_steady_tick();
                }
                pb.set_message(Self::repo_label(owner, name));
            } else {
                pb.set_message(format!("saved - {owner}/{name}"));
            }
        }
    }

    fn handle_persist_error(state: &mut ProgressState, owner: &str, name: &str, error: &str) {
        if let Some(pb) = state.save_bar.as_ref() {
            pb.inc(1);
            pb.set_message(format!("✗ {owner}/{name}: {error}"));
        }
    }

    fn handle_sync_namespaces_complete(
        state: &mut ProgressState,
        successful: usize,
        failed: usize,
    ) {
        if let Some(pb) = state.save_bar.as_ref() {
            let message = if failed > 0 {
                format!("✓ {successful} orgs done, {failed} failed")
            } else {
                format!("✓ {successful} orgs done")
            };
            pb.finish_with_message(message);
        }
    }

    fn handle_prune_event(&self, state: &mut ProgressState, event: SyncProgress) {
        match event {
            SyncProgress::PruningRepos { count, dry_run } => {
                self.handle_pruning_repos(state, count, dry_run);
            }
            SyncProgress::PrunedRepo { owner, name } => {
                Self::handle_pruned_repo(state, &owner, &name);
            }
            SyncProgress::PruneError { owner, name, error } => {
                Self::handle_prune_error(state, &owner, &name, &error);
            }
            SyncProgress::PruningComplete { pruned, errors } => {
                Self::handle_pruning_complete(state, pruned, errors);
            }
            other => unreachable!("unexpected prune event: {other:?}"),
        }
    }

    fn handle_pruning_repos(&self, state: &mut ProgressState, count: usize, dry_run: bool) {
        state.prune_total += count;

        if state.prune_bar.is_none() {
            let pb = self.multi.add(ProgressBar::new(state.prune_total as u64));
            pb.set_style(Self::bar_style());
            Self::set_prefix(&pb, "Pruning");
            let action = if dry_run {
                "Checking..."
            } else {
                "Unstarring..."
            };
            pb.set_message(action);
            state.prune_bar = Some(pb);
        } else if let Some(pb) = state.prune_bar.as_ref() {
            pb.set_length(state.prune_total as u64);
        }
    }

    fn handle_pruned_repo(state: &mut ProgressState, owner: &str, name: &str) {
        if let Some(pb) = state.prune_bar.as_ref() {
            pb.inc(1);
            pb.set_message(format!("- {owner}/{name}"));
        }
    }

    fn handle_prune_error(state: &mut ProgressState, owner: &str, name: &str, error: &str) {
        if let Some(pb) = state.prune_bar.as_ref() {
            pb.inc(1);
            pb.set_message(format!("✗ {owner}/{name}: {error}"));
        }
    }

    fn handle_pruning_complete(state: &mut ProgressState, pruned: usize, errors: usize) {
        if let Some(pb) = state.prune_bar.as_ref() {
            let message = if errors > 0 {
                format!("✓ {pruned} pruned, {errors} errors")
            } else {
                format!("✓ {pruned} pruned")
            };
            pb.finish_with_message(message);
        }
    }

    fn handle_warning(&self, message: &str) {
        self.multi.println(format!("⚠ {message}")).ok();
    }

    fn handle_page_fetch_retry(&self, page: u32, retry_after_ms: u64, attempt: u32) {
        let seconds = Self::retry_after_seconds(retry_after_ms);
        self.multi
            .println(format!(
                "⏳ page {page} rate limited, retry {attempt} in {seconds}s"
            ))
            .ok();
    }

    pub fn handle(&self, event: SyncProgress) {
        match event {
            event @ SyncProgress::FetchingRepos { .. }
            | event @ SyncProgress::FetchedPage { .. }
            | event @ SyncProgress::FetchComplete { .. }
            | event @ SyncProgress::FilteringByActivity { .. }
            | event @ SyncProgress::FilterComplete { .. }
            | event @ SyncProgress::FilteredPage { .. }
            | event @ SyncProgress::CacheHit { .. } => {
                let mut state = self.lock_state();
                self.handle_fetch_event(&mut state, event);
            }
            event @ SyncProgress::StarringRepos { .. }
            | event @ SyncProgress::StarredRepo { .. }
            | event @ SyncProgress::StarError { .. }
            | event @ SyncProgress::StarringComplete { .. }
            | event @ SyncProgress::RateLimitBackoff { .. } => {
                let mut state = self.lock_state();
                self.handle_star_event(&mut state, event);
            }
            event @ SyncProgress::ConvertingModels
            | event @ SyncProgress::ModelsReady { .. }
            | event @ SyncProgress::PersistingBatch { .. }
            | event @ SyncProgress::Persisted { .. }
            | event @ SyncProgress::PersistError { .. }
            | event @ SyncProgress::SyncingNamespaces { .. }
            | event @ SyncProgress::SyncNamespacesComplete { .. } => {
                let mut state = self.lock_state();
                self.handle_persistence_event(&mut state, event);
            }
            event @ SyncProgress::PruningRepos { .. }
            | event @ SyncProgress::PrunedRepo { .. }
            | event @ SyncProgress::PruneError { .. }
            | event @ SyncProgress::PruningComplete { .. } => {
                let mut state = self.lock_state();
                self.handle_prune_event(&mut state, event);
            }
            SyncProgress::Warning { message } => self.handle_warning(&message),
            SyncProgress::PageFetchRetry {
                page,
                retry_after_ms,
                attempt,
            } => self.handle_page_fetch_retry(page, retry_after_ms, attempt),
            other => tracing::debug!(event = ?other, "Unhandled sync progress event"),
        }
    }

    #[allow(dead_code)]
    pub fn clear(&self) {
        self.multi.clear().ok();
    }

    pub fn finish(&self) {
        let state = self.lock_state();
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
        if let Some(ref pb) = state.prune_bar
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
    fn save_spinner_is_converted_in_place_after_models_ready() {
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

        reporter.handle(SyncProgress::ModelsReady { count: 3 });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let save_bar = state
            .save_bar
            .as_ref()
            .expect("save bar should remain after models ready");

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
    fn pruning_events_create_and_finish_prune_bar() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::PruningRepos {
            count: 2,
            dry_run: false,
        });
        reporter.handle(SyncProgress::PrunedRepo {
            owner: "rust-lang".to_string(),
            name: "rust".to_string(),
        });
        reporter.handle(SyncProgress::PruningComplete {
            pruned: 1,
            errors: 0,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let prune_bar = state
            .prune_bar
            .as_ref()
            .expect("prune bar should exist after pruning starts");

        assert_eq!(state.prune_total, 2);
        assert_eq!(prune_bar.length(), Some(2));
        assert_eq!(prune_bar.position(), 2);
        assert!(prune_bar.is_finished());
        assert_eq!(prune_bar.message().to_string(), "✓ 1 pruned");
    }

    #[test]
    fn create_bar_helpers_cover_spinner_and_known_total_paths() {
        let reporter = InteractiveReporter::hidden();

        let mut state = ProgressState {
            save_total: 4,
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
        reporter.handle(SyncProgress::PruningRepos {
            count: 1,
            dry_run: false,
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
        assert!(
            state
                .prune_bar
                .as_ref()
                .expect("prune bar should exist")
                .is_finished()
        );
    }

    #[test]
    fn star_and_prune_messages_cover_all_branch_variants() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::StarringRepos {
            count: 2,
            concurrency: 1,
            dry_run: false,
        });
        reporter.handle(SyncProgress::StarredRepo {
            owner: "o".to_string(),
            name: "n".to_string(),
            already_starred: false,
        });
        reporter.handle(SyncProgress::StarredRepo {
            owner: "o".to_string(),
            name: "n2".to_string(),
            already_starred: true,
        });
        reporter.handle(SyncProgress::StarError {
            owner: "o".to_string(),
            name: "n3".to_string(),
            error: "boom".to_string(),
        });
        reporter.handle(SyncProgress::RateLimitBackoff {
            owner: "o".to_string(),
            name: "n4".to_string(),
            retry_after_ms: 1500,
            attempt: 2,
        });
        reporter.handle(SyncProgress::StarringComplete {
            starred: 1,
            already_starred: 1,
            errors: 1,
        });

        reporter.handle(SyncProgress::PruningRepos {
            count: 2,
            dry_run: true,
        });
        reporter.handle(SyncProgress::PruneError {
            owner: "o".to_string(),
            name: "stale".to_string(),
            error: "deny".to_string(),
        });
        reporter.handle(SyncProgress::PruningComplete {
            pruned: 1,
            errors: 1,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(state.star_bar.as_ref().expect("star bar").is_finished());
        assert_eq!(
            state.star_bar.as_ref().unwrap().message().to_string(),
            "✓ 1 starred, 1 skipped, 1 errors"
        );
        assert!(state.prune_bar.as_ref().expect("prune bar").is_finished());
        assert_eq!(
            state.prune_bar.as_ref().unwrap().message().to_string(),
            "✓ 1 pruned, 1 errors"
        );
    }

    #[test]
    fn save_bar_paths_cover_models_ready_and_namespace_completion() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::ModelsReady { count: 2 });
        reporter.handle(SyncProgress::Persisted {
            owner: "o".to_string(),
            name: "repo".to_string(),
        });
        reporter.handle(SyncProgress::SyncNamespacesComplete {
            successful: 2,
            failed: 1,
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let save_bar = state.save_bar.as_ref().expect("save bar should exist");
        assert!(save_bar.is_finished());
        assert_eq!(save_bar.length(), Some(2));
        assert_eq!(save_bar.message().to_string(), "✓ 2 orgs done, 1 failed");
    }

    #[test]
    fn warning_and_cache_hit_paths_cover_additional_branches() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::Warning {
            message: "careful".to_string(),
        });
        reporter.handle(SyncProgress::CacheHit {
            namespace: "cached-org".to_string(),
            cached_count: 3,
        });
        reporter.handle(SyncProgress::FetchedPage {
            namespace: "cached-org".to_string(),
            page: 2,
            count: 5,
            total_so_far: 5,
            expected_pages: Some(3),
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        let fetch = state
            .fetch_bars
            .get("cached-org")
            .expect("cache hit state should exist");
        assert!(fetch.done);
        assert_eq!(fetch.bar.message().to_string(), "✓ 3 repos (cached)");
    }

    #[test]
    fn save_and_prune_progress_cover_error_messages() {
        let reporter = InteractiveReporter::hidden();

        reporter.handle(SyncProgress::PersistingBatch {
            count: 2,
            final_batch: true,
        });
        reporter.handle(SyncProgress::PersistError {
            owner: "o".to_string(),
            name: "r".to_string(),
            error: "db".to_string(),
        });
        reporter.handle(SyncProgress::PruningRepos {
            count: 1,
            dry_run: false,
        });
        reporter.handle(SyncProgress::PruneError {
            owner: "o".to_string(),
            name: "old".to_string(),
            error: "api".to_string(),
        });

        let state = reporter.state.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            state.save_bar.as_ref().unwrap().message().to_string(),
            "✗ o/r: db"
        );
        assert_eq!(
            state.prune_bar.as_ref().unwrap().message().to_string(),
            "✗ o/old: api"
        );
    }
}
