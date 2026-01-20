use chrono::{DateTime, Duration, Utc};

use crate::platform::PlatformRepo;

pub(super) fn is_active_repo(repo: &PlatformRepo, cutoff: DateTime<Utc>) -> bool {
    repo.pushed_at
        .or(repo.updated_at)
        .map(|t| t > cutoff)
        .unwrap_or(false)
}

/// Filter repositories by recent activity.
///
/// Returns repositories that have been updated within the specified duration.
pub fn filter_by_activity(repos: &[PlatformRepo], active_within: Duration) -> Vec<&PlatformRepo> {
    let cutoff = Utc::now() - active_within;

    repos
        .iter()
        .filter(|repo| is_active_repo(repo, cutoff))
        .collect()
}
