use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use crate::platform::PlatformRepo;
use crate::repository::RepoSyncInfo;

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

/// Check if a repository needs to be refreshed based on sync info.
///
/// A repository needs refresh if:
/// - It's not in the sync info (new repo)
/// - Its `updated_at` or `pushed_at` is newer than `synced_at`
///
/// Returns `true` if the repository should be re-fetched/processed.
pub fn needs_refresh(repo: &PlatformRepo, sync_info: Option<&RepoSyncInfo>) -> bool {
    match sync_info {
        // New repo - not in DB yet, needs processing
        None => true,
        Some(info) => {
            // Get the most recent activity timestamp from the platform
            let platform_latest = repo.pushed_at.or(repo.updated_at);

            match platform_latest {
                // No timestamp from platform - be safe, refresh it
                None => true,
                // Compare platform's latest with our last sync
                Some(latest) => latest > info.synced_at,
            }
        }
    }
}

/// Filter repositories for incremental sync.
///
/// Given a list of repos from the platform and existing sync info from the database,
/// returns only the repos that have been updated since the last sync.
///
/// This enables incremental sync by skipping repos that haven't changed.
///
/// # Arguments
///
/// * `repos` - All repositories fetched from the platform
/// * `sync_info` - Existing sync information from the database (keyed by platform_id)
///
/// # Returns
///
/// A vector of repos that need to be refreshed (new or updated since last sync).
pub fn filter_for_incremental_sync<'a>(
    repos: &'a [PlatformRepo],
    sync_info: &[RepoSyncInfo],
) -> Vec<&'a PlatformRepo> {
    // Build a lookup map from platform_id to sync info for O(1) lookups
    let sync_map: HashMap<i64, &RepoSyncInfo> = sync_info
        .iter()
        .map(|info| (info.platform_id, info))
        .collect();

    repos
        .iter()
        .filter(|repo| needs_refresh(repo, sync_map.get(&repo.platform_id).copied()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::code_visibility::CodeVisibility;
    use chrono::Duration as ChronoDuration;

    fn make_repo(platform_id: i64, name: &str, updated_at: Option<DateTime<Utc>>) -> PlatformRepo {
        PlatformRepo {
            platform_id,
            owner: "test-owner".to_string(),
            name: name.to_string(),
            description: None,
            default_branch: "main".to_string(),
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_archived: false,
            stars: None,
            forks: None,
            language: None,
            topics: vec![],
            created_at: None,
            updated_at,
            pushed_at: None,
            license: None,
            homepage: None,
            size_kb: None,
            metadata: serde_json::json!({}),
        }
    }

    fn make_sync_info(platform_id: i64, name: &str, synced_at: DateTime<Utc>) -> RepoSyncInfo {
        RepoSyncInfo {
            platform_id,
            name: name.to_string(),
            updated_at: None,
            pushed_at: None,
            synced_at,
        }
    }

    #[test]
    fn test_needs_refresh_new_repo() {
        let repo = make_repo(123, "new-repo", Some(Utc::now()));
        assert!(needs_refresh(&repo, None));
    }

    #[test]
    fn test_needs_refresh_unchanged_repo() {
        let synced_at = Utc::now();
        let updated_at = synced_at - ChronoDuration::hours(1);

        let repo = make_repo(123, "unchanged", Some(updated_at));
        let info = make_sync_info(123, "unchanged", synced_at);

        assert!(!needs_refresh(&repo, Some(&info)));
    }

    #[test]
    fn test_needs_refresh_updated_repo() {
        let synced_at = Utc::now() - ChronoDuration::hours(2);
        let updated_at = Utc::now() - ChronoDuration::hours(1);

        let repo = make_repo(123, "updated", Some(updated_at));
        let info = make_sync_info(123, "updated", synced_at);

        assert!(needs_refresh(&repo, Some(&info)));
    }

    #[test]
    fn test_needs_refresh_no_timestamp() {
        let synced_at = Utc::now();

        let repo = make_repo(123, "no-time", None);
        let info = make_sync_info(123, "no-time", synced_at);

        // No timestamp from platform means we should refresh to be safe
        assert!(needs_refresh(&repo, Some(&info)));
    }

    #[test]
    fn test_filter_for_incremental_sync() {
        let now = Utc::now();
        let old_sync = now - ChronoDuration::days(1);

        let repos = vec![
            make_repo(1, "new-repo", Some(now)), // New, not in DB
            make_repo(2, "unchanged", Some(old_sync - ChronoDuration::hours(1))), // Older than sync
            make_repo(3, "updated", Some(now - ChronoDuration::hours(1))), // Newer than sync
        ];

        let sync_info = vec![
            make_sync_info(2, "unchanged", old_sync),
            make_sync_info(3, "updated", old_sync),
        ];

        let filtered = filter_for_incremental_sync(&repos, &sync_info);

        assert_eq!(filtered.len(), 2);
        let names: Vec<_> = filtered.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"new-repo")); // New repo
        assert!(names.contains(&"updated")); // Updated repo
        assert!(!names.contains(&"unchanged")); // Unchanged, skipped
    }

    #[test]
    fn test_filter_for_incremental_sync_empty_db() {
        let repos = vec![
            make_repo(1, "repo-a", Some(Utc::now())),
            make_repo(2, "repo-b", Some(Utc::now())),
        ];

        // No sync info - first sync, all repos should be included
        let filtered = filter_for_incremental_sync(&repos, &[]);

        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_filter_for_incremental_sync_all_unchanged() {
        let old_sync = Utc::now() - ChronoDuration::days(1);
        let even_older = old_sync - ChronoDuration::days(1);

        let repos = vec![
            make_repo(1, "repo-a", Some(even_older)),
            make_repo(2, "repo-b", Some(even_older)),
        ];

        let sync_info = vec![
            make_sync_info(1, "repo-a", old_sync),
            make_sync_info(2, "repo-b", old_sync),
        ];

        let filtered = filter_for_incremental_sync(&repos, &sync_info);

        assert_eq!(filtered.len(), 0); // All unchanged
    }
}
