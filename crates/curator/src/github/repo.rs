//! Repository operations: listing, filtering, and starring.

use backon::Retryable;
use chrono::{Duration, Utc};
use octocrab::Octocrab;
use octocrab::models::Repository as GitHubRepo;

use super::client::check_rate_limit;
use super::error::{GitHubError, is_rate_limit_error_from_github, short_error_message};
use super::types::{ProgressCallback, SyncProgress, emit};
use crate::retry::default_backoff;

/// List all repositories for an organization with pagination.
///
/// Handles rate limiting by checking remaining quota before requests.
pub async fn list_org_repos(
    client: &Octocrab,
    org: &str,
    on_progress: Option<&ProgressCallback>,
) -> Result<Vec<GitHubRepo>, GitHubError> {
    use super::client::get_org_info;

    let mut all_repos = Vec::new();
    let mut page = 1u32;

    // Get org info to know total repos upfront
    let org_info = get_org_info(client, org).await.ok();
    let total_repos = org_info.as_ref().map(|i| i.public_repos);
    let expected_pages = total_repos.map(|t| t.div_ceil(100) as u32);

    emit(
        on_progress,
        SyncProgress::FetchingRepos {
            namespace: org.to_string(),
            total_repos,
            expected_pages,
        },
    );

    loop {
        // Check rate limit before making request
        check_rate_limit(client).await?;

        let page_result = client
            .orgs(org)
            .list_repos()
            .per_page(100)
            .page(page)
            .send()
            .await?;

        let repos: Vec<GitHubRepo> = page_result.items;
        let count = repos.len();

        all_repos.extend(repos);

        emit(
            on_progress,
            SyncProgress::FetchedPage {
                page,
                count,
                total_so_far: all_repos.len(),
                expected_pages,
            },
        );

        // If we got fewer than 100, we've reached the end
        if count < 100 {
            break;
        }

        page += 1;
    }

    emit(
        on_progress,
        SyncProgress::FetchComplete {
            total: all_repos.len(),
        },
    );

    Ok(all_repos)
}

/// Filter repositories by recent activity.
pub fn filter_by_activity(repos: Vec<GitHubRepo>, active_within: Duration) -> Vec<GitHubRepo> {
    let cutoff = Utc::now() - active_within;

    repos
        .into_iter()
        .filter(|repo| {
            // Use pushed_at as the primary activity indicator, fall back to updated_at
            let activity_time = repo.pushed_at.or(repo.updated_at);
            match activity_time {
                Some(time) => time > cutoff,
                None => false, // No activity timestamp, exclude
            }
        })
        .collect()
}

/// Check if a repository is starred by the authenticated user.
pub async fn is_repo_starred(
    client: &Octocrab,
    owner: &str,
    repo: &str,
) -> Result<bool, GitHubError> {
    // GET /user/starred/{owner}/{repo} returns 204 if starred, 404 if not
    let route = format!("/user/starred/{}/{}", owner, repo);
    let response: Result<(), octocrab::Error> = client.get(&route, None::<&()>).await;

    match response {
        Ok(_) => Ok(true),
        Err(octocrab::Error::GitHub { source, .. }) if source.status_code.as_u16() == 404 => {
            Ok(false)
        }
        Err(e) => Err(GitHubError::Api(e)),
    }
}

/// Star a repository.
///
/// Returns true if the repo was starred, false if it was already starred.
pub async fn star_repo(client: &Octocrab, owner: &str, repo: &str) -> Result<bool, GitHubError> {
    // Check if already starred
    if is_repo_starred(client, owner, repo).await? {
        return Ok(false);
    }

    // PUT /user/starred/{owner}/{repo}
    let route = format!("/user/starred/{}/{}", owner, repo);
    client
        ._put(&route, None::<&()>)
        .await
        .map_err(GitHubError::Api)?;

    Ok(true)
}

/// Unstar a repository.
///
/// Returns true if the repo was unstarred, false if it wasn't starred.
#[allow(dead_code)]
pub async fn unstar_repo(client: &Octocrab, owner: &str, repo: &str) -> Result<bool, GitHubError> {
    // Check if starred first
    if !is_repo_starred(client, owner, repo).await? {
        return Ok(false);
    }

    // DELETE /user/starred/{owner}/{repo}
    let route = format!("/user/starred/{}/{}", owner, repo);
    client
        ._delete(&route, None::<&()>)
        .await
        .map_err(GitHubError::Api)?;

    Ok(true)
}

/// List all repositories starred by the authenticated user with pagination.
///
/// Handles rate limiting by checking remaining quota before requests.
#[allow(dead_code)]
pub async fn list_starred_repos(
    client: &Octocrab,
    on_progress: Option<&ProgressCallback>,
) -> Result<Vec<GitHubRepo>, GitHubError> {
    let mut all_repos = Vec::new();
    let mut page = 1u32;

    emit(
        on_progress,
        SyncProgress::FetchingRepos {
            namespace: "starred".to_string(),
            total_repos: None,
            expected_pages: None,
        },
    );

    loop {
        // Check rate limit before making request
        check_rate_limit(client).await?;

        // GET /user/starred with pagination
        let route = format!("/user/starred?per_page=100&page={}", page);
        let repos: Vec<GitHubRepo> = client.get(&route, None::<&()>).await?;

        let count = repos.len();
        all_repos.extend(repos);

        emit(
            on_progress,
            SyncProgress::FetchedPage {
                page,
                count,
                total_so_far: all_repos.len(),
                expected_pages: None,
            },
        );

        // If we got fewer than 100, we've reached the end
        if count < 100 {
            break;
        }

        page += 1;
    }

    emit(
        on_progress,
        SyncProgress::FetchComplete {
            total: all_repos.len(),
        },
    );

    Ok(all_repos)
}

/// Star a repository with exponential backoff retry on rate limit errors.
///
/// Returns Ok(true) if starred, Ok(false) if already starred, Err on permanent failure.
/// The optional progress callback receives backoff notifications for UI feedback.
pub async fn star_repo_with_retry(
    client: &Octocrab,
    owner: &str,
    repo: &str,
    on_progress: Option<&ProgressCallback>,
) -> Result<bool, GitHubError> {
    // Check if already starred first (before any delays)
    match is_repo_starred(client, owner, repo).await {
        Ok(true) => return Ok(false),
        Ok(false) => {}
        Err(e) if is_rate_limit_error_from_github(&e) => {
            // Will retry below
        }
        Err(e) => return Err(e),
    }

    let owner_str = owner.to_string();
    let repo_str = repo.to_string();

    // Track attempt number for progress reporting
    let attempt = std::sync::atomic::AtomicU32::new(0);

    let star_op = || async {
        attempt.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // PUT /user/starred/{owner}/{repo}
        let route = format!("/user/starred/{}/{}", owner_str, repo_str);
        client
            ._put(&route, None::<&()>)
            .await
            .map_err(GitHubError::Api)
    };

    let result = star_op
        .retry(default_backoff())
        .notify(|err, dur| {
            let current_attempt = attempt.load(std::sync::atomic::Ordering::SeqCst);
            emit(
                on_progress,
                SyncProgress::RateLimitBackoff {
                    owner: owner_str.clone(),
                    name: repo_str.clone(),
                    retry_after_ms: dur.as_millis() as u64,
                    attempt: current_attempt,
                },
            );
            tracing::debug!(
                "Rate limited on {}/{}, retrying in {:?} (attempt {}): {}",
                owner_str,
                repo_str,
                dur,
                current_attempt,
                short_error_message(err)
            );
        })
        .when(is_rate_limit_error_from_github)
        .await;

    result.map(|_| true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::github::types::RepoIdentity;
    use chrono::{Duration, Utc};

    /// Create a mock GitHubRepo for testing.
    /// Note: octocrab::models::Repository has many fields, we set required ones.
    fn mock_repo_with_activity(
        name: &str,
        pushed_at: Option<chrono::DateTime<Utc>>,
        updated_at: Option<chrono::DateTime<Utc>>,
    ) -> GitHubRepo {
        let pushed_str = pushed_at
            .map(|t| format!("\"{}\"", t.to_rfc3339()))
            .unwrap_or_else(|| "null".to_string());
        let updated_str = updated_at
            .map(|t| format!("\"{}\"", t.to_rfc3339()))
            .unwrap_or_else(|| "null".to_string());

        // Use string replacement to avoid format! argument counting issues
        let template = r#"{
            "id": 12345,
            "node_id": "MDEwOlJlcG9zaXRvcnkxMjM0NQ==",
            "name": "__NAME__",
            "full_name": "test-owner/__NAME__",
            "private": false,
            "owner": {
                "login": "test-owner",
                "id": 1,
                "node_id": "MDQ6VXNlcjE=",
                "avatar_url": "https://avatars.githubusercontent.com/u/1?v=4",
                "gravatar_id": "",
                "url": "https://api.github.com/users/test-owner",
                "html_url": "https://github.com/test-owner",
                "followers_url": "https://api.github.com/users/test-owner/followers",
                "following_url": "https://api.github.com/users/test-owner/following{/other_user}",
                "gists_url": "https://api.github.com/users/test-owner/gists{/gist_id}",
                "starred_url": "https://api.github.com/users/test-owner/starred{/owner}{/repo}",
                "subscriptions_url": "https://api.github.com/users/test-owner/subscriptions",
                "organizations_url": "https://api.github.com/users/test-owner/orgs",
                "repos_url": "https://api.github.com/users/test-owner/repos",
                "events_url": "https://api.github.com/users/test-owner/events{/privacy}",
                "received_events_url": "https://api.github.com/users/test-owner/received_events",
                "type": "User",
                "site_admin": false
            },
            "html_url": "https://github.com/test-owner/__NAME__",
            "description": "A test repo",
            "fork": false,
            "url": "https://api.github.com/repos/test-owner/__NAME__",
            "forks_url": "https://api.github.com/repos/test-owner/__NAME__/forks",
            "keys_url": "https://api.github.com/repos/test-owner/__NAME__/keys{/key_id}",
            "collaborators_url": "https://api.github.com/repos/test-owner/__NAME__/collaborators{/collaborator}",
            "teams_url": "https://api.github.com/repos/test-owner/__NAME__/teams",
            "hooks_url": "https://api.github.com/repos/test-owner/__NAME__/hooks",
            "issue_events_url": "https://api.github.com/repos/test-owner/__NAME__/issues/events{/number}",
            "events_url": "https://api.github.com/repos/test-owner/__NAME__/events",
            "assignees_url": "https://api.github.com/repos/test-owner/__NAME__/assignees{/user}",
            "branches_url": "https://api.github.com/repos/test-owner/__NAME__/branches{/branch}",
            "tags_url": "https://api.github.com/repos/test-owner/__NAME__/tags",
            "blobs_url": "https://api.github.com/repos/test-owner/__NAME__/git/blobs{/sha}",
            "git_tags_url": "https://api.github.com/repos/test-owner/__NAME__/git/tags{/sha}",
            "git_refs_url": "https://api.github.com/repos/test-owner/__NAME__/git/refs{/sha}",
            "trees_url": "https://api.github.com/repos/test-owner/__NAME__/git/trees{/sha}",
            "statuses_url": "https://api.github.com/repos/test-owner/__NAME__/statuses/{sha}",
            "languages_url": "https://api.github.com/repos/test-owner/__NAME__/languages",
            "stargazers_url": "https://api.github.com/repos/test-owner/__NAME__/stargazers",
            "contributors_url": "https://api.github.com/repos/test-owner/__NAME__/contributors",
            "subscribers_url": "https://api.github.com/repos/test-owner/__NAME__/subscribers",
            "subscription_url": "https://api.github.com/repos/test-owner/__NAME__/subscription",
            "commits_url": "https://api.github.com/repos/test-owner/__NAME__/commits{/sha}",
            "git_commits_url": "https://api.github.com/repos/test-owner/__NAME__/git/commits{/sha}",
            "comments_url": "https://api.github.com/repos/test-owner/__NAME__/comments{/number}",
            "issue_comment_url": "https://api.github.com/repos/test-owner/__NAME__/issues/comments{/number}",
            "contents_url": "https://api.github.com/repos/test-owner/__NAME__/contents/{+path}",
            "compare_url": "https://api.github.com/repos/test-owner/__NAME__/compare/{base}...{head}",
            "merges_url": "https://api.github.com/repos/test-owner/__NAME__/merges",
            "archive_url": "https://api.github.com/repos/test-owner/__NAME__/{archive_format}{/ref}",
            "downloads_url": "https://api.github.com/repos/test-owner/__NAME__/downloads",
            "issues_url": "https://api.github.com/repos/test-owner/__NAME__/issues{/number}",
            "pulls_url": "https://api.github.com/repos/test-owner/__NAME__/pulls{/number}",
            "milestones_url": "https://api.github.com/repos/test-owner/__NAME__/milestones{/number}",
            "notifications_url": "https://api.github.com/repos/test-owner/__NAME__/notifications{?since,all,participating}",
            "labels_url": "https://api.github.com/repos/test-owner/__NAME__/labels{/name}",
            "releases_url": "https://api.github.com/repos/test-owner/__NAME__/releases{/id}",
            "deployments_url": "https://api.github.com/repos/test-owner/__NAME__/deployments",
            "pushed_at": __PUSHED__,
            "updated_at": __UPDATED__,
            "created_at": "2020-01-01T00:00:00Z"
        }"#;

        let json = template
            .replace("__NAME__", name)
            .replace("__PUSHED__", &pushed_str)
            .replace("__UPDATED__", &updated_str);

        serde_json::from_str(&json).expect("Failed to parse mock repo JSON")
    }

    #[test]
    fn test_filter_by_activity_includes_recent() {
        let now = Utc::now();
        let recent = now - Duration::days(10);

        let repos = vec![mock_repo_with_activity("recent-repo", Some(recent), None)];

        let filtered = filter_by_activity(repos, Duration::days(30));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "recent-repo");
    }

    #[test]
    fn test_filter_by_activity_excludes_old() {
        let now = Utc::now();
        let old = now - Duration::days(100);

        let repos = vec![mock_repo_with_activity("old-repo", Some(old), None)];

        let filtered = filter_by_activity(repos, Duration::days(30));
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_filter_by_activity_uses_updated_at_fallback() {
        let now = Utc::now();
        let recent = now - Duration::days(10);

        // No pushed_at, but recent updated_at
        let repos = vec![mock_repo_with_activity("updated-repo", None, Some(recent))];

        let filtered = filter_by_activity(repos, Duration::days(30));
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn test_filter_by_activity_excludes_no_timestamp() {
        let repos = vec![mock_repo_with_activity("no-timestamp", None, None)];

        let filtered = filter_by_activity(repos, Duration::days(30));
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_filter_by_activity_mixed() {
        let now = Utc::now();
        let recent = now - Duration::days(10);
        let old = now - Duration::days(100);

        let repos = vec![
            mock_repo_with_activity("recent", Some(recent), None),
            mock_repo_with_activity("old", Some(old), None),
            mock_repo_with_activity("no-time", None, None),
        ];

        let filtered = filter_by_activity(repos, Duration::days(30));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "recent");
    }

    #[test]
    fn test_repo_identity_trait() {
        let repo = mock_repo_with_activity("test-repo", None, None);

        assert_eq!(repo.owner_login(), "test-owner");
        assert_eq!(repo.repo_name(), "test-repo");
        assert_eq!(
            repo.owner_and_name(),
            ("test-owner".to_string(), "test-repo".to_string())
        );
    }

    #[test]
    fn test_default_backoff_builder() {
        // Just verify it builds without panicking
        let _backoff = default_backoff();
    }
}
