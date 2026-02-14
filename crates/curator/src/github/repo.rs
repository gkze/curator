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
                namespace: org.to_string(),
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
            namespace: org.to_string(),
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
    let response = client._get(&route).await.map_err(GitHubError::Api)?;

    match response.status().as_u16() {
        204 => Ok(true),
        404 => Ok(false),
        status => Err(GitHubError::Internal(format!(
            "Unexpected status {} checking starred status for {}/{}",
            status, owner, repo
        ))),
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
                namespace: "starred".to_string(),
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
            namespace: "starred".to_string(),
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
    use std::collections::VecDeque;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[derive(Clone, Debug)]
    struct ResponseSpec {
        method: &'static str,
        path_prefix: &'static str,
        status: u16,
        content_type: Option<&'static str>,
        body: String,
    }

    struct TestServer {
        addr: SocketAddr,
        seen: Arc<Mutex<Vec<(String, String)>>>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl TestServer {
        fn new(specs: Vec<ResponseSpec>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
            listener.set_nonblocking(true).expect("set_nonblocking");
            let addr = listener.local_addr().expect("local_addr");
            let specs = Arc::new(Mutex::new(VecDeque::from(specs)));
            let seen = Arc::new(Mutex::new(Vec::new()));
            let specs_thread = Arc::clone(&specs);
            let seen_thread = Arc::clone(&seen);

            let handle = thread::spawn(move || {
                let mut last_progress = std::time::Instant::now();
                while !specs_thread
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_empty()
                {
                    let stream = match listener.accept() {
                        Ok((s, _)) => Ok(s),
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            if last_progress.elapsed() > std::time::Duration::from_secs(5) {
                                let remaining =
                                    specs_thread.lock().unwrap_or_else(|e| e.into_inner()).len();
                                let seen = seen_thread
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .clone();
                                panic!(
                                    "timed out waiting for request; remaining specs={remaining}; seen={seen:?}"
                                );
                            }
                            std::thread::sleep(std::time::Duration::from_millis(10));
                            continue;
                        }
                        Err(e) => Err(e),
                    };

                    let mut stream = stream.expect("accept");
                    stream
                        .set_nonblocking(false)
                        .expect("set_nonblocking(false)");

                    let (method, path) = read_request_line(&mut stream);
                    seen_thread
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push((method.clone(), path.clone()));

                    let spec = specs_thread
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .pop_front()
                        .expect("unexpected request (no remaining response specs)");

                    assert_eq!(method, spec.method, "method mismatch for {path}");
                    assert!(
                        path.starts_with(spec.path_prefix),
                        "path mismatch. expected prefix {:?}, got {:?}",
                        spec.path_prefix,
                        path
                    );

                    write_response(&mut stream, &spec);

                    last_progress = std::time::Instant::now();
                }
            });

            Self {
                addr,
                seen,
                handle: Some(handle),
            }
        }

        fn base_url(&self) -> String {
            format!("http://{}", self.addr)
        }

        fn build_octocrab(&self) -> Octocrab {
            Octocrab::builder()
                .base_uri(self.base_url())
                .expect("base_uri")
                // Any token is fine; we only assert routing.
                .personal_token("token".to_string())
                .build()
                .expect("build octocrab")
        }

        fn seen_requests(&self) -> Vec<(String, String)> {
            self.seen.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            if let Some(handle) = self.handle.take() {
                let res = handle.join();
                if res.is_err() && !std::thread::panicking() {
                    panic!("test server thread panicked");
                }
            }
        }
    }

    fn read_request_line(stream: &mut TcpStream) -> (String, String) {
        let mut buf = [0u8; 8192];
        let mut raw = Vec::new();
        loop {
            let n = stream.read(&mut buf).expect("read request");
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
            if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
            if raw.len() > 64 * 1024 {
                panic!("request too large");
            }
        }

        let text = String::from_utf8_lossy(&raw);
        let first_line = text.lines().next().expect("request line");
        let mut parts = first_line.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let path = parts.next().unwrap_or("").to_string();
        (method, path)
    }

    fn write_response(stream: &mut TcpStream, spec: &ResponseSpec) {
        let body = spec.body.as_bytes();
        let mut headers = String::new();
        if let Some(ct) = spec.content_type {
            headers.push_str(&format!("Content-Type: {}\r\n", ct));
        }
        headers.push_str(&format!("Content-Length: {}\r\n", body.len()));
        headers.push_str("Connection: close\r\n");

        let status_text = match spec.status {
            200 => "OK",
            204 => "No Content",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            _ => "OK",
        };

        let response = format!(
            "HTTP/1.1 {} {}\r\n{}\r\n",
            spec.status, status_text, headers
        );
        stream
            .write_all(response.as_bytes())
            .expect("write headers");
        stream.write_all(body).expect("write body");
        stream.flush().ok();
    }

    fn rate_limit_json(remaining: u32) -> String {
        // Octocrab's ratelimit model expects a nested resources/core structure.
        // Include both `rate` and `resources.core` to be tolerant to model changes.
        let reset = (Utc::now() + Duration::hours(1)).timestamp();
        serde_json::json!({
            "rate": {"limit": 5000, "used": 0, "remaining": remaining, "reset": reset},
            "resources": {
                "core": {"limit": 5000, "used": 0, "remaining": remaining, "reset": reset},
                "search": {"limit": 30, "used": 0, "remaining": 30, "reset": reset},
                "graphql": {"limit": 5000, "used": 0, "remaining": 5000, "reset": reset}
            },
        })
        .to_string()
    }

    fn org_info_json(org: &str, public_repos: usize) -> String {
        serde_json::json!({"login": org, "public_repos": public_repos, "description": "test"})
            .to_string()
    }

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
    fn test_filter_by_activity_excludes_repo_exactly_at_cutoff() {
        let now = Utc::now();
        let active_within = Duration::days(30);
        let at_cutoff = now - active_within;

        let repos = vec![mock_repo_with_activity("at-cutoff", Some(at_cutoff), None)];

        let filtered = filter_by_activity(repos, active_within);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_by_activity_prefers_pushed_at_over_updated_at() {
        let now = Utc::now();
        let old = now - Duration::days(100);
        let recent = now - Duration::days(1);

        // Even with a recent updated_at, old pushed_at should be authoritative.
        let repos = vec![mock_repo_with_activity(
            "push-wins",
            Some(old),
            Some(recent),
        )];

        let filtered = filter_by_activity(repos, Duration::days(30));
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_by_activity_includes_recent_pushed_at_even_if_updated_at_old() {
        let now = Utc::now();
        let recent = now - Duration::hours(1);
        let old = now - Duration::days(365);

        let repos = vec![mock_repo_with_activity(
            "recent-push",
            Some(recent),
            Some(old),
        )];

        let filtered = filter_by_activity(repos, Duration::days(30));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "recent-push");
    }

    #[test]
    fn test_filter_by_activity_excludes_updated_at_exactly_at_cutoff() {
        let now = Utc::now();
        let active_within = Duration::days(14);
        let at_cutoff = now - active_within;

        let repos = vec![mock_repo_with_activity(
            "updated-at-cutoff",
            None,
            Some(at_cutoff),
        )];

        let filtered = filter_by_activity(repos, active_within);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_by_activity_includes_updated_at_just_inside_cutoff() {
        let now = Utc::now();
        let active_within = Duration::days(14);
        let just_inside = now - active_within + Duration::seconds(1);

        let repos = vec![mock_repo_with_activity(
            "updated-just-inside",
            None,
            Some(just_inside),
        )];

        let filtered = filter_by_activity(repos, active_within);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "updated-just-inside");
    }

    #[test]
    fn test_filter_by_activity_with_negative_window_excludes_all_past_activity() {
        let now = Utc::now();
        let repos = vec![
            mock_repo_with_activity("recent", Some(now - Duration::minutes(1)), None),
            mock_repo_with_activity("fallback", None, Some(now - Duration::seconds(10))),
        ];

        let filtered = filter_by_activity(repos, Duration::seconds(-1));
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_by_activity_returns_empty_for_empty_input() {
        let filtered = filter_by_activity(Vec::new(), Duration::days(30));
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_filter_by_activity_with_zero_window_keeps_only_future_activity() {
        let repos = vec![
            mock_repo_with_activity("past", Some(Utc::now() - Duration::seconds(1)), None),
            mock_repo_with_activity("future", Some(Utc::now() + Duration::days(3650)), None),
            mock_repo_with_activity(
                "future-updated",
                None,
                Some(Utc::now() + Duration::days(3650)),
            ),
        ];

        let filtered = filter_by_activity(repos, Duration::zero());
        let names: Vec<_> = filtered.into_iter().map(|repo| repo.name).collect();

        assert_eq!(
            names,
            vec!["future".to_string(), "future-updated".to_string()]
        );
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

    #[tokio::test]
    async fn is_repo_starred_maps_204_and_404() {
        let server = TestServer::new(vec![
            ResponseSpec {
                method: "GET",
                path_prefix: "/user/starred/org/repo",
                status: 204,
                content_type: None,
                body: String::new(),
            },
            ResponseSpec {
                method: "GET",
                path_prefix: "/user/starred/org/other",
                status: 404,
                content_type: None,
                body: String::new(),
            },
        ]);
        let client = server.build_octocrab();

        assert!(is_repo_starred(&client, "org", "repo").await.unwrap());
        assert!(!is_repo_starred(&client, "org", "other").await.unwrap());
    }

    #[tokio::test]
    async fn star_repo_stars_when_not_already_starred_and_skips_when_already_starred() {
        let server = TestServer::new(vec![
            // not starred => PUT
            ResponseSpec {
                method: "GET",
                path_prefix: "/user/starred/org/new",
                status: 404,
                content_type: None,
                body: String::new(),
            },
            ResponseSpec {
                method: "PUT",
                path_prefix: "/user/starred/org/new",
                status: 204,
                content_type: None,
                body: String::new(),
            },
            // already starred => no PUT
            ResponseSpec {
                method: "GET",
                path_prefix: "/user/starred/org/already",
                status: 204,
                content_type: None,
                body: String::new(),
            },
        ]);
        let client = server.build_octocrab();

        assert!(star_repo(&client, "org", "new").await.unwrap());
        assert!(!star_repo(&client, "org", "already").await.unwrap());

        let seen = server.seen_requests();
        assert_eq!(seen.len(), 3);
    }

    #[tokio::test]
    async fn unstar_repo_unstars_only_when_starred() {
        let server = TestServer::new(vec![
            // starred => DELETE
            ResponseSpec {
                method: "GET",
                path_prefix: "/user/starred/org/starred",
                status: 204,
                content_type: None,
                body: String::new(),
            },
            ResponseSpec {
                method: "DELETE",
                path_prefix: "/user/starred/org/starred",
                status: 204,
                content_type: None,
                body: String::new(),
            },
            // not starred => no DELETE
            ResponseSpec {
                method: "GET",
                path_prefix: "/user/starred/org/not-starred",
                status: 404,
                content_type: None,
                body: String::new(),
            },
        ]);
        let client = server.build_octocrab();

        assert!(unstar_repo(&client, "org", "starred").await.unwrap());
        assert!(!unstar_repo(&client, "org", "not-starred").await.unwrap());
    }

    #[tokio::test]
    async fn list_starred_repos_stops_after_short_page() {
        let repo_a = mock_repo_with_activity("a", None, None);
        let repo_b = mock_repo_with_activity("b", None, None);
        let body = serde_json::to_string(&vec![repo_a, repo_b]).expect("serialize repos");

        let server = TestServer::new(vec![
            ResponseSpec {
                method: "GET",
                path_prefix: "/rate_limit",
                status: 200,
                content_type: Some("application/json"),
                body: rate_limit_json(1),
            },
            ResponseSpec {
                method: "GET",
                path_prefix: "/user/starred?per_page=100&page=1",
                status: 200,
                content_type: Some("application/json"),
                body,
            },
        ]);
        let client = server.build_octocrab();

        let repos = list_starred_repos(&client, None).await.unwrap();
        assert_eq!(repos.len(), 2);
    }

    #[tokio::test]
    async fn list_org_repos_paginates_until_end() {
        let page1: Vec<GitHubRepo> = (0..100)
            .map(|i| mock_repo_with_activity(&format!("repo-{i}"), None, None))
            .collect();
        let page2: Vec<GitHubRepo> = vec![mock_repo_with_activity("last", None, None)];

        let server = TestServer::new(vec![
            ResponseSpec {
                method: "GET",
                path_prefix: "/orgs/test-org",
                status: 200,
                content_type: Some("application/json"),
                body: org_info_json("test-org", 101),
            },
            ResponseSpec {
                method: "GET",
                path_prefix: "/rate_limit",
                status: 200,
                content_type: Some("application/json"),
                body: rate_limit_json(1),
            },
            ResponseSpec {
                method: "GET",
                path_prefix: "/orgs/test-org/repos?",
                status: 200,
                content_type: Some("application/json"),
                body: serde_json::to_string(&page1).expect("serialize page1"),
            },
            ResponseSpec {
                method: "GET",
                path_prefix: "/rate_limit",
                status: 200,
                content_type: Some("application/json"),
                body: rate_limit_json(1),
            },
            ResponseSpec {
                method: "GET",
                path_prefix: "/orgs/test-org/repos?",
                status: 200,
                content_type: Some("application/json"),
                body: serde_json::to_string(&page2).expect("serialize page2"),
            },
        ]);
        let client = server.build_octocrab();

        let repos = list_org_repos(&client, "test-org", None).await.unwrap();
        assert_eq!(repos.len(), 101);
    }

    #[tokio::test]
    async fn star_repo_with_retry_succeeds_without_retry_on_first_put() {
        let server = TestServer::new(vec![
            ResponseSpec {
                method: "GET",
                path_prefix: "/user/starred/org/repo",
                status: 404,
                content_type: None,
                body: String::new(),
            },
            ResponseSpec {
                method: "PUT",
                path_prefix: "/user/starred/org/repo",
                status: 204,
                content_type: None,
                body: String::new(),
            },
        ]);
        let client = server.build_octocrab();
        let ok = star_repo_with_retry(&client, "org", "repo", None)
            .await
            .unwrap();
        assert!(ok);
    }
}
