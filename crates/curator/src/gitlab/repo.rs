//! Repository operations: listing, filtering, and starring.

use std::sync::Arc;

use chrono::{Duration, Utc};
use tokio::sync::Semaphore;

use super::client::GitLabClient;
use super::error::GitLabError;
use super::types::{GitLabProject, ProgressCallback, SyncProgress, emit};
use crate::sync::{StarResult, StarringStats};

/// List all projects for a group with automatic pagination.
pub async fn list_group_projects(
    client: &GitLabClient,
    group: &str,
    include_subgroups: bool,
    on_progress: Option<&ProgressCallback>,
) -> Result<Vec<GitLabProject>, GitLabError> {
    emit(
        on_progress,
        SyncProgress::FetchingRepos {
            namespace: group.to_string(),
            total_repos: None,
            expected_pages: None,
        },
    );

    let projects: Vec<GitLabProject> = client.list_group_projects(group, include_subgroups).await?;

    emit(
        on_progress,
        SyncProgress::FetchComplete {
            total: projects.len(),
        },
    );

    Ok(projects)
}

/// Filter projects by recent activity.
pub fn filter_by_activity(
    projects: &[GitLabProject],
    active_within: Duration,
) -> Vec<&GitLabProject> {
    let cutoff = Utc::now() - active_within;

    projects
        .iter()
        .filter(|project| project.last_activity_at > cutoff)
        .collect()
}

/// Filter projects by recent activity (consuming version).
#[allow(dead_code)]
pub fn filter_by_activity_owned(
    projects: Vec<GitLabProject>,
    active_within: Duration,
) -> Vec<GitLabProject> {
    let cutoff = Utc::now() - active_within;

    projects
        .into_iter()
        .filter(|project| project.last_activity_at > cutoff)
        .collect()
}

/// Star a project by ID.
///
/// Returns Ok(true) if the project was starred, Ok(false) if already starred.
pub async fn star_project(client: &GitLabClient, project_id: u64) -> Result<bool, GitLabError> {
    client.star_project(project_id).await
}

/// Star a project with exponential backoff retry on rate limit errors.
///
/// Returns Ok(true) if starred, Ok(false) if already starred, Err on permanent failure.
pub async fn star_project_with_retry(
    client: &GitLabClient,
    project_id: u64,
    on_progress: Option<&ProgressCallback>,
) -> Result<bool, GitLabError> {
    use super::error::{is_rate_limit_error, short_error_message};
    use crate::retry::with_retry;

    let full_name = format!("project_{}", project_id);
    let client = client.clone();

    with_retry(
        || async { client.star_project(project_id).await },
        is_rate_limit_error,
        short_error_message,
        &full_name,
        "",
        on_progress,
    )
    .await
}

/// Star multiple projects concurrently with progress reporting.
#[allow(dead_code)]
pub async fn star_projects_batch(
    client: &GitLabClient,
    projects: &[&GitLabProject],
    concurrency: usize,
    dry_run: bool,
    on_progress: Option<&ProgressCallback>,
) -> StarringStats {
    let mut stats = StarringStats::default();

    if projects.is_empty() {
        return stats;
    }

    let concurrency = std::cmp::min(concurrency, projects.len());
    let semaphore = Arc::new(Semaphore::new(concurrency));

    emit(
        on_progress,
        SyncProgress::StarringRepos {
            count: projects.len(),
            concurrency,
            dry_run,
        },
    );

    let project_info: Vec<_> = projects
        .iter()
        .map(|p| (p.id, p.namespace.full_path.clone(), p.name.clone()))
        .collect();

    let mut handles = Vec::with_capacity(projects.len());

    for (project_id, owner, name) in project_info {
        let semaphore = Arc::clone(&semaphore);
        let client = client.clone();

        let handle = tokio::spawn(async move {
            let _permit = match semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    return (
                        owner,
                        name,
                        StarResult::Error("Semaphore closed unexpectedly".to_string()),
                    );
                }
            };

            if dry_run {
                return (owner, name, StarResult::Starred);
            }

            match client.star_project(project_id).await {
                Ok(true) => (owner, name, StarResult::Starred),
                Ok(false) => (owner, name, StarResult::AlreadyStarred),
                Err(e) => (owner, name, StarResult::Error(e.to_string())),
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok((owner, name, StarResult::Starred)) => {
                stats.starred += 1;
                emit(
                    on_progress,
                    SyncProgress::StarredRepo {
                        owner,
                        name,
                        already_starred: false,
                    },
                );
            }
            Ok((owner, name, StarResult::AlreadyStarred)) => {
                stats.skipped += 1;
                emit(
                    on_progress,
                    SyncProgress::StarredRepo {
                        owner,
                        name,
                        already_starred: true,
                    },
                );
            }
            Ok((owner, name, StarResult::Error(err))) => {
                stats.errors.push(format!("{}/{}: {}", owner, name, err));
                emit(
                    on_progress,
                    SyncProgress::StarError {
                        owner,
                        name,
                        error: err,
                    },
                );
            }
            Err(e) => {
                stats.errors.push(format!("Task failed: {}", e));
            }
        }
    }

    emit(
        on_progress,
        SyncProgress::StarringComplete {
            starred: stats.starred,
            already_starred: stats.skipped,
            errors: stats.errors.len(),
        },
    );

    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};

    /// Create a mock GitLabProject for testing.
    fn mock_project(name: &str, last_activity_days_ago: i64) -> GitLabProject {
        let last_activity = Utc::now() - Duration::days(last_activity_days_ago);

        GitLabProject {
            id: 12345,
            name: name.to_string(),
            path: name.to_lowercase(),
            path_with_namespace: format!("test-group/{}", name.to_lowercase()),
            description: Some("A test project".to_string()),
            default_branch: Some("main".to_string()),
            visibility: "public".to_string(),
            archived: false,
            topics: vec!["rust".to_string()],
            star_count: 10,
            forks_count: 2,
            open_issues_count: Some(5),
            created_at: Utc::now() - Duration::days(365),
            last_activity_at: last_activity,
            namespace: super::super::types::GitLabNamespace {
                id: 1,
                name: "test-group".to_string(),
                path: "test-group".to_string(),
                full_path: "test-group".to_string(),
                kind: "group".to_string(),
            },
            forked_from_project: None,
            mirror: None,
            issues_enabled: Some(true),
            wiki_enabled: Some(true),
            merge_requests_enabled: Some(true),
            web_url: format!("https://gitlab.com/test-group/{}", name.to_lowercase()),
            ssh_url_to_repo: Some(format!(
                "git@gitlab.com:test-group/{}.git",
                name.to_lowercase()
            )),
            http_url_to_repo: Some(format!(
                "https://gitlab.com/test-group/{}.git",
                name.to_lowercase()
            )),
        }
    }

    #[test]
    fn test_filter_by_activity_includes_recent() {
        let projects = vec![mock_project("recent-project", 10)];
        let filtered = filter_by_activity(&projects, Duration::days(30));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "recent-project");
    }

    #[test]
    fn test_filter_by_activity_excludes_old() {
        let projects = vec![mock_project("old-project", 100)];
        let filtered = filter_by_activity(&projects, Duration::days(30));
        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_filter_by_activity_mixed() {
        let projects = vec![
            mock_project("recent", 10),
            mock_project("old", 100),
            mock_project("borderline", 29),
        ];
        let filtered = filter_by_activity(&projects, Duration::days(30));
        assert_eq!(filtered.len(), 2);
        let names: Vec<_> = filtered.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"recent"));
        assert!(names.contains(&"borderline"));
        assert!(!names.contains(&"old"));
    }

    #[test]
    fn test_filter_by_activity_owned() {
        let projects = vec![mock_project("recent", 10), mock_project("old", 100)];
        let filtered = filter_by_activity_owned(projects, Duration::days(30));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "recent");
    }
}
