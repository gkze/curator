//! Model conversion from GitHub API types to curator entities.

use octocrab::models::Repository as GitHubRepo;
use uuid::Uuid;

use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::code_visibility::CodeVisibility;
use crate::platform::{PlatformRepo, strip_null_values};

/// Determine visibility from a GitHub repository.
///
/// GitHub can return "public", "private", or "internal" (GitHub Enterprise only).
/// For public org repos (which is what we typically fetch), this will be Public.
/// Internal visibility requires GitHub Enterprise.
fn github_visibility(repo: &GitHubRepo) -> CodeVisibility {
    if repo.private.unwrap_or(false) {
        CodeVisibility::Private
    } else {
        CodeVisibility::Public
    }
}

/// Convert a GitHub repository to a CodeRepository active model.
pub fn to_code_repository(repo: &GitHubRepo, instance_id: Uuid) -> CodeRepositoryActiveModel {
    let platform_repo = to_platform_repo(repo);
    platform_repo.to_active_model(instance_id)
}

/// Convert a GitHub repository to a platform-agnostic PlatformRepo.
pub fn to_platform_repo(repo: &GitHubRepo) -> PlatformRepo {
    let owner = repo
        .owner
        .as_ref()
        .map(|o| o.login.clone())
        .unwrap_or_default();

    let primary_language = repo
        .language
        .as_ref()
        .and_then(|v| v.as_str().map(String::from));

    let license_spdx = repo.license.as_ref().map(|l| l.spdx_id.clone());

    let platform_metadata = strip_null_values(serde_json::json!({
        "node_id": repo.node_id,
        "private": repo.private,
        "allow_squash_merge": repo.allow_squash_merge,
        "allow_merge_commit": repo.allow_merge_commit,
        "allow_rebase_merge": repo.allow_rebase_merge,
        "allow_auto_merge": repo.allow_auto_merge,
        "delete_branch_on_merge": repo.delete_branch_on_merge,
        "mirror": repo.mirror_url.is_some(),
        "template": repo.is_template,
        "empty": repo.size.map(|size| size == 0),
        "open_issues_count": repo.open_issues_count,
        "watchers_count": repo.watchers_count,
        "has_issues": repo.has_issues,
        "has_wiki": repo.has_wiki,
        "has_pull_requests": true,
    }));

    PlatformRepo {
        platform_id: repo.id.0 as i64,
        owner,
        name: repo.name.clone(),
        description: repo.description.clone(),
        default_branch: repo
            .default_branch
            .clone()
            .unwrap_or_else(|| "main".to_string()),
        visibility: github_visibility(repo),
        is_fork: repo.fork.unwrap_or(false),
        is_archived: repo.archived.unwrap_or(false),
        stars: repo.stargazers_count,
        forks: repo.forks_count,
        language: primary_language,
        topics: repo.topics.clone().unwrap_or_default(),
        created_at: repo.created_at,
        updated_at: repo.updated_at,
        pushed_at: repo.pushed_at,
        license: license_spdx,
        homepage: repo.homepage.clone(),
        size_kb: repo.size.map(|s| s as u64),
        metadata: platform_metadata,
    }
}

/// Convert a PlatformRepo to a CodeRepository active model for a specific instance.
pub fn platform_repo_to_active_model(
    repo: &PlatformRepo,
    instance_id: Uuid,
) -> CodeRepositoryActiveModel {
    repo.to_active_model(instance_id)
}

#[cfg(test)]
mod tests {
    use chrono::Datelike;
    use octocrab::models::Repository as GitHubRepo;
    use sea_orm::ActiveValue::Set;
    use uuid::Uuid;

    use super::*;

    /// Build a fully-populated GitHub `Repository` for conversion tests.
    ///
    /// Includes all fields (license, topics, permissions, etc.) to exercise
    /// the full conversion path.  See `client::tests::repo_json` for a
    /// lightweight variant used in HTTP response mocking.
    fn sample_repo(private: Option<bool>, default_branch: Option<&str>) -> GitHubRepo {
        let mut repo_json: serde_json::Value = serde_json::from_str(
            r#"{
                "id": 42,
                "node_id": "R_kgDOExample",
                "name": "curator",
                "full_name": "gkze/curator",
                "owner": {
                    "login": "gkze",
                    "id": 7,
                    "node_id": "MDQ6VXNlcjc=",
                    "avatar_url": "https://example.com/avatar.png",
                    "gravatar_id": "",
                    "url": "https://api.github.com/users/gkze",
                    "html_url": "https://github.com/gkze",
                    "followers_url": "https://api.github.com/users/gkze/followers",
                    "following_url": "https://api.github.com/users/gkze/following{/other_user}",
                    "gists_url": "https://api.github.com/users/gkze/gists{/gist_id}",
                    "starred_url": "https://api.github.com/users/gkze/starred{/owner}{/repo}",
                    "subscriptions_url": "https://api.github.com/users/gkze/subscriptions",
                    "organizations_url": "https://api.github.com/users/gkze/orgs",
                    "repos_url": "https://api.github.com/users/gkze/repos",
                    "events_url": "https://api.github.com/users/gkze/events{/privacy}",
                    "received_events_url": "https://api.github.com/users/gkze/received_events",
                    "type": "User",
                    "site_admin": false
                },
                "html_url": "https://github.com/gkze/curator",
                "description": "repo tracker",
                "fork": false,
                "url": "https://api.github.com/repos/gkze/curator",
                "forks_url": "https://api.github.com/repos/gkze/curator/forks",
                "keys_url": "https://api.github.com/repos/gkze/curator/keys{/key_id}",
                "collaborators_url": "https://api.github.com/repos/gkze/curator/collaborators{/collaborator}",
                "teams_url": "https://api.github.com/repos/gkze/curator/teams",
                "hooks_url": "https://api.github.com/repos/gkze/curator/hooks",
                "issue_events_url": "https://api.github.com/repos/gkze/curator/issues/events{/number}",
                "events_url": "https://api.github.com/repos/gkze/curator/events",
                "assignees_url": "https://api.github.com/repos/gkze/curator/assignees{/user}",
                "branches_url": "https://api.github.com/repos/gkze/curator/branches{/branch}",
                "tags_url": "https://api.github.com/repos/gkze/curator/tags",
                "blobs_url": "https://api.github.com/repos/gkze/curator/git/blobs{/sha}",
                "git_tags_url": "https://api.github.com/repos/gkze/curator/git/tags{/sha}",
                "git_refs_url": "https://api.github.com/repos/gkze/curator/git/refs{/sha}",
                "trees_url": "https://api.github.com/repos/gkze/curator/git/trees{/sha}",
                "statuses_url": "https://api.github.com/repos/gkze/curator/statuses/{sha}",
                "languages_url": "https://api.github.com/repos/gkze/curator/languages",
                "stargazers_url": "https://api.github.com/repos/gkze/curator/stargazers",
                "contributors_url": "https://api.github.com/repos/gkze/curator/contributors",
                "subscribers_url": "https://api.github.com/repos/gkze/curator/subscribers",
                "subscription_url": "https://api.github.com/repos/gkze/curator/subscription",
                "commits_url": "https://api.github.com/repos/gkze/curator/commits{/sha}",
                "git_commits_url": "https://api.github.com/repos/gkze/curator/git/commits{/sha}",
                "comments_url": "https://api.github.com/repos/gkze/curator/comments{/number}",
                "issue_comment_url": "https://api.github.com/repos/gkze/curator/issues/comments{/number}",
                "contents_url": "https://api.github.com/repos/gkze/curator/contents/{+path}",
                "compare_url": "https://api.github.com/repos/gkze/curator/compare/{base}...{head}",
                "merges_url": "https://api.github.com/repos/gkze/curator/merges",
                "archive_url": "https://api.github.com/repos/gkze/curator/{archive_format}{/ref}",
                "downloads_url": "https://api.github.com/repos/gkze/curator/downloads",
                "issues_url": "https://api.github.com/repos/gkze/curator/issues{/number}",
                "pulls_url": "https://api.github.com/repos/gkze/curator/pulls{/number}",
                "milestones_url": "https://api.github.com/repos/gkze/curator/milestones{/number}",
                "notifications_url": "https://api.github.com/repos/gkze/curator/notifications{?since,all,participating}",
                "labels_url": "https://api.github.com/repos/gkze/curator/labels{/name}",
                "releases_url": "https://api.github.com/repos/gkze/curator/releases{/id}",
                "deployments_url": "https://api.github.com/repos/gkze/curator/deployments",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-02T00:00:00Z",
                "pushed_at": "2024-01-03T00:00:00Z",
                "git_url": "git://github.com/gkze/curator.git",
                "ssh_url": "git@github.com:gkze/curator.git",
                "clone_url": "https://github.com/gkze/curator.git",
                "svn_url": "https://github.com/gkze/curator",
                "homepage": "https://curator.example",
                "size": 123,
                "stargazers_count": 10,
                "watchers_count": 20,
                "language": "Rust",
                "has_issues": true,
                "has_projects": true,
                "has_downloads": true,
                "has_wiki": false,
                "has_pages": false,
                "has_discussions": false,
                "forks_count": 5,
                "mirror_url": null,
                "archived": false,
                "disabled": false,
                "open_issues_count": 4,
                "license": {
                    "key": "mit",
                    "name": "MIT License",
                    "spdx_id": "MIT",
                    "url": "https://api.github.com/licenses/mit",
                    "node_id": "MDc6TGljZW5zZW1pdA=="
                },
                "allow_forking": true,
                "is_template": false,
                "web_commit_signoff_required": false,
                "topics": ["rust", "cli"],
                "visibility": "public",
                "forks": 5,
                "open_issues": 4,
                "watchers": 20,
                "allow_squash_merge": true,
                "allow_merge_commit": true,
                "allow_rebase_merge": true,
                "allow_auto_merge": null,
                "delete_branch_on_merge": true,
                "permissions": {
                    "admin": false,
                    "maintain": false,
                    "push": false,
                    "triage": false,
                    "pull": true
                }
            }"#,
        )
        .expect("sample repository JSON should parse");

        repo_json["private"] = serde_json::to_value(private).expect("private should serialize");
        repo_json["default_branch"] =
            serde_json::to_value(default_branch).expect("default branch should serialize");

        serde_json::from_value(repo_json).expect("sample repository JSON should deserialize")
    }

    #[test]
    fn to_platform_repo_maps_expected_fields() {
        let repo = sample_repo(Some(true), Some("trunk"));

        let converted = to_platform_repo(&repo);

        assert_eq!(converted.platform_id, 42);
        assert_eq!(converted.owner, "gkze");
        assert_eq!(converted.name, "curator");
        assert_eq!(converted.default_branch, "trunk");
        assert_eq!(converted.visibility, CodeVisibility::Private);
        assert_eq!(converted.language.as_deref(), Some("Rust"));
        assert_eq!(converted.license.as_deref(), Some("MIT"));
        assert_eq!(
            converted.topics,
            vec!["rust".to_string(), "cli".to_string()]
        );
        assert_eq!(converted.size_kb, Some(123));
        assert_eq!(converted.created_at.map(|dt| dt.year()), Some(2024));
        assert_eq!(
            converted.metadata.get("allow_auto_merge"),
            None,
            "null values should be stripped from metadata"
        );
    }

    #[test]
    fn to_platform_repo_uses_defaults_for_missing_optional_fields() {
        let mut repo_json =
            serde_json::to_value(sample_repo(None, None)).expect("serialize sample");
        repo_json["owner"] = serde_json::Value::Null;
        repo_json["topics"] = serde_json::Value::Null;
        repo_json["language"] = serde_json::Value::Null;
        repo_json["license"] = serde_json::Value::Null;

        let repo: GitHubRepo =
            serde_json::from_value(repo_json).expect("deserialize modified sample");
        let converted = to_platform_repo(&repo);

        assert_eq!(converted.owner, "");
        assert_eq!(converted.default_branch, "main");
        assert_eq!(converted.visibility, CodeVisibility::Public);
        assert_eq!(converted.topics, Vec::<String>::new());
        assert_eq!(converted.language, None);
        assert_eq!(converted.license, None);
    }

    #[test]
    fn wrappers_delegate_to_platform_repo_conversion() {
        let repo = sample_repo(Some(false), Some("main"));
        let instance_id = Uuid::new_v4();

        let via_code_repository = to_code_repository(&repo, instance_id);
        let via_platform_repo =
            platform_repo_to_active_model(&to_platform_repo(&repo), instance_id);

        assert_eq!(via_code_repository.instance_id, Set(instance_id));
        assert_eq!(
            via_code_repository.platform_id,
            via_platform_repo.platform_id
        );
        assert_eq!(via_code_repository.owner, via_platform_repo.owner);
        assert_eq!(via_code_repository.name, via_platform_repo.name);
        assert_eq!(
            via_code_repository.default_branch,
            via_platform_repo.default_branch
        );
        assert_eq!(via_code_repository.visibility, via_platform_repo.visibility);
    }
}
