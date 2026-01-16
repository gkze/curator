//! Model conversion functions for Gitea repositories.

use chrono::Utc;
use sea_orm::Set;
use uuid::Uuid;

use super::types::GiteaRepo;
use crate::entity::code_platform::CodePlatform;
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::code_visibility::CodeVisibility;
use crate::platform::PlatformRepo;

/// Determine visibility from a Gitea repository.
///
/// Gitea/Forgejo only has public and private visibility (no internal).
fn gitea_visibility(repo: &GiteaRepo) -> CodeVisibility {
    if repo.private {
        CodeVisibility::Private
    } else {
        CodeVisibility::Public
    }
}

/// Convert a Gitea repository to a platform-agnostic representation.
pub fn to_platform_repo(repo: &GiteaRepo) -> PlatformRepo {
    PlatformRepo {
        platform_id: repo.id,
        owner: repo.owner.login.clone(),
        name: repo.name.clone(),
        description: repo.description.clone(),
        default_branch: repo
            .default_branch
            .clone()
            .unwrap_or_else(|| "main".to_string()),
        visibility: gitea_visibility(repo),
        is_fork: repo.fork,
        is_archived: repo.archived,
        stars: Some(repo.stars_count),
        forks: Some(repo.forks_count),
        language: repo.language.clone(),
        topics: repo.topics.clone(),
        created_at: Some(repo.created_at),
        updated_at: Some(repo.updated_at),
        pushed_at: repo.pushed_at,
        license: None, // Gitea doesn't include license in repo list response
        homepage: repo.website.clone(),
        size_kb: Some(repo.size),
        metadata: serde_json::json!({
            "mirror": repo.mirror,
            "empty": repo.empty,
            "template": repo.template,
            "html_url": repo.html_url,
            "ssh_url": repo.ssh_url,
            "clone_url": repo.clone_url,
            "open_issues_count": repo.open_issues_count,
            "watchers_count": repo.watchers_count,
            "has_issues": repo.has_issues,
            "has_wiki": repo.has_wiki,
            "has_pull_requests": repo.has_pull_requests,
        }),
    }
}

/// Convert a Gitea repository directly to a CodeRepository active model.
///
/// Uses Codeberg as the default platform for backwards compatibility.
pub fn to_code_repository(repo: &GiteaRepo) -> CodeRepositoryActiveModel {
    to_code_repository_with_platform(repo, CodePlatform::Codeberg)
}

/// Convert a Gitea repository to a CodeRepository active model with specified platform.
pub fn to_code_repository_with_platform(
    repo: &GiteaRepo,
    platform: CodePlatform,
) -> CodeRepositoryActiveModel {
    let now = Utc::now().fixed_offset();
    let visibility = gitea_visibility(repo);

    // Serialize topics to JSON
    let topics_json = serde_json::json!(repo.topics);

    // Build platform-specific metadata
    let metadata = serde_json::json!({
        "mirror": repo.mirror,
        "empty": repo.empty,
        "template": repo.template,
        "html_url": repo.html_url,
        "ssh_url": repo.ssh_url,
        "clone_url": repo.clone_url,
        "open_issues_count": repo.open_issues_count,
        "watchers_count": repo.watchers_count,
        "has_issues": repo.has_issues,
        "has_wiki": repo.has_wiki,
        "has_pull_requests": repo.has_pull_requests,
    });

    CodeRepositoryActiveModel {
        id: Set(Uuid::new_v4()),
        platform: Set(platform),
        platform_id: Set(repo.id),
        owner: Set(repo.owner.login.clone()),
        name: Set(repo.name.clone()),
        description: Set(repo.description.clone()),
        default_branch: Set(repo
            .default_branch
            .clone()
            .unwrap_or_else(|| "main".to_string())),
        topics: Set(topics_json),
        primary_language: Set(repo.language.clone()),
        license_spdx: Set(None), // Not available in list endpoint
        homepage: Set(repo.website.clone()),
        visibility: Set(visibility),
        is_fork: Set(repo.fork),
        is_mirror: Set(repo.mirror),
        is_archived: Set(repo.archived),
        is_template: Set(repo.template),
        is_empty: Set(repo.empty),
        stars: Set(Some(repo.stars_count as i32)),
        forks: Set(Some(repo.forks_count as i32)),
        open_issues: Set(Some(repo.open_issues_count as i32)),
        watchers: Set(Some(repo.watchers_count as i32)),
        size_kb: Set(Some(repo.size as i64)),
        has_issues: Set(repo.has_issues),
        has_wiki: Set(repo.has_wiki),
        has_pull_requests: Set(repo.has_pull_requests),
        created_at: Set(Some(repo.created_at.fixed_offset())),
        updated_at: Set(Some(repo.updated_at.fixed_offset())),
        pushed_at: Set(repo.pushed_at.map(|t| t.fixed_offset())),
        synced_at: Set(now),
        platform_metadata: Set(metadata),
        etag: Set(None),
    }
}

/// Convert a PlatformRepo to a CodeRepository active model.
///
/// This is used by the PlatformClient trait implementation.
pub fn platform_repo_to_active_model(
    repo: &PlatformRepo,
    platform: CodePlatform,
) -> CodeRepositoryActiveModel {
    let now = Utc::now().fixed_offset();

    // Serialize topics to JSON
    let topics_json = serde_json::json!(repo.topics);

    // Extract metadata fields
    let metadata = &repo.metadata;
    let is_mirror = metadata
        .get("mirror")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let is_template = metadata
        .get("template")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let is_empty = metadata
        .get("empty")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let open_issues = metadata
        .get("open_issues_count")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);
    let watchers = metadata
        .get("watchers_count")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);
    let has_issues = metadata
        .get("has_issues")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let has_wiki = metadata
        .get("has_wiki")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let has_pull_requests = metadata
        .get("has_pull_requests")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    CodeRepositoryActiveModel {
        id: Set(Uuid::new_v4()),
        platform: Set(platform),
        platform_id: Set(repo.platform_id),
        owner: Set(repo.owner.clone()),
        name: Set(repo.name.clone()),
        description: Set(repo.description.clone()),
        default_branch: Set(repo.default_branch.clone()),
        topics: Set(topics_json),
        primary_language: Set(repo.language.clone()),
        license_spdx: Set(repo.license.clone()),
        homepage: Set(repo.homepage.clone()),
        visibility: Set(repo.visibility.clone()),
        is_fork: Set(repo.is_fork),
        is_mirror: Set(is_mirror),
        is_archived: Set(repo.is_archived),
        is_template: Set(is_template),
        is_empty: Set(is_empty),
        stars: Set(repo.stars.map(|v| v as i32)),
        forks: Set(repo.forks.map(|v| v as i32)),
        open_issues: Set(open_issues),
        watchers: Set(watchers),
        size_kb: Set(repo.size_kb.map(|v| v as i64)),
        has_issues: Set(has_issues),
        has_wiki: Set(has_wiki),
        has_pull_requests: Set(has_pull_requests),
        created_at: Set(repo.created_at.map(|t| t.fixed_offset())),
        updated_at: Set(repo.updated_at.map(|t| t.fixed_offset())),
        pushed_at: Set(repo.pushed_at.map(|t| t.fixed_offset())),
        synced_at: Set(now),
        platform_metadata: Set(repo.metadata.clone()),
        etag: Set(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gitea::types::GiteaUser;
    use chrono::TimeZone;

    fn mock_gitea_repo() -> GiteaRepo {
        GiteaRepo {
            id: 12345,
            name: "test-repo".to_string(),
            full_name: "test-org/test-repo".to_string(),
            description: Some("A test repository".to_string()),
            default_branch: Some("main".to_string()),
            private: false,
            fork: false,
            archived: false,
            mirror: false,
            empty: false,
            template: false,
            stars_count: 42,
            forks_count: 5,
            open_issues_count: 3,
            watchers_count: 10,
            size: 1024,
            language: Some("Rust".to_string()),
            topics: vec!["cli".to_string(), "tool".to_string()],
            created_at: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            updated_at: Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap(),
            pushed_at: Some(Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap()),
            owner: GiteaUser {
                id: 1,
                login: "test-org".to_string(),
                full_name: Some("Test Organization".to_string()),
                avatar_url: None,
            },
            html_url: "https://codeberg.org/test-org/test-repo".to_string(),
            ssh_url: Some("git@codeberg.org:test-org/test-repo.git".to_string()),
            clone_url: Some("https://codeberg.org/test-org/test-repo.git".to_string()),
            website: Some("https://example.com".to_string()),
            has_issues: true,
            has_wiki: true,
            has_pull_requests: true,
        }
    }

    #[test]
    fn test_to_platform_repo() {
        let gitea_repo = mock_gitea_repo();
        let platform_repo = to_platform_repo(&gitea_repo);

        assert_eq!(platform_repo.platform_id, 12345);
        assert_eq!(platform_repo.owner, "test-org");
        assert_eq!(platform_repo.name, "test-repo");
        assert_eq!(
            platform_repo.description,
            Some("A test repository".to_string())
        );
        assert_eq!(platform_repo.default_branch, "main");
        assert_eq!(platform_repo.visibility, CodeVisibility::Public);
        assert!(!platform_repo.is_fork);
        assert!(!platform_repo.is_archived);
        assert_eq!(platform_repo.stars, Some(42));
        assert_eq!(platform_repo.forks, Some(5));
        assert_eq!(platform_repo.language, Some("Rust".to_string()));
        assert_eq!(platform_repo.topics, vec!["cli", "tool"]);
        assert_eq!(
            platform_repo.homepage,
            Some("https://example.com".to_string())
        );
    }

    #[test]
    fn test_to_code_repository() {
        let gitea_repo = mock_gitea_repo();
        let active_model = to_code_repository(&gitea_repo);

        // Check key fields
        if let Set(platform) = active_model.platform {
            assert_eq!(platform, CodePlatform::Codeberg);
        } else {
            panic!("platform should be Set");
        }

        if let Set(ref owner) = active_model.owner {
            assert_eq!(owner, "test-org");
        } else {
            panic!("owner should be Set");
        }

        if let Set(ref name) = active_model.name {
            assert_eq!(name, "test-repo");
        } else {
            panic!("name should be Set");
        }

        if let Set(visibility) = active_model.visibility {
            assert_eq!(visibility, CodeVisibility::Public);
        } else {
            panic!("visibility should be Set");
        }
    }

    #[test]
    fn test_to_code_repository_with_gitea_platform() {
        let gitea_repo = mock_gitea_repo();
        let active_model = to_code_repository_with_platform(&gitea_repo, CodePlatform::Gitea);

        if let Set(platform) = active_model.platform {
            assert_eq!(platform, CodePlatform::Gitea);
        } else {
            panic!("platform should be Set");
        }
    }

    #[test]
    fn test_private_visibility() {
        let mut gitea_repo = mock_gitea_repo();
        gitea_repo.private = true;

        let active_model = to_code_repository(&gitea_repo);

        if let Set(visibility) = active_model.visibility {
            assert_eq!(visibility, CodeVisibility::Private);
        } else {
            panic!("visibility should be Set");
        }
    }

    #[test]
    fn test_platform_repo_to_active_model() {
        let gitea_repo = mock_gitea_repo();
        let platform_repo = to_platform_repo(&gitea_repo);
        let active_model = platform_repo_to_active_model(&platform_repo, CodePlatform::Codeberg);

        if let Set(platform) = active_model.platform {
            assert_eq!(platform, CodePlatform::Codeberg);
        }

        if let Set(ref owner) = active_model.owner {
            assert_eq!(owner, "test-org");
        }
    }
}
