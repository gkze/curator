//! Model conversion functions for Gitea repositories.

use uuid::Uuid;

use super::types::GiteaRepo;
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::code_visibility::CodeVisibility;
use crate::platform::{PlatformRepo, strip_null_values};

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
        metadata: strip_null_values(serde_json::json!({
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
        })),
    }
}

/// Convert a Gitea repository directly to a CodeRepository active model.
///
/// # Arguments
///
/// * `repo` - The Gitea repository to convert
/// * `instance_id` - The instance ID to associate with the repository
pub fn to_code_repository(repo: &GiteaRepo, instance_id: Uuid) -> CodeRepositoryActiveModel {
    let platform_repo = to_platform_repo(repo);
    platform_repo.to_active_model(instance_id)
}

/// Convert a PlatformRepo to a CodeRepository active model.
///
/// This is used by the PlatformClient trait implementation.
pub fn platform_repo_to_active_model(
    repo: &PlatformRepo,
    instance_id: Uuid,
) -> CodeRepositoryActiveModel {
    repo.to_active_model(instance_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gitea::types::GiteaUser;
    use chrono::{TimeZone, Utc};
    use sea_orm::Set;

    /// Returns a consistent test instance ID for unit tests.
    fn test_instance_id() -> Uuid {
        Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap()
    }

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
        let instance_id = test_instance_id();
        let active_model = to_code_repository(&gitea_repo, instance_id);

        // Check key fields
        if let Set(id) = active_model.instance_id {
            assert_eq!(id, instance_id);
        } else {
            panic!("instance_id should be Set");
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
    fn test_private_visibility() {
        let mut gitea_repo = mock_gitea_repo();
        gitea_repo.private = true;

        let instance_id = test_instance_id();
        let active_model = to_code_repository(&gitea_repo, instance_id);

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
        let instance_id = test_instance_id();
        let active_model = platform_repo_to_active_model(&platform_repo, instance_id);

        if let Set(id) = active_model.instance_id {
            assert_eq!(id, instance_id);
        }

        if let Set(ref owner) = active_model.owner {
            assert_eq!(owner, "test-org");
        }
    }
}
