//! Model conversion from GitLab API types to curator entities.

use uuid::Uuid;

use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::code_visibility::CodeVisibility;
use crate::platform::{PlatformRepo, strip_null_values};

use super::types::GitLabProject;

/// Determine visibility from a GitLab project.
///
/// GitLab has three visibility levels: public, private, and internal.
fn gitlab_visibility(project: &GitLabProject) -> CodeVisibility {
    match project.visibility.as_str() {
        "private" => CodeVisibility::Private,
        "internal" => CodeVisibility::Internal,
        _ => CodeVisibility::Public,
    }
}

/// Convert a GitLab project to a CodeRepository active model.
pub fn to_code_repository(project: &GitLabProject, instance_id: Uuid) -> CodeRepositoryActiveModel {
    let platform_repo = to_platform_repo(project);
    platform_repo.to_active_model(instance_id)
}

/// Convert a GitLab project to a platform-agnostic PlatformRepo.
pub fn to_platform_repo(project: &GitLabProject) -> PlatformRepo {
    // Extract owner from path_with_namespace (e.g., "group/subgroup/project" → "group/subgroup")
    let owner = project
        .path_with_namespace
        .rsplit_once('/')
        .map(|(ns, _): (&str, &str)| ns.to_string())
        .unwrap_or_else(|| project.namespace.full_path.clone());

    // Build platform-specific metadata (strip nulls to reduce storage)
    let platform_metadata = strip_null_values(serde_json::json!({
        "web_url": project.web_url,
        "ssh_url_to_repo": project.ssh_url_to_repo,
        "http_url_to_repo": project.http_url_to_repo,
        "namespace_id": project.namespace.id,
        "namespace_kind": project.namespace.kind,
        "namespace_path": project.namespace.path,
        "mirror": project.mirror,
        "path": project.path,
        "open_issues_count": project.open_issues_count,
        "has_issues": project.issues_enabled,
        "has_wiki": project.wiki_enabled,
        "has_pull_requests": project.merge_requests_enabled,
    }));

    PlatformRepo {
        platform_id: project.id as i64,
        owner,
        name: project.name.clone(),
        description: project.description.clone(),
        default_branch: project
            .default_branch
            .clone()
            .unwrap_or_else(|| "main".to_string()),
        visibility: gitlab_visibility(project),
        is_fork: project.forked_from_project.is_some(),
        is_archived: project.archived,
        stars: Some(project.star_count),
        forks: Some(project.forks_count),
        language: None, // GitLab doesn't return this in list endpoint
        topics: project.topics.clone(),
        created_at: Some(project.created_at),
        updated_at: Some(project.last_activity_at),
        pushed_at: None, // GitLab uses last_activity_at instead
        license: None,   // Not included in list response
        homepage: None,  // GitLab projects don't have a separate homepage
        size_kb: None,   // Would need statistics=true parameter
        metadata: platform_metadata,
    }
}

/// Convert a PlatformRepo to a CodeRepository active model for GitLab.
pub fn platform_repo_to_active_model(
    repo: &PlatformRepo,
    instance_id: Uuid,
) -> CodeRepositoryActiveModel {
    repo.to_active_model(instance_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};

    /// Test instance ID for GitLab conversion tests.
    fn test_instance_id() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    }

    /// Create a mock GitLabProject for testing.
    fn mock_project() -> GitLabProject {
        use super::super::types::{GitLabNamespace, GitLabProject};

        GitLabProject {
            id: 12345,
            name: "test-project".to_string(),
            path: "test-project".to_string(),
            path_with_namespace: "test-group/subgroup/test-project".to_string(),
            description: Some("A test project".to_string()),
            default_branch: Some("main".to_string()),
            visibility: "public".to_string(),
            archived: false,
            topics: vec!["rust".to_string(), "api".to_string()],
            star_count: 42,
            forks_count: 5,
            open_issues_count: Some(10),
            created_at: Utc::now() - Duration::days(365),
            last_activity_at: Utc::now() - Duration::days(1),
            namespace: GitLabNamespace {
                id: 1,
                name: "subgroup".to_string(),
                path: "subgroup".to_string(),
                full_path: "test-group/subgroup".to_string(),
                kind: "group".to_string(),
            },
            forked_from_project: None,
            mirror: Some(false),
            issues_enabled: Some(true),
            wiki_enabled: Some(false),
            merge_requests_enabled: Some(true),
            web_url: "https://gitlab.com/test-group/subgroup/test-project".to_string(),
            ssh_url_to_repo: Some(
                "git@gitlab.com:test-group/subgroup/test-project.git".to_string(),
            ),
            http_url_to_repo: Some(
                "https://gitlab.com/test-group/subgroup/test-project.git".to_string(),
            ),
        }
    }

    #[test]
    fn test_to_code_repository_basic_fields() {
        let project = mock_project();
        let model = to_code_repository(&project, test_instance_id());

        assert_eq!(model.instance_id.as_ref(), &test_instance_id());
        assert_eq!(model.platform_id.as_ref(), &12345i64);
        assert_eq!(model.owner.as_ref(), "test-group/subgroup");
        assert_eq!(model.name.as_ref(), "test-project");
        assert_eq!(
            model.description.as_ref(),
            &Some("A test project".to_string())
        );
        assert_eq!(model.default_branch.as_ref(), "main");
    }

    #[test]
    fn test_to_code_repository_visibility() {
        let mut project = mock_project();

        project.visibility = "public".to_string();
        let model = to_code_repository(&project, test_instance_id());
        assert_eq!(model.visibility.as_ref(), &CodeVisibility::Public);

        project.visibility = "private".to_string();
        let model = to_code_repository(&project, test_instance_id());
        assert_eq!(model.visibility.as_ref(), &CodeVisibility::Private);

        project.visibility = "internal".to_string();
        let model = to_code_repository(&project, test_instance_id());
        assert_eq!(model.visibility.as_ref(), &CodeVisibility::Internal);
    }

    #[test]
    fn test_to_code_repository_stats() {
        let project = mock_project();
        let model = to_code_repository(&project, test_instance_id());

        assert_eq!(model.stars.as_ref(), &Some(42));
        assert_eq!(model.forks.as_ref(), &Some(5));
        assert_eq!(model.open_issues.as_ref(), &Some(10));
    }

    #[test]
    fn test_to_code_repository_features() {
        let project = mock_project();
        let model = to_code_repository(&project, test_instance_id());

        assert_eq!(model.has_issues.as_ref(), &true);
        assert_eq!(model.has_wiki.as_ref(), &false);
        assert_eq!(model.has_pull_requests.as_ref(), &true);
    }

    #[test]
    fn test_to_code_repository_fork() {
        use super::super::types::ForkedFrom;

        let mut project = mock_project();

        // Not a fork
        let model = to_code_repository(&project, test_instance_id());
        assert_eq!(model.is_fork.as_ref(), &false);

        // Is a fork
        project.forked_from_project = Some(Box::new(ForkedFrom { id: 999 }));
        let model = to_code_repository(&project, test_instance_id());
        assert_eq!(model.is_fork.as_ref(), &true);
    }

    #[test]
    fn test_to_code_repository_topics() {
        let project = mock_project();
        let model = to_code_repository(&project, test_instance_id());

        let topics: Vec<String> =
            serde_json::from_value(model.topics.as_ref().clone()).expect("valid JSON");
        assert_eq!(topics, vec!["rust", "api"]);
    }

    #[test]
    fn test_to_code_repository_metadata() {
        let project = mock_project();
        let model = to_code_repository(&project, test_instance_id());

        let metadata = model.platform_metadata.as_ref();
        assert_eq!(
            metadata["web_url"],
            "https://gitlab.com/test-group/subgroup/test-project"
        );
        assert_eq!(metadata["namespace_kind"], "group");
    }

    #[test]
    fn test_to_code_repository_missing_description() {
        let mut project = mock_project();
        project.description = None;

        let model = to_code_repository(&project, test_instance_id());
        assert_eq!(model.description.as_ref(), &None);
    }

    #[test]
    fn test_to_code_repository_missing_default_branch() {
        let mut project = mock_project();
        project.default_branch = None;

        let model = to_code_repository(&project, test_instance_id());
        // Should default to "main" when not provided
        assert_eq!(model.default_branch.as_ref(), "main");
    }

    #[test]
    fn test_to_code_repository_unknown_visibility() {
        let mut project = mock_project();
        project.visibility = "unknown_value".to_string();

        let model = to_code_repository(&project, test_instance_id());
        // Unknown visibility should default to Public
        assert_eq!(model.visibility.as_ref(), &CodeVisibility::Public);
    }

    #[test]
    fn test_to_code_repository_single_level_namespace() {
        use super::super::types::GitLabNamespace;

        let mut project = mock_project();
        // Single-level path: "group/project" (no subgroup)
        project.path_with_namespace = "simple-group/test-project".to_string();
        project.namespace = GitLabNamespace {
            id: 1,
            name: "simple-group".to_string(),
            path: "simple-group".to_string(),
            full_path: "simple-group".to_string(),
            kind: "group".to_string(),
        };

        let model = to_code_repository(&project, test_instance_id());
        assert_eq!(model.owner.as_ref(), "simple-group");
    }

    #[test]
    fn test_to_code_repository_user_namespace() {
        use super::super::types::GitLabNamespace;

        let mut project = mock_project();
        project.path_with_namespace = "johndoe/my-project".to_string();
        project.namespace = GitLabNamespace {
            id: 1,
            name: "johndoe".to_string(),
            path: "johndoe".to_string(),
            full_path: "johndoe".to_string(),
            kind: "user".to_string(),
        };

        let model = to_code_repository(&project, test_instance_id());
        assert_eq!(model.owner.as_ref(), "johndoe");
    }

    #[test]
    fn test_to_code_repository_empty_topics() {
        let mut project = mock_project();
        project.topics = vec![];

        let model = to_code_repository(&project, test_instance_id());
        let topics: Vec<String> =
            serde_json::from_value(model.topics.as_ref().clone()).expect("valid JSON");
        assert!(topics.is_empty());
    }

    #[test]
    fn test_to_code_repository_archived() {
        let mut project = mock_project();
        project.archived = true;

        let model = to_code_repository(&project, test_instance_id());
        assert_eq!(model.is_archived.as_ref(), &true);
    }

    #[test]
    fn test_to_code_repository_timestamps() {
        let project = mock_project();
        let model = to_code_repository(&project, test_instance_id());

        // created_at and updated_at should be set
        assert!(model.created_at.as_ref().is_some());
        assert!(model.updated_at.as_ref().is_some());
    }

    #[test]
    fn test_to_platform_repo_fields_not_in_gitlab_api() {
        let project = mock_project();
        let platform_repo = to_platform_repo(&project);

        // These fields are not returned by GitLab list endpoint
        assert!(platform_repo.language.is_none());
        assert!(platform_repo.pushed_at.is_none());
        assert!(platform_repo.license.is_none());
        assert!(platform_repo.homepage.is_none());
        assert!(platform_repo.size_kb.is_none());
    }

    #[test]
    fn test_platform_repo_to_active_model() {
        let project = mock_project();
        let platform_repo = to_platform_repo(&project);
        let model = platform_repo_to_active_model(&platform_repo, test_instance_id());

        assert_eq!(model.instance_id.as_ref(), &test_instance_id());
        assert_eq!(model.name.as_ref(), "test-project");
    }
}
