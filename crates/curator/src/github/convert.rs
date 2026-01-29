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
