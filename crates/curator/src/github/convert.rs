//! Model conversion from GitHub API types to curator entities.

use chrono::Utc;
use octocrab::models::Repository as GitHubRepo;
use sea_orm::Set;
use uuid::Uuid;

use crate::entity::code_platform::CodePlatform;
use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::entity::code_visibility::CodeVisibility;
use crate::platform::PlatformRepo;

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
pub fn to_code_repository(repo: &GitHubRepo) -> CodeRepositoryActiveModel {
    let now = Utc::now().fixed_offset();

    let owner = repo
        .owner
        .as_ref()
        .map(|o| o.login.clone())
        .unwrap_or_default();

    let name = repo.name.clone();

    let visibility = github_visibility(repo);

    let topics = serde_json::json!(repo.topics.clone().unwrap_or_default());

    let primary_language = repo
        .language
        .as_ref()
        .and_then(|v| v.as_str().map(String::from));

    let license_spdx = repo.license.as_ref().map(|l| l.spdx_id.clone());

    let platform_metadata = serde_json::json!({
        "node_id": repo.node_id,
        "private": repo.private,
        "allow_squash_merge": repo.allow_squash_merge,
        "allow_merge_commit": repo.allow_merge_commit,
        "allow_rebase_merge": repo.allow_rebase_merge,
        "allow_auto_merge": repo.allow_auto_merge,
        "delete_branch_on_merge": repo.delete_branch_on_merge,
    });

    CodeRepositoryActiveModel {
        id: Set(Uuid::new_v4()),
        platform: Set(CodePlatform::GitHub),
        platform_id: Set(repo.id.0 as i64),
        owner: Set(owner),
        name: Set(name),
        description: Set(repo.description.clone()),
        default_branch: Set(repo
            .default_branch
            .clone()
            .unwrap_or_else(|| "main".to_string())),
        topics: Set(topics),
        primary_language: Set(primary_language),
        license_spdx: Set(license_spdx),
        homepage: Set(repo.homepage.clone()),
        visibility: Set(visibility),
        is_fork: Set(repo.fork.unwrap_or(false)),
        is_mirror: Set(repo.mirror_url.is_some()),
        is_archived: Set(repo.archived.unwrap_or(false)),
        is_template: Set(repo.is_template.unwrap_or(false)),
        is_empty: Set(false),
        stars: Set(repo.stargazers_count.map(|c| c as i32)),
        forks: Set(repo.forks_count.map(|c| c as i32)),
        open_issues: Set(repo.open_issues_count.map(|c| c as i32)),
        watchers: Set(repo.watchers_count.map(|c| c as i32)),
        size_kb: Set(repo.size.map(|s| s as i64)),
        has_issues: Set(repo.has_issues.unwrap_or(true)),
        has_wiki: Set(repo.has_wiki.unwrap_or(true)),
        has_pull_requests: Set(true),
        created_at: Set(repo.created_at.map(|t| t.fixed_offset())),
        updated_at: Set(repo.updated_at.map(|t| t.fixed_offset())),
        pushed_at: Set(repo.pushed_at.map(|t| t.fixed_offset())),
        platform_metadata: Set(platform_metadata),
        synced_at: Set(now),
        etag: Set(None),
    }
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

    let platform_metadata = serde_json::json!({
        "node_id": repo.node_id,
        "private": repo.private,
        "allow_squash_merge": repo.allow_squash_merge,
        "allow_merge_commit": repo.allow_merge_commit,
        "allow_rebase_merge": repo.allow_rebase_merge,
        "allow_auto_merge": repo.allow_auto_merge,
        "delete_branch_on_merge": repo.delete_branch_on_merge,
    });

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

/// Convert a PlatformRepo to a CodeRepository active model for GitHub.
pub fn platform_repo_to_active_model(repo: &PlatformRepo) -> CodeRepositoryActiveModel {
    let now = Utc::now().fixed_offset();
    let topics = serde_json::json!(repo.topics);

    CodeRepositoryActiveModel {
        id: Set(Uuid::new_v4()),
        platform: Set(CodePlatform::GitHub),
        platform_id: Set(repo.platform_id),
        owner: Set(repo.owner.clone()),
        name: Set(repo.name.clone()),
        description: Set(repo.description.clone()),
        default_branch: Set(repo.default_branch.clone()),
        topics: Set(topics),
        primary_language: Set(repo.language.clone()),
        license_spdx: Set(repo.license.clone()),
        homepage: Set(repo.homepage.clone()),
        visibility: Set(repo.visibility.clone()),
        is_fork: Set(repo.is_fork),
        is_mirror: Set(false), // Not available in PlatformRepo
        is_archived: Set(repo.is_archived),
        is_template: Set(false), // Not available in PlatformRepo
        is_empty: Set(false),
        stars: Set(repo.stars.map(|c| c as i32)),
        forks: Set(repo.forks.map(|c| c as i32)),
        open_issues: Set(None), // Not available in PlatformRepo
        watchers: Set(None),    // Not available in PlatformRepo
        size_kb: Set(repo.size_kb.map(|s| s as i64)),
        has_issues: Set(true),
        has_wiki: Set(true),
        has_pull_requests: Set(true),
        created_at: Set(repo.created_at.map(|t| t.fixed_offset())),
        updated_at: Set(repo.updated_at.map(|t| t.fixed_offset())),
        pushed_at: Set(repo.pushed_at.map(|t| t.fixed_offset())),
        platform_metadata: Set(repo.metadata.clone()),
        synced_at: Set(now),
        etag: Set(None),
    }
}
