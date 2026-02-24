use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepoLink {
    pub host: String,
    pub owner: String,
    pub name: String,
    pub original_url: String,
    pub canonical_url: String,
}

impl RepoLink {
    pub fn key(&self) -> String {
        format!(
            "{}/{}/{}",
            self.host.to_lowercase(),
            self.owner.to_lowercase(),
            self.name.to_lowercase()
        )
    }
}

pub fn extract_repo_link(url: &Url) -> Option<RepoLink> {
    let host = normalize_host(url.host_str()?);

    if is_non_repo_host(&host) {
        return None;
    }

    let segments: Vec<&str> = url.path_segments()?.filter(|s| !s.is_empty()).collect();

    if segments.len() < 2 {
        return None;
    }

    let mut relevant = segments;
    if let Some(index) = relevant
        .iter()
        .position(|segment| *segment == "-" || is_trailing_segment(segment))
    {
        relevant.truncate(index);
    }

    if relevant.len() < 2 {
        return None;
    }

    let raw_name = relevant.last()?;
    let name = raw_name
        .strip_suffix(".git")
        .unwrap_or(raw_name)
        // Strip trailing punctuation that may leak from markdown / HTML contexts
        .trim_end_matches([')', ':', ',', ';', '.', ']', '>', '"', '\''])
        .to_string();
    if name.is_empty() {
        return None;
    }

    // Reject names that are well-known non-repo file suffixes or GitHub artifacts
    if is_non_repo_name(&name) {
        return None;
    }

    let owner = relevant[..relevant.len() - 1].join("/");
    if owner.is_empty() {
        return None;
    }

    if is_reserved_owner(&owner) || is_reserved_segment(&name) {
        return None;
    }

    let canonical_url = format!("https://{}/{}/{}", host, owner, name);

    Some(RepoLink {
        host,
        owner,
        name,
        original_url: url.to_string(),
        canonical_url,
    })
}

pub fn normalize_host(host: &str) -> String {
    let host = host.trim_end_matches('.').to_lowercase();
    host.strip_prefix("www.").unwrap_or(&host).to_string()
}

fn is_reserved_owner(owner: &str) -> bool {
    owner
        .split('/')
        .next()
        .map(is_reserved_segment)
        .unwrap_or(false)
}

fn is_reserved_segment(segment: &str) -> bool {
    let segment = segment.to_lowercase();

    matches!(
        segment.as_str(),
        // GitHub / GitLab / Gitea top-level site pages
        "about"
            | "apps"
            | "collections"
            | "contact"
            | "customer-stories"
            | "dashboard"
            | "enterprise"
            | "events"
            | "explore"
            | "features"
            | "groups"
            | "help"
            | "issues"
            | "login"
            | "logout"
            | "marketplace"
            | "mcp"
            | "new"
            | "notifications"
            | "orgs"
            | "partners"
            | "premium-support"
            | "pricing"
            | "pulls"
            | "readme"
            | "resources"
            | "search"
            | "security"
            | "settings"
            | "site"
            | "site-policy"
            | "solutions"
            | "sponsors"
            | "signup"
            | "team"
            | "topics"
            | "trending"
            | "trust-center"
            | "user-attachments"
            | "users"
            | "why-github"
            | "wiki"
            // GitHub internal AJAX / fragment endpoints
            | "_view_fragments"
    )
}

/// Names that are never real repositories (RSS feeds, CI badges, JS artifacts, etc.)
fn is_non_repo_name(name: &str) -> bool {
    let name_lower = name.to_lowercase();
    matches!(
        name_lower.as_str(),
        "releases.atom" | "tags.atom" | "badge.svg" | "pull_request_layout" | "sbom" | "undefined"
    ) || name_lower.ends_with(".wiki")
}

fn is_trailing_segment(segment: &str) -> bool {
    let segment = segment.to_lowercase();

    matches!(
        segment.as_str(),
        // Issue / PR trackers
        "issues"
            | "pulls"
            | "pull"
            | "merge_requests"
            | "merge-requests"
            // Source browsing
            | "wiki"
            | "tree"
            | "blob"
            | "raw"
            | "commit"
            | "commits"
            | "compare"
            | "blame"
            | "edit"
            // Releases & refs
            | "releases"
            | "tags"
            | "branches"
            | "archive"
            // CI / CD
            | "actions"
            | "pipelines"
            | "jobs"
            | "deployments"
            | "environments"
            // Social / meta pages
            | "forks"
            | "stargazers"
            | "watchers"
            | "contributors"
            | "community"
            | "discussions"
            // Project management
            | "projects"
            | "milestones"
            | "labels"
            // Analytics
            | "activity"
            | "graphs"
            | "network"
            | "pulse"
            // Settings & admin
            | "settings"
            | "security"
            | "custom-properties"
            | "models"
            // Misc GitHub/GitLab/Gitea UI pages
            | "packages"
            | "codespaces"
            | "pages"
            | "insights"
            | "assets"
            | "suites"
            | "runs"
            | "invitations"
            | "workflows"
            | "dependency-graph"
            | "rules"
    )
}

/// Hosts that look like they belong to a forge but never host repositories
/// (CDN, image proxies, API endpoints, documentation sites, etc.).
fn is_non_repo_host(host: &str) -> bool {
    matches!(
        host,
        // GitHub CDN / image proxies
        "camo.githubusercontent.com"
            | "private-user-images.githubusercontent.com"
            | "user-images.githubusercontent.com"
            | "raw.githubusercontent.com"
            | "avatars.githubusercontent.com"
            | "objects.githubusercontent.com"
            | "repository-images.githubusercontent.com"
            // GitHub non-repo subdomains
            | "api.github.com"
            | "docs.github.com"
            | "education.github.com"
            | "gist.github.com"
            | "pages.github.com"
            | "status.github.com"
            | "resources.github.com"
            | "community.github.com"
            | "desktop.github.com"
            | "cli.github.com"
            | "copilot.github.com"
            | "training.github.com"
            | "partner.github.com"
            | "support.github.com"
            // GitHub assets / tracking / OpenGraph
            | "github.githubassets.com"
            | "opengraph.githubassets.com"
            | "collector.github.com"
            // Badge / shield services
            | "img.shields.io"
            // Package registries
            | "registry.npmjs.org"
            // Social media image CDNs
            | "pbs.twimg.com"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_github_repo() {
        let url = Url::parse("https://github.com/rust-lang/rust").unwrap();
        let repo = extract_repo_link(&url).expect("should parse repo");

        assert_eq!(repo.host, "github.com");
        assert_eq!(repo.owner, "rust-lang");
        assert_eq!(repo.name, "rust");
    }

    #[test]
    fn test_extract_gitlab_nested_repo() {
        let url = Url::parse("https://gitlab.com/group/subgroup/project").unwrap();
        let repo = extract_repo_link(&url).expect("should parse repo");

        assert_eq!(repo.host, "gitlab.com");
        assert_eq!(repo.owner, "group/subgroup");
        assert_eq!(repo.name, "project");
    }

    #[test]
    fn test_extract_repo_with_dash_segment() {
        let url = Url::parse("https://gitlab.com/group/subgroup/project/-/issues").unwrap();
        let repo = extract_repo_link(&url).expect("should parse repo");

        assert_eq!(repo.owner, "group/subgroup");
        assert_eq!(repo.name, "project");
    }

    #[test]
    fn test_extract_repo_with_github_issues() {
        let url = Url::parse("https://github.com/owner/repo/issues").unwrap();
        let repo = extract_repo_link(&url).expect("should parse repo");

        assert_eq!(repo.owner, "owner");
        assert_eq!(repo.name, "repo");
    }

    #[test]
    fn test_extract_repo_with_gitea_pulls() {
        let url = Url::parse("https://codeberg.org/owner/repo/pulls").unwrap();
        let repo = extract_repo_link(&url).expect("should parse repo");

        assert_eq!(repo.owner, "owner");
        assert_eq!(repo.name, "repo");
    }

    #[test]
    fn test_extract_repo_with_blob_path() {
        let url = Url::parse("https://github.com/owner/repo/blob/main/README.md").unwrap();
        let repo = extract_repo_link(&url).expect("should parse repo");

        assert_eq!(repo.owner, "owner");
        assert_eq!(repo.name, "repo");
    }

    #[test]
    fn test_extract_repo_strips_git_suffix() {
        let url = Url::parse("https://github.com/rust-lang/rust.git").unwrap();
        let repo = extract_repo_link(&url).expect("should parse repo");

        assert_eq!(repo.name, "rust");
    }

    #[test]
    fn test_extract_repo_skips_reserved_paths() {
        let url = Url::parse("https://github.com/about").unwrap();
        assert!(extract_repo_link(&url).is_none());
    }

    #[test]
    fn test_extract_repo_skips_solutions_paths() {
        let url =
            Url::parse("https://github.com/solutions/use-case/aka.ms/ghcp-appmod/blog").unwrap();
        assert!(extract_repo_link(&url).is_none());
    }

    #[test]
    fn test_extract_repo_skips_partners_paths() {
        let url = Url::parse("https://github.com/partners/resources").unwrap();
        assert!(extract_repo_link(&url).is_none());
    }

    #[test]
    fn test_normalize_host_strips_www() {
        assert_eq!(normalize_host("www.github.com"), "github.com");
        assert_eq!(normalize_host("gitlab.com"), "gitlab.com");
    }

    // --- Non-repo host tests ---

    #[test]
    fn test_non_repo_host_github_cdn_domains() {
        let cdn_hosts = [
            "camo.githubusercontent.com",
            "private-user-images.githubusercontent.com",
            "user-images.githubusercontent.com",
            "raw.githubusercontent.com",
            "avatars.githubusercontent.com",
            "objects.githubusercontent.com",
            "repository-images.githubusercontent.com",
        ];
        for host in cdn_hosts {
            let url = Url::parse(&format!("https://{}/owner/repo", host)).unwrap();
            assert!(
                extract_repo_link(&url).is_none(),
                "expected None for CDN host: {}",
                host,
            );
        }
    }

    #[test]
    fn test_non_repo_host_github_subdomains() {
        let subdomains = [
            "api.github.com",
            "docs.github.com",
            "education.github.com",
            "gist.github.com",
            "pages.github.com",
            "status.github.com",
            "resources.github.com",
            "desktop.github.com",
            "cli.github.com",
            "copilot.github.com",
        ];
        for host in subdomains {
            let url = Url::parse(&format!("https://{}/owner/repo", host)).unwrap();
            assert!(
                extract_repo_link(&url).is_none(),
                "expected None for subdomain: {}",
                host,
            );
        }
    }

    #[test]
    fn test_non_repo_host_assets_and_tracking() {
        let hosts = [
            "github.githubassets.com",
            "opengraph.githubassets.com",
            "collector.github.com",
            "img.shields.io",
            "pbs.twimg.com",
            "registry.npmjs.org",
        ];
        for host in hosts {
            let url = Url::parse(&format!("https://{}/owner/repo", host)).unwrap();
            assert!(
                extract_repo_link(&url).is_none(),
                "expected None for host: {}",
                host,
            );
        }
    }

    #[test]
    fn test_non_repo_host_does_not_block_real_repos() {
        // Ensure github.com itself is NOT blocked
        let url = Url::parse("https://github.com/rust-lang/rust").unwrap();
        assert!(extract_repo_link(&url).is_some());

        let url = Url::parse("https://gitlab.com/group/project").unwrap();
        assert!(extract_repo_link(&url).is_some());

        let url = Url::parse("https://codeberg.org/owner/repo").unwrap();
        assert!(extract_repo_link(&url).is_some());
    }

    // --- Trailing segment tests ---

    #[test]
    fn test_trailing_segments_are_stripped() {
        let trailing = [
            "issues",
            "pulls",
            "pull",
            "merge_requests",
            "wiki",
            "tree",
            "blob",
            "raw",
            "commit",
            "commits",
            "compare",
            "blame",
            "edit",
            "releases",
            "tags",
            "branches",
            "archive",
            "actions",
            "pipelines",
            "jobs",
            "deployments",
            "environments",
            "forks",
            "stargazers",
            "watchers",
            "contributors",
            "community",
            "discussions",
            "projects",
            "milestones",
            "labels",
            "activity",
            "graphs",
            "network",
            "pulse",
            "settings",
            "security",
            "custom-properties",
            "models",
            "packages",
            "codespaces",
            "pages",
            "insights",
            "assets",
            "suites",
            "runs",
            "invitations",
        ];

        for seg in trailing {
            let url = Url::parse(&format!("https://github.com/owner/repo/{}", seg)).unwrap();
            let repo = extract_repo_link(&url);
            assert!(
                repo.is_some(),
                "should extract repo from URL with trailing segment: {}",
                seg,
            );
            let repo = repo.unwrap();
            assert_eq!(repo.owner, "owner", "wrong owner for segment: {}", seg);
            assert_eq!(repo.name, "repo", "wrong name for segment: {}", seg);
        }
    }

    #[test]
    fn test_trailing_segments_with_deeper_paths() {
        // /owner/repo/pull/123 -> owner/repo
        let url = Url::parse("https://github.com/owner/repo/pull/123").unwrap();
        let repo = extract_repo_link(&url).unwrap();
        assert_eq!(repo.owner, "owner");
        assert_eq!(repo.name, "repo");

        // /owner/repo/issues/456/comments -> owner/repo
        let url = Url::parse("https://github.com/owner/repo/issues/456/comments").unwrap();
        let repo = extract_repo_link(&url).unwrap();
        assert_eq!(repo.owner, "owner");
        assert_eq!(repo.name, "repo");

        // /owner/repo/tree/main/src/lib.rs -> owner/repo
        let url = Url::parse("https://github.com/owner/repo/tree/main/src/lib.rs").unwrap();
        let repo = extract_repo_link(&url).unwrap();
        assert_eq!(repo.owner, "owner");
        assert_eq!(repo.name, "repo");
    }

    // --- Reserved segment tests ---

    #[test]
    fn test_reserved_segments_are_rejected() {
        let reserved = [
            "about",
            "apps",
            "collections",
            "contact",
            "customer-stories",
            "dashboard",
            "enterprise",
            "events",
            "explore",
            "features",
            "groups",
            "help",
            "issues",
            "login",
            "logout",
            "marketplace",
            "mcp",
            "new",
            "notifications",
            "orgs",
            "partners",
            "premium-support",
            "pricing",
            "pulls",
            "readme",
            "resources",
            "search",
            "security",
            "settings",
            "site",
            "site-policy",
            "solutions",
            "sponsors",
            "signup",
            "team",
            "topics",
            "trending",
            "trust-center",
            "user-attachments",
            "users",
            "why-github",
            "wiki",
        ];

        for seg in reserved {
            // As an owner segment: /reserved/something -> None
            let url = Url::parse(&format!("https://github.com/{}/something", seg)).unwrap();
            assert!(
                extract_repo_link(&url).is_none(),
                "expected None for reserved owner segment: {}",
                seg,
            );
        }
    }

    #[test]
    fn test_reserved_segment_as_repo_name_rejected() {
        // /validowner/about -> None (about is reserved as a name too)
        let url = Url::parse("https://github.com/validowner/about").unwrap();
        assert!(extract_repo_link(&url).is_none());
    }

    // --- RepoLink key tests ---

    #[test]
    fn test_repo_link_key_is_lowercase() {
        let repo = RepoLink {
            host: "GitHub.Com".to_string(),
            owner: "Owner".to_string(),
            name: "Repo".to_string(),
            original_url: "https://GitHub.Com/Owner/Repo".to_string(),
            canonical_url: "https://github.com/owner/repo".to_string(),
        };
        assert_eq!(repo.key(), "github.com/owner/repo");
    }

    // --- Single-segment path tests ---

    #[test]
    fn test_single_segment_path_rejected() {
        let url = Url::parse("https://github.com/user").unwrap();
        assert!(extract_repo_link(&url).is_none());
    }

    #[test]
    fn test_empty_name_rejected() {
        let url = Url::parse("https://github.com/user/").unwrap();
        assert!(extract_repo_link(&url).is_none());
    }

    // --- Non-repo name tests ---

    #[test]
    fn test_non_repo_names_rejected() {
        let non_repo_names = [
            "releases.atom",
            "tags.atom",
            "badge.svg",
            "pull_request_layout",
            "sbom",
            "undefined",
        ];

        for name in non_repo_names {
            let url = Url::parse(&format!("https://github.com/owner/{}", name)).unwrap();
            assert!(
                extract_repo_link(&url).is_none(),
                "expected None for non-repo name: {}",
                name,
            );
        }
    }

    #[test]
    fn test_wiki_suffix_rejected() {
        let url = Url::parse("https://github.com/owner/repo.wiki").unwrap();
        assert!(extract_repo_link(&url).is_none());
    }

    #[test]
    fn test_view_fragments_rejected() {
        let url = Url::parse("https://github.com/_view_fragments/voltron/pull_requests/show/owner/repo/10/pull_request_layout").unwrap();
        assert!(extract_repo_link(&url).is_none());
    }

    #[test]
    fn test_workflows_badge_stripped() {
        // /owner/repo/workflows/ci/badge.svg -> should extract owner/repo
        let url = Url::parse("https://github.com/owner/repo/workflows/ci/badge.svg").unwrap();
        let repo = extract_repo_link(&url).unwrap();
        assert_eq!(repo.owner, "owner");
        assert_eq!(repo.name, "repo");
    }

    #[test]
    fn test_dependency_graph_stripped() {
        let url = Url::parse("https://github.com/owner/repo/dependency-graph/sbom").unwrap();
        let repo = extract_repo_link(&url).unwrap();
        assert_eq!(repo.owner, "owner");
        assert_eq!(repo.name, "repo");
    }

    #[test]
    fn test_rules_trailing_stripped() {
        let url = Url::parse("https://github.com/owner/repo/rules").unwrap();
        let repo = extract_repo_link(&url).unwrap();
        assert_eq!(repo.owner, "owner");
        assert_eq!(repo.name, "repo");
    }

    #[test]
    fn test_trailing_punctuation_stripped_from_name() {
        // Simulate URL with trailing punctuation that might leak from markdown
        let url = Url::parse("https://github.com/owner/repo):").unwrap();
        // URL parsing may handle this differently, test the extraction logic
        if let Some(repo) = extract_repo_link(&url) {
            assert!(
                !repo.name.ends_with("):"),
                "name should not end with '):', got: {}",
                repo.name,
            );
            assert!(
                !repo.name.ends_with(')'),
                "name should not end with ')', got: {}",
                repo.name,
            );
        }
    }
}
