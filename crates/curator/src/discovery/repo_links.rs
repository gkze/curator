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
        .to_string();
    if name.is_empty() {
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
        "about"
            | "apps"
            | "collections"
            | "dashboard"
            | "events"
            | "explore"
            | "features"
            | "groups"
            | "help"
            | "login"
            | "logout"
            | "marketplace"
            | "notifications"
            | "orgs"
            | "pricing"
            | "pulls"
            | "search"
            | "security"
            | "settings"
            | "site"
            | "signup"
            | "topics"
            | "users"
            | "wiki"
    )
}

fn is_trailing_segment(segment: &str) -> bool {
    let segment = segment.to_lowercase();

    matches!(
        segment.as_str(),
        "issues"
            | "pulls"
            | "merge_requests"
            | "merge-requests"
            | "wiki"
            | "tree"
            | "blob"
            | "commit"
            | "commits"
            | "compare"
            | "releases"
            | "tags"
            | "branches"
            | "actions"
            | "pipelines"
            | "jobs"
            | "activity"
            | "graphs"
            | "network"
            | "settings"
            | "security"
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
    fn test_normalize_host_strips_www() {
        assert_eq!(normalize_host("www.github.com"), "github.com");
        assert_eq!(normalize_host("gitlab.com"), "gitlab.com");
    }
}
