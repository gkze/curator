//! GitHub API data types.

use chrono::{DateTime, Utc};
use octocrab::models::Repository as GitHubRepo;
use serde::{Deserialize, Serialize};

// Re-export shared constants from sync module
pub use crate::sync::{
    DEFAULT_CONCURRENCY, DEFAULT_PAGE_FETCH_CONCURRENCY, DEFAULT_STAR_CONCURRENCY,
};

// Re-export shared types from sync module
pub use crate::sync::{ProgressCallback, SyncProgress, emit};

/// A single rate limit resource entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitResource {
    /// Maximum requests allowed per period.
    pub limit: usize,
    /// Requests used in current period.
    pub used: usize,
    /// Remaining requests in current period.
    pub remaining: usize,
    /// Unix timestamp when the rate limit resets.
    pub reset: u64,
}

impl RateLimitResource {
    /// Get the reset time as a DateTime.
    pub fn reset_at(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.reset as i64, 0).unwrap_or_else(Utc::now)
    }
}

/// All rate limit resources from GitHub's API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitHubRateLimits {
    /// Core API rate limit (non-search REST endpoints).
    pub core: RateLimitResource,
    /// Search API rate limit.
    pub search: RateLimitResource,
    /// Code search API rate limit.
    #[serde(default)]
    pub code_search: Option<RateLimitResource>,
    /// GraphQL API rate limit.
    #[serde(default)]
    pub graphql: Option<RateLimitResource>,
    /// Integration manifest API rate limit.
    #[serde(default)]
    pub integration_manifest: Option<RateLimitResource>,
    /// Source import API rate limit.
    #[serde(default)]
    pub source_import: Option<RateLimitResource>,
    /// Code scanning upload rate limit.
    #[serde(default)]
    pub code_scanning_upload: Option<RateLimitResource>,
    /// Actions runner registration rate limit.
    #[serde(default)]
    pub actions_runner_registration: Option<RateLimitResource>,
    /// SCIM API rate limit.
    #[serde(default)]
    pub scim: Option<RateLimitResource>,
    /// Dependency snapshots rate limit.
    #[serde(default)]
    pub dependency_snapshots: Option<RateLimitResource>,
    /// Audit log rate limit.
    #[serde(default)]
    pub audit_log: Option<RateLimitResource>,
    /// Code scanning autofix rate limit.
    #[serde(default)]
    pub code_scanning_autofix: Option<RateLimitResource>,
}

/// Full rate limit response from GitHub's API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitHubRateLimitResponse {
    /// All rate limit resources.
    pub resources: GitHubRateLimits,
}

/// Helper trait to extract owner and name from a GitHub repository.
pub trait RepoIdentity {
    /// Get the owner login (user or organization).
    fn owner_login(&self) -> String;
    /// Get the repository name.
    fn repo_name(&self) -> String;
    /// Get both owner and name as a tuple.
    fn owner_and_name(&self) -> (String, String) {
        (self.owner_login(), self.repo_name())
    }
}

impl RepoIdentity for GitHubRepo {
    fn owner_login(&self) -> String {
        self.owner
            .as_ref()
            .map(|o| o.login.clone())
            .unwrap_or_default()
    }

    fn repo_name(&self) -> String {
        self.name.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_resource_reset_at() {
        let resource = RateLimitResource {
            limit: 5000,
            used: 100,
            remaining: 4900,
            reset: 1700000000,
        };

        let reset_time = resource.reset_at();
        // Verify it returns a valid DateTime
        assert!(reset_time.timestamp() > 0);
    }

    #[test]
    fn test_rate_limit_resource_reset_at_boundary() {
        // Test with a valid but large timestamp - this should work
        let resource = RateLimitResource {
            limit: 5000,
            used: 100,
            remaining: 4900,
            reset: 2000000000, // Year 2033, valid timestamp
        };

        let reset_time = resource.reset_at();
        // Verify it returns the expected timestamp
        assert_eq!(reset_time.timestamp(), 2000000000);
    }

    #[test]
    fn test_rate_limit_resource_fields() {
        let resource = RateLimitResource {
            limit: 5000,
            used: 1000,
            remaining: 4000,
            reset: 1700000000,
        };

        assert_eq!(resource.limit, 5000);
        assert_eq!(resource.used, 1000);
        assert_eq!(resource.remaining, 4000);
        assert_eq!(resource.reset, 1700000000);
    }

    #[test]
    fn test_github_rate_limits_required_fields() {
        let json = r#"{
            "core": {
                "limit": 5000,
                "used": 100,
                "remaining": 4900,
                "reset": 1700000000
            },
            "search": {
                "limit": 30,
                "used": 5,
                "remaining": 25,
                "reset": 1700000000
            }
        }"#;

        let limits: GitHubRateLimits = serde_json::from_str(json).unwrap();

        assert_eq!(limits.core.limit, 5000);
        assert_eq!(limits.core.remaining, 4900);
        assert_eq!(limits.search.limit, 30);
        assert_eq!(limits.search.remaining, 25);

        // Optional fields should be None when not provided
        assert!(limits.code_search.is_none());
        assert!(limits.graphql.is_none());
        assert!(limits.integration_manifest.is_none());
    }

    #[test]
    fn test_github_rate_limits_all_fields() {
        let json = r#"{
            "core": {
                "limit": 5000,
                "used": 100,
                "remaining": 4900,
                "reset": 1700000000
            },
            "search": {
                "limit": 30,
                "used": 5,
                "remaining": 25,
                "reset": 1700000000
            },
            "code_search": {
                "limit": 10,
                "used": 1,
                "remaining": 9,
                "reset": 1700000000
            },
            "graphql": {
                "limit": 5000,
                "used": 50,
                "remaining": 4950,
                "reset": 1700000000
            }
        }"#;

        let limits: GitHubRateLimits = serde_json::from_str(json).unwrap();

        assert!(limits.code_search.is_some());
        assert_eq!(limits.code_search.as_ref().unwrap().limit, 10);

        assert!(limits.graphql.is_some());
        assert_eq!(limits.graphql.as_ref().unwrap().limit, 5000);
    }

    #[test]
    fn test_github_rate_limit_response() {
        let json = r#"{
            "resources": {
                "core": {
                    "limit": 5000,
                    "used": 100,
                    "remaining": 4900,
                    "reset": 1700000000
                },
                "search": {
                    "limit": 30,
                    "used": 5,
                    "remaining": 25,
                    "reset": 1700000000
                }
            }
        }"#;

        let response: GitHubRateLimitResponse = serde_json::from_str(json).unwrap();

        assert_eq!(response.resources.core.limit, 5000);
        assert_eq!(response.resources.search.limit, 30);
    }

    #[test]
    fn test_rate_limit_resource_serialize() {
        let resource = RateLimitResource {
            limit: 5000,
            used: 100,
            remaining: 4900,
            reset: 1700000000,
        };

        let json = serde_json::to_string(&resource).unwrap();
        assert!(json.contains("5000"));
        assert!(json.contains("4900"));
    }

    #[test]
    fn test_rate_limit_resource_clone() {
        let resource = RateLimitResource {
            limit: 5000,
            used: 100,
            remaining: 4900,
            reset: 1700000000,
        };

        let cloned = resource.clone();
        assert_eq!(cloned.limit, resource.limit);
        assert_eq!(cloned.remaining, resource.remaining);
    }

    #[test]
    fn test_rate_limit_resource_debug() {
        let resource = RateLimitResource {
            limit: 5000,
            used: 100,
            remaining: 4900,
            reset: 1700000000,
        };

        let debug_str = format!("{:?}", resource);
        assert!(debug_str.contains("RateLimitResource"));
        assert!(debug_str.contains("5000"));
    }

    #[test]
    fn test_constants_re_exported() {
        // Verify the constants are re-exported from sync module
        const { assert!(DEFAULT_CONCURRENCY > 0) };
        const { assert!(DEFAULT_STAR_CONCURRENCY > 0) };
        const { assert!(DEFAULT_PAGE_FETCH_CONCURRENCY > 0) };
    }
}
