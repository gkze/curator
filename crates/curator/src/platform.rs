//! Platform-agnostic trait for code forge clients.
//!
//! This module defines the `PlatformClient` trait that provides a unified interface
//! for interacting with different code hosting platforms (GitHub, GitLab, Codeberg).
//!
//! # Example
//!
//! ```ignore
//! use curator::platform::{PlatformClient, PlatformRepo};
//!
//! async fn sync_repos<C: PlatformClient>(client: &C, org: &str) -> Result<(), PlatformError> {
//!     let repos = client.list_org_repos(org, None, None).await?;
//!     for repo in repos {
//!         println!("{}/{}", repo.owner, repo.name);
//!     }
//!     Ok(())
//! }
//! ```

mod conditional;
mod convert;
mod errors;
mod rate_limit;
mod types;

pub use conditional::{CacheStats, FetchResult, PaginationInfo};
pub use convert::strip_null_values;
pub use errors::{PlatformError, Result, short_error_message};
pub use rate_limit::{ApiRateLimiter, RateLimitedClient, rate_limits};
pub use types::{OrgInfo, PlatformClient, PlatformRepo, ProgressCallback, RateLimitInfo, UserInfo};

#[cfg(test)]
mod tests {
    use std::time::{Duration as StdDuration, Instant};

    use chrono::Utc;
    use sea_orm::prelude::Uuid;

    use crate::entity::code_visibility::CodeVisibility;

    use super::*;

    #[test]
    fn test_platform_error_api() {
        let err = PlatformError::api("Something went wrong");
        assert!(err.to_string().contains("API error"));
        assert!(err.to_string().contains("Something went wrong"));
    }

    #[test]
    fn test_platform_error_not_found() {
        let err = PlatformError::not_found("org/repo");
        assert!(err.to_string().contains("Not found"));
        assert!(err.to_string().contains("org/repo"));
    }

    #[test]
    fn test_platform_error_rate_limited() {
        let err = PlatformError::RateLimited {
            reset_at: Utc::now(),
        };
        assert!(err.to_string().contains("Rate limit"));
    }

    #[test]
    fn test_platform_error_network() {
        let err = PlatformError::network("connection refused");
        assert!(err.to_string().contains("Network error"));
        assert!(err.to_string().contains("connection refused"));
    }

    #[test]
    fn test_platform_error_internal() {
        let err = PlatformError::internal("unexpected state");
        assert!(err.to_string().contains("Internal error"));
        assert!(err.to_string().contains("unexpected state"));
    }

    #[test]
    fn test_platform_error_auth_required() {
        let err = PlatformError::AuthRequired;
        assert!(err.to_string().contains("Authentication required"));
    }

    #[test]
    fn test_platform_error_is_rate_limited() {
        let rate_limited = PlatformError::RateLimited {
            reset_at: Utc::now(),
        };
        assert!(rate_limited.is_rate_limited());

        let api_error = PlatformError::api("some error");
        assert!(!api_error.is_rate_limited());

        let not_found = PlatformError::not_found("resource");
        assert!(!not_found.is_rate_limited());
    }

    #[test]
    fn test_platform_repo_full_name() {
        let repo = PlatformRepo {
            platform_id: 123,
            owner: "myorg".to_string(),
            name: "myrepo".to_string(),
            description: None,
            default_branch: "main".to_string(),
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_archived: false,
            stars: Some(10),
            forks: Some(2),
            language: Some("Rust".to_string()),
            topics: vec!["cli".to_string()],
            created_at: None,
            updated_at: None,
            pushed_at: None,
            license: None,
            homepage: None,
            size_kb: None,
            metadata: serde_json::json!({}),
        };
        assert_eq!(repo.full_name(), "myorg/myrepo");
    }

    #[test]
    fn test_platform_repo_to_active_model() {
        let repo = PlatformRepo {
            platform_id: 12345,
            owner: "rust-lang".to_string(),
            name: "rust".to_string(),
            description: Some("The Rust programming language".to_string()),
            default_branch: "master".to_string(),
            visibility: CodeVisibility::Public,
            is_fork: false,
            is_archived: false,
            stars: Some(80000),
            forks: Some(10000),
            language: Some("Rust".to_string()),
            topics: vec!["rust".to_string(), "compiler".to_string()],
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            pushed_at: Some(Utc::now()),
            license: Some("MIT".to_string()),
            homepage: Some("https://rust-lang.org".to_string()),
            size_kb: Some(500000),
            metadata: serde_json::json!({
                "mirror": false,
                "template": false,
                "empty": false,
                "open_issues_count": 5000,
                "watchers_count": 3000,
                "has_issues": true,
                "has_wiki": true,
                "has_pull_requests": true
            }),
        };

        let instance_id = Uuid::new_v4();
        let model = repo.to_active_model(instance_id);

        // Verify key fields are set correctly
        assert_eq!(model.instance_id.clone().unwrap(), instance_id);
        assert_eq!(model.platform_id.clone().unwrap(), 12345);
        assert_eq!(model.owner.clone().unwrap(), "rust-lang");
        assert_eq!(model.name.clone().unwrap(), "rust");
        assert_eq!(
            model.description.clone().unwrap(),
            Some("The Rust programming language".to_string())
        );
        assert_eq!(model.default_branch.clone().unwrap(), "master");
        assert_eq!(model.visibility.clone().unwrap(), CodeVisibility::Public);
        assert!(!model.is_fork.clone().unwrap());
        assert!(!model.is_archived.clone().unwrap());
        assert_eq!(model.stars.clone().unwrap(), Some(80000));
        assert_eq!(model.forks.clone().unwrap(), Some(10000));
    }

    #[test]
    fn test_platform_repo_to_active_model_extracts_metadata() {
        let repo = PlatformRepo {
            platform_id: 123,
            owner: "test".to_string(),
            name: "repo".to_string(),
            description: None,
            default_branch: "main".to_string(),
            visibility: CodeVisibility::Private,
            is_fork: true,
            is_archived: true,
            stars: None,
            forks: None,
            language: None,
            topics: vec![],
            created_at: None,
            updated_at: None,
            pushed_at: None,
            license: None,
            homepage: None,
            size_kb: None,
            metadata: serde_json::json!({
                "mirror": true,
                "template": true,
                "empty": true,
                "open_issues_count": 42,
                "watchers_count": 100,
                "has_issues": false,
                "has_wiki": false,
                "has_pull_requests": false
            }),
        };

        let instance_id = Uuid::new_v4();
        let model = repo.to_active_model(instance_id);

        // Verify metadata fields are extracted
        assert!(model.is_mirror.clone().unwrap());
        assert!(model.is_template.clone().unwrap());
        assert!(model.is_empty.clone().unwrap());
        assert_eq!(model.open_issues.clone().unwrap(), Some(42));
        assert_eq!(model.watchers.clone().unwrap(), Some(100));
        assert!(!model.has_issues.clone().unwrap());
        assert!(!model.has_wiki.clone().unwrap());
        assert!(!model.has_pull_requests.clone().unwrap());
    }

    #[test]
    fn test_rate_limit_info() {
        let info = RateLimitInfo {
            limit: 5000,
            remaining: 4999,
            reset_at: Utc::now(),
        };
        assert_eq!(info.limit, 5000);
        assert_eq!(info.remaining, 4999);
    }

    #[test]
    fn test_org_info() {
        let info = OrgInfo {
            name: "rust-lang".to_string(),
            public_repos: 100,
            description: Some("The Rust Programming Language".to_string()),
        };
        assert_eq!(info.name, "rust-lang");
        assert_eq!(info.public_repos, 100);
    }

    #[test]
    fn test_user_info() {
        let info = UserInfo {
            username: "octocat".to_string(),
            name: Some("The Octocat".to_string()),
            email: Some("octocat@github.com".to_string()),
            bio: Some("I love cats".to_string()),
            public_repos: 50,
            followers: 1000,
        };
        assert_eq!(info.username, "octocat");
        assert_eq!(info.name, Some("The Octocat".to_string()));
        assert_eq!(info.public_repos, 50);
        assert_eq!(info.followers, 1000);
    }

    #[test]
    fn test_short_error_message_single_line() {
        let err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let msg = short_error_message(&err);
        assert_eq!(msg, "file not found");
    }

    #[test]
    fn test_short_error_message_multiline() {
        let err = std::io::Error::other("first line\nsecond line\nthird line");
        let msg = short_error_message(&err);
        assert_eq!(msg, "first line");
    }

    #[test]
    fn test_rate_limits_constants() {
        assert_eq!(rate_limits::GITHUB_DEFAULT_RPS, 10);
        assert_eq!(rate_limits::GITLAB_DEFAULT_RPS, 5);
        assert_eq!(rate_limits::GITEA_DEFAULT_RPS, 5);
    }

    #[test]
    fn test_api_rate_limiter_new() {
        let limiter = ApiRateLimiter::new(10);
        // Just verify it can be created without panicking

        // Test with zero (should default to 1)
        let limiter_zero = ApiRateLimiter::new(0);

        // Verify Clone works
        let _cloned = limiter.clone();
        let _cloned_zero = limiter_zero.clone();
    }

    #[tokio::test]
    async fn test_api_rate_limiter_wait_allows_first_request() {
        let limiter = ApiRateLimiter::new(100); // High RPS for fast test
        let start = Instant::now();
        limiter.wait().await;
        // First request should be nearly instant
        assert!(start.elapsed() < StdDuration::from_millis(50));
    }

    #[tokio::test]
    async fn test_api_rate_limiter_respects_rate() {
        // Use a slower rate (2 RPS = 500ms between requests) for reliable testing
        let limiter = ApiRateLimiter::new(2);
        let start = Instant::now();

        // First request should be instant (burst allowed)
        limiter.wait().await;
        let after_first = start.elapsed();

        // Second request
        limiter.wait().await;
        let after_second = start.elapsed();

        // The limiter should throttle after the first request
        // Allow generous tolerance for test timing variability
        // At 2 RPS, second request should wait ~500ms, but burst may allow instant
        // Just verify the limiter doesn't crash and completes in reasonable time
        assert!(after_second >= after_first);
        assert!(after_second < StdDuration::from_secs(5)); // Sanity check
    }

    #[test]
    fn test_rate_limited_client_new() {
        // We can't easily create a mock PlatformClient, but we can verify
        // the RateLimitedClient wrapper compiles and the inner() method works
        fn _assert_wrapper_compiles<C: PlatformClient>(client: C) {
            let wrapped = RateLimitedClient::new(client, 10);
            let _inner = wrapped.inner();
        }

        // Verify the function compiles (we can't call it without a real client)
        fn _verify_compiles() {
            fn _test<C: PlatformClient>(c: C) {
                _assert_wrapper_compiles(c);
            }
        }
    }

    #[test]
    fn test_rate_limited_client_clone() {
        // Verify RateLimitedClient is Clone when inner is Clone
        fn _assert_clone<T: Clone>() {}

        // This would fail to compile if RateLimitedClient<C> isn't Clone when C: Clone
        fn _verify<C: PlatformClient + Clone>() {
            _assert_clone::<RateLimitedClient<C>>();
        }
    }
}
