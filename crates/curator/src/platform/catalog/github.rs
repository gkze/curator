use crate::entity::platform_type::PlatformType;

use super::{PlatformCatalog, WellKnownInstance, simplified_host};

pub const DEFAULT_RPS: u32 = 10;

pub const GITHUB_COM: WellKnownInstance = WellKnownInstance {
    name: "github",
    platform_type: PlatformType::GitHub,
    host: "github.com",
    oauth_client_id: Some("Ov23liN0721EfoUpRrLl"),
};

const WELL_KNOWN_INSTANCES: &[WellKnownInstance] = &[GITHUB_COM];

pub(super) struct GitHubCatalog;

pub(super) static GITHUB: GitHubCatalog = GitHubCatalog;

impl PlatformCatalog for GitHubCatalog {
    fn default_rps(&self) -> u32 {
        DEFAULT_RPS
    }

    fn api_url(&self, host: &str) -> String {
        if host == GITHUB_COM.host {
            "https://api.github.com".to_string()
        } else {
            format!("https://{host}/api/v3")
        }
    }

    fn instance_name(&self, host: &str) -> String {
        self.well_known_instance(host).map_or_else(
            || format!("github-{}", simplified_host(host)),
            |instance| instance.name.to_string(),
        )
    }

    fn well_known_instances(&self) -> &'static [WellKnownInstance] {
        WELL_KNOWN_INSTANCES
    }
}
