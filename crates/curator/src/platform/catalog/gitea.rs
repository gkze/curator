use crate::entity::platform_type::PlatformType;

use super::{PlatformCatalog, WellKnownInstance, simplified_host};

pub const DEFAULT_RPS: u32 = 5;

pub const CODEBERG: WellKnownInstance = WellKnownInstance {
    name: "codeberg",
    platform_type: PlatformType::Gitea,
    host: "codeberg.org",
    oauth_client_id: Some("dfe120ce-2440-4f13-8bb0-9ba5620542a7"),
};

const WELL_KNOWN_INSTANCES: &[WellKnownInstance] = &[CODEBERG];

pub(super) struct GiteaCatalog;

pub(super) static GITEA: GiteaCatalog = GiteaCatalog;

impl PlatformCatalog for GiteaCatalog {
    fn default_rps(&self) -> u32 {
        DEFAULT_RPS
    }

    fn api_url(&self, host: &str) -> String {
        format!("https://{host}/api/v1")
    }

    fn instance_name(&self, host: &str) -> String {
        self.well_known_instance(host).map_or_else(
            || simplified_host(host),
            |instance| instance.name.to_string(),
        )
    }

    fn well_known_instances(&self) -> &'static [WellKnownInstance] {
        WELL_KNOWN_INSTANCES
    }
}
