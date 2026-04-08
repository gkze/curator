use crate::entity::platform_type::PlatformType;

use super::{PlatformCatalog, WellKnownInstance, simplified_host};

pub const DEFAULT_RPS: u32 = 5;

pub const GITLAB_COM: WellKnownInstance = WellKnownInstance {
    name: "gitlab",
    platform_type: PlatformType::GitLab,
    host: "gitlab.com",
    oauth_client_id: Some("eba8ea9cbb5e8ddd455a3b3db35871963d8aa6b0a344a4b8c8e34ae8d71f336f"),
};

pub const GNOME_GITLAB: WellKnownInstance = WellKnownInstance {
    name: "gnome-gitlab",
    platform_type: PlatformType::GitLab,
    host: "gitlab.gnome.org",
    oauth_client_id: None,
};

pub const FREEDESKTOP_GITLAB: WellKnownInstance = WellKnownInstance {
    name: "freedesktop-gitlab",
    platform_type: PlatformType::GitLab,
    host: "gitlab.freedesktop.org",
    oauth_client_id: None,
};

pub const KDE_GITLAB: WellKnownInstance = WellKnownInstance {
    name: "kde-gitlab",
    platform_type: PlatformType::GitLab,
    host: "invent.kde.org",
    oauth_client_id: None,
};

pub const KITWARE_GITLAB: WellKnownInstance = WellKnownInstance {
    name: "kitware-gitlab",
    platform_type: PlatformType::GitLab,
    host: "gitlab.kitware.com",
    oauth_client_id: Some("2860b6473e16b639ccb37ce9ffdc6643cd5d09f6e55168621a72f6d687f3c637"),
};

pub const HASKELL_GITLAB: WellKnownInstance = WellKnownInstance {
    name: "haskell-gitlab",
    platform_type: PlatformType::GitLab,
    host: "gitlab.haskell.org",
    oauth_client_id: None,
};

pub const ARCHLINUX_GITLAB: WellKnownInstance = WellKnownInstance {
    name: "archlinux-gitlab",
    platform_type: PlatformType::GitLab,
    host: "gitlab.archlinux.org",
    oauth_client_id: None,
};

const WELL_KNOWN_INSTANCES: &[WellKnownInstance] = &[
    GITLAB_COM,
    GNOME_GITLAB,
    FREEDESKTOP_GITLAB,
    KDE_GITLAB,
    KITWARE_GITLAB,
    HASKELL_GITLAB,
    ARCHLINUX_GITLAB,
];

pub(super) struct GitLabCatalog;

pub(super) static GITLAB: GitLabCatalog = GitLabCatalog;

impl PlatformCatalog for GitLabCatalog {
    fn default_rps(&self) -> u32 {
        DEFAULT_RPS
    }

    fn api_url(&self, host: &str) -> String {
        format!("https://{host}/api/v4")
    }

    fn instance_name(&self, host: &str) -> String {
        self.well_known_instance(host).map_or_else(
            || format!("gitlab-{}", simplified_host(host)),
            |instance| instance.name.to_string(),
        )
    }

    fn well_known_instances(&self) -> &'static [WellKnownInstance] {
        WELL_KNOWN_INSTANCES
    }
}
