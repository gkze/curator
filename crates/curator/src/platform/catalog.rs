use crate::entity::platform_type::PlatformType;

mod gitea;
mod github;
mod gitlab;

pub const GITHUB_DEFAULT_RPS: u32 = github::DEFAULT_RPS;
pub const GITLAB_DEFAULT_RPS: u32 = gitlab::DEFAULT_RPS;
pub const GITEA_DEFAULT_RPS: u32 = gitea::DEFAULT_RPS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WellKnownInstance {
    pub name: &'static str,
    pub platform_type: PlatformType,
    pub host: &'static str,
    pub oauth_client_id: Option<&'static str>,
}

pub(crate) trait PlatformCatalog {
    fn default_rps(&self) -> u32;

    fn api_url(&self, host: &str) -> String;

    fn instance_name(&self, host: &str) -> String;

    fn well_known_instances(&self) -> &'static [WellKnownInstance];

    fn well_known_instance(&self, host: &str) -> Option<&'static WellKnownInstance> {
        self.well_known_instances()
            .iter()
            .find(|instance| instance.host == host)
    }
}

pub(crate) const ALL_WELL_KNOWN_INSTANCES: &[WellKnownInstance] = &[
    github::GITHUB_COM,
    gitlab::GITLAB_COM,
    gitlab::GNOME_GITLAB,
    gitlab::FREEDESKTOP_GITLAB,
    gitlab::KDE_GITLAB,
    gitlab::KITWARE_GITLAB,
    gitlab::HASKELL_GITLAB,
    gitlab::ARCHLINUX_GITLAB,
    gitea::CODEBERG,
];

pub(crate) fn platform_catalog(platform_type: PlatformType) -> &'static dyn PlatformCatalog {
    match platform_type {
        PlatformType::GitHub => &github::GITHUB,
        PlatformType::GitLab => &gitlab::GITLAB,
        PlatformType::Gitea => &gitea::GITEA,
    }
}

fn simplified_host(host: &str) -> String {
    host.trim_end_matches(".com")
        .trim_end_matches(".org")
        .trim_end_matches(".io")
        .replace('.', "-")
}
