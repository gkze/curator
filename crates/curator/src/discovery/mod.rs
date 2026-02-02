//! Discovery utilities for finding repository links on arbitrary websites.

mod crawler;
mod repo_links;
mod sitemap;

pub use crawler::{
    CrawlOptions, DiscoveryError, DiscoveryProgress, DiscoveryProgressCallback, DiscoveryResult,
    discover_repo_links,
};
pub use repo_links::RepoLink;
