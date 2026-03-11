use clap::ValueEnum;

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) mod auth;
pub(crate) mod instance;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) mod limits;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) mod login;
pub(crate) mod meta;
pub(crate) mod migrate;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) mod sync;

#[cfg(all(
    feature = "discovery",
    any(feature = "github", feature = "gitlab", feature = "gitea")
))]
pub(crate) mod discover;

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) mod shared;

/// Output format for CLI display commands.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub(crate) enum OutputFormat {
    /// Display as a formatted table (default)
    #[default]
    Table,
    /// Display as JSON
    Json,
}
