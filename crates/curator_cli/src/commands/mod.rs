pub(crate) mod limits;
pub(crate) mod meta;
pub(crate) mod migrate;

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) mod shared;

#[cfg(feature = "github")]
pub(crate) mod github;

#[cfg(feature = "gitlab")]
pub(crate) mod gitlab;

#[cfg(feature = "gitea")]
pub(crate) mod gitea;
