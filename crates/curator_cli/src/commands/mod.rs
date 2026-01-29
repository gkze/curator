pub(crate) mod instance;
pub(crate) mod limits;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) mod login;
pub(crate) mod meta;
pub(crate) mod migrate;
#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) mod sync;

#[cfg(any(feature = "github", feature = "gitlab", feature = "gitea"))]
pub(crate) mod shared;
