//! Sync context builder for simplified sync operations.
//!
//! This module provides a builder pattern for constructing sync contexts,
//! reducing the number of parameters needed for sync functions.
//!
//! # Example
//!
//! ```ignore
//! use curator::sync::{SyncContext, SyncOptions};
//!
//! let ctx = SyncContext::builder()
//!     .client(github_client)
//!     .options(SyncOptions::default())
//!     .database(db)
//!     .progress(callback)
//!     .build()?;
//!
//! let result = ctx.sync_namespace("rust-lang").await?;
//! ```

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use sea_orm::DatabaseConnection;
use tokio::sync::mpsc;

use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::platform::{PlatformClient, PlatformError};

#[cfg(feature = "discovery")]
use super::engine::sync_repo_list_streaming;
use super::engine::{
    sync_namespace, sync_namespace_streaming, sync_namespaces_streaming, sync_starred_streaming,
    sync_user_streaming, sync_users_streaming,
};
use super::persist_task::{
    MODEL_CHANNEL_BUFFER_SIZE, PersistTaskResult, await_persist_task, spawn_persist_task,
};
use super::progress::ProgressCallback;
use super::types::{NamespaceSyncResultStreaming, SyncOptions, SyncResult};

/// Error type for sync context operations.
#[derive(Debug, thiserror::Error)]
pub enum SyncContextError {
    /// Missing required field in builder.
    #[error("Missing required field: {field}")]
    MissingField { field: &'static str },

    /// Platform error during sync.
    #[error(transparent)]
    Platform(#[from] PlatformError),
}

/// Result type for sync context operations.
pub type Result<T> = std::result::Result<T, SyncContextError>;

/// Builder for creating a `SyncContext`.
///
/// Use this to configure all the parameters needed for sync operations.
pub struct SyncContextBuilder<C> {
    client: Option<C>,
    options: Option<SyncOptions>,
    database: Option<Arc<DatabaseConnection>>,
    progress: Option<Arc<ProgressCallback>>,
    shutdown_flag: Option<Arc<AtomicBool>>,
}

impl<C> Default for SyncContextBuilder<C> {
    fn default() -> Self {
        Self {
            client: None,
            options: None,
            database: None,
            progress: None,
            shutdown_flag: None,
        }
    }
}

impl<C: PlatformClient + Clone + 'static> SyncContextBuilder<C> {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the platform client.
    pub fn client(mut self, client: C) -> Self {
        self.client = Some(client);
        self
    }

    /// Set sync options.
    pub fn options(mut self, options: SyncOptions) -> Self {
        self.options = Some(options);
        self
    }

    /// Set the database connection.
    pub fn database(mut self, db: Arc<DatabaseConnection>) -> Self {
        self.database = Some(db);
        self
    }

    /// Set the progress callback.
    pub fn progress(mut self, callback: Arc<ProgressCallback>) -> Self {
        self.progress = Some(callback);
        self
    }

    /// Set the shutdown flag for graceful shutdown.
    pub fn shutdown_flag(mut self, flag: Arc<AtomicBool>) -> Self {
        self.shutdown_flag = Some(flag);
        self
    }

    /// Build the sync context.
    ///
    /// # Errors
    ///
    /// Returns `SyncContextError::MissingField` if required fields are not set.
    pub fn build(self) -> Result<SyncContext<C>> {
        let client = self
            .client
            .ok_or(SyncContextError::MissingField { field: "client" })?;
        let options = self.options.unwrap_or_default();

        // Validate: non-dry-run syncs require a database
        if !options.dry_run && self.database.is_none() {
            return Err(SyncContextError::MissingField { field: "database" });
        }

        Ok(SyncContext {
            client,
            options,
            database: self.database,
            progress: self.progress,
            shutdown_flag: self.shutdown_flag,
        })
    }
}

/// Context for sync operations.
///
/// Encapsulates all the configuration needed to perform sync operations,
/// providing a cleaner API than passing many individual parameters.
pub struct SyncContext<C> {
    client: C,
    options: SyncOptions,
    database: Option<Arc<DatabaseConnection>>,
    progress: Option<Arc<ProgressCallback>>,
    shutdown_flag: Option<Arc<AtomicBool>>,
}

impl<C: PlatformClient + Clone + 'static> SyncContext<C> {
    /// Create a new builder.
    pub fn builder() -> SyncContextBuilder<C> {
        SyncContextBuilder::new()
    }

    /// Get a reference to the client.
    pub fn client(&self) -> &C {
        &self.client
    }

    /// Get a reference to the options.
    pub fn options(&self) -> &SyncOptions {
        &self.options
    }

    /// Get a reference to the database connection.
    pub fn database(&self) -> Option<&Arc<DatabaseConnection>> {
        self.database.as_ref()
    }

    /// Check if dry run mode is enabled.
    pub fn is_dry_run(&self) -> bool {
        self.options.dry_run
    }

    /// Execute an async operation with automatic persist task management.
    ///
    /// This helper handles the common pattern of:
    /// 1. Creating a channel for streaming models
    /// 2. Spawning a persist task (if not dry_run)
    /// 3. Executing the provided async operation with the sender
    /// 4. Awaiting the persist task and returning the result
    ///
    /// Use this for fallible operations that return `Result`.
    async fn with_persist_task<F, Fut, T, E>(
        &self,
        f: F,
    ) -> std::result::Result<(T, PersistTaskResult), E>
    where
        F: FnOnce(mpsc::Sender<CodeRepositoryActiveModel>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, E>>,
    {
        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);

        let persist_handle = if !self.options.dry_run {
            let db = self
                .database
                .clone()
                .expect("database required for non-dry-run sync");
            let (handle, _counter) =
                spawn_persist_task(db, rx, self.shutdown_flag.clone(), self.progress.clone());
            Some(handle)
        } else {
            drop(rx);
            None
        };

        let result = f(tx).await?;

        let persist_result = if let Some(handle) = persist_handle {
            await_persist_task(handle).await
        } else {
            PersistTaskResult::default()
        };

        Ok((result, persist_result))
    }

    /// Execute an async operation with automatic persist task management (infallible version).
    ///
    /// Similar to [`with_persist_task`] but for operations that don't return `Result`.
    async fn with_persist_task_infallible<F, Fut, T>(&self, f: F) -> (T, PersistTaskResult)
    where
        F: FnOnce(mpsc::Sender<CodeRepositoryActiveModel>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let (tx, rx) = mpsc::channel::<CodeRepositoryActiveModel>(MODEL_CHANNEL_BUFFER_SIZE);

        let persist_handle = if !self.options.dry_run {
            let db = self
                .database
                .clone()
                .expect("database required for non-dry-run sync");
            let (handle, _counter) =
                spawn_persist_task(db, rx, self.shutdown_flag.clone(), self.progress.clone());
            Some(handle)
        } else {
            drop(rx);
            None
        };

        let result = f(tx).await;

        let persist_result = if let Some(handle) = persist_handle {
            await_persist_task(handle).await
        } else {
            PersistTaskResult::default()
        };

        (result, persist_result)
    }

    /// Sync a single namespace (organization/group).
    ///
    /// This is the simplest sync method - it fetches, filters, and returns results
    /// without streaming persistence.
    pub async fn sync_namespace(
        &self,
        namespace: &str,
    ) -> std::result::Result<(SyncResult, Vec<CodeRepositoryActiveModel>), PlatformError> {
        sync_namespace(
            &self.client,
            namespace,
            &self.options,
            self.database.as_ref().map(|db| db.as_ref()),
            self.progress.as_ref().map(|p| p.as_ref()),
        )
        .await
    }

    /// Sync a single namespace with streaming persistence.
    ///
    /// Spawns a persist task that writes to the database as models are processed.
    /// Returns both the sync result and the persist task result.
    pub async fn sync_namespace_streaming(
        &self,
        namespace: &str,
    ) -> std::result::Result<SyncStreamingResult, PlatformError> {
        let (sync, persist) = self
            .with_persist_task(|tx| {
                sync_namespace_streaming(
                    &self.client,
                    namespace,
                    &self.options,
                    self.database.as_ref().map(|db| db.as_ref()),
                    tx,
                    self.progress.as_ref().map(|p| p.as_ref()),
                )
            })
            .await?;

        Ok(SyncStreamingResult { sync, persist })
    }

    /// Sync multiple namespaces concurrently with streaming persistence.
    pub async fn sync_namespaces_streaming(
        &self,
        namespaces: &[String],
    ) -> (Vec<NamespaceSyncResultStreaming>, PersistTaskResult) {
        self.with_persist_task_infallible(|tx| {
            sync_namespaces_streaming(
                &self.client,
                namespaces,
                &self.options,
                self.database.clone(),
                tx,
                self.progress.as_ref().map(|p| p.as_ref()),
            )
        })
        .await
    }

    /// Sync a single user's repositories with streaming persistence.
    pub async fn sync_user_streaming(
        &self,
        username: &str,
    ) -> std::result::Result<SyncStreamingResult, PlatformError> {
        let (sync, persist) = self
            .with_persist_task(|tx| {
                sync_user_streaming(
                    &self.client,
                    username,
                    &self.options,
                    self.database.as_ref().map(|db| db.as_ref()),
                    tx,
                    self.progress.as_ref().map(|p| p.as_ref()),
                )
            })
            .await?;

        Ok(SyncStreamingResult { sync, persist })
    }

    /// Sync multiple users' repositories concurrently with streaming persistence.
    pub async fn sync_users_streaming(
        &self,
        usernames: &[String],
    ) -> (Vec<NamespaceSyncResultStreaming>, PersistTaskResult) {
        self.with_persist_task_infallible(|tx| {
            sync_users_streaming(
                &self.client,
                usernames,
                &self.options,
                self.database.clone(),
                tx,
                self.progress.as_ref().map(|p| p.as_ref()),
            )
        })
        .await
    }

    /// Sync an explicit repository list with streaming persistence.
    #[cfg(feature = "discovery")]
    pub async fn sync_repo_list_streaming(
        &self,
        label: &str,
        repos: &[(String, String)],
    ) -> std::result::Result<SyncStreamingResult, PlatformError> {
        let (sync, persist) = self
            .with_persist_task(|tx| {
                sync_repo_list_streaming(
                    &self.client,
                    label,
                    repos,
                    &self.options,
                    self.database.clone(),
                    tx,
                    self.progress.as_ref().map(|p| p.as_ref()),
                )
            })
            .await?;

        Ok(SyncStreamingResult { sync, persist })
    }

    /// Sync starred repositories with streaming persistence.
    ///
    /// This handles the full starred sync workflow including optional pruning.
    pub async fn sync_starred_streaming(
        &self,
        skip_rate_checks: bool,
    ) -> std::result::Result<SyncStreamingResult, PlatformError> {
        let (sync, persist) = self
            .with_persist_task(|tx| {
                sync_starred_streaming(
                    &self.client,
                    &self.options,
                    self.database.as_ref().map(|db| db.as_ref()),
                    self.options.concurrency,
                    skip_rate_checks,
                    tx,
                    self.progress.as_ref().map(|p| p.as_ref()),
                )
            })
            .await?;

        Ok(SyncStreamingResult { sync, persist })
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_flag
            .as_ref()
            .is_some_and(|f| f.load(Ordering::Relaxed))
    }
}

/// Result of a streaming sync operation.
///
/// Combines the sync result with persistence information.
/// This type should not be silently ignored as it contains error information
/// from both the sync and persistence operations.
#[derive(Debug)]
#[must_use = "SyncStreamingResult may contain errors that should be checked"]
pub struct SyncStreamingResult {
    /// The sync operation result (processed, matched, starred, etc.)
    pub sync: SyncResult,
    /// The persist task result (saved count, errors)
    pub persist: PersistTaskResult,
}

impl SyncStreamingResult {
    /// Total number of repositories successfully saved.
    pub fn saved_count(&self) -> usize {
        self.persist.saved_count
    }

    /// Check if there were any errors (sync or persist).
    pub fn has_errors(&self) -> bool {
        !self.sync.errors.is_empty() || self.persist.has_errors()
    }

    /// Get total error count.
    pub fn error_count(&self) -> usize {
        self.sync.errors.len() + self.persist.failed_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_default() {
        // Test that builder can be created with Default
        let builder: SyncContextBuilder<String> = SyncContextBuilder::default();
        assert!(builder.client.is_none());
        assert!(builder.options.is_none());
        assert!(builder.database.is_none());
        assert!(builder.progress.is_none());
        assert!(builder.shutdown_flag.is_none());
    }

    #[test]
    fn test_sync_context_error_display() {
        let err = SyncContextError::MissingField { field: "client" };
        assert!(err.to_string().contains("client"));
    }

    #[test]
    fn test_sync_streaming_result() {
        let result = SyncStreamingResult {
            sync: SyncResult {
                processed: 100,
                matched: 50,
                saved: 50,
                errors: vec!["sync error".to_string()],
                ..Default::default()
            },
            persist: PersistTaskResult {
                saved_count: 48,
                errors: vec![("o".to_string(), "r".to_string(), "e".to_string())],
                panic_info: None,
            },
        };

        assert_eq!(result.saved_count(), 48);
        assert!(result.has_errors());
        assert_eq!(result.error_count(), 2); // 1 sync + 1 persist
    }

    #[test]
    fn test_sync_streaming_result_no_errors() {
        let result = SyncStreamingResult {
            sync: SyncResult::default(),
            persist: PersistTaskResult::default(),
        };

        assert!(!result.has_errors());
        assert_eq!(result.error_count(), 0);
    }

    #[test]
    fn test_options_default() {
        let options = SyncOptions::default();
        assert!(options.prune);
        assert!(!options.dry_run);
        assert!(options.star); // Default is true
    }
}
