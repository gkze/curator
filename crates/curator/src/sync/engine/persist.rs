use chrono::Utc;
use tokio::sync::mpsc;

use crate::entity::code_repository::ActiveModel as CodeRepositoryActiveModel;
use crate::platform::{PlatformClient, PlatformRepo};

use super::super::progress::{ProgressCallback, SyncProgress, emit};
use super::super::types::{SyncOptions, SyncResult};
use super::filter::is_active_repo;

pub(super) struct StreamingProcessResult {
    pub(super) result: SyncResult,
    pub(super) repos_to_star: Vec<(String, String)>,
}

pub(super) fn build_models<C: PlatformClient>(
    client: &C,
    repos: &[&PlatformRepo],
) -> Vec<CodeRepositoryActiveModel> {
    repos
        .iter()
        .map(|repo| client.to_active_model(repo))
        .collect()
}

pub(super) async fn process_streaming_repos<C: PlatformClient>(
    client: &C,
    namespace: &str,
    repos: &[PlatformRepo],
    options: &SyncOptions,
    model_tx: &mpsc::Sender<CodeRepositoryActiveModel>,
    on_progress: Option<&ProgressCallback>,
) -> StreamingProcessResult {
    let cutoff = Utc::now() - options.active_within;

    emit(
        on_progress,
        SyncProgress::FilteringByActivity {
            namespace: namespace.to_string(),
            days: options.active_within.num_days(),
        },
    );

    let mut result = SyncResult {
        processed: repos.len(),
        ..SyncResult::default()
    };

    let mut channel_closed = false;
    let mut repos_to_star: Vec<(String, String)> = Vec::new();

    for repo in repos {
        if is_active_repo(repo, cutoff) {
            result.matched += 1;

            if options.star {
                repos_to_star.push((repo.owner.clone(), repo.name.clone()));
            }

            if !options.dry_run && !channel_closed {
                let model = client.to_active_model(repo);
                if model_tx.send(model).await.is_err() {
                    channel_closed = true;
                    emit(
                        on_progress,
                        SyncProgress::Warning {
                            message: "persistence channel closed".to_string(),
                        },
                    );
                } else {
                    result.saved += 1;
                }
            }
        }
    }

    emit(
        on_progress,
        SyncProgress::FilterComplete {
            namespace: namespace.to_string(),
            matched: result.matched,
            total: result.processed,
        },
    );

    StreamingProcessResult {
        result,
        repos_to_star,
    }
}
