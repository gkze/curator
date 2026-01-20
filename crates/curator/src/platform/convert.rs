use chrono::Utc;
use sea_orm::Set;
use uuid::Uuid;

use crate::entity::code_platform::CodePlatform;
use crate::entity::code_repository::{
    ActiveModel as CodeRepositoryActiveModel, Model as CodeRepositoryModel,
};

use super::types::PlatformRepo;

/// Strip null values from a JSON object to reduce storage size.
///
/// This recursively removes null values from objects, which can significantly
/// reduce database storage for platform_metadata fields where most values are null.
///
/// # Example
///
/// ```ignore
/// let json = serde_json::json!({
///     "node_id": "abc123",
///     "private": false,
///     "allow_squash_merge": null,
/// });
/// let stripped = strip_null_values(json);
/// // Result: {"node_id": "abc123", "private": false}
/// ```
pub fn strip_null_values(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let filtered: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k, strip_null_values(v)))
                .collect();
            serde_json::Value::Object(filtered)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(strip_null_values).collect())
        }
        other => other,
    }
}

impl PlatformRepo {
    /// Convert this platform repository to a database active model.
    ///
    /// This is the shared conversion logic used by all platforms. Platform-specific
    /// fields that aren't available in `PlatformRepo` (like `is_mirror`, `is_template`,
    /// `is_empty`, `open_issues`, `watchers`, `has_issues`, `has_wiki`, `has_pull_requests`)
    /// can be extracted from the `metadata` JSON field if needed, or will use defaults.
    ///
    /// # Arguments
    ///
    /// * `platform` - The platform this repository came from
    pub fn to_active_model(&self, platform: CodePlatform) -> CodeRepositoryActiveModel {
        let now = Utc::now().fixed_offset();
        let topics = serde_json::json!(self.topics);

        // Extract optional fields from metadata if available
        let metadata = &self.metadata;
        let is_mirror = metadata
            .get("mirror")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let is_template = metadata
            .get("template")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let is_empty = metadata
            .get("empty")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let open_issues = metadata
            .get("open_issues_count")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);
        let watchers = metadata
            .get("watchers_count")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);
        let has_issues = metadata
            .get("has_issues")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let has_wiki = metadata
            .get("has_wiki")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let has_pull_requests = metadata
            .get("has_pull_requests")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        CodeRepositoryActiveModel {
            id: Set(Uuid::new_v4()),
            platform: Set(platform),
            platform_id: Set(self.platform_id),
            owner: Set(self.owner.clone()),
            name: Set(self.name.clone()),
            description: Set(self.description.clone()),
            default_branch: Set(self.default_branch.clone()),
            topics: Set(topics),
            primary_language: Set(self.language.clone()),
            license_spdx: Set(self.license.clone()),
            homepage: Set(self.homepage.clone()),
            visibility: Set(self.visibility.clone()),
            is_fork: Set(self.is_fork),
            is_mirror: Set(is_mirror),
            is_archived: Set(self.is_archived),
            is_template: Set(is_template),
            is_empty: Set(is_empty),
            stars: Set(self.stars.map(|c| c as i32)),
            forks: Set(self.forks.map(|c| c as i32)),
            open_issues: Set(open_issues),
            watchers: Set(watchers),
            size_kb: Set(self.size_kb.map(|s| s as i64)),
            has_issues: Set(has_issues),
            has_wiki: Set(has_wiki),
            has_pull_requests: Set(has_pull_requests),
            created_at: Set(self.created_at.map(|t| t.fixed_offset())),
            updated_at: Set(self.updated_at.map(|t| t.fixed_offset())),
            pushed_at: Set(self.pushed_at.map(|t| t.fixed_offset())),
            platform_metadata: Set(self.metadata.clone()),
            synced_at: Set(now),
            etag: Set(None),
        }
    }

    /// Convert a database model back to a PlatformRepo.
    ///
    /// This is useful when loading cached repos from the database to stream
    /// them through the sync pipeline without re-fetching from the API.
    pub fn from_model(model: &CodeRepositoryModel) -> Self {
        // Extract topics from JSON array
        let topics: Vec<String> = model
            .topics
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        Self {
            platform_id: model.platform_id,
            owner: model.owner.clone(),
            name: model.name.clone(),
            description: model.description.clone(),
            default_branch: model.default_branch.clone(),
            visibility: model.visibility.clone(),
            is_fork: model.is_fork,
            is_archived: model.is_archived,
            stars: model.stars.map(|s| s as u32),
            forks: model.forks.map(|f| f as u32),
            language: model.primary_language.clone(),
            topics,
            created_at: model.created_at.map(|t| t.with_timezone(&Utc)),
            updated_at: model.updated_at.map(|t| t.with_timezone(&Utc)),
            pushed_at: model.pushed_at.map(|t| t.with_timezone(&Utc)),
            license: model.license_spdx.clone(),
            homepage: model.homepage.clone(),
            size_kb: model.size_kb.map(|s| s as u64),
            metadata: model.platform_metadata.clone(),
        }
    }
}
