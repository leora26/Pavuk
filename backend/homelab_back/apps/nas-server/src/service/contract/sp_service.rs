use async_trait::async_trait;
use uuid::Uuid;
use homelab_core::events::{UserCreatedEvent, UserUpdatedEvent};
use homelab_core::nas_domain::storage_profile::StorageProfile;
use homelab_core::nas_domain::storage_stats::StorageStats;
use crate::helpers::data_error::DataError;

#[async_trait]
pub trait StorageProfileService: Send + Sync {
    async fn save_storage_profile (&self, event: &UserCreatedEvent) -> Result<StorageProfile, DataError>;
    /// Apply an admin-driven user update to the storage profile: set `allowed_storage`
    /// when the event carries it. Ignores `taken_storage` (nas owns that) and
    /// `is_blocked` (projected into `nas_identities` by `NasIdentityService`).
    /// Idempotent — safe to re-apply nas's own emitted updates.
    async fn apply_user_update(&self, event: UserUpdatedEvent) -> Result<(), DataError>;
    async fn get_by_id(&self, id: Uuid) -> Result<Option<StorageProfile>, DataError>;
    /// Rebuilds `taken_storage` from the user's files and announces the result. Call it
    /// after any change to what a user stores; it replaces the old delta arithmetic, which
    /// could not recover from a single missed adjustment.
    async fn sync_taken_storage(&self, id: Uuid) -> Result<(), DataError>;
    async fn get_storage_stats(&self, id: Uuid) -> Result<StorageStats, DataError>; 
}
