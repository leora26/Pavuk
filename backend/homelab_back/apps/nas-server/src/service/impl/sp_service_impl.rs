use std::sync::Arc;
use async_trait::async_trait;
use derive_new::new;
use uuid::Uuid;
use homelab_core::events::{UserCreatedEvent, UserUpdatedEvent};
use homelab_core::nas_domain::storage_profile::StorageProfile;
use homelab_core::nas_domain::storage_stats::StorageStats;
use crate::db::nas_identity_repository::NasIdentityRepository;
use crate::db::storage_profile_repository::StorageProfileRepository;
use crate::events::rabbitmq::RabbitMqPublisher;
use crate::helpers::data_error::DataError;
use crate::service::contract::sp_service::StorageProfileService;

#[derive(new)]
pub struct StorageProfileServiceImpl {
    storage_profile_repo: Arc<dyn StorageProfileRepository>,
    nas_identity_repo: Arc<dyn NasIdentityRepository>,
    publisher: Arc<RabbitMqPublisher>,
}

#[async_trait]
impl StorageProfileService for StorageProfileServiceImpl {
    async fn save_storage_profile(&self, event: &UserCreatedEvent) -> Result<StorageProfile, DataError> {
        let profile: StorageProfile = StorageProfile::new(
            event.user_id,
            event.default_storage,
            0i64,
        );

        self.storage_profile_repo.create(profile).await
    }

    async fn apply_user_update(&self, event: UserUpdatedEvent) -> Result<(), DataError> {
        let sp = self.storage_profile_repo.get_by_id(event.user_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("Storage profile".to_string()))?;

        // Only the quota comes from this event; `taken_storage` is owned by the file
        // operations and must not be written back from a stale read, and `is_blocked` is
        // projected separately by `NasIdentityService`.
        self.storage_profile_repo
            .update_quota(
                event.user_id,
                event.allowed_storage.unwrap_or(sp.allowed_storage),
            )
            .await
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<StorageProfile>, DataError> {
        self.storage_profile_repo.get_by_id(id).await
    }

    async fn sync_taken_storage(&self, id: Uuid) -> Result<(), DataError> {
        // Recompute first, then announce — the event has to carry the value that was
        // actually stored, and publishing from a pre-write read is how these two drifted
        // apart before.
        let taken_storage = self.storage_profile_repo.recompute_taken_storage(id).await?;

        let sp = self
            .storage_profile_repo
            .get_by_id(id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("Storage Profile".to_string()))?;

        let is_blocked = self.nas_identity_repo.is_user_blocked(id).await?;

        let sp_event: UserUpdatedEvent = UserUpdatedEvent::new(
            sp.user_id.clone(),
            None,
            None,
            Some(sp.allowed_storage.clone()),
            Some(taken_storage),
            is_blocked,
        );

        if let Err(e) = self.publisher.publish(&sp_event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        Ok(())
    }

    async fn get_storage_stats(&self, id: Uuid) -> Result<StorageStats, DataError> {
        self.storage_profile_repo.get_stats(id).await
    }
}