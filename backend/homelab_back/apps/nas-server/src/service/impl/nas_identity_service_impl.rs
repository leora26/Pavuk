use crate::db::nas_identity_repository::NasIdentityRepository;
use crate::helpers::data_error::DataError;
use crate::service::contract::nas_identity_service::NasIdentityService;
use async_trait::async_trait;
use derive_new::new;
use homelab_core::events::{UserCreatedEvent, UserUpdatedEvent};
use std::sync::Arc;
use uuid::Uuid;

#[derive(new)]
pub struct NasIdentityServiceImpl {
    nas_identity_repo: Arc<dyn NasIdentityRepository>,
}

#[async_trait]
impl NasIdentityService for NasIdentityServiceImpl {
    async fn project_user_created(&self, event: &UserCreatedEvent) -> Result<(), DataError> {
        self.nas_identity_repo
            .upsert(event.user_id, &event.external_id)
            .await
    }

    async fn project_block_state(&self, event: &UserUpdatedEvent) -> Result<(), DataError> {
        self.nas_identity_repo
            .set_blocked(event.user_id, event.is_blocked)
            .await
    }

    async fn is_blocked(&self, user_id: Uuid) -> Result<bool, DataError> {
        self.nas_identity_repo.is_user_blocked(user_id).await
    }
}
