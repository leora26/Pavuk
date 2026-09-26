use crate::helpers::data_error::DataError;
use async_trait::async_trait;
use uuid::Uuid;
use homelab_core::events::{UserCreatedEvent, UserUpdatedEvent};

#[async_trait]
pub trait NasIdentityService: Send + Sync {
    async fn project_user_created(&self, event: &UserCreatedEvent)
    -> Result<(), DataError>;
    async fn project_block_state(&self, event: &UserUpdatedEvent) -> Result<(), DataError>;
    async fn is_blocked(&self, user_id: Uuid) -> Result<bool, DataError>;
}
