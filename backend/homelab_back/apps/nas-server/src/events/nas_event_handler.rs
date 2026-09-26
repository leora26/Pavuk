use std::sync::Arc;
use async_trait::async_trait;
use derive_new::new;
use homelab_core::events::{TrashCleanUpTriggeredEvent, UserCreatedEvent, UserUpdatedEvent};
use homelab_core::helpers::event_handler::EventHandler;
use crate::service::contract::clean_up_service::CleanUpService;
use crate::service::contract::nas_identity_service::NasIdentityService;
use crate::service::contract::sp_service::StorageProfileService;

#[derive(new)]
pub struct NasEventHandler {
    storage_profile_service: Arc<dyn StorageProfileService>,
    nas_identity_service: Arc<dyn NasIdentityService>,
    clean_up_service: Arc<dyn CleanUpService>
}

#[async_trait]
impl EventHandler for NasEventHandler {
    async fn handle(&self, routing_key: &str, data: &[u8]) -> Result<(), String> {
        match routing_key {
            "user.created" => {
                let event: UserCreatedEvent = serde_json::from_slice(data)
                    .map_err(|e| format!("Json Error: {}", e))?;

                println!("👤 Handling User Creation: {}", event.user_id);

                let profile = self.storage_profile_service.save_storage_profile(&event).await
                    .map_err(|e| format!("DB Error: {}", e))?;

                self.nas_identity_service.project_user_created(&event).await
                    .map_err(|e| format!("DB Error: {}", e))?;

                eprintln!("Create storage profile: {}; {}; {}",
                          profile.user_id,
                          profile.allowed_storage,
                          profile.taken_storage
                );

                Ok(())
            },
            "user.updated" => {
                let event: UserUpdatedEvent = serde_json::from_slice(data)
                    .map_err(|e| format!("Json Error: {}", e))?;

                println!("👤 Handling User Update: {} (blocked={})", event.user_id, event.is_blocked);

                // Block first, quota second. These are two writes, not one transaction:
                // if the quota half fails the message dead-letters and replays, and by
                // then access has already been tightened. The reverse order would leave a
                // blocked user unblocked until the replay landed.
                self.nas_identity_service.project_block_state(&event).await
                    .map_err(|e| format!("DB Error: {}", e))?;

                self.storage_profile_service.apply_user_update(event).await
                    .map_err(|e| format!("DB Error: {}", e))?;

                Ok(())
            },
            "cleanup.triggered" => {
                let event: TrashCleanUpTriggeredEvent = serde_json::from_slice(data)
                    .map_err(|e| format!("Json Error: {}", e))?;

                println!("Handling Trash CleanUp Triggered: {}", event.user_id);

                self.clean_up_service.handle_trash_delete(event).await
                    .map_err(|e| format!("DB Error: {}", e))?;

                Ok(())
            },

            _ => {
                println!("⚠️ Ignoring unknown event: {}", routing_key);
                Ok(())
            }
        }
    }
}
