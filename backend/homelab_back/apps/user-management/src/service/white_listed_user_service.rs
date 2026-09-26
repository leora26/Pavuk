use crate::data::create_white_listed_user_command::CreateWhiteListedUserCommand;
use crate::db::user_repository::UserRepository;
use crate::db::white_listed_user_repository::WhiteListedUserRepository;
use crate::helpers::data_error::DataError;
use crate::helpers::user_email::UserEmail;
use actix_web::cookie::time::OffsetDateTime;
use async_trait::async_trait;
use derive_new::new;
use homelab_core::events::{WhiteListedUserCreatedEvent, WhiteListedUserUpdatedEvent};
use homelab_core::user_domain::user::User;
use homelab_core::user_domain::white_listed_user::WhiteListedUser;
use std::sync::Arc;
use uuid::Uuid;
use homelab_core::helpers::rabbitmq_publisher::RabbitMqPublisher;

#[async_trait]
pub trait WhiteListedUserService: Send + Sync {
    async fn get_all(&self) -> Result<Vec<WhiteListedUser>, DataError>;
    async fn confirm(&self, user_id: Uuid) -> Result<User, DataError>;
    async fn create(
        &self,
        command: CreateWhiteListedUserCommand,
    ) -> Result<WhiteListedUser, DataError>;
}

#[derive(new)]
pub struct WhiteListedServiceImpl {
    white_listed_repo: Arc<dyn WhiteListedUserRepository>,
    user_repo: Arc<dyn UserRepository>,
    publisher: Arc<RabbitMqPublisher>,
}

#[async_trait]
impl WhiteListedUserService for WhiteListedServiceImpl {
    async fn get_all(&self) -> Result<Vec<WhiteListedUser>, DataError> {
        self.white_listed_repo.get_all().await
    }

    async fn confirm(&self, user_id: Uuid) -> Result<User, DataError> {
        let wlu = self
            .white_listed_repo
            .get_by_id(user_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("WhiteListedUser".to_string()))?;

        let new_user_entity = User::new_pending(Uuid::new_v4(), wlu.email.clone(), wlu.full_name.clone(), wlu.external_id.clone());

        let saved_user = self.user_repo.create(new_user_entity).await.map_err(|e| {
            DataError::EntityCreationError(format!("White listed user failed creation: {}", e))
        })?;

        let event = WhiteListedUserUpdatedEvent::new(
            wlu.id,
            wlu.email,
            wlu.full_name,
            true
        );

        if let Err(r) = self.publisher.publish(&event).await {
            eprintln!("Failed to publish event: {:?}", r);
        }

        self.white_listed_repo.delete_by_id(user_id).await?;

        Ok(saved_user)
    }

    async fn create(
        &self,
        command: CreateWhiteListedUserCommand,
    ) -> Result<WhiteListedUser, DataError> {
        let valid_email =
            UserEmail::parse(command.email).map_err(|e| DataError::ValidationError(e))?;

        let clean_name = command.full_name.trim().to_string();

        let u = WhiteListedUser::new(
            Uuid::new_v4(),
            valid_email.into_inner(),
            clean_name,
            OffsetDateTime::now_utc(),
            command.external_id
        );

        let event: WhiteListedUserCreatedEvent = WhiteListedUserCreatedEvent::new(
            u.id.clone(),
            u.email.clone(),
            u.full_name.clone(),
            u.created_at.clone(),
            u.external_id.clone(),
        );

        if let Err(e) = self.publisher.publish(&event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        self.white_listed_repo.create(u).await
    }
}
