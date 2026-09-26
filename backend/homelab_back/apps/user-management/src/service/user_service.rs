use crate::data::create_user_command::CreateUserCommand;
use crate::db::user_repository::UserRepository;
use crate::events::rabbitmq::RabbitMqPublisher;
use crate::helpers::data_error::DataError;
use crate::helpers::user_email::UserEmail;
use async_trait::async_trait;
use derive_new::new;
use homelab_core::events::{UserCreatedEvent, UserUpdatedEvent};
use homelab_core::user_domain::user::User;
use std::sync::Arc;
use uuid::Uuid;

#[async_trait]
pub trait UserService: Send + Sync {
    async fn get_by_email(&self, email: &String) -> Result<Option<User>, DataError>;
    async fn get_all(&self) -> Result<Vec<User>, DataError>;
    async fn finalize(&self, command: CreateUserCommand) -> Result<(), DataError>;
    async fn get_by_id(&self, id: Uuid) -> Result<Option<User>, DataError>;
    async fn toggle_blocked(&self, id: Uuid, blocked: bool) -> Result<(), DataError>;
    async fn set_storage_quota(&self, id: Uuid, allowed_storage: i64) -> Result<(), DataError>;
    async fn get_user_count(&self) -> Result<i64, DataError>;
    async fn update_profile(&self, user_id: Uuid, full_name: String) -> Result<User, DataError>;
}

#[derive(new)]
pub struct UserServiceImpl {
    user_repo: Arc<dyn UserRepository>,
    publisher: Arc<RabbitMqPublisher>,
}

#[async_trait]
impl UserService for UserServiceImpl {
    async fn get_by_email(&self, email: &String) -> Result<Option<User>, DataError> {
        self.user_repo.get_by_email(email.to_string()).await
    }

    async fn get_all(&self) -> Result<Vec<User>, DataError> {
        self.user_repo.get_all().await
    }

    async fn finalize(&self, command: CreateUserCommand) -> Result<(), DataError> {

        let user = self.user_repo.get_by_id(command.user_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("User".to_string()))?;

        let valid_email =
            UserEmail::parse(command.email).map_err(|e| DataError::ValidationError(e))?;

        let cleaned_name = command.full_name.trim().to_string();

        let u = User::new_complete(
            user.id,
            valid_email.into_inner(),
            cleaned_name,
            user.external_id
        );

        // Commit before announcing. Publishing first advertised an email and full_name
        // that were not in the database yet, so a failing `save` left nas provisioned and
        // admin-console logged for a finalize that never happened. Matches the
        // save-then-publish shape `update_profile` already uses.
        self.user_repo.save(u.clone()).await?;

        let event: UserCreatedEvent = UserCreatedEvent::new(
            u.id.clone(),
            u.email.clone(),
            u.full_name.clone(),
            u.created_at.clone(),
            10 * 1024 * 1024 * 1024, // 10GB
            u.external_id.clone(),
        );

        // The account is committed; a failed projection must not fail it.
        if let Err(e) = self.publisher.publish(&event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        Ok(())
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<User>, DataError> {
        self.user_repo.get_by_id(id).await
    }

    async fn toggle_blocked(&self, id: Uuid, blocked: bool) -> Result<(), DataError> {
        let mut user =
            self.user_repo.get_by_id(id).await?.ok_or_else(|| {
                DataError::EntityNotFoundException(format!("User not found: {}", id))
            })?;

        user.toggle_blocked(blocked);

        let updated_event: UserUpdatedEvent = UserUpdatedEvent::new(
            user.id.clone(),
            Some(user.email.clone()),
            Some(user.full_name.clone()),
            None,
            None,
            user.is_blocked.clone(),
        );

        if let Err(e) = self.publisher.publish(&updated_event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }


        self.user_repo.toggle_blocked(user).await?;

        Ok(())
    }

    async fn set_storage_quota(&self, id: Uuid, allowed_storage: i64) -> Result<(), DataError> {
        let user = self.user_repo.get_by_id(id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("User".to_string()))?;

        let event = UserUpdatedEvent::new(
          id,
          None,
          None,
          Some(allowed_storage),
          None,
          user.is_blocked.clone(),
        );

        if let Err(e) = self.publisher.publish(&event).await {
            eprintln!("Failed to publish event: {:?}", e);
        };

        Ok(())
    }

    async fn get_user_count(&self) -> Result<i64, DataError> {
        self.user_repo.get_user_count().await
    }

    /// Renames a user, leaving everything else about the account alone.
    ///
    /// Deliberately not routed through `finalize`: that rebuilds the user with
    /// `User::new_complete`, which resets `role` to `User` and `created_at` to now, and
    /// publishes `user.created` — which would try to insert a second storage profile.
    /// `user.updated` is the non-destructive event; the nas handler keeps the existing
    /// quota when the storage fields are absent.
    async fn update_profile(&self, user_id: Uuid, full_name: String) -> Result<User, DataError> {
        let cleaned = full_name.trim().to_string();

        if cleaned.is_empty() {
            return Err(DataError::ValidationError(
                "Your name cannot be empty".to_string(),
            ));
        }

        let mut user = self
            .user_repo
            .get_by_id(user_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("User".to_string()))?;

        user.full_name = cleaned;

        self.user_repo.save(user.clone()).await?;

        let event = UserUpdatedEvent::new(
            user.id,
            Some(user.email.clone()),
            Some(user.full_name.clone()),
            None,
            None,
            user.is_blocked,
        );

        if let Err(e) = self.publisher.publish(&event).await {
            // The rename is already committed; a failed projection shouldn't fail it.
            eprintln!("Failed to publish user.updated: {:?}", e);
        }

        Ok(user)
    }
}
