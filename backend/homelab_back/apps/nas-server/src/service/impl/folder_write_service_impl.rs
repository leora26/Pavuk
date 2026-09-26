use std::sync::Arc;
use crate::data::create_folder_command::CreateFolderCommand;
use crate::data::move_folder_command::MoveFolderCommand;
use crate::data::update_folder_name_command::UpdateFolderNameCommand;
use crate::helpers::data_error::DataError;
use async_trait::async_trait;
use derive_new::new;
use uuid::Uuid;
use homelab_core::events::{DeletionType, TrashCleanUpTriggeredEvent};
use homelab_core::helpers::rabbitmq_publisher::RabbitMqPublisher;
use homelab_core::nas_domain::folder::Folder;
use crate::db::folder_repository::FolderRepository;
use crate::service::contract::folder_write_service::FolderWriteService;

#[derive(new)]
pub struct FolderWriteServiceImpl {
    folder_repo: Arc<dyn FolderRepository>,
    publisher: Arc<RabbitMqPublisher>,
}

#[async_trait]
impl FolderWriteService for FolderWriteServiceImpl {

    async fn update_folder_name(
        &self,
        command: UpdateFolderNameCommand,
        folder_id: Uuid,
    ) -> Result<Folder, DataError> {
        let mut folder: Folder = self
            .folder_repo
            .get_by_id(folder_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        folder.rename(command.new_name);

        self.folder_repo.update_folder(folder).await
    }

    async fn trash_chosen_folders(&self, folder_ids: &[Uuid]) -> Result<(), DataError> {
        self.folder_repo.mark_folders_deleted(folder_ids).await
    }

    async fn trash(&self, folder_id: Uuid) -> Result<(), DataError> {
        self.folder_repo.mark_folder_deleted(folder_id).await
    }

    async fn create(&self, command: CreateFolderCommand) -> Result<Folder, DataError> {
        let f = Folder::new(
            Uuid::new_v4(),
            Some(command.parent_folder_id),
            command.name,
            command.owner_id,
        );

        self.folder_repo.create(f).await
    }

    async fn move_folder(&self, command: MoveFolderCommand) -> Result<Folder, DataError> {
        let mut folder = self
            .folder_repo
            .get_by_id(command.folder_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("Folder".to_string()))?;

        folder.update_parent_folder(command.target_folder);

        Ok(self.folder_repo.update_folder(folder).await?)
    }

    async fn clean_up_trash(&self, user_id: Uuid) -> Result<(), DataError> {
        // `All` is the whole-trash branch. `File` would make the consumer look for an
        // id that this event deliberately doesn't carry, and fail with "Missing File ID"
        // — which is why emptying the trash silently did nothing.
        let event = TrashCleanUpTriggeredEvent::new(
            user_id,
            DeletionType::All,
            None
        );

        self
            .publisher
            .publish(&event)
            .await
            .map_err(|e| DataError::MessageQueueError(format!("TrashCleanUpTriggeredEvent to clean up user's trash {:?}", e)))?;

        Ok(())
    }

    async fn permanently_delete_folder(&self, folder_id: Uuid, user_id: Uuid) -> Result<(), DataError> {
        println!("Cleaning up deleted folder: {:?}", folder_id);
        let folder = self.folder_repo.get_by_id(folder_id).await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        if folder.owner_id != user_id {
            return Err(DataError::InvalidDataError("Cannot delete folder that does not belong to this user".to_string()));
        }

        let event = TrashCleanUpTriggeredEvent::new(
            user_id,
            DeletionType::Folder,
            Some(folder_id)
        );

        self
            .publisher
            .publish(&event)
            .await
            .map_err(|e| DataError::MessageQueueError(format!("TrashCleanUpTriggeredEvent to delete a folder {:?}", e)))?;

        Ok(())
    }

    async fn restore_deleted_folder(&self, folder_id: Uuid) -> Result<(), DataError> {
        let mut folder = self.folder_repo.get_by_id(folder_id).await?
            .ok_or_else(|| DataError::EntityNotFoundException("Folder".to_string()))?;

        let parent_folder = self.folder_repo.get_by_id(folder.parent_folder_id.unwrap())
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("Folder".to_string()))?;

        let root_folder = self.folder_repo.get_root(folder.owner_id)
            .await?.ok_or_else(|| DataError::EntityNotFoundException("Folder".to_string()))?;

        if parent_folder.is_deleted {
            folder.update_parent_folder(root_folder.id);

            let _f = self.folder_repo.update_folder(folder).await?;
        }

        self.folder_repo.restore_deleted_folder(folder_id).await
    }
}
