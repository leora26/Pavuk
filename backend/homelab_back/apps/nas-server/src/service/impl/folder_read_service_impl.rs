use crate::db::folder_repository::FolderRepository;
use crate::helpers::data_error::DataError;
use async_recursion::async_recursion;
use async_trait::async_trait;
use std::sync::Arc;
use derive_new::new;
use uuid::Uuid;
use homelab_core::nas_domain::file::{File, FileType};
use homelab_core::nas_domain::folder::Folder;
use crate::service::contract::folder_read_service::FolderReadService;

#[derive(new)]
pub struct FolderReadServiceImpl {
    folder_repo: Arc<dyn FolderRepository>,
}

impl FolderReadServiceImpl {
    #[async_recursion]
    async fn get_parent_folder_name(&self, f_id: Uuid) -> Result<String, DataError> {
        let f = self
            .folder_repo
            .get_by_id(f_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("Folder".to_string()))?;

        if let Some(parent_id) = f.parent_folder_id {
            let parent_path = self.get_parent_folder_name(parent_id).await?;
            Ok(format!("{}/{}", parent_path, f.name))
        } else {
            Ok(f.name)
        }
    }
}

#[async_trait]
impl FolderReadService for FolderReadServiceImpl {
    async fn get_root(&self, user_id: Uuid) -> Result<Option<Folder>, DataError> {
        self.folder_repo.get_root(user_id).await
    }

    async fn get_by_id(&self, folder_id: Uuid) -> Result<Option<Folder>, DataError> {
        self.folder_repo.get_by_id(folder_id).await
    }

    async fn get_children_by_id(&self, folder_id: Uuid) -> Result<Vec<Folder>, DataError> {
        self.folder_repo.get_children_by_id(folder_id).await
    }

    async fn search_folder(&self, search_query: String, user_id: Uuid) -> Result<Vec<Folder>, DataError> {
        self.folder_repo
            .search_by_name(format!("%{}%", search_query), user_id)
            .await
    }

    async fn filter_files_by_folder(
        &self,
        file_types: &[FileType],
        folder_id: Uuid,
    ) -> Result<Vec<File>, DataError> {
        self.folder_repo
            .filter_files_in_folder(file_types, folder_id)
            .await
    }

    async fn get_folder_path(&self, folder_id: Uuid) -> Result<String, DataError> {
        let path = self.get_parent_folder_name(folder_id).await?;
        Ok(path)
    }

    async fn get_by_folder(&self, folder_id: Uuid) -> Result<Vec<File>, DataError> {
        self.folder_repo.get_by_folder_id(folder_id).await
    }

    async fn get_trash_files(&self, folder_id: Uuid) -> Result<Vec<File>, DataError> {
        self.folder_repo.get_trash_file_for_folder(folder_id).await
    }

    async fn get_trash_subfolder(&self, folder_id: Uuid) -> Result<Vec<Folder>, DataError> {
        self.folder_repo.get_trash_subfolder_for_folder(folder_id).await
    }

    async fn get_deleted_folders(&self, user_id: Uuid) -> Result<Vec<Folder>, DataError> {
        self.folder_repo.get_deleted_folders(user_id).await
    }

}
