use async_trait::async_trait;
use uuid::Uuid;
use homelab_core::nas_domain::file::{File, FileType};
use homelab_core::nas_domain::folder::Folder;
use crate::helpers::data_error::DataError;

#[async_trait]
pub trait FolderReadService: Send + Sync {
    async fn get_root(&self, user_id: Uuid) -> Result<Option<Folder>, DataError>;
    async fn get_by_id(&self, folder_id: Uuid) -> Result<Option<Folder>, DataError>;
    async fn get_children_by_id(&self, folder_id: Uuid) -> Result<Vec<Folder>, DataError>;
    async fn search_folder(&self, search_query: String, user_id: Uuid) -> Result<Vec<Folder>, DataError>;
    async fn filter_files_by_folder(
        &self,
        file_types: &[FileType],
        folder_id: Uuid,
    ) -> Result<Vec<File>, DataError>;
    async fn get_folder_path(&self, folder_id: Uuid) -> Result<String, DataError>;
    async fn get_by_folder(&self, folder_id: Uuid) -> Result<Vec<File>, DataError>;
    async fn get_trash_files(&self, folder_id: Uuid) -> Result<Vec<File>, DataError>;
    async fn get_trash_subfolder(&self, folder_id: Uuid) -> Result<Vec<Folder>, DataError>;
    async fn get_deleted_folders(&self, user_id: Uuid) -> Result<Vec<Folder>, DataError>;
}