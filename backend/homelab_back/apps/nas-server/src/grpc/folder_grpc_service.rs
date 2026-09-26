use crate::data::create_folder_command::CreateFolderCommand;
use crate::data::move_folder_command::MoveFolderCommand;
use crate::data::update_folder_name_command::UpdateFolderNameCommand;
use crate::grpc::ownership::folder_owned_by;
use crate::helpers::proto_mappers::{map_entity_id, map_file_to_proto, map_folder_to_proto};
use crate::AppState;
use async_trait::async_trait;
use derive_new::new;
use homelab_core::auth::extractor::RequestIdentityExt;
use homelab_proto::nas::folder_service_server::FolderService;
use homelab_proto::nas::{CleanUpDeletedFolderRequest, CleanUpTrashRequest, CreateFolderRequest, DeleteAllFolderRequest, DeleteFolderRequest, FileListResponse, FolderResponse, FolderResponseList, GetAllSubfoldersRequest, GetDeletedFoldersRequest, GetFilesForFolderRequest, GetFolderRequest, GetRootFolderRequest, GetTrashFilesForFolderRequest, GetTrashSubfoldersForFolderRequest, MoveFolderRequest, RenameFolderRequest, RestoreDeletedFolderRequest, SearchFolderRequest};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

#[derive(new)]
pub struct GrpcFolderService {
    app_state: Arc<AppState>,
}

#[async_trait]
impl FolderService for GrpcFolderService {
    async fn get_root_folder(
        &self,
        request: Request<GetRootFolderRequest>,
    ) -> Result<Response<FolderResponse>, Status> {
        let internal_user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let folder = self
            .app_state
            .folder_read_service
            .get_root(internal_user_id)
            .await?
            .ok_or_else(|| {
                Status::not_found(format!("Failed to find root {}", internal_user_id))
            })?;

        Ok(Response::new(map_folder_to_proto(folder)))
    }

    async fn get_folder(
        &self,
        request: Request<GetFolderRequest>,
    ) -> Result<Response<FolderResponse>, Status> {
        let user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let folder_id = map_entity_id(req.id)?;

        let folder = folder_owned_by(&self.app_state, folder_id, user_id).await?;

        Ok(Response::new(map_folder_to_proto(folder)))
    }

    async fn get_subfolders(
        &self,
        request: Request<GetAllSubfoldersRequest>,
    ) -> Result<Response<FolderResponseList>, Status> {
        let user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let folder_id = map_entity_id(req.id)?;

        folder_owned_by(&self.app_state, folder_id, user_id).await?;

        let folders = self
            .app_state
            .folder_read_service
            .get_children_by_id(folder_id)
            .await?;

        let proto_folders = folders
            .into_iter()
            .map(|f| map_folder_to_proto(f))
            .collect();

        Ok(Response::new(FolderResponseList {
            folders: proto_folders,
        }))
    }

    async fn get_files_for_folder(
        &self,
        request: Request<GetFilesForFolderRequest>,
    ) -> Result<Response<FileListResponse>, Status> {
        let user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let folder_id = map_entity_id(req.id)?;

        folder_owned_by(&self.app_state, folder_id, user_id).await?;

        let files = self
            .app_state
            .folder_read_service
            .get_by_folder(folder_id)
            .await?;

        let file_ids: Vec<Uuid> = files.iter().map(|f| f.id).collect();

        let mut labels = self
            .app_state
            .file_label_service
            .get_labels_for_files(&file_ids, user_id)
            .await?;

        let proto_files = files
            .into_iter()
            .map(|f| {
                let labels = labels.remove(&f.id).unwrap_or_default();
                map_file_to_proto(f, labels)
            })
            .collect();

        Ok(Response::new(FileListResponse { files: proto_files }))
    }

    async fn get_trash_files_for_folder(
        &self,
        request: Request<GetTrashFilesForFolderRequest>,
    ) -> Result<Response<FileListResponse>, Status> {
        let user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let folder_id = map_entity_id(req.id)?;

        folder_owned_by(&self.app_state, folder_id, user_id).await?;

        let files = self
            .app_state
            .folder_read_service
            .get_trash_files(folder_id)
            .await?;

        let proto_files = files.into_iter().map(|f| map_file_to_proto(f, Vec::new())).collect();

        Ok(Response::new(FileListResponse { files: proto_files }))
    }

    async fn get_trash_subfolder_for_folder(
        &self,
        request: Request<GetTrashSubfoldersForFolderRequest>,
    ) -> Result<Response<FolderResponseList>, Status> {
        let user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let folder_id = map_entity_id(req.id)?;

        folder_owned_by(&self.app_state, folder_id, user_id).await?;

        let subfolder = self
            .app_state
            .folder_read_service
            .get_trash_subfolder(folder_id)
            .await?;

        let proto_folders = subfolder
            .into_iter()
            .map(|f| map_folder_to_proto(f))
            .collect();

        Ok(Response::new(FolderResponseList {
            folders: proto_folders,
        }))
    }

    async fn get_deleted_folders(
        &self,
        request: Request<GetDeletedFoldersRequest>,
    ) -> Result<Response<FolderResponseList>, Status> {
        let internal_user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let folders = self
            .app_state
            .folder_read_service
            .get_deleted_folders(internal_user_id)
            .await?;

        let proto_folders = folders
            .into_iter()
            .map(|f| map_folder_to_proto(f))
            .collect();

        Ok(Response::new(FolderResponseList {
            folders: proto_folders,
        }))
    }

    async fn delete_folder(
        &self,
        request: Request<DeleteFolderRequest>,
    ) -> Result<Response<()>, Status> {
        let user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let folder_id = map_entity_id(req.id)?;

        folder_owned_by(&self.app_state, folder_id, user_id).await?;

        self.app_state.folder_write_service.trash(folder_id).await?;

        Ok(Response::new(()))
    }

    async fn rename_folder(
        &self,
        request: Request<RenameFolderRequest>,
    ) -> Result<Response<FolderResponse>, Status> {
        let user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let folder_id = map_entity_id(req.id)?;

        folder_owned_by(&self.app_state, folder_id, user_id).await?;

        let command = UpdateFolderNameCommand::new(req.new_name);

        let folder = self
            .app_state
            .folder_write_service
            .update_folder_name(command, folder_id)
            .await?;

        Ok(Response::new(map_folder_to_proto(folder)))
    }

    async fn search_folder(
        &self,
        request: Request<SearchFolderRequest>,
    ) -> Result<Response<FolderResponseList>, Status> {

        let user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let folders = self
            .app_state
            .folder_read_service
            .search_folder(req.query, user_id)
            .await?;

        let proto_folders = folders
            .into_iter()
            .map(|f| map_folder_to_proto(f))
            .collect();

        Ok(Response::new(FolderResponseList {
            folders: proto_folders,
        }))
    }

    async fn delete_chosen_folders(
        &self,
        request: Request<DeleteAllFolderRequest>,
    ) -> Result<Response<()>, Status> {
        let user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let folder_ids: Vec<Uuid> = req
            .id
            .into_iter()
            .map(|folder_id| map_entity_id(Some(folder_id)))
            .collect::<Result<Vec<_>, _>>()?;

        for folder_id in &folder_ids {
            folder_owned_by(&self.app_state, *folder_id, user_id).await?;
        }

        self.app_state
            .folder_write_service
            .trash_chosen_folders(&folder_ids)
            .await?;

        Ok(Response::new(()))
    }

    async fn move_folder(
        &self,
        request: Request<MoveFolderRequest>,
    ) -> Result<Response<FolderResponse>, Status> {
        let user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let target_folder = map_entity_id(req.target_folder)?;

        let folder_id = map_entity_id(req.folder_id)?;

        // Both the folder being moved and its destination must belong to the caller.
        folder_owned_by(&self.app_state, folder_id, user_id).await?;
        folder_owned_by(&self.app_state, target_folder, user_id).await?;

        let command = MoveFolderCommand::new(target_folder, folder_id);

        let folder = self.app_state.folder_write_service.move_folder(command).await?;

        Ok(Response::new(map_folder_to_proto(folder)))
    }

    async fn create_folder(
        &self,
        request: Request<CreateFolderRequest>,
    ) -> Result<Response<FolderResponse>, Status> {
        let internal_user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let parent_folder_id = map_entity_id(req.parent_folder_id)?;

        // The parent must belong to the caller, otherwise a user could plant a
        // folder inside another user's tree.
        folder_owned_by(&self.app_state, parent_folder_id, internal_user_id).await?;

        let command = CreateFolderCommand::new(parent_folder_id, req.name, internal_user_id);

        let folder = self.app_state.folder_write_service.create(command).await?;

        Ok(Response::new(map_folder_to_proto(folder)))
    }

    async fn clean_up_deleted_folder(
        &self,
        request: Request<CleanUpDeletedFolderRequest>,
    ) -> Result<Response<()>, Status> {
        let internal_user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;
        let req = request.into_inner();

        let folder_id = map_entity_id(req.folder_id)?;

        folder_owned_by(&self.app_state, folder_id, internal_user_id).await?;

        self.app_state
            .folder_write_service
            .permanently_delete_folder(folder_id, internal_user_id)
            .await?;

        Ok(Response::new(()))
    }

    async fn clean_up_trash(
        &self,
        request: Request<CleanUpTrashRequest>,
    ) -> Result<Response<()>, Status> {
        let internal_user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        self.app_state
            .folder_write_service
            .clean_up_trash(internal_user_id)
            .await?;

        Ok(Response::new(()))
    }

    async fn restore_deleted_folder(
        &self,
        request: Request<RestoreDeletedFolderRequest>,
    ) -> Result<Response<()>, Status> {
        let user_id = request
            .get_internal_id(&self.app_state.cached_identity_resolver)
            .await?;

        let req = request.into_inner();

        let folder_id = map_entity_id(req.folder_id)?;

        folder_owned_by(&self.app_state, folder_id, user_id).await?;

        self.app_state
            .folder_write_service
            .restore_deleted_folder(folder_id)
            .await?;

        Ok(Response::new(()))
    }
}
