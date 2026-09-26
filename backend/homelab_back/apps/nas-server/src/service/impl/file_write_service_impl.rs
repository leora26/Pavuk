use crate::data::copy_file_command::CopyFileCommand;
use crate::data::init_file_command::InitFileCommand;
use crate::data::move_file_command::MoveFileCommand;
use crate::data::update_file_name_command::UpdateFileNameCommand;
use crate::db::file_repository::FileRepository;
use crate::db::folder_repository::FolderRepository;
use crate::db::nas_identity_repository::NasIdentityRepository;
use crate::db::storage_profile_repository::StorageProfileRepository;
use crate::events::rabbitmq::RabbitMqPublisher;
use crate::helpers::data_error::DataError;
use async_compression::tokio::write::{GzipDecoder, GzipEncoder};
use async_trait::async_trait;
use derive_new::new;
use homelab_core::constants::MB;
use homelab_core::events::{DeletionType, FileUpdatedEvent, FileUploadedEvent, TrashCleanUpTriggeredEvent, UserUpdatedEvent};
use homelab_core::nas_domain::file::{File, FileType, UploadStatus};
use homelab_core::nas_domain::folder::Folder;
use homelab_core::nas_domain::storage_profile::StorageProfile;
use sqlx::types::time::OffsetDateTime;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc::Receiver;
use uuid::Uuid;
use crate::service::contract::file_write_service::FileWriteService;
use crate::service::contract::preview_service::PreviewService;
use crate::service::r#impl::preview_service_impl::PreviewServiceImpl;

#[derive(new)]
pub struct FileWriteServiceImpl {
    file_repo: Arc<dyn FileRepository>,
    folder_repo: Arc<dyn FolderRepository>,
    sp_repo: Arc<dyn StorageProfileRepository>,
    nas_identity_repo: Arc<dyn NasIdentityRepository>,
    storage_path: PathBuf,
    publisher: Arc<RabbitMqPublisher>,
}

#[async_trait]
impl FileWriteService for FileWriteServiceImpl {

    async fn upload(&self, command: InitFileCommand) -> Result<File, DataError> {
        let folder: Folder = self
            .folder_repo
            .get_by_id(command.destination)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("Folder".to_string()))?;

        if let Some(_) = self
            .file_repo
            .get_by_folder_and_file_name(folder.id, command.name.clone())
            .await?
        {
            return Err(DataError::FileAlreadyExistsError);
        }

        let sp: StorageProfile = self
            .sp_repo
            .get_by_id(command.owner_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("User".to_string()))?;

        if sp.validate_storage_size(command.expected_size) {
            let f = File::new(
                Uuid::new_v4(),
                command.name,
                sp.user_id,
                folder.id,
                false,
                command.expected_size,
                OffsetDateTime::now_utc(),
                OffsetDateTime::now_utc(),
            );

            let file_event: FileUploadedEvent = FileUploadedEvent::new(
                f.id.clone(),
                f.parent_folder_id.clone(),
                f.file_type.clone(),
                f.is_deleted.clone(),
                f.ttl.clone(),
                f.size.clone(),
                f.upload_status.clone(),
                f.is_archived(&self.storage_path),
                f.created_at.clone(),
            );

            if let Err(e) = self.publisher.publish(&file_event).await {
                eprintln!("Failed to publish event: {:?}", e);
            }

            // Block state comes from nas's identity projection; admin-console writes this
            // field straight into `console_users.is_blocked`.
            let is_blocked = self.nas_identity_repo.is_user_blocked(sp.user_id).await?;

            let sp_event: UserUpdatedEvent = UserUpdatedEvent::new(
                sp.user_id.clone(),
                None,
                None,
                Some(sp.allowed_storage.clone()),
                Some(sp.taken_storage.clone()),
                is_blocked,
            );

            if let Err(e) = self.publisher.publish(&sp_event).await {
                eprintln!("Failed to publish event: {:?}", e);
            }

            self.file_repo.save(f).await
        } else {
            Err(DataError::NoFreeStorageError)
        }
    }

    async fn upload_stream(
        &self,
        file_id: Uuid,
        mut rx: Receiver<Result<Vec<u8>, DataError>>,
    ) -> Result<(), DataError> {
        let mut hasher = blake3::Hasher::new();
        let mut f = self
            .file_repo
            .get_by_id(file_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        if f.upload_status != UploadStatus::Pending {
            return Err(DataError::ValidationError(
                "File is not pending".to_string(),
            ));
        }

        let file_path = f.build_file_path(&self.storage_path);

        // Make sure that parent directories exist.
        // Storing a bunch of files without parent directories that come build_file_path would be an unoptimized
        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| DataError::IOError(format!("Failed to create buckets: {}", e)))?;
        }

        let file_handle = tokio::fs::File::create(&file_path)
            .await
            .map_err(|e| DataError::IOError(e.to_string()))?;

        let mut writer = BufWriter::with_capacity(MB as usize, file_handle);
        let mut total_bytes = 0i64;

        while let Some(chunk_result) = rx.recv().await {
            let data = chunk_result?;

            total_bytes += data.len() as i64;
            hasher.update(&data);

            if let Err(e) = writer.write_all(&data).await {
                let _ = tokio::fs::remove_file(&file_path).await;
                return Err(DataError::IOError(e.to_string()));
            }
        }

        if let Err(e) = writer.flush().await {
            let _ = tokio::fs::remove_file(&file_path).await;
            return Err(DataError::IOError(e.to_string()));
        }

        // If the file size is not the same as provided during initialization,
        // we need to remove file from the disk since the file was probably corrupted
        if !f.validate_size(total_bytes) {
            f.update_status(UploadStatus::Failed);
            self.file_repo.update(f.clone()).await?;
            let _ = tokio::fs::remove_file(&file_path).await;

            let event: FileUpdatedEvent = FileUpdatedEvent::new(
                f.id.clone(),
                f.parent_folder_id.clone(),
                f.is_deleted.clone(),
                f.ttl.clone(),
                f.size.clone(),
                f.upload_status.clone(),
                f.is_archived(&self.storage_path),
            );

            if let Err(e) = self.publisher.publish(&event).await {
                eprintln!("Failed to publish event: {:?}", e);
            }

            return Err(DataError::NotMatchingByteSizeError);
        }

        let final_hash = hasher.finalize().to_hex().to_string();

        f.update_status(UploadStatus::Completed);
        f.set_file_hash(final_hash);
        self.file_repo.update(f.clone()).await?;

        // The row is `completed` now, so a resync picks these bytes up.
        self.resync_storage(f.owner_id).await?;

        let event: FileUpdatedEvent = FileUpdatedEvent::new(
            f.id.clone(),
            f.parent_folder_id.clone(),
            f.is_deleted.clone(),
            f.ttl.clone(),
            f.size.clone(),
            f.upload_status.clone(),
            f.is_archived(&self.storage_path),
        );

        if let Err(e) = self.publisher.publish(&event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        // After the file has been uploaded we need to create a preview of this file
        PreviewServiceImpl::spawn_generation(f, self.storage_path.clone());

        Ok(())
    }

    async fn update_file_name(
        &self,
        command: UpdateFileNameCommand,
        file_id: Uuid,
    ) -> Result<File, DataError> {
        let mut file: File = self
            .file_repo
            .get_by_id(file_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        if file.is_deleted {
            return Err(DataError::ValidationError(
                "Cannot rename a deleted file".to_string(),
            ));
        }

        file.rename(command.new_name);

        self.file_repo.update(file).await
    }

    async fn update_deleted_file(&self, id: Uuid) -> Result<File, DataError> {
        println!("Trying to restore a file: {}", id.to_string());
        let mut file = self
            .file_repo
            .get_deleted_by_id(id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        file.set_as_undeleted();

        let event: FileUpdatedEvent = FileUpdatedEvent::new(
            file.id.clone(),
            file.parent_folder_id.clone(),
            file.is_deleted.clone(),
            file.ttl.clone(),
            file.size.clone(),
            file.upload_status.clone(),
            file.is_archived(&self.storage_path),
        );

        if let Err(e) = self.publisher.publish(&event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        self.file_repo.update(file).await
    }

    async fn delete_chosen_files(&self, file_ids: &[Uuid]) -> Result<(), DataError> {
        let files = self.file_repo.get_all_by_ids(file_ids).await?;

        for mut file in files {
            file.set_as_deleted();

            let event: FileUpdatedEvent = FileUpdatedEvent::new(
                file.id.clone(),
                file.parent_folder_id.clone(),
                file.is_deleted.clone(),
                file.ttl.clone(),
                file.size.clone(),
                file.upload_status.clone(),
                file.is_archived(&self.storage_path),
            );

            self.file_repo.update(file).await?;

            if let Err(e) = self.publisher.publish(&event).await {
                eprintln!("Failed to publish event: {:?}", e);
            }
        }

        Ok(())
    }

    async fn delete(&self, file_id: Uuid) -> Result<(), DataError> {
        let mut file = self
            .file_repo
            .get_by_id(file_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        file.set_as_deleted();

        let event: FileUpdatedEvent = FileUpdatedEvent::new(
            file.id.clone(),
            file.parent_folder_id.clone(),
            file.is_deleted.clone(),
            file.ttl.clone(),
            file.size.clone(),
            file.upload_status.clone(),
            file.is_archived(&self.storage_path),
        );

        if let Err(e) = self.publisher.publish(&event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        let _ = self.file_repo.update(file).await?;

        Ok(())
    }

    async fn move_file(&self, command: MoveFileCommand) -> Result<File, DataError> {
        let mut file = self
            .file_repo
            .get_by_id(command.file_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        if file.is_deleted {
            return Err(DataError::ValidationError(
                "Cannot rename a deleted file".to_string(),
            ));
        }

        file.update_parent_folder(command.folder_id);

        let event: FileUpdatedEvent = FileUpdatedEvent::new(
            file.id.clone(),
            file.parent_folder_id.clone(),
            file.is_deleted.clone(),
            file.ttl.clone(),
            file.size.clone(),
            file.upload_status.clone(),
            file.is_archived(&self.storage_path),
        );

        if let Err(e) = self.publisher.publish(&event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        Ok(self.file_repo.update(file).await?)
    }

    async fn copy_file(&self, command: CopyFileCommand) -> Result<File, DataError> {
        let file = self
            .file_repo
            .get_by_id(command.file_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        let sp = self
            .sp_repo
            .get_by_id(file.owner_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("User".to_string()))?;

        if !sp.validate_storage_size(file.size) {
            return Err(DataError::NoFreeStorageError);
        }

        let new_name = if file.parent_folder_id == command.target_folder_id {
            let path = Path::new(&file.name);

            let file_stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(&file.name);

            match path.extension().and_then(|e| e.to_str()) {
                Some(ext) => format!("{}.{}", file_stem, ext),
                None => file_stem.to_string(),
            }
        } else {
            file.name.clone()
        };

        let new_file_id = Uuid::new_v4();

        let mut new_file = File::new(
            new_file_id,
            new_name,
            sp.user_id.clone(),
            command.target_folder_id,
            false,
            file.size,
            file.created_at,
            OffsetDateTime::now_utc(),
        );

        new_file.upload_status = UploadStatus::Completed;

        let source_path = file.build_file_path(&self.storage_path);
        let dest_path = new_file.build_file_path(&self.storage_path);

        if let Some(parent) = dest_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| DataError::IOError(format!("Failed to create buckets: {}", e)))?;
        }

        if let Err(e) = tokio::fs::copy(&source_path, &dest_path).await {
            return Err(DataError::IOError(e.to_string()));
        }

        // Previews live beside the original as `{id}.preview.png` / `{id}.preview.jpg`
        // and are keyed by file id, so a copy gets a new id and would otherwise have no
        // preview at all. Carry whichever one exists across.
        //
        // Best-effort on purpose: a copy that lands without its thumbnail is still a
        // valid copy, and failing the whole operation over a thumbnail would be worse.
        for preview_extension in ["preview.png", "preview.jpg"] {
            let source_preview = source_path.with_extension(preview_extension);

            if tokio::fs::try_exists(&source_preview).await.unwrap_or(false) {
                let dest_preview = dest_path.with_extension(preview_extension);

                if let Err(e) = tokio::fs::copy(&source_preview, &dest_preview).await {
                    eprintln!(
                        "Failed to copy {} preview for {}: {}",
                        preview_extension, new_file_id, e
                    );
                }
            }
        }

        // No usage event here — the copy hasn't been recorded yet, so anything published
        // now would carry the pre-copy figure. `resync_storage` announces it below.
        match self.file_repo.save(new_file.clone()).await {
            Ok(uploaded_file) => {
                self.resync_storage(new_file.owner_id).await?;

                let file_event: FileUploadedEvent = FileUploadedEvent::new(
                    new_file.id.clone(),
                    new_file.parent_folder_id.clone(),
                    new_file.file_type.clone(),
                    new_file.is_deleted.clone(),
                    new_file.ttl.clone(),
                    new_file.size.clone(),
                    new_file.upload_status.clone(),
                    new_file.is_archived(&self.storage_path),
                    new_file.created_at.clone(),
                );

                if let Err(e) = self.publisher.publish(&file_event).await {
                    eprintln!("Failed to publish event: {:?}", e);
                }

                Ok(uploaded_file)
            }
            Err(err) => {
                if let Err(del_err) = tokio::fs::remove_file(&dest_path).await {
                    return Err(DataError::IOError(format!(
                        "Failed to delete ghost file. Needs immediate attention: {}",
                        del_err.to_string()
                    )));
                }

                Err(err)
            }
        }
    }

    async fn update_stream(
        &self,
        file_id: Uuid,
        mut rx: Receiver<Result<Vec<u8>, DataError>>,
    ) -> Result<(), DataError> {
        let mut f = self
            .file_repo
            .get_by_id(file_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        if f.is_deleted {
            return Err(DataError::ValidationError(
                "Cannot rename a deleted file".to_string(),
            ));
        }

        let target_path = f.build_file_path(&self.storage_path);

        let parent = target_path
            .parent()
            .ok_or_else(|| DataError::UnknownError("Invalid file path structure".to_string()))?;

        let temp_path = parent.join(format!("{}.tmp", f.id));

        let file_handle = fs::File::create(&temp_path)
            .await
            .map_err(|e| DataError::IOError(e.to_string()))?;

        let mut writer = BufWriter::with_capacity(MB as usize, file_handle);
        let mut new_total_bytes = 0i64;

        while let Some(chunk_result) = rx.recv().await {
            let data = chunk_result?;
            new_total_bytes += data.len() as i64;

            if let Err(e) = writer.write_all(&data).await {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(DataError::IOError(e.to_string()));
            }
        }

        if let Err(e) = writer.flush().await {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(DataError::IOError(e.to_string()));
        }

        if let Err(e) = writer.get_ref().sync_all().await {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(DataError::IOError(e.to_string()));
        }

        let old_size = f.size;
        let size_diff = new_total_bytes - old_size;

        // Only growth needs checking; replacing content with something smaller can never
        // breach the quota. The usage figure itself is resynced from the row further down.
        if size_diff > 0 {
            let sp = self
                .sp_repo
                .get_by_id(f.owner_id)
                .await?
                .ok_or_else(|| DataError::EntityNotFoundException("User".to_string()))?;

            if !sp.validate_storage_size(size_diff) {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(DataError::NoFreeStorageError);
            }
        }

        if let Err(e) = tokio::fs::rename(&temp_path, &target_path).await {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(DataError::IOError(e.to_string()));
        }

        f.update_size(new_total_bytes);

        let event: FileUpdatedEvent = FileUpdatedEvent::new(
            f.id.clone(),
            f.parent_folder_id.clone(),
            f.is_deleted.clone(),
            f.ttl.clone(),
            f.size.clone(),
            f.upload_status.clone(),
            f.is_archived(&self.storage_path),
        );

        if let Err(e) = self.publisher.publish(&event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        let owner_id = f.owner_id;

        self.file_repo.update(f).await?;
        self.resync_storage(owner_id).await?;

        Ok(())
    }

    async fn archive_file(&self, file_id: Uuid) -> Result<(), DataError> {
        let mut file = self
            .file_repo
            .get_by_id(file_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        if file.is_archived(&self.storage_path) {
            return Err(DataError::FileIsAlreadyArchivedError);
        }

        let original_path = file.build_file_path(&self.storage_path);

        let mut compressed_path = original_path.clone();
        compressed_path.set_extension("gz");

        let mut source_file = fs::File::open(&original_path)
            .await
            .map_err(|e| DataError::IOError(e.to_string()))?;

        let dest_file = fs::File::create(&compressed_path)
            .await
            .map_err(|e| DataError::IOError(e.to_string()))?;

        let buffered_writer = BufWriter::new(dest_file);
        let mut encoder = GzipEncoder::new(buffered_writer);

        if let Err(e) = tokio::io::copy(&mut source_file, &mut encoder).await {
            let _ = fs::remove_file(&compressed_path).await;
            return Err(DataError::IOError(e.to_string()));
        }

        if let Err(e) = encoder.shutdown().await {
            let _ = fs::remove_file(&compressed_path).await;
            return Err(DataError::IOError(e.to_string()));
        }

        let metadata = fs::metadata(&compressed_path)
            .await
            .map_err(|e| DataError::IOError(e.to_string()))?;

        let archived_size = metadata.len() as i64;

        fs::remove_file(&original_path)
            .await
            .map_err(|e| DataError::IOError(e.to_string()))?;

        file.rename(format!("{}.gz", file.name));
        file.size = archived_size;
        file.update_type(FileType::Zip);

        let event: FileUpdatedEvent = FileUpdatedEvent::new(
            file.id.clone(),
            file.parent_folder_id.clone(),
            file.is_deleted.clone(),
            file.ttl.clone(),
            file.size.clone(),
            file.upload_status.clone(),
            file.is_archived(&self.storage_path),
        );

        let owner_id = file.owner_id;

        self.file_repo.update(file).await?;

        // After the row carries the archived size, never before — the figure is derived
        // from the rows, so resyncing early would just recompute the old value.
        self.resync_storage(owner_id).await?;

        if let Err(e) = self.publisher.publish(&event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        Ok(())
    }

    async fn unarchive_file(&self, file_id: Uuid) -> Result<(), DataError> {
        let mut file = self
            .file_repo
            .get_by_id(file_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        if !file.is_archived(&self.storage_path) {
            return Err(DataError::FileIsNotArchivedError);
        }

        let compressed_path = file.build_file_path(&self.storage_path);

        // The restored file keeps its ORIGINAL extension (e.g. foo.txt.gz -> foo.txt),
        // so the decompressed bytes must land at that path. Stripping the extension
        // entirely would orphan the file, since build_file_path expects `{id}.<ext>`.
        let restored_name = file
            .name
            .strip_suffix(".gz")
            .unwrap_or(&file.name)
            .to_string();

        let mut output_path = compressed_path.clone();
        match Path::new(&restored_name).extension().and_then(|e| e.to_str()) {
            Some(ext) => output_path.set_extension(ext),
            None => output_path.set_extension(""),
        };

        let source_file = fs::File::open(&compressed_path)
            .await
            .map_err(|e| DataError::IOError(e.to_string()))?;

        let reader = BufReader::new(source_file);
        let mut decoder = GzipDecoder::new(reader);

        let dest_file = fs::File::create(&output_path)
            .await
            .map_err(|e| DataError::IOError(e.to_string()))?;

        let mut buffered_writer = BufWriter::new(dest_file);

        if let Err(e) = tokio::io::copy(&mut decoder, &mut buffered_writer).await {
            let _ = fs::remove_file(&output_path).await;
            return Err(DataError::IOError(e.to_string()));
        }

        if let Err(e) = buffered_writer.flush().await {
            let _ = fs::remove_file(&output_path).await;
            return Err(DataError::IOError(e.to_string()));
        }

        let metadata = fs::metadata(&output_path)
            .await
            .map_err(|e| DataError::IOError(e.to_string()))?;

        let unarchived_size = metadata.len() as i64;
        let original_compressed_size = file.size;

        let sp = self
            .sp_repo
            .get_by_id(file.owner_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("User".to_string()))?;

        // Extracting usually grows the file, so it needs the same quota check an upload
        // gets. Gzip inflates already-compressed formats, so this delta can also be
        // negative — `validate_storage_size` handles that fine, it just always passes.
        let size_difference = unarchived_size - original_compressed_size;

        if !sp.validate_storage_size(size_difference) {
            let _ = fs::remove_file(&output_path).await;
            return Err(DataError::NoFreeStorageError);
        }

        fs::remove_file(&compressed_path)
            .await
            .map_err(|e| DataError::IOError(e.to_string()))?;

        file.rename(restored_name);
        file.size = unarchived_size;
        let file_type = FileType::from_filename(&file.name);
        file.update_type(file_type);

        let event: FileUpdatedEvent = FileUpdatedEvent::new(
            file.id.clone(),
            file.parent_folder_id.clone(),
            file.is_deleted.clone(),
            file.ttl.clone(),
            file.size.clone(),
            file.upload_status.clone(),
            file.is_archived(&self.storage_path),
        );

        let owner_id = file.owner_id;

        self.file_repo.update(file).await?;
        self.resync_storage(owner_id).await?;

        if let Err(e) = self.publisher.publish(&event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        Ok(())
    }

    async fn remove_deleted_file(&self, file_id: Uuid) -> Result<(), DataError> {
        let file = self
            .file_repo
            .get_deleted_by_id(file_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("File".to_string()))?;

        let clean_up_event: TrashCleanUpTriggeredEvent = TrashCleanUpTriggeredEvent::new(
            file.owner_id.clone(),
            DeletionType::File,
            Some(file.id)
        );

        if let Err(e) = self.publisher.publish(&clean_up_event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        Ok(())
    }
}

impl FileWriteServiceImpl {
    /// Rebuilds the owner's usage from their file rows, then announces the result.
    ///
    /// `taken_storage` used to be a running total that each operation nudged by a delta it
    /// worked out itself. Every one of those deltas was a chance to miss a sign, skip a
    /// direction, or lose the write to a concurrent update — and since nothing ever
    /// re-derived the figure, each mistake stayed for good and they accumulated, which is
    /// how a profile with no files left at all ended up reporting negative usage.
    ///
    /// Deriving it instead makes the next operation repair whatever the last one got
    /// wrong. Call this *after* the file row is written, or it recomputes the old value.
    async fn resync_storage(&self, owner_id: Uuid) -> Result<(), DataError> {
        self.sp_repo.recompute_taken_storage(owner_id).await?;

        let sp = self
            .sp_repo
            .get_by_id(owner_id)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("User".to_string()))?;

        let is_blocked = self.nas_identity_repo.is_user_blocked(owner_id).await?;

        let sp_event = UserUpdatedEvent::new(
            sp.user_id.clone(),
            None,
            None,
            Some(sp.allowed_storage.clone()),
            Some(sp.taken_storage.clone()),
            is_blocked,
        );

        if let Err(e) = self.publisher.publish(&sp_event).await {
            eprintln!("Failed to publish event: {:?}", e);
        }

        Ok(())
    }
}
