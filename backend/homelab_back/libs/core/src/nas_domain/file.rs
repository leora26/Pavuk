use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::path::{Path, PathBuf};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "file_type", rename_all = "lowercase")]
pub enum FileType {
    Text,
    Image,
    Video,
    Unknown,
    Audio,
    Pdf,
    Zip
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "upload_status", rename_all = "lowercase")]
pub enum UploadStatus {
    Pending,
    Completed,
    Failed,
}

impl FileType {
    pub fn from_filename(name: &str) -> Self {
        let extension = Path::new(name)
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| s.to_lowercase());

        if let Some(ext) = extension {
            match ext.as_str() {
                "txt" | "md" | "json" | "xml" | "html" | "css" | "js" | "rs" => FileType::Text,
                "png" | "jpg" | "jpeg" | "gif" | "bmp" | "svg" | "webp" => FileType::Image,
                "mp4" | "mov" | "avi" | "mkv" | "webm" => FileType::Video,
                "zip" | "gz" | "tar" | "rar" | "7z" | "bz2" | "xz" | "iso" => FileType::Zip,
                "pdf" => FileType::Pdf,
                "mp3" | "acc" | "ogg" | "wav" | "flac" | "aiff"  => FileType::Audio,
                _ => FileType::Unknown,
            }
        } else {
            FileType::Unknown
        }
    }
}

#[derive(Debug, Serialize, Deserialize, FromRow, Clone)]
pub struct File {
    pub id: Uuid,
    pub name: String,
    pub owner_id: Uuid,
    pub parent_folder_id: Uuid,
    pub file_type: FileType,
    pub is_deleted: bool,
    pub ttl: Option<OffsetDateTime>,
    pub size: i64,
    pub upload_status: UploadStatus,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub hash: Option<String>,
    /// When the file was moved to the trash. `None` while it is live — `ttl` still
    /// carries the retention deadline, this is the moment the user deleted it.
    pub deleted_at: Option<OffsetDateTime>,
}

impl File {
    pub fn get_file_type(name: &str) -> FileType {
        FileType::from_filename(name)
    }

    pub fn new(
        id: Uuid,
        name: String,
        owner_id: Uuid,
        parent_folder_id: Uuid,
        is_deleted: bool,
        size: i64,
        created_at: OffsetDateTime,
        updated_at: OffsetDateTime
    ) -> Self {
        let file_type: FileType = File::get_file_type(&name);

        Self {
            id,
            name,
            owner_id,
            parent_folder_id,
            file_type,
            is_deleted,
            ttl: None,
            size,
            upload_status: UploadStatus::Pending,
            created_at,
            updated_at,
            hash: None,
            deleted_at: None,
        }
    }

    pub fn rename(&mut self, new_name: String) {
        self.file_type = File::get_file_type(&new_name);
        self.name = new_name;
        self.updated_at = OffsetDateTime::now_utc();
    }

    pub fn set_as_deleted(&mut self) {
        let now = OffsetDateTime::now_utc();

        self.is_deleted = true;
        self.deleted_at = Some(now);
        self.ttl = Some(now + Duration::days(30));
    }

    pub fn set_as_undeleted(&mut self) {
        self.is_deleted = false;
        self.deleted_at = None;
        self.ttl = None;
    }

    pub fn set_file_hash(&mut self, hash: String) {
        self.hash = Some(hash);
    }

    // TODO: For when i implement overwriting file
    pub fn update_size(&mut self, new_size: i64) {
        self.size = new_size;
        self.updated_at = OffsetDateTime::now_utc();
    }

    pub fn build_file_path(&self, storage_path: &Path) -> PathBuf {
        let id_string = self.id.to_string();

        let bucket1 = &id_string[0..2];
        let bucket2 = &id_string[2..4];

        let extension = Path::new(&self.name)
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("");

        let mut final_filename = id_string.clone();
        if !extension.is_empty() {
            final_filename = format!("{}.{}", final_filename, extension);
        }

        storage_path.join(bucket1).join(bucket2).join(final_filename)
    }

    pub fn validate_size(&self, size: i64) -> bool {
        if self.size == size {
            true
        } else {
            false
        }
    }

    pub fn update_status(&mut self, status: UploadStatus) {
        self.upload_status = status
    }
    
    pub fn update_type(&mut self, file_type: FileType) {
        self.file_type = file_type;
        self.updated_at = OffsetDateTime::now_utc();
    }

    pub fn update_parent_folder(&mut self, new_parent_folder: Uuid) {
        self.parent_folder_id = new_parent_folder;
        self.updated_at = OffsetDateTime::now_utc();
    }

    pub fn is_archived(&self, storage_path: &Path) -> bool {
        let path = self.build_file_path(storage_path);

        match path.extension().and_then(|s| s.to_str()) {
            Some(ext) => {
                matches!(
                    ext.to_lowercase().as_str(),
                    "zip" | "gz" | "tar" | "rar" | "7z" | "bz2" | "xz" | "iso"
                )
            }
            None => false,
        }
    }
}
