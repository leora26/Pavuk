use derive_new::new;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;
use crate::nas_domain::file::{FileType, UploadStatus};

pub trait DomainEvent {
    fn routing_key (&self) -> &'static str;
}

#[derive(Deserialize, Serialize, Debug, Clone, new)]
pub struct UserCreatedEvent {
    pub user_id: Uuid,
    pub email: String,
    pub full_name: String,
    pub created_at: OffsetDateTime,
    pub default_storage: i64,
    pub external_id: String,
}

impl DomainEvent for UserCreatedEvent {
    fn routing_key (&self) -> &'static str {
        "user.created"
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, new)]
pub struct UserUpdatedEvent {
    pub user_id: Uuid,
    pub email: Option<String>,
    pub full_name: Option<String>,
    pub allowed_storage: Option<i64>,
    pub taken_storage: Option<i64>,
    pub is_blocked: bool,
}

impl DomainEvent for UserUpdatedEvent {
    fn routing_key (&self) -> &'static str {
        "user.updated"
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, new)]
pub struct WhiteListedUserCreatedEvent {
    pub user_id: Uuid,
    pub email: String,
    pub full_name: String,
    pub created_at: OffsetDateTime,
    pub external_id: String,
}

impl DomainEvent for WhiteListedUserCreatedEvent {
    fn routing_key(&self) -> &'static str {
        "whitelisted.user.created"
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, new)]
pub struct WhiteListedUserUpdatedEvent {
    pub user_id: Uuid,
    pub email: String,
    pub full_name: String,
    pub is_confirmed: bool,
}

impl DomainEvent for WhiteListedUserUpdatedEvent {
    fn routing_key(&self) -> &'static str {
        "whitelisted.user.updated"
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, new)]
pub struct FileUploadedEvent {
    pub file_id: Uuid,
    pub folder_id: Uuid,
    pub file_type: FileType,
    pub is_deleted: bool,
    pub ttl: Option<OffsetDateTime>,
    pub size: i64,
    pub upload_status: UploadStatus,
    pub is_archived: bool,
    pub created_at: OffsetDateTime
}

impl DomainEvent for FileUploadedEvent {
    fn routing_key(&self) -> &'static str {
        "file.uploaded"
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, new)]
pub struct FileUpdatedEvent {
    pub file_id: Uuid,
    pub folder_id: Uuid,
    pub is_deleted: bool,
    pub ttl: Option<OffsetDateTime>,
    pub size: i64,
    pub upload_status: UploadStatus,
    pub is_archived: bool,
}

impl DomainEvent for FileUpdatedEvent {
    fn routing_key(&self) -> &'static str {
        "file.updated"
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum DeletionType {
    File,
    Folder,
    All
}

#[derive(Deserialize, Serialize, Debug, Clone, new)]
pub struct TrashCleanUpTriggeredEvent {
    pub user_id: Uuid,
    pub deletion_type: DeletionType,
    pub id: Option<Uuid>
}

impl DomainEvent for TrashCleanUpTriggeredEvent {
    fn routing_key(&self) -> &'static str {"cleanup.triggered"}
}