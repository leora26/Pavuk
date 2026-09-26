use homelab_core::nas_domain::file::{File, FileType as DomainFileType, UploadStatus as DomainUploadStatus};
use homelab_core::nas_domain::file_label::FileLabel;
use homelab_core::nas_domain::folder::Folder;
use crate::db::global_file_repository::GlobalFileWithMeta;
use crate::db::label_repository::LabelWithCount;
use homelab_core::nas_domain::label::Label;
use homelab_proto::common::EntityId;
use homelab_proto::nas::{FileLabelResponse, FileResponse, FileType as ProtoFileType, FolderResponse, GlobalFileResponse, LabelResponse, LabelWithCountResponse, StorageProfileResponse, UploadStatus as ProtoUploadStatus};
use tonic::Status;
use uuid::Uuid;
use homelab_core::nas_domain::storage_profile::StorageProfile;
use homelab_core::nas_domain::storage_stats::StorageStats;
use homelab_proto::nas::StorageStatsResponse;
use homelab_core::nas_domain::volume::VolumeStatus;
use homelab_proto::nas::VolumeStatusResponse;

pub fn map_volume_to_proto (s: VolumeStatus) -> VolumeStatusResponse {
    VolumeStatusResponse {
        dataset: s.dataset,
        mountpoint: s.mountpoint.to_string_lossy().into_owned(),
        used: s.used, available: s.available,
        quota: s.quota, reservation: s.reservation,
        referenced: s.referenced, used_by_snapshots: s.used_by_snapshots,
        pool_free: s.pool_free,
    }
}

/// Shared timestamp conversion — the domain uses `time::OffsetDateTime`, the wire uses
/// `prost_types::Timestamp`.
pub fn map_time_to_proto(t: sqlx::types::time::OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: t.unix_timestamp(),
        nanos: t.nanosecond() as i32,
    }
}

pub fn map_storage_stats_to_proto(s: StorageStats) -> StorageStatsResponse {
    StorageStatsResponse {
        file_count: s.file_count,
        folder_count: s.folder_count,
        trashed_item_count: s.trashed_item_count,
        trashed_bytes: s.trashed_bytes,
        labelled_file_count: s.labelled_file_count,
        unlabelled_file_count: s.unlabelled_file_count,
        shared_file_count: s.shared_file_count,
        shared_bytes: s.shared_bytes,
    }
}

/// `is_blocked` is passed in rather than read off the profile: it lives in
/// `nas_identities` now, and `storage_profiles` is quota-only.
pub fn map_storage_profile_to_proto(
    sp: StorageProfile,
    is_blocked: bool,
) -> StorageProfileResponse {
    StorageProfileResponse {
        user_id: Option::from(map_id_to_proto(sp.user_id)),
        allowed_storage: sp.allowed_storage,
        taken_storage: sp.taken_storage,
        is_blocked,
    }
}

pub fn map_file_to_proto(f: File, labels: Vec<Label>) -> FileResponse {
    FileResponse {
        id: Option::from(map_id_to_proto(f.id)),
        name: f.name,
        owner_id: Option::from(map_id_to_proto(f.owner_id)),
        parent_folder_id: Option::from(map_id_to_proto(f.parent_folder_id)),
        file_type: match f.file_type {
            DomainFileType::Image => ProtoFileType::Image,
            DomainFileType::Text => ProtoFileType::Text,
            DomainFileType::Video => ProtoFileType::Video,
            DomainFileType::Audio => ProtoFileType::Audio,
            DomainFileType::Pdf => ProtoFileType::Pdf,
            DomainFileType::Zip => ProtoFileType::Zip,
            DomainFileType::Unknown => ProtoFileType::Unknown,
        } as i32,
        is_deleted: f.is_deleted,
        ttl: f.ttl.map(|t| prost_types::Timestamp {
            seconds: t.unix_timestamp(),
            nanos: t.nanosecond() as i32,
        }),
        size: f.size,
        upload_status: match f.upload_status {
            DomainUploadStatus::Failed => ProtoUploadStatus::Failed,
            DomainUploadStatus::Completed => ProtoUploadStatus::Completed,
            DomainUploadStatus::Pending => ProtoUploadStatus::Pending,
        } as i32,
        created_at: Some(prost_types::Timestamp {
            seconds: f.created_at.unix_timestamp(),
            nanos: f.created_at.nanosecond() as i32,
        }),
        updated_at: Some(prost_types::Timestamp {
            seconds: f.updated_at.unix_timestamp(),
            nanos: f.updated_at.nanosecond() as i32,
        }),
        hash: f.hash,
        labels: labels.into_iter().map(map_label_to_proto).collect(),
        deleted_at: f.deleted_at.map(map_time_to_proto),
    }
}

pub fn map_global_file_to_proto(g: GlobalFileWithMeta) -> GlobalFileResponse {
    let original_id = g.file.id;
    GlobalFileResponse {
        id: Option::from(map_id_to_proto(g.id)),
        original_id: Option::from(map_id_to_proto(original_id)),
        file: Some(map_file_to_proto(g.file, Vec::new())),
        owner_name: g.owner_name,
        shared_at: Some(prost_types::Timestamp {
            seconds: g.shared_at.unix_timestamp(),
            nanos: g.shared_at.nanosecond() as i32,
        }),
    }
}

pub fn map_folder_to_proto(f: Folder) -> FolderResponse {
    FolderResponse {
        id: Option::from(map_id_to_proto(f.id)),
        parent_folder_id: f.parent_folder_id.map(map_id_to_proto),
        name: f.name,
        owner_id: Option::from(map_id_to_proto(f.owner_id)),
        created_at: Some(prost_types::Timestamp {
            seconds: f.created_at.unix_timestamp(),
            nanos: f.created_at.nanosecond() as i32,
        }),
        deleted_at: f.deleted_at.map(map_time_to_proto),
    }
}

pub fn map_label_to_proto(l: Label) -> LabelResponse {
    LabelResponse {
        id: Option::from(map_id_to_proto(l.id)),
        name: l.name,
        color: l.color,
        created_at: Some(prost_types::Timestamp {
            seconds: l.created_at.unix_timestamp(),
            nanos: l.created_at.nanosecond() as i32,
        }),
    }
}

pub fn map_label_with_count_to_proto(lwc: LabelWithCount) -> LabelWithCountResponse {
    LabelWithCountResponse {
        label: Some(map_label_to_proto(lwc.label)),
        file_count: lwc.file_count,
    }
}

pub fn map_file_label_to_proto(fl: FileLabel) -> FileLabelResponse {
    FileLabelResponse {
        file_id: Option::from(map_id_to_proto(fl.file_id)),
        label_id: Option::from(map_id_to_proto(fl.label_id)),
    }
}

pub fn map_id_to_proto(id: Uuid) -> EntityId {
    EntityId {
        value: id.to_string(),
    }
}

pub fn map_entity_id(id: Option<EntityId>) -> Result<Uuid, Status> {
    let entity_id = id.ok_or_else(|| Status::invalid_argument("Missing ID"))?;

    Uuid::parse_str(&entity_id.value).map_err(|_| Status::invalid_argument("Invalid UUID format"))
}
