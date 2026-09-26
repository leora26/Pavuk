use crate::helpers::data_error::DataError;
use async_trait::async_trait;
use homelab_core::nas_domain::file::{File, FileType};
use homelab_core::nas_domain::folder::Folder;
use sqlx::PgPool;
use uuid::Uuid;

#[async_trait]
pub trait FolderRepository: Send + Sync {
    async fn get_root(&self, user_id: Uuid) -> Result<Option<Folder>, DataError>;
    async fn get_by_id(&self, folder_id: Uuid) -> Result<Option<Folder>, DataError>;
    async fn get_children_by_id(&self, folder_id: Uuid) -> Result<Vec<Folder>, DataError>;
    async fn search_by_name(&self, search_query: String, user_id: Uuid) -> Result<Vec<Folder>, DataError>;
    async fn filter_files_in_folder(
        &self,
        file_types: &[FileType],
        folder_id: Uuid,
    ) -> Result<Vec<File>, DataError>;
    async fn get_by_folder_id(&self, folder_id: Uuid) -> Result<Vec<File>, DataError>;
    async fn create(&self, folder: Folder) -> Result<Folder, DataError>;
    async fn update_folder(&self, folder: Folder) -> Result<Folder, DataError>;
    async fn delete_all(&self, folder_ids: &[Uuid]) -> Result<(), DataError>;
    async fn delete_by_id(&self, folder_id: Uuid) -> Result<(), DataError>;
    async fn mark_folder_deleted(&self, folder_id: Uuid) -> Result<(), DataError>;
    async fn mark_folders_deleted(&self, folder_ids: &[Uuid]) -> Result<(), DataError>;
    async fn get_trash_file_for_folder(&self, folder_id: Uuid) -> Result<Vec<File>, DataError>;
    async fn get_trash_subfolder_for_folder(&self, folder_id: Uuid) -> Result<Vec<Folder>, DataError>;
    async fn get_deleted_folders(&self, user_id: Uuid) -> Result<Vec<Folder>, DataError>;
    async fn hard_delete_folder_tree(&self, folder_id: Uuid) -> Result<(), DataError>;
    async fn hard_delete_all_trashed_folders(&self, user_id: Uuid) -> Result<(), DataError>;
    async fn hard_delete_global_trashed_folders(&self) -> Result<(), DataError>;
    async fn restore_deleted_folder(&self, folder_id: Uuid) -> Result<(), DataError>;
}

pub struct FolderRepositoryImpl {
    pool: PgPool,
}

impl FolderRepositoryImpl {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl FolderRepository for FolderRepositoryImpl {
    async fn get_root(&self, user_id: Uuid) -> Result<Option<Folder>, DataError> {
        let folder = sqlx::query_as!(
            Folder,
            r#"
        SELECT f.*
        FROM folders f
        WHERE parent_folder_id IS NULL AND owner_id = $1"#,
            user_id
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(folder)
    }

    async fn get_by_id(&self, folder_id: Uuid) -> Result<Option<Folder>, DataError> {
        let folder = sqlx::query_as!(
            Folder,
            r#"
        SELECT f.*
        FROM folders f
        WHERE id = $1"#,
            folder_id
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(folder)
    }

    async fn get_children_by_id(&self, folder_id: Uuid) -> Result<Vec<Folder>, DataError> {
        let folders = sqlx::query_as!(
            Folder,
            r#"
        SELECT f.*
        FROM folders f
        WHERE parent_folder_id = $1 AND is_deleted = false"#,
            folder_id
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(folders)
    }

    async fn search_by_name(&self, search_query: String, user_id: Uuid) -> Result<Vec<Folder>, DataError> {
        let f: Vec<Folder> = sqlx::query_as!(
            Folder,
            r#"
            SELECT f.*
            FROM folders f
            WHERE LOWER(name) LIKE LOWER($1) AND is_deleted = false AND owner_id = $2
            "#,
            search_query,
            user_id
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(f)
    }

    async fn filter_files_in_folder(
        &self,
        file_types: &[FileType],
        folder_id: Uuid,
    ) -> Result<Vec<File>, DataError> {
        let files = sqlx::query_as!(
            File,
            r#"
            SELECT
                id, name, owner_id,
                parent_folder_id,
                file_type as "file_type: _",
                is_deleted, ttl, size,
                upload_status as "upload_status: _",
                created_at, updated_at, hash, deleted_at
            FROM files
            WHERE parent_folder_id = $1 AND file_type = ANY($2::file_type[]) AND is_deleted = FALSE
            "#,
            folder_id,
            file_types as &[FileType]
        )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DataError::DatabaseError(e))?;

        Ok(files)
    }

    async fn get_by_folder_id(&self, folder_id: Uuid) -> Result<Vec<File>, DataError> {
        let files = sqlx::query_as!(
        File,
        r#"
        SELECT
            id, name, owner_id, parent_folder_id,
            file_type as "file_type: _",
            is_deleted, ttl, size,
            upload_status as "upload_status: _",
            created_at, updated_at, hash, deleted_at
        FROM files
        WHERE parent_folder_id = $1 AND is_deleted = FALSE
        "#,
        folder_id
    )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DataError::DatabaseError(e))?;

        Ok(files)
    }

    async fn create(&self, folder: Folder) -> Result<Folder, DataError> {
        let folder = sqlx::query_as!(
            Folder,
            r#"
            INSERT INTO folders (id, name, owner_id, created_at, parent_folder_id, is_deleted)
            VALUES ($1, $2, $3, $4, $5, FALSE)
            RETURNING id, name, owner_id, created_at, parent_folder_id, is_deleted, deleted_at
            "#,
            folder.id,
            folder.name,
            folder.owner_id,
            folder.created_at,
            folder.parent_folder_id
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(folder)
    }

    async fn update_folder(&self, folder: Folder) -> Result<Folder, DataError> {
        let f = sqlx::query_as!(
            Folder,
            r#"
            UPDATE folders
            SET name = $1, owner_id = $2, parent_folder_id = $3, is_deleted = $4, deleted_at = $6
            WHERE id = $5
            RETURNING id, name, owner_id, created_at, parent_folder_id, is_deleted, deleted_at
            "#,
            folder.name,
            folder.owner_id,
            folder.parent_folder_id,
            folder.is_deleted,
            folder.id,
            folder.deleted_at,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(f)
    }

    async fn delete_all(&self, folder_ids: &[Uuid]) -> Result<(), DataError> {
        sqlx::query!(
            r#"
            DELETE FROM folders
            WHERE id = ANY($1)
            "#,
            folder_ids
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(())
    }

    async fn delete_by_id(&self, folder_id: Uuid) -> Result<(), DataError> {
        sqlx::query!(
            r#"
            DELETE FROM folders
            WHERE id = $1
            "#,
            folder_id
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(())
    }

    async fn mark_folder_deleted(&self, folder_id: Uuid) -> Result<(), DataError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DataError::DatabaseError(e))?;

        sqlx::query!(
            r#"
        WITH RECURSIVE folder_tree AS (
            SELECT id FROM folders WHERE id = $1
            
            UNION ALL
            
            SELECT f.id FROM folders f
            INNER JOIN folder_tree ft ON f.parent_folder_id = ft.id
        )
        UPDATE files 
        SET is_deleted = true, deleted_at = NOW() 
        WHERE parent_folder_id IN (SELECT id FROM folder_tree);
        "#,
            folder_id
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        sqlx::query!(
            r#"
            WITH RECURSIVE folder_tree as (
                SELECT id FROM folders WHERE id = $1
                UNION ALL
                SELECT f.id FROM folders f
                INNER JOIN folder_tree ft ON f.parent_folder_id = ft.id
            )
            UPDATE folders
            SET is_deleted = true, deleted_at = NOW()
            WHERE id IN (SELECT id FROM folder_tree);
            "#,
            folder_id
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        tx.commit().await.map_err(|e| DataError::DatabaseError(e))?;

        Ok(())
    }

    async fn mark_folders_deleted(&self, folder_ids: &[Uuid]) -> Result<(), DataError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DataError::DatabaseError(e))?;

        sqlx::query!(
            r#"
        WITH RECURSIVE folder_tree AS (
            SELECT id FROM folders WHERE id = ANY($1)
            UNION ALL
            
            SELECT f.id FROM folders f
            INNER JOIN folder_tree ft ON f.parent_folder_id = ft.id
        )
        UPDATE files 
        SET is_deleted = true, deleted_at = NOW() 
        WHERE parent_folder_id IN (SELECT id FROM folder_tree);
        "#,
            folder_ids
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        sqlx::query!(
            r#"
        WITH RECURSIVE folder_tree as (
            SELECT id FROM folders WHERE id = ANY($1)
            UNION ALL
            SELECT f.id FROM folders f
            INNER JOIN folder_tree ft ON f.parent_folder_id = ft.id
        )
        UPDATE folders
        SET is_deleted = true, deleted_at = NOW()
        WHERE id IN (SELECT id FROM folder_tree);
        "#,
            folder_ids
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        tx.commit().await.map_err(|e| DataError::DatabaseError(e))?;

        Ok(())
    }

    async fn get_trash_file_for_folder(&self, folder_id: Uuid) -> Result<Vec<File>, DataError> {
        let f: Vec<File> = sqlx::query_as!(
            File,
            r#"
        SELECT
            f.id, f.name, f.owner_id,
            f.file_type as "file_type: _",
            f.parent_folder_id, f.is_deleted, f.ttl, f.size,
            f.upload_status as "upload_status: _",
            f.created_at, f.updated_at, f.hash, f.deleted_at
        FROM files f
        LEFT JOIN folders p ON f.parent_folder_id = p.id
        WHERE f.is_deleted = TRUE
          AND f.parent_folder_id = $1
        "#,
            folder_id
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(f)
    }

    async fn get_trash_subfolder_for_folder(&self, folder_id: Uuid) -> Result<Vec<Folder>, DataError> {
        let deleted_subfolder = sqlx::query_as!(
            Folder,
            r#"
        SELECT f.* FROM folders f
        WHERE f.is_deleted = true AND f.parent_folder_id = $1
        "#,
            folder_id
        )
            .fetch_all(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(deleted_subfolder)
    }

    async fn get_deleted_folders(&self, user_id: Uuid) -> Result<Vec<Folder>, DataError> {
        let deleted_folders: Vec<Folder> = sqlx::query_as!(
            Folder,
            r#"
        SELECT f.* FROM folders f
        LEFT JOIN folders p ON f.parent_folder_id = p.id
        WHERE f.is_deleted = true 
          AND f.owner_id = $1
          AND (p.id IS NULL OR p.is_deleted = false)
        "#,
            user_id
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(deleted_folders)
    }

    async fn hard_delete_folder_tree(&self, folder_id: Uuid) -> Result<(), DataError> {
        sqlx::query!(
            r#"
        WITH RECURSIVE folder_tree AS (
            SELECT id FROM folders WHERE id = $1
            UNION ALL
            SELECT f.id FROM folders f
            INNER JOIN folder_tree ft ON f.parent_folder_id = ft.id
        )
        DELETE FROM folders WHERE id IN (SELECT id FROM folder_tree);
        "#,
            folder_id
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(())
    }

    async fn hard_delete_all_trashed_folders(&self, user_id: Uuid) -> Result<(), DataError> {
        sqlx::query!(
            "DELETE FROM folders WHERE owner_id = $1 AND is_deleted = true",
            user_id
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(())
    }
    async fn hard_delete_global_trashed_folders(&self) -> Result<(), DataError> {
        sqlx::query!("DELETE FROM folders WHERE is_deleted = true")
            .execute(&self.pool)
            .await
            .map_err(|e| DataError::DatabaseError(e))?;

        Ok(())
    }

    async fn restore_deleted_folder(&self, folder_id: Uuid) -> Result<(), DataError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DataError::DatabaseError(e))?;

        sqlx::query!(
            r#"
        WITH RECURSIVE folder_tree AS (
            SELECT id FROM folders WHERE id = $1

            UNION ALL

            SELECT f.id FROM folders f
            INNER JOIN folder_tree ft ON f.parent_folder_id = ft.id
        )
        UPDATE files
        SET is_deleted = false, deleted_at = NULL
        WHERE parent_folder_id IN (SELECT id FROM folder_tree);
        "#,
            folder_id
        )
            .execute(&mut *tx)
            .await
            .map_err(|e| DataError::DatabaseError(e))?;

        sqlx::query!(
            r#"
            WITH RECURSIVE folder_tree as (
                SELECT id FROM folders WHERE id = $1
                UNION ALL
                SELECT f.id FROM folders f
                INNER JOIN folder_tree ft ON f.parent_folder_id = ft.id
            )
            UPDATE folders
            SET is_deleted = false, deleted_at = NULL
            WHERE id IN (SELECT id FROM folder_tree);
            "#,
            folder_id
        )
            .execute(&mut *tx)
            .await
            .map_err(|e| DataError::DatabaseError(e))?;

        tx.commit().await.map_err(|e| DataError::DatabaseError(e))?;

        Ok(())
    }
}
