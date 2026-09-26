use crate::helpers::data_error::DataError;
use async_trait::async_trait;
use homelab_core::nas_domain::storage_profile::StorageProfile;
use sqlx::PgPool;
use uuid::Uuid;
use homelab_core::nas_domain::storage_stats::StorageStats;

#[async_trait]
pub trait StorageProfileRepository: Send + Sync {
    async fn create(&self, storage_profile: StorageProfile) -> Result<StorageProfile, DataError>;
    async fn get_by_id(&self, id: Uuid) -> Result<Option<StorageProfile>, DataError>;
    /// Updates only the quota, leaving `taken_storage` untouched. Block state lives in
    /// `nas_identities`.
    async fn update_quota(&self, user_id: Uuid, allowed_storage: i64) -> Result<(), DataError>;
    /// Rebuilds `taken_storage` from the files that actually exist, returning the new value.
    async fn recompute_taken_storage(&self, user_id: Uuid) -> Result<i64, DataError>;
    async fn get_stats(&self, id: Uuid) -> Result<StorageStats, DataError>;
}

#[derive(Clone)]
pub struct StorageProfileRepositoryImpl {
    pool: PgPool,
}

impl StorageProfileRepositoryImpl {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}



#[async_trait]
impl StorageProfileRepository for StorageProfileRepositoryImpl {
    async fn create(&self, storage_profile: StorageProfile) -> Result<StorageProfile, DataError> {
        let sp = sqlx::query_as!(
            StorageProfile,
            r#"
        INSERT INTO storage_profiles (user_id, allowed_storage, taken_storage)
        VALUES ($1, $2, $3)
        -- A no-op DO UPDATE rather than DO NOTHING: it makes the statement idempotent
        -- under redelivery while still emitting a RETURNING row for fetch_one. Writing
        -- allowed_storage here would clobber a quota an admin has since raised.
        ON CONFLICT (user_id) DO UPDATE SET taken_storage = storage_profiles.taken_storage
        RETURNING user_id, allowed_storage, taken_storage
        "#,
            storage_profile.user_id,
            storage_profile.allowed_storage,
            storage_profile.taken_storage,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(sp)
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<StorageProfile>, DataError> {
        let sp = sqlx::query_as!(
            StorageProfile,
            r#"
        SELECT user_id, allowed_storage, taken_storage
        FROM storage_profiles
        WHERE user_id = $1
        "#,
            id
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(sp)
    }

    async fn update_quota(&self, user_id: Uuid, allowed_storage: i64) -> Result<(), DataError> {
        // Deliberately narrow: a whole-row write here would push back a `taken_storage`
        // read before a concurrent upload, delete or archive committed, silently undoing
        // that change.
        sqlx::query!(
            r#"
            UPDATE storage_profiles
            SET allowed_storage = $1
            WHERE user_id = $2
            "#,
            allowed_storage,
            user_id
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(())
    }

    async fn recompute_taken_storage(&self, user_id: Uuid) -> Result<i64, DataError> {
        // `taken_storage` used to be a running total that every file operation nudged by a
        // delta. One missed or duplicated adjustment stuck permanently, and the errors
        // compounded — which is how a profile with no files at all ended up negative.
        //
        // Deriving it from the rows instead makes the counter self-healing: whatever the
        // deltas got wrong, the next operation corrects.
        //
        // Completed files only. A pending upload has a row sized from the client's
        // declared length but not all of its bytes on disk yet, and a failed one has no
        // bytes at all — counting either would charge the user for storage they don't use.
        // Trashed files are still counted: they occupy disk until the cleanup purges them.
        let taken = sqlx::query_scalar!(
            r#"
            UPDATE storage_profiles
            SET taken_storage = COALESCE((
                SELECT SUM(size)
                FROM files
                WHERE owner_id = $1 AND upload_status = 'completed'
            ), 0)
            WHERE user_id = $1
            RETURNING taken_storage
            "#,
            user_id
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(taken)
    }

    async fn get_stats(&self, id: Uuid) -> Result<StorageStats, DataError> {
        let stats = sqlx::query_as!(
            StorageStats,
            r#"
            SELECT
                (SELECT COUNT(*) FROM files
                  WHERE owner_id = $1 AND is_deleted = FALSE) AS "file_count!",

                (SELECT COUNT(*) FROM folders
                  WHERE owner_id = $1 AND is_deleted = FALSE
                    AND parent_folder_id IS NOT NULL) AS "folder_count!",

                -- Top-level only: an item whose parent is itself trashed is not its own
                -- row in Trash. Same filter the Trash listing queries use, so this count
                -- matches what the screen renders.
                (
                  (SELECT COUNT(*) FROM files f
                     LEFT JOIN folders p ON p.id = f.parent_folder_id
                    WHERE f.owner_id = $1 AND f.is_deleted = TRUE
                      AND (p.id IS NULL OR p.is_deleted = FALSE))
                  +
                  (SELECT COUNT(*) FROM folders f
                     LEFT JOIN folders p ON p.id = f.parent_folder_id
                    WHERE f.owner_id = $1 AND f.is_deleted = TRUE
                      AND (p.id IS NULL OR p.is_deleted = FALSE))
                ) AS "trashed_item_count!",

                (SELECT COALESCE(SUM(size), 0)::BIGINT FROM files
                  WHERE owner_id = $1 AND is_deleted = TRUE) AS "trashed_bytes!",

                (SELECT COUNT(*) FROM files f
                  WHERE f.owner_id = $1 AND f.is_deleted = FALSE
                    AND EXISTS (SELECT 1 FROM file_labels fl
                                 WHERE fl.file_id = f.id)) AS "labelled_file_count!",

                (SELECT COUNT(*) FROM files f
                  WHERE f.owner_id = $1 AND f.is_deleted = FALSE
                    AND NOT EXISTS (SELECT 1 FROM file_labels fl
                                     WHERE fl.file_id = f.id)) AS "unlabelled_file_count!",

                (SELECT COUNT(*) FROM global_files g
                   JOIN files f ON f.id = g.original_id
                  WHERE f.owner_id = $1 AND f.is_deleted = FALSE) AS "shared_file_count!",

                (SELECT COALESCE(SUM(f.size), 0)::BIGINT FROM global_files g
                   JOIN files f ON f.id = g.original_id
                  WHERE f.owner_id = $1 AND f.is_deleted = FALSE) AS "shared_bytes!"
            "#,
            id
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| DataError::DatabaseError(e))?;

        Ok(stats)
    }
}
