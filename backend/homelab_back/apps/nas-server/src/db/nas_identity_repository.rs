use std::error::Error;
use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;
use homelab_core::auth::resolver::ExternalIdResolver;
use crate::helpers::data_error::DataError;

#[async_trait]
pub trait NasIdentityRepository: Send + Sync {
    async fn upsert(&self, user_id: Uuid, external_id: &str) -> Result<(), DataError>;
    async fn set_blocked(&self, user_id: Uuid, is_blocked: bool) -> Result<(), DataError>;
    async fn is_user_blocked(&self, user_id: Uuid) -> Result<bool, DataError>;
}

#[derive(Clone)]
pub struct NasIdentityRepositoryImpl {
    pool: PgPool,
}

impl NasIdentityRepositoryImpl {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}


#[async_trait]
impl NasIdentityRepository for NasIdentityRepositoryImpl {
    async fn upsert(&self, user_id: Uuid, external_id: &str) -> Result<(), DataError> {
        sqlx::query!(
            r#"
            INSERT INTO nas_identities (user_id, external_id, is_blocked)
            VALUES ($1, $2, FALSE)
            ON CONFLICT (user_id) DO UPDATE SET external_id = EXCLUDED.external_id
            "#,
            user_id,
            external_id
        )
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn set_blocked(&self, user_id: Uuid, is_blocked: bool) -> Result<(), DataError> {
        let result = sqlx::query!(
            r#"
            UPDATE nas_identities SET is_blocked = $2 WHERE user_id = $1
            "#,
            user_id,
            is_blocked
        )
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            eprintln!("No nas identity for {user_id}; block state not projected")
        }

        Ok(())
    }

    async fn is_user_blocked(&self, user_id: Uuid) -> Result<bool, DataError> {
        let record = sqlx::query!(
            r#"
            SELECT is_blocked FROM nas_identities WHERE user_id = $1
            "#,
            user_id
        )
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| DataError::EntityNotFoundException("Nas identity".to_string()))?;

        Ok(record.is_blocked)
    }
}

#[async_trait]
impl ExternalIdResolver for NasIdentityRepositoryImpl {
    async fn resolve_external_id(&self, external_id: &str) -> Result<String, Box<dyn Error>> {
        let record = sqlx::query!(
            "SELECT user_id FROM nas_identities WHERE external_id = $1",
            external_id
        )
            .fetch_one(&self.pool)
            .await?;

        Ok(record.user_id.to_string())
    }

    async fn is_blocked(&self, internal_id: Uuid) -> Result<bool, Box<dyn Error>> {
        let record = sqlx::query!(
            "SELECT is_blocked FROM nas_identities WHERE user_id = $1",
            internal_id
        )
            .fetch_one(&self.pool)
            .await?;

        Ok(record.is_blocked)
    }
}