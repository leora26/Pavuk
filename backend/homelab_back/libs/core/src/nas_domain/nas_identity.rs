use derive_new::new;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(new, Clone, Debug, Serialize, Deserialize, FromRow)]
pub struct NasIdentity {
    pub user_id: Uuid,
    pub external_id: Uuid,
    pub is_blocked: bool,
}