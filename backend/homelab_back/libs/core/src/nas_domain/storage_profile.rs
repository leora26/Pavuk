use derive_new::new;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Deserialize, Serialize, FromRow, new, Clone)]
pub struct StorageProfile {
    pub user_id: Uuid,
    pub allowed_storage: i64,
    pub taken_storage: i64,
}

impl StorageProfile {
    pub fn validate_storage_size(&self, file_size: i64) -> bool {
        let future_size = self.taken_storage + file_size;

        if future_size > self.allowed_storage {
            false
        } else {
            true
        }
    }

    pub fn reduce_taken_storage_size(&mut self, size: i64) {
        self.taken_storage -= size;
    }

    pub fn increase_storage_size(&mut self, size: i64) {
        self.taken_storage += size;
    }
}
