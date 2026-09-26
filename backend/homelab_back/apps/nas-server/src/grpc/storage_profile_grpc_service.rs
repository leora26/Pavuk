use std::sync::Arc;
use async_trait::async_trait;
use derive_new::new;
use tonic::{Request, Response, Status};
use homelab_proto::nas::{GetStorageProfileByIdRequest, StorageProfileResponse, StorageStatsResponse};
use homelab_proto::nas::storage_profile_service_server::StorageProfileService;
use crate::AppState;
use crate::helpers::proto_mappers::{map_storage_profile_to_proto, map_storage_stats_to_proto};
use homelab_core::auth::extractor::RequestIdentityExt;

#[derive(new)]
pub struct GrpcStorageProfileService {
    pub app_state: Arc<AppState>
}


#[async_trait]
impl StorageProfileService for GrpcStorageProfileService {
    async fn get_by_id(&self, request: Request<GetStorageProfileByIdRequest>) -> Result<Response<StorageProfileResponse>, Status> {
        // Resolve the caller from their token rather than a client-supplied id.
        let id = request.get_internal_id(&self.app_state.cached_identity_resolver).await?;

        let sp = self.app_state
            .storage_profile_service
            .get_by_id(id)
            .await?
            .ok_or_else(|| Status::not_found(format!("No storage was found with given id: {}", id)))?;

        // Structurally always false here — `get_internal_id` rejects a blocked caller
        // before this runs. Read it anyway so the wire contract stays honest if that
        // guard is ever relaxed.
        let is_blocked = self.app_state.nas_identity_service.is_blocked(id).await?;

        Ok(Response::new(map_storage_profile_to_proto(sp, is_blocked)))
    }

    async fn get_storage_stats(&self, request: Request<()>) -> Result<Response<StorageStatsResponse>, Status> {
        // Identity from the token — every figure below is owner-scoped to the caller.
        let id = request.get_internal_id(&self.app_state.cached_identity_resolver).await?;

        let stats = self.app_state
            .storage_profile_service
            .get_storage_stats(id)
            .await?;

        Ok(Response::new(map_storage_stats_to_proto(stats)))
    }
}