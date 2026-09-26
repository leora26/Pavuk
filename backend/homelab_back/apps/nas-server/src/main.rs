pub mod data;
pub mod db;
pub mod events;
pub mod grpc;
pub mod handler;
pub mod helpers;
pub mod jobs;
pub mod service;

use crate::db::file_label_repository::FileLabelRepositoryImpl;
use crate::db::file_repository::{FileRepository, FileRepositoryImpl};
use crate::db::folder_repository::{FolderRepository, FolderRepositoryImpl};
use crate::db::global_file_repository::GlobalFileRepositoryImpl;
use crate::db::label_repository::LabelRepositoryImpl;
use crate::db::nas_identity_repository::NasIdentityRepositoryImpl;
use crate::db::shared_file_repository::SharedFileRepositoryImpl;
use crate::db::storage_profile_repository::StorageProfileRepositoryImpl;
use crate::events::nas_event_handler::NasEventHandler;
use crate::grpc::file_grpc_service::GrpcFileService;
use crate::grpc::file_label_grpc_service::GrpcFileLabelService;
use crate::grpc::folder_grpc_service::GrpcFolderService;
use crate::grpc::global_file_grpc_service::GrpcGlobalFileService;
use crate::grpc::grpc_label_service::GrpcLabelService;
use crate::grpc::storage_profile_grpc_service::GrpcStorageProfileService;
use crate::jobs::delete_cron_job::init_delete_job;
use crate::service::contract::file_label_service::FileLabelService;
use crate::service::contract::file_read_service::FileReadService;
use crate::service::contract::file_write_service::FileWriteService;
use crate::service::contract::folder_read_service::FolderReadService;
use crate::service::contract::folder_write_service::FolderWriteService;
use crate::service::contract::global_file_service::GlobalFileService;
use crate::service::contract::label_service::LabelService;
use crate::service::contract::shared_file_service::SharedFileService;
use crate::service::contract::nas_identity_service::NasIdentityService;
use crate::service::contract::sp_service::StorageProfileService;
use crate::service::contract::volume_service::VolumeService;
use crate::service::r#impl::clean_up_service_impl::CleanUpServiceImpl;
use crate::service::r#impl::file_label_service_impl::FileLabelServiceImpl;
use crate::service::r#impl::file_read_service_impl::FileReadServiceImpl;
use crate::service::r#impl::file_write_service_impl::FileWriteServiceImpl;
use crate::service::r#impl::folder_read_service_impl::FolderReadServiceImpl;
use crate::service::r#impl::folder_write_service_impl::FolderWriteServiceImpl;
use crate::service::r#impl::global_file_service_impl::GlobalFileServiceImpl;
use crate::service::r#impl::label_service_impl::LabelServiceImpl;
use crate::service::r#impl::nas_identity_service_impl::NasIdentityServiceImpl;
use crate::service::r#impl::shared_file_service_impl::SharedFileServiceImpl;
use crate::service::r#impl::sp_service_impl::StorageProfileServiceImpl;
use crate::service::r#impl::volume_service_impl::VolumeServiceImpl;
use actix_web::web::Data;
use actix_web::{web, App, HttpServer};
use dotenvy::dotenv;
use homelab_core::auth::auth::AuthState;
use homelab_core::auth::identity_cache::CacheIdentityResolver;
use homelab_core::helpers::rabbitmq_consumer::RabbitMqConsumer;
use homelab_proto::nas::file_label_service_server::FileLabelServiceServer;
use homelab_proto::nas::file_service_server::FileServiceServer;
use homelab_proto::nas::folder_service_server::FolderServiceServer;
use homelab_proto::nas::global_file_service_server::GlobalFileServiceServer;
use homelab_proto::nas::label_service_server::LabelServiceServer;
use homelab_proto::nas::storage_profile_service_server::StorageProfileServiceServer;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use tonic::service::Interceptor;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;
use homelab_core::auth::init_auth_interceptor::init_auth_interceptor;
use homelab_core::helpers::rabbitmq_publisher::RabbitMqPublisher;
use homelab_proto::nas::volume_service_server::VolumeServiceServer;
use crate::grpc::volume_grpc_service::GrpcVolumeService;

pub struct AppState {
    pub file_write_service: Arc<dyn FileWriteService>,
    pub folder_write_service: Arc<dyn FolderWriteService>,
    pub folder_repo: Arc<dyn FolderRepository>,
    pub shared_file_service: Arc<dyn SharedFileService>,
    pub file_repo: Arc<dyn FileRepository>,
    pub global_file_service: Arc<dyn GlobalFileService>,
    pub label_service: Arc<dyn LabelService>,
    pub file_label_service: Arc<dyn FileLabelService>,
    pub storage_profile_service: Arc<dyn StorageProfileService>,
    pub nas_identity_service: Arc<dyn NasIdentityService>,
    pub volume_service: Arc<dyn VolumeService>,
    pub folder_read_service: Arc<dyn FolderReadService>,
    pub file_read_service: Arc<dyn FileReadService>,
    pub cached_identity_resolver: Arc<CacheIdentityResolver<NasIdentityRepositoryImpl>>,
    pub auth_state: AuthState,
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    dotenv().ok();

    let zitadel_domain =
        env::var("ZITADEL_DOMAIN").expect("ZITADEL_DOMAIN must be set in .env file");

    let target_client_id =
        env::var("ZITADEL_API_CLIENT_ID").expect("ZITADEL_API_CLIENT_ID must be set in .env file");

    let auth_state = AuthState::init(&zitadel_domain, &target_client_id).await?;

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");

    let root_folder_path =
        env::var("ROOT_FOLDER_PATH").expect("ROOT_FOLDER_PATH must be set in .env file");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to create database pool");

    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    println!("🚀 Server started successfully at http://127.0.0.1:8080");

    let zfs_pool = env::var("ZFS_POOL").unwrap_or_default();
    let zfs_dataset = env::var("ZFS_DATASET").unwrap_or_default();
    let zfs_headroom: i64 = env::var("ZFS_MIN_HEADROOM_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024 * 1024 * 1024);
    let volume_service: Arc<dyn VolumeService> =
        Arc::new(VolumeServiceImpl::new(zfs_pool, zfs_dataset.clone(), zfs_headroom));

    let root_path: PathBuf = if !zfs_dataset.is_empty() {
        volume_service
            .ensure_mounted()
            .await
            .expect("ZFS dataset must be mounted before nas-server can start")
    } else {
        let p = PathBuf::from(&root_folder_path);
        if !p.exists() {
            std::fs::create_dir_all(&p).expect("Failed to create root directory");
            println!("Root folder was created.");
        }
        p
    };
    println!("📁 Storage root: {}", root_path.display());
    let rabbit_url = env::var("RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://admin:password@localhost:5672".to_string());

    let publisher = Arc::new(RabbitMqPublisher::new(&rabbit_url).await?);

    let app_state = init_app_state(pool, publisher, root_path.clone(), auth_state.clone(), volume_service).await;

    // ZFS volume smoke-check (STOR-1) — non-fatal so the server still boots without ZFS
    match app_state.volume_service.status().await {
        Ok(s) => println!(
            "✅ ZFS volume '{}' ready — used={} available={} quota={:?} reservation={:?}",
            s.dataset, s.used, s.available, s.quota, s.reservation
        ),
        Err(e) => eprintln!("⚠️  ZFS volume check failed (STOR-1): {e}"),
    }

    let rest_addr = ("0.0.0.0", 8080);
    let grpc_addr: std::net::SocketAddr = "[::1]:50051".parse()?;

    let clean_up_service = Arc::new(CleanUpServiceImpl::new(
        app_state.folder_repo.clone(),
        app_state.file_repo.clone(),
        app_state.storage_profile_service.clone(),
        root_path.to_path_buf(),
    ));

    let _cleanup_scheduler = init_delete_job(clean_up_service.clone()).await;

    let event_handler = Arc::new(NasEventHandler::new(
        app_state.storage_profile_service.clone(),
        app_state.nas_identity_service.clone(),
        clean_up_service.clone(),
    ));

    tokio::spawn(async move {
        let patterns = vec!["user.#", "file.#", "cleanup.#"];

        if let Err(e) = RabbitMqConsumer::start(&rabbit_url, event_handler, patterns).await {
            eprintln!("🔥 Consumer died: {}", e);
        }
    });

    println!("🚀 Starting gRPC Server only at {}", grpc_addr);
    let app_state_arc = app_state.clone().into_inner();

    let file_impl = GrpcFileService::new(app_state_arc.clone());
    let folder_impl = GrpcFolderService::new(app_state_arc.clone());
    let file_label_impl = GrpcFileLabelService::new(app_state_arc.clone());
    let global_file_impl = GrpcGlobalFileService::new(app_state_arc.clone());
    let label_impl = GrpcLabelService::new(app_state_arc.clone());
    let storage_profile_impl = GrpcStorageProfileService::new(app_state_arc.clone());
    let volume_impl = GrpcVolumeService::new(app_state_arc.clone());

    let auth_interceptor = init_auth_interceptor(auth_state);

    let grpc_server = Server::builder()
        .add_service(FileServiceServer::with_interceptor(
            file_impl,
            auth_interceptor.clone(),
        ))
        .add_service(FolderServiceServer::with_interceptor(
            folder_impl,
            auth_interceptor.clone(),
        ))
        .add_service(FileLabelServiceServer::with_interceptor(
            file_label_impl,
            auth_interceptor.clone(),
        ))
        .add_service(GlobalFileServiceServer::with_interceptor(
            global_file_impl,
            auth_interceptor.clone(),
        ))
        .add_service(LabelServiceServer::with_interceptor(
            label_impl,
            auth_interceptor.clone(),
        ))
        .add_service(StorageProfileServiceServer::with_interceptor(
            storage_profile_impl,
            auth_interceptor.clone(),
        ))
        .add_service(VolumeServiceServer::new(volume_impl)) // Add security later
        .serve(grpc_addr);

    tokio::spawn(async move {
        if let Err(e) = grpc_server.await {
            eprintln!("gRPC Server crashed: {}", e);
        }
    });

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .configure(handler_config)
    })
        .bind(rest_addr)?
        .run()
        .await?;

    Ok(())
}

fn handler_config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/api").configure(handler::file_handler::config));
}


async fn init_app_state(
    pool: Pool<Postgres>,
    publisher: Arc<RabbitMqPublisher>,
    root_path: PathBuf,
    auth_state: AuthState,
    volume_service: Arc<dyn VolumeService>
) -> Data<AppState> {
    let file_repo = Arc::new(FileRepositoryImpl::new(pool.clone()));
    let storage_profile_repo = Arc::new(StorageProfileRepositoryImpl::new(pool.clone()));
    let nas_identity_repo = Arc::new(NasIdentityRepositoryImpl::new(pool.clone()));
    let folder_repo = Arc::new(FolderRepositoryImpl::new(pool.clone()));
    let share_file_repo = Arc::new(SharedFileRepositoryImpl::new(pool.clone()));
    let global_file_repo = Arc::new(GlobalFileRepositoryImpl::new(pool.clone()));
    let label_repo = Arc::new(LabelRepositoryImpl::new(pool.clone()));
    let file_label_repo = Arc::new(FileLabelRepositoryImpl::new(pool.clone()));

    let folder_write_service = Arc::new(FolderWriteServiceImpl::new(
        folder_repo.clone(),
        publisher.clone(),
    ));
    let folder_read_service = Arc::new(FolderReadServiceImpl::new(
        folder_repo.clone(),
    ));
    let file_write_service = Arc::new(FileWriteServiceImpl::new(
        file_repo.clone(),
        folder_repo.clone(),
        storage_profile_repo.clone(),
        nas_identity_repo.clone(),
        root_path.to_path_buf(),
        publisher.clone(),
    ));
    let file_read_service = Arc::new(FileReadServiceImpl::new(
        file_repo.clone(),
        root_path.to_path_buf(),
    ));
    let shared_file_service = Arc::new(SharedFileServiceImpl::new(
        share_file_repo.clone(),
        storage_profile_repo.clone(),
        file_repo.clone(),
    ));
    let global_file_service = Arc::new(GlobalFileServiceImpl::new(global_file_repo.clone()));
    let label_service = Arc::new(LabelServiceImpl::new(
        label_repo.clone(),
    ));
    let file_label_service = Arc::new(FileLabelServiceImpl::new(
        label_repo.clone(),
        file_repo.clone(),
        file_label_repo.clone(),
    ));
    let storage_profile_service = Arc::new(StorageProfileServiceImpl::new(
        storage_profile_repo.clone(),
        nas_identity_repo.clone(),
        publisher.clone(),
    ));
    let nas_identity_service = Arc::new(NasIdentityServiceImpl::new(nas_identity_repo.clone()));

    let cached_identity_resolver =
        Arc::new(CacheIdentityResolver::new((*nas_identity_repo).clone()));

    Data::new(AppState {
        file_write_service,
        folder_write_service,
        folder_repo,
        shared_file_service,
        file_repo: file_repo.clone(),
        global_file_service,
        label_service,
        file_label_service,
        storage_profile_service,
        nas_identity_service,
        volume_service,
        file_read_service,
        folder_read_service,
        cached_identity_resolver,
        auth_state,
    })
}
