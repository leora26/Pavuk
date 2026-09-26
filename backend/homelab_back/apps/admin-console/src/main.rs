use std::env;
use std::error::Error;
use std::sync::Arc;
use actix_web::{web};
use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;
use homelab_core::helpers::rabbitmq_consumer::RabbitMqConsumer;
use homelab_proto::admin::console_file_service_server::ConsoleFileServiceServer;
use homelab_proto::admin::console_user_service_server::ConsoleUserServiceServer;
use homelab_proto::admin::console_wlu_service_server::ConsoleWluServiceServer;
use homelab_proto::admin::storage_admin_service_server::StorageAdminServiceServer;
use crate::db::file_repo::FileRepoImpl;
use crate::db::user_repo::UserRepoImpl;
use crate::db::wlu_repo::WluRepoImpl;
use crate::events::homelab_event_handler::HomelabEventHandler;
use crate::grpc::clients::user_grpc_client::{UserRemoteClient, UserRemoteClientImpl};
use crate::grpc::clients::wlu_grpc_client::{WluRemoteClient, WluRemoteClientImpl};
use crate::grpc::clients::volume_grpc_client::{VolumeRemoteClient, VolumeRemoteClientImpl};
use crate::grpc::file_grpc_service::GrpcFileService;
use crate::grpc::user_grpc_service::GrpcUserService;
use crate::grpc::wlu_grpc_service::GrpcWluService;
use crate::grpc::storage_admin_service::GrpcStorageAdminService;
use crate::service::file_service::{FileService, FileServiceImpl};
use crate::service::user_service::{UserService, UserServiceImpl};
use crate::service::wlu_service::{WluService, WluServiceImpl};

pub mod data;
pub mod db;
pub mod helpers;
pub mod events;
pub mod service;
pub mod grpc;

pub struct AppState {
    user_service: Arc<dyn UserService>,
    wlu_service: Arc<dyn WluService>,
    file_service: Arc<dyn FileService>,
    wlu_client: Arc<dyn WluRemoteClient>,
    user_client: Arc<dyn UserRemoteClient>,
    volume_client: Arc<dyn VolumeRemoteClient>,
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    dotenv().ok();

    let server_mode = env::var("SERVER_MODE")
        .unwrap_or_else(|_| "hybrid".to_string())
        .to_lowercase();

    let database_url = env::var("DATABASE_URL").expect("DATABSE_URL must be set in .env file");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to create database pool");

    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    let console_user_repo = Arc::new(UserRepoImpl::new(pool.clone()));
    let console_wlu_repo = Arc::new(WluRepoImpl::new(pool.clone()));
    let console_file_repo = Arc::new(FileRepoImpl::new(pool.clone()));

    let user_service = Arc::new(UserServiceImpl::new(console_user_repo.clone()));
    let wlu_service = Arc::new(WluServiceImpl::new(console_wlu_repo.clone()));
    let file_service = Arc::new(FileServiceImpl::new(console_file_repo.clone()));

    let rabbit_url = env::var("RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://admin:password@localhost:5672".to_string());

    let event_handler = Arc::new(HomelabEventHandler::new(
        user_service.clone(),
        wlu_service.clone(),
        file_service.clone(),
    ));

    tokio::spawn(async move {
        let patterns = vec![
            "user.created", "user.updated",
            "whitelisted.user.created", "whitelisted.user.updated",
            "file.uploaded", "file.updated",
        ];

        if let Err(e) = RabbitMqConsumer::start(&rabbit_url, event_handler, patterns, "admin_console").await {
            eprintln!("🔥 Consumer died: {}", e);
        }
    });

    let user_client_url = env::var("USER_SERVICE_URL")
        .unwrap_or_else(|_| "http://localhost:50052".to_string());

    println!("Connecting to User Service at {}...", user_client_url);
    let wlu_client_impl = WluRemoteClientImpl::connect(user_client_url.clone()).await?;
    let user_client_impl = UserRemoteClientImpl::connect(user_client_url.clone()).await?;

    let nas_service_url = env::var("NAS_SERVICE_URL")
        .unwrap_or_else(|_| "http://localhost:50051".to_string());
    println!("Connecting to NAS Service at {}...", nas_service_url);
    let volume_client_impl = VolumeRemoteClientImpl::connect(nas_service_url).await?;

    let app_state = web::Data::new(AppState {
        user_service,
        wlu_service,
        file_service,
        wlu_client: Arc::new(wlu_client_impl),
        user_client: Arc::new(user_client_impl),
        volume_client: Arc::new(volume_client_impl),
    });

    let grpc_addr: std::net::SocketAddr = "[::1]:50053".parse().unwrap();

    println!(
        "System starting in [{}] mode...",
        server_mode.to_uppercase()
    );

    match server_mode.as_str() {
        "grpc" => {
            println!("🚀 Starting gRPC Server only at {}", grpc_addr);
            let app_state_arc = app_state.clone().into_inner();

            let user_grpc_impl = GrpcUserService::new(app_state_arc.clone());
            let wlu_grpc_impl = GrpcWluService::new(app_state_arc.clone());
            let storage_admin_impl = GrpcStorageAdminService::new(app_state_arc.clone());
            let file_grpc_impl = GrpcFileService::new(app_state_arc.clone());

            Server::builder()
                .add_service(ConsoleUserServiceServer::new(user_grpc_impl))
                .add_service(ConsoleWluServiceServer::new(wlu_grpc_impl))
                .add_service(StorageAdminServiceServer::new(storage_admin_impl))
                .add_service(ConsoleFileServiceServer::new(file_grpc_impl))
                .serve(grpc_addr)
                .await?;
        }
        _ => panic!(
            "Invalid SERVER_MODE: {}. Use 'rest', 'grpc', or 'hybrid'",
            server_mode
        ),
    }

    Ok(())
}