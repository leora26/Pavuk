use crate::db::user_repository::UserRepositoryImpl;
use crate::db::white_listed_user_repository::WhiteListedUserRepositoryImpl;
use crate::grpc::user_grpc_service::GrpcUserService;
use crate::service::user_service::{UserService, UserServiceImpl};
use crate::service::white_listed_user_service::{WhiteListedServiceImpl, WhiteListedUserService};
use actix_web::web::Data;
use dotenvy::dotenv;
use homelab_core::auth::auth::AuthState;
use homelab_core::auth::identity_cache::CacheIdentityResolver;
use homelab_proto::user::user_service_server::UserServiceServer;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::env;
use std::sync::Arc;
use tonic::service::Interceptor;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;
use homelab_core::auth::init_auth_interceptor::init_auth_interceptor;
use homelab_core::helpers::rabbitmq_publisher::RabbitMqPublisher;

pub mod data;
pub mod db;
pub mod grpc;
pub mod helpers;
pub mod service;

pub struct AppState {
    pub user_service: Arc<dyn UserService>,
    pub white_listed_user_service: Arc<dyn WhiteListedUserService>,
    pub cached_identity_resolver: Arc<CacheIdentityResolver<UserRepositoryImpl>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    dotenv().ok();

    let zitadel_domain = env::var("ZITADEL_DOMAIN").expect("ZITADEL_DOMAIN must be set");
    let target_client_id =
        env::var("ZITADEL_API_CLIENT_ID").expect("ZITADEL_API_CLIENT_ID must be set");

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to create database pool");

    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    println!("🚀 Server started successfully at http://127.0.0.1:8081");

    let rabbit_url = env::var("RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://admin:password@localhost:5672".to_string());

    let publisher = Arc::new(RabbitMqPublisher::new(&rabbit_url).await?);

    let app_state = init_app_state(pool, publisher).await;
    let auth_state = AuthState::init(&zitadel_domain, &target_client_id).await?;
    let auth_interceptor = init_auth_interceptor(auth_state);

    let grpc_addr: std::net::SocketAddr = "[::1]:50052".parse()?;

    println!("🚀 Starting gRPC Server only at {}", grpc_addr);
    let app_state_arc = app_state.clone().into_inner();

    let user_impl = GrpcUserService::new(app_state_arc.clone());

    Server::builder()
        .add_service(UserServiceServer::with_interceptor(
            user_impl,
            auth_interceptor,
        ))
        .serve(grpc_addr)
        .await?;

    Ok(())
}

async fn init_app_state(pool: Pool<Postgres>, publisher: Arc<RabbitMqPublisher>) -> Data<AppState> {
    let user_repo = Arc::new(UserRepositoryImpl::new(pool.clone()));
    let wlu_repo = Arc::new(WhiteListedUserRepositoryImpl::new(pool.clone()));

    let user_service = Arc::new(UserServiceImpl::new(user_repo.clone(), publisher.clone()));
    let white_listed_user_service = Arc::new(WhiteListedServiceImpl::new(
        wlu_repo.clone(),
        user_repo.clone(),
        publisher.clone(),
    ));

    let cached_identity_resolver = Arc::new(CacheIdentityResolver::new((*user_repo).clone()));

    Data::new(AppState {
        user_service,
        white_listed_user_service,
        cached_identity_resolver,
    })
}

