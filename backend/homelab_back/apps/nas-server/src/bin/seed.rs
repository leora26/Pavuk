use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::path::Path;
use std::{env, fs};
use uuid::Uuid;
use serde_json::json;
use reqwest::StatusCode;
use homelab_core::auth::diplomat::ZitadelDiplomat;

const ZITADEL_BASE: &str = "http://localhost:8085";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    // --- 1. SETUP DB & FOLDERS ---
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");
    let storage_dir_str = env::var("ROOT_FOLDER_PATH").expect("ROOT_FOLDER_PATH must be set in .env file");
    let storage_dir = Path::new(&storage_dir_str);

    if !storage_dir.exists() {
        fs::create_dir_all(storage_dir)?;
    }

    let pool = PgPoolOptions::new()
        .max_connections(5) // Protects your laptop's memory from connection spam
        .connect(&database_url)
        .await
        .expect("Failed to create database pool");

    // --- 2. AUTHENTICATE WITH ZITADEL ---
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let key_path = format!("{}/backend-key.json", manifest_dir);

    println!("Looking for key at: {}", key_path);

    let diplomat = ZitadelDiplomat::new(
        ZITADEL_BASE.to_string(),
        &key_path
    ).await.expect("Failed to initialize Zitadel Diplomat");

    let access_token = diplomat.get_token().await?;

    // We borrow this client multiple times below to utilize connection pooling
    let client = reqwest::Client::new();

    // --- 3. SEED TEST USERS ---
    // Two users so cross-user features (like global files) can be exercised: one user
    // publishes a file, the other logs in and confirms they can see and download it.
    seed_user(
        &client,
        &pool,
        &access_token,
        "testUser@homelab.local",
        "PavukTestUser@2026!",
        "Test",
        "User",
    ).await?;

    seed_user(
        &client,
        &pool,
        &access_token,
        "testUser2@homelab.local",
        "PavukTestUser2@2026!",
        "Test",
        "User Two",
    ).await?;

    println!("🚀 Seeding Complete!");

    Ok(())
}

/// Idempotently provisions a single test user: creates (or reuses) the Zitadel human
/// user, locks a permanent password, syncs the user into the local DB, and ensures a
/// root folder and storage profile exist. Safe to run repeatedly.
async fn seed_user(
    client: &reqwest::Client,
    pool: &PgPool,
    access_token: &str,
    email: &str,
    password: &str,
    first_name: &str,
    last_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let display_name = format!("{} {}", first_name, last_name);
    let full_name = display_name.clone();

    println!("\n👤 Seeding user {}...", email);

    // --- STEP ONE: CREATE USER ---
    let zitadel_payload = json!({
        "userName": email,
        "profile": {
            "firstName": first_name,
            "lastName": last_name,
            "displayName": display_name
        },
        "email": {
            "email": email,
            "isEmailVerified": true
        },
        // Must be a flat string to pass the gRPC type checker
        "initialPassword": password
    });

    let res = client.post(format!("{}/management/v1/users/human", ZITADEL_BASE))
        .header("Authorization", format!("Bearer {}", access_token))
        .json(&zitadel_payload)
        .send()
        .await?;

    let zitadel_id = if res.status() == StatusCode::CONFLICT {
        println!("⚠️ User already exists in Zitadel. Fetching existing ID...");

        let search_payload = json!({
            "queries": [{"userNameQuery": {"userName": email, "method": "TEXT_QUERY_METHOD_EQUALS"}}]
        });

        let search_res = client.post(format!("{}/management/v1/users/_search", ZITADEL_BASE))
            .header("Authorization", format!("Bearer {}", access_token))
            .json(&search_payload)
            .send()
            .await?;

        let search_data: serde_json::Value = search_res.json().await?;

        search_data["result"][0]["id"]
            .as_str()
            .ok_or("Could not parse existing userId from Zitadel search response")?
            .to_string()
    } else if res.status().is_success() {
        let response_data: serde_json::Value = res.json().await?;
        response_data["userId"]
            .as_str()
            .ok_or("Could not parse userId from Zitadel response")?
            .to_string()
    } else {
        let err = res.text().await?;
        return Err(format!("Failed to create user in Zitadel: {}", err).into());
    };

    println!("✅ Resolved Zitadel User ID: {}. Securing permanent password...", zitadel_id);

    // --- STEP TWO: LOCK THE PASSWORD (Bypass the setup screen) ---
    let password_payload = json!({
        "password": password,
        "noChangeRequired": true // This is the magic flag!
    });

    let pw_res = client.post(format!("{}/management/v1/users/{}/password", ZITADEL_BASE, zitadel_id))
        .header("Authorization", format!("Bearer {}", access_token))
        .json(&password_payload)
        .send()
        .await?;

    if !pw_res.status().is_success() {
        let err = pw_res.text().await?;
        return Err(format!("Failed to lock permanent password: {}", err).into());
    }

    // --- STEP THREE: CREATE OR UPDATE USER IN LOCAL DB ---
    let user_record = sqlx::query!(
        r#"
        INSERT INTO users (id, external_id, email, full_name, role)
        VALUES ($1, $2, $3, $4, 'admin')
        ON CONFLICT (email) DO UPDATE
        SET external_id = EXCLUDED.external_id
        RETURNING id
        "#,
        Uuid::new_v4(),
        zitadel_id,
        email,
        full_name
    )
        .fetch_one(pool)
        .await?;

    let local_user_id = user_record.id;
    println!("✅ Synced User into local DB with internal ID: {}", local_user_id);

    // --- STEP FOUR: CREATE FOLDER STRUCTURE & STORAGE PROFILE ---
    // Only create a Root folder if the user doesn't already have one, so re-runs don't
    // pile up duplicate roots.
    let existing_root = sqlx::query!(
        r#"SELECT id FROM folders WHERE owner_id = $1 AND parent_folder_id IS NULL LIMIT 1"#,
        local_user_id
    ).fetch_optional(pool).await?;

    if existing_root.is_none() {
        sqlx::query!(
            r#"
            INSERT INTO folders (id, name, owner_id, parent_folder_id)
            VALUES ($1, $2, $3, NULL)
            "#,
            Uuid::new_v4(), "Root", local_user_id
        ).execute(pool).await?;
    }

    let allowed_storage: i64 = 100 * 1024 * 1024;
    sqlx::query!(
        r#"
        INSERT INTO storage_profiles (user_id, allowed_storage, taken_storage)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id) DO NOTHING
        "#,
        local_user_id,
        allowed_storage,
        0i64
    ).execute(pool).await?;

    // The identity projection nas authenticates against. DO UPDATE on external_id, not
    // DO NOTHING: the users upsert above refreshes it on a re-run against a recreated
    // Zitadel, and this row has to follow or the seeded admin stops resolving.
    sqlx::query!(
        r#"
        INSERT INTO nas_identities (user_id, external_id, is_blocked)
        VALUES ($1, $2, FALSE)
        ON CONFLICT (user_id) DO UPDATE SET external_id = EXCLUDED.external_id
        "#,
        local_user_id,
        zitadel_id
    ).execute(pool).await?;

    println!("✅ Ensured Storage Profile, Nas Identity and Root Folder exist");
    println!("🔑 Login: {} / {}", email, password);

    Ok(())
}
