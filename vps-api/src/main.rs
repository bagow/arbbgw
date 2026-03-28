mod db;
mod models;
mod routes;

use axum::{
    routing::{delete, get, patch, post},
    Router,
};
use std::env;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rusqlite::Connection;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
pub struct AppState {
    pub db: Connection,
    pub collector_token: String,
    pub pass_dima: String,
    pub pass_sergei: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "vps_api=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load .env ignores if not found
    let _ = dotenvy::dotenv();

    let collector_token = env::var("COLLECTOR_TOKEN").unwrap_or_else(|_| "secret_token_123".into());
    let pass_dima = env::var("PASS_DIMA").unwrap_or_else(|_| "pass_dima".into());
    let pass_sergei = env::var("PASS_SERGEI").unwrap_or_else(|_| "pass_sergei".into());

    // Connect to SQLite DB
    let db_path = env::var("DATABASE_URL").unwrap_or_else(|_| "../paper-trader/trades.db".into());
    tracing::info!("Connecting to SQLite at {}", db_path);
    let db = Connection::open(db_path).await?;
    
    // Initialize DB schema
    db::init_db(&db).await?;

    let state = AppState {
        db,
        collector_token,
        pass_dima,
        pass_sergei,
    };

    // Build the router
    let app = Router::new()
        .route("/api/update", post(routes::handle_update))
        .route("/api/unified", get(routes::handle_get_unified))
        .route("/api/trading/settings/all", get(routes::handle_get_all_settings))
        .route("/api/profiles", get(routes::handle_get_profiles).post(routes::handle_post_profile))
        .route("/api/profiles/:id", get(routes::handle_get_profile).patch(routes::handle_patch_profile).delete(routes::handle_delete_profile))
        .route("/api/profiles/:id/trades", delete(routes::handle_clear_trades))
        .with_state(state)
        .layer(CorsLayer::permissive()); // allow all origins since API Tunnel handles public exposure

    let addr = "0.0.0.0:4000";
    tracing::info!("Server listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

