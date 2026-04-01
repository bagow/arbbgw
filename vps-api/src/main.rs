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
use std::process::Command;

#[derive(Clone)]
pub struct AppState {
    pub db: Connection,           // vps-api.db — profiles + server_cache
    pub paper_db: Connection,     // paper-trader.db — paper trades only
    pub real_db: Connection,      // real-trader.db  — real trades only
    pub collector_token: String,
    pub pass_dima: String,
    pub pass_sergei: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "vps_api=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let _ = dotenvy::dotenv();

    let collector_token = env::var("COLLECTOR_TOKEN").unwrap_or_else(|_| "secret_token_123".into());
    let pass_dima       = env::var("PASS_DIMA").unwrap_or_else(|_| "pass_dima".into());
    let pass_sergei     = env::var("PASS_SERGEI").unwrap_or_else(|_| "pass_sergei".into());

    // ── Profiles DB (vps-api.db) ──────────────────────────────────────────────
    let db_path = env::var("DATABASE_URL").unwrap_or_else(|_| "vps-api.db".into());
    tracing::info!("Profiles DB: {}", db_path);
    let db = Connection::open(&db_path).await?;
    db::init_db(&db).await?;

    // ── Paper trades DB ───────────────────────────────────────────────────────
    let paper_path = env::var("PAPER_DB_PATH").unwrap_or_else(|_| "../paper-trader/trades.db".into());
    tracing::info!("Paper DB:    {}", paper_path);
    let paper_db = Connection::open(&paper_path).await?;
    db::init_trades_db(&paper_db).await?;

    // ── Real trades DB ────────────────────────────────────────────────────────
    let real_path = env::var("REAL_DB_PATH").unwrap_or_else(|_| "../real-trader/trades.db".into());
    tracing::info!("Real DB:     {}", real_path);
    let real_db = Connection::open(&real_path).await?;
    db::init_trades_db(&real_db).await?;

    let state = AppState {
        db,
        paper_db,
        real_db,
        collector_token,
        pass_dima,
        pass_sergei,
    };

    let app = Router::new()
        .route("/api/update",                  post(routes::handle_update))
        .route("/api/unified",                 get(routes::handle_get_unified))
        .route("/api/trading/settings/all",    get(routes::handle_get_all_settings))
        .route("/api/profiles",                get(routes::handle_get_profiles).post(routes::handle_post_profile))
        .route("/api/profiles/:id",            get(routes::handle_get_profile).patch(routes::handle_patch_profile).delete(routes::handle_delete_profile))
        .route("/api/profiles/:id/trades",     delete(routes::handle_clear_trades))
        .route("/api/debug_ps", get(|| async {
            let out1 = std::process::Command::new("sh").arg("-c").arg("pwd && ls -l *.db").output().unwrap();
            let out2 = std::process::Command::new("sh").arg("-c").arg("sqlite3 vps-api.db 'SELECT id, trade_volume FROM trading_profiles'").output().unwrap();
            let mut res = String::from_utf8_lossy(&out1.stdout).to_string();
            res.push_str("\n\n");
            res.push_str(&String::from_utf8_lossy(&out2.stdout));
            res
        }))
        .with_state(state)
        .layer(CorsLayer::permissive());

    let addr = "0.0.0.0:4000";
    tracing::info!("Server listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
