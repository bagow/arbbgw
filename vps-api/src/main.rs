mod db;
mod models;
mod routes;

use axum::{
    routing::{delete, get, post},
    Router,
};
use std::env;

use tokio::net::TcpListener;
use tokio_rusqlite::Connection;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
pub struct AppState {
    pub db: Connection,      // vps-api.db — profiles + server_cache
    pub real_db: Connection, // real-trader.db  — real trades only
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

    let collector_token = env::var("COLLECTOR_TOKEN")
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow::anyhow!("COLLECTOR_TOKEN must be configured"))?;
    let pass_dima = env::var("PASS_DIMA")
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow::anyhow!("PASS_DIMA must be configured"))?;
    let pass_sergei = env::var("PASS_SERGEI")
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow::anyhow!("PASS_SERGEI must be configured"))?;

    let db_path = env::var("PROFILES_DB_PATH")
        .unwrap_or_else(|_| "/home/opc/core-node/vps-api.db".to_string());
    let real_path = env::var("REAL_DB_PATH")
        .unwrap_or_else(|_| "/home/opc/core-node/real-trader.db".to_string());

    // ── Profiles DB (vps-api.db) ──────────────────────────────────────────────
    tracing::info!("Profiles DB: {}", db_path);
    let db = Connection::open(&db_path).await?;
    db::init_db(&db).await?;

    // ── Real trades DB ────────────────────────────────────────────────────────
    tracing::info!("Real DB:     {}", real_path);
    let real_db = Connection::open(&real_path).await?;
    db::init_trades_db(&real_db).await?;

    let state = AppState {
        db,
        real_db,
        collector_token,
        pass_dima,
        pass_sergei,
    };

    let app = Router::new()
        .route("/api/update", post(routes::handle_update))
        .route("/api/unified", get(routes::handle_get_unified))
        .route("/api/logs/core-node", get(routes::handle_get_logs))
        .route(
            "/api/trading/settings/all",
            get(routes::handle_get_all_settings),
        )
        .route(
            "/api/profiles",
            get(routes::handle_get_profiles).post(routes::handle_post_profile),
        )
        .route(
            "/api/profiles/:id",
            get(routes::handle_get_profile)
                .patch(routes::handle_patch_profile)
                .delete(routes::handle_delete_profile),
        )
        .route(
            "/api/profiles/:id/trades",
            delete(routes::handle_clear_trades),
        )
        .route(
            "/api/profiles/:id/market-close-all",
            post(routes::handle_market_close_all),
        )
        .route(
            "/api/trades/:id/close",
            post(routes::handle_market_close_trade),
        )
        .route(
            "/api/positions/close",
            post(routes::handle_close_exchange_position),
        )
        .route(
            "/api/kill-switch/reset",
            post(routes::handle_kill_switch_reset),
        )
        .route(
            "/api/technical-settings",
            get(routes::handle_get_technical_settings)
                .patch(routes::handle_patch_technical_settings),
        )
        .route(
            "/api/trading/settings",
            get(routes::handle_get_technical_settings),
        )
        .route(
            "/api/system/metrics",
            get(routes::handle_get_system_metrics),
        )
        .route(
            "/api/klines/:exchange/:pair/:interval/:limit",
            get(routes::handle_get_klines),
        )
        .with_state(state)
        .layer(CorsLayer::permissive());

    let addr = "[::]:4000";
    tracing::info!("Server listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
// Trigger vps-api rebuild
// redeploy all
// deploy trigger 1778745106
