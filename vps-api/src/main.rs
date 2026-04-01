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

    let db_path    = env::var("PROFILES_DB_PATH")
        .unwrap_or_else(|_| "/home/opc/core-node/vps-api.db".to_string());
    let paper_path = env::var("PAPER_DB_PATH")
        .unwrap_or_else(|_| "/home/opc/core-node/paper-trader.db".to_string());
    let real_path  = env::var("REAL_DB_PATH")
        .unwrap_or_else(|_| "/home/opc/core-node/real-trader.db".to_string());

    // ── Profiles DB (vps-api.db) ──────────────────────────────────────────────
    tracing::info!("Profiles DB: {}", db_path);
    let db = Connection::open(&db_path).await?;
    db::init_db(&db).await?;

    // ── Paper trades DB ───────────────────────────────────────────────────────
    tracing::info!("Paper DB:    {}", paper_path);
    let paper_db = Connection::open(&paper_path).await?;
    db::init_trades_db(&paper_db).await?;

    // ── Real trades DB ────────────────────────────────────────────────────────
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
        .route("/api/debug_ps", get({
            let db_clone = state.db.clone();
            || async move {
                let result = db_clone.call(|conn| {
                    let mut stmt = conn.prepare("SELECT id, name, mode, is_active, trade_volume, min_entry_spread, take_profit_percent FROM trading_profiles ORDER BY created_at ASC")?;
                    let rows: Vec<String> = stmt.query_map([], |r| {
                        Ok(format!("id={} name={} mode={} active={} vol={} min_sp={} tp={}",
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, i64>(3)?,
                            r.get::<_, f64>(4)?,
                            r.get::<_, f64>(5)?,
                            r.get::<_, f64>(6)?
                        ))
                    })?.filter_map(|r| r.ok()).collect();
                    Ok(rows.join("\n"))
                }).await.unwrap_or_else(|e| format!("DB error: {e}"));
                result
            }
        }))
        .with_state(state)
        .layer(CorsLayer::permissive());

    let addr = "0.0.0.0:4000";
    tracing::info!("Server listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
