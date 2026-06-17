//! core-node — single-process CEX arbitrage engine (Tokyo deploy)

mod ema_warmup;
mod settings;
mod spreads;
mod state;

use std::{
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;

use anyhow::{Context, Result};
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use reqwest::Client;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio_rusqlite::Connection;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use settings::load_all_settings;
use spreads::compute_spreads;
use state::{PriceMap, UpdateSignal};

mod collector {
    pub mod binance;
    pub mod bybit;
    pub mod gate;
    pub mod kucoin;
    pub mod bitget;
    pub mod bingx;
    pub mod bitmex;
    pub mod ascendex;
    pub mod xt;
    pub mod toobit;
}

mod engine;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("core-node starting");

    let profiles_db_path =
        std::env::var("PROFILES_DB_PATH").unwrap_or_else(|_| "vps-api.db".to_string());
    let real_db_path =
        std::env::var("REAL_DB_PATH").unwrap_or_else(|_| "real-trader.db".to_string());
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3003".to_string())
        .parse()
        .unwrap_or(3003);

    // ── HTTP client ──────────────────────────────────────────────────────────
    let http = Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        .timeout(Duration::from_secs(15))
        .http1_only()  // Force HTTP/1.1: avoids H2 "locally-reset streams reached limit (1024)"
        .pool_max_idle_per_host(10)
        .pool_idle_timeout(Duration::from_secs(60))
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .build()?;

    let engine_ctx = Arc::new(crate::state::EngineContext::default());

    // ── Profiles DB (settings) — open and ensure schema is current ───────────
    let profiles_db = Connection::open(&profiles_db_path)
        .await
        .context(format!("Cannot open profiles DB: {}", profiles_db_path))?;
    // Enable WAL so vps-api (writer) and core-node (reader) don't block each other.
    // Migrate older DBs: add columns that may be missing (errors ignored).
    let _ = profiles_db
        .call(|conn| {
            let _ = conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;");
            let _ = conn.execute(
                "ALTER TABLE trading_profiles ADD COLUMN disabled_exchanges TEXT DEFAULT '[]'",
                [],
            );
            let _ = conn.execute_batch("CREATE TABLE IF NOT EXISTS server_cache (key TEXT PRIMARY KEY, value TEXT, expires_at INTEGER);");
            Ok(())
        })
        .await;


    // ── Price map and signal ─────────────────────────────────────────────────
    let prices: PriceMap = Arc::new(dashmap::DashMap::new());
    let signal: UpdateSignal = Arc::new(tokio::sync::Notify::new());

    // Note: collectors are spawned later, after settings cache is initialized

    // ── Trade DBs ────────────────────────────────────────────────────────────
    let real_db = Connection::open(&real_db_path)
        .await
        .context(format!("Cannot open real-trader DB: {}", real_db_path))?;
    engine::startup::init_trades_db(&real_db).await?;

    info!("Profiles DB path: {}", profiles_db_path);
    info!("Real DB path:     {}", real_db_path);

    // ── STARTUP DIAGNOSTIC: log and persist profile settings ─────────────────
    {
        let startup_profiles = load_all_settings(&profiles_db, "real").await;
        let active_count = startup_profiles.iter().filter(|p| p.is_active).count();
        info!(
            "STARTUP: {} active real profiles found in DB='{}' (total {})",
            active_count,
            profiles_db_path,
            startup_profiles.len()
        );
        let mut lines = format!(
            "db_path={}\nactive_real_count={}\n",
            profiles_db_path, active_count
        );
        for p in &startup_profiles {
            let line = format!(
                "id={} vol={} active={} min_spread={} max_spread={}\n",
                p.id, p.trade_volume, p.is_active, p.min_entry_spread, p.max_entry_spread
            );
            info!("STARTUP-PROFILE: {}", line.trim());
            lines.push_str(&line);
        }
        let _ = std::fs::write("/tmp/core-node-profiles.txt", &lines);
    }

    // ── Cached settings (refreshed every 2s from DB, not per-tick) ────────────
    let real_settings_cache: Arc<RwLock<Vec<settings::TradingSettings>>> =
        Arc::new(RwLock::new(Vec::new()));

    // Initial load
    *real_settings_cache.write().await = load_all_settings(&profiles_db, "real").await;

    // Background refresher — every 2 seconds
    settings::spawn_all_settings_refresher(
        profiles_db.clone(),
        "real",
        real_settings_cache.clone(),
        2,
    );

    // ── Global technical settings (refreshed every 10s) ──────────────────────
    let tech_settings_cache: Arc<RwLock<settings::TechnicalSettings>> = Arc::new(RwLock::new(
        settings::load_technical_settings(&profiles_db).await,
    ));
    settings::spawn_technical_settings_refresher(
        profiles_db.clone(),
        tech_settings_cache.clone(),
        10,
    );
    info!(
        "Technical settings loaded: tau={}s",
        tech_settings_cache.read().await.ema_tau_secs
    );

    // ── EMA Warmup: pre-seed structural spread norms from 24h candle history ──
    // Must be after tech_settings_cache init so we can pass real tau/settings.
    {
        let http_w = http.clone();
        let prices_w = prices.clone();
        let ts_warmup = tech_settings_cache.read().await.clone();
        tokio::spawn(async move { ema_warmup::run(http_w, prices_w, ts_warmup).await });
    }

    // ── Exchange keys — loaded once at startup ───────────────────────────────
    let exchange_keys = Arc::new(engine::real::ExchangeKeys::from_env());
    info!(
        "Exchange keys loaded: gate={} kucoin={} binance={} bybit={}",
        exchange_keys.supports("gate"),
        exchange_keys.supports("kucoin"),
        exchange_keys.supports("binance"),
        exchange_keys.supports("bybit")
    );

    // ── Pre-warm and fetch loops ──────────────────────────────────────────────
    engine::workers::market_data::start_multiplier_fetcher(
        http.clone(),
        engine_ctx.clone(),
        real_settings_cache.clone(),
    );
    engine::fetcher::start_balance_fetcher(
        http.clone(),
        exchange_keys.clone(),
        engine_ctx.clone(),
        real_settings_cache.clone(),
    );

    // ── Pre-init KuCoin Private WS so execution reports arrive for the first trade ──
    // Without this, first IOC order's fill is missed because WS connects AFTER the order.
    if exchange_keys.supports("kucoin") {
        info!("[startup] Pre-initializing KuCoin Private WS...");
        engine::exchanges::ws_kucoin_private::ensure_private_ws(
            &exchange_keys.kucoin_key,
            &exchange_keys.kucoin_secret,
            &exchange_keys.kucoin_passphrase,
        ).await;
        // Give WS time to connect and subscribe before first trade
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        info!("[startup] KuCoin Private WS ready.");
    }

    // ── Spawn collectors ─────────────────────────────────────────────────────
    engine::startup::spawn_collectors(&prices, &signal, &engine_ctx, &real_settings_cache);

    // ── Position sync — enabled with symbol normalization in real.rs ───────────
    // Detects externally closed/liquidated legs and emergency-closes the remaining leg.
    engine::workers::position_sync::start_position_sync(
        http.clone(),
        exchange_keys.clone(),
        real_db.clone(),
        engine_ctx.clone(),
    );

    // ── Pre-warm HTTP connections to exchange APIs ─────────────────────────
    engine::startup::prewarm_http(http.clone());

    // ── Engine loop (wakes on every WS tick) ─────────────────────────────────
    {
        let prices = prices.clone();
        let signal = signal.clone();
        let http = http.clone();
        let real_db = real_db.clone();
        let real_cache = real_settings_cache.clone();
        let keys = exchange_keys.clone();
        let ts_cache = tech_settings_cache.clone();
        let e_ctx = engine_ctx.clone();
        let profiles_db_engine = profiles_db.clone();

        tokio::spawn(async move {
            let mut last_run = std::time::Instant::now();
            loop {
                signal.notified().await;
                let elapsed = last_run.elapsed();
                if elapsed < Duration::from_millis(10) {
                    tokio::time::sleep(Duration::from_millis(10) - elapsed).await;
                }
                last_run = std::time::Instant::now();

                // Read cached settings (no SQL, just RwLock read)
                let real_profiles = real_cache.read().await.clone();
                let ts = ts_cache.read().await.clone();

                let min_vol = real_profiles
                    .iter()
                    .map(|p| p.min_volume_millions)
                    .fold(f64::MAX, f64::min);
                let min_vol = if min_vol == f64::MAX { 1.0 } else { min_vol };

                let spreads = compute_spreads(&prices, min_vol, &ts);

                // Writes to profiles_db since vps-api reads from it
                static LAST_SNAP: std::sync::atomic::AtomicU64 =
                    std::sync::atomic::AtomicU64::new(0);
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                if now_ms.saturating_sub(LAST_SNAP.load(std::sync::atomic::Ordering::Relaxed))
                    > 1000
                {
                    LAST_SNAP.store(now_ms, std::sync::atomic::Ordering::Relaxed);
                    let snapshot_spreads: Vec<_> = spreads.iter().take(500).cloned().collect();
                    let snapshot_val =
                        serde_json::json!({ "ts": now_ms as i64, "data": snapshot_spreads })
                            .to_string();
                    let cache_db = profiles_db_engine.clone();
                    tokio::spawn(async move {
                        let _ = cache_db.call(move |conn| {
                            if let Err(e) = conn.execute(
                                "INSERT INTO server_cache(key,value,expires_at) VALUES('snapshot',?,?) \
                                 ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
                                rusqlite::params![snapshot_val, now_ms as i64 + 10_000],
                            ) {
                                tracing::error!("Failed to save snapshot to db: {}", e);
                            }
                            Ok(())
                        }).await;
                    });
                }

                if spreads.is_empty() {
                    continue;
                }

                // ── BACKGROUND LEVERAGE WARMUP ──
                crate::engine::real::warmup_leverages(
                    &spreads,
                    &real_profiles,
                    http.clone(),
                    keys.clone(),
                );

                // ── UPDATE L2 WANTED SET (demand-driven subscriptions) ──
                // Populate from spread rows so collectors know which pairs need L2.
                // Using spread rows (not signals) avoids chicken-and-egg:
                // signals require L2, but L2 needs subscription BEFORE signal generation.
                {
                    e_ctx.l2_wanted.clear();
                    for row in spreads.iter().take(200) {
                        e_ctx.l2_wanted.insert(format!("{}:{}", row.exchange_a, row.pair));
                        e_ctx.l2_wanted.insert(format!("{}:{}", row.exchange_b, row.pair));
                    }
                }

                // ── EVALUATE SMART SIGNALS & RUN ENGINE ──
                let signals = crate::engine::scanner::generate_signals(
                    &spreads,
                    &prices,
                    &real_profiles,
                    &keys,
                    &e_ctx,
                    &ts,
                );

                for profile in &real_profiles {
                    engine::cycle::run_cycle(
                        signals.clone(),
                        spreads.clone(),
                        prices.clone(),
                        profile.clone(),
                        http.clone(),
                        real_db.clone(),
                        keys.clone(),
                        &ts,
                        e_ctx.clone(),
                    )
                    .await;
                }
            }
        });
    }

    engine::startup::spawn_exchange_maps_publisher(
        prices.clone(),
        profiles_db.clone(),
        engine_ctx.clone(),
    );

    // ── HTTP API ─────────────────────────────────────────────────────────────
    let cors = CorsLayer::new()
        .allow_origin(
            "http://127.0.0.1:4000"
                .parse::<axum::http::HeaderValue>()
                .unwrap(),
        )
        .allow_methods(Any)
        .allow_headers(Any);
    let app_state = (http.clone(), exchange_keys.clone());
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/api/positions/close", post(handle_close_position))
        .with_state(app_state)
        .layer(cors);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await.context("HTTP bind failed")?;
    info!("core-node listening on {}", addr);
    axum::serve(listener, app)
        .await
        .context("HTTP server failed")?;

    Ok(())
}


#[derive(Deserialize)]
pub struct CloseReq {
    pub exchange: String,
    pub symbol: String,
    pub size: f64,
}

async fn handle_close_position(
    State((http, keys)): State<(Client, Arc<crate::engine::real::ExchangeKeys>)>,
    Json(req): Json<CloseReq>,
) -> Result<&'static str, (axum::http::StatusCode, String)> {
    let conn = crate::engine::exchanges::get_connector(&req.exchange).ok_or((
        axum::http::StatusCode::BAD_REQUEST,
        "Unknown exchange".into(),
    ))?;

    let is_buy = req.size < 0.0;
    let size = req.size.abs();

    match conn
        .close_position(
            &http,
            &keys,
            &req.symbol,
            is_buy,
            size,
            false,
            10.0,
            0.0,
            false,
            0.0,
            0.0,
            0.000001,
            5,
            20,
            5000,
        )
        .await
    {
        Ok(_) => Ok("ok"),
        Err(e) => {
            tracing::error!(
                "[api] handle_close_position failed for {}: {}",
                req.symbol,
                e
            );
            Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e))
        }
    }
}
