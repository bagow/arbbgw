//! core-node — single-process CEX arbitrage engine

mod settings;
mod spreads;
mod state;

use std::{collections::{HashMap, HashSet}, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use reqwest::Client;
use tokio::net::TcpListener;
use tokio_rusqlite::Connection;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use settings::load_all_settings;
use spreads::compute_spreads;
use state::{PriceMap, UpdateSignal};

mod collector {
    pub mod gate;
    pub mod htx;
    pub mod kucoin;
    pub mod binance;
    pub mod bybit;
    pub mod mexc;
    pub mod okx;
    pub mod bitget;
}

mod engine {
    pub mod real;
    pub mod paper;
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("core-node starting");

    let profiles_db_path = std::env::var("PROFILES_DB_PATH")
        .unwrap_or_else(|_| "vps-api.db".to_string());
    let real_db_path = std::env::var("REAL_DB_PATH")
        .unwrap_or_else(|_| "real-trader.db".to_string());
    let paper_db_path = std::env::var("PAPER_DB_PATH")
        .unwrap_or_else(|_| "paper-trader.db".to_string());
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3003".to_string())
        .parse().unwrap_or(3003);

    // ── HTTP client ──────────────────────────────────────────────────────────
    let http = Client::builder()
        .user_agent("core-node/1.0")
        .timeout(Duration::from_secs(15))
        .build()?;

    // ── Profiles DB (settings) ───────────────────────────────────────────────
    let profiles_db = Connection::open(&profiles_db_path).await
        .context(format!("Cannot open profiles DB: {}", profiles_db_path))?;
    info!("Profiles DB: {}", profiles_db_path);

    // ── Price map and signal ─────────────────────────────────────────────────
    let prices: PriceMap    = Arc::new(dashmap::DashMap::new());
    let signal: UpdateSignal = Arc::new(tokio::sync::Notify::new());

    // ── Spawn collectors ─────────────────────────────────────────────────────
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::gate::run(p, s).await }); }
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::htx::run(p, s).await }); }
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::kucoin::run(p, s).await }); }
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::binance::run(p, s).await }); }
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::bybit::run(p, s).await }); }
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::mexc::run(p, s).await }); }
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::okx::run(p, s).await }); }
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::bitget::run(p, s).await }); }

    // ── Trade DBs ────────────────────────────────────────────────────────────
    let real_db = Connection::open(&real_db_path).await
        .context(format!("Cannot open real-trader DB: {}", real_db_path))?;
    init_trades_db(&real_db).await?;

    let paper_db = Connection::open(&paper_db_path).await
        .context(format!("Cannot open paper-trader DB: {}", paper_db_path))?;
    init_trades_db(&paper_db).await?;

    // ── Pre-load & Spawn    // Bypassing cache to ensure real-time settings update

    // ── Engine loop (wakes on every WS tick) ─────────────────────────────────
    {
        let prices      = prices.clone();
        let signal      = signal.clone();
        let http        = http.clone();
        let real_db     = real_db.clone();
        let paper_db    = paper_db.clone();
        let profiles_db = profiles_db.clone();

        tokio::spawn(async move {
            let mut last_run = std::time::Instant::now();
            loop {
                signal.notified().await;
                let elapsed = last_run.elapsed();
                if elapsed < Duration::from_millis(50) {
                    tokio::time::sleep(Duration::from_millis(50) - elapsed).await;
                }
                last_run = std::time::Instant::now();

                // Load ALL profiles DIRECTLY from database (bypasses any caching bugs)
                let paper_profiles = settings::load_all_settings(&profiles_db, "paper").await;
                let real_profiles  = settings::load_all_settings(&profiles_db, "real").await;

                let min_vol = paper_profiles.iter().chain(real_profiles.iter())
                    .map(|p| p.min_volume_millions)
                    .fold(f64::MAX, f64::min);
                let min_vol = if min_vol == f64::MAX { 1.0 } else { min_vol };

                let spreads = compute_spreads(&prices, min_vol);
                if spreads.is_empty() { continue; }

                // Run paper engine for each paper profile
                for paper_s in paper_profiles {
                    engine::paper::run_cycle(spreads.clone(), paper_s, http.clone(), paper_db.clone()).await;
                }
                // Run real engine for each real profile
                for real_s in real_profiles {
                    engine::real::run_cycle(spreads.clone(), real_s, http.clone(), real_db.clone()).await;
                }
            }
        });
    }

    // ── Publish exchange_maps to server_cache every 30s (feeds CEX tab) ──────
    {
        let prices_snap = prices.clone();
        let cache_db    = profiles_db.clone();
        tokio::spawn(async move {
            // Wait just 5s for collectors to connect, then publish immediately
            // Subsequent updates every 30s
            let mut first = true;
            loop {
                if first {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    first = false;
                } else {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                let exchanges_list: Vec<String> = prices_snap.iter().map(|e| e.key().clone()).collect();
                let sym_map: HashMap<String, HashSet<String>> = prices_snap.iter()
                    .map(|e| {
                        let ex: String = e.key().clone();
                        let syms: HashSet<String> = e.value().keys().cloned().collect();
                        (ex, syms)
                    })
                    .collect();

                let mut pairs_map: HashMap<String, serde_json::Value> = HashMap::new();
                for ex_a in &exchanges_list {
                    let syms_a = sym_map.get(ex_a).cloned().unwrap_or_default();
                    let mut vs = serde_json::Map::new();
                    for ex_b in &exchanges_list {
                        if ex_a == ex_b { continue; }
                        let syms_b = sym_map.get(ex_b).cloned().unwrap_or_default();
                        let n = syms_a.intersection(&syms_b).count();
                        if n > 0 { vs.insert(format!("vs {}", ex_b), serde_json::json!(n)); }
                    }
                    pairs_map.insert(ex_a.clone(), serde_json::Value::Object(vs));
                }

                let val = serde_json::json!({ "ts": now_ms, "data": pairs_map }).to_string();
                let n = exchanges_list.len();
                let _ = cache_db.call(move |conn| {
                    let _ = conn.execute_batch("CREATE TABLE IF NOT EXISTS server_cache (key TEXT PRIMARY KEY, value TEXT, expires_at INTEGER);");
                    conn.execute(
                        "INSERT INTO server_cache(key,value,expires_at) VALUES('exchange_maps',?,?) \
                         ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
                        rusqlite::params![val, now_ms + 300_000],
                    )?;
                    Ok(())
                }).await;
                info!("exchange_maps: {} exchanges published", n);
            }
        });
    }

    // ── HTTP API ─────────────────────────────────────────────────────────────
    let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .layer(cors);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await.context("HTTP bind failed")?;
    info!("core-node listening on {}", addr);
    axum::serve(listener, app).await.context("HTTP server failed")?;

    Ok(())
}

async fn init_trades_db(db: &Connection) -> Result<()> {
    db.call(|conn| {
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        // 1. Create table if not exists
        conn.execute_batch("
            CREATE TABLE IF NOT EXISTS trades (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                pair                  TEXT    NOT NULL,
                exchange_a            TEXT    NOT NULL,
                exchange_b            TEXT    NOT NULL,
                direction             TEXT    NOT NULL,
                volume                REAL    NOT NULL,
                entry_price_a         REAL    NOT NULL,
                entry_price_b         REAL    NOT NULL,
                entry_spread          REAL    NOT NULL,
                entry_time            INTEGER NOT NULL,
                highest_spread_reached REAL   NOT NULL,
                status                TEXT    NOT NULL DEFAULT 'OPEN',
                exit_price_a          REAL,
                exit_price_b          REAL,
                exit_spread           REAL,
                exit_time             INTEGER,
                pnl                   REAL,
                gross_pnl             REAL,
                commission_cost       REAL,
                funding_cost          REAL,
                last_price_a          REAL,
                last_price_b          REAL,
                last_spread           REAL,
                curr_gross_pnl        REAL,
                pnl_a                 REAL,
                pnl_b                 REAL,
                close_reason          TEXT,
                profile_id            TEXT DEFAULT 'paper-default'
            );
        ")?;
        // 2. Migrate older DBs (errors ignored — columns might already exist)
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN gross_pnl REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN profile_id TEXT DEFAULT 'paper-default'", []);
        // 3. Create indexes AFTER ensuring columns exist
        let _ = conn.execute_batch("
            CREATE INDEX IF NOT EXISTS idx_trades_status  ON trades(status);
            CREATE INDEX IF NOT EXISTS idx_trades_profile ON trades(profile_id);
        ");
        Ok(())
    }).await?;
    Ok(())
}
