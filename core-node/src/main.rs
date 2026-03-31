//! core-node — single-process CEX arbitrage engine
//!
//! Architecture (Arc<DashMap> + Notify):
//!
//!   WS handlers ──writes──▶ PriceMap ──▶ Notify.notify_one()
//!                                              │
//!   Engine loop ◀──notified()────────────────┘
//!        │
//!        ├─▶ compute_spreads(PriceMap)   [zero HTTP, zero copy]
//!        ├─▶ real engine (place orders)
//!        └─▶ paper engine (simulate)
//!
//! Replacing rust-collector → HTTP → real/paper-trader eliminates 200-400ms
//! of inter-process latency on every spread event.

mod settings;
mod spreads;
mod state;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use reqwest::Client;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_rusqlite::Connection;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use settings::{load_settings, spawn_settings_refresher, TradingSettings};
use spreads::compute_spreads;
use state::{PriceMap, UpdateSignal};

// ─── Collector modules (WS handlers) ─────────────────────────────────────────
// Each handler runs in its own tokio task, writes to the shared PriceMap,
// then calls signal.notify_one() so the engine wakes up immediately.

mod collector {
    pub mod gate;
    pub mod htx;
    pub mod kucoin;
}

// ─── Engine module (trade decisions) ─────────────────────────────────────────
mod engine {
    pub mod real;
    pub mod paper;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env if present (dev convenience)
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("core-node starting");

    // ── Config from environment ──────────────────────────────────────────────
    let profiles_db_path = std::env::var("PROFILES_DB_PATH")
        .unwrap_or_else(|_| "vps-api.db".to_string());
    let real_db_path = std::env::var("REAL_DB_PATH")
        .unwrap_or_else(|_| "real-trader.db".to_string());
    let paper_db_path = std::env::var("PAPER_DB_PATH")
        .unwrap_or_else(|_| "paper-trader.db".to_string());
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3003".to_string())
        .parse().unwrap_or(3003);
    let _worker_url = std::env::var("WORKER_URL")
        .unwrap_or_else(|_| "https://api.arb.bagow.com".to_string());

    // ── HTTP client shared across REST order calls ───────────────────────────
    let http = Client::builder()
        .user_agent("core-node/1.0")
        .timeout(Duration::from_secs(15))
        .build()?;

    // ── Profiles DB (read settings) ──────────────────────────────────────────
    let profiles_db = Connection::open(&profiles_db_path).await
        .context(format!("Cannot open profiles DB: {}", profiles_db_path))?;
    info!("Profiles DB: {}", profiles_db_path);

    // ── Load initial settings ────────────────────────────────────────────────
    let real_settings_init  = load_settings(&profiles_db, "real").await;
    let paper_settings_init = load_settings(&profiles_db, "paper").await;
    info!("Real  settings: active={} min_spread={:.3} vol={}",
        real_settings_init.is_active, real_settings_init.min_entry_spread, real_settings_init.trade_volume);
    info!("Paper settings: active={} min_spread={:.3} vol={}",
        paper_settings_init.is_active, paper_settings_init.min_entry_spread, paper_settings_init.trade_volume);

    let real_settings:  Arc<RwLock<TradingSettings>> = Arc::new(RwLock::new(real_settings_init));
    let paper_settings: Arc<RwLock<TradingSettings>> = Arc::new(RwLock::new(paper_settings_init));

    // Background settings refresh every 5 seconds
    spawn_settings_refresher(profiles_db.clone(), "real",  Arc::clone(&real_settings),  5);
    spawn_settings_refresher(profiles_db.clone(), "paper", Arc::clone(&paper_settings), 5);

    // ── Shared price map + event signal ─────────────────────────────────────
    let prices: PriceMap  = Arc::new(dashmap::DashMap::new());
    let signal: UpdateSignal = Arc::new(tokio::sync::Notify::new());

    // ── Start WS collectors (Gate, HTX, KuCoin + Binance, Bybit, MEXC, OKX, Bitget) ──
    {
        let p = Arc::clone(&prices); let s = Arc::clone(&signal);
        tokio::spawn(async move { collector::gate::run(p, s).await });
    }
    {
        let p = Arc::clone(&prices); let s = Arc::clone(&signal);
        tokio::spawn(async move { collector::htx::run(p, s).await });
    }
    {
        let p = Arc::clone(&prices); let s = Arc::clone(&signal);
        tokio::spawn(async move { collector::kucoin::run(p, s).await });
    }
    {
        let p = Arc::clone(&prices); let s = Arc::clone(&signal);
        tokio::spawn(async move { collector::binance::run(p, s).await });
    }
    {
        let p = Arc::clone(&prices); let s = Arc::clone(&signal);
        tokio::spawn(async move { collector::bybit::run(p, s).await });
    }
    {
        let p = Arc::clone(&prices); let s = Arc::clone(&signal);
        tokio::spawn(async move { collector::mexc::run(p, s).await });
    }
    {
        let p = Arc::clone(&prices); let s = Arc::clone(&signal);
        tokio::spawn(async move { collector::okx::run(p, s).await });
    }
    {
        let p = Arc::clone(&prices); let s = Arc::clone(&signal);
        tokio::spawn(async move { collector::bitget::run(p, s).await });
    }

    // ── Real trade DB ────────────────────────────────────────────────────────
    let real_db = Connection::open(&real_db_path).await
        .context(format!("Cannot open real-trader DB: {}", real_db_path))?;
    init_trades_db(&real_db).await?;

    // ── Paper trade DB ───────────────────────────────────────────────────────
    let paper_db = Connection::open(&paper_db_path).await
        .context(format!("Cannot open paper-trader DB: {}", paper_db_path))?;
    init_trades_db(&paper_db).await?;

    // ── Engine loop — wakes on every WS tick ────────────────────────────────
    {
        let prices   = Arc::clone(&prices);
        let signal   = Arc::clone(&signal);
        let rs       = Arc::clone(&real_settings);
        let ps       = Arc::clone(&paper_settings);
        let http     = http.clone();
        let real_db  = real_db.clone();
        let paper_db = paper_db.clone();

        tokio::spawn(async move {
            let mut last_run = std::time::Instant::now();
            loop {
                // Wait for next price update
                signal.notified().await;

                // Debounce: don't run faster than 50ms
                let elapsed = last_run.elapsed();
                if elapsed < Duration::from_millis(50) {
                    tokio::time::sleep(Duration::from_millis(50) - elapsed).await;
                }
                last_run = std::time::Instant::now();

                // Read settings (nanosecond lock)
                let real_s  = rs.read().await.clone();
                let paper_s = ps.read().await.clone();

                // Compute spreads directly from PriceMap — zero HTTP, zero copy
                let min_vol = real_s.min_volume_millions.min(paper_s.min_volume_millions);
                let spreads = compute_spreads(&prices, min_vol);

                if spreads.is_empty() { continue; }

                // Run real and paper engines concurrently
                let real_fut = engine::real::run_cycle(
                    spreads.clone(), real_s, http.clone(), real_db.clone(),
                );
                let paper_fut = engine::paper::run_cycle(
                    spreads, paper_s, http.clone(), paper_db.clone(),
                );

                tokio::join!(real_fut, paper_fut);
            }
        });
    }

    // ── HTTP API (health + dashboard passthrough) ────────────────────────────
    let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .layer(cors);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await
        .context("HTTP server failed to bind")?;
    info!("core-node listening on {}", addr);

    axum::serve(listener, app).await
        .context("HTTP server failed")?;

    Ok(())
}
// ─── DB schema init ───────────────────────────────────────────────────────────

async fn init_trades_db(db: &tokio_rusqlite::Connection) -> anyhow::Result<()> {
    db.call(|conn| {
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
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
                highest_spread_reached REAL   NOT NULL DEFAULT -9999,
                status                TEXT    NOT NULL DEFAULT 'OPEN',
                exit_price_a          REAL,
                exit_price_b          REAL,
                exit_spread           REAL,
                exit_time             INTEGER,
                pnl                   REAL,
                gross_pnl             REAL,
                pnl_a                 REAL    DEFAULT 0,
                pnl_b                 REAL    DEFAULT 0,
                curr_gross_pnl        REAL,
                close_reason          TEXT,
                order_id_a            TEXT,
                order_id_b            TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
        ")?;
        // Migrate old schemas — ignore errors if column already exists
        for col in &[
            "ALTER TABLE trades ADD COLUMN gross_pnl REAL",
            "ALTER TABLE trades ADD COLUMN order_id_a TEXT",
            "ALTER TABLE trades ADD COLUMN order_id_b TEXT",
            "ALTER TABLE trades ADD COLUMN curr_gross_pnl REAL",
        ] {
            let _ = conn.execute_batch(col);
        }
        Ok(())
    }).await.map_err(|e| anyhow::anyhow!("DB init failed: {}", e))
}
