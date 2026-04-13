//! core-node — single-process CEX arbitrage engine (v2026.04.01-r2)

mod settings;
mod spreads;
mod state;
mod ema_warmup;

use std::{collections::{HashMap, HashSet}, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::RwLock;

use anyhow::{Context, Result};
use axum::{routing::{get, post}, Router, Json, extract::State};
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
    pub mod gate;
    pub mod kucoin;
    pub mod binance;
    pub mod bybit;
}

mod engine;
 

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
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3003".to_string())
        .parse().unwrap_or(3003);

    // ── HTTP client ──────────────────────────────────────────────────────────
    let http = Client::builder()
        .user_agent("core-node/1.0")
        .timeout(Duration::from_secs(15))
        .pool_max_idle_per_host(10)       // Keep connections alive to exchanges
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)                // Disable Nagle's for lower latency
        .build()?;

    let engine_ctx = Arc::new(crate::state::EngineContext::default());

    engine::real::start_multiplier_fetcher(http.clone(), engine_ctx.clone());

    // ── Profiles DB (settings) — open and ensure schema is current ───────────
    let profiles_db = Connection::open(&profiles_db_path).await
        .context(format!("Cannot open profiles DB: {}", profiles_db_path))?;
    // Enable WAL so vps-api (writer) and core-node (reader) don't block each other.
    // Migrate older DBs: add columns that may be missing (errors ignored).
    let _ = profiles_db.call(|conn| {
        let _ = conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;");
        let _ = conn.execute(
            "ALTER TABLE trading_profiles ADD COLUMN disabled_exchanges TEXT DEFAULT '[]'",
            [],
        );
        Ok(())
    }).await;
    info!("Profiles DB: {}", profiles_db_path);

    // ── Price map and signal ─────────────────────────────────────────────────
    let prices: PriceMap    = Arc::new(dashmap::DashMap::new());
    let signal: UpdateSignal = Arc::new(tokio::sync::Notify::new());

    // ── Spawn collectors ─────────────────────────────────────────────────────
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::gate::run(p, s).await }); }
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::kucoin::run(p, s).await }); }
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::binance::run(p, s).await }); }
    { let p = prices.clone(); let s = signal.clone(); tokio::spawn(async move { collector::bybit::run(p, s).await }); }

    // ── Trade DBs ────────────────────────────────────────────────────────────
    let real_db = Connection::open(&real_db_path).await
        .context(format!("Cannot open real-trader DB: {}", real_db_path))?;
    init_trades_db(&real_db).await?;

    info!("Profiles DB path: {}", profiles_db_path);
    info!("Real DB path:     {}", real_db_path);

    // ── STARTUP DIAGNOSTIC: log and persist profile settings ─────────────────
    {
        let startup_profiles = load_all_settings(&profiles_db, "real").await;
        let active_count = startup_profiles.iter().filter(|p| p.is_active).count();
        info!("STARTUP: {} active real profiles found in DB='{}' (total {})",
            active_count, profiles_db_path, startup_profiles.len());
        let mut lines = format!("db_path={}\nactive_real_count={}\n",
            profiles_db_path, active_count);
        for p in &startup_profiles {
            let line = format!("id={} vol={} active={} min_spread={} max_spread={}\n",
                p.id, p.trade_volume, p.is_active, p.min_entry_spread, p.max_entry_spread);
            info!("STARTUP-PROFILE: {}", line.trim());
            lines.push_str(&line);
        }
        let _ = std::fs::write("/tmp/core-node-profiles.txt", &lines);
    }

    // ── Cached settings (refreshed every 2s from DB, not per-tick) ────────────
    let real_settings_cache: Arc<RwLock<Vec<settings::TradingSettings>>> = Arc::new(RwLock::new(Vec::new()));

    // Initial load
    *real_settings_cache.write().await = load_all_settings(&profiles_db, "real").await;

    // Background refresher — every 2 seconds
    settings::spawn_all_settings_refresher(profiles_db.clone(), "real", real_settings_cache.clone(), 2);

    // ── Global technical settings (refreshed every 10s) ──────────────────────
    let tech_settings_cache: Arc<RwLock<settings::TechnicalSettings>> = Arc::new(RwLock::new(
        settings::load_technical_settings(&profiles_db).await
    ));
    settings::spawn_technical_settings_refresher(profiles_db.clone(), tech_settings_cache.clone(), 10);
    info!("Technical settings loaded: tau={}s",
        tech_settings_cache.read().await.ema_tau_secs);

    // ── EMA Warmup: pre-seed structural spread norms from 24h candle history ──
    // Must be after tech_settings_cache init so we can pass real tau/settings.
    {
        let http_w  = http.clone();
        let prices_w = prices.clone();
        let ts_warmup = tech_settings_cache.read().await.clone();
        tokio::spawn(async move { ema_warmup::run(http_w, prices_w, ts_warmup).await });
    }

    // ── Exchange keys — loaded once at startup ───────────────────────────────
    let exchange_keys = Arc::new(engine::real::ExchangeKeys::from_env());
    info!("Exchange keys loaded: gate={} kucoin={} binance={} bybit={}",
        exchange_keys.supports("gate"),
        exchange_keys.supports("kucoin"), exchange_keys.supports("binance"),
        exchange_keys.supports("bybit"));

    // ── Balance fetcher — refreshes available margin every 30s ────────────────
    engine::fetcher::start_balance_fetcher(http.clone(), exchange_keys.clone(), engine_ctx.clone());

    // ── Position sync — DISABLED: caused false EXTERNAL_PARTIAL_CLOSE ──────────
    // Symbol format mismatch between DB (ARIAUSDT) and exchange API responses
    // caused false positives. Needs proper symbol normalization before re-enabling.
    // engine::real::start_position_sync(http.clone(), exchange_keys.clone(), real_db.clone(), engine_ctx.clone());

    // ── Pre-warm HTTP connections to exchange APIs ────────────────────────────
    // Establishes DNS + TCP + TLS ahead of time so the first trade doesn't
    // pay a 200-500ms cold-start penalty for handshake.
    {
        let h = http.clone();
        tokio::spawn(async move {
            let endpoints = [
                "https://fapi.binance.com/fapi/v1/time",
                "https://api.bybit.com/v5/market/time",
                "https://api.gateio.ws/api/v4/futures/usdt/contracts",
                "https://api-futures.kucoin.com/api/v1/timestamp",
            ];
            for url in endpoints {
                let _ = h.get(url).timeout(Duration::from_secs(5)).send().await;
            }
            info!("HTTP connections pre-warmed to {} exchanges", endpoints.len());
        });
    }

    // ── Engine loop (wakes on every WS tick) ─────────────────────────────────
    {
        let prices      = prices.clone();
        let signal      = signal.clone();
        let http        = http.clone();
        let real_db     = real_db.clone();
        let real_cache  = real_settings_cache.clone();
        let keys        = exchange_keys.clone();
        let ts_cache    = tech_settings_cache.clone();
        let e_ctx       = engine_ctx.clone();

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
                let real_profiles  = real_cache.read().await.clone();

                // Log settings being used (every ~5s = ~500 ticks at 10ms)
                static TICK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let tick = TICK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if tick % 500 == 0 {
                    for p in real_profiles.iter() {
                        info!("[settings] real  id={} active={} vol={} tp={}% min_spread={}",
                            p.id, p.is_active, p.trade_volume, p.take_profit_percent, p.min_entry_spread);
                    }
                }

                let min_vol = real_profiles.iter()
                    .map(|p| p.min_volume_millions)
                    .fold(f64::MAX, f64::min);
                let min_vol = if min_vol == f64::MAX { 1.0 } else { min_vol };

                let ts = ts_cache.read().await.clone();

                let spreads = compute_spreads(&prices, min_vol, &ts);
                
                // Writes to real_db (server_cache) to avoid locking profiles_db
                static LAST_SNAP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
                if now_ms.saturating_sub(LAST_SNAP.load(std::sync::atomic::Ordering::Relaxed)) > 1000 {
                    LAST_SNAP.store(now_ms, std::sync::atomic::Ordering::Relaxed);
                    let snapshot_spreads: Vec<_> = spreads.iter().take(500).cloned().collect();
                    let snapshot_val = serde_json::json!({ "ts": now_ms as i64, "data": snapshot_spreads }).to_string();
                    let cache_db = real_db.clone();
                    tokio::spawn(async move {
                        let _ = cache_db.call(move |conn| {
                            let _ = conn.execute("CREATE TABLE IF NOT EXISTS server_cache (key TEXT PRIMARY KEY, value TEXT, expires_at INTEGER);", []);
                            Ok(conn.execute(
                                "INSERT INTO server_cache(key,value,expires_at) VALUES('snapshot',?,?) \
                                 ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
                                rusqlite::params![snapshot_val, now_ms as i64 + 10_000],
                            )?)
                        }).await;
                    });
                }

                if spreads.is_empty() { continue; }

                // ── BACKGROUND LEVERAGE WARMUP ──
                crate::engine::real::warmup_leverages(&spreads, &real_profiles, http.clone(), keys.clone());

                // ── EVALUATE SMART SIGNALS ──
                let signals = crate::engine::scanner::generate_signals(
                    &spreads, &prices, &real_profiles, &keys, &e_ctx, &ts
                );

                // Run REAL engine first — real money needs freshest prices
                for real_s in real_profiles {
                    engine::cycle::run_cycle(signals.clone(), spreads.clone(), prices.clone(), real_s, http.clone(), real_db.clone(), keys.clone(), &ts, e_ctx.clone()).await;
                }
            }
        });
    }

    {
        let prices_snap  = prices.clone();
        let cache_db     = real_db.clone();  // write to real-trader.db, not profiles
        let e_ctx        = engine_ctx.clone();
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

                // Collect real balances + fee rates from engine caches
                let mut balances: HashMap<String, f64> = HashMap::new();
                let mut margin_balances: HashMap<String, f64> = HashMap::new();
                let mut positions: HashMap<String, Vec<crate::engine::exchanges::traits::PositionInfo>> = HashMap::new();
                let mut fees: HashMap<String, f64> = HashMap::new();
                for entry in e_ctx.balances.iter() {
                    balances.insert(entry.key().clone(), *entry.value());
                }
                for entry in e_ctx.margin_balances.iter() {
                    margin_balances.insert(entry.key().clone(), *entry.value());
                }
                for entry in e_ctx.positions.iter() {
                    positions.insert(entry.key().clone(), entry.value().clone());
                }
                for entry in e_ctx.fee_rates.iter() {
                    fees.insert(entry.key().clone(), *entry.value());
                }

                let val = serde_json::json!({ 
                    "ts": now_ms, 
                    "data": pairs_map, 
                    "balances": balances, 
                    "margin_balances": margin_balances,
                    "positions": positions,
                    "fees": fees 
                }).to_string();
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
    let cors = CorsLayer::new()
        .allow_origin("http://127.0.0.1:4000".parse::<axum::http::HeaderValue>().unwrap())
        .allow_methods(Any).allow_headers(Any);
    let app_state = (http.clone(), exchange_keys.clone());
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/api/positions/close", post(handle_close_position))
        .with_state(app_state)
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
                slippage_cost         REAL,
                last_price_a          REAL,
                last_price_b          REAL,
                last_spread           REAL,
                curr_gross_pnl        REAL,
                pnl_a                 REAL,
                pnl_b                 REAL,
                close_reason          TEXT,
                profile_id            TEXT DEFAULT 'real-default',
                order_id_a            TEXT,
                order_id_b            TEXT,
                contracts_a           INTEGER,
                contracts_b           INTEGER
            );
        ")?;
        // 2. Migrate older DBs (errors ignored — columns might already exist)
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN gross_pnl REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN profile_id TEXT DEFAULT 'real-default'", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN slippage_cost REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN order_id_a TEXT", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN order_id_b TEXT", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN contracts_a INTEGER", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN contracts_b INTEGER", []);
        // New columns that were missing in previous migrations:
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN last_price_a REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN last_price_b REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN last_spread REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN curr_gross_pnl REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN pnl_a REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN pnl_b REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN close_reason TEXT", []);
        // Funding rate tracking columns (added for accurate PnL)
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN funding_rate_a REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN funding_rate_b REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN funding_next_ts INTEGER", []);
        // 3. Create indexes AFTER ensuring columns exist
        let _ = conn.execute_batch("
            CREATE INDEX IF NOT EXISTS idx_trades_status  ON trades(status);
            CREATE INDEX IF NOT EXISTS idx_trades_profile ON trades(profile_id);
        ");
        Ok(())
    }).await?;
    Ok(())
}
// Trigger redeploy
// trigger deploy-core
// trigger core node again

// trigger deploy

#[derive(Deserialize)]
pub struct CloseReq { pub exchange: String, pub symbol: String, pub size: f64 }

async fn handle_close_position(
    State((http, keys)): State<(Client, Arc<crate::engine::real::ExchangeKeys>)>,
    Json(req): Json<CloseReq>
) -> Result<&'static str, (axum::http::StatusCode, String)> {
    let conn = crate::engine::exchanges::get_connector(&req.exchange)
        .ok_or((axum::http::StatusCode::BAD_REQUEST, "Unknown exchange".into()))?;
    
    let is_buy = req.size < 0.0;
    let size = req.size.abs();
    
    match conn.close_position(&http, &keys, &req.symbol, is_buy, size, false, 10.0, 0.0, false, 0.0, 0.0, 0.000001, 5, 20).await {
        Ok(_) => Ok("ok"),
        Err(e) => {
            tracing::error!("[api] handle_close_position failed for {}: {}", req.symbol, e);
            Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e))
        }
    }
}
