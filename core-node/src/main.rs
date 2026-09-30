//! core-node — single-process CEX arbitrage engine (Tokyo deploy)

mod ema_warmup;
mod settings;
mod spreads;
mod state;

use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::RwLock;

use anyhow::{Context, Result};
use futures_util::FutureExt;
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
use spreads::{compute_spreads, discovery_spread_threshold, DISCOVERY_PREWARM_RATIO};
use state::{PriceMap, UpdateSignal};

mod collector {
    pub mod ascendex;
    pub mod binance;
    pub mod bingx;
    pub mod bitget;
    pub mod bitmex;
    pub mod bybit;
    pub mod gate;
    pub mod kucoin;
    pub mod toobit;
    pub mod xt;
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

    // engine_ctx is created after profiles_db init below (needs profiles_db for kill-switch persistence)

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
            let _ = conn.execute("ALTER TABLE trading_profiles ADD COLUMN hard_stop_percent REAL DEFAULT 1.0", []);
            let _ = conn.execute("ALTER TABLE trading_profiles ADD COLUMN trailing_activation_percent REAL DEFAULT 0.1", []);
            let _ = conn.execute("ALTER TABLE trading_profiles ADD COLUMN daily_loss_limit_usdt REAL DEFAULT 5.0", []);
            let _ = conn.execute("ALTER TABLE trading_profiles ADD COLUMN max_total_exposure_usdt REAL DEFAULT 300.0", []);
            let _ = conn.execute_batch("CREATE TABLE IF NOT EXISTS server_cache (key TEXT PRIMARY KEY, value TEXT, expires_at INTEGER);");
            Ok(())
        })
        .await;

    // ── Engine context (created after profiles_db so kill-switch can persist state) ──
    let mut ctx_default = crate::state::EngineContext::default();
    ctx_default.profiles_db = Some(profiles_db.clone());
    let persisted_kill: bool = profiles_db
        .call(|c| {
            let active: i64 = c.query_row(
                "SELECT COUNT(*) FROM server_cache WHERE key='kill_switch_active' AND value='1'",
                [],
                |r| r.get(0),
            )?;
            Ok(active != 0)
        })
        .await
        .context("Cannot read persisted kill switch")?;
    ctx_default
        .is_killed
        .store(persisted_kill, std::sync::atomic::Ordering::Release);
    let engine_ctx = Arc::new(ctx_default);

    // ── Price map and signal ─────────────────────────────────────────────────
    let prices: PriceMap = Arc::new(dashmap::DashMap::new());
    let signal: UpdateSignal = Arc::new(tokio::sync::Notify::new());

    // Note: collectors are spawned later, after settings cache is initialized

    // ── Trade DBs ────────────────────────────────────────────────────────────
    let real_db = Connection::open(&real_db_path)
        .await
        .context(format!("Cannot open real-trader DB: {}", real_db_path))?;
    engine::startup::init_trades_db(&real_db).await?;
    let journal = engine::journal::Journal {
        db: real_db.clone(),
    };
    journal.init().await?;
    let shadow_tx = engine::shadow::start(
        std::path::Path::new(&real_db_path).with_file_name("shadow-books.db"),
    )
    .await?;
    let legacy_active = real_db.call(|c| {
        c.query_row("SELECT COUNT(*) FROM trades t LEFT JOIN execution_plans p ON p.trade_id=t.id WHERE t.status NOT IN ('CLOSED','CANCELLED') AND p.trade_id IS NULL", [], |r| r.get::<_,i64>(0)).map_err(Into::into)
    }).await?;
    if legacy_active > 0 {
        engine::managed::kill(
            &engine_ctx,
            &journal,
            format!("{legacy_active} legacy positions require exchange reconciliation"),
        )
        .await;
    }

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
        let diagnostic_path = std::env::var("CORE_DIAGNOSTIC_PATH")
            .unwrap_or_else(|_| "/tmp/core-node-profiles.txt".into());
        let _ = std::fs::write(diagnostic_path, &lines);
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
        Some(engine_ctx.clone()),
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
        let books_w = engine_ctx.l2_books.clone();
        let ts_warmup = tech_settings_cache.read().await.clone();
        tokio::spawn(async move { ema_warmup::run(http_w, prices_w, books_w, ts_warmup).await });
    }

    // ── Exchange keys — loaded once at startup ───────────────────────────────
    let exchange_keys = Arc::new(engine::real::ExchangeKeys::from_env());
    info!(
        "Active-route exchange keys loaded: kucoin={} binance={}",
        exchange_keys.supports("kucoin"),
        exchange_keys.supports("binance"),
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
        info!("[startup] Pre-initializing KuCoin execution and order-report WS channels...");
        engine::exchanges::ws_kucoin_order::prewarm(
            &exchange_keys.kucoin_key,
            &exchange_keys.kucoin_secret,
            &exchange_keys.kucoin_passphrase,
        );
        engine::exchanges::ws_kucoin_private::ensure_private_ws(
            &exchange_keys.kucoin_key,
            &exchange_keys.kucoin_secret,
            &exchange_keys.kucoin_passphrase,
        )
        .await;
        match engine::exchanges::ws_kucoin_order::wait_ready(
            &exchange_keys.kucoin_key,
            &exchange_keys.kucoin_secret,
            &exchange_keys.kucoin_passphrase,
            std::time::Duration::from_secs(12),
        )
        .await
        {
            Ok(()) => info!("[startup] KuCoin Pro order WS authenticated."),
            Err(error) => tracing::error!("[startup] {error}; order entry remains fail-closed"),
        }
        // The Classic report stream has no readiness acknowledgement exposed
        // yet; give its subscription a short head start without delaying the
        // already-authenticated Pro order lane.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    // ── Pre-init Binance Trading WebSocket ───────────────────────────────────
    if exchange_keys.supports("binance") {
        info!("[startup] Pre-initializing Binance query and order WS APIs...");
        engine::exchanges::ws_trader::prewarm_ws_connections();
    }

    // ── Spawn collectors ─────────────────────────────────────────────────────
    engine::startup::spawn_collectors(&prices, &signal, &engine_ctx, &real_settings_cache);

    // Independent of market ticks and entry eligibility, including while entries are halted.
    engine::managed::supervise(
        journal.clone(),
        http.clone(),
        exchange_keys.clone(),
        prices.clone(),
        engine_ctx.clone(),
    );

    engine::account_monitor::start(
        journal.clone(),
        http.clone(),
        exchange_keys.clone(),
        engine_ctx.clone(),
    );
    engine::settlement::start(journal.clone(), http.clone(), exchange_keys.clone());

    // ── Pre-warm HTTP connections to exchange APIs ─────────────────────────
    engine::startup::prewarm_http(http.clone());

    // ── Engine loop (wakes on every WS tick) ─────────────────────────────────
    {
        let prices = prices.clone();
        let signal = signal.clone();
        let http = http.clone();
        let journal = journal.clone();
        let real_cache = real_settings_cache.clone();
        let keys = exchange_keys.clone();
        let ts_cache = tech_settings_cache.clone();
        let e_ctx = engine_ctx.clone();
        let profiles_db_engine = profiles_db.clone();

        tokio::spawn(async move {
            // KuCoin publishes every 10 ms; the old 20 ms floor discarded that
            // edge. This remains event-driven, so an idle market does no scans.
            const MIN_ENGINE_CYCLE: Duration = Duration::from_millis(5);
            let mut last_run = std::time::Instant::now();
            loop {
                signal.notified().await;
                let elapsed = last_run.elapsed();
                if elapsed < MIN_ENGINE_CYCLE {
                    tokio::time::sleep(MIN_ENGINE_CYCLE - elapsed).await;
                }
                last_run = std::time::Instant::now();

                // Preserve panic isolation without scheduling a fresh Tokio task
                // for every market event.
                let t_prices = prices.clone();
                let t_real_cache = real_cache.clone();
                let t_ts_cache = ts_cache.clone();
                let t_http = http.clone();
                let t_signal = signal.clone();
                let t_journal = journal.clone();
                let t_keys = keys.clone();
                let t_e_ctx = e_ctx.clone();
                let t_profiles_db = profiles_db_engine.clone();
                let t_shadow_tx = shadow_tx.clone();

                let tick_result = std::panic::AssertUnwindSafe(async move {
                    let cycle_started = std::time::Instant::now();
                    // Read cached settings (no SQL, just RwLock read)
                    let real_profiles = t_real_cache.read().await.clone();
                    let ts = t_ts_cache.read().await.clone();

                    // Candidate discovery must start before the actual entry threshold so
                    // demand-driven L2 has time to subscribe and receive a fresh book.
                    // The original settings and profile thresholds are still passed to
                    // the scanner below, so this cannot authorize an early trade.
                    let mut discovery_ts = ts.clone();
                    discovery_ts.spread_threshold =
                        discovery_spread_threshold(ts.spread_threshold, &real_profiles);
                    let spreads = compute_spreads(&t_prices, 0.0, &discovery_ts);

                    // Writes to profiles_db since vps-api reads from it
                    // Replay must resolve the scanner's 500 ms confirmation rule.
                    // Keep the public API snapshot at one hertz, but retain candidate
                    // books at 100 ms while they exist. `try_send` and the one-slot
                    // writer queue keep this passive recorder out of the trading path.
                    static LAST_SHADOW: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(0);
                    static LAST_SNAP: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(0);
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    if now_ms.saturating_sub(
                        LAST_SHADOW.load(std::sync::atomic::Ordering::Relaxed),
                    ) > 100
                    {
                        LAST_SHADOW.store(now_ms, std::sync::atomic::Ordering::Relaxed);
                        let shadow_rows = engine::shadow::capture(
                            &spreads,
                            &t_e_ctx,
                            now_ms as i64,
                            ts.default_fee_rate,
                        );
                        if !shadow_rows.is_empty() {
                            let _ = t_shadow_tx.try_send(shadow_rows);
                        }
                    }
                    if now_ms.saturating_sub(LAST_SNAP.load(std::sync::atomic::Ordering::Relaxed))
                        > 1000
                    {
                        LAST_SNAP.store(now_ms, std::sync::atomic::Ordering::Relaxed);
                        let snapshot_spreads: Vec<_> = spreads.iter().take(500).cloned().collect();
                        let snapshot_val =
                            serde_json::json!({ "ts": now_ms as i64, "data": snapshot_spreads })
                                .to_string();
                        let cache_db = t_profiles_db.clone();
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


                    // ── UPDATE L2 WANTED SET (demand-driven subscriptions) ──
                    {
                        t_e_ctx.l2_wanted.clear();
                        for key in t_e_ctx.owned_l2_wanted.read().await.iter() {t_e_ctx.l2_wanted.insert(key.clone());}
                        // L2 is expensive on Binance. Follow only routes which are close
                        // to an active profile's entry window; open trades above remain
                        // mandatory regardless of rank. The collector retains candidates
                        // briefly, so a single L1 wobble cannot churn subscriptions.
                        let mut candidate_rows = 0usize;
                        let mut candidate_preflight_scheduled = false;
                        let mut candidate_depth_prewarm_scheduled = false;
                        for row in &spreads {
                            let key_a = format!("{}:{}", row.exchange_a, row.pair);
                            let key_b = format!("{}:{}", row.exchange_b, row.pair);
                            // Discovery feeds can contain same-named indices or
                            // TradFi contracts which are not the crypto contract
                            // available on the other venue. Only warm routes for
                            // which both venues published executable metadata.
                            if [&key_a, &key_b].iter().any(|key| {
                                !t_e_ctx.specifications.contains_key(*key)
                                    || !t_e_ctx.multipliers.contains_key(*key)
                                    || !t_e_ctx.tick_sizes.contains_key(*key)
                            }) {
                                continue;
                            }
                            let pair_cooldown = t_e_ctx
                                .pair_cooldown
                                .get(&row.pair)
                                .map(|value| *value)
                                .unwrap_or(0);
                            if engine::utils::cooldown_active(
                                pair_cooldown,
                                now_ms as i64,
                                ts.pair_cooldown_ms,
                            ) {
                                continue;
                            }
                            let wanted_profile = real_profiles.iter().find(|profile| {
                                profile.is_active
                                    && profile.strategy_type == "ema"
                                    && !profile.disabled_exchanges.contains(&row.exchange_a)
                                    && !profile.disabled_exchanges.contains(&row.exchange_b)
                                    && row.spread
                                        >= profile.min_entry_spread * DISCOVERY_PREWARM_RATIO
                                    && row.spread <= profile.max_entry_spread
                                    && row.vol_a >= profile.min_volume_millions * 1_000_000.0
                                    && row.vol_b >= profile.min_volume_millions * 1_000_000.0
                                    && engine::scanner_common::directional_structural_regime_is_stable(
                                        row.structural_verified,
                                        row.has_structural_spread,
                                        row.structural_min,
                                        row.structural_max,
                                    )
                                    && engine::scanner_common::configured_structural_reserve(
                                        profile.min_structural_deviation,
                                        row.structural_verified,
                                        row.structural_spread,
                                    )
                                    .is_some_and(|structural_reserve| {
                                        let fee_a = engine::utils::get_exchange_fee(
                                            &row.exchange_a,
                                            &row.pair,
                                            ts.default_fee_rate,
                                            &t_e_ctx,
                                        );
                                        let fee_b = engine::utils::get_exchange_fee(
                                            &row.exchange_b,
                                            &row.pair,
                                            ts.default_fee_rate,
                                            &t_e_ctx,
                                        );
                                        let break_even_pct =
                                            engine::scanner_common::ioc_break_even_pct(
                                                fee_a, fee_b,
                                            );
                                        engine::scanner_common::prewarm_execution_budget(
                                            row.spread,
                                            profile.min_entry_spread,
                                            profile.max_entry_spread,
                                            structural_reserve,
                                            profile.min_structural_deviation,
                                            break_even_pct,
                                            DISCOVERY_PREWARM_RATIO,
                                        )
                                        .is_some()
                                    })
                            });
                            let Some(wanted_profile) = wanted_profile else {
                                continue;
                            };
                            t_e_ctx.l2_wanted.insert(format!("{}:{}", row.exchange_a, row.pair));
                            t_e_ctx.l2_wanted.insert(format!("{}:{}", row.exchange_b, row.pair));
                            if !candidate_depth_prewarm_scheduled
                                && (row.exchange_a == "binance" || row.exchange_b == "binance")
                            {
                                candidate_depth_prewarm_scheduled =
                                    engine::managed::prewarm_binance_execution_book(
                                        &row.pair,
                                        t_prices.clone(),
                                        t_signal.clone(),
                                        t_http.clone(),
                                        t_e_ctx.clone(),
                                    );
                            }
                            if !candidate_preflight_scheduled {
                                candidate_preflight_scheduled = engine::managed::prewarm_candidate(
                                    &row.exchange_a,
                                    &row.exchange_b,
                                    &row.pair,
                                    wanted_profile.leverage as f64,
                                    t_http.clone(),
                                    t_keys.clone(),
                                    t_e_ctx.clone(),
                                );
                            }
                            candidate_rows += 1;
                            if candidate_rows >= 24 {
                                break;
                            }
                        }
                    }

                    // ── EVALUATE SMART SIGNALS & RUN ENGINE ──
                    let signals = crate::engine::scanner::generate_signals(
                        &spreads,
                        &t_prices,
                        &real_profiles,
                        &t_keys,
                        &t_e_ctx,
                        &ts,
                    );

                    engine::managed::propose(signals, real_profiles, t_prices, t_http, t_keys, t_journal, t_e_ctx.clone(), ts);
                    t_e_ctx.scanner_status.insert(
                        "engine_cycle_us".to_string(),
                        cycle_started.elapsed().as_micros().min(u64::MAX as u128) as u64,
                    );
                }).catch_unwind().await;

                if let Err(panic) = tick_result {
                    let msg = match panic.downcast::<String>() {
                        Ok(s) => *s,
                        Err(panic) => match panic.downcast::<&str>() {
                            Ok(s) => s.to_string(),
                            Err(_) => "unknown panic".to_string(),
                        },
                    };
                    tracing::error!("🔥 ENGINE LOOP PANIC (recovered, continuing): {}", msg);
                    // Small delay to avoid tight panic-loop
                    tokio::time::sleep(Duration::from_secs(1)).await;
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
    let app_state = (journal.clone(), engine_ctx.clone());
    let app = Router::new()
        .route("/health", get(handle_health))
        .route("/api/positions/close", post(handle_close_position))
        .with_state(app_state)
        .layer(cors);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
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
    State((journal, _ctx)): State<(engine::journal::Journal, Arc<state::EngineContext>)>,
    Json(req): Json<CloseReq>,
) -> Result<(axum::http::StatusCode, &'static str), (axum::http::StatusCode, String)> {
    let trades = journal
        .active()
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let trade = trades
        .iter()
        .find(|t| {
            let s = &t.plan.signal;
            engine::utils::symbols_match(&s.pair, &req.symbol)
                && (s.exchange_a == req.exchange || s.exchange_b == req.exchange)
        })
        .ok_or((
            axum::http::StatusCode::CONFLICT,
            "Position ownership is not established; exchange reconciliation required".into(),
        ))?;
    // Never trust a browser's cached position size. The supervisor closes both owned residuals.
    journal
        .request_close(trade.id)
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok((
        axum::http::StatusCode::ACCEPTED,
        "Close requested; execution is being reconciled",
    ))
}

async fn handle_health(
    State((journal, ctx)): State<(engine::journal::Journal, Arc<state::EngineContext>)>,
) -> (axum::http::StatusCode, Json<serde_json::Value>) {
    let now = engine::utils::now_ms();
    let supervisor_tick_ms = ctx
        .supervisor_tick_ms
        .load(std::sync::atomic::Ordering::Acquire);
    let age = now - supervisor_tick_ms;
    let active = journal.active().await;
    let process_healthy = active.is_ok()
        && engine::utils::local_timestamp_is_fresh(now, supervisor_tick_ms, 4_999);
    let mut market_ready_venues = 0usize;
    let venues: Vec<_> = ["binance", "kucoin"]
        .iter()
        .map(|ex| {
            let prefix = format!("{ex}:");
            let books = ctx
                .l2_books
                .iter()
                .filter(|b| b.key().starts_with(&prefix))
                .count();
            let fresh = ctx
                .l2_books
                .iter()
                .filter(|b| {
                    b.key().starts_with(&prefix)
                        && engine::utils::local_timestamp_is_fresh(now, b.updated_ms, 750)
                })
                .count();
            let specs = ctx
                .specifications
                .iter()
                .filter(|s| {
                    s.key().starts_with(&prefix)
                        && engine::utils::local_timestamp_is_fresh(now, s.fetched_ms, 600_000)
                })
                .count();
            let wanted_books = ctx
                .l2_wanted
                .iter()
                .filter(|key| key.starts_with(&prefix))
                .count();
            if fresh > 0 && specs > 0 {
                market_ready_venues += 1;
            }
            let balance_age_ms = ctx.balances_updated_ms.get(*ex).map(|stamp| now - *stamp);
            let positions_age_ms = ctx
                .positions_updated_ms
                .get(*ex)
                .map(|stamp| now - *stamp);
            serde_json::json!({"exchange":ex,"books":books,"fresh_books":fresh,"wanted_books":wanted_books,"current_specs":specs,"balance_age_ms":balance_age_ms,"positions_age_ms":positions_age_ms})
        })
        .collect();
    let market_ready = market_ready_venues >= 2;
    let private_ready_venues = ["binance", "kucoin"]
        .iter()
        .filter(|ex| {
            let balance_fresh = ctx
                .balances_updated_ms
                .get(**ex)
                .map(|stamp| {
                    engine::utils::local_timestamp_is_fresh(
                        now,
                        *stamp,
                        engine::risk_manager::PRIVATE_SNAPSHOT_MAX_AGE_MS,
                    )
                })
                .unwrap_or(false);
            let positions_fresh = ctx
                .positions_updated_ms
                .get(**ex)
                .map(|stamp| {
                    engine::utils::local_timestamp_is_fresh(
                        now,
                        *stamp,
                        engine::risk_manager::PRIVATE_SNAPSHOT_MAX_AGE_MS,
                    )
                })
                .unwrap_or(false);
            balance_fresh && positions_fresh
        })
        .count();
    let private_ready = private_ready_venues >= 2;
    let order_ws_ready = engine::exchanges::ws_trader::order_lane_ready()
        && engine::exchanges::ws_kucoin_order::is_ready();
    let healthy = process_healthy && market_ready;
    let trading_ready = healthy
        && private_ready
        && order_ws_ready
        && !ctx.is_killed.load(std::sync::atomic::Ordering::Acquire);
    let entry_decisions: std::collections::BTreeMap<String, u64> = ctx
        .entry_decisions
        .iter()
        .map(|entry| (entry.key().clone(), *entry.value()))
        .collect();
    let scanner_status: std::collections::BTreeMap<String, u64> = ctx
        .scanner_status
        .iter()
        .map(|entry| (entry.key().clone(), *entry.value()))
        .collect();
    (
        if healthy {
            axum::http::StatusCode::OK
        } else {
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        },
        Json(
            serde_json::json!({"venues":venues,"healthy":healthy,"process_healthy":process_healthy,"market_ready":market_ready,"private_ready":private_ready,"order_ws_ready":order_ws_ready,"trading_ready":trading_ready,"build_sha":option_env!("BUILD_SHA").unwrap_or("unknown"),"entries_halted":ctx.is_killed.load(std::sync::atomic::Ordering::Acquire),"supervisor_age_ms":age,"active_trades":active.ok().map(|v|v.len()),"scanner_status":scanner_status,"entry_decisions":entry_decisions}),
        ),
    )
}
