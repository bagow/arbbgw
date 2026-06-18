//! Startup helpers — extracted from main.rs to keep it focused on orchestration.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio_rusqlite::Connection;
use tracing::info;

use crate::settings::TradingSettings;
use crate::state::{EngineContext, PriceMap, UpdateSignal};

/// Spawn all exchange WS collectors. Each collector runs in its own tokio task
/// and updates the shared PriceMap + notifies the engine loop via UpdateSignal.
///
/// Replaces 70 lines of 10× copy-paste blocks in main.rs with a single macro call.
macro_rules! spawn_collector {
    ($mod:path, $prices:expr, $signal:expr, $e_ctx:expr, $settings:expr) => {{
        let p = $prices.clone();
        let s = $signal.clone();
        let l2 = $e_ctx.l2_books.clone();
        let l2w = $e_ctx.l2_wanted.clone();
        let sc = $settings.clone();
        tokio::spawn(async move { $mod(p, s, l2, l2w, sc).await });
    }};
}

pub fn spawn_collectors(
    prices: &PriceMap,
    signal: &UpdateSignal,
    e_ctx: &Arc<EngineContext>,
    settings_cache: &Arc<RwLock<Vec<TradingSettings>>>,
) {
    spawn_collector!(crate::collector::gate::run, prices, signal, e_ctx, settings_cache);
    spawn_collector!(crate::collector::kucoin::run, prices, signal, e_ctx, settings_cache);
    spawn_collector!(crate::collector::binance::run, prices, signal, e_ctx, settings_cache);
    spawn_collector!(crate::collector::bybit::run, prices, signal, e_ctx, settings_cache);
    spawn_collector!(crate::collector::bitget::run, prices, signal, e_ctx, settings_cache);
    spawn_collector!(crate::collector::bingx::run, prices, signal, e_ctx, settings_cache);
    spawn_collector!(crate::collector::bitmex::run, prices, signal, e_ctx, settings_cache);
    spawn_collector!(crate::collector::ascendex::run, prices, signal, e_ctx, settings_cache);
    spawn_collector!(crate::collector::xt::run, prices, signal, e_ctx, settings_cache);
    spawn_collector!(crate::collector::toobit::run, prices, signal, e_ctx, settings_cache);
}

/// Pre-warm HTTP connections to exchange APIs.
/// Establishes DNS + TCP + TLS ahead of time so the first trade doesn't
/// pay a 200-500ms cold-start penalty for handshake.
pub fn prewarm_http(http: reqwest::Client) {
    tokio::spawn(async move {
        let endpoints = [
            "https://fapi.binance.com/fapi/v1/time",
            "https://api.bybit.com/v5/market/time",
            "https://api.gateio.ws/api/v4/futures/usdt/contracts",
            "https://api-futures.kucoin.com/api/v1/timestamp",
        ];
        for url in endpoints {
            // Skip Binance if banned (any request extends the ban)
            if url.contains("binance") && crate::engine::exchanges::binance::is_binance_banned() {
                continue;
            }
            let _ = http.get(url).timeout(Duration::from_secs(5)).send().await;
        }
        info!(
            "HTTP connections pre-warmed to {} exchanges",
            endpoints.len()
        );
    });
}

/// Spawn background task that publishes exchange_maps snapshot to profiles DB
/// every 30 seconds, including balances, positions, and fees.
pub fn spawn_exchange_maps_publisher(
    prices: PriceMap,
    cache_db: Connection,
    e_ctx: Arc<EngineContext>,
) {
    tokio::spawn(async move {
        // Wait just 5s for collectors to connect, then publish immediately
        let mut first = true;
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            if first {
                tokio::time::sleep(Duration::from_secs(5)).await;
                first = false;
            } else {
                interval.tick().await;
            }
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let exchanges_list: Vec<String> =
                prices.iter().map(|e| e.key().clone()).collect();
            let sym_map: HashMap<String, HashSet<String>> = prices
                .iter()
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
                    if ex_a == ex_b {
                        continue;
                    }
                    let syms_b = sym_map.get(ex_b).cloned().unwrap_or_default();
                    let n = syms_a.intersection(&syms_b).count();
                    if n > 0 {
                        vs.insert(format!("vs {}", ex_b), serde_json::json!(n));
                    }
                }
                pairs_map.insert(ex_a.clone(), serde_json::Value::Object(vs));
            }

            // Collect real balances + fee rates from engine caches
            let mut balances: HashMap<String, f64> = HashMap::new();
            let mut margin_balances: HashMap<String, f64> = HashMap::new();
            let mut positions: HashMap<
                String,
                Vec<crate::engine::exchanges::traits::PositionInfo>,
            > = HashMap::new();
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
                "snapshot_ts": now_ms,
                "balances_ts": now_ms,
                "positions_ts": now_ms,
                "fees_ts": now_ms,
                "data": pairs_map,
                "balances": balances,
                "margin_balances": margin_balances,
                "positions": positions,
                "fees": fees
            })
            .to_string();
            let n = exchanges_list.len();
            let _ = cache_db.call(move |conn| {
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

/// Initialize trade database schema and run migrations.
pub async fn init_trades_db(db: &Connection) -> anyhow::Result<()> {
    db.call(|conn| {
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        // 1. Create table if not exists
        conn.execute_batch(
            "
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
                close_attempts        INTEGER DEFAULT 0,
                last_close_attempt_at INTEGER,
                close_error           TEXT,
                profile_id            TEXT DEFAULT 'real-default',
                order_id_a            TEXT,
                order_id_b            TEXT,
                contracts_a           INTEGER,
                contracts_b           INTEGER
            );
        ",
        )?;
        // 2. Create decision_snapshots table for replay logging
        let _ = conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS decision_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER,
                pair TEXT,
                exchange_a TEXT,
                exchange_b TEXT,
                snapshot_json TEXT
            );
        ",
        )?;
        // 3. Migrate older DBs (errors ignored — columns might already exist)
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN gross_pnl REAL", []);
        let _ = conn.execute(
            "ALTER TABLE trades ADD COLUMN profile_id TEXT DEFAULT 'real-default'",
            [],
        );
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN slippage_cost REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN order_id_a TEXT", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN order_id_b TEXT", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN contracts_a INTEGER", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN contracts_b INTEGER", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN last_price_a REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN last_price_b REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN last_spread REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN curr_gross_pnl REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN pnl_a REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN pnl_b REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN close_reason TEXT", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN close_attempts INTEGER DEFAULT 0", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN last_close_attempt_at INTEGER", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN close_error TEXT", []);
        // Funding rate tracking columns
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN funding_rate_a REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN funding_rate_b REAL", []);
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN funding_next_ts INTEGER", []);
        // Structural spread at entry — used by exit_manager for realistic convergence target
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN structural_spread REAL DEFAULT 0.0", []);
        // Separate exit slippage tracking (entry slippage stays in slippage_cost)
        let _ = conn.execute("ALTER TABLE trades ADD COLUMN exit_slippage_cost REAL DEFAULT 0.0", []);
        // 4. Create indexes AFTER ensuring columns exist
        let _ = conn.execute_batch(
            "
            CREATE INDEX IF NOT EXISTS idx_trades_status  ON trades(status);
            CREATE INDEX IF NOT EXISTS idx_trades_profile ON trades(profile_id);
        ",
        );
        Ok(())
    })
    .await?;
    Ok(())
}
