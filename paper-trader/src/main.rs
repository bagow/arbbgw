//! paper-trader — Standalone paper trading engine
//!
//! HTTP endpoints:
//!   POST /internal/spreads              ← receives spreads from Cloudflare Worker
//!   GET  /api/trading/trades            ← active trades + stats (closed served from D1 by Worker)
//!   DELETE /api/trading/trades          ← clear all open trades
//!
//! The engine:
//!   - Reads settings from Cloudflare Worker GET /api/trading/settings
//!   - Processes SpreadRows against open SQLite trades
//!   - On trade close → archives to Cloudflare D1 via POST /api/archive

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{
    extract::{State, Json},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Router,
};
use reqwest::Client;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;
use tokio_rusqlite::Connection;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info, warn};

// ─── Types ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerData {
    pub bid: f64,
    pub ask: f64,
    pub price: f64,
    pub vol: f64,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SpreadRow {
    pub pair: String,
    #[serde(rename = "exchangeA")]
    pub exchange_a: String,
    #[serde(rename = "exchangeB")]
    pub exchange_b: String,
    #[serde(rename = "priceA")]
    pub price_a: f64,
    #[serde(rename = "priceB")]
    pub price_b: f64,
    #[serde(rename = "bidA")]
    pub bid_a: f64,
    #[serde(rename = "askA")]
    pub ask_a: f64,
    #[serde(rename = "bidB")]
    pub bid_b: f64,
    #[serde(rename = "askB")]
    pub ask_b: f64,
    #[serde(rename = "volA", default)]
    pub vol_a: f64,
    #[serde(rename = "volB", default)]
    pub vol_b: f64,
    #[serde(rename = "fundingA", default)]
    pub funding_a: f64,
    #[serde(rename = "fundingB", default)]
    pub funding_b: f64,
    pub spread: f64,
    pub direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadPayload {
    pub spreads: Vec<SpreadRow>,
    pub maps: std::collections::HashMap<String, std::collections::HashMap<String, TickerData>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingSettings {
    pub id: String,
    pub is_active: bool,
    pub min_entry_spread: f64,
    pub max_entry_spread: f64,
    pub trade_volume: f64,
    pub take_profit_percent: f64,
    pub trailing_stop_percent: f64,
    pub max_active_trades: i64,
    pub commission_rate: f64,
    pub slippage_percent: f64,
    pub funding_rate_8h: f64,
    pub min_volume_millions: f64,
    pub collector_interval_secs: i64,
    pub pending_commands: Option<String>,
}


impl Default for TradingSettings {
    fn default() -> Self {
        Self {
            id: "paper-default".into(),
            is_active: false,
            min_entry_spread: 0.3,
            max_entry_spread: 5.0,
            trade_volume: 1000.0,
            take_profit_percent: 0.1,
            trailing_stop_percent: 0.5,
            max_active_trades: 5,
            commission_rate: 0.45,
            slippage_percent: 0.1,
            funding_rate_8h: 0.01,
            min_volume_millions: 10.0,
            collector_interval_secs: 5,
            pending_commands: None,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: i64,
    pub pair: String,
    pub exchange_a: String,
    pub exchange_b: String,
    pub direction: String,
    pub volume: f64,
    pub entry_price_a: f64,
    pub entry_price_b: f64,
    pub entry_spread: f64,
    pub entry_time: i64,
    pub highest_spread_reached: f64,
    pub status: String,
    pub exit_price_a: Option<f64>,
    pub exit_price_b: Option<f64>,
    pub exit_spread: Option<f64>,
    pub exit_time: Option<i64>,
    pub pnl: Option<f64>,
    pub commission_cost: Option<f64>,
    pub funding_cost: Option<f64>,
    pub last_price_a: Option<f64>,
    pub last_price_b: Option<f64>,
    pub last_spread: Option<f64>,
    pub curr_gross_pnl: Option<f64>,
    pub pnl_a: Option<f64>,
    pub pnl_b: Option<f64>,
    pub close_reason: Option<String>,
    pub profile_id: Option<String>,
    // enriched fields (not stored in DB directly)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_price_a: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_price_b: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_spread: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_pnl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gross_pnl: Option<f64>,
}

// ─── App state ────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    db: Connection,
    http: Client,
    worker_url: String,
    collector_token: String,
    internal_token: String,
    engine_lock: Arc<Mutex<()>>,
    /// Cached settings to reduce Worker load (Array of profiles)
    settings_cache: Arc<Mutex<(Option<Vec<TradingSettings>>, std::time::Instant)>>,
    /// Throttling for active trades push
    last_push_time: Arc<Mutex<std::time::Instant>>,
}

// ─── DB init ──────────────────────────────────────────────────────────────────

async fn init_db(db: &Connection) -> Result<()> {
    db.call(|conn| {
        conn.execute_batch("
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                exchange_a TEXT NOT NULL,
                exchange_b TEXT NOT NULL,
                direction TEXT NOT NULL,
                volume REAL NOT NULL,
                entry_price_a REAL NOT NULL,
                entry_price_b REAL NOT NULL,
                entry_spread REAL NOT NULL,
                entry_time INTEGER NOT NULL,
                highest_spread_reached REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                exit_price_a REAL,
                exit_price_b REAL,
                exit_spread REAL,
                exit_time INTEGER,
                pnl REAL,
                commission_cost REAL,
                funding_cost REAL,
                last_price_a REAL,
                last_price_b REAL,
                last_spread REAL,
                curr_gross_pnl REAL,
                pnl_a REAL,
                pnl_b REAL,
                close_reason TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
        ").map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
    }).await?;

    let _ = db.call(|conn| {
        conn.execute(
            "ALTER TABLE trades ADD COLUMN profile_id TEXT NOT NULL DEFAULT 'paper-default'",
            [],
        )
        .map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
    })
    .await;

    Ok(())
}

// ─── Read Trade from row ──────────────────────────────────────────────────────

fn row_to_trade(row: &rusqlite::Row<'_>) -> rusqlite::Result<Trade> {
    Ok(Trade {
        id: row.get(0)?,
        pair: row.get(1)?,
        exchange_a: row.get(2)?,
        exchange_b: row.get(3)?,
        direction: row.get(4)?,
        volume: row.get(5)?,
        entry_price_a: row.get(6)?,
        entry_price_b: row.get(7)?,
        entry_spread: row.get(8)?,
        entry_time: row.get(9)?,
        highest_spread_reached: row.get(10)?,
        status: row.get(11)?,
        exit_price_a: row.get(12)?,
        exit_price_b: row.get(13)?,
        exit_spread: row.get(14)?,
        exit_time: row.get(15)?,
        pnl: row.get(16)?,
        commission_cost: row.get(17)?,
        funding_cost: row.get(18)?,
        last_price_a: row.get(19)?,
        last_price_b: row.get(20)?,
        last_spread: row.get(21)?,
        curr_gross_pnl: row.get(22)?,
        pnl_a: row.get(23).ok(),
        pnl_b: row.get(24).ok(),
        close_reason: row.get(25).ok(),
        profile_id: row.get::<_, Option<String>>(26).unwrap_or(Some("paper-default".into())),
        current_price_a: None,
        current_price_b: None,
        current_spread: None,
        current_pnl: None,
        gross_pnl: None,
    })
}

// ─── Trading Engine ───────────────────────────────────────────────────────────

async fn fetch_settings(state: &AppState) -> Vec<TradingSettings> {
    let settings = match state.db.call(|conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE mode='paper'")?;
        let rows = stmt.query_map([], |row| {
            Ok(TradingSettings {
                id: row.get(0)?,
                is_active: row.get::<_, i64>(3)? != 0,
                min_entry_spread: row.get(4)?,
                max_entry_spread: row.get(5)?,
                trade_volume: row.get(6)?,
                take_profit_percent: row.get(7)?,
                trailing_stop_percent: row.get(8)?,
                max_active_trades: row.get(9)?,
                commission_rate: row.get(10)?,
                slippage_percent: row.get(11)?,
                funding_rate_8h: row.get(12)?,
                min_volume_millions: row.get(13)?,
                collector_interval_secs: row.get(14)?,
                pending_commands: row.get(15)?,
            })
        })?;
        let items: Vec<TradingSettings> = rows.filter_map(|r| r.ok()).collect();
        Ok(items)
    }).await {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to fetch settings from DB: {} — using defaults", e);
            vec![TradingSettings::default()]
        }
    };

    // Process pending commands if any
    for s in &settings {
        if let Some(cmd) = &s.pending_commands {
            if cmd == "CLEAR_TRADES" {
                info!("Received CLEAR_TRADES command for profile {}, wiping local trades...", s.id);
                let pid = s.id.clone();
                let _ = state.db.call(move |conn| {
                    conn.execute("DELETE FROM trades WHERE profile_id=?", rusqlite::params![pid])?;
                    // Ack command
                    conn.execute("UPDATE trading_profiles SET pending_commands=NULL WHERE id=?", rusqlite::params![pid])?;
                    Ok(())
                }).await;
            }
        }
    }

    settings
}

// Network push functions deleted

async fn run_trading_engine(state: &AppState, payload: &SpreadPayload) {
    let _lock = state.engine_lock.lock().await;

    let settings_list = fetch_settings(state).await;
    let active_profiles: Vec<&TradingSettings> = settings_list.iter().filter(|s| s.is_active).collect();
    
    // Detailed Heartbeat for ALL profiles
    for s in &settings_list {
        info!("PROFILE_STATUS: id={} active={} min_sp={:.2} vol={} max_avg={}", 
            s.id, s.is_active, s.min_entry_spread, s.trade_volume, s.max_active_trades);
    }
    
    if active_profiles.is_empty() {
        return;
    }
    
    let settings_map: std::collections::HashMap<String, &TradingSettings> = 
        active_profiles.iter().map(|s| (s.id.clone(), *s)).collect();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // --- Load open trades ---
    let open_trades: Vec<Trade> = match state
        .db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id,pair,exchange_a,exchange_b,direction,volume,
                        entry_price_a,entry_price_b,entry_spread,entry_time,
                        highest_spread_reached,status,
                        exit_price_a,exit_price_b,exit_spread,exit_time,pnl,
                        commission_cost,funding_cost,
                        last_price_a,last_price_b,last_spread,curr_gross_pnl,
                        pnl_a,pnl_b,close_reason,profile_id
                 FROM trades WHERE status='OPEN'",
            )?;
            let rows = stmt
                .query_map([], row_to_trade)?
                .filter_map(|r| r.ok())
                .collect::<Vec<_>>();
            Ok(rows)
        })
        .await
    {
        Ok(t) => t,
        Err(e) => {
            error!("DB error loading trades: {}", e);
            return;
        }
    };

    let mut num_active_by_profile: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut open_pair_keys_by_profile: std::collections::HashMap<String, std::collections::HashSet<String>> = std::collections::HashMap::new();
    let mut open_tickers_by_profile: std::collections::HashMap<String, std::collections::HashSet<String>> = std::collections::HashMap::new();

    for t in &open_trades {
        let pid = t.profile_id.clone().unwrap_or_else(|| "paper-default".into());
        *num_active_by_profile.entry(pid.clone()).or_insert(0) += 1;
        let key = format!("{}_{}_{}", t.pair, t.exchange_a, t.exchange_b);
        open_pair_keys_by_profile.entry(pid.clone()).or_default().insert(key);
        open_tickers_by_profile.entry(pid.clone()).or_default().insert(t.pair.clone());
    }

    let maps = &payload.maps;

    // Helper: look up TickerData from maps
    let get_ticker = |exchange: &str, pair: &str| -> Option<&TickerData> {
        maps.get(exchange)?.get(pair)
    };

    // --- Process open trades ---
    for trade in &open_trades {
        let profile_id = trade.profile_id.clone().unwrap_or_else(|| "paper-default".into());
        let settings = match settings_map.get(&profile_id) {
            Some(s) => s,
            None => continue,
        };

        let data_a = match get_ticker(&trade.exchange_a, &trade.pair) {
            Some(d) => d,
            None => continue,
        };
        let data_b = match get_ticker(&trade.exchange_b, &trade.pair) {
            Some(d) => d,
            None => continue,
        };

        let is_a_higher = trade.direction.starts_with(&trade.exchange_a);

        let execution_spread = if is_a_higher {
            if data_b.ask > 0.0 {
                (data_a.bid - data_b.ask) / data_b.ask * 100.0
            } else {
                0.0
            }
        } else {
            if data_a.ask > 0.0 {
                (data_b.bid - data_a.ask) / data_a.ask * 100.0
            } else {
                0.0
            }
        };

        // Exit spread defines the spread based on closing operations
        // If we shorted A, we buy back at Ask A. We longed B, we sell at Bid B.
        // The spread between Ask A and Bid B is what we care about for closing.
        let exit_spread = if is_a_higher {
            if data_b.bid > 0.0 {
                (data_a.ask - data_b.bid) / data_b.bid * 100.0
            } else {
                0.0
            }
        } else {
            if data_a.bid > 0.0 {
                (data_b.ask - data_a.bid) / data_a.bid * 100.0
            } else {
                0.0
            }
        };

        let mid_spread = if is_a_higher {
            (data_a.price - data_b.price) / data_b.price * 100.0
        } else {
            (data_b.price - data_a.price) / data_a.price * 100.0
        };

        let slippage = settings.slippage_percent / 100.0;
        let c_price_a = data_a.price;
        let c_price_b = data_b.price;

        let exit_price_a = if is_a_higher {
            data_a.ask * (1.0 + slippage) // Buy back short at ask + slippage
        } else {
            data_a.bid * (1.0 - slippage) // Sell long at bid - slippage
        };
        let exit_price_b = if is_a_higher {
            data_b.bid * (1.0 - slippage) // Sell long at bid - slippage
        } else {
            data_b.ask * (1.0 + slippage) // Buy back short at ask + slippage
        };

        let coins_a = trade.volume / trade.entry_price_a;
        let coins_b = trade.volume / trade.entry_price_b;

        let pnl_a = if is_a_higher {
            (trade.entry_price_a - exit_price_a) * coins_a
        } else {
            (exit_price_a - trade.entry_price_a) * coins_a
        };
        let pnl_b = if is_a_higher {
            (exit_price_b - trade.entry_price_b) * coins_b
        } else {
            (trade.entry_price_b - exit_price_b) * coins_b
        };
        let gross_pnl = pnl_a + pnl_b;

        let hours_open = (now - trade.entry_time) as f64 / 3_600_000.0;
        let comm_cost = trade.volume * (settings.commission_rate / 100.0) * 4.0;

        let funding_cost_a = trade.volume
            * (data_a.funding_rate)
            * (if !is_a_higher { 1.0 } else { -1.0 })
            * (hours_open / 8.0);
        let funding_cost_b = trade.volume
            * (data_b.funding_rate)
            * (if is_a_higher { 1.0 } else { -1.0 })
            * (hours_open / 8.0);
        let funding_cost = funding_cost_a + funding_cost_b;

        let net_pnl = gross_pnl - comm_cost - funding_cost;

        // Initialize best_pnl cleanly if it's the first check.
        let mut best_pnl = trade.highest_spread_reached;
        if best_pnl < -5000.0 {
            best_pnl = net_pnl;
        } else if net_pnl > best_pnl {
            best_pnl = net_pnl;
        }

        // Exit conditions
        let target_pnl_usd = trade.volume * (settings.take_profit_percent / 100.0);
        let stop_drop_usd  = trade.volume * (settings.trailing_stop_percent / 100.0);

        let triggered_tp   = net_pnl >= target_pnl_usd;
        let triggered_stop = best_pnl > 0.0 && (best_pnl - net_pnl) >= stop_drop_usd;

        // ZERO_SPREAD: close only when the exit spread has converged AND net_pnl is non-negative.
        // This prevents closing at a loss just because the raw spread crossed zero —
        // the actual fill prices (bid/ask + slippage + commission) may still be underwater.
        let triggered_zero_spread = exit_spread <= 0.0 && net_pnl >= 0.0;

        // Hard stop: close at any spread if position has been open too long and is losing
        // significantly (protects against stuck positions eating funding).
        // Uses trailing_stop as the max loss threshold when position never went positive.
        let triggered_hard_stop = best_pnl <= 0.0 && net_pnl <= -(stop_drop_usd);

        if triggered_tp || triggered_stop || triggered_zero_spread || triggered_hard_stop {
            // Close the trade
            let reason = if triggered_tp {
                "TP"
            } else if triggered_zero_spread {
                "ZERO_SPREAD"
            } else if triggered_stop {
                "STOP"
            } else {
                "HARD_STOP"
            };
            info!(
                "Closing trade {} {} — reason={} net_pnl={:.4}",
                trade.id, trade.pair, reason, net_pnl
            );

            let trade_id = trade.id;
            let ep_a = exit_price_a;
            let ep_b = exit_price_b;
            let cs = mid_spread; // Log mid_spread as the exit spread to match chart
            let cc = comm_cost;
            let fc = funding_cost;

            if let Err(e) = state
                .db
                .call(move |conn| {
                    conn.execute(
                        "UPDATE trades SET status='CLOSED',
                            exit_price_a=?,exit_price_b=?,exit_spread=?,exit_time=?,
                            pnl=?,commission_cost=?,funding_cost=?,highest_spread_reached=?,
                            pnl_a=?,pnl_b=?,close_reason=?
                         WHERE id=?",
                        params![ep_a, ep_b, cs, now, net_pnl, cc, fc, best_pnl, pnl_a, pnl_b, reason, trade_id],
                    ).map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
                })
                .await
            {
                error!("DB error closing trade {}: {}", trade.id, e);
                continue;
            }

            // Build full closed trade for archive
            let mut closed = trade.clone();
            closed.status = "CLOSED".to_string();
            closed.close_reason = Some(reason.to_string());
            closed.exit_price_a = Some(exit_price_a);
            closed.exit_price_b = Some(exit_price_b);
            closed.exit_spread = Some(mid_spread);
            closed.exit_time = Some(now);
            closed.pnl = Some(net_pnl);
            closed.pnl_a = Some(pnl_a);
            closed.pnl_b = Some(pnl_b);
            closed.commission_cost = Some(comm_cost);
            closed.funding_cost = Some(funding_cost);
            closed.highest_spread_reached = best_pnl;

            // SQLite already persists the trade! No archiving to D1 needed.

            if let Some(c) = num_active_by_profile.get_mut(&profile_id) { *c -= 1; }
            let key = format!("{}_{}_{}", trade.pair, trade.exchange_a, trade.exchange_b);
            if let Some(keys) = open_pair_keys_by_profile.get_mut(&profile_id) { keys.remove(&key); }
            if let Some(tickers) = open_tickers_by_profile.get_mut(&profile_id) { tickers.remove(&trade.pair); }
        } else {
            // Update live prices.
            // Store NET pnl in curr_gross_pnl so vps-api routes.rs (which reads this field
            // directly as current_pnl) shows the correct after-cost figure without double-counting.
            let trade_id = trade.id;
            if let Err(e) = state
                .db
                .call(move |conn| {
                    conn.execute(
                        "UPDATE trades SET highest_spread_reached=?, last_price_a=?, last_price_b=?, last_spread=?, curr_gross_pnl=?, pnl_a=?, pnl_b=? WHERE id=?",
                        params![best_pnl, c_price_a, c_price_b, mid_spread, net_pnl, pnl_a, pnl_b, trade_id],
                    ).map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
                })
                .await
            {
                error!("DB error updating trade {}: {}", trade.id, e);
            }
        }
    }

    // --- Open new trades ---
    for settings in active_profiles {
        let pid = &settings.id;
        let max_trades = settings.max_active_trades;

        // Break-even spread: the minimum spread needed to cover all round-trip costs.
        // 4 legs × commission + 2 legs × slippage (entry only; exit slippage is baked into exit price)
        let commission_pct = settings.commission_rate / 100.0;
        let slippage_pct   = settings.slippage_percent / 100.0;
        // Round-trip cost as a % of notional:
        //   4 commission legs + 2 slippage legs at entry (exit slippage is marginal)
        let break_even_pct = 4.0 * commission_pct + 2.0 * slippage_pct;

        for row in &payload.spreads {
            let num_active = *num_active_by_profile.get(pid).unwrap_or(&0);
            if num_active >= max_trades {
                if row == payload.spreads.first().unwrap() {
                    info!("SKIP_MATCH: Profile {} is FULL ({} trades)", pid, num_active);
                }
                break;
            }

            let spread_pct = row.spread / 100.0; // spread is already in %
            let spread_ok = row.spread >= settings.min_entry_spread;
            if !spread_ok || row.spread > settings.max_entry_spread {
                continue;
            }

            // ── Pre-trade profitability guard ──────────────────────────────
            // Only enter if the spread covers break-even + a minimum net profit buffer.
            // This prevents entering trades that are guaranteed losers even on perfect execution.
            let min_net_profit_pct = break_even_pct * 0.5; // require at least 50% margin over costs
            if spread_pct < break_even_pct + min_net_profit_pct {
                // Spread exists but is too thin to be profitable after costs
                continue;
            }

            info!("BOT SEES PAIR: {} [Tab={} Spread={:.3}% BreakEven={:.3}%]",
                row.pair, pid, row.spread, break_even_pct * 100.0);

            let tickers = open_tickers_by_profile.entry(pid.clone()).or_default();
            if tickers.contains(&row.pair) { continue; }

            let key = format!("{}_{}_{}", row.pair, row.exchange_a, row.exchange_b);
            let key_rev = format!("{}_{}_{}", row.pair, row.exchange_b, row.exchange_a);
            let keys = open_pair_keys_by_profile.entry(pid.clone()).or_default();
            if keys.contains(&key) || keys.contains(&key_rev) { continue; }

            info!(
                "MATCH: Profile={} Pair={} Spread={:.3}% BreakEven={:.3}% NetEdge={:.3}%",
                pid, row.pair, row.spread, break_even_pct * 100.0,
                (spread_pct - break_even_pct) * 100.0
            );

            let slippage = slippage_pct;
            let is_a_higher = row.direction.starts_with(&row.exchange_a);

            // Entry prices: use Bid/Ask directly (no additional slippage penalty on top —
            // slippage is already modelled by the Bid/Ask spread itself vs. mid price).
            // We only apply slippage as an extra haircut to model imperfect fills.
            let entry_price_a = if is_a_higher {
                row.bid_a * (1.0 - slippage) // SHORT A: sell at Bid, slippage = worse fill
            } else {
                row.ask_a * (1.0 + slippage) // LONG A: buy at Ask, slippage = worse fill
            };
            let entry_price_b = if is_a_higher {
                row.ask_b * (1.0 + slippage) // LONG B: buy at Ask, slippage = worse fill
            } else {
                row.bid_b * (1.0 - slippage) // SHORT B: sell at Bid, slippage = worse fill
            };

            let pair = row.pair.clone();
            let ex_a  = row.exchange_a.clone();
            let ex_b  = row.exchange_b.clone();
            let dir   = row.direction.clone();
            let vol   = settings.trade_volume;
            let spread = row.spread;
            let profile_id_db = pid.clone();
            let comm_at_open = vol * commission_pct * 4.0;

            info!("OPENING: Profile={} Pair={} Vol={} Spread={:.3}% Comm_est={:.4}",
                profile_id_db, pair, vol, spread, comm_at_open);

            match state
                .db
                .call(move |conn| {
                    conn.execute(
                        "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,
                                             entry_price_a,entry_price_b,entry_spread,entry_time,
                                             highest_spread_reached,status,pnl_a,pnl_b,close_reason,profile_id,commission_cost)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        params![
                            pair, ex_a, ex_b, dir, vol,
                            entry_price_a, entry_price_b, spread, now,
                            -9999.0, "OPEN", 0.0, 0.0, None::<String>, profile_id_db, comm_at_open
                        ],
                    ).map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
                })
                .await
            {
                Ok(_) => {
                    *num_active_by_profile.entry(pid.clone()).or_insert(0) += 1;
                    open_pair_keys_by_profile.entry(pid.clone()).or_default().insert(key);
                    open_tickers_by_profile.entry(pid.clone()).or_default().insert(row.pair.clone());
                }
                Err(e) => error!("DB error opening trade: {}", e),
            }
        }
    }

    // --- Final Step: Push current active state to Cloudflare Worker ---
    let final_trades: Vec<Trade> = match state
        .db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id,pair,exchange_a,exchange_b,direction,volume,
                        entry_price_a,entry_price_b,entry_spread,entry_time,
                        highest_spread_reached,status,
                        exit_price_a,exit_price_b,exit_spread,exit_time,pnl,
                        commission_cost,funding_cost,
                        last_price_a,last_price_b,last_spread,curr_gross_pnl,
                        pnl_a,pnl_b,close_reason,profile_id
                 FROM trades WHERE status='OPEN'",
            )?;
            let rows = stmt
                .query_map([], row_to_trade)?
                .filter_map(|r| r.ok())
                .collect::<Vec<_>>();
            Ok(rows)
        })
        .await
    {
        Ok(mut t) => {
            for tr in t.iter_mut() {
                if let (Some(c_price_a), Some(c_price_b), Some(c_spread)) = (tr.last_price_a, tr.last_price_b, tr.last_spread) {
                    let is_a_higher = tr.direction.starts_with(&tr.exchange_a);
                    let slippage = 0.001;
                    let ep_a = if is_a_higher { c_price_a * (1.0 + slippage) } else { c_price_a * (1.0 - slippage) };
                    let ep_b = if is_a_higher { c_price_b * (1.0 - slippage) } else { c_price_b * (1.0 + slippage) };
                    let coins_a = tr.volume / tr.entry_price_a;
                    let coins_b = tr.volume / tr.entry_price_b;
                    let pnl_a = if is_a_higher { (tr.entry_price_a - ep_a) * coins_a } else { (ep_a - tr.entry_price_a) * coins_a };
                    let pnl_b = if is_a_higher { (ep_b - tr.entry_price_b) * coins_b } else { (tr.entry_price_b - ep_b) * coins_b };
                    let gross = pnl_a + pnl_b;
                    // curr_gross_pnl now stores NET pnl (after comm+funding) written by engine loop.
                    // For the worker-push branch, fall back to gross - commission_cost.
                    let comm = tr.commission_cost.unwrap_or(0.0);
                    tr.current_price_a = Some(c_price_a);
                    tr.current_price_b = Some(c_price_b);
                    tr.current_spread = Some(c_spread);
                    tr.gross_pnl = Some(gross);
                    tr.current_pnl = Some(tr.curr_gross_pnl.unwrap_or(gross - comm));
                    tr.pnl_a = Some(pnl_a);
                    tr.pnl_b = Some(pnl_b);
                }
            }
            t
        }
        Err(_) => vec![],
    };

    let mut mapped_trades: std::collections::HashMap<String, Vec<Trade>> = std::collections::HashMap::new();
    for tr in final_trades {
        let pid = tr.profile_id.clone().unwrap_or_else(|| "paper-default".into());
        mapped_trades.entry(pid).or_default().push(tr);
    }
    
    // Ensure ALL active profiles are explicitly mentioned (for clearing zero-trades)
    for s in &settings_list {
        if s.is_active {
            mapped_trades.entry(s.id.clone()).or_insert_with(Vec::new);
        }
    }

    let http = state.http.clone();
    let worker_url = state.worker_url.clone();
    let token = state.collector_token.clone();
    
    let mut should_push = false;
    {
        let mut last_push = state.last_push_time.lock().await;
        if last_push.elapsed() > Duration::from_secs(15) || true {
            should_push = true;
            *last_push = std::time::Instant::now();
        }
    }
}

// ─── Axum helpers ─────────────────────────────────────────────────────────────

fn json_ok(data: impl Serialize) -> Response {
    (StatusCode::OK, axum::Json(data)).into_response()
}

fn json_err(status: StatusCode, msg: &str) -> Response {
    (status, axum::Json(json!({ "error": msg }))).into_response()
}

fn check_internal_token(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == format!("Bearer {}", expected))
        .unwrap_or(false)
}

// ─── Handlers ────────────────────────────────────────────────────────────────

/// POST /internal/spreads — called by Cloudflare Worker after each rust-collector push
async fn handle_internal_spreads(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SpreadPayload>,
) -> Response {
    if !check_internal_token(&headers, &state.internal_token) {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    // Run engine in the background so we don't block the Worker's waitUntil
    let spreads_count = payload.spreads.len();
    info!("Received {} spreads from rust-collector", spreads_count);
    let state2 = state.clone();
    tokio::spawn(async move {
        run_trading_engine(&state2, &payload).await;
    });

    json_ok(json!({ "ok": true, "spreads": spreads_count }))
}

/// GET /api/trading/trades — active trades enriched with current prices
async fn handle_get_trades(State(state): State<AppState>) -> Response {
    let trades = match state
        .db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id,pair,exchange_a,exchange_b,direction,volume,
                        entry_price_a,entry_price_b,entry_spread,entry_time,
                        highest_spread_reached,status,
                        exit_price_a,exit_price_b,exit_spread,exit_time,pnl,
                        commission_cost,funding_cost,
                        last_price_a,last_price_b,last_spread,curr_gross_pnl,
                        pnl_a,pnl_b,close_reason,profile_id
                 FROM trades WHERE status='OPEN' ORDER BY entry_time DESC",
            )?;
            let rows = stmt
                .query_map([], row_to_trade)?
                .filter_map(|r| r.ok())
                .collect::<Vec<_>>();
            Ok(rows)
        })
        .await
    {
        Ok(t) => t,
        Err(e) => {
            error!("DB error: {}", e);
            return json_err(StatusCode::INTERNAL_SERVER_ERROR, "DB error");
        }
    };

    // Enrich active trades with last known live prices from SQLite cache columns
    let mut active = trades;
    for t in active.iter_mut() {
        if let (Some(c_price_a), Some(c_price_b), Some(c_spread)) =
            (t.last_price_a, t.last_price_b, t.last_spread)
        {
            let is_a_higher = t.direction.starts_with(&t.exchange_a);
            let slippage = 0.001; // 0.1% default since we don't have settings here
            let ep_a = if is_a_higher { c_price_a * (1.0 + slippage) } else { c_price_a * (1.0 - slippage) };
            let ep_b = if is_a_higher { c_price_b * (1.0 - slippage) } else { c_price_b * (1.0 + slippage) };
            let coins_a = t.volume / t.entry_price_a;
            let coins_b = t.volume / t.entry_price_b;
            let pnl_a = if is_a_higher { (t.entry_price_a - ep_a) * coins_a } else { (ep_a - t.entry_price_a) * coins_a };
            let pnl_b = if is_a_higher { (ep_b - t.entry_price_b) * coins_b } else { (t.entry_price_b - ep_b) * coins_b };
            let gross = pnl_a + pnl_b;
            let comm = t.commission_cost.unwrap_or(0.0); // saved at open time with correct rate
            t.current_price_a = Some(c_price_a);
            t.current_price_b = Some(c_price_b);
            t.current_spread = Some(c_spread);
            t.gross_pnl = Some(gross);
            t.current_pnl = Some(t.curr_gross_pnl.unwrap_or(gross - comm));
        }
    }

    json_ok(json!({ "active": active }))
}

/// DELETE /api/trading/trades — wipe all open trades
async fn handle_delete_trades(State(state): State<AppState>) -> Response {
    match state
        .db
        .call(|conn| {
            conn.execute("DELETE FROM trades", [])
                .map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
        })
        .await
    {
        Ok(n) => json_ok(json!({ "success": true, "deleted": n })),
        Err(e) => json_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("DB error: {}", e),
        ),
    }
}

// ─── Config ───────────────────────────────────────────────────────────────────

struct Config {
    port: u16,
    internal_token: String,
    worker_url: String,
    collector_token: String,
    db_path: String,
}

impl Config {
    fn from_env() -> Self {
        Config {
            port: std::env::var("PAPER_TRADER_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8080),
            internal_token: std::env::var("INTERNAL_TOKEN").unwrap_or_default(),
            worker_url: std::env::var("WORKER_URL")
                .unwrap_or_else(|_| "https://cex-spread-scanner.workers.dev".into()),
            collector_token: std::env::var("COLLECTOR_TOKEN").unwrap_or_default(),
            db_path: std::env::var("DB_PATH").unwrap_or_else(|_| "./trades.db".into()),
        }
    }
}

// ─── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env();

    info!(
        "Starting paper-trader on port {} | db={}",
        cfg.port, cfg.db_path
    );
    if cfg.internal_token.is_empty() {
        warn!("INTERNAL_TOKEN is empty — /internal/spreads will reject all requests!");
    }

    let db = Connection::open(&cfg.db_path)
        .await
        .context("Failed to open SQLite")?;

    init_db(&db).await?;

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;

    let state = AppState {
        db,
        http,
        worker_url: cfg.worker_url,
        collector_token: cfg.collector_token,
        internal_token: cfg.internal_token,
        engine_lock: Arc::new(Mutex::new(())),
        settings_cache: Arc::new(Mutex::new((None, std::time::Instant::now()))),
        last_push_time: Arc::new(Mutex::new(std::time::Instant::now())),
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/internal/spreads", post(handle_internal_spreads))
        .route("/api/trading/trades", get(handle_get_trades))
        .route("/api/trading/trades", delete(handle_delete_trades))
        .layer(cors)
        .with_state(state);

    let addr: SocketAddr = format!("0.0.0.0:{}", cfg.port).parse()?;
    info!("Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
