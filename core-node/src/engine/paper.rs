//! Paper trading engine — runs in-process on each WS tick.
//! No real orders placed; simulates execution against market mid-prices.

use std::time::Duration;
use rusqlite::params;
use tokio_rusqlite::Connection;
use tracing::info;

use crate::settings::TradingSettings;
use shared::SpreadRow;

#[derive(Debug, Clone)]
struct Trade {
    id: i64,
    pair: String,
    exchange_a: String,
    exchange_b: String,
    direction: String,
    volume: f64,
    entry_price_a: f64,
    entry_price_b: f64,
    entry_spread: f64,
    highest_spread: f64,
    pnl_a: f64,
    pnl_b: f64,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default().as_millis() as i64
}

/// Called by engine loop on every WS tick for the paper profile.
/// Reads DB, closes qualifying trades, opens new ones.
pub async fn run_cycle(spreads: Vec<SpreadRow>, settings: TradingSettings, _http: reqwest::Client, db: Connection) {
    if !settings.is_active { return; }

    // Load open trades for this profile
    let trades = load_open_trades(&db).await;
    let num_active = trades.len() as i64;

    // Build lookup for current prices
    let spread_map: std::collections::HashMap<String, &SpreadRow> =
        spreads.iter().map(|r| (r.pair.clone(), r)).collect();

    let commission = settings.commission_rate / 100.0;
    let funding    = settings.funding_rate_8h / 100.0;
    let tp_pct     = settings.take_profit_percent / 100.0;
    let stop_pct   = settings.trailing_stop_percent / 100.0;

    // ── Close qualifying trade ───────────────────────────────────────────────
    for trade in &trades {
        let is_a_higher = trade.direction == format!("{}>{}",trade.exchange_a, trade.exchange_b);
        let row = match spread_map.get(&trade.pair) { Some(r) => r, None => continue };

        let c_price_a = if is_a_higher { row.bid_a } else { row.ask_a };
        let c_price_b = if is_a_higher { row.ask_b } else { row.bid_b };
        let contracts = trade.volume / trade.entry_price_a.max(0.001);

        let pnl_a = if is_a_higher { (trade.entry_price_a - c_price_a) * contracts } else { (c_price_a - trade.entry_price_a) * contracts };
        let pnl_b = if is_a_higher { (c_price_b - trade.entry_price_b) * contracts } else { (trade.entry_price_b - c_price_b) * contracts };
        let gross_pnl = pnl_a + pnl_b;
        let costs     = 4.0 * commission * trade.volume + funding * trade.volume;
        let net_pnl   = gross_pnl - costs;
        let net_pct   = net_pnl / trade.volume * 100.0;

        let current_spread = (row.bid_a - row.ask_b).abs() / row.ask_b.max(0.001) * 100.0;
        let top_spread = trade.highest_spread.max(current_spread);

        let triggered_tp   = net_pct >= settings.take_profit_percent;
        let triggered_stop = top_spread > settings.trailing_stop_percent && current_spread < top_spread * (1.0 - stop_pct);
        let zero_spread    = current_spread <= 0.0;

        if triggered_tp || triggered_stop || zero_spread {
            let reason = if triggered_tp { "TP" } else if triggered_stop { "STOP" } else { "ZERO_SPREAD" };
            info!("[paper] Closing {} {} reason={} net={:.4}", trade.id, trade.pair, reason, net_pnl);
            let id = trade.id; let np = net_pnl; let gp = gross_pnl; let cs = current_spread;
            let cpa = c_price_a; let cpb = c_price_b; let pa = pnl_a; let pb = pnl_b;
            let reason_s = reason.to_string();
            let _ = db.call(move |conn| {
                conn.execute("UPDATE trades SET status='CLOSED',exit_price_a=?,exit_price_b=?,exit_spread=?,exit_time=?,pnl=?,gross_pnl=?,pnl_a=?,pnl_b=?,curr_gross_pnl=?,close_reason=? WHERE id=?",
                    params![cpa, cpb, cs, now_ms(), np, gp, pa, pb, gp, reason_s, id]
                ).map_err(tokio_rusqlite::Error::Rusqlite)
            }).await;
        } else if top_spread > trade.highest_spread {
            let id = trade.id;
            let _ = db.call(move |conn| {
                conn.execute("UPDATE trades SET highest_spread_reached=? WHERE id=?", params![top_spread, id])
                    .map_err(tokio_rusqlite::Error::Rusqlite)
            }).await;
        }
    }

    // ── Open new paper trades ─────────────────────────────────────────────────
    if num_active >= settings.max_active_trades { return; }

    let open_pairs: std::collections::HashSet<String> = trades.iter().map(|t| t.pair.clone()).collect();

    for row in spreads.iter().take(50) {
        if num_active + 1 > settings.max_active_trades { break; }
        if row.spread < settings.min_entry_spread || row.spread > settings.max_entry_spread { continue; }
        if open_pairs.contains(&row.pair) { continue; }
        if settings.disabled_exchanges.contains(&row.exchange_a) ||
           settings.disabled_exchanges.contains(&row.exchange_b) { continue; }
        if row.vol_a < settings.min_volume_millions * 1_000_000.0 { continue; }
        if row.vol_b < settings.min_volume_millions * 1_000_000.0 { continue; }

        let is_a_higher = row.direction.starts_with(&row.exchange_a);
        let entry_a = if is_a_higher { row.bid_a } else { row.ask_a };
        let entry_b = if is_a_higher { row.ask_b } else { row.bid_b };

        info!("[paper] Opening {} {} {} spread={:.4}%", row.pair, row.exchange_a, row.exchange_b, row.spread);

        let pair = row.pair.clone(); let exa = row.exchange_a.clone(); let exb = row.exchange_b.clone();
        let dir  = row.direction.clone(); let vol = settings.trade_volume; let sp = row.spread;
        let _ = db.call(move |conn| {
            conn.execute(
                "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,highest_spread_reached,status,pnl_a,pnl_b,close_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![pair, exa, exb, dir, vol, entry_a, entry_b, sp, now_ms(), -9999.0, "OPEN", 0.0, 0.0, None::<String>]
            ).map_err(tokio_rusqlite::Error::Rusqlite)
        }).await;
        break; // one trade per cycle to avoid opening multiple on same tick
    }
}

async fn load_open_trades(db: &Connection) -> Vec<Trade> {
    db.call(|conn| {
        let mut stmt = conn.prepare(
            "SELECT id,pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,highest_spread_reached,pnl_a,pnl_b FROM trades WHERE status='OPEN'"
        )?;
        let rows = stmt.query_map([], |row| Ok(Trade {
            id: row.get(0)?, pair: row.get(1)?,
            exchange_a: row.get(2)?, exchange_b: row.get(3)?,
            direction: row.get(4)?, volume: row.get(5)?,
            entry_price_a: row.get(6)?, entry_price_b: row.get(7)?,
            entry_spread: row.get(8)?, highest_spread: row.get(9)?,
            pnl_a: row.get(10)?, pnl_b: row.get(11)?,
        }))?.filter_map(|r| r.ok()).collect();
        Ok(rows)
    }).await.unwrap_or_default()
}
