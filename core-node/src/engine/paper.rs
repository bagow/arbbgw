//! Paper trading engine — runs in-process on each WS tick.
//! No real orders placed; simulates execution against market mid-prices.

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
    entry_time: i64,
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
pub async fn run_cycle(spreads: Vec<SpreadRow>, prices: crate::state::PriceMap, settings: TradingSettings, _http: reqwest::Client, db: Connection) {
    if !settings.is_active { return; }
    let profile_id = settings.id.clone();

    let trades = load_open_trades(&db, &profile_id).await;
    let num_active = trades.len() as i64;

    let commission = settings.commission_rate / 100.0;
    let funding    = settings.funding_rate_8h / 100.0;
    let tp_pct     = settings.take_profit_percent / 100.0;
    let stop_pct   = settings.trailing_stop_percent / 100.0;

    // Close qualifying trades
    let mut live_updates = Vec::new();
    for trade in &trades {
        let is_a_higher = trade.direction.starts_with(&trade.exchange_a);

        let data_a = match prices.get(&trade.exchange_a).and_then(|m| m.get(&trade.pair).cloned()) {
            Some(d) => d,
            None => continue,
        };
        let data_b = match prices.get(&trade.exchange_b).and_then(|m| m.get(&trade.pair).cloned()) {
            Some(d) => d,
            None => continue,
        };

        if data_a.bid <= 0.0 || data_a.ask <= 0.0 || data_b.bid <= 0.0 || data_b.ask <= 0.0 { 
            continue; 
        }

        // We apply a minimal slippage factor here if needed, but let's stick to base bids/asks.
        let slippage = settings.slippage_percent / 100.0;
        let c_price_a = data_a.price;
        let c_price_b = data_b.price;

        let exit_price_a = if is_a_higher { data_a.ask } else { data_a.bid };
        let exit_price_b = if is_a_higher { data_b.bid } else { data_b.ask };

        let contracts = trade.volume / trade.entry_price_a.max(1e-12);

        let pnl_a = if is_a_higher { (trade.entry_price_a - exit_price_a) * contracts } else { (exit_price_a - trade.entry_price_a) * contracts };
        let pnl_b = if is_a_higher { (exit_price_b - trade.entry_price_b) * contracts } else { (trade.entry_price_b - exit_price_b) * contracts };
        
        let gross_pnl = pnl_a + pnl_b;
        let comm_cost = 4.0 * commission * trade.volume;
        let fund_cost = funding * trade.volume;
        let slip_cost = 4.0 * slippage * trade.volume;
        let net_pnl   = gross_pnl - (comm_cost + fund_cost + slip_cost);
        let net_pct   = net_pnl / trade.volume * 100.0;

        let current_spread = if is_a_higher {
            (data_a.price - data_b.price) / data_b.price.max(1e-12) * 100.0
        } else {
            (data_b.price - data_a.price) / data_a.price.max(1e-12) * 100.0
        };

        let exit_spread = if is_a_higher {
            (data_a.ask - data_b.bid) / data_b.bid.max(1e-12) * 100.0
        } else {
            (data_b.ask - data_a.bid) / data_a.bid.max(1e-12) * 100.0
        };

        let top_spread = trade.highest_spread.max(current_spread);

        let triggered_tp   = net_pct >= tp_pct * 100.0;
        let stop_limit     = top_spread - settings.trailing_stop_percent;
        let triggered_stop = top_spread > 0.0 && current_spread < stop_limit;
        let zero_spread    = exit_spread <= 0.0 && net_pnl >= 0.0;


        if triggered_tp || triggered_stop || zero_spread {
            let reason = if triggered_tp { "TP" } else if zero_spread { "ZERO_SPREAD" } else { "STOP" };
            info!("[paper] Closing {} {} reason={} net={:.4} exit_sp={:.4} curr_sp={:.4} top_sp={:.4} stop_limit={:.4} gross={:.4}", 
                trade.id, trade.pair, reason, net_pnl, exit_spread, current_spread, top_spread, stop_limit, gross_pnl);

            let id = trade.id; let np = net_pnl; let gp = gross_pnl; let cs = current_spread;
            let cpa = exit_price_a; let cpb = exit_price_b; let pa = pnl_a; let pb = pnl_b;
            let reason_s = reason.to_string();
            let _ = db.call(move |conn| {
                conn.execute("UPDATE trades SET status='CLOSED',exit_price_a=?,exit_price_b=?,exit_spread=?,exit_time=?,pnl=?,gross_pnl=?,pnl_a=?,pnl_b=?,curr_gross_pnl=?,close_reason=?,commission_cost=?,funding_cost=?,slippage_cost=? WHERE id=?",
                    params![cpa, cpb, cs, now_ms(), np, gp, pa, pb, gp, reason_s, comm_cost, fund_cost, slip_cost, id]
                )?;
                Ok(())
            }).await;
        } else {
            // Did not close, track live PnL for UI
            live_updates.push((trade.id, c_price_a, c_price_b, current_spread, net_pnl));

            if top_spread > trade.highest_spread {
                let id = trade.id;
                let _ = db.call(move |conn| {
                    conn.execute("UPDATE trades SET highest_spread_reached=? WHERE id=?", params![top_spread, id])?;
                    Ok(())
                }).await;
            }
        }
    }

    static LAST_LIVE_FLUSH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let current_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    // Flush live PnL to DB every 1000ms per profile
    let elapsed = current_ms.saturating_sub(LAST_LIVE_FLUSH.load(std::sync::atomic::Ordering::Relaxed));
    if elapsed > 1000 && !live_updates.is_empty() {
        LAST_LIVE_FLUSH.store(current_ms, std::sync::atomic::Ordering::Relaxed);
        let db_clone = db.clone();
        tokio::spawn(async move {
            let _ = db_clone.call(move |conn| {
                let tx = conn.transaction()?;
                {
                    let mut stmt = tx.prepare_cached("UPDATE trades SET last_price_a=?, last_price_b=?, last_spread=?, curr_gross_pnl=? WHERE id=?")?;
                    for (id, pa, pb, cs, pnl) in live_updates {
                        stmt.execute(params![pa, pb, cs, pnl, id])?;
                    }
                }
                tx.commit()?;
                Ok::<(), tokio_rusqlite::Error>(())
            }).await;
        });
    }

    // ── Open new paper trades ─────────────────────────────────────────────────
    if num_active >= settings.max_active_trades { return; }

    let open_pairs: std::collections::HashSet<String> = trades.iter()
        .map(|t| format!("{}|{}|{}", t.pair, t.exchange_a, t.exchange_b))
        .collect();

    for row in spreads.iter().take(50) {
        if num_active + 1 > settings.max_active_trades { break; }
        if row.spread < settings.min_entry_spread || row.spread > settings.max_entry_spread { continue; }
        // Skip if already have an open trade for this exact exchange pair
        let key = format!("{}|{}|{}", row.pair, row.exchange_a, row.exchange_b);
        if open_pairs.contains(&key) { continue; }
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
        let profile_id = settings.id.clone();
        let commission = settings.commission_rate / 100.0;
        let slippage = settings.slippage_percent / 100.0;
        let comm_cost = 4.0 * commission * vol;
        let slip_cost = 4.0 * slippage * vol;

        let _ = db.call(move |conn| {
            conn.execute(
                "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,highest_spread_reached,status,pnl_a,pnl_b,close_reason,profile_id,commission_cost,slippage_cost,funding_cost) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![pair, exa, exb, dir, vol, entry_a, entry_b, sp, now_ms(), sp, "OPEN", 0.0, 0.0, None::<String>, profile_id, comm_cost, slip_cost, 0.0]
            )?;
            Ok(())
        }).await;
        break;
    }
}

async fn load_open_trades(db: &Connection, profile_id: &str) -> Vec<Trade> {
    let pid = profile_id.to_string();
    db.call(move |conn| {
        let mut stmt = conn.prepare(
            "SELECT id,pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,highest_spread_reached,pnl_a,pnl_b FROM trades WHERE status='OPEN' AND profile_id=?"
        )?;
        let rows = stmt.query_map([&pid], |row| Ok(Trade {
            id: row.get(0)?, pair: row.get(1)?,
            exchange_a: row.get(2)?, exchange_b: row.get(3)?,
            direction: row.get(4)?, volume: row.get(5)?,
            entry_price_a: row.get(6)?, entry_price_b: row.get(7)?,
            entry_spread: row.get(8)?,
            entry_time: row.get(9)?,
            highest_spread: row.get(10)?,
            pnl_a: row.get(11)?, pnl_b: row.get(12)?,
        }))?.filter_map(|r| r.ok()).collect();
        Ok(rows)
    }).await.unwrap_or_default()
}
