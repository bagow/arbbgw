use reqwest::Client;
use std::sync::Arc;
use tokio_rusqlite::Connection;
use tracing::error;
use std::time::Duration;
use crate::engine::real::Trade;
use crate::engine::real::ExchangeKeys;
use crate::engine::real::now_ms;
use crate::engine::real::symbols_match;

pub fn start_position_sync(
    http: Client,
    keys: Arc<ExchangeKeys>,
    db: Connection,
    e_ctx: Arc<crate::state::EngineContext>,
) {
    tokio::spawn(async move {
        // Wait briefly for initial fetcher pass to populate e_ctx.positions
        tokio::time::sleep(Duration::from_secs(6)).await;

        loop {
            // 1. Reconcile untracked/orphan exchange positions into DB
            let open_trades_for_rec: Vec<Trade> = db.call(|conn| {
                let mut stmt = conn.prepare("SELECT id, pair, status, exchange_a, exchange_b, direction, volume, entry_price_a, entry_price_b, entry_spread, entry_time, highest_spread_reached, pnl_a, pnl_b, close_reason, contracts_a, contracts_b, commission_cost, slippage_cost, COALESCE(funding_cost,0.0), COALESCE(structural_spread,0.0) FROM trades WHERE status IN ('OPEN','CLOSING')")?;
                let list = stmt.query_map([], |row| {
                    Ok(Trade {
                        id: row.get(0)?,
                        pair: row.get(1)?,
                        status: row.get(2)?,
                        exchange_a: row.get(3)?,
                        exchange_b: row.get(4)?,
                        direction: row.get(5)?,
                        volume: row.get(6)?,
                        entry_price_a: row.get(7)?,
                        entry_price_b: row.get(8)?,
                        entry_spread: row.get(9)?,
                        entry_time: row.get(10)?,
                        highest_spread: row.get(11)?,
                        pnl_a: row.get(12)?,
                        pnl_b: row.get(13)?,
                        close_reason: row.get(14)?,
                        contracts_a: row.get::<_, Option<f64>>(15)?.unwrap_or(0.0),
                        contracts_b: row.get::<_, Option<f64>>(16)?.unwrap_or(0.0),
                        commission_cost: row.get::<_, Option<f64>>(17)?.unwrap_or(0.0),
                        slippage_cost: row.get::<_, Option<f64>>(18)?.unwrap_or(0.0),
                        funding_cost: row.get::<_, Option<f64>>(19)?.unwrap_or(0.0),
                        structural_spread: row.get::<_, Option<f64>>(20)?.unwrap_or(0.0),
                    })
                })?.filter_map(|r| r.ok()).collect::<Vec<_>>();
                Ok(list)
            }).await.unwrap_or_default();

            reconcile_untracked_positions(&http, &keys, &db, &e_ctx, &open_trades_for_rec).await;

            // 2. Load ALL open trades (across all profiles)
            // Load trades WITH last_close_attempt_at for BUG-3 race condition fix
            let open_trades_with_close_ts: Vec<(Trade, i64)> = db.call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, pair, status, exchange_a, exchange_b, direction, volume, \
                     entry_price_a, entry_price_b, entry_spread, entry_time, highest_spread_reached, \
                     pnl_a, pnl_b, close_reason, contracts_a, contracts_b, commission_cost, slippage_cost, \
                     COALESCE(funding_cost, 0.0), COALESCE(structural_spread, 0.0), \
                     COALESCE(last_close_attempt_at, 0) \
                     FROM trades WHERE status IN ('OPEN','CLOSING')"
                )?;
                let trades = stmt.query_map([], |row| {
                    let trade = Trade {
                        id: row.get(0)?,
                        pair: row.get(1)?,
                        status: row.get(2)?,
                        exchange_a: row.get(3)?,
                        exchange_b: row.get(4)?,
                        direction: row.get(5)?,
                        volume: row.get(6)?,
                        entry_price_a: row.get(7)?,
                        entry_price_b: row.get(8)?,
                        entry_spread: row.get(9)?,
                        entry_time: row.get(10)?,
                        highest_spread: row.get(11)?,
                        pnl_a: row.get(12)?,
                        pnl_b: row.get(13)?,
                        close_reason: row.get(14)?,
                        contracts_a: row.get::<_, Option<f64>>(15)?.unwrap_or(0.0),
                        contracts_b: row.get::<_, Option<f64>>(16)?.unwrap_or(0.0),
                        commission_cost: row.get::<_, Option<f64>>(17)?.unwrap_or(0.0),
                        slippage_cost: row.get::<_, Option<f64>>(18)?.unwrap_or(0.0),
                        funding_cost: row.get::<_, Option<f64>>(19)?.unwrap_or(0.0),
                        structural_spread: row.get::<_, Option<f64>>(20)?.unwrap_or(0.0),
                    };
                    let last_close_at: i64 = row.get::<_, Option<i64>>(21)?.unwrap_or(0);
                    Ok((trade, last_close_at))
                })?.filter_map(|t| t.ok()).collect::<Vec<_>>();
                Ok(trades)
            }).await.unwrap_or_default();

            if open_trades_with_close_ts.is_empty() {
                e_ctx.position_missing_counter.clear();
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }

            // 2. Collect which exchanges have open trades
            let mut exchanges_in_use = std::collections::HashSet::new();
            for (t, _) in &open_trades_with_close_ts {
                exchanges_in_use.insert(t.exchange_a.clone());
                exchanges_in_use.insert(t.exchange_b.clone());
            }

            // 3. Fetch positions from each exchange (1 API call per exchange)
            // Track which exchanges had fetch errors vs successful empty results
            let mut all_positions: std::collections::HashMap<
                String,
                Vec<crate::engine::exchanges::traits::PositionInfo>,
            > = std::collections::HashMap::new();
            let mut fetch_errors: std::collections::HashSet<String> = std::collections::HashSet::new();
            for ex in &exchanges_in_use {
                if !keys.supports(ex) {
                    continue;
                }
                if let Some(conn) = crate::engine::exchanges::get_connector(ex) {
                    match conn.fetch_positions(&http, &keys).await {
                        Ok(positions) => {
                            all_positions.insert(ex.clone(), positions);
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            fetch_errors.insert(ex.clone());
                            if msg.contains("banned") || msg.contains("ban") {
                                tracing::debug!("[pos_sync] {} positions skipped (banned): {}", ex, msg);
                            } else {
                                tracing::warn!("[pos_sync] Failed to fetch positions from {}: {}", ex, msg);
                            }
                        }
                    }
                }
            }

            // 4. Check each open trade against exchange positions
            // Collect trade IDs that are still confirmed present (both positions alive)
            let mut confirmed_present: std::collections::HashSet<i64> = std::collections::HashSet::new();

            for (trade, last_close_attempt_at) in &open_trades_with_close_ts {
                let now = now_ms();
                // Ignore newly opened trades because REST APIs on exchanges (like KuCoin/Binance) 
                // can aggressively cache positions, leading to false "missing position" detects.
                // 120s grace period accounts for worst-case REST API cache staleness.
                let age_ms = now - trade.entry_time;
                if age_ms < 120_000 {
                    continue;
                }

                // ── BUG-3 FIX: Check closing_in_flight BEFORE anything else ──
                // If exit_manager is actively closing this trade right now, NEVER
                // make any decisions — one leg may already be closed on the exchange
                // while the other is still in progress.
                if e_ctx.closing_in_flight.get(&trade.id).map(|v| *v).unwrap_or(false) {
                    tracing::debug!(
                        "[pos_sync] Skipping trade id={} {} — close is in-flight (exit_manager active)",
                        trade.id, trade.pair
                    );
                    continue;
                }

                // ── BUG-3 FIX pt2: Check last_close_attempt_at ──
                // Even if closing_in_flight was already cleared, if the last close attempt
                // was < 5 min ago, the exchange may still be processing. Skip.
                let close_attempt_age_ms = if *last_close_attempt_at > 0 { now - last_close_attempt_at } else { i64::MAX };
                if close_attempt_age_ms < 300_000 {
                    tracing::debug!(
                        "[pos_sync] Skipping trade id={} {} — last close attempt was {}s ago (< 5 min)",
                        trade.id, trade.pair, close_attempt_age_ms / 1000
                    );
                    continue;
                }

                // FIX: If either exchange returned a fetch error, do NOT make any decisions
                // about this trade's positions — we simply don't have reliable data.
                let a_had_error = fetch_errors.contains(&trade.exchange_a);
                let b_had_error = fetch_errors.contains(&trade.exchange_b);
                if a_had_error || b_had_error {
                    tracing::debug!(
                        "[pos_sync] Skipping trade id={} {} — exchange fetch error (a_err={} b_err={})",
                        trade.id, trade.pair, a_had_error, b_had_error
                    );
                    continue;
                }

                let has_pos_a = all_positions
                    .get(&trade.exchange_a)
                    .map(|ps| {
                        ps.iter()
                            .any(|p| symbols_match(&p.symbol, &trade.pair) && p.size.abs() > 1e-12)
                    })
                    .unwrap_or(true); // conservative: if can't check, assume exists
                let has_pos_b = all_positions
                    .get(&trade.exchange_b)
                    .map(|ps| {
                        ps.iter()
                            .any(|p| symbols_match(&p.symbol, &trade.pair) && p.size.abs() > 1e-12)
                    })
                    .unwrap_or(true); // conservative

                // CLOSING status handling — use last_close_attempt_at (NOT entry_time).
                // Old code used age_ms (from entry_time) which doesn't protect old trades.
                if trade.status == "CLOSING" {
                    // If close attempt is recent (< 5 min from DB), skip entirely.
                    // Already handled above via last_close_attempt_at check.
                    // This branch handles stuck CLOSING trades (> 10 min since close attempt).
                    if close_attempt_age_ms > 600_000 && has_pos_a && has_pos_b {
                        tracing::warn!(
                            "[pos_sync] Trade id={} {} stuck in CLOSING for {}s with BOTH positions alive. Reverting to OPEN for retry.",
                            trade.id, trade.pair, close_attempt_age_ms / 1000
                        );
                        let repo = crate::engine::db::TradeRepository::new(db.clone());
                        repo.revert_to_open(trade.id).await;
                        e_ctx.closing_in_flight.remove(&trade.id);
                        continue;
                    }
                    tracing::debug!(
                        "[pos_sync] Trade id={} {} in CLOSING state, close_attempt_age={}s, checking positions...",
                        trade.id, trade.pair, close_attempt_age_ms / 1000
                    );
                }

                if has_pos_a && has_pos_b {
                    confirmed_present.insert(trade.id);
                    continue; // Both positions exist, all normal
                }

                // ── BUG-4 FIX: Require 6 TRULY CONSECUTIVE "missing" detections ──
                // Track timestamp of last miss. If gap > 30s, reset counter (not consecutive).
                let miss_count = {
                    let mut entry = e_ctx.position_missing_counter.entry(trade.id).or_insert((0, now));
                    let (count, last_miss_ts) = entry.value_mut();
                    // Reset if previous miss was > 30s ago (not consecutive)
                    if now - *last_miss_ts > 30_000 {
                        *count = 0;
                    }
                    *count += 1;
                    *last_miss_ts = now;
                    *count
                };

                // Log ALL positions from the exchange for debugging
                if let Some(positions) = all_positions.get(&trade.exchange_a) {
                    if !has_pos_a {
                        let syms: Vec<String> = positions.iter().map(|p| format!("{}({:.4})", p.symbol, p.size)).collect();
                        tracing::warn!(
                            "[pos_sync] Trade id={} {} — {} has NO matching position! Exchange returned {} positions: [{}]",
                            trade.id, trade.pair, trade.exchange_a, positions.len(), syms.join(", ")
                        );
                    }
                }
                if let Some(positions) = all_positions.get(&trade.exchange_b) {
                    if !has_pos_b {
                        let syms: Vec<String> = positions.iter().map(|p| format!("{}({:.4})", p.symbol, p.size)).collect();
                        tracing::warn!(
                            "[pos_sync] Trade id={} {} — {} has NO matching position! Exchange returned {} positions: [{}]",
                            trade.id, trade.pair, trade.exchange_b, positions.len(), syms.join(", ")
                        );
                    }
                }

                if miss_count < 6 {
                    tracing::warn!(
                        "[pos_sync] Trade id={} {} — position missing detected ({}/6 confirmations). has_a={} has_b={} Waiting for more confirmations...",
                        trade.id, trade.pair, miss_count, has_pos_a, has_pos_b
                    );
                    continue;
                }

                // Reset counter since we're acting
                e_ctx.position_missing_counter.remove(&trade.id);

                if !has_pos_a && !has_pos_b {
                    // BOTH positions gone externally
                    error!(
                        "[pos_sync] Trade id={} {} — BOTH positions gone (confirmed {}x)! Marking EXTERNAL_CLOSE",
                        trade.id, trade.pair, miss_count
                    );
                    let repo = crate::engine::db::TradeRepository::new(db.clone());
                    repo.emergency_close_trade(trade.id).await;
                    continue;
                }

                // ONE leg is gone — emergency close the remaining leg
                let (missing_ex, remaining_ex) = if !has_pos_a {
                    (&trade.exchange_a, &trade.exchange_b)
                } else {
                    (&trade.exchange_b, &trade.exchange_a)
                };
                error!(
                    "[pos_sync] Trade id={} {} — {} gone (confirmed {}x), {} still open! Emergency closing",
                    trade.id, trade.pair, missing_ex, miss_count, remaining_ex
                );

                // FIX: DB stores direction as "binance>gate", NOT "A>B".
                // The exchange whose name comes FIRST in direction is the "higher" leg (we shorted it).
                let is_a_higher = trade.direction.starts_with(&trade.exchange_a);
                let remaining_is_a = remaining_ex == &trade.exchange_a;
                // If remaining leg is A and A was shorted (is_a_higher), close by BUYING.
                // If remaining leg is B and A was higher (B was longed), close by SELLING.
                let close_is_buy = if remaining_is_a {
                    is_a_higher  // A was shorted → buy to close
                } else {
                    !is_a_higher // B was longed → sell to close
                };
                let contracts = if remaining_is_a {
                    trade.contracts_a
                } else {
                    trade.contracts_b
                };
                let mult = e_ctx
                    .multipliers
                    .get(&format!("{}:{}", remaining_ex, trade.pair))
                    .map(|v| *v)
                    .unwrap_or(1.0);
                // FIX: close_position expects physical volume (coins), not raw contract count.
                // contracts * multiplier = physical coins.
                let q_qty = contracts * mult;
                let tick_for_close = e_ctx
                    .tick_sizes
                    .get(&format!("{}:{}", remaining_ex, trade.pair))
                    .map(|v| *v)
                    .unwrap_or(0.000001);

                if let Some(conn) = crate::engine::exchanges::get_connector(remaining_ex) {
                    match conn
                        .close_position(
                            &http,
                            &keys,
                            &trade.pair,
                            close_is_buy,
                            q_qty,
                            false,
                            10.0,
                            0.0,
                            false,
                            0.0,
                            mult,
                            tick_for_close,
                            5,
                            25,
                            5000,
                        )
                        .await
                    {
                        Ok(rc) => {
                            let close_fill = rc.final_price;
                            let close_fee = rc.total_fee;
                            error!("[pos_sync] Emergency close done id={} {} on {} fill={:.6} fee={:.4}",
                                trade.id, trade.pair, remaining_ex, close_fill, close_fee);
                            let total_fee = trade.commission_cost + close_fee; let repo = crate::engine::db::TradeRepository::new(db.clone()); repo.emergency_partial_close_trade(trade.id, total_fee).await;
                        }
                        Err(e) => {
                            error!(
                                "[pos_sync] Emergency close FAILED id={} {} on {}: {}",
                                trade.id, trade.pair, remaining_ex, e
                            );
                        }
                    }
                }
            }

            // Clear confirmation counters for trades where both positions are confirmed present
            for id in &confirmed_present {
                e_ctx.position_missing_counter.remove(id);
            }
        }
    });
}

/// Automatically detect untracked or orphan arbitrage positions on exchanges
/// (e.g. after crash, restart, or deploy) and restore them into the active trades DB
/// so exit_manager can monitor and close them according to trading settings.
/// If an untracked position is a single-legged unhedged orphan (no opposite leg anywhere),
/// automatically liquidate it to prevent directional market risk.
async fn reconcile_untracked_positions(
    http: &Client,
    keys: &Arc<ExchangeKeys>,
    db: &Connection,
    e_ctx: &crate::state::EngineContext,
    open_trades: &[Trade],
) {
    // 1. Gather all live exchange positions from e_ctx.positions
    let mut sym_to_pos: std::collections::HashMap<String, Vec<(String, crate::engine::exchanges::traits::PositionInfo)>> = std::collections::HashMap::new();

    for entry in e_ctx.positions.iter() {
        let ex = entry.key().clone();
        for pos in entry.value() {
            if pos.size.abs() > 1e-12 {
                let norm = pos.symbol.replace('_', "").replace('-', "").to_uppercase();
                let clean_sym = if norm.ends_with("USDTM") {
                    norm.replace("USDTM", "USDT")
                } else {
                    norm
                };
                if clean_sym.ends_with("USDT") {
                    sym_to_pos.entry(clean_sym).or_default().push((ex.clone(), pos.clone()));
                }
            }
        }
    }

    if sym_to_pos.is_empty() {
        return;
    }

    // 2. Identify untracked positions and reconstruct arbitrage pairs
    let repo = crate::engine::db::TradeRepository::new(db.clone());

    for (sym, pos_list) in sym_to_pos {
        let is_tracked = open_trades.iter().any(|t| symbols_match(&t.pair, &sym));
        if is_tracked {
            continue;
        }

        let shorts: Vec<_> = pos_list.iter().filter(|(_, p)| p.size < 0.0).collect();
        let longs: Vec<_> = pos_list.iter().filter(|(_, p)| p.size > 0.0).collect();

        if let (Some((ex_short, pos_short)), Some((ex_long, pos_long))) = (shorts.first(), longs.first()) {
            if ex_short != ex_long {
                let now = now_ms();
                let dir = format!("{}>{}", ex_short, ex_long);
                let entry_a = if pos_short.entry_price > 0.0 { pos_short.entry_price } else { 1.0 };
                let entry_b = if pos_long.entry_price > 0.0 { pos_long.entry_price } else { 1.0 };
                let spread = ((entry_a - entry_b) / entry_b) * 100.0;
                let vol_usd = (pos_short.size.abs() * entry_a).max(pos_long.size.abs() * entry_b);

                tracing::info!(
                    "[reconcile] 🛡️ AUTO-RECOVERED open arbitrage position for {}: {} (SHORT {:.4} @ {:.6}) + {} (LONG {:.4} @ {:.6}) | Vol: ${:.2}. Restoring to active trades DB.",
                    sym, ex_short, pos_short.size.abs(), entry_a, ex_long, pos_long.size.abs(), entry_b, vol_usd
                );

                // Estimate entry commission from known taker fee rates (both legs)
                let fee_a = e_ctx.fee_rates.get(ex_short.as_str()).map(|v| *v).unwrap_or(0.0006);
                let fee_b = e_ctx.fee_rates.get(ex_long.as_str()).map(|v| *v).unwrap_or(0.0006);
                let estimated_entry_comm = vol_usd * (fee_a + fee_b);

                repo.insert_new_entry(
                    sym.clone(),
                    ex_short.clone(),
                    ex_long.clone(),
                    dir,
                    vol_usd,
                    entry_a,
                    entry_b,
                    spread,
                    now,
                    "real-default".to_string(),
                    estimated_entry_comm,
                    0.0,
                    String::new(),
                    String::new(),
                    pos_short.size.abs(),
                    pos_long.size.abs(),
                    0.0,
                ).await;
                continue;
            }
        }

        // UNHEDGED ORPHAN POSITION: No opposite leg exists on another exchange.
        // Confirm across multiple cycles before liquidating to avoid closing in-flight entries.
        for (ex, pos) in &pos_list {
            if pos.size.abs() > 1e-12 {
                let orphan_key = format!("{}:{}:{}", ex, sym, if pos.size > 0.0 { "LONG" } else { "SHORT" });
                let now = now_ms();
                let count = {
                    let mut entry = e_ctx.orphan_confirm_counter.entry(orphan_key.clone()).or_insert((0, now));
                    let (c, last_ts) = entry.value_mut();
                    if now - *last_ts > 30_000 {
                        *c = 0;
                    }
                    *c += 1;
                    *last_ts = now;
                    *c
                };

                if count >= 4 {
                    // Confirmed unhedged orphan for >= 4 cycles (40 seconds). Auto-close it!
                    tracing::error!(
                        "[pos_sync] 🧹 AUTO-LIQUIDATING unhedged orphan position on {}: {} {} size={:.4} (confirmed {}x)",
                        ex, if pos.size > 0.0 { "LONG" } else { "SHORT" }, sym, pos.size.abs(), count
                    );
                    e_ctx.orphan_confirm_counter.remove(&orphan_key);

                    let http_c = http.clone();
                    let keys_c = keys.clone();
                    let ex_c = ex.clone();
                    let sym_c = sym.clone();
                    let is_buy = pos.size < 0.0;
                    let qty = pos.size.abs();
                    let mult = e_ctx.multipliers.get(&format!("{}:{}", ex, sym)).map(|v| *v).unwrap_or(1.0);
                    let tick = e_ctx.tick_sizes.get(&format!("{}:{}", ex, sym)).map(|v| *v).unwrap_or(0.000001);

                    tokio::spawn(async move {
                        if let Some(conn) = crate::engine::exchanges::get_connector(&ex_c) {
                            match conn.close_position(
                                &http_c, &keys_c, &sym_c, is_buy, qty * mult,
                                false, 10.0, 0.0, false, 0.0, mult, tick, 5, 50, 5000
                            ).await {
                                Ok(rc) => {
                                    tracing::info!("[pos_sync] ✅ Orphan closed on {}: {} fill={:.6} fee={:.4}", ex_c, sym_c, rc.final_price, rc.total_fee);
                                }
                                Err(e) => {
                                    tracing::error!("[pos_sync] ❌ Orphan close FAILED on {}: {}: {}", ex_c, sym_c, e);
                                }
                            }
                        }
                    });
                } else {
                    tracing::warn!(
                        "[pos_sync] ⚠️ Unhedged orphan detected on {}: {} {} size={:.4} ({}/4 confirmations)",
                        ex, if pos.size > 0.0 { "LONG" } else { "SHORT" }, sym, pos.size.abs(), count
                    );
                }
            }
        }
    }
}
