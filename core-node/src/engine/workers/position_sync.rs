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
        // Wait for initial startup to settle
        tokio::time::sleep(Duration::from_secs(30)).await;

        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            // 1. Load ALL open trades (across all profiles)
            let open_trades: Vec<Trade> = db.call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, pair, status, exchange_a, exchange_b, direction, volume, \
                     entry_price_a, entry_price_b, entry_spread, entry_time, highest_spread_reached, \
                     pnl_a, pnl_b, close_reason, contracts_a, contracts_b, commission_cost, slippage_cost, \
                     COALESCE(funding_cost, 0.0), COALESCE(structural_spread, 0.0) \
                     FROM trades WHERE status IN ('OPEN','CLOSING')"
                )?;
                let trades = stmt.query_map([], |row| Ok(Trade {
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
                }))?.filter_map(|t| t.ok()).collect::<Vec<_>>();
                Ok(trades)
            }).await.unwrap_or_default();

            if open_trades.is_empty() {
                // No open trades — clear missing counter but DON'T clear positions cache.
                // Positions cache reflects what's actually on exchanges (managed by fetcher),
                // not what we track internally. Clearing it here causes flickering in UI
                // for positions that exist on the exchange but have no matching trade in DB.
                e_ctx.position_missing_counter.clear();
                continue;
            }

            // 2. Collect which exchanges have open trades
            let mut exchanges_in_use = std::collections::HashSet::new();
            for t in &open_trades {
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

            for trade in &open_trades {
                // Ignore newly opened trades because REST APIs on exchanges (like KuCoin/Binance) 
                // can aggressively cache positions, leading to false "missing position" detects.
                // 120s grace period accounts for worst-case REST API cache staleness.
                let age_ms = now_ms() - trade.entry_time;
                if age_ms < 120_000 {
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

                // BUG-S3 FIX: Only skip fresh CLOSING trades (< 5 min). Stale CLOSING trades
                // may be stuck (e.g., after panic in close spawn, or external liquidation).
                // For stale ones, fall through to position check so we can detect and recover.
                if trade.status == "CLOSING" && age_ms < 300_000 {
                    continue;
                }

                // CLOSING RECOVERY: If trade is stuck in CLOSING > 10 min AND both positions
                // still exist, re-trigger the close. This prevents trades hanging for hours.
                if trade.status == "CLOSING" && age_ms > 600_000 && has_pos_a && has_pos_b {
                    tracing::warn!(
                        "[pos_sync] Trade id={} {} stuck in CLOSING for {}s with BOTH positions alive. Reverting to OPEN for retry.",
                        trade.id, trade.pair, age_ms / 1000
                    );
                    let repo = crate::engine::db::TradeRepository::new(db.clone());
                    repo.revert_to_open(trade.id).await;
                    // Clear closing_in_flight so exit_manager can re-trigger the close
                    e_ctx.closing_in_flight.remove(&trade.id);
                    continue;
                }

                if trade.status == "CLOSING" {
                    tracing::warn!(
                        "[pos_sync] Trade id={} {} stuck in CLOSING for {}s, checking positions...",
                        trade.id, trade.pair, age_ms / 1000
                    );
                }

                if has_pos_a && has_pos_b {
                    confirmed_present.insert(trade.id);
                    continue; // Both positions exist, all normal
                }

                // ── CONFIRMATION COUNTER: Require 3 consecutive "missing" detections ──
                // This prevents false positives from API cache staleness or transient errors.
                let miss_count = {
                    let mut entry = e_ctx.position_missing_counter.entry(trade.id).or_insert(0);
                    *entry += 1;
                    *entry
                };

                if miss_count < 3 {
                    tracing::warn!(
                        "[pos_sync] Trade id={} {} — position missing detected ({}/3 confirmations). has_a={} has_b={} Waiting for more confirmations...",
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
                    let repo = crate::engine::db::TradeRepository::new(db.clone()); repo.emergency_close_trade(trade.id).await;
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
