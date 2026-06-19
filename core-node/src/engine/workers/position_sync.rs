use reqwest::Client;
use std::sync::Arc;
use tokio_rusqlite::Connection;
use tracing::error;
use std::time::Duration;
use crate::engine::real::Trade;
use crate::engine::real::ExchangeKeys;
use crate::engine::real::now_ms;
use crate::engine::real::symbols_match;
use dashmap::DashMap;
use std::sync::OnceLock;

/// Tracks orphan position confirmations: key = "exchange:symbol" → (consecutive_count, last_seen_ms)
static ORPHAN_COUNTER: OnceLock<DashMap<String, (u32, i64)>> = OnceLock::new();
fn orphan_counter() -> &'static DashMap<String, (u32, i64)> {
    ORPHAN_COUNTER.get_or_init(|| DashMap::new())
}
const ORPHAN_CONFIRM_REQUIRED: u32 = 6; // 6 × 10s = 60s of sustained orphan

pub fn start_position_sync(
    http: Client,
    keys: Arc<ExchangeKeys>,
    db: Connection,
    e_ctx: Arc<crate::state::EngineContext>,
) {
    tokio::spawn(async move {
        // Wait for initial startup to settle
        tokio::time::sleep(Duration::from_secs(30)).await;

        let all_exchanges = [
            "binance", "gate", "kucoin", "bybit", "bitget",
            "bingx", "bitmex", "ascendex", "xt", "toobit",
        ];

        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            // 1. Load ALL open trades (across all profiles)
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
                // No open trades — clear missing counter but DON'T clear positions cache.
                // Positions cache reflects what's actually on exchanges (managed by fetcher),
                // not what we track internally. Clearing it here causes flickering in UI
                // for positions that exist on the exchange but have no matching trade in DB.
                e_ctx.position_missing_counter.clear();
                // Still run orphan detection below (there may be orphans with zero open trades)
            }

            // 2. Collect which exchanges have open trades
            let mut exchanges_in_use = std::collections::HashSet::new();
            for (t, _) in &open_trades_with_close_ts {
                exchanges_in_use.insert(t.exchange_a.clone());
                exchanges_in_use.insert(t.exchange_b.clone());
            }

            // 3. Fetch positions from each exchange (1 API call per exchange)
            // Track which exchanges had fetch errors vs successful empty results
            // For orphan detection: fetch ALL exchanges, not just ones in use
            let mut all_positions: std::collections::HashMap<
                String,
                Vec<crate::engine::exchanges::traits::PositionInfo>,
            > = std::collections::HashMap::new();
            let mut fetch_errors: std::collections::HashSet<String> = std::collections::HashSet::new();

            // Fetch from exchanges that have open trades (for trade-sync)
            // AND from all supported exchanges (for orphan detection)
            let exchanges_to_fetch: std::collections::HashSet<String> = {
                let mut set = exchanges_in_use.clone();
                for ex in &all_exchanges {
                    if keys.supports(ex) {
                        set.insert(ex.to_string());
                    }
                }
                set
            };

            for ex in &exchanges_to_fetch {
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

            if !open_trades_with_close_ts.is_empty() {
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
            } // end if !open_trades_with_close_ts.is_empty()

            // ═══════════════════════════════════════════════════════════════════
            // 5. ORPHAN POSITION DETECTOR
            // ═══════════════════════════════════════════════════════════════════
            // Compare ALL exchange positions against ALL open trades.
            // Any position that doesn't match ANY open trade is an orphan (ghost).
            // This catches positions left behind by failed trim_position calls
            // during network outages, or by ASYMMETRIC_CLOSE with failed cleanup.
            //
            // Safety: require 6 consecutive confirmations (60s) before closing.
            // This avoids race conditions with trades being opened/closed.

            // Build set of (exchange, normalized_symbol) pairs that ARE expected
            let mut expected_positions: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
            for (trade, _) in &open_trades_with_close_ts {
                expected_positions.insert((trade.exchange_a.clone(), trade.pair.to_uppercase()));
                expected_positions.insert((trade.exchange_b.clone(), trade.pair.to_uppercase()));
            }

            let now = now_ms();
            let counter = orphan_counter();
            let mut seen_orphan_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

            for (ex, positions) in &all_positions {
                // Skip exchanges with fetch errors — can't trust the data
                if fetch_errors.contains(ex) {
                    continue;
                }

                for pos in positions {
                    if pos.size.abs() < 1e-12 {
                        continue;
                    }

                    // Normalize symbol for matching
                    let sym_upper = pos.symbol.to_uppercase();

                    // Check if this position matches any open trade
                    let is_expected = expected_positions.iter().any(|(exp_ex, exp_sym)| {
                        exp_ex == ex && symbols_match(&sym_upper, exp_sym)
                    });

                    if is_expected {
                        // Position belongs to an open trade — not orphan
                        let key = format!("{}:{}", ex, sym_upper);
                        counter.remove(&key);
                        continue;
                    }

                    // This position has NO matching open trade — potential orphan
                    let key = format!("{}:{}", ex, sym_upper);
                    seen_orphan_keys.insert(key.clone());

                    let mut entry = counter.entry(key.clone()).or_insert((0, now));
                    let (count, last_ts) = entry.value_mut();
                    // Reset if last detection was >30s ago (not consecutive)
                    if now - *last_ts > 30_000 {
                        *count = 0;
                    }
                    *count += 1;
                    *last_ts = now;
                    let current_count = *count;
                    drop(entry);

                    if current_count < ORPHAN_CONFIRM_REQUIRED {
                        tracing::warn!(
                            "[pos_sync] 👻 ORPHAN DETECTED: {} on {} size={:.6} ({}/{} confirmations)",
                            pos.symbol, ex, pos.size, current_count, ORPHAN_CONFIRM_REQUIRED
                        );
                        continue;
                    }

                    // Confirmed orphan — close it
                    counter.remove(&key);
                    error!(
                        "[pos_sync] 👻 ORPHAN CONFIRMED ({}x): {} on {} size={:.6} — AUTO-CLOSING",
                        current_count, pos.symbol, ex, pos.size
                    );

                    // Determine close direction: if size > 0 (long), sell to close; if < 0 (short), buy to close
                    let close_is_buy = pos.size < 0.0;
                    let phys_qty = pos.size.abs();
                    let mult = e_ctx
                        .multipliers
                        .get(&format!("{}:{}", ex, sym_upper))
                        .map(|v| *v)
                        .unwrap_or(1.0);
                    let tick = e_ctx
                        .tick_sizes
                        .get(&format!("{}:{}", ex, sym_upper))
                        .map(|v| *v)
                        .unwrap_or(0.000001);

                    if let Some(conn) = crate::engine::exchanges::get_connector(ex) {
                        match conn
                            .close_position(
                                &http, &keys, &pos.symbol,
                                close_is_buy, phys_qty, false,
                                10.0, 0.0, false, 0.0,
                                mult, tick, 5, 25, 5000,
                            )
                            .await
                        {
                            Ok(rc) => {
                                error!(
                                    "[pos_sync] 👻 ORPHAN CLOSED: {} on {} fill={:.6} fee={:.4}",
                                    pos.symbol, ex, rc.final_price, rc.total_fee
                                );
                            }
                            Err(e) => {
                                error!(
                                    "[pos_sync] 👻 ORPHAN CLOSE FAILED: {} on {}: {}",
                                    pos.symbol, ex, e
                                );
                            }
                        }
                    }
                }
            }

            // Clean up stale orphan counter entries (not seen this cycle)
            let keys_to_remove: Vec<String> = counter
                .iter()
                .filter(|e| !seen_orphan_keys.contains(e.key()))
                .map(|e| e.key().clone())
                .collect();
            for k in keys_to_remove {
                counter.remove(&k);
            }
        }
    });
}

