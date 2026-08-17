use crate::engine::db::TradeRepository;
use reqwest::Client;
use std::sync::Arc;
use tokio_rusqlite::Connection;
use tracing::{error, info};
use shared::{TechnicalSettings, TradingSettings};
use crate::engine::pnl::*;

use crate::engine::real::*;
use crate::state::EngineContext;


pub async fn process_closes(
    trades: &[crate::engine::real::Trade],
    prices: &crate::state::PriceMap,
    settings: &TradingSettings,
    http: &Client,
    db: &Connection,
    keys: &Arc<ExchangeKeys>,
    ts: &TechnicalSettings,
    e_ctx: &Arc<EngineContext>,
) -> Vec<(i64, f64, f64, f64, f64, f64, f64, i64, f64, f64, f64)> {

    let repo = TradeRepository::new(db.clone());
    // Tuple: (trade_id, price_a, price_b, spread, net_pnl, funding_rate_a, funding_rate_b, funding_next_ts, accumulated_funding_cost)
    let mut live_updates: Vec<(i64, f64, f64, f64, f64, f64, f64, i64, f64, f64, f64)> = Vec::new();
    // ── Close qualifying trades ────────────────────────────────────────────────
    if !trades.is_empty() {
        tracing::debug!(
            "[real] Processing {} open trades for profile {}",
            trades.len(),
            settings.id
        );
    }
    for trade in trades {
        let is_a_higher = trade.direction.starts_with(&trade.exchange_a);
        let _elapsed_ms = now_ms() - trade.entry_time;

        let manual_close = trade.close_reason.as_deref() == Some("MANUAL_CLOSE");
        let closing_retry = trade.status == "CLOSING";
        if manual_close {
            tracing::debug!(
                "[real] MANUAL_CLOSE detected for trade {} {} on {}/{}",
                trade.id, trade.pair, trade.exchange_a, trade.exchange_b
            );
        }

        // CRITICAL: skip trades where entry prices aren't confirmed yet (fast-poll pending)
        // Bypass this check if the user is forcing a manual close.
        if (trade.entry_price_a <= 0.0 || trade.entry_price_b <= 0.0) && !manual_close && !closing_retry {
            info!(
                "[real] Skipping close for {} {} — entry prices not confirmed yet",
                trade.id, trade.pair
            );
            continue;
        }

        let live_state = crate::engine::pnl::calculate_live_pnl_state(
            &trade, prices, e_ctx, manual_close, closing_retry,
        );

        if !live_state.is_valid {
            continue;
        }

        let key_a = format!("{}:{}", trade.exchange_a, trade.pair);
        let key_b = format!("{}:{}", trade.exchange_b, trade.pair);

        let mult_a_live = e_ctx.multipliers.get(&key_a).map(|v| *v).unwrap_or(1.0);
        let mult_b_live = e_ctx.multipliers.get(&key_b).map(|v| *v).unwrap_or(1.0);
        let coins_a = live_state.coins_a;
        let coins_b = live_state.coins_b;

        let fund_cost = trade.funding_cost;
        let exec_net_pnl = crate::engine::pnl::calculate_net_pnl(live_state.exec_gross_pnl, trade.commission_cost, trade.slippage_cost, fund_cost);
        let exec_net_pct = exec_net_pnl / trade.volume * 100.0;

        let top_spread = trade.highest_spread.max(live_state.current_spread);
        let stop_limit = top_spread - settings.trailing_stop_percent;

        let mut exit_price_a = live_state.exit_price_a;
        let mut exit_price_b = live_state.exit_price_b;



        let triggered_tp = exec_net_pct >= settings.take_profit_percent;
        // In arbitrage, a STOP triggers when the spread WIDENS against our position (increasing loss).
        // If entry spread was 2.0% and trailing_stop is 1.0%, stop fires if exit_spread exceeds 3.0%.
        let triggered_stop = settings.trailing_stop_percent > 0.0
            && live_state.exit_spread > (trade.entry_spread + settings.trailing_stop_percent);

        // ZERO_SPREAD = spread converged to zero or to its natural structural baseline.
        let min_exit_profit_pct = 0.15;
        let target_convergence_spread = trade.structural_spread.max(0.0);
        let zero_spread = (live_state.exit_spread <= target_convergence_spread || live_state.exit_spread <= 0.0)
            && exec_net_pct >= min_exit_profit_pct;

        let mut reason_str = "";
        if manual_close { reason_str = "MANUAL"; }
        else if triggered_stop { reason_str = "STOP"; }
        else if triggered_tp { reason_str = "TP"; }
        else if zero_spread { reason_str = "ZERO_SPREAD"; }

        if closing_retry || manual_close || triggered_tp || triggered_stop || zero_spread {
            if e_ctx.closing_in_flight.get(&trade.id).map(|v| *v).unwrap_or(false) {
                tracing::debug!("[real] Close already in-flight for id={} {}, skipping", trade.id, trade.pair);
                continue;
            }

            let exa = trade.exchange_a.clone();
            let exb = trade.exchange_b.clone();
            let pair = trade.pair.clone();
            let vol = trade.volume;

            let mult_a = e_ctx.multipliers.get(&key_a).map(|v| *v).unwrap_or(1.0);
            let mult_b = e_ctx.multipliers.get(&key_b).map(|v| *v).unwrap_or(1.0);

            // 🛡️ JIT L2 VERIFICATION FOR EXITS (TP / ZERO_SPREAD) ───────────────────
            // Strategy:
            //   1. If L2 WS data available → VWAP verification (best accuracy)
            //   2. If L2 WS data NOT available → require 3 consecutive L1 zero-spread
            //      confirmations (~30s) before allowing close. Prevents L1 tick flashes.

            if !closing_retry && !manual_close && !triggered_stop && (reason_str == "TP" || reason_str == "ZERO_SPREAD") {
                // MIRAGE cooldown: skip this check if we already logged MIRAGE for this trade recently (10s)
                let last_mirage = e_ctx.mirage_cooldown.get(&trade.id).map(|v| *v).unwrap_or(0);
                let now_cd = now_ms();
                if (now_cd - last_mirage) < 10_000 {
                    // Still in MIRAGE cooldown — skip silently (no log spam)
                    continue;
                }
                // L2 WS cache only — zero REST calls
                let cached_a = e_ctx.l2_books.get(&key_a).map(|v| v.clone());
                let cached_b = e_ctx.l2_books.get(&key_b).map(|v| v.clone());
                let now_l2 = now_ms();

                let has_l2 = match (&cached_a, &cached_b) {
                    (Some(a), Some(b)) if (now_l2 - a.updated_ms) < 2_000 && (now_l2 - b.updated_ms) < 2_000 => true,
                    _ => false,
                };

                if has_l2 {
                    // ═══ PATH A: L2 available → VWAP verification ═══
                    let ca = cached_a.unwrap();
                    let cb = cached_b.unwrap();
                    let ob_a_res = Ok((ca.bids.clone(), ca.asks.clone()));
                    let ob_b_res = Ok((cb.bids.clone(), cb.asks.clone()));

                    let vwap_a = crate::engine::execution::verify_l2_vwap(ob_a_res, is_a_higher, coins_a, mult_a);
                    let vwap_b = crate::engine::execution::verify_l2_vwap(ob_b_res, !is_a_higher, coins_b, mult_b);

                    if let (Some(px_a), Some(px_b)) = (vwap_a, vwap_b) {
                        let vwap_exec_pnl_a = if is_a_higher {
                            (trade.entry_price_a - px_a) * coins_a
                        } else {
                            (px_a - trade.entry_price_a) * coins_a
                        };
                        let vwap_exec_pnl_b = if is_a_higher {
                            (px_b - trade.entry_price_b) * coins_b
                        } else {
                            (trade.entry_price_b - px_b) * coins_b
                        };
                        let vwap_gross_pnl = vwap_exec_pnl_a + vwap_exec_pnl_b;
                        let vwap_net_pnl =
                            vwap_gross_pnl - trade.commission_cost - trade.funding_cost;
                        let vwap_net_pct = vwap_net_pnl / vol * 100.0;

                        let vwap_exit_spread = if is_a_higher {
                            (px_a - px_b) / px_b.max(1e-12) * 100.0
                        } else {
                            (px_b - px_a) / px_a.max(1e-12) * 100.0
                        };

                        let still_tp = reason_str == "TP" && vwap_net_pct >= settings.take_profit_percent;
                        let still_zero = reason_str == "ZERO_SPREAD" && vwap_exit_spread <= 0.0 && vwap_net_pnl >= 0.0;

                        if !still_tp && !still_zero {
                            e_ctx.mirage_cooldown.insert(trade.id, now_cd);
                            e_ctx.zero_spread_confirms.remove(&trade.id); // reset L1 counter on VWAP fail
                            tracing::warn!("[real] 🪄 MIRAGE ({}): L1 spread={:.2}%, VWAP spread={:.2}% — skipping close this cycle (cooldown 10s)", 
                                reason_str, live_state.exit_spread, vwap_exit_spread);
                            continue;
                        } else {
                            tracing::info!("[real] ✅ VWAP Confirmed: VWAP spread={:.2}%, VWAP PnL={:.2}%", vwap_exit_spread, vwap_net_pct);
                            exit_price_a = px_a;
                            exit_price_b = px_b;
                            e_ctx.zero_spread_confirms.remove(&trade.id);
                        }
                    } else {
                        // L2 exists but not enough depth for VWAP — fall through to L1 multi-check
                        tracing::debug!("[real] L2 depth insufficient for VWAP on {}/{}, using L1 multi-check", exa, exb);
                        // fall through to PATH B below
                        let (prev_count, prev_ts) = e_ctx.zero_spread_confirms.get(&trade.id).map(|v| *v).unwrap_or((0, 0));
                        let gap_ok = (now_cd - prev_ts) < 15_000; // checks must be within 15s of each other
                        let new_count = if gap_ok { prev_count + 1 } else { 1 };
                        e_ctx.zero_spread_confirms.insert(trade.id, (new_count, now_cd));
                        let required = 3u32;
                        if new_count < required || exec_net_pnl <= 0.0 {
                            e_ctx.mirage_cooldown.insert(trade.id, now_cd);
                            tracing::info!("[real] 📊 L1 confirm {}/{} for {} (no L2 depth) net_pnl={:.4} — waiting for {} consecutive checks", 
                                new_count, required, pair, exec_net_pnl, required);
                            continue;
                        }
                        tracing::info!("[real] ✅ L1 Multi-Check CONFIRMED: {}/{} checks, net_pnl={:.4}% — proceeding to close {}", 
                            new_count, required, exec_net_pnl / vol * 100.0, pair);
                        e_ctx.zero_spread_confirms.remove(&trade.id);
                    }
                } else {
                    // ═══ PATH B: No L2 data → L1 consecutive zero-spread confirmation ═══
                    // Require 3 consecutive L1 polls showing zero_spread = true AND net_pnl > 0.
                    // Each poll is ~10s apart (due to mirage_cooldown), so 3 checks ≈ 30s of sustained convergence.
                    let (prev_count, prev_ts) = e_ctx.zero_spread_confirms.get(&trade.id).map(|v| *v).unwrap_or((0, 0));
                    // Gap must be within 15s — if longer, spread likely fluctuated and counter resets.
                    let gap_ok = (now_cd - prev_ts) < 15_000;
                    let new_count = if gap_ok { prev_count + 1 } else { 1 };
                    e_ctx.zero_spread_confirms.insert(trade.id, (new_count, now_cd));

                    let required = 3u32;
                    if new_count < required || exec_net_pnl <= 0.0 {
                        e_ctx.mirage_cooldown.insert(trade.id, now_cd);
                        tracing::info!("[real] 📊 L1 confirm {}/{} for {} (no L2 WS) exit_sp={:.2}% net_pnl={:.4} — waiting for {} consecutive checks", 
                            new_count, required, pair, live_state.exit_spread, exec_net_pnl, required);
                        continue;
                    }
                    // 3 consecutive L1 confirmations + positive PnL — safe to close
                    tracing::info!("[real] ✅ L1 Multi-Check CONFIRMED: {}/{} checks, exit_sp={:.2}%, net_pnl={:.4}% — proceeding to close {}", 
                        new_count, required, live_state.exit_spread, exec_net_pnl / vol * 100.0, pair);
                    e_ctx.zero_spread_confirms.remove(&trade.id);
                }
            }

            let reason = if reason_str.is_empty() {
                trade.close_reason.as_deref().unwrap_or("RETRY_CLOSE")
            } else {
                reason_str
            };
            if !repo.begin_close_attempt(trade.id, reason).await {
                continue;
            }
            e_ctx.closing_in_flight.insert(trade.id, true);
            info!("[real] Closing {} {} reason={} pct={:.3}% exit_sp={:.4}", trade.id, pair, reason, exec_net_pct, live_state.exit_spread);

            let ctx_close = crate::engine::execution::CloseTaskContext {
                trade_id: trade.id,
                pair: pair.clone(),
                exa: exa.clone(),
                exb: exb.clone(),
                is_a_higher,
                q_a: coins_a,
                q_b: coins_b,
                mult_a: mult_a_live,
                mult_b: mult_b_live,
                tick_a: e_ctx.tick_sizes.get(&key_a).map(|v| *v).unwrap_or(0.000001),
                tick_b: e_ctx.tick_sizes.get(&key_b).map(|v| *v).unwrap_or(0.000001),
                leverage: settings.leverage as f64,
                exit_price_a: exit_price_a,
                exit_price_b: exit_price_b,
                use_ioc: ts.entry_order_type == "ioc" && !manual_close,
                slippage_percent: settings.slippage_percent,
                entry_a: trade.entry_price_a,
                entry_b: trade.entry_price_b,
                trade_coins_a: coins_a,
                trade_coins_b: coins_b,
                close_verify_retries: ts.close_verify_retries as u32,
                close_verify_delay: ts.close_verify_delay_ms as u64,
                api_timeout_ms: ts.api_timeout_ms,
                reason: reason.to_string(),
                fund_cost: trade.funding_cost,
                exit_spread: live_state.exit_spread,
            };

            crate::engine::execution::spawn_close_task(
                ctx_close, http.clone(), Arc::clone(&keys), db.clone(), Arc::clone(&e_ctx)
            );
        } else {
            // Fetch real funding rates from local cache to avoid blocking event loop
            let (fr_a, next_a) = e_ctx.funding_rates.get(&key_a).map(|f| (f.rate, f.next_ts)).unwrap_or((0.0, 0_i64));
            
            let (fr_b, next_b) = e_ctx.funding_rates.get(&key_b).map(|f| (f.rate, f.next_ts)).unwrap_or((0.0, 0_i64));
            let fund_next = next_a.min(next_b); // nearest funding event

            // BUG-4 FIX: Incremental funding accumulation instead of linear extrapolation.
            let dt_8h = ts.live_update_throttle_ms as f64 / (8.0 * 3_600_000.0);
            let fund_increment = calculate_funding_increment(
                is_a_higher, trade.entry_price_a, trade.entry_price_b,
                coins_a, coins_b, fr_a, fr_b, dt_8h,
            );
            let acc_fund_cost = trade.funding_cost + fund_increment;

            // Net PnL with incrementally accumulated funding cost.
            let real_live_net_pnl = calculate_net_pnl(
                live_state.live_gross_pnl, trade.commission_cost, 0.0, acc_fund_cost,
            );

            live_updates.push((
                trade.id,
                live_state.c_price_a,
                live_state.c_price_b,
                live_state.current_spread,
                real_live_net_pnl,
                fr_a,
                fr_b,
                fund_next,
                acc_fund_cost,
                live_state.live_pnl_a,
                live_state.live_pnl_b,
            ));
            if top_spread > trade.highest_spread {
                let id = trade.id;
                // Wait, need await here, earlier it was synchronous?
                // The original code was synchronous `db.call` which returns a Future but was not awaited! I will await it!
                let rb = repo.clone();
                tokio::spawn(async move {
                    rb.update_highest_spread(id, top_spread).await;
                });
            }
        }
    }


    live_updates
}
