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
    let _dry_run = false;
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
            info!(
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

        let data_a = prices
            .get(&trade.exchange_a)
            .and_then(|m| m.get(&trade.pair).cloned());
        let data_b = prices
            .get(&trade.exchange_b)
            .and_then(|m| m.get(&trade.pair).cloned());

        if (data_a.is_none() || data_b.is_none()) && !manual_close && !closing_retry {
            continue;
        }

        // Fallback prices in case of missing WebSocket data during a manual close
        let mut exit_price_a = trade.entry_price_a.max(1e-12);
        let mut exit_price_b = trade.entry_price_b.max(1e-12);
        let mut c_price_a = exit_price_a;
        let mut c_price_b = exit_price_b;
        let mut current_spread = 0.0;
        let mut exit_spread = 0.0;

        if let (Some(da), Some(db)) = (&data_a, &data_b) {
            if da.bid <= 0.0 || da.ask <= 0.0 || db.bid <= 0.0 || db.ask <= 0.0 {
                // If orderbook is completely empty or just preloaded (0 prices), fallback to entry prices for limit projection
                c_price_a = if da.price > 0.0 {
                    da.price
                } else {
                    trade.entry_price_a
                };
                c_price_b = if db.price > 0.0 {
                    db.price
                } else {
                    trade.entry_price_b
                };
                exit_price_a = c_price_a;
                exit_price_b = c_price_b;
                if !manual_close {
                    continue;
                }
            } else {
                c_price_a = da.price;
                c_price_b = db.price;
                exit_price_a = if is_a_higher { da.ask } else { da.bid };
                exit_price_b = if is_a_higher { db.bid } else { db.ask };

                // Use absolute worst-case bid/ask for current spread so trailing stops trigger correctly
                current_spread = calculate_current_spread(
                    is_a_higher, da.bid, da.ask, db.bid, db.ask,
                );
                exit_spread = calculate_exit_spread(
                    is_a_higher, da.bid, da.ask, db.bid, db.ask,
                );
            }
        }

        // BUG-2 FIX: Use actual physical coins per leg (contracts × multiplier)
        // instead of approximating via volume/price_a which diverges when
        // signal price ≠ fill price or after asymmetric trimming.
        let mult_a_live = e_ctx.multipliers.get(&format!("{}:{}", trade.exchange_a, trade.pair)).map(|v| *v).unwrap_or(1.0);
        let mult_b_live = e_ctx.multipliers.get(&format!("{}:{}", trade.exchange_b, trade.pair)).map(|v| *v).unwrap_or(1.0);
        let coins_a = trade.contracts_a * mult_a_live;
        let coins_b = trade.contracts_b * mult_b_live;

        // PnL for DISPLAY (using mark/mid prices — matches exchange PnL)
        let live_pnl = calculate_gross_pnl(
            is_a_higher, trade.entry_price_a, trade.entry_price_b,
            c_price_a, c_price_b, coins_a, coins_b,
        );
        let live_pnl_a = live_pnl.pnl_a;
        let live_pnl_b = live_pnl.pnl_b;
        let live_gross_pnl = live_pnl.gross_pnl;

        // Use the accumulated real funding cost from DB
        let fund_cost = trade.funding_cost;
        let _live_net_pnl = calculate_net_pnl(live_gross_pnl, trade.commission_cost, 0.0, fund_cost);

        // PnL for CLOSE TRIGGERS (using bid/ask — pessimistic, for safe execution)
        let exec_pnl = calculate_gross_pnl(
            is_a_higher, trade.entry_price_a, trade.entry_price_b,
            exit_price_a, exit_price_b, coins_a, coins_b,
        );
        let exec_gross_pnl = exec_pnl.gross_pnl;
        let exec_net_pnl = calculate_net_pnl(exec_gross_pnl, trade.commission_cost, trade.slippage_cost, fund_cost);
        let exec_net_pct = exec_net_pnl / trade.volume * 100.0;

        let top_spread = trade.highest_spread.max(current_spread);
        let stop_limit = top_spread - settings.trailing_stop_percent;

        let triggered_tp = exec_net_pct >= settings.take_profit_percent;
        let triggered_stop = top_spread > 0.0 && current_spread < stop_limit;
        // Structural-aware exit: for pairs with structural spread > 0, the spread
        // will converge to structural_spread, not 0.0. Use it as the effective zero.
        let structural_target = trade.structural_spread.max(0.0);
        let zero_spread = exit_spread <= structural_target && exec_net_pnl >= 0.0;

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

            let mult_a = e_ctx.multipliers.get(&format!("{}:{}", exa, pair)).map(|v| *v).unwrap_or(1.0);
            let mult_b = e_ctx.multipliers.get(&format!("{}:{}", exb, pair)).map(|v| *v).unwrap_or(1.0);

            // 🛡️ JIT L2 VERIFICATION FOR EXITS (TP / ZERO_SPREAD) ───────────────────
            // Force close after 5 minutes of MIRAGE to avoid holding positions forever
            let trade_age_ms = now_ms() - trade.entry_time;
            let force_close_after_mirage = trade_age_ms > 300_000; // 5 minutes

            if !closing_retry && !manual_close && !triggered_stop && (reason_str == "TP" || reason_str == "ZERO_SPREAD") && !force_close_after_mirage {
                // MIRAGE cooldown: skip this check if we already logged MIRAGE for this trade recently (10s)
                let last_mirage = e_ctx.mirage_cooldown.get(&trade.id).map(|v| *v).unwrap_or(0);
                let now_cd = now_ms();
                if (now_cd - last_mirage) < 10_000 {
                    // Still in MIRAGE cooldown — skip silently (no log spam)
                    continue;
                }

                // L2 WS cache only — zero REST calls
                let l2_key_a = format!("{}:{}", exa, pair);
                let l2_key_b = format!("{}:{}", exb, pair);
                let cached_a = e_ctx.l2_books.get(&l2_key_a).map(|v| v.clone());
                let cached_b = e_ctx.l2_books.get(&l2_key_b).map(|v| v.clone());
                let now_l2 = now_ms();

                let (ob_a_res, ob_b_res) = match (&cached_a, &cached_b) {
                    (Some(a), Some(b)) if (now_l2 - a.updated_ms) < 2_000 && (now_l2 - b.updated_ms) < 2_000 => {
                        (Ok((a.bids.clone(), a.asks.clone())), Ok((b.bids.clone(), b.asks.clone())))
                    },
                    _ => {
                        // No fresh L2 WS data — skip VWAP verification, allow close to proceed
                        tracing::info!("[real] ⏭️ No L2 WS data for {}/{} — skipping VWAP exit check", exa, exb);
                        (Ok((vec![], vec![])), Ok((vec![], vec![])))
                    }
                };

                let analyze_vwap = |ob_res: Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), String>, is_buy: bool, required_vol: f64, m: f64| -> Option<f64> {
                    if let Ok((bids, asks)) = ob_res {
                        let levels = if is_buy { asks } else { bids };
                        let mut cum_vol = 0.0;
                        let mut cum_cost = 0.0;
                        
                        for (px, sz) in levels {
                            let base_sz = if m > 0.0 && m != 1.0 { sz * m } else { sz };
                            let remaining = required_vol - cum_vol;
                            if remaining <= 0.0 { break; }
                            
                            let take = if base_sz > remaining { remaining } else { base_sz };
                            cum_vol += take;
                            cum_cost += take * px;
                        }
                        // We tolerate tiny floating point gap for full fill check
                        if cum_vol >= required_vol * 0.99 { 
                            Some(cum_cost / cum_vol)
                        } else {
                            None 
                        }
                    } else {
                        None
                    }
                };

                // Exit A requires us to do the OPPOSITE of entry. 
                // If Entry A was Short (is_a_higher = true), we must BUY.
                let vwap_a = analyze_vwap(ob_a_res, is_a_higher, coins_a, mult_a);
                // If Entry B was Long (is_a_higher = true), we must SELL (is_buy = false).
                let vwap_b = analyze_vwap(ob_b_res, !is_a_higher, coins_b, mult_b);

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
                        vwap_gross_pnl - trade.commission_cost - trade.slippage_cost - trade.funding_cost;
                    let vwap_net_pct = vwap_net_pnl / vol * 100.0;

                    let vwap_exit_spread = if is_a_higher {
                        (px_a - px_b) / px_b.max(1e-12) * 100.0
                    } else {
                        (px_b - px_a) / px_a.max(1e-12) * 100.0
                    };

                    let still_tp = reason_str == "TP" && vwap_net_pct >= settings.take_profit_percent;
                    let still_zero = reason_str == "ZERO_SPREAD" && vwap_exit_spread <= 0.0 && vwap_net_pnl >= 0.0;

                    if !still_tp && !still_zero {
                        tracing::warn!("[real] ⚠️ VWAP MIRAGE ({}): L1 spread={:.2}%, VWAP spread={:.2}%. Closing anyway to prevent getting stuck.", 
                            reason_str, exit_spread, vwap_exit_spread);
                        exit_price_a = px_a;
                        exit_price_b = px_b;
                    } else {
                        tracing::info!("[real] ✅ VWAP Confirmed: VWAP spread={:.2}%, VWAP PnL={:.2}%", vwap_exit_spread, vwap_net_pct);
                        exit_price_a = px_a;
                        exit_price_b = px_b;
                    }
                } else {
                    tracing::warn!("[real] ⚠️ MIRAGE ({}): Insufficient L2 depth for {:.4}/{:.4} phys coins on {}/{}. Using L1 fallback prices to force close.", reason_str, coins_a, coins_b, exa, exb);
                    // exit_price_a and b remain at L1 prices
                }
            }

            // Force close after 5 minutes of MIRAGE — position is stuck, close at any price
            if force_close_after_mirage && reason_str == "ZERO_SPREAD" && !closing_retry && !manual_close {
                tracing::warn!("[real] ⏰ FORCE CLOSE {} after {}s of MIRAGE — accepting loss to avoid holding forever", pair, trade_age_ms / 1000);
                // Clear mirage cooldown since we're forcing close
                e_ctx.mirage_cooldown.remove(&trade.id);
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
            info!("[real] Closing {} {} reason={} pct={:.3}% exit_sp={:.4}", trade.id, pair, reason, exec_net_pct, exit_spread);

            let c_a = trade.contracts_a;
            let c_b = trade.contracts_b;
            let id = trade.id;
            let _vol = trade.volume;
            let entry_a = trade.entry_price_a;
            let entry_b = trade.entry_price_b;
            let ca_coins = coins_a;
            let cb_coins = coins_b;
            let h = http.clone();
            let k = Arc::clone(&keys);
            let db_c = db.clone();
            let r = reason.to_string();
            let lev = settings.leverage;
            let _exit_sanity = ts.exit_sanity_pct;
            let close_verify_delay = ts.close_verify_delay_ms as u64;
            let close_verify_retries = ts.close_verify_retries;
            let api_timeout_ms = ts.api_timeout_ms;

            let use_ioc = ts.entry_order_type == "ioc" && !manual_close;
            let tick_a = e_ctx
                .tick_sizes
                .get(&format!("{}:{}", exa, pair))
                .map(|v| *v)
                .unwrap_or(0.000001);
            let tick_b = e_ctx
                .tick_sizes
                .get(&format!("{}:{}", exb, pair))
                .map(|v| *v)
                .unwrap_or(0.000001);
            let mult_a = e_ctx
                .multipliers
                .get(&format!("{}:{}", exa, pair))
                .map(|v| *v)
                .unwrap_or(1.0);
            let mult_b = e_ctx
                .multipliers
                .get(&format!("{}:{}", exb, pair))
                .map(|v| *v)
                .unwrap_or(1.0);

            let q_a = c_a as f64 * mult_a;
            let q_b = c_b as f64 * mult_b;
            let slippage_percent = settings.slippage_percent;

            let _e_ctx_c = Arc::clone(&e_ctx);
            let closing_guard = Arc::clone(&e_ctx.closing_in_flight);
            let trade_id_for_guard = trade.id;
            tokio::spawn(async move {
                let conn_a = crate::engine::exchanges::get_connector(&exa).unwrap();
                let conn_b = crate::engine::exchanges::get_connector(&exb).unwrap();

                let cr = crate::engine::execution::execute_close(
                    &conn_a, &conn_b, &h, &k, &pair,
                    is_a_higher, q_a, q_b, lev as f64,
                    exit_price_a, exit_price_b,
                    use_ioc, slippage_percent, mult_a, mult_b, tick_a, tick_b,
                    close_verify_retries as u32, close_verify_delay, api_timeout_ms,
                    entry_a, entry_b, ca_coins, cb_coins,
                ).await;

                let repo_close = TradeRepository::new(db_c.clone());

                if cr.real_exit_a <= 0.0 || cr.real_exit_b <= 0.0 {
                    tracing::error!(
                        "[real] ⚠️ CLOSE FAILED OR UNVERIFIED id={} {}. Error: {}",
                        id, pair, cr.error_msg
                    );
                    let keep_closing = cr.leg_a_external || cr.leg_b_external || r == "MANUAL";
                    repo_close
                        .record_close_failure(id, &cr.error_msg, keep_closing)
                        .await;
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    closing_guard.remove(&trade_id_for_guard);
                    return;
                }

                let close_reason_db = if cr.leg_a_external && cr.leg_b_external {
                    "EXTERNAL_CLOSE".to_string()
                } else if cr.leg_a_external || cr.leg_b_external {
                    "EXTERNAL_PARTIAL_CLOSE".to_string()
                } else {
                    r.clone()
                };

                let real_gp = cr.pnl_a + cr.pnl_b;

                let slip_cost_exit_a = if cr.real_exit_a > 0.0 {
                    if is_a_higher {
                        ((cr.real_exit_a - exit_price_a) * ca_coins).max(0.0)
                    } else {
                        ((exit_price_a - cr.real_exit_a) * ca_coins).max(0.0)
                    }
                } else {
                    0.0
                };
                let slip_cost_exit_b = if cr.real_exit_b > 0.0 {
                    if !is_a_higher {
                        ((cr.real_exit_b - exit_price_b) * cb_coins).max(0.0)
                    } else {
                        ((exit_price_b - cr.real_exit_b) * cb_coins).max(0.0)
                    }
                } else {
                    0.0
                };
                let exit_slip = slip_cost_exit_a + slip_cost_exit_b;

                info!("[real] [EXCHANGE_VERIFIED] PnL id={} pna={:.4} pnb={:.4} gross={:.4} exit_fee={:.4} exit_slip={:.4}", id, cr.pnl_a, cr.pnl_b, real_gp, cr.exit_fee, exit_slip);

                // Verify positions are actually gone from both exchanges
                let pos_check_a = conn_a.fetch_positions(&h, &k).await;
                let pos_check_b = conn_b.fetch_positions(&h, &k).await;
                let still_a = pos_check_a
                    .as_ref()
                    .ok()
                    .map(|ps| {
                        ps.iter().any(|p| {
                            symbols_match(&p.symbol, &pair)
                                && p.size.abs() > 1e-12
                        })
                    })
                    .unwrap_or(true);
                let still_b = pos_check_b
                    .as_ref()
                    .ok()
                    .map(|ps| {
                        ps.iter().any(|p| {
                            symbols_match(&p.symbol, &pair)
                                && p.size.abs() > 1e-12
                        })
                    })
                    .unwrap_or(true);
                if still_a || still_b {
                    error!(
                        "[real] ⚠️ POSITION STILL EXISTS after close id={} {} still_a={} still_b={} — keeping trade OPEN for retry/recovery",
                        id, pair, still_a, still_b
                    );
                    repo_close
                        .record_close_failure(
                            id,
                            &format!("post-close verification failed still_a={} still_b={}", still_a, still_b),
                            true,
                        )
                        .await;
                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                    closing_guard.remove(&trade_id_for_guard);
                    return;
                }

                repo_close
                    .mark_trade_closed_confirmed(
                        id,
                        cr.real_exit_a,
                        cr.real_exit_b,
                        exit_spread,
                        real_gp,
                        cr.pnl_a,
                        cr.pnl_b,
                        &close_reason_db,
                        fund_cost,
                        cr.exit_fee,
                        exit_slip,
                    )
                    .await;
                closing_guard.remove(&trade_id_for_guard);
            });
        } else {
            // Fetch real funding rates from local cache to avoid blocking event loop
            let key_a = format!("{}:{}", trade.exchange_a, trade.pair);
            let (fr_a, next_a) = e_ctx.funding_rates.get(&key_a).map(|f| (f.rate, f.next_ts)).unwrap_or((0.0, 0_i64));
            
            let key_b = format!("{}:{}", trade.exchange_b, trade.pair);
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
                live_gross_pnl, trade.commission_cost, 0.0, acc_fund_cost,
            );

            live_updates.push((
                trade.id,
                c_price_a,
                c_price_b,
                current_spread,
                real_live_net_pnl,
                fr_a,
                fr_b,
                fund_next,
                acc_fund_cost,
                live_pnl_a,
                live_pnl_b,
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
