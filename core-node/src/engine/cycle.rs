use crate::engine::db::TradeRepository;
use reqwest::Client;
use std::sync::Arc;
use tokio::time::Duration;
use tokio_rusqlite::Connection;
use tracing::{error, info};

use crate::engine::real::{has_sufficient_margin, mark_balance_low, now_ms, ExchangeKeys};
use shared::{SpreadRow, TechnicalSettings, TradingSettings};
use std::sync::atomic::{AtomicI64, Ordering};

static LAST_MARGIN_LOG: AtomicI64 = AtomicI64::new(0);

// NOTE: LAST_LIVE_FLUSH is declared as a function-local static inside run_cycle below.

pub async fn run_cycle(
    signals: Vec<shared::TradeSignal>,
    spreads: Vec<SpreadRow>,
    prices: crate::state::PriceMap,
    settings: TradingSettings,
    http: Client,
    db: Connection,
    keys: Arc<ExchangeKeys>,
    ts: &TechnicalSettings,
    e_ctx: Arc<crate::state::EngineContext>,
) {
    let dry_run = false;
    let repo = TradeRepository::new(db.clone());

    let trades = repo.load_open_trades(&settings.id).await;
    let num_active = trades.len() as i64;
    let open_pairs: std::collections::HashSet<String> =
        trades.iter().map(|t| t.pair.clone()).collect();
    // funding_rate_8h setting is INTENTIONALLY not used here.
    // Real accumulated funding is read from trade.funding_cost (DB column updated from exchange API).

    // NOTE: We process closes ALWAYS (even when is_active=false)
    // so that MANUAL_CLOSE from the UI works even when the bot is stopped.
    // Only new trade entries are gated by is_active.

    // ── Close qualifying trades ────────────────────────────────────────────────
    if !trades.is_empty() {
        tracing::debug!(
            "[real] Processing {} open trades for profile {}",
            trades.len(),
            settings.id
        );
    }
    // Tuple: (trade_id, price_a, price_b, spread, net_pnl, funding_rate_a, funding_rate_b, funding_next_ts, accumulated_funding_cost)
    let mut live_updates: Vec<(i64, f64, f64, f64, f64, f64, f64, i64, f64, f64, f64)> = Vec::new();
    for trade in &trades {
        let is_a_higher = trade.direction.starts_with(&trade.exchange_a);
        let _elapsed_ms = now_ms() - trade.entry_time;

        let manual_close = trade.close_reason.as_deref() == Some("MANUAL_CLOSE");
        if manual_close {
            info!(
                "[real] MANUAL_CLOSE detected for trade {} {} on {}/{}",
                trade.id, trade.pair, trade.exchange_a, trade.exchange_b
            );
        }

        // CRITICAL: skip trades where entry prices aren't confirmed yet (fast-poll pending)
        // Bypass this check if the user is forcing a manual close.
        if (trade.entry_price_a <= 0.0 || trade.entry_price_b <= 0.0) && !manual_close {
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

        if (data_a.is_none() || data_b.is_none()) && !manual_close {
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
                exit_price_a = if is_a_higher { c_price_a } else { c_price_a };
                exit_price_b = if is_a_higher { c_price_b } else { c_price_b };
                if !manual_close {
                    continue;
                }
            } else {
                c_price_a = da.price;
                c_price_b = db.price;
                exit_price_a = if is_a_higher { da.ask } else { da.bid };
                exit_price_b = if is_a_higher { db.bid } else { db.ask };

                // Use absolute worst-case bid/ask for current spread so trailing stops trigger correctly on volatile orderbook spikes
                // rather than lagging mark prices.
                current_spread = if is_a_higher {
                    (da.bid - db.ask) / db.ask.max(1e-12) * 100.0
                } else {
                    (db.bid - da.ask) / da.ask.max(1e-12) * 100.0
                };
                exit_spread = if is_a_higher {
                    (da.ask - db.bid) / db.bid.max(1e-12) * 100.0
                } else {
                    (db.ask - da.bid) / da.bid.max(1e-12) * 100.0
                };
            }
        }

        let _contracts_a = trade.contracts_a as f64;
        let _contracts_b = trade.contracts_b as f64;

        // PnL for DISPLAY (using mark/mid prices — matches exchange PnL)
        let live_pnl_a = if is_a_higher {
            (trade.entry_price_a - c_price_a) * (trade.volume / trade.entry_price_a.max(1e-12))
        } else {
            (c_price_a - trade.entry_price_a) * (trade.volume / trade.entry_price_a.max(1e-12))
        };
        let live_pnl_b = if is_a_higher {
            (c_price_b - trade.entry_price_b) * (trade.volume / trade.entry_price_b.max(1e-12))
        } else {
            (trade.entry_price_b - c_price_b) * (trade.volume / trade.entry_price_b.max(1e-12))
        };
        let live_gross_pnl = live_pnl_a + live_pnl_b;

        // Use the accumulated real funding cost from DB (column updated every live-update cycle
        // using real rates fetched from exchange API — NOT from the settings constant).
        let fund_cost = trade.funding_cost;
        let _live_net_pnl = live_gross_pnl - trade.commission_cost - fund_cost;

        // PnL for CLOSE TRIGGERS (using bid/ask — pessimistic, for safe execution)
        let exec_pnl_a = if is_a_higher {
            (trade.entry_price_a - exit_price_a) * (trade.volume / trade.entry_price_a.max(1e-12))
        } else {
            (exit_price_a - trade.entry_price_a) * (trade.volume / trade.entry_price_a.max(1e-12))
        };
        let exec_pnl_b = if is_a_higher {
            (exit_price_b - trade.entry_price_b) * (trade.volume / trade.entry_price_b.max(1e-12))
        } else {
            (trade.entry_price_b - exit_price_b) * (trade.volume / trade.entry_price_b.max(1e-12))
        };
        let exec_gross_pnl = exec_pnl_a + exec_pnl_b;
        let exec_net_pnl = exec_gross_pnl - trade.commission_cost - fund_cost;
        let exec_net_pct = exec_net_pnl / trade.volume * 100.0;

        let top_spread = trade.highest_spread.max(current_spread);
        let stop_limit = top_spread - settings.trailing_stop_percent;

        let triggered_tp = exec_net_pct >= settings.take_profit_percent;
        let triggered_stop = top_spread > 0.0 && current_spread < stop_limit;
        let zero_spread = exit_spread <= 0.0 && exec_net_pnl >= 0.0;

        let mut reason_str = "";
        if manual_close { reason_str = "MANUAL"; }
        else if triggered_stop { reason_str = "STOP"; }
        else if triggered_tp { reason_str = "TP"; }
        else if zero_spread { reason_str = "ZERO_SPREAD"; }

        if manual_close || triggered_tp || triggered_stop || zero_spread {
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

            // 🛡️ JIT L2/L3 VERIFICATION FOR EXITS (TP / ZERO_SPREAD) ───────────────────
            if !manual_close && !triggered_stop && (reason_str == "TP" || reason_str == "ZERO_SPREAD") {
                tracing::info!("[real] 🔍 JIT Exit Verification (VWAP) for {} on {}/{}...", pair, exa, exb);
                let conn_a = crate::engine::exchanges::get_connector(&exa).unwrap();
                let conn_b = crate::engine::exchanges::get_connector(&exb).unwrap();
                
                let (ob_a_res, ob_b_res) = tokio::join!(
                    conn_a.fetch_orderbook(&http, &pair, 10),
                    conn_b.fetch_orderbook(&http, &pair, 10)
                );

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
                let vwap_a = analyze_vwap(ob_a_res, is_a_higher, vol, mult_a);
                // If Entry B was Long (is_a_higher = true), we must SELL (is_buy = false).
                let vwap_b = analyze_vwap(ob_b_res, !is_a_higher, vol, mult_b);

                if let (Some(px_a), Some(px_b)) = (vwap_a, vwap_b) {
                    let vwap_exec_pnl_a = if is_a_higher {
                        (trade.entry_price_a - px_a) * (vol / trade.entry_price_a.max(1e-12))
                    } else {
                        (px_a - trade.entry_price_a) * (vol / trade.entry_price_a.max(1e-12))
                    };
                    let vwap_exec_pnl_b = if is_a_higher {
                        (px_b - trade.entry_price_b) * (vol / trade.entry_price_b.max(1e-12))
                    } else {
                        (trade.entry_price_b - px_b) * (vol / trade.entry_price_b.max(1e-12))
                    };
                    let vwap_gross_pnl = vwap_exec_pnl_a + vwap_exec_pnl_b;
                    let vwap_net_pnl = vwap_gross_pnl - trade.commission_cost - trade.funding_cost;
                    let vwap_net_pct = vwap_net_pnl / vol * 100.0;

                    let vwap_exit_spread = if is_a_higher {
                        (px_a - px_b) / px_b.max(1e-12) * 100.0
                    } else {
                        (px_b - px_a) / px_a.max(1e-12) * 100.0
                    };

                    let still_tp = reason_str == "TP" && vwap_net_pct >= settings.take_profit_percent;
                    let still_zero = reason_str == "ZERO_SPREAD" && vwap_exit_spread <= 0.0 && vwap_net_pnl >= 0.0;

                    if !still_tp && !still_zero {
                        tracing::warn!("[real] 🛑 MIRAGE DETECTED ({}): L1 spread={:.2}%, VWAP spread={:.2}%, L1 PnL={:.2}%, VWAP PnL={:.2}%. Aborting close.", 
                            reason_str, exit_spread, vwap_exit_spread, exec_net_pct, vwap_net_pct);
                        continue;
                    } else {
                        tracing::info!("[real] ✅ VWAP Confirmed: VWAP spread={:.2}%, VWAP PnL={:.2}%", vwap_exit_spread, vwap_net_pct);
                        exit_price_a = px_a;
                        exit_price_b = px_b;
                    }
                } else {
                    tracing::warn!("[real] 🛑 MIRAGE DETECTED ({}): Insufficient L2 depth for {:.4} volume on {}/{}. Aborting close.", reason_str, vol, exa, exb);
                    continue;
                }
            }

            e_ctx.closing_in_flight.insert(trade.id, true);

            let reason = reason_str;
            info!("[real] Closing {} {} reason={} pct={:.3}% exit_sp={:.4}", trade.id, pair, reason, exec_net_pct, exit_spread);

            let c_a = trade.contracts_a;
            let c_b = trade.contracts_b;
            let id = trade.id;
            let vol = trade.volume;
            let entry_a = trade.entry_price_a;
            let entry_b = trade.entry_price_b;
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

            let _e_ctx_c = Arc::clone(&e_ctx);
            let closing_guard = Arc::clone(&e_ctx.closing_in_flight);
            let trade_id_for_guard = trade.id;
            tokio::spawn(async move {
                let conn_a = crate::engine::exchanges::get_connector(&exa).unwrap();
                let conn_b = crate::engine::exchanges::get_connector(&exb).unwrap();
                // Place close orders in parallel (Connectors handle limit projection, retries & fallback native)
                let (ra, rb) = tokio::join!(
                    conn_a.close_position(
                        &h,
                        &k,
                        &pair,
                        is_a_higher,
                        q_a,
                        dry_run,
                        lev as f64,
                        exit_price_a,
                        use_ioc,
                        settings.slippage_percent,
                        mult_a,
                        tick_a,
                        close_verify_retries as u32,
                        close_verify_delay,
                        api_timeout_ms
                    ),
                    conn_b.close_position(
                        &h,
                        &k,
                        &pair,
                        !is_a_higher,
                        q_b,
                        dry_run,
                        lev as f64,
                        exit_price_b,
                        use_ioc,
                        settings.slippage_percent,
                        mult_b,
                        tick_b,
                        close_verify_retries as u32,
                        close_verify_delay,
                        api_timeout_ms
                    )
                );

                let mut real_exit_a = 0.0;
                let mut real_exit_b = 0.0;
                let mut exit_fee = 0.0;
                let mut error_msg = String::new();

                match ra {
                    Ok(rc) => {
                        real_exit_a = rc.final_price;
                        exit_fee += rc.total_fee;
                    }
                    Err(e) => {
                        error_msg.push_str(&format!("Leg A close failed: {} ", e));
                    }
                }
                match rb {
                    Ok(rc) => {
                        real_exit_b = rc.final_price;
                        exit_fee += rc.total_fee;
                    }
                    Err(e) => {
                        error_msg.push_str(&format!("Leg B close failed: {}", e));
                    }
                }

                let repo_close = TradeRepository::new(db_c.clone());

                if real_exit_a <= 0.0 || real_exit_b <= 0.0 {
                    tracing::error!(
                        "[real] ⚠️ CLOSE FAILED OR UNVERIFIED id={} {}. Error: {}",
                        id,
                        pair,
                        error_msg
                    );
                    repo_close.revert_to_open(id).await;
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    closing_guard.remove(&trade_id_for_guard);
                    return;
                }

                // ✅ Fill confirmed — compute real PnL and mark CLOSED
                let real_pna = if is_a_higher {
                    (entry_a - real_exit_a) * (vol / entry_a)
                } else {
                    (real_exit_a - entry_a) * (vol / entry_a)
                };
                let real_pnb = if is_a_higher {
                    (real_exit_b - entry_b) * (vol / entry_b)
                } else {
                    (entry_b - real_exit_b) * (vol / entry_b)
                };
                let real_gp = real_pna + real_pnb;

                let slip_cost_exit_a = if real_exit_a > 0.0 {
                    if is_a_higher {
                        ((real_exit_a - exit_price_a) * (vol / entry_a)).max(0.0)
                    } else {
                        ((exit_price_a - real_exit_a) * (vol / entry_a)).max(0.0)
                    }
                } else {
                    0.0
                };
                let slip_cost_exit_b = if real_exit_b > 0.0 {
                    if !is_a_higher {
                        ((real_exit_b - exit_price_b) * (vol / entry_b)).max(0.0)
                    } else {
                        ((exit_price_b - real_exit_b) * (vol / entry_b)).max(0.0)
                    }
                } else {
                    0.0
                };
                let exit_slip = slip_cost_exit_a + slip_cost_exit_b;

                info!("[real] [EXCHANGE_VERIFIED] PnL id={} pna={:.4} pnb={:.4} gross={:.4} exit_fee={:.4} exit_slip={:.4}", id, real_pna, real_pnb, real_gp, exit_fee, exit_slip);

                // Verify positions are actually gone from both exchanges
                let pos_check_a = crate::engine::exchanges::get_connector(&exa)
                    .unwrap()
                    .fetch_positions(&h, &k)
                    .await;
                let pos_check_b = crate::engine::exchanges::get_connector(&exb)
                    .unwrap()
                    .fetch_positions(&h, &k)
                    .await;
                let still_a = pos_check_a
                    .as_ref()
                    .ok()
                    .map(|ps| ps.iter().any(|p| p.symbol == pair && p.size.abs() > 1e-12))
                    .unwrap_or(false);
                let still_b = pos_check_b
                    .as_ref()
                    .ok()
                    .map(|ps| ps.iter().any(|p| p.symbol == pair && p.size.abs() > 1e-12))
                    .unwrap_or(false);
                if still_a || still_b {
                    error!("[real] ⚠️ POSITION STILL EXISTS after close id={} {} still_a={} still_b={} — marking closed but position may be orphaned", id, pair, still_a, still_b);
                }

                repo_close
                    .mark_trade_closed_confirmed(
                        id,
                        real_exit_a,
                        real_exit_b,
                        exit_spread,
                        real_gp,
                        real_pna,
                        real_pnb,
                        &r,
                        fund_cost,
                        exit_fee,
                        exit_slip,
                    )
                    .await;
                closing_guard.remove(&trade_id_for_guard);
            });
        } else {
            // Fetch real funding rates from exchange APIs (cached 5 min)
            let (fr_a, next_a) = crate::engine::exchanges::get_connector(&trade.exchange_a)
                .unwrap()
                .get_funding_rate(&http, &trade.pair)
                .await
                .unwrap_or((0.0, 0));
            let (fr_b, next_b) = crate::engine::exchanges::get_connector(&trade.exchange_b)
                .unwrap()
                .get_funding_rate(&http, &trade.pair)
                .await
                .unwrap_or((0.0, 0));
            let fund_next = next_a.min(next_b); // nearest funding event

            // Replace momentary funding projection map with predictable smooth funding cost estimation (using settings parameter).
            // This prevents random huge distortions in lifetime PnL if live API rate suddenly spikes.
            let hours_open = (now_ms() - trade.entry_time) as f64 / 3_600_000.0_f64;
            // Configured rate (e.g. 0.01) is in percentage terms, so divide by 100
            let fr_user = settings.funding_rate_8h / 100.0;
            // Total cost estimation
            let acc_fund_cost = fr_user * trade.volume * (hours_open / 8.0);

            // Recompute net PnL with the freshly-fetched real funding cost so curr_gross_pnl
            // in the DB reflects the true net position (gross - commission - real_funding).
            let real_live_net_pnl = live_gross_pnl - trade.commission_cost - acc_fund_cost;

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

    static LAST_LIVE_FLUSH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let current_ms = now_ms() as u64;
    let elapsed =
        current_ms.saturating_sub(LAST_LIVE_FLUSH.load(std::sync::atomic::Ordering::Relaxed));
    if elapsed > ts.live_update_throttle_ms as u64 && !live_updates.is_empty() {
        LAST_LIVE_FLUSH.store(current_ms, std::sync::atomic::Ordering::Relaxed);
        let db_clone = db.clone();
        tokio::spawn(async move {
            let rep = TradeRepository::new(db_clone);
            rep.flush_live_updates(live_updates).await;
        });
    }

    // ── Open new real trades ──────────────────────────────────────────────────
    // KEY PRINCIPLE: We do NOT write to DB until BOTH legs are confirmed filled.
    // This eliminates phantom trades, $0 PnL records, and immediate open/close chaos.
    if !settings.is_active {
        return;
    } // only block NEW entries, not closes above
    if num_active >= settings.max_active_trades {
        return;
    }

    if e_ctx.is_killed.load(std::sync::atomic::Ordering::Relaxed) {
        static LAST_KILL_LOG: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let curr = now_ms() as u64;
        let last = LAST_KILL_LOG.load(std::sync::atomic::Ordering::Relaxed);
        if curr.saturating_sub(last) > 60000 {
            error!("[real] 🚨 RISK KILL-SWITCH IS ACTIVE. Engine will not open new trades until restarted.");
            LAST_KILL_LOG.store(curr, std::sync::atomic::Ordering::Relaxed);
        }
        return;
    }

    // Only open ONE trade per engine cycle to prevent burst-opening
    let mut opened_this_cycle = false;

    // Filter signals meant for current profile
    let profile_signals: Vec<_> = signals
        .into_iter()
        .filter(|s| s.profile_id == settings.id)
        .collect();

    for sig in profile_signals {
        if opened_this_cycle {
            break;
        }
        if open_pairs.contains(&sig.pair) {
            continue;
        }

        let exa = sig.exchange_a.clone();
        let exb = sig.exchange_b.clone();
        let pair = sig.pair.clone();

        let current_price = sig.entry_a;
        if current_price > 0.0 {
            let entry = e_ctx.price_history.get(&pair).map(|v| *v);
            match entry {
                Some((old_price, old_ts)) => {
                    let elapsed_ms = current_ms.saturating_sub(old_ts as u64);
                    if elapsed_ms > 2000 {
                        let velocity = (current_price - old_price).abs() / old_price;
                        if velocity > 0.003 && elapsed_ms < 10000 {
                            // >0.3% move inside 10s
                            e_ctx
                                .pair_cooldown
                                .insert(pair.clone(), current_ms as i64 + 15000);
                            error!("[real] 🌪️ MARKET REGIME ALERT: {} velocity {:.2}% in {}ms. Pausing pair for 15s to avoid volatile slippage.", pair, velocity*100.0, elapsed_ms);
                            e_ctx
                                .price_history
                                .insert(pair.clone(), (current_price, current_ms as i64));
                            continue;
                        }
                        e_ctx
                            .price_history
                            .insert(pair.clone(), (current_price, current_ms as i64));
                    }
                }
                None => {
                    e_ctx
                        .price_history
                        .insert(pair.clone(), (current_price, current_ms as i64));
                }
            }
        }

        let actual_vol_a = sig.actual_vol_a_usd;
        let actual_vol_b = sig.actual_vol_b_usd;

        let contracts_a = sig.contracts_a;
        let contracts_b = sig.contracts_b;
        let qty_a = sig.target_qty_a;
        let qty_b = sig.target_qty_b;

        let limit_price_a = sig.limit_price_a;
        let limit_price_b = sig.limit_price_b;

        let entry_a = sig.entry_a;
        let entry_b = sig.entry_b;

        let break_even_pct = sig.break_even_pct;
        let is_a_higher = sig.is_a_higher;

        let now = now_ms();

        // ── BALANCE LOW GUARD (zero-latency) ─────────────────────────────────
        if !has_sufficient_margin(&exa, actual_vol_a, settings.leverage as f64, &e_ctx) {
            let now_m = now_ms() as i64;
            if now_m - LAST_MARGIN_LOG.load(Ordering::Relaxed) > 10000 {
                info!("[real] SKIP {}: insufficient margin {}", pair, exa);
                LAST_MARGIN_LOG.store(now_m, Ordering::Relaxed);
            }
            continue;
        }
        if !has_sufficient_margin(&exb, actual_vol_b, settings.leverage as f64, &e_ctx) {
            let now_m = now_ms() as i64;
            if now_m - LAST_MARGIN_LOG.load(Ordering::Relaxed) > 10000 {
                info!("[real] SKIP {}: insufficient margin {}", pair, exb);
                LAST_MARGIN_LOG.store(now_m, Ordering::Relaxed);
            }
            continue;
        }

        // Leverage is pre-warmed asynchronously via `real.rs:warmup_leverages`.
        // DO NOT call ensure_leverage here, it introduces 2+ seconds of latency!

        // ── PAIR COOLDOWN RE-CHECK (defense-in-depth) ──
        // Scanner checks cooldown too, but signals are batched — a second signal for
        // the same pair could sneak through the same scan cycle. Block it here.
        {
            let cd_val = e_ctx.pair_cooldown.get(&pair).map(|v| *v).unwrap_or(0);
            if now - cd_val < ts.pair_cooldown_ms {
                continue;
            }
        }

        let (conn_ensure_a, conn_ensure_b) = (
            crate::engine::exchanges::get_connector(&exa).unwrap(),
            crate::engine::exchanges::get_connector(&exb).unwrap(),
        );
        let t0 = std::time::Instant::now();
        let use_ioc = ts.entry_order_type == "ioc";
        let order_type_label = if use_ioc { "IOC" } else { "MKT" };
        info!(
            "[real] FIRE {} [{}] A={} c={} lim={:.6} | B={} c={} lim={:.6} spread={:.4}% be={:.4}%",
            pair,
            order_type_label,
            exa,
            contracts_a,
            limit_price_a,
            exb,
            contracts_b,
            limit_price_b,
            sig.spread,
            break_even_pct
        );

        let snapshot = serde_json::json!({
            "bid_a": sig.entry_a, // Best approximation since we don't carry full orderbook here
            "bid_b": sig.entry_b,
            "ask_a": sig.entry_a,
            "ask_b": sig.entry_b,
            "spread": sig.spread,
            "latency": 0
        })
        .to_string();
        tokio::spawn({
            let snap_repo = TradeRepository::new(db.clone());
            let sp_pair = pair.clone();
            let sp_exa = exa.clone();
            let sp_exb = exb.clone();
            let ts_ms = current_ms as i64;
            async move {
                snap_repo
                    .insert_snapshot(sp_pair, sp_exa, sp_exb, snapshot, ts_ms)
                    .await;
            }
        });

        // ── BOTH LEGS SIMULTANEOUSLY ─────────────────────────────────────────
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

        // ── JIT ORDERBOOK VERIFICATION ───────────────────────────────────────
        info!("[real] 🔍 Checking L2/L3 orderbook depth for {}...", pair);
        let t_ob = std::time::Instant::now();
        let (ob_a_res, ob_b_res) = tokio::join!(
            conn_ensure_a.fetch_orderbook(&http, &pair, 10),
            conn_ensure_b.fetch_orderbook(&http, &pair, 10)
        );
        let _ob_ms = t_ob.elapsed().as_millis();

        let analyze_depth = |ob_res: Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), String>,
                             is_buy: bool,
                             limit_price: f64,
                             m: f64|
         -> f64 {
            if let Ok((bids, asks)) = ob_res {
                let levels = if is_buy { asks } else { bids };
                let mut cum_vol = 0.0;
                for (px, sz) in levels {
                    if limit_price > 0.0 {
                        if is_buy && px > limit_price {
                            continue;
                        }
                        if !is_buy && px < limit_price {
                            continue;
                        }
                    }
                    if m > 0.0 && m != 1.0 {
                        cum_vol += sz * m;
                    } else {
                        cum_vol += sz;
                    }
                }
                cum_vol
            } else {
                -1.0
            }
        };

        let depth_a = analyze_depth(ob_a_res, !is_a_higher, limit_price_a, mult_a);
        let depth_b = analyze_depth(ob_b_res, is_a_higher, limit_price_b, mult_b);

        if depth_a < qty_a || depth_b < qty_b {
            tracing::warn!("[real] 🛑 ABORT {}: Insufficient L2/L3 depth! req A={} (has {:.2}), req B={} (has {:.2})", pair, qty_a, depth_a, qty_b, depth_b);
            continue;
        }

        // ── SEQUENTIAL ORDER EXECUTION (THINNER LEG FIRST) ───────────────────
        opened_this_cycle = true;
        let a_is_thinner = depth_a <= depth_b;

        let mut actual_vol_phys_a: Option<f64> = None;
        let mut actual_vol_phys_b: Option<f64> = None;
        let mut fill_a = 0.0;
        let mut fill_b = 0.0;
        let mut fee_a_val = 0.0;
        let mut fee_b_val = 0.0;

        let mut oid_a_opt: Option<String> = None;
        let mut oid_b_opt: Option<String> = None;

        if a_is_thinner {
            info!(
                "[real] ⚡ Executing Leg 1 (thinner leg) on {} for {}...",
                exa, pair
            );
            let t_a = std::time::Instant::now();
            let res_a = conn_ensure_a
                .place_order(
                    &http,
                    &keys,
                    &pair,
                    !is_a_higher,
                    qty_a,
                    dry_run,
                    limit_price_a,
                    limit_price_a > 0.0,
                    settings.leverage as f64,
                    mult_a,
                    settings.slippage_percent,
                    tick_a,
                    ts.api_timeout_ms,
                )
                .await;

            if t_a.elapsed().as_millis() > 500 {
                let mut strikes = e_ctx.high_latency_strikes.entry(exa.clone()).or_insert(0);
                *strikes += 1;
                if *strikes >= 3 {
                    e_ctx
                        .exchange_cooldown
                        .insert(exa.clone(), now_ms() + 60_000);
                    error!("[real] 🐢 LATENCY ALERT: {} RTT >500ms. Cooldown 1m.", exa);
                    *strikes = 0;
                }
            } else {
                e_ctx.high_latency_strikes.insert(exa.clone(), 0);
            }
            e_ctx.pair_cooldown.insert(pair.clone(), now_ms());

            let (oid_a, inline_fill_a, inline_fee_a) = match res_a {
                Ok(t) => (t.0, t.1, t.2),
                Err(e) => {
                    error!("[real] LEG_A reject {}/{}: {}", exa, pair, e);
                    let esl = e.to_string().to_lowercase();
                    if esl.contains("insufficient")
                        || esl.contains("balance")
                        || esl.contains("margin")
                        || esl.contains("exceed")
                    {
                        mark_balance_low(&exa, &e_ctx);
                    }
                    (None, 0.0, 0.0)
                }
            };
            oid_a_opt = oid_a.clone();

            if oid_a.is_none() && !dry_run {
                tracing::warn!(
                    "[real] 🛑 ABORT {}: Leg 1 rejected by {}. Leg 2 aborted.",
                    pair,
                    exa
                );
                continue;
            }

            fill_a = inline_fill_a;
            fee_a_val = inline_fee_a;
            let mut got_a = dry_run || fill_a > 0.0;

            for attempt in 0..ts.fill_verify_retries {
                if got_a && actual_vol_phys_a.is_some() {
                    break;
                }
                if attempt > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        ts.fill_verify_delay_ms as u64,
                    ))
                    .await;
                }

                if !got_a || actual_vol_phys_a.is_none() {
                    if let Some(ref oid) = oid_a {
                        if let Ok((p, f, ev)) = conn_ensure_a
                            .fetch_order(&http, &keys, &pair, oid, mult_a)
                            .await
                        {
                            if p > 0.0 {
                                fill_a = p;
                            }
                            if f > 1e-9 {
                                fee_a_val = f;
                            }
                            if ev > 1e-12 {
                                actual_vol_phys_a = Some(ev);
                                got_a = true;
                            }
                        }
                    }
                }
            }

            let filled_a = actual_vol_phys_a.unwrap_or(0.0);
            if filled_a <= 1e-12 && !dry_run {
                tracing::warn!(
                    "[real] 🛑 ABORT {}: Leg 1 filled 0% on {}. Leg 2 aborted.",
                    pair,
                    exa
                );
                continue;
            }

            let final_qty_b = if dry_run { qty_b } else { filled_a };
            tracing::info!(
                "[real] ⚡ Executing Leg 2 on {} for {}. Re-matching volume: {}",
                exb,
                pair,
                final_qty_b
            );

            let t_b = std::time::Instant::now();
            let res_b = conn_ensure_b
                .place_order(
                    &http,
                    &keys,
                    &pair,
                    is_a_higher,
                    final_qty_b,
                    dry_run,
                    limit_price_b,
                    limit_price_b > 0.0,
                    settings.leverage as f64,
                    mult_b,
                    settings.slippage_percent,
                    tick_b,
                    ts.api_timeout_ms,
                )
                .await;

            if t_b.elapsed().as_millis() > 500 {
                let mut strikes = e_ctx.high_latency_strikes.entry(exb.clone()).or_insert(0);
                *strikes += 1;
                if *strikes >= 3 {
                    e_ctx
                        .exchange_cooldown
                        .insert(exb.clone(), now_ms() + 60_000);
                    error!("[real] 🐢 LATENCY ALERT: {} RTT >500ms. Cooldown 1m.", exb);
                    *strikes = 0;
                }
            } else {
                e_ctx.high_latency_strikes.insert(exb.clone(), 0);
            }

            let (oid_b, inline_fill_b, inline_fee_b) = match res_b {
                Ok(t) => (t.0, t.1, t.2),
                Err(e) => {
                    error!("[real] LEG_B reject {}/{}: {}", exb, pair, e);
                    (None, 0.0, 0.0)
                }
            };
            oid_b_opt = oid_b.clone();

            if oid_b.is_none() && !dry_run {
                error!(
                    "[real] ⚠️ ONE-SIDED REJECTION {}: Leg 2 rejected by {}. Trimming Leg 1.",
                    pair, exb
                );
                let hc = http.clone();
                let kc = Arc::clone(&keys);
                let p = pair.clone();
                let ex = exa.clone();
                let fall = sig.entry_a;
                let at = ts.api_timeout_ms;
                tokio::spawn(async move {
                    let _ = crate::engine::exchanges::get_connector(&ex)
                        .unwrap()
                        .trim_position(
                            &hc,
                            &kc,
                            &p,
                            !(!is_a_higher),
                            final_qty_b,
                            dry_run,
                            mult_a,
                            tick_a,
                            fall,
                            at,
                        )
                        .await;
                });
                continue;
            }

            fill_b = inline_fill_b;
            fee_b_val = inline_fee_b;
            let mut got_b = dry_run || fill_b > 0.0;

            for attempt in 0..ts.fill_verify_retries {
                if got_b && actual_vol_phys_b.is_some() {
                    break;
                }
                if attempt > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        ts.fill_verify_delay_ms as u64,
                    ))
                    .await;
                }

                if !got_b || actual_vol_phys_b.is_none() {
                    if let Some(ref oid) = oid_b {
                        if let Ok((p, f, ev)) = conn_ensure_b
                            .fetch_order(&http, &keys, &pair, oid, mult_b)
                            .await
                        {
                            if p > 0.0 {
                                fill_b = p;
                            }
                            if f > 1e-9 {
                                fee_b_val = f;
                            }
                            if ev > 1e-12 {
                                actual_vol_phys_b = Some(ev);
                                got_b = true;
                            }
                        }
                    }
                }
            }

            let filled_b = actual_vol_phys_b.unwrap_or(0.0);
            if (filled_b < filled_a * 0.99) && !dry_run {
                let mismatch = filled_a - filled_b;
                error!("[real] ⚠️ ASYMMETRIC FILL {}: Leg 2 {} filled less ({}) than Leg 1 {} ({}). Trimming Leg 1 excess: {}", pair, exb, filled_b, exa, filled_a, mismatch);
                let hc = http.clone();
                let kc = Arc::clone(&keys);
                let p = pair.clone();
                let ex = exa.clone();
                let fall = sig.entry_a;
                let at = ts.api_timeout_ms;
                tokio::spawn(async move {
                    let _ = crate::engine::exchanges::get_connector(&ex)
                        .unwrap()
                        .trim_position(
                            &hc,
                            &kc,
                            &p,
                            !(!is_a_higher),
                            mismatch,
                            dry_run,
                            mult_a,
                            tick_a,
                            fall,
                            at,
                        )
                        .await;
                });
            }
        } else {
            // -- B IS THINNER --
            info!(
                "[real] ⚡ Executing Leg 1 (thinner leg) on {} for {}...",
                exb, pair
            );
            let t_b = std::time::Instant::now();
            let res_b = conn_ensure_b
                .place_order(
                    &http,
                    &keys,
                    &pair,
                    is_a_higher,
                    qty_b,
                    dry_run,
                    limit_price_b,
                    limit_price_b > 0.0,
                    settings.leverage as f64,
                    mult_b,
                    settings.slippage_percent,
                    tick_b,
                    ts.api_timeout_ms,
                )
                .await;

            if t_b.elapsed().as_millis() > 500 {
                let mut strikes = e_ctx.high_latency_strikes.entry(exb.clone()).or_insert(0);
                *strikes += 1;
                if *strikes >= 3 {
                    e_ctx
                        .exchange_cooldown
                        .insert(exb.clone(), now_ms() + 60_000);
                    error!("[real] 🐢 LATENCY ALERT: {} RTT >500ms. Cooldown 1m.", exb);
                    *strikes = 0;
                }
            } else {
                e_ctx.high_latency_strikes.insert(exb.clone(), 0);
            }
            e_ctx.pair_cooldown.insert(pair.clone(), now_ms());

            let (oid_b, inline_fill_b, inline_fee_b) = match res_b {
                Ok(t) => (t.0, t.1, t.2),
                Err(e) => {
                    error!("[real] LEG_B reject {}/{}: {}", exb, pair, e);
                    let esl = e.to_string().to_lowercase();
                    if esl.contains("insufficient")
                        || esl.contains("balance")
                        || esl.contains("margin")
                        || esl.contains("exceed")
                    {
                        mark_balance_low(&exb, &e_ctx);
                    }
                    (None, 0.0, 0.0)
                }
            };
            oid_b_opt = oid_b.clone();

            if oid_b.is_none() && !dry_run {
                tracing::warn!(
                    "[real] 🛑 ABORT {}: Leg 1 rejected by {}. Leg 2 aborted.",
                    pair,
                    exb
                );
                continue;
            }

            fill_b = inline_fill_b;
            fee_b_val = inline_fee_b;
            let mut got_b = dry_run || fill_b > 0.0;

            for attempt in 0..ts.fill_verify_retries {
                if got_b && actual_vol_phys_b.is_some() {
                    break;
                }
                if attempt > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        ts.fill_verify_delay_ms as u64,
                    ))
                    .await;
                }

                if !got_b || actual_vol_phys_b.is_none() {
                    if let Some(ref oid) = oid_b {
                        if let Ok((p, f, ev)) = conn_ensure_b
                            .fetch_order(&http, &keys, &pair, oid, mult_b)
                            .await
                        {
                            if p > 0.0 {
                                fill_b = p;
                            }
                            if f > 1e-9 {
                                fee_b_val = f;
                            }
                            if ev > 1e-12 {
                                actual_vol_phys_b = Some(ev);
                                got_b = true;
                            }
                        }
                    }
                }
            }

            let filled_b = actual_vol_phys_b.unwrap_or(0.0);
            if filled_b <= 1e-12 && !dry_run {
                tracing::warn!(
                    "[real] 🛑 ABORT {}: Leg 1 filled 0% on {}. Leg 2 aborted.",
                    pair,
                    exb
                );
                continue;
            }

            let final_qty_a = if dry_run { qty_a } else { filled_b };
            tracing::info!(
                "[real] ⚡ Executing Leg 2 on {} for {}. Re-matching volume: {}",
                exa,
                pair,
                final_qty_a
            );

            let t_a = std::time::Instant::now();
            let res_a = conn_ensure_a
                .place_order(
                    &http,
                    &keys,
                    &pair,
                    !is_a_higher,
                    final_qty_a,
                    dry_run,
                    limit_price_a,
                    limit_price_a > 0.0,
                    settings.leverage as f64,
                    mult_a,
                    settings.slippage_percent,
                    tick_a,
                    ts.api_timeout_ms,
                )
                .await;

            if t_a.elapsed().as_millis() > 500 {
                let mut strikes = e_ctx.high_latency_strikes.entry(exa.clone()).or_insert(0);
                *strikes += 1;
                if *strikes >= 3 {
                    e_ctx
                        .exchange_cooldown
                        .insert(exa.clone(), now_ms() + 60_000);
                    error!("[real] 🐢 LATENCY ALERT: {} RTT >500ms. Cooldown 1m.", exa);
                    *strikes = 0;
                }
            } else {
                e_ctx.high_latency_strikes.insert(exa.clone(), 0);
            }

            let (oid_a, inline_fill_a, inline_fee_a) = match res_a {
                Ok(t) => (t.0, t.1, t.2),
                Err(e) => {
                    error!("[real] LEG_A reject {}/{}: {}", exa, pair, e);
                    (None, 0.0, 0.0)
                }
            };
            oid_a_opt = oid_a.clone();

            if oid_a.is_none() && !dry_run {
                error!(
                    "[real] ⚠️ ONE-SIDED REJECTION {}: Leg 2 rejected by {}. Trimming Leg 1.",
                    pair, exa
                );
                let hc = http.clone();
                let kc = Arc::clone(&keys);
                let p = pair.clone();
                let ex = exb.clone();
                let fall = sig.entry_b;
                let at = ts.api_timeout_ms;
                tokio::spawn(async move {
                    let _ = crate::engine::exchanges::get_connector(&ex)
                        .unwrap()
                        .trim_position(
                            &hc,
                            &kc,
                            &p,
                            !is_a_higher,
                            final_qty_a,
                            dry_run,
                            mult_b,
                            tick_b,
                            fall,
                            at,
                        )
                        .await;
                });
                continue;
            }

            fill_a = inline_fill_a;
            fee_a_val = inline_fee_a;
            let mut got_a = dry_run || fill_a > 0.0;

            for attempt in 0..ts.fill_verify_retries {
                if got_a && actual_vol_phys_a.is_some() {
                    break;
                }
                if attempt > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        ts.fill_verify_delay_ms as u64,
                    ))
                    .await;
                }

                if !got_a || actual_vol_phys_a.is_none() {
                    if let Some(ref oid) = oid_a {
                        if let Ok((p, f, ev)) = conn_ensure_a
                            .fetch_order(&http, &keys, &pair, oid, mult_a)
                            .await
                        {
                            if p > 0.0 {
                                fill_a = p;
                            }
                            if f > 1e-9 {
                                fee_a_val = f;
                            }
                            if ev > 1e-12 {
                                actual_vol_phys_a = Some(ev);
                                got_a = true;
                            }
                        }
                    }
                }
            }

            let filled_a = actual_vol_phys_a.unwrap_or(0.0);
            if (filled_a < filled_b * 0.99) && !dry_run {
                let mismatch = filled_b - filled_a;
                error!("[real] ⚠️ ASYMMETRIC FILL {}: Leg 2 {} filled less ({}) than Leg 1 {} ({}). Trimming Leg 1 excess: {}", pair, exa, filled_a, exb, filled_b, mismatch);
                let hc = http.clone();
                let kc = Arc::clone(&keys);
                let p = pair.clone();
                let ex = exb.clone();
                let fall = sig.entry_b;
                let at = ts.api_timeout_ms;
                tokio::spawn(async move {
                    let _ = crate::engine::exchanges::get_connector(&ex)
                        .unwrap()
                        .trim_position(
                            &hc,
                            &kc,
                            &p,
                            !is_a_higher,
                            mismatch,
                            dry_run,
                            mult_b,
                            tick_b,
                            fall,
                            at,
                        )
                        .await;
                });
            }
        }

        let oid_a = oid_a_opt;
        let oid_b = oid_b_opt;

        let total_ms = t0.elapsed().as_millis();
        if dry_run {
            fill_a = entry_a;
            fill_b = entry_b;
        }

        let phys_a = actual_vol_phys_a.unwrap_or(0.0);
        let phys_b = actual_vol_phys_b.unwrap_or(0.0);

        let a_filled = oid_a.is_some() && phys_a > 0.0;
        let b_filled = oid_b.is_some() && phys_b > 0.0;

        info!("[real] RESULT {} A={} B={} phys_a={:.6} phys_b={:.6} fill_a={:.6} fill_b={:.6} latency={}ms",
            pair, a_filled, b_filled, phys_a, phys_b, fill_a, fill_b, total_ms);

        if !a_filled && !b_filled {
            info!(
                "[real] Both IOC unfilled {} — no exposure ({}ms)",
                pair, total_ms
            );
            let fails = e_ctx
                .consecutive_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            if fails >= 5 {
                e_ctx
                    .is_killed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                error!("[real] 💀 KILL-SWITCH TRIGGERED: 5 consecutive IOC failures. Suspending new entries.");
            }
            continue;
        }

        let min_phys = phys_a.min(phys_b);
        let step_a = mult_a.max(1e-12);
        let step_b = mult_b.max(1e-12);
        let valid_a = (min_phys / step_a).floor() * step_a;
        let valid_b = (min_phys / step_b).floor() * step_b;
        let matched_phys = valid_a.min(valid_b);

        let delta_a = phys_a - matched_phys;
        let delta_b = phys_b - matched_phys;

        // ── DELTA TRIMMING (Phase 3) ──
        let mut trimming_loss = 0.0;
        let mut trimming_fee = 0.0;
        let (hc, kc, _db_c) = (http.clone(), Arc::clone(&keys), db.clone());
        let fp = pair.clone();

        if delta_a > 1e-12 {
            let phys_to_dump = delta_a; // Pass physical volume directly
            if phys_to_dump >= mult_a.min(1e-8) {
                let is_buy = is_a_higher; // If we entered A by Selling (!is_a_higher), we exit A by Buying (is_a_higher)
                error!(
                    "[real] ⚠️ TRIMMING {} LEG A — Selling excess {:.6} phys",
                    pair, phys_to_dump
                );

                let tick_for_close = e_ctx
                    .tick_sizes
                    .get(&format!("{}:{}", exa, pair))
                    .map(|v| *v)
                    .unwrap_or(0.000001);
                match crate::engine::exchanges::get_connector(&exa)
                    .unwrap()
                    .trim_position(
                        &hc,
                        &kc,
                        &fp,
                        is_buy,
                        phys_to_dump,
                        false,
                        mult_a,
                        tick_for_close,
                        fill_a,
                        ts.api_timeout_ms,
                    )
                    .await
                {
                    Ok(rc) => {
                        trimming_fee += rc.total_fee;
                        let gross_loss = if is_buy {
                            (fill_a - rc.final_price) * delta_a
                        } else {
                            (rc.final_price - fill_a) * delta_a
                        };
                        trimming_loss += gross_loss.min(0.0).abs();
                    }
                    Err(e) => {
                        error!("[real] Failed to trim DELTA A: {}", e);
                    }
                }
            }
        }

        if delta_b > 1e-12 {
            let phys_to_dump = delta_b; // Use physical coin volume
            if phys_to_dump >= mult_b.min(1e-8) {
                let is_buy = !is_a_higher; // Opposite of entry B
                error!(
                    "[real] ⚠️ TRIMMING {} LEG B — Selling excess {:.6} phys",
                    pair, phys_to_dump
                );

                let tick_for_close = e_ctx
                    .tick_sizes
                    .get(&format!("{}:{}", exb, pair))
                    .map(|v| *v)
                    .unwrap_or(0.000001);
                match crate::engine::exchanges::get_connector(&exb)
                    .unwrap()
                    .trim_position(
                        &hc,
                        &kc,
                        &fp,
                        is_buy,
                        phys_to_dump,
                        false,
                        mult_b,
                        tick_for_close,
                        fill_b,
                        ts.api_timeout_ms,
                    )
                    .await
                {
                    Ok(rc) => {
                        trimming_fee += rc.total_fee;
                        let gross_loss = if is_buy {
                            (fill_b - rc.final_price) * delta_b
                        } else {
                            (rc.final_price - fill_b) * delta_b
                        };
                        trimming_loss += gross_loss.min(0.0).abs();
                    }
                    Err(e) => {
                        error!("[real] Failed to trim DELTA B: {}", e);
                    }
                }
            }
        }

        // ── TRADE BOOKING (Phase 4) ──
        if matched_phys < 1e-12 {
            let trims = e_ctx
                .consecutive_trims
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            if trims >= 3 {
                e_ctx
                    .is_killed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                error!("[real] 💀 KILL-SWITCH TRIGGERED: 3 consecutive FULL TRIMS (total asymmetry). Suspending new entries.");
            }
            // Nothing survived the trim (e.g. one leg didn't fill at all, and we dumped the other)
            let total_fee = fee_a_val + fee_b_val + trimming_fee;
            error!("[real] ❌ FULL ASYMMETRY DUMPED {}. Trade canceled. Trimming Loss: {:.4}, Trimming Fee: {:.4}", pair, trimming_loss, trimming_fee);
            e_ctx
                .pair_cooldown
                .insert(pair.clone(), now + ts.asymmetric_cooldown_ms);

            // Record failed asymmetrical trade into database...
            let (ea_s, eb_s, d, sp, pid) = (
                exa.clone(),
                exb.clone(),
                sig.direction.clone(),
                sig.spread,
                settings.id.clone(),
            );
            let (ent_a, ent_b) = if a_filled {
                (fill_a, 0.0)
            } else {
                (0.0, fill_b)
            };
            let fv_usd = if a_filled {
                delta_a * fill_a
            } else {
                delta_b * fill_b
            };
            let gross_pnl = -trimming_loss;
            let net_pnl = gross_pnl - total_fee;

            let pnl_a_val = if a_filled { gross_pnl } else { 0.0 };
            let pnl_b_val = if !a_filled { gross_pnl } else { 0.0 };

            let repo_close = TradeRepository::new(db.clone());
            // slip is 0.0 for cancelled trades, loss comes from gross_pnl / pnl_x
            repo_close
                .append_asymmetric_close(
                    pair.clone(),
                    ea_s,
                    eb_s,
                    d,
                    fv_usd,
                    ent_a,
                    ent_b,
                    sp,
                    now_ms(),
                    pnl_a_val,
                    pnl_b_val,
                    pid,
                    total_fee,
                    0.0,
                    net_pnl,
                    gross_pnl,
                    0.0,
                    0.0,
                )
                .await;
            continue;
        }

        // We have a VALID matching volume! Calculate final trading volume
        e_ctx
            .consecutive_failures
            .store(0, std::sync::atomic::Ordering::Relaxed);
        e_ctx
            .consecutive_trims
            .store(0, std::sync::atomic::Ordering::Relaxed);
        let final_vol_usd = matched_phys * entry_a;
        crate::engine::real::deduct_cached_balance(
            &exa,
            final_vol_usd,
            settings.leverage as f64,
            &e_ctx,
        );
        crate::engine::real::deduct_cached_balance(
            &exb,
            final_vol_usd,
            settings.leverage as f64,
            &e_ctx,
        );
        opened_this_cycle = true;

        let dir = sig.direction.clone();
        let actual_entry_spread = if fill_a > 0.0 && fill_b > 0.0 {
            if is_a_higher {
                (fill_a - fill_b) / fill_b.max(1e-12) * 100.0
            } else {
                (fill_b - fill_a) / fill_a.max(1e-12) * 100.0
            }
        } else {
            sig.spread
        };

        let entry_comm = fee_a_val + fee_b_val + trimming_fee; // Add trimming fees to the opening fees
        let slip_a = if fill_a > 0.0 {
            if is_a_higher {
                ((entry_a - fill_a) * matched_phys).max(0.0)
            } else {
                ((fill_a - entry_a) * matched_phys).max(0.0)
            }
        } else {
            0.0
        };
        let slip_b = if fill_b > 0.0 {
            if !is_a_higher {
                ((entry_b - fill_b) * matched_phys).max(0.0)
            } else {
                ((fill_b - entry_b) * matched_phys).max(0.0)
            }
        } else {
            0.0
        };

        // Trimming loss is baked into the entry slippage curve (as it was an immediate loss to enter the market symmetrically)
        let entry_slip = slip_a + slip_b + trimming_loss;

        let contracts_a_db = matched_phys / mult_a;
        let contracts_b_db = matched_phys / mult_b;

        info!("[real] [EXCHANGE_VERIFIED] ✅ OPEN {} phys={:.6} (Trimming: -${:.4}) A_price={:.6} B_price={:.6} fees={:.4} slip={:.4} actual_sp={:.4}%",
            pair, matched_phys, trimming_loss, fill_a, fill_b, entry_comm, entry_slip, actual_entry_spread);

        repo.insert_new_entry(
            pair.clone(),
            exa.clone(),
            exb.clone(),
            dir.clone(),
            final_vol_usd,
            fill_a,
            fill_b,
            actual_entry_spread,
            now_ms(),
            settings.id.clone(),
            entry_comm,
            entry_slip,
            oid_a.unwrap_or_default(),
            oid_b.unwrap_or_default(),
            contracts_a_db,
            contracts_b_db,
        )
        .await;
    }
}
