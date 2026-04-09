use std::sync::Arc;
use tokio::time::Duration;
use reqwest::Client;
use tracing::{info, warn, error};
use tokio_rusqlite::{Connection, params};
use crate::engine::db::TradeRepository;

use shared::{TradingSettings, TechnicalSettings, SpreadRow};
use crate::state::{EngineContext, PriceMap};
use crate::engine::real::{ExchangeKeys, Trade, load_open_trades, now_ms, now_secs, has_sufficient_margin, binance_sign}; // added signatures if needed
use std::collections::{HashMap, HashSet};

static LAST_LIVE_FLUSH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub async fn run_cycle(spreads: Vec<SpreadRow>, prices: crate::state::PriceMap, settings: TradingSettings, http: Client, db: Connection, keys: Arc<ExchangeKeys>, ts: &TechnicalSettings, e_ctx: Arc<crate::state::EngineContext>) {
    let dry_run = false;
    let repo = TradeRepository::new(db.clone());

    let trades = repo.load_open_trades(&settings.id).await;
    let num_active = trades.len() as i64;
    let funding = settings.funding_rate_8h / 100.0;

    // NOTE: We process closes ALWAYS (even when is_active=false)
    // so that MANUAL_CLOSE from the UI works even when the bot is stopped.
    // Only new trade entries are gated by is_active.

    // ── Close qualifying trades ────────────────────────────────────────────────
    if !trades.is_empty() {
        info!("[real] Processing {} open trades for profile {}", trades.len(), settings.id);
    }
    let mut live_updates: Vec<(i64, f64, f64, f64, f64, f64, f64, i64)> = Vec::new();
    for trade in &trades {
        let is_a_higher = trade.direction.starts_with(&trade.exchange_a);
        let elapsed_ms = now_ms() - trade.entry_time;

        let manual_close = trade.close_reason.as_deref() == Some("MANUAL_CLOSE");
        if manual_close {
            info!("[real] MANUAL_CLOSE detected for trade {} {} on {}/{}", trade.id, trade.pair, trade.exchange_a, trade.exchange_b);
        }

        // CRITICAL: skip trades where entry prices aren't confirmed yet (fast-poll pending)
        // Bypass this check if the user is forcing a manual close.
        if (trade.entry_price_a <= 0.0 || trade.entry_price_b <= 0.0) && !manual_close {
            info!("[real] Skipping close for {} {} — entry prices not confirmed yet", trade.id, trade.pair);
            continue;
        }

        let data_a = prices.get(&trade.exchange_a).and_then(|m| m.get(&trade.pair).cloned());
        let data_b = prices.get(&trade.exchange_b).and_then(|m| m.get(&trade.pair).cloned());

        if (data_a.is_none() || data_b.is_none()) && !manual_close { continue; }
        
        // Fallback prices in case of missing WebSocket data during a manual close
        let mut exit_price_a = trade.entry_price_a.max(1e-12);
        let mut exit_price_b = trade.entry_price_b.max(1e-12);
        let mut c_price_a = exit_price_a;
        let mut c_price_b = exit_price_b;
        let mut current_spread = 0.0;
        let mut exit_spread = 0.0;

        if let (Some(da), Some(db)) = (&data_a, &data_b) {
            if da.bid <= 0.0 || da.ask <= 0.0 || db.bid <= 0.0 || db.ask <= 0.0 {
                if !manual_close { continue; }
            } else {
                c_price_a = da.price;
                c_price_b = db.price;
                exit_price_a = if is_a_higher { da.ask } else { da.bid };
                exit_price_b = if is_a_higher { db.bid } else { db.ask };
                
                current_spread = if is_a_higher {
                    (da.price - db.price) / db.price.max(1e-12) * 100.0
                } else {
                    (db.price - da.price) / da.price.max(1e-12) * 100.0
                };
                exit_spread = if is_a_higher {
                    (da.ask - db.bid) / db.bid.max(1e-12) * 100.0
                } else {
                    (db.ask - da.bid) / da.bid.max(1e-12) * 100.0
                };
            }
        }

        let contracts_a = trade.contracts_a as f64;
        let contracts_b = trade.contracts_b as f64;

        // PnL for DISPLAY (using mark/mid prices — matches exchange PnL)
        let live_pnl_a = if is_a_higher { (trade.entry_price_a - c_price_a) * (trade.volume / trade.entry_price_a.max(1e-12)) }
                    else { (c_price_a - trade.entry_price_a) * (trade.volume / trade.entry_price_a.max(1e-12)) };
        let live_pnl_b = if is_a_higher { (c_price_b - trade.entry_price_b) * (trade.volume / trade.entry_price_b.max(1e-12)) }
                    else { (trade.entry_price_b - c_price_b) * (trade.volume / trade.entry_price_b.max(1e-12)) };
        let live_gross_pnl = live_pnl_a + live_pnl_b;
        let fund_cost = funding * trade.volume;
        let live_net_pnl = live_gross_pnl - trade.commission_cost - fund_cost;

        // PnL for CLOSE TRIGGERS (using bid/ask — pessimistic, for safe execution)
        let exec_pnl_a = if is_a_higher { (trade.entry_price_a - exit_price_a) * (trade.volume / trade.entry_price_a.max(1e-12)) }
                    else { (exit_price_a - trade.entry_price_a) * (trade.volume / trade.entry_price_a.max(1e-12)) };
        let exec_pnl_b = if is_a_higher { (exit_price_b - trade.entry_price_b) * (trade.volume / trade.entry_price_b.max(1e-12)) }
                    else { (trade.entry_price_b - exit_price_b) * (trade.volume / trade.entry_price_b.max(1e-12)) };
        let exec_gross_pnl = exec_pnl_a + exec_pnl_b;
        let exec_net_pnl = exec_gross_pnl - trade.commission_cost - fund_cost;
        let exec_net_pct = exec_net_pnl / trade.volume * 100.0;

        let top_spread = trade.highest_spread.max(current_spread);
        let stop_limit = top_spread - settings.trailing_stop_percent;

        let triggered_tp   = exec_net_pct >= settings.take_profit_percent;
        let triggered_stop = top_spread > 0.0 && current_spread < stop_limit;
        let zero_spread    = exit_spread <= 0.0 && exec_net_pnl >= 0.0;

        if manual_close || triggered_tp || triggered_stop || zero_spread {
            let reason = if manual_close { "MANUAL" } else if triggered_tp { "TP" } else if zero_spread { "ZERO_SPREAD" } else { "STOP" };
            info!("[real] Closing {} {} reason={} pct={:.3}% exit_sp={:.4} norm={:.4}", trade.id, trade.pair, reason, exec_net_pct, exit_spread,
                spreads.iter().find(|s| s.pair == trade.pair && s.direction == trade.direction).map(|s| s.structural_spread).unwrap_or(0.0));

            let exa = trade.exchange_a.clone(); let exb = trade.exchange_b.clone(); let pair = trade.pair.clone();
            let c_a = trade.contracts_a; let c_b = trade.contracts_b;
            let id = trade.id; let vol = trade.volume;
            let entry_a = trade.entry_price_a; let entry_b = trade.entry_price_b;
            let h = http.clone(); let k = Arc::clone(&keys); let db_c = db.clone();
            let r = reason.to_string();
            let lev = settings.leverage;
            let exit_sanity = ts.exit_sanity_pct;
            let close_verify_delay = ts.close_verify_delay_ms as u64;
            let close_verify_retries = ts.close_verify_retries;

            let use_ioc = ts.entry_order_type == "ioc";
            let max_slip_frac = (settings.slippage_percent / 100.0).max(ts.min_ioc_tolerance_pct / 100.0);
            let tick_a = e_ctx.tick_sizes.get(&format!("{}:{}", exa, pair)).map(|v| *v).unwrap_or(0.000001);
            let tick_b = e_ctx.tick_sizes.get(&format!("{}:{}", exb, pair)).map(|v| *v).unwrap_or(0.000001);
            let mult_a = e_ctx.multipliers.get(&format!("{}:{}", exa, pair)).map(|v| *v).unwrap_or(1.0);
            let mult_b = e_ctx.multipliers.get(&format!("{}:{}", exb, pair)).map(|v| *v).unwrap_or(1.0);

            let (limit_close_a, limit_close_b) = if use_ioc {
                let lpa = if is_a_higher { exit_price_a * (1.0 + max_slip_frac) } else { exit_price_a * (1.0 - max_slip_frac) };
                let lpb = if !is_a_higher { exit_price_b * (1.0 + max_slip_frac) } else { exit_price_b * (1.0 - max_slip_frac) };
                ((lpa / tick_a).round() * tick_a, (lpb / tick_b).round() * tick_b)
            } else {
                (0.0, 0.0)
            };

            tokio::spawn(async move {
                let conn_a = crate::engine::exchanges::get_connector(&exa).unwrap();
                let conn_b = crate::engine::exchanges::get_connector(&exb).unwrap();
                // Place close orders in parallel
                let (ra, rb) = tokio::join!(
                    conn_a.close_order(&h, &k, &pair, is_a_higher, c_a, dry_run, lev as f64, limit_close_a, use_ioc),
                    conn_b.close_order(&h, &k, &pair, !is_a_higher, c_b, dry_run, lev as f64, limit_close_b, use_ioc)
                );
                let oid_close_a = ra.ok();
                let oid_close_b = rb.ok();

                // Mark CLOSED immediately with current prices and estimated PnL
                let repo_close = TradeRepository::new(db_c.clone());
                repo_close.mark_trade_closed(id, exit_price_a, exit_price_b, exit_spread, exec_net_pnl, exec_gross_pnl, exec_pnl_a, exec_pnl_b, &r, fund_cost).await;

                // Fast-poll real exit prices and recalculate PnL
                let mut real_exit_a = exit_price_a; let mut real_exit_b = exit_price_b;
                let mut exit_fee = 0.0; let mut got_a = false; let mut got_b = false;

                for _ in 0..close_verify_retries {
                    if !got_a {
                        if let Some(ref oid) = oid_close_a {
                            if let Ok::<(f64, f64), String>((p, f)) = crate::engine::exchanges::get_connector(&exa).unwrap().fetch_order(&h, &k, &pair, oid, mult_a).await {
                                if p > 0.0 {
                                    real_exit_a = p;
                                    let rate_a = e_ctx.fee_rates.get(&exa).map(|v| *v).unwrap_or(0.0005);
                                    let actual_f = if f > 1e-9 { f.abs() } else { p * (vol / entry_a) * rate_a };
                                    exit_fee += actual_f; got_a = true;
                                }
                            }
                        } else { got_a = true; }
                    }
                    if !got_b {
                        if let Some(ref oid) = oid_close_b {
                            if let Ok::<(f64, f64), String>((p, f)) = crate::engine::exchanges::get_connector(&exb).unwrap().fetch_order(&h, &k, &pair, oid, mult_b).await {
                                if p > 0.0 {
                                    real_exit_b = p;
                                    let rate_b = e_ctx.fee_rates.get(&exb).map(|v| *v).unwrap_or(0.0005);
                                    let actual_f = if f > 1e-9 { f.abs() } else { p * (vol / entry_b) * rate_b };
                                    exit_fee += actual_f; got_b = true;
                                }
                            }
                        } else { got_b = true; }
                    }
                    if got_a && got_b { break; }
                    tokio::time::sleep(tokio::time::Duration::from_millis(close_verify_delay)).await;
                }

                // Recalculate PnL with real exit prices
                let real_pna = if is_a_higher { (entry_a - real_exit_a) * (vol / entry_a) } else { (real_exit_a - entry_a) * (vol / entry_a) };
                let real_pnb = if is_a_higher { (real_exit_b - entry_b) * (vol / entry_b) } else { (entry_b - real_exit_b) * (vol / entry_b) };
                let real_gp = real_pna + real_pnb;
                
                // Exit slippage cost — clamped to >= 0 (price improvement is not cost)
                let slip_cost_exit_a = if real_exit_a > 0.0 {
                    if is_a_higher { ((real_exit_a - exit_price_a) * (vol / entry_a)).max(0.0) }
                    else           { ((exit_price_a - real_exit_a) * (vol / entry_a)).max(0.0) }
                } else { 0.0 };
                let slip_cost_exit_b = if real_exit_b > 0.0 {
                    if !is_a_higher { ((real_exit_b - exit_price_b) * (vol / entry_b)).max(0.0) }
                    else            { ((exit_price_b - real_exit_b) * (vol / entry_b)).max(0.0) }
                } else { 0.0 };
                let exit_slip = slip_cost_exit_a + slip_cost_exit_b;

                info!("[real] PnL raw id={} pna={:.4} pnb={:.4} gross={:.4} exit_fee={:.4} exit_slip={:.4}", id, real_pna, real_pnb, real_gp, exit_fee, exit_slip);

                let repo_close = TradeRepository::new(db_c.clone());
                repo_close.update_trade_real_exit(id, real_exit_a, real_exit_b, real_pna, real_pnb, real_gp, exit_fee, exit_slip).await;
            });
        } else {
            // Fetch real funding rates from exchange APIs (cached 5 min)
            let (fr_a, next_a) = crate::engine::exchanges::get_connector(&trade.exchange_a).unwrap().get_funding_rate(&http, &trade.pair).await.unwrap_or((0.0, 0));
            let (fr_b, next_b) = crate::engine::exchanges::get_connector(&trade.exchange_b).unwrap().get_funding_rate(&http, &trade.pair).await.unwrap_or((0.0, 0));
            let fund_next = next_a.min(next_b); // nearest funding event
            live_updates.push((trade.id, c_price_a, c_price_b, current_spread, live_net_pnl, fr_a, fr_b, fund_next));
            if top_spread > trade.highest_spread {
                let id = trade.id;
                // Wait, need await here, earlier it was synchronous?
                // The original code was synchronous `db.call` which returns a Future but was not awaited! I will await it!
                let rb = repo.clone();
                tokio::spawn(async move { rb.update_highest_spread(id, top_spread).await; });
            }
        }
    }

    static LAST_LIVE_FLUSH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let current_ms = now_ms() as u64;
    let elapsed = current_ms.saturating_sub(LAST_LIVE_FLUSH.load(std::sync::atomic::Ordering::Relaxed));
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
    if !settings.is_active { return; } // only block NEW entries, not closes above
    if num_active >= settings.max_active_trades { return; }
    let open_pairs: std::collections::HashSet<String> = trades.iter().map(|t| t.pair.clone()).collect();

    // ── Cooldown maps (persist across cycles to prevent spam) ─────────────────
    let ex_cd = &e_ctx.exchange_cooldown;
    let pair_cd = &e_ctx.pair_cooldown;
    let now = now_ms();

    // Diagnostic logging (throttled)
    static DIAG_TICK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dtick = DIAG_TICK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if dtick % ts.diag_log_interval as u64 == 0 {
        let fees: Vec<_> = e_ctx.fee_rates.iter().map(|e| format!("{}={:.3}%", e.key(), e.value() * 100.0)).collect();
        let in_range = spreads.iter().filter(|r| r.spread >= settings.min_entry_spread && r.spread <= settings.max_entry_spread).count();
        let supported = spreads.iter().filter(|r| keys.supports(&r.exchange_a) && keys.supports(&r.exchange_b)).count();
        let cooled_ex: Vec<_> = ex_cd.iter().filter(|e| now - *e.value() < ts.exchange_cooldown_ms).map(|e| e.key().clone()).collect();
        info!("[diag] spreads={} in_range={} supported={} active={}/{} cooled={:?} fees={:?}",
            spreads.len(), in_range, supported, num_active, settings.max_active_trades, cooled_ex, fees);
    }

    // Only open ONE trade per engine cycle to prevent burst-opening
    let mut opened_this_cycle = false;

    for row in spreads.iter().take(ts.max_spreads_per_cycle as usize) {
        if opened_this_cycle { break; }
        if row.spread < settings.min_entry_spread || row.spread > settings.max_entry_spread { continue; }

        // Absolute floor: spread must exceed round-trip costs from REAL exchange fee rates
        let fee_a = crate::engine::real::get_exchange_fee(&row.exchange_a, ts.default_fee_rate, &e_ctx);
        let fee_b = crate::engine::real::get_exchange_fee(&row.exchange_b, ts.default_fee_rate, &e_ctx);
        let slippage_frac = settings.slippage_percent / 100.0;
        let break_even_pct = (2.0 * (fee_a + fee_b) + 4.0 * slippage_frac) * 100.0;

        // Safety margin and deviation checks have been removed as per user request.

        if open_pairs.contains(&row.pair) { continue; }

        if !keys.supports(&row.exchange_a) || !keys.supports(&row.exchange_b) { info!("[real] SKIP {}: missing exchange keys", row.pair); continue; }
        if settings.disabled_exchanges.contains(&row.exchange_a) || settings.disabled_exchanges.contains(&row.exchange_b) { info!("[real] SKIP {}: disabled exchange", row.pair); continue; }

        let cd_a = ex_cd.get(&row.exchange_a).map(|v| *v).unwrap_or(0);
        let cd_b = ex_cd.get(&row.exchange_b).map(|v| *v).unwrap_or(0);
        if now - cd_a < ts.exchange_cooldown_ms || now - cd_b < ts.exchange_cooldown_ms { info!("[real] SKIP {}: exchange on cooldown", row.pair); continue; }

        let cd_pair = pair_cd.get(&row.pair).map(|v| *v).unwrap_or(0);
        if now - cd_pair < ts.pair_cooldown_ms { info!("[real] SKIP {}: pair on cooldown", row.pair); continue; }

        let is_a_higher = row.direction.starts_with(&row.exchange_a);
        let mut entry_a = if is_a_higher { row.bid_a } else { row.ask_a };
        let mut entry_b = if is_a_higher { row.ask_b } else { row.bid_b };

        let price_ratio = if entry_a > entry_b { entry_a / entry_b } else { entry_b / entry_a };
        if price_ratio > ts.max_price_ratio { info!("[real] SKIP {}: price_ratio {:.3} > max", row.pair, price_ratio); continue; }

        // ── LIQUIDITY DEPTH FILTER ────────────────────────────────────────────
        let data_a = prices.get(&row.exchange_a).and_then(|m| m.get(&row.pair).cloned());
        let data_b = prices.get(&row.exchange_b).and_then(|m| m.get(&row.pair).cloned());
        let min_depth_usd = settings.trade_volume * ts.depth_multiplier;
        if let (Some(ref da), Some(ref db)) = (&data_a, &data_b) {
            let depth_sell = if is_a_higher { da.bid_size } else { db.bid_size };
            let depth_buy  = if is_a_higher { db.ask_size } else { da.ask_size };
            if depth_sell > 0.0 && depth_sell < min_depth_usd { info!("[real] SKIP {}: depth_sell {:.1} < min {:.1}", row.pair, depth_sell, min_depth_usd); continue; }
            if depth_buy > 0.0 && depth_buy < min_depth_usd { info!("[real] SKIP {}: depth_buy {:.1} < min {:.1}", row.pair, depth_buy, min_depth_usd); continue; }
            let intra_a = (da.ask - da.bid) / da.bid.max(1e-12) * 100.0;
            let intra_b = (db.ask - db.bid) / db.bid.max(1e-12) * 100.0;
            // FIX: configurable intra-spread max (was hardcoded 0.5%)
            if intra_a > ts.intra_spread_max_pct || intra_b > ts.intra_spread_max_pct { info!("[real] SKIP {}: intra_spread a={:.2} b={:.2} > max {:.2}", row.pair, intra_a, intra_b, ts.intra_spread_max_pct); continue; }
        }

        let map = &e_ctx.multipliers;
        let mult_a = match map.get(&format!("{}:{}", row.exchange_a, row.pair)) { Some(v) => *v, None => { info!("[real] SKIP {}: no mult_a", row.pair); continue } };
        let mult_b = match map.get(&format!("{}:{}", row.exchange_b, row.pair)) { Some(v) => *v, None => { info!("[real] SKIP {}: no mult_b", row.pair); continue } };

        let contracts_a = ((settings.trade_volume / entry_a.max(1e-12)) / mult_a).round();
        if contracts_a < 1.0 { info!("[real] SKIP {}: contracts_a < 1", row.pair); continue; }
        let actual_vol_a = contracts_a * mult_a * entry_a;
        let contracts_b = ((actual_vol_a / entry_b.max(1e-12)) / mult_b).round();
        if contracts_b < 1.0 { info!("[real] SKIP {}: contracts_b < 1", row.pair); continue; }
        let actual_vol_b = contracts_b * mult_b * entry_b;
        let vol_diff_pct = (actual_vol_a - actual_vol_b).abs() / actual_vol_a * 100.0;
        if vol_diff_pct > ts.vol_diff_max_pct { info!("[real] SKIP {}: vol_diff {:.2} > max {:.2}", row.pair, vol_diff_pct, ts.vol_diff_max_pct); continue; }

        let max_v = settings.trade_volume * (1.0 + ts.vol_tolerance_pct / 100.0);
        if actual_vol_a > max_v { info!("[real] SKIP {}: actual_vol {:.2} > max_v {:.2}", row.pair, actual_vol_a, max_v); continue; }

        let lmap = &e_ctx.launch_times;
        let now_s = now_secs();
        let min_age_secs = ts.min_listing_age_days * 24 * 3600;
        let age_a = lmap.get(&format!("{}:{}", row.exchange_a, row.pair)).map(|r| *r).unwrap_or(0);
        let age_b = lmap.get(&format!("{}:{}", row.exchange_b, row.pair)).map(|r| *r).unwrap_or(0);
        if (age_a > 0 && now_s - age_a < min_age_secs) || (age_b > 0 && now_s - age_b < min_age_secs) { info!("[real] SKIP {}: new coin listing", row.pair); continue; }

        if !has_sufficient_margin(&row.exchange_a, actual_vol_a, &e_ctx) { info!("[real] SKIP {}: insufficient margin {}", row.pair, row.exchange_a); continue; }
        if !has_sufficient_margin(&row.exchange_b, actual_vol_b, &e_ctx) { info!("[real] SKIP {}: insufficient margin {}", row.pair, row.exchange_b); continue; }
        if (row.exchange_a == "binance" && actual_vol_a < ts.binance_min_notional) || (row.exchange_b == "binance" && actual_vol_b < ts.binance_min_notional) { info!("[real] SKIP {}: binance min notional", row.pair); continue; }

        let has_tick_a = e_ctx.tick_sizes.contains_key(&format!("{}:{}", row.exchange_a, row.pair));
        let has_tick_b = e_ctx.tick_sizes.contains_key(&format!("{}:{}", row.exchange_b, row.pair));
        if !has_tick_a || !has_tick_b { info!("[real] SKIP {}: tick size missing", row.pair); continue; }
        let tick_a = e_ctx.tick_sizes.get(&format!("{}:{}", row.exchange_a, row.pair)).map(|v| *v).unwrap_or(0.000001);
        let tick_b = e_ctx.tick_sizes.get(&format!("{}:{}", row.exchange_b, row.pair)).map(|v| *v).unwrap_or(0.000001);

        let use_ioc = ts.entry_order_type == "ioc";
        let (mut limit_price_a, mut limit_price_b) = if use_ioc {
            let user_slip_frac = settings.slippage_percent / 100.0;
            let ioc_min_frac = ts.min_ioc_tolerance_pct / 100.0;
            let max_slip_frac = user_slip_frac.max(ioc_min_frac);
            if max_slip_frac > user_slip_frac {
                info!("[real] WARN {}: Slippage overridden from {:.2}% to {:.2}% for IOC", row.pair, settings.slippage_percent, ts.min_ioc_tolerance_pct);
            }
            let lpa = if is_a_higher { entry_a * (1.0 - max_slip_frac) } else { entry_a * (1.0 + max_slip_frac) };
            let lpb = if is_a_higher { entry_b * (1.0 + max_slip_frac) } else { entry_b * (1.0 - max_slip_frac) };
            
            let lpa_rounded = (lpa / tick_a).round() * tick_a;
            let lpb_rounded = (lpb / tick_b).round() * tick_b;
            (lpa_rounded, lpb_rounded)
        } else {
            (0.0, 0.0) // market order — price=0 means no limit
        };

        let exa = row.exchange_a.clone();
        let exb = row.exchange_b.clone();
        let pair = row.pair.clone();

        // Leverage: parallel, cached after first call → instant on subsequent
        let (conn_ensure_a, conn_ensure_b) = (
            crate::engine::exchanges::get_connector(&exa).unwrap(),
            crate::engine::exchanges::get_connector(&exb).unwrap()
        );
        tokio::join!(
            conn_ensure_a.ensure_leverage(&http, &keys, &pair, settings.leverage as f64),
            conn_ensure_b.ensure_leverage(&http, &keys, &pair, settings.leverage as f64)
        );

        // ── PRE-EXECUTION SPREAD RE-CHECK ───────────────────────────────────
        // After leverage setup (which may take 200-500ms on first call per pair),
        // re-read FRESH bid/ask from in-memory PriceMap and verify spread hasn't collapsed.
        // This prevents entering on stale prices that moved during the leverage API call.
        let fresh_a = prices.get(&exa).and_then(|m| m.get(&pair).cloned());
        let fresh_b = prices.get(&exb).and_then(|m| m.get(&pair).cloned());
        if let (Some(fa), Some(fb)) = (&fresh_a, &fresh_b) {
            let fresh_entry_a = if is_a_higher { fa.bid } else { fa.ask };
            let fresh_entry_b = if is_a_higher { fb.ask } else { fb.bid };
            let fresh_spread = if is_a_higher {
                (fa.bid - fb.ask) / fb.ask.max(1e-12) * 100.0
            } else {
                (fb.bid - fa.ask) / fa.ask.max(1e-12) * 100.0
            };
            if fresh_spread < settings.min_entry_spread {
                info!("[real] PRE-EXEC ABORT {} — spread collapsed {:.4}% → {:.4}% (be={:.4}%)",
                    pair, row.spread, fresh_spread, break_even_pct);
                continue;
            }
            // Use the fresh prices for the actual order
            entry_a = fresh_entry_a;
            entry_b = fresh_entry_b;
            // Re-calc limit prices if IOC mode
            if use_ioc {
                let max_slip_frac = (settings.slippage_percent / 100.0).max(ts.min_ioc_tolerance_pct / 100.0);
                let raw_lpa = if is_a_higher { entry_a * (1.0 - max_slip_frac) } else { entry_a * (1.0 + max_slip_frac) };
                let raw_lpb = if is_a_higher { entry_b * (1.0 + max_slip_frac) } else { entry_b * (1.0 - max_slip_frac) };
                limit_price_a = (raw_lpa / tick_a).round() * tick_a;
                limit_price_b = (raw_lpb / tick_b).round() * tick_b;
            }
        }

        let t0 = std::time::Instant::now();

        let order_type_label = if use_ioc { "IOC" } else { "MKT" };
        info!("[real] FIRE {} [{}] A={} c={} lim={:.6} | B={} c={} lim={:.6} spread={:.4}% be={:.4}%",
            pair, order_type_label, exa, contracts_a, limit_price_a, exb, contracts_b, limit_price_b, row.spread, break_even_pct);

        // ── BOTH LEGS SIMULTANEOUSLY ─────────────────────────────────────────
        let conn_place_a = crate::engine::exchanges::get_connector(&exa).unwrap();
        let conn_place_b = crate::engine::exchanges::get_connector(&exb).unwrap();
        let is_qty_based_a = exa == "binance" || exa == "bybit";
        let is_qty_based_b = exb == "binance" || exb == "bybit";
        let qty_a = if is_qty_based_a { contracts_a * mult_a } else { contracts_a };
        let qty_b = if is_qty_based_b { contracts_b * mult_b } else { contracts_b };

        let (result_a, result_b) = tokio::join!(
            conn_place_a.place_order(&http, &keys, &pair, !is_a_higher, qty_a, dry_run, limit_price_a, limit_price_a > 0.0),
            conn_place_b.place_order(&http, &keys, &pair, is_a_higher, qty_b, dry_run, limit_price_b, limit_price_b > 0.0)
        );

        let order_ms = t0.elapsed().as_millis();

        // Destructure (order_id, inline_fill_price, inline_fill_fee)
        let (oid_a, inline_fill_a, inline_fee_a) = match result_a {
            Ok(t) => (t.0, t.1, 0.0),
            Err(e) => {
                let es = e.to_string();
                error!("[real] LEG_A reject {}/{}: {} ({}ms)", exa, pair, es, order_ms);
                
                (None, 0.0, 0.0)
            }
        };
        let (oid_b, inline_fill_b, inline_fee_b) = match result_b {
            Ok(t) => (t.0, t.1, 0.0),
            Err(e) => {
                let es = e.to_string();
                error!("[real] LEG_B reject {}/{}: {} ({}ms)", exb, pair, es, order_ms);
                
                (None, 0.0, 0.0)
            }
        };

        if oid_a.is_none() && oid_b.is_none() { pair_cd.insert(pair.clone(), now); continue; }

        // ── PARALLEL FILL VERIFICATION ───────────────────────────────────────
        // Pre-seed with inline fill data (Binance provides avgPrice in order response)
        let mut fill_a = inline_fill_a; let mut fee_a_val = inline_fee_a;
        let mut fill_b = inline_fill_b; let mut fee_b_val = inline_fee_b;
        let mut got_a = oid_a.is_none() || dry_run || fill_a > 0.0;
        let mut got_b = oid_b.is_none() || dry_run || fill_b > 0.0;

        for attempt in 0..ts.fill_verify_retries {
            if got_a && got_b { break; }
            // For MARKET orders, skip verification — fill is guaranteed, use WS prices
            if !use_ioc { break; }
            if attempt > 0 { tokio::time::sleep(Duration::from_millis(ts.fill_verify_delay_ms as u64)).await; }
            let (fa, fb) = tokio::join!(
                async { if !got_a { if let Some(ref oid) = oid_a { if let Ok::<(f64, f64), String>((p, f)) = crate::engine::exchanges::get_connector(&exa).unwrap().fetch_order(&http, &keys, &pair, oid.as_str(), mult_a).await { if p > 0.0 { let rate = e_ctx.fee_rates.get(&exa).map(|v| *v).unwrap_or(0.0005); let af = if f > 1e-9 { f.abs() } else { p * qty_a * rate }; return Some((p, af)); } } } } None },
                async { if !got_b { if let Some(ref oid) = oid_b { if let Ok::<(f64, f64), String>((p, f)) = crate::engine::exchanges::get_connector(&exb).unwrap().fetch_order(&http, &keys, &pair, oid.as_str(), mult_b).await { if p > 0.0 { let rate = e_ctx.fee_rates.get(&exb).map(|v| *v).unwrap_or(0.0005); let af = if f > 1e-9 { f.abs() } else { p * qty_b * rate }; return Some((p, af)); } } } } None }
            );
            if let Some((p, f)) = fa { fill_a = p; fee_a_val = f; got_a = true; }
            if let Some((p, f)) = fb { fill_b = p; fee_b_val = f; got_b = true; }
        }

        // For MARKET orders: fill is guaranteed, use WS bid/ask as fill price if not yet known
        if !use_ioc {
            if oid_a.is_some() && fill_a <= 0.0 { fill_a = entry_a; }
            if oid_b.is_some() && fill_b <= 0.0 { fill_b = entry_b; }
        }

        let total_ms = t0.elapsed().as_millis();
        if dry_run { fill_a = entry_a; fill_b = entry_b; }

        let a_filled = oid_a.is_some() && fill_a > 0.0;
        let b_filled = oid_b.is_some() && fill_b > 0.0;

        info!("[real] RESULT {} A={} B={} fill_a={:.6} fill_b={:.6} orders={}ms total={}ms",
            pair, a_filled, b_filled, fill_a, fill_b, order_ms, total_ms);

        if !a_filled && !b_filled {
            info!("[real] Both IOC unfilled {} — no exposure ({}ms)", pair, total_ms);
            pair_cd.insert(pair.clone(), now);
            continue;
        }

        if a_filled && b_filled {
            // ✅ SUCCESS
            crate::engine::real::deduct_cached_balance(&exa, actual_vol_a);
            crate::engine::real::deduct_cached_balance(&exb, actual_vol_b);
            opened_this_cycle = true;

            let vol = settings.trade_volume;
            let dir = row.direction.clone();
            let sp = row.spread;
            let profile_id = settings.id.clone();
            let api_comm = fee_a_val + fee_b_val;
            // Use exchange API fees. If API returned 0, estimate with standard 0.05% taker rate per leg
            let entry_comm = if api_comm > 1e-9 { api_comm } else {
                let est = vol * 2.0 * 0.0005; // 0.05% per leg (industry standard max)
                tracing::warn!("[real] No API fee for {} — using estimate {:.4}", pair, est);
                est
            };
            let slip_a = if fill_a > 0.0 { if is_a_higher { ((entry_a - fill_a) * (vol / entry_a)).max(0.0) } else { ((fill_a - entry_a) * (vol / entry_a)).max(0.0) } } else { 0.0 };
            let slip_b = if fill_b > 0.0 { if !is_a_higher { ((entry_b - fill_b) * (vol / entry_b)).max(0.0) } else { ((fill_b - entry_b) * (vol / entry_b)).max(0.0) } } else { 0.0 };
            let entry_slip = slip_a + slip_b;
            let entry_time = now_ms();
            let oid_a_db = oid_a.unwrap_or_default();
            let oid_b_db = oid_b.unwrap_or_default();
            // Clone before move into DB closure for async verify
            let oid_a_for_verify = oid_a_db.clone();
            let oid_b_for_verify = oid_b_db.clone();
            let exa_for_verify = exa.clone();
            let exb_for_verify = exb.clone();
            let pair_for_verify = pair.clone();

            info!("[real] ✅ OPEN {} A={:.6}(c={}) B={:.6}(c={}) fees={:.4} slip={:.4} latency={}ms",
                pair, fill_a, contracts_a, fill_b, contracts_b, entry_comm, entry_slip, total_ms);

            repo.insert_new_entry(pair.clone(), exa.clone(), exb.clone(), dir.clone(), vol, fill_a, fill_b, sp, entry_time, profile_id.clone(), entry_comm, entry_slip, oid_a_db, oid_b_db, contracts_a, contracts_b).await;

            // ── ASYNC FILL PRICE CORRECTION (market orders) ──────────────────
            // For market orders where we used WS bid/ask as fill price, spawn a
            // background task to fetch the real fill price and update the DB.
            if !use_ioc && (inline_fill_a <= 0.0 || inline_fill_b <= 0.0) {
                let (h2, k2, db2) = (http.clone(), Arc::clone(&keys), db.clone());
                let (ex_a2, ex_b2, pair2) = (exa_for_verify, exb_for_verify, pair_for_verify);
                let oid_a2 = oid_a_for_verify; let oid_b2 = oid_b_for_verify;
                let verify_retries = ts.fill_verify_retries;
                let verify_delay = ts.fill_verify_delay_ms as u64;
                let q_a = qty_a; let q_b = qty_b;
                let rate_a = e_ctx.fee_rates.get(&ex_a2).map(|v| *v).unwrap_or(0.0005);
                let rate_b = e_ctx.fee_rates.get(&ex_b2).map(|v| *v).unwrap_or(0.0005);
                let e_a = entry_a; let e_b = entry_b;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(200)).await; // let exchange settle
                    let mut real_a = 0.0f64; let mut real_b = 0.0f64;
                    let mut ga = inline_fill_a > 0.0; let mut gb = inline_fill_b > 0.0;
                    let mut fee_a = if ga { real_a = inline_fill_a; inline_fee_a } else { e_a * q_a * rate_a };
                    let mut fee_b = if gb { real_b = inline_fill_b; inline_fee_b } else { e_b * q_b * rate_b };
                    let ex_a2_c = ex_a2.clone(); let pair2_a = pair2.clone(); let pair2_b = pair2.clone();
                    for _ in 0..verify_retries {
                        if ga && gb { break; }
                        if !ga && !oid_a2.is_empty() {
                            if let Ok::<(f64, f64), String>((p, f)) = crate::engine::exchanges::get_connector(&ex_a2).unwrap().fetch_order(&h2, &k2, &pair2_a, oid_a2.as_str(), mult_a).await {
                                if p > 0.0 { real_a = p; fee_a = if f > 1e-9 { f.abs() } else { p * q_a * rate_a }; ga = true; }
                            }
                        }
                        if !gb && !oid_b2.is_empty() {
                            if let Ok::<(f64, f64), String>((p, f)) = crate::engine::exchanges::get_connector(&ex_b2).unwrap().fetch_order(&h2, &k2, &pair2_b, oid_b2.as_str(), mult_b).await {
                                if p > 0.0 { real_b = p; fee_b = if f > 1e-9 { f.abs() } else { p * q_b * rate_b }; gb = true; }
                            }
                        }
                        if !ga || !gb { tokio::time::sleep(Duration::from_millis(verify_delay.max(100))).await; }
                    }
                    if ga || gb {
                        let total_fee = fee_a + fee_b;
                        let repo_entry = TradeRepository::new(db2.clone());
                        repo_entry.correct_entry_prices(oid_a2.clone(), oid_b2.clone(), real_a, real_b, total_fee).await;
                        info!("[real] ASYNC_VERIFY {} fill_a={:.6} fill_b={:.6} fee={:.4}", pair2, real_a, real_b, total_fee);
                    }
                });
            }
        } else {
            // ⚠️ ASYMMETRIC — emergency close the filled leg
            let (fe, _fo, fs, fc, fp_price, ff, fv) = if a_filled {
                (exa.clone(), oid_a.as_ref().unwrap().clone(), is_a_higher, contracts_a, fill_a, fee_a_val, actual_vol_a)
            } else {
                (exb.clone(), oid_b.as_ref().unwrap().clone(), !is_a_higher, contracts_b, fill_b, fee_b_val, actual_vol_b)
            };

            error!("[real] ⚠️ ASYMMETRIC {} — only {} filled @ {:.6} ({}ms)", pair, fe, fp_price, total_ms);
            pair_cd.insert(pair.clone(), now + ts.asymmetric_cooldown_ms);

            let (hc, kc, db_c) = (http.clone(), Arc::clone(&keys), db.clone());
            let fp = pair.clone();
            let (ea_s, eb_s, d, sp, pid) = (exa.clone(), exb.clone(), row.direction.clone(), row.spread, settings.id.clone());
            let lev = settings.leverage;
            let emergency_retries = ts.emergency_close_retries;

            tokio::spawn(async move {
                let mut close_oid = None;
                for attempt in 1u32..=emergency_retries as u32 {
                    match crate::engine::exchanges::get_connector(&fe).unwrap().close_order(&hc, &kc, &fp, fs, fc, false, lev as f64, 0.0, false).await {
                        Ok(id) => { error!("🛡️ Emergency close ok {}/{}: {}", fe, fp, id); close_oid = Some(id); break; }
                        Err(err) => { tokio::time::sleep(Duration::from_millis(50 * attempt as u64)).await; error!("Emergency close {}/{}: {}", attempt, emergency_retries, err); }
                    }
                }
                let mut close_fill = fp_price;
                let mut total_fee = ff;
                let m = e_ctx.multipliers.get(&format!("{}:{}", fe, fp)).map(|v| *v).unwrap_or(1.0);
                let rate = e_ctx.fee_rates.get(&fe).map(|v| *v).unwrap_or(0.0005);
                let q = if fc > 0.0 { fc } else { fv / fp_price };
                
                if let Some(ref coid) = close_oid {
                    for _ in 0..5 {
                        if let Ok::<(f64, f64), String>((p, f)) = crate::engine::exchanges::get_connector(&fe).unwrap().fetch_order(&hc, &kc, &fp, coid.as_str(), m).await {
                            if p > 0.0 { close_fill = p; total_fee += if f > 1e-9 { f.abs() } else { p * q * rate }; break; }
                        }
                        tokio::time::sleep(Duration::from_millis(15)).await;
                    }
                }
                let qty = fv / fp_price.max(1e-12);
                let gross_pnl = if fs { (fp_price - close_fill) * qty } else { (close_fill - fp_price) * qty };
                let slip = if fs { ((close_fill - fp_price) * qty).max(0.0) } else { ((fp_price - close_fill) * qty).max(0.0) };
                
                let net_pnl = gross_pnl - total_fee;
                let entry_time = now_ms();

                error!("[real] ASYMMETRIC PnL {}: gross={:.4} fee={:.4} net={:.4}", fp, gross_pnl, total_fee, net_pnl);

                let (ent_a, ent_b) = if a_filled { (fp_price, 0.0f64) } else { (0.0f64, fp_price) };
                let (ext_a, ext_b) = if a_filled { (close_fill, 0.0f64) } else { (0.0f64, close_fill) };
                let (pnl_a_val, pnl_b_val) = if a_filled { (gross_pnl, 0.0f64) } else { (0.0f64, gross_pnl) };

                let repo_close = TradeRepository::new(db_c.clone());
                repo_close.append_asymmetric_close(fp, ea_s, eb_s, d, fv, ent_a, ent_b, sp, entry_time, pnl_a_val, pnl_b_val, pid, total_fee, slip, net_pnl, gross_pnl, ext_a, ext_b).await;
            });
        }
    }
}