//! HFT / Latency Arbitrage Cycle Engine
//!
//! Completely isolated from the EMA reversion engine (cycle.rs).
//! - Opens trades the same way (thinner leg first, sequential)
//! - Closes by TIMEOUT (hft_timeout_ms) or when spread reverts ≤ 0
//! - No trailing stop, no take-profit percentage
//! - Close reasons: HFT_TIMEOUT, HFT_REVERT, MANUAL_CLOSE

use crate::engine::db::TradeRepository;
use reqwest::Client;
use std::sync::Arc;
use tokio_rusqlite::Connection;
use tracing::{error, info};

use crate::engine::real::now_ms;
use crate::engine::utils::cooldown_active;
use crate::engine::risk_manager::{has_sufficient_margin, mark_balance_low};
use shared::{SpreadRow, TechnicalSettings, TradingSettings};
use std::sync::atomic::AtomicI64;

static LAST_HFT_MARGIN_LOG: AtomicI64 = AtomicI64::new(0);

pub async fn run_hft_cycle(
    signals: Vec<shared::TradeSignal>,
    _spreads: Vec<SpreadRow>,
    prices: crate::state::PriceMap,
    settings: TradingSettings,
    http: Client,
    db: Connection,
    keys: Arc<crate::engine::real::ExchangeKeys>,
    ts: &TechnicalSettings,
    e_ctx: Arc<crate::state::EngineContext>,
) {
    // Only process HFT profiles
    if settings.strategy_type != "hft" {
        return;
    }

    let dry_run = false;
    let repo = TradeRepository::new(db.clone());
    let trades = repo.load_open_trades(&settings.id).await;
    let num_active = trades.len() as i64;
    let open_pairs: std::collections::HashSet<String> =
        trades.iter().map(|t| t.pair.clone()).collect();

    // ── HFT CLOSE LOGIC ─────────────────────────────────────────────────────
    // Two exit conditions:
    // 1) hft_timeout_ms elapsed since entry
    // 2) spread reverted to ≤ 0 (instant profit-take)
    // 3) MANUAL_CLOSE from UI
    let mut live_updates: Vec<(i64, f64, f64, f64, f64, f64, f64, i64, f64, f64, f64)> = Vec::new();

    for trade in &trades {
        let is_a_higher = trade.direction.starts_with(&trade.exchange_a);
        let elapsed_ms = now_ms() - trade.entry_time;
        let manual_close = trade.close_reason.as_deref() == Some("MANUAL_CLOSE");
        let closing_retry = trade.status == "CLOSING";

        if (trade.entry_price_a <= 0.0 || trade.entry_price_b <= 0.0) && !manual_close && !closing_retry {
            continue;
        }

        let data_a = prices.get(&trade.exchange_a).and_then(|m| m.get(&trade.pair).cloned());
        let data_b = prices.get(&trade.exchange_b).and_then(|m| m.get(&trade.pair).cloned());

        if (data_a.is_none() || data_b.is_none()) && !manual_close && !closing_retry {
            continue;
        }

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

        let trade_coins = trade.volume / trade.entry_price_a.max(1e-12);

        // Live PnL
        let live_pnl_a = if is_a_higher {
            (trade.entry_price_a - c_price_a) * trade_coins
        } else {
            (c_price_a - trade.entry_price_a) * trade_coins
        };
        let live_pnl_b = if is_a_higher {
            (c_price_b - trade.entry_price_b) * trade_coins
        } else {
            (trade.entry_price_b - c_price_b) * trade_coins
        };
        let live_gross_pnl = live_pnl_a + live_pnl_b;
        let live_net_pnl = live_gross_pnl - trade.commission_cost - trade.slippage_cost - trade.funding_cost;

        // ── HFT EXIT CONDITIONS ──
        let timeout_hit = elapsed_ms >= settings.hft_timeout_ms;
        // Exit when the entry-direction spread has reverted to zero.
        // current_spread <= 0 means bid_a ≈ ask_b — the price gap is gone.
        // This is the optimal exit: we've captured the full arbitrage move.
        //
        // NOTE: exit_spread (ask_a - bid_b) stays positive even at current_spread=0
        // because it measures the cost to reverse (bid-ask cross), NOT spread closure.
        // Using exit_spread <= 0 would require full price inversion — too late, often unreachable.
        // Exit costs at current_spread=0 are already baked into break_even_pct (4x slippage).
        let spread_reverted = current_spread <= 0.0;

        let mut reason_str = "";
        if manual_close { reason_str = "MANUAL"; }
        else if timeout_hit { reason_str = "HFT_TIMEOUT"; }
        else if spread_reverted { reason_str = "HFT_REVERT"; }

        if closing_retry || manual_close || timeout_hit || spread_reverted {
            if e_ctx.closing_in_flight.get(&trade.id).map(|v| *v).unwrap_or(false) {
                continue;
            }

            let exa = trade.exchange_a.clone();
            let exb = trade.exchange_b.clone();
            let pair = trade.pair.clone();

            let mult_a = e_ctx.multipliers.get(&format!("{}:{}", exa, pair)).map(|v| *v).unwrap_or(1.0);
            let mult_b = e_ctx.multipliers.get(&format!("{}:{}", exb, pair)).map(|v| *v).unwrap_or(1.0);

            let reason = if reason_str.is_empty() {
                trade.close_reason.as_deref().unwrap_or("HFT_RETRY")
            } else {
                reason_str
            };
            if !repo.begin_close_attempt(trade.id, reason).await {
                continue;
            }
            e_ctx.closing_in_flight.insert(trade.id, true);
            info!("[hft] Closing {} {} reason={} elapsed={}ms curr_spread={:.4}% exit_spread={:.4}%",
                trade.id, pair, reason, elapsed_ms, current_spread, exit_spread);

            let id = trade.id;
            let entry_a = trade.entry_price_a;
            let entry_b = trade.entry_price_b;
            let tc = trade_coins;
            let h = http.clone();
            let k = Arc::clone(&keys);
            let db_c = db.clone();
            let r = reason.to_string();
            let lev = settings.leverage;
            let close_verify_delay = ts.close_verify_delay_ms as u64;
            let close_verify_retries = ts.close_verify_retries;
            let api_timeout_ms = ts.api_timeout_ms;
            let fund_cost = trade.funding_cost;
            let use_ioc = true; // HFT always IOC
            let tick_a = e_ctx.tick_sizes.get(&format!("{}:{}", exa, pair)).map(|v| *v).unwrap_or(0.000001);
            let tick_b = e_ctx.tick_sizes.get(&format!("{}:{}", exb, pair)).map(|v| *v).unwrap_or(0.000001);
            let q_a = trade.contracts_a as f64 * mult_a;
            let q_b = trade.contracts_b as f64 * mult_b;
            let closing_guard = Arc::clone(&e_ctx.closing_in_flight);
            let trade_id_for_guard = trade.id;
            let _comm_cost = trade.commission_cost;
            let _slip_cost = trade.slippage_cost;

            tokio::spawn(async move {
                let conn_a = crate::engine::exchanges::get_connector(&exa).unwrap();
                let conn_b = crate::engine::exchanges::get_connector(&exb).unwrap();

                let cr = crate::engine::execution::execute_close(
                    &conn_a, &conn_b, &h, &k, &pair,
                    is_a_higher, q_a, q_b, lev as f64,
                    exit_price_a, exit_price_b,
                    use_ioc, 0.5, mult_a, mult_b, tick_a, tick_b,
                    close_verify_retries as u32, close_verify_delay, api_timeout_ms,
                    entry_a, entry_b, tc, tc,
                ).await;

                let repo_close = TradeRepository::new(db_c);
                if cr.real_exit_a <= 0.0 || cr.real_exit_b <= 0.0 {
                    error!("[hft] CLOSE FAILED id={} {}: {}", id, pair, cr.error_msg);
                    repo_close.record_close_failure(id, &cr.error_msg, cr.leg_a_external || cr.leg_b_external || r == "MANUAL").await;
                    closing_guard.remove(&trade_id_for_guard);
                    return;
                }

                let close_reason_db = if cr.leg_a_external && cr.leg_b_external { "EXTERNAL_CLOSE".to_string() }
                    else if cr.leg_a_external || cr.leg_b_external { "EXTERNAL_PARTIAL".to_string() }
                    else { r.clone() };

                let real_gp = cr.pnl_a + cr.pnl_b;
                info!("[hft] [VERIFIED] PnL id={} gross={:.4} fee={:.4} reason={}", id, real_gp, cr.exit_fee, close_reason_db);

                repo_close.mark_trade_closed_confirmed(
                    id, cr.real_exit_a, cr.real_exit_b, exit_spread, real_gp,
                    cr.pnl_a, cr.pnl_b, &close_reason_db, fund_cost, cr.exit_fee, 0.0,
                ).await;
                closing_guard.remove(&trade_id_for_guard);
            });
        } else {
            // Live update (no funding for HFT — trades last seconds)
            let top_spread = trade.highest_spread.max(current_spread);
            live_updates.push((
                trade.id, c_price_a, c_price_b, current_spread,
                live_net_pnl, 0.0, 0.0, 0, 0.0, live_pnl_a, live_pnl_b,
            ));
            if top_spread > trade.highest_spread {
                let rb = repo.clone();
                let id = trade.id;
                tokio::spawn(async move { rb.update_highest_spread(id, top_spread).await; });
            }
        }
    }

    // Flush live updates
    static LAST_HFT_FLUSH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let current_ms = now_ms() as u64;
    let elapsed = current_ms.saturating_sub(LAST_HFT_FLUSH.load(std::sync::atomic::Ordering::Relaxed));
    if elapsed > ts.live_update_throttle_ms as u64 && !live_updates.is_empty() {
        LAST_HFT_FLUSH.store(current_ms, std::sync::atomic::Ordering::Relaxed);
        let db_clone = db.clone();
        tokio::spawn(async move {
            TradeRepository::new(db_clone).flush_live_updates(live_updates).await;
        });
    }

    // ── OPEN NEW HFT TRADES ──────────────────────────────────────────────────
    if !settings.is_active { return; }
    if num_active >= settings.max_active_trades { return; }
    if e_ctx.is_killed.load(std::sync::atomic::Ordering::Relaxed) { return; }

    let mut opened_this_cycle = false;
    let profile_signals: Vec<_> = signals.into_iter().filter(|s| s.profile_id == settings.id).collect();

    for sig in profile_signals {
        if opened_this_cycle { break; }
        if open_pairs.contains(&sig.pair) { continue; }

        let exa = sig.exchange_a.clone();
        let exb = sig.exchange_b.clone();
        let pair = sig.pair.clone();
        let now = now_ms();

        // Margin check
        if !has_sufficient_margin(&exa, sig.actual_vol_a_usd, settings.leverage as f64, &e_ctx) {
            continue;
        }
        if !has_sufficient_margin(&exb, sig.actual_vol_b_usd, settings.leverage as f64, &e_ctx) {
            continue;
        }

        // Pair cooldown
        let cd_val = e_ctx.pair_cooldown.get(&pair).map(|v| *v).unwrap_or(0);
        if cooldown_active(cd_val, now, ts.pair_cooldown_ms) { continue; }

        let conn_a = crate::engine::exchanges::get_connector(&exa).unwrap();
        let conn_b = crate::engine::exchanges::get_connector(&exb).unwrap();

        let is_a_higher = sig.is_a_higher;
        let mult_a = sig.mult_a;
        let mult_b = sig.mult_b;
        let tick_a = sig.tick_a;
        let tick_b = sig.tick_b;
        let qty_a = sig.target_qty_a;
        let qty_b = sig.target_qty_b;
        let limit_price_a = sig.limit_price_a;
        let limit_price_b = sig.limit_price_b;
        let entry_a = sig.entry_a;
        let entry_b = sig.entry_b;

        let t0 = std::time::Instant::now();
        info!("[hft] ⚡ FIRE {} spread={:.4}% on {}/{}", pair, sig.spread, exa, exb);

        // HFT skips JIT orderbook verification for speed — L1 depth was already checked in hft_scanner

        // ── SEQUENTIAL EXECUTION (always IOC) ──
        opened_this_cycle = true;
        let mut fill_a = 0.0;
        let mut fill_b = 0.0;
        let mut fee_a_val = 0.0;
        let mut fee_b_val = 0.0;
        let mut oid_a: Option<String> = None;
        let mut oid_b: Option<String> = None;
        let mut actual_vol_phys_a: Option<f64> = None;
        let mut actual_vol_phys_b: Option<f64> = None;

        // Leg A first (always — HFT doesn't check depth ordering for speed)
        let res_a = conn_a.place_order(
            &http, &keys, &pair, !is_a_higher, qty_a, dry_run,
            limit_price_a, true, settings.leverage as f64, mult_a,
            settings.slippage_percent, tick_a, ts.api_timeout_ms,
        ).await;
        e_ctx.pair_cooldown.insert(pair.clone(), now_ms());

        let (o_a, inf_a, inf_fee_a) = match res_a {
            Ok(t) => (t.0, t.1, t.2),
            Err(e) => {
                error!("[hft] LEG_A reject {}/{}: {}", exa, pair, e);
                if e.to_lowercase().contains("margin") || e.to_lowercase().contains("insufficient") {
                    mark_balance_low(&exa, &e_ctx);
                }
                (None, 0.0, 0.0)
            }
        };
        oid_a = o_a.clone();
        if o_a.is_none() && !dry_run { continue; }
        fill_a = inf_a;
        fee_a_val = inf_fee_a;

        // Verify fill A
        let vr_a = crate::engine::execution::verify_fill(
            &conn_a, &http, &keys, &pair, &oid_a,
            fill_a, fee_a_val, actual_vol_phys_a, qty_a, dry_run,
            &crate::engine::execution::VerifyConfig {
                fast_retries: ts.fill_verify_retries,
                fast_delay_ms: ts.fill_verify_delay_ms as u64,
                slow_retries: 6,
                slow_delay_ms: 400,
                position_fallback: true,
                mult: mult_a,
            },
            "hft",
        ).await;
        fill_a = vr_a.fill_price;
        fee_a_val = vr_a.fee;
        actual_vol_phys_a = vr_a.phys_volume;

        let filled_a = actual_vol_phys_a.unwrap_or(0.0);
        if filled_a <= 1e-12 && !dry_run { continue; }

        // Leg B — match volume from A
        let final_qty_b = if dry_run { qty_b } else { filled_a };
        let res_b = conn_b.place_order(
            &http, &keys, &pair, is_a_higher, final_qty_b, dry_run,
            limit_price_b, true, settings.leverage as f64, mult_b,
            settings.slippage_percent, tick_b, ts.api_timeout_ms,
        ).await;

        let (o_b, inf_b, inf_fee_b) = match res_b {
            Ok(t) => (t.0, t.1, t.2),
            Err(e) => {
                error!("[hft] LEG_B reject {}/{}: {}", exb, pair, e);
                // Trim A on failure
                let hc = http.clone(); let kc = Arc::clone(&keys);
                let p = pair.clone(); let ex = exa.clone(); let fall = entry_a; let at = ts.api_timeout_ms;
                tokio::spawn(async move {
                    let _ = crate::engine::exchanges::get_connector(&ex).unwrap()
                        .trim_position(&hc, &kc, &p, !(!is_a_higher), final_qty_b, false, mult_a, tick_a, fall, at).await;
                });
                (None, 0.0, 0.0)
            }
        };
        oid_b = o_b.clone();
        if o_b.is_none() && !dry_run { continue; }
        fill_b = inf_b;
        fee_b_val = inf_fee_b;

        // Verify fill B
        let vr_b = crate::engine::execution::verify_fill(
            &conn_b, &http, &keys, &pair, &oid_b,
            fill_b, fee_b_val, actual_vol_phys_b, final_qty_b, dry_run,
            &crate::engine::execution::VerifyConfig {
                fast_retries: ts.fill_verify_retries,
                fast_delay_ms: ts.fill_verify_delay_ms as u64,
                slow_retries: 6,
                slow_delay_ms: 400,
                position_fallback: false,
                mult: mult_b,
            },
            "hft",
        ).await;
        fill_b = vr_b.fill_price;
        fee_b_val = vr_b.fee;
        actual_vol_phys_b = vr_b.phys_volume;

        let phys_a = actual_vol_phys_a.unwrap_or(0.0);
        let phys_b = actual_vol_phys_b.unwrap_or(0.0);
        let a_filled = oid_a.is_some() && phys_a > 0.0;
        let b_filled = oid_b.is_some() && phys_b > 0.0;
        let total_ms = t0.elapsed().as_millis();

        info!("[hft] RESULT {} A={} B={} latency={}ms", pair, a_filled, b_filled, total_ms);

        if !a_filled && !b_filled {
            e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + ts.pair_cooldown_ms);
            continue;
        }

        // Match volumes
        let mv = crate::engine::execution::match_volumes(phys_a, phys_b, mult_a, mult_b);
        let matched_phys = mv.matched_phys;

        if matched_phys < 1e-12 {
            e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + ts.asymmetric_cooldown_ms);
            error!("[hft] FULL ASYMMETRY {}", pair);
            continue;
        }

        // Book the trade
        if dry_run { fill_a = entry_a; fill_b = entry_b; }
        let final_vol_usd = matched_phys * entry_a;
        crate::engine::risk_manager::deduct_cached_balance(&exa, final_vol_usd, settings.leverage as f64, &e_ctx);
        crate::engine::risk_manager::deduct_cached_balance(&exb, final_vol_usd, settings.leverage as f64, &e_ctx);

        // 🛡️ Apply cooldown to the pair so we don't spam duplicate entries
        e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + ts.pair_cooldown_ms);

        let actual_entry_spread = crate::engine::execution::compute_entry_spread(
            fill_a, fill_b, is_a_higher, sig.spread,
        );

        let entry_comm = fee_a_val + fee_b_val;
        let contracts_a_db = matched_phys / mult_a;
        let contracts_b_db = matched_phys / mult_b;

        info!("[hft] ✅ OPEN {} phys={:.6} fill_a={:.6} fill_b={:.6} spread={:.4}% latency={}ms",
            pair, matched_phys, fill_a, fill_b, actual_entry_spread, total_ms);

        repo.insert_new_entry(
            pair, exa, exb, sig.direction.clone(), final_vol_usd,
            fill_a, fill_b, actual_entry_spread, now_ms(), settings.id.clone(),
            entry_comm, 0.0,
            oid_a.unwrap_or_default(), oid_b.unwrap_or_default(),
            contracts_a_db, contracts_b_db,
        ).await;
    }
}
