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

#[allow(dead_code)]
static LAST_HFT_MARGIN_LOG: AtomicI64 = AtomicI64::new(0);

#[allow(unused_assignments)]
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

        let live_state = crate::engine::pnl::calculate_live_pnl_state(
            &trade, &prices, &e_ctx, manual_close, closing_retry,
        );

        if !live_state.is_valid {
            continue;
        }

        let elapsed_ms = now_ms() - trade.entry_time;

        // ── HFT EXIT CONDITIONS ──
        let timeout_hit = elapsed_ms >= settings.hft_timeout_ms;
        // Exit when the entry-direction spread has reverted to zero.
        let spread_reverted = live_state.current_spread <= 0.0;

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
            
            let key_a = format!("{}:{}", exa, pair);
            let key_b = format!("{}:{}", exb, pair);

            let mult_a = e_ctx.multipliers.get(&key_a).map(|v| *v).unwrap_or(1.0);
            let mult_b = e_ctx.multipliers.get(&key_b).map(|v| *v).unwrap_or(1.0);

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
                trade.id, pair, reason, elapsed_ms, live_state.current_spread, live_state.exit_spread);

            let tick_a = e_ctx.tick_sizes.get(&key_a).map(|v| *v).unwrap_or(0.000001);
            let tick_b = e_ctx.tick_sizes.get(&key_b).map(|v| *v).unwrap_or(0.000001);

            let ctx_close = crate::engine::execution::CloseTaskContext {
                trade_id: trade.id,
                pair: pair.clone(),
                exa: exa.clone(),
                exb: exb.clone(),
                is_a_higher: trade.direction.starts_with(&exa),
                q_a: live_state.coins_a,
                q_b: live_state.coins_b,
                mult_a,
                mult_b,
                tick_a,
                tick_b,
                leverage: settings.leverage as f64,
                exit_price_a: live_state.exit_price_a,
                exit_price_b: live_state.exit_price_b,
                use_ioc: true, // HFT always IOC
                slippage_percent: 0.5,
                entry_a: trade.entry_price_a,
                entry_b: trade.entry_price_b,
                trade_coins_a: live_state.coins_a,
                trade_coins_b: live_state.coins_b,
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
            // Live update (no funding for HFT — trades last seconds)
            let top_spread = trade.highest_spread.max(live_state.current_spread);
            let live_net_pnl = crate::engine::pnl::calculate_net_pnl(
                live_state.live_gross_pnl, trade.commission_cost, trade.slippage_cost, trade.funding_cost,
            );
            live_updates.push((
                trade.id, live_state.c_price_a, live_state.c_price_b, live_state.current_spread,
                live_net_pnl, 0.0, 0.0, 0, 0.0, live_state.live_pnl_a, live_state.live_pnl_b,
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

        // Exchange ban check & balance guard
        if !crate::engine::risk_manager::verify_trade_health(&exa, &exb, &e_ctx) {
            continue;
        }

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
        let ctx = crate::engine::execution::SequentialEntryContext {
            pair: pair.clone(),
            exa: exa.clone(),
            exb: exb.clone(),
            is_a_higher,
            qty_a,
            qty_b,
            limit_price_a,
            limit_price_b,
            mult_a,
            mult_b,
            tick_a,
            tick_b,
            ioc_slippage_pct: settings.slippage_percent,
            leverage: settings.leverage as f64,
            dry_run,
            sig_spread: sig.spread,
            sig_structural_spread: sig.structural_spread,
            sig_direction: sig.direction.clone(),
            sig_entry_a: entry_a,
            sig_entry_b: entry_b,
            profile_id: settings.id.clone(),
        };

        if crate::engine::execution::execute_sequential_entry(
            ctx,
            &http,
            &keys,
            &db,
            &e_ctx,
            ts,
            false, // sort_by_liquidity = false (HFT always shoots A first)
        ).await {
            opened_this_cycle = true;
        }
    }
}
