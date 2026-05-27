//! HFT / Latency Arbitrage Scanner
//!
//! Completely isolated from the EMA reversion scanner (scanner.rs).
//! Scans for raw instantaneous spread > break-even — no structural EMA needed.
//! Produces the same `TradeSignal` struct for execution by `hft_cycle.rs`.
//!
//! ## Two HFT-specific filters (not present in EMA scanner):
//!
//! ### 1. Spike-above-norm filter
//! The raw spread must be a FRESH DEVIATION above the EMA structural baseline,
//! not just the chronic normal gap. A pair with structural_spread=1.0% trading at
//! 1.0% is NOT a latency spike — it's just its normal state, won't revert.
//! We require: raw_spread > structural_spread + hft_min_spike_above_norm.
//!
//! ### 2. Velocity asymmetry filter
//! Latency arbitrage = ONE exchange moved, the OTHER is still.
//! If both exchanges moved together (even if one moved faster), it's a trend.
//! We measure price change per exchange over the last ~3 seconds using price_history,
//! keyed by "exchange:PAIR". If asymmetry < hft_min_velocity_asymmetry_pct, skip.

use crate::engine::real::{now_ms, now_secs, ExchangeKeys};
use crate::state::{EngineContext, PriceMap};
use shared::{SpreadRow, TechnicalSettings, TradeSignal, TradingSettings};
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};

static LAST_HFT_DIAG_LOG: AtomicI64 = AtomicI64::new(0);

/// Updates per-exchange price snapshot in price_history (key = "exchange:PAIR").
/// Called every scanner cycle so velocity is always fresh.
fn update_exchange_price_snapshot(e_ctx: &EngineContext, exchange: &str, pair: &str, current_price: f64, now: i64) {
    let key = format!("{}:{}", exchange, pair);
    // Only update if old snapshot is >500ms old to avoid overwriting mid-cycle
    let should_update = e_ctx.price_history
        .get(&key)
        .map(|r| now - r.1 > 500)
        .unwrap_or(true);
    if should_update {
        e_ctx.price_history.insert(key, (current_price, now));
    }
}

fn cooldown_active(marker: i64, now: i64, cooldown_ms: i64) -> bool {
    if marker <= 0 {
        return false;
    }
    if marker > now {
        return true;
    }
    now - marker < cooldown_ms
}

/// HFT signal generator — scans for raw instantaneous spread opportunities.
///
/// Key differences from EMA scanner:
/// - No structural guard (profit_potential check)
/// - Pure raw spread: `spread > break_even`
/// - Same lot sizing, multiplier, tick_size, cooldown logic
///
/// HFT-specific additions (vs EMA scanner):
/// - Spike-above-norm: spread must exceed structural_spread EMA by a margin
/// - Velocity asymmetry: only ONE exchange must have moved (latency, not trend)
pub fn generate_hft_signals(
    spreads: &[SpreadRow],
    prices: &PriceMap,
    profiles: &[TradingSettings],
    keys: &ExchangeKeys,
    e_ctx: &EngineContext,
    ts: &TechnicalSettings,
) -> Vec<TradeSignal> {
    let mut signals = Vec::new();
    let mut emitted_pairs: HashSet<String> = HashSet::new();
    let now = now_ms();
    let now_s = now_secs();

    let ex_cd = &e_ctx.exchange_cooldown;
    let pair_cd = &e_ctx.pair_cooldown;

    // Diagnostics throttled
    let last_diag = LAST_HFT_DIAG_LOG.load(Ordering::Relaxed);
    let log_diag = (now - last_diag) > (ts.diag_log_interval as i64 * 100);
    if log_diag {
        LAST_HFT_DIAG_LOG.store(now, Ordering::Relaxed);
    }

    // HFT profiles only
    let hft_profiles: Vec<&TradingSettings> = profiles
        .iter()
        .filter(|p| p.strategy_type == "hft" && p.is_active)
        .collect();

    if hft_profiles.is_empty() {
        return signals;
    }

    for row in spreads.iter().take(ts.max_spreads_per_cycle as usize) {
        if !keys.supports(&row.exchange_a) || !keys.supports(&row.exchange_b) {
            continue;
        }

        // Exchange cooldowns
        let cd_a = ex_cd.get(&row.exchange_a).map(|v| *v).unwrap_or(0);
        let cd_b = ex_cd.get(&row.exchange_b).map(|v| *v).unwrap_or(0);
        if cooldown_active(cd_a, now, ts.exchange_cooldown_ms)
            || cooldown_active(cd_b, now, ts.exchange_cooldown_ms)
        {
            continue;
        }

        // Pair cooldown
        let cd_pair = pair_cd.get(&row.pair).map(|v| *v).unwrap_or(0);
        if cooldown_active(cd_pair, now, ts.pair_cooldown_ms) {
            continue;
        }

        // Only 1 signal per pair per cycle
        if emitted_pairs.contains(&row.pair) {
            continue;
        }

        // Balance guard
        if e_ctx
            .balance_low
            .get(&row.exchange_a)
            .map(|v| *v)
            .unwrap_or(false)
            || e_ctx
                .balance_low
                .get(&row.exchange_b)
                .map(|v| *v)
                .unwrap_or(false)
        {
            continue;
        }

        let is_a_higher = row.direction.starts_with(&row.exchange_a);
        let entry_a = if is_a_higher { row.bid_a } else { row.ask_a };
        let entry_b = if is_a_higher { row.ask_b } else { row.bid_b };

        // Price data required
        let data_a = prices
            .get(&row.exchange_a)
            .and_then(|m| m.get(&row.pair).cloned());
        let data_b = prices
            .get(&row.exchange_b)
            .and_then(|m| m.get(&row.pair).cloned());

        if let (Some(ref da), Some(ref db)) = (&data_a, &data_b) {
            let intra_a = (da.ask - da.bid) / da.bid.max(1e-12) * 100.0;
            let intra_b = (db.ask - db.bid) / db.bid.max(1e-12) * 100.0;
            if intra_a > ts.intra_spread_max_pct || intra_b > ts.intra_spread_max_pct {
                continue;
            }
        } else {
            continue;
        }

        // Multipliers
        let map = &e_ctx.multipliers;
        let mult_a = match map.get(&format!("{}:{}", row.exchange_a, row.pair)) {
            Some(v) => *v,
            None => continue,
        };
        let mult_b = match map.get(&format!("{}:{}", row.exchange_b, row.pair)) {
            Some(v) => *v,
            None => continue,
        };

        // Tick sizes
        let has_tick_a = e_ctx
            .tick_sizes
            .contains_key(&format!("{}:{}", row.exchange_a, row.pair));
        let has_tick_b = e_ctx
            .tick_sizes
            .contains_key(&format!("{}:{}", row.exchange_b, row.pair));
        if !has_tick_a || !has_tick_b {
            continue;
        }
        let tick_a = e_ctx
            .tick_sizes
            .get(&format!("{}:{}", row.exchange_a, row.pair))
            .map(|v| *v)
            .unwrap_or(0.000001);
        let tick_b = e_ctx
            .tick_sizes
            .get(&format!("{}:{}", row.exchange_b, row.pair))
            .map(|v| *v)
            .unwrap_or(0.000001);

        // Listing age check
        let lmap = &e_ctx.launch_times;
        let min_age_secs = ts.min_listing_age_days * 24 * 3600;
        let mut oldest_elapsed = -1i64;
        for ex in [&row.exchange_a, &row.exchange_b] {
            let age = lmap
                .get(&format!("{}:{}", ex, row.pair))
                .map(|r| *r)
                .unwrap_or(0);
            if age > 0 {
                let el = now_s - age;
                if el > oldest_elapsed {
                    oldest_elapsed = el;
                }
            }
        }
        // Also check other exchanges for older listing
        for ex in ["binance", "bybit", "gate", "kucoin", "bitget", "bingx", "bitmex", "ascendex", "xt", "toobit"] {
            if ex == row.exchange_a || ex == row.exchange_b {
                continue;
            }
            let age = lmap
                .get(&format!("{}:{}", ex, row.pair))
                .map(|r| *r)
                .unwrap_or(0);
            if age > 0 {
                let el = now_s - age;
                if el > oldest_elapsed {
                    oldest_elapsed = el;
                }
            }
        }
        let final_oldest = if oldest_elapsed == -1 {
            i64::MAX
        } else {
            oldest_elapsed
        };
        if final_oldest < min_age_secs {
            continue;
        }

        let fee_a =
            crate::engine::real::get_exchange_fee(&row.exchange_a, ts.default_fee_rate, e_ctx);
        let fee_b =
            crate::engine::real::get_exchange_fee(&row.exchange_b, ts.default_fee_rate, e_ctx);

        // ── FILTER 1: SPIKE-ABOVE-NORM ───────────────────────────────────────
        // The spread must be a fresh deviation ABOVE its EMA structural baseline.
        // A pair trading at its chronic norm (raw ≈ structural) will NOT revert
        // on HFT timescales — it's been there for hours, not seconds.
        // We require: raw_spread > structural_spread + min_spike_margin.
        //
        // min_spike_margin default: 0.3% (configurable via hft_min_spike_above_norm in TechnicalSettings)
        // If structural_spread is not yet computed (has_structural_spread=false), skip this filter.
        let hft_min_spike_above_norm = 0.3_f64; // % above EMA norm to qualify as latency spike
        if row.has_structural_spread && row.structural_spread > 0.0 {
            let spike = row.spread - row.structural_spread;
            if spike < hft_min_spike_above_norm {
                if log_diag {
                    tracing::debug!(
                        "[hft_scanner] SKIP {} spike={:.4}% < {:.4}% (spread={:.4}% structural={:.4}%) — chronic norm, not latency",
                        row.pair, spike, hft_min_spike_above_norm, row.spread, row.structural_spread
                    );
                }
                continue;
            }
        }

        // ── FILTER 2: VELOCITY ASYMMETRY ────────────────────────────────────
        // In true latency arb, ONE exchange's price moved (e.g. large order hit Bybit)
        // while the OTHER exchange is still (Gate hasn't reacted yet).
        // If BOTH exchanges moved together → it's a trend, not latency → skip.
        //
        // Velocity window: 3 seconds. Asymmetry threshold: 0.2% difference.
        // If we have no per-exchange history yet (first cycles) → allow entry (benefit of doubt).
        let vel_window_ms = 3_000_i64;
        let hft_min_asymmetry_pct = 0.2_f64; // minimum difference between exchange velocities

        let price_a_now = if is_a_higher { row.bid_a } else { row.ask_a };
        let price_b_now = if is_a_higher { row.ask_b } else { row.bid_b };

        // Update per-exchange snapshots so next cycle can measure velocity
        update_exchange_price_snapshot(e_ctx, &row.exchange_a, &row.pair, price_a_now, now);
        update_exchange_price_snapshot(e_ctx, &row.exchange_b, &row.pair, price_b_now, now);

        // Compute velocity for each exchange independently
        let key_a = format!("{}:{}", row.exchange_a, row.pair);
        let key_b = format!("{}:{}", row.exchange_b, row.pair);

        let vel_a_pct = e_ctx.price_history.get(&key_a).and_then(|r| {
            let (old_p, old_ts) = *r;
            let elapsed = now - old_ts;
            if elapsed >= 500 && elapsed <= vel_window_ms * 3 && old_p > 1e-12 {
                Some(((price_a_now - old_p) / old_p).abs() * 100.0)
            } else { None }
        });
        let vel_b_pct = e_ctx.price_history.get(&key_b).and_then(|r| {
            let (old_p, old_ts) = *r;
            let elapsed = now - old_ts;
            if elapsed >= 500 && elapsed <= vel_window_ms * 3 && old_p > 1e-12 {
                Some(((price_b_now - old_p) / old_p).abs() * 100.0)
            } else { None }
        });

        if let (Some(va), Some(vb)) = (vel_a_pct, vel_b_pct) {
            let asymmetry = (va - vb).abs();
            if asymmetry < hft_min_asymmetry_pct {
                // Both exchanges moved similarly — this is a synchronized trend, not latency
                if log_diag {
                    tracing::debug!(
                        "[hft_scanner] SKIP {} vel_a={:.4}% vel_b={:.4}% asymmetry={:.4}% < {:.4}% — synchronized trend, not latency spike",
                        row.pair, va, vb, asymmetry, hft_min_asymmetry_pct
                    );
                }
                continue;
            }
            // Log when we DO pass — useful for tuning thresholds
            if log_diag {
                tracing::debug!(
                    "[hft_scanner] PASS {} vel_a={:.4}% vel_b={:.4}% asymmetry={:.4}% spike={:.4}%",
                    row.pair, va, vb, asymmetry,
                    row.spread - row.structural_spread
                );
            }
        }
        // If no history yet for either exchange → allow entry (benefit of the doubt on first cycles)

        // ── HFT PROFILE EVALUATION ──
        for profile in &hft_profiles {
            if profile.disabled_exchanges.contains(&row.exchange_a)
                || profile.disabled_exchanges.contains(&row.exchange_b)
            {
                continue;
            }

            // HFT uses min_entry_spread as minimum raw spread threshold
            if row.spread < profile.min_entry_spread || row.spread > profile.max_entry_spread {
                continue;
            }

            // Volume filter
            if row.vol_a < profile.min_volume_millions * 1_000_000.0
                || row.vol_b < profile.min_volume_millions * 1_000_000.0
            {
                continue;
            }

            let slippage_frac = profile.slippage_percent / 100.0;
            let break_even_pct = (2.0 * (fee_a + fee_b) + 2.0 * slippage_frac) * 100.0;

            // HFT CORE: raw spread must exceed break-even (NO structural guard)
            if row.spread <= break_even_pct {
                if log_diag {
                    tracing::debug!(
                        "[hft_scanner] SKIP {} spread={:.4}% <= BE={:.4}%",
                        row.pair,
                        row.spread,
                        break_even_pct
                    );
                }
                continue;
            }

            // L1 depth filter
            if profile.min_depth_percent > 0.0 {
                let min_depth_usd = profile.trade_volume * (profile.min_depth_percent / 100.0);
                // bid_size/ask_size from collectors are ALREADY in TRUE USD
                // (same as EMA scanner). Do NOT multiply by price — that would square the value.
                let depth_a_usd = if is_a_higher {
                    data_a.as_ref().unwrap().bid_size
                } else {
                    data_a.as_ref().unwrap().ask_size
                };
                let depth_b_usd = if is_a_higher {
                    data_b.as_ref().unwrap().ask_size
                } else {
                    data_b.as_ref().unwrap().bid_size
                };
                if depth_a_usd < min_depth_usd || depth_b_usd < min_depth_usd {
                    continue;
                }
            }

            // Lot sizing (same algorithm as EMA scanner)
            let big_step = mult_a.max(mult_b);
            let target_phys = profile.trade_volume / entry_a.max(1e-12);
            let mut base_steps = (target_phys / big_step).round();
            if base_steps < 1.0 {
                base_steps = 1.0;
            }

            let mut phys_qty = 0.0;
            let mut found_match = false;
            for delta in 0..=5 {
                for dir in [1.0, -1.0] {
                    let test_steps = base_steps + (delta as f64) * dir;
                    if test_steps < 1.0 {
                        continue;
                    }
                    let test_phys = test_steps * big_step;
                    let c_a = (test_phys / mult_a).round();
                    let c_b = (test_phys / mult_b).round();
                    if (c_a * mult_a - test_phys).abs() < 1e-8
                        && (c_b * mult_b - test_phys).abs() < 1e-8
                    {
                        phys_qty = test_phys;
                        found_match = true;
                        break;
                    }
                }
                if found_match {
                    break;
                }
            }
            if !found_match {
                continue;
            }

            let max_v = profile.trade_volume * (1.0 + ts.vol_tolerance_pct / 100.0);
            while phys_qty * entry_a > max_v && phys_qty > big_step {
                phys_qty -= big_step;
            }
            if phys_qty <= 0.0 {
                continue;
            }

            let contracts_a = (phys_qty / mult_a).round();
            let contracts_b = (phys_qty / mult_b).round();
            let actual_vol_a = phys_qty * entry_a;
            let actual_vol_b = phys_qty * entry_b;

            if actual_vol_a > max_v {
                continue;
            }

            if (row.exchange_a == "binance" && actual_vol_a < ts.binance_min_notional)
                || (row.exchange_b == "binance" && phys_qty * entry_b < ts.binance_min_notional)
            {
                continue;
            }

            // HFT always uses IOC for tightest execution
            let (limit_price_a, limit_price_b) = (entry_a, entry_b);

            let target_qty_a = phys_qty;
            let target_qty_b = phys_qty;

            // Volume sanity check
            let max_vol = profile.trade_volume * (1.0 + ts.vol_tolerance_pct / 100.0);
            if actual_vol_a > max_vol || actual_vol_b > max_vol {
                continue;
            }

            tracing::info!(
                "[hft_scanner] ⚡ SIGNAL {} spread={:.4}% > BE={:.4}% on {}/{}",
                row.pair,
                row.spread,
                break_even_pct,
                row.exchange_a,
                row.exchange_b
            );

            signals.push(TradeSignal {
                profile_id: profile.id.clone(),
                pair: row.pair.clone(),
                exchange_a: row.exchange_a.clone(),
                exchange_b: row.exchange_b.clone(),
                direction: row.direction.clone(),

                contracts_a,
                contracts_b,
                target_qty_a,
                target_qty_b,
                limit_price_a,
                limit_price_b,

                entry_a,
                entry_b,
                actual_vol_a_usd: actual_vol_a,
                actual_vol_b_usd: actual_vol_b,

                break_even_pct,
                spread: row.spread,
                structural_spread: row.structural_spread,
                is_a_higher,

                tick_a,
                tick_b,
                mult_a,
                mult_b,
            });
            emitted_pairs.insert(row.pair.clone());
        }
    }
    signals
}
