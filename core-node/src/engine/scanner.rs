use crate::engine::utils::{now_ms, now_secs, cooldown_active, get_exchange_fee};
use crate::engine::real::ExchangeKeys;
use crate::state::{EngineContext, PriceMap};
use shared::{SpreadRow, TechnicalSettings, TradeSignal, TradingSettings};
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;
use dashmap::DashMap;

/// Throttle map for SIGNAL log messages: key = "pair:exa:exb" → last_log_ms
static SIGNAL_LOG_THROTTLE: OnceLock<DashMap<String, i64>> = OnceLock::new();
fn signal_throttle() -> &'static DashMap<String, i64> {
    SIGNAL_LOG_THROTTLE.get_or_init(|| DashMap::new())
}

static LAST_DIAG_LOG: AtomicI64 = AtomicI64::new(0);

/// Core logic of token analysis: translates bare `SpreadRow` into ready-to-execute `TradeSignal`.
pub fn generate_signals(
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

    // Log diagnostics throttled
    let last_diag = LAST_DIAG_LOG.load(Ordering::Relaxed);
    let log_diag = (now - last_diag) > (ts.diag_log_interval as i64 * 100);
    if log_diag {
        LAST_DIAG_LOG.store(now, Ordering::Relaxed);
    }

    for row in spreads.iter().take(ts.max_spreads_per_cycle as usize) {
        if !keys.supports(&row.exchange_a) || !keys.supports(&row.exchange_b) {
            continue;
        }

        let cd_a = ex_cd.get(&row.exchange_a).map(|v| *v).unwrap_or(0);
        let cd_b = ex_cd.get(&row.exchange_b).map(|v| *v).unwrap_or(0);
        if cooldown_active(cd_a, now, ts.exchange_cooldown_ms)
            || cooldown_active(cd_b, now, ts.exchange_cooldown_ms)
        {
            continue;
        }

        let cd_pair = pair_cd.get(&row.pair).map(|v| *v).unwrap_or(0);
        if cooldown_active(cd_pair, now, ts.pair_cooldown_ms) {
            continue;
        }

        // Only 1 signal per pair per scan cycle (prevents duplicate exchange combos)
        if emitted_pairs.contains(&row.pair) {
            continue;
        }

        // ── HEALTH & RISK GUARD ──
        if !crate::engine::risk_manager::verify_trade_health(&row.exchange_a, &row.exchange_b, e_ctx) {
            continue;
        }

        // ── PRE-FORMAT CACHE KEYS (Optimization) ──
        let key_a = format!("{}:{}", row.exchange_a, row.pair);
        let key_b = format!("{}:{}", row.exchange_b, row.pair);

        let is_a_higher = row.direction.starts_with(&row.exchange_a);
        let entry_a = if is_a_higher { row.bid_a } else { row.ask_a };
        let entry_b = if is_a_higher { row.ask_b } else { row.bid_b };

        // ── LIQUIDITY DEPTH FILTER ──
        let data_a = prices
            .get(&row.exchange_a)
            .and_then(|m| m.get(&row.pair).cloned());
        let data_b = prices
            .get(&row.exchange_b)
            .and_then(|m| m.get(&row.pair).cloned());


        // Check if this spread is worth diagnosing (exceeds any active profile's min)
        let worth_diag = log_diag
            && profiles.iter().any(|p| {
                p.is_active && row.spread >= p.min_entry_spread && row.spread <= p.max_entry_spread
            });

        if let (Some(ref da), Some(ref db)) = (&data_a, &data_b) {

            let intra_a = (da.ask - da.bid) / da.bid.max(1e-12) * 100.0;
            let intra_b = (db.ask - db.bid) / db.bid.max(1e-12) * 100.0;
            if intra_a > ts.intra_spread_max_pct || intra_b > ts.intra_spread_max_pct {
                if worth_diag {
                    tracing::debug!("[scanner] REJECT {} spread={:.4}% -> intra_spread too wide ({:.2}%/{:.2}% > max {:.2}%)", row.pair, row.spread, intra_a, intra_b, ts.intra_spread_max_pct);
                }
                continue;
            }
        } else {
            if worth_diag {
                tracing::debug!(
                    "[scanner] REJECT {} spread={:.4}% -> no price data for {}/{}",
                    row.pair,
                    row.spread,
                    row.exchange_a,
                    row.exchange_b
                );
            }
            continue;
        }

        // ── BASE MODIFIERS ──
        let map = &e_ctx.multipliers;
        let mult_a = match map.get(&key_a) {
            Some(v) => *v,
            None => {
                if worth_diag {
                    tracing::debug!(
                        "[scanner] REJECT {} spread={:.4}% -> no multiplier for {}",
                        row.pair,
                        row.spread,
                        key_a
                    );
                }
                continue;
            }
        };
        let mult_b = match map.get(&key_b) {
            Some(v) => *v,
            None => {
                if worth_diag {
                    tracing::debug!(
                        "[scanner] REJECT {} spread={:.4}% -> no multiplier for {}",
                        row.pair,
                        row.spread,
                        key_b
                    );
                }
                continue;
            }
        };

        let has_tick_a = e_ctx.tick_sizes.contains_key(&key_a);
        let has_tick_b = e_ctx.tick_sizes.contains_key(&key_b);
        if !has_tick_a || !has_tick_b {
            if worth_diag {
                tracing::debug!(
                    "[scanner] REJECT {} spread={:.4}% -> missing tick_size (a={} b={})",
                    row.pair,
                    row.spread,
                    has_tick_a,
                    has_tick_b
                );
            }
            continue;
        }

        let tick_a = e_ctx.tick_sizes.get(&key_a).map(|v| *v).unwrap_or(0.000001);
        let tick_b = e_ctx.tick_sizes.get(&key_b).map(|v| *v).unwrap_or(0.000001);

        let min_age_secs = ts.min_listing_age_days * 24 * 3600;
        let final_oldest_elapsed = crate::engine::scanner_common::check_listing_age(
            &row.exchange_a, &row.exchange_b, &row.pair, e_ctx, now_s,
        );

        if final_oldest_elapsed < min_age_secs {
            if worth_diag {
                tracing::debug!(
                    "[scanner] REJECT {} ({} vs {}) spread={:.4}% -> listing too young",
                    row.pair,
                    row.exchange_a,
                    row.exchange_b,
                    row.spread
                );
            }
            continue;
        }

        let fee_a =
            get_exchange_fee(&row.exchange_a, ts.default_fee_rate, e_ctx);
        let fee_b =
            get_exchange_fee(&row.exchange_b, ts.default_fee_rate, e_ctx);

        // (Qty basis is now handled universally by sending phys_amount to exchange connectors)

        // ── PROFILE EVALUATION ──
        for profile in profiles {
            if !profile.is_active {
                continue;
            }
            if profile.disabled_exchanges.contains(&row.exchange_a)
                || profile.disabled_exchanges.contains(&row.exchange_b)
            {
                continue;
            }
            if row.spread < profile.min_entry_spread || row.spread > profile.max_entry_spread {
                continue;
            }
            // Intentional product rule: listing age is treated as token age from the
            // oldest known listing venue, but liquidity threshold is profile-specific.
            if row.vol_a < profile.min_volume_millions * 1_000_000.0
                || row.vol_b < profile.min_volume_millions * 1_000_000.0
            {
                continue;
            }

            let diag_prefix = if log_diag && row.spread >= profile.min_entry_spread {
                Some(format!(
                    "[scanner] REJECT {} ({} vs {}) {}={:.4}%",
                    row.pair, row.exchange_a, row.exchange_b, profile.id, row.spread
                ))
            } else {
                None
            };

            // ── STRUCTURAL VERIFICATION GUARD ──
            // Block entry until warmup has verified this pair from 24h historical data.
            // Without verification we don't know if there's a structural spread — entering
            // could be a guaranteed loss if the pair has a hidden structural premium.
            if !row.structural_verified {
                if let Some(p) = &diag_prefix {
                    tracing::debug!("{} -> structural NOT VERIFIED (failed historical warmup - insufficient candles or API error)", p);
                }
                continue;
            }

            // structural_spread is always ≥ 0 (clamped at source in spreads.rs)
            let structural_deviation = row.spread - row.structural_spread;
            let dev_threshold = if profile.min_structural_deviation > 0.0 {
                profile.min_structural_deviation
            } else {
                0.0 // 0.0 means filter disabled
            };

            // Enforce deviation filter if explicitly configured (> 0.0)
            if profile.min_structural_deviation > 0.0 && structural_deviation < dev_threshold {
                if let Some(p) = &diag_prefix {
                    tracing::info!(
                        "{} -> ema deviation {:.4}% < {:.4}% req (base: {:.4}%)",
                        p,
                        structural_deviation,
                        dev_threshold,
                        row.structural_spread
                    );
                }
                continue;
            }

            // Break-even = exchange fees on all 4 legs (entry + exit on both exchanges)
            // + expected slippage budget (~0.02% per leg × 4 legs = 0.08%).
            // fee_a/fee_b are REAL taker rates fetched from exchange APIs.
            // Including slippage in BE ensures only trades with real profit potential pass.
            let expected_slippage_per_leg = 0.02; // ~0.02% per leg
            let break_even_pct = 2.0 * (fee_a + fee_b) * 100.0 + 4.0 * expected_slippage_per_leg;

            // ── ABSOLUTE STRUCTURAL GUARD (ALWAYS ACTIVE) ──
            // Blocks entry when spread - structural_spread < break_even (fees).
            // structural_spread is the EMA baseline — what the spread will converge TO.
            // If structural_spread ≈ 0 (no premium), this simply checks spread > BE.
            // If structural_spread is high (persistent premium), this blocks entry
            // because the spread will never converge enough to cover fees.
            {
                let profit_potential = row.spread - row.structural_spread - break_even_pct;
                if profit_potential <= 0.0 {
                    if let Some(p) = &diag_prefix {
                        tracing::debug!("{} -> structural_guard: profit impossible (spread={:.4}% - structural={:.4}% - BE={:.4}% = {:.4}%)",
                            p, row.spread, row.structural_spread, break_even_pct, profit_potential);
                    }
                    continue;
                }
            }

            // ── L1 DEPTH FILTER (Zero-Latency Fake Liquidity Protection) ──
            // Uses exact websocket Bid/Ask volume received in the same tick as the price.
            if profile.min_depth_percent > 0.0 {
                let min_depth_usd = profile.trade_volume * (profile.min_depth_percent / 100.0);

                // bid_size from collectors is ALREADY in TRUE USD (for all exchanges, natively calculated in the collectors).
                // If A is higher, we SELL A (hit bid) and BUY B (hit ask).
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
                    if let Some(p) = &diag_prefix {
                        tracing::info!(
                            "{} -> L1 depth shallow: A_USD={:.2}, B_USD={:.2} < REQUIRED={:.2}",
                            p,
                            depth_a_usd,
                            depth_b_usd,
                            min_depth_usd
                        );
                    }
                    continue; // Skip spread, orderbook is too thin to absorb full IOC cleanly
                }
            }

            // ENSURE PERFECT PHYSICAL BALANCE (0.0% COIN DUST)
            let lot = match crate::engine::scanner_common::compute_lot_size(
                profile.trade_volume, entry_a, mult_a, mult_b, ts.vol_tolerance_pct,
            ) {
                Some(l) => l,
                None => {
                    if let Some(p) = &diag_prefix {
                        tracing::info!("{} -> physically impossible to balance lot sizes", p);
                    }
                    continue;
                }
            };

            let phys_qty = lot.phys_qty;
            let contracts_a = lot.contracts_a;
            let contracts_b = lot.contracts_b;

            let actual_vol_a = phys_qty * entry_a;
            let actual_vol_b = phys_qty * entry_b;
            let max_v = profile.trade_volume * (1.0 + ts.vol_tolerance_pct / 100.0);

            // mult_trap: if actual volume exceeds tolerance, check if it's at minimum lot.
            // When min contract lot (big_step * price) > max_v, there's no way to trade
            // smaller — allow it with a warning instead of blocking all trades.
            if actual_vol_a > max_v {
                let big_step = mult_a.max(mult_b);
                let min_lot_vol = big_step * entry_a;
                if actual_vol_a > min_lot_vol * 1.01 {
                    // Genuinely oversized (not at minimum) — reject
                    if let Some(p) = &diag_prefix {
                        tracing::info!(
                            "{} -> actual_vol exceeds max_v (${:.2} > ${:.2}) (mult_trap)",
                            p, actual_vol_a, max_v
                        );
                    }
                    continue;
                }
                // At minimum lot — allow with warning
                if let Some(p) = &diag_prefix {
                    tracing::debug!(
                        "{} -> min lot ${:.2} > max_v ${:.2} but accepting (min_lot_override)",
                        p, actual_vol_a, max_v
                    );
                }
            }

            // Universal minimum notional check — all major CEXes reject orders below ~$5
            let min_notional = ts.binance_min_notional; // reuse setting as universal floor
            if actual_vol_a < min_notional || actual_vol_b < min_notional {
                if let Some(p) = &diag_prefix {
                    tracing::info!(
                        "{} -> below min_notional (A=${:.2} B=${:.2} < ${:.2})",
                        p,
                        actual_vol_a,
                        actual_vol_b,
                        min_notional
                    );
                }
                continue;
            }

            let use_ioc = ts.entry_order_type == "ioc";
            let (limit_price_a, limit_price_b) = if use_ioc {
                // Pass raw entry prices to the engine. Slippage and rounding are handled
                // by the exchange-specific connectors (place_order) using settings.slippage_percent.
                (entry_a, entry_b)
            } else {
                (0.0, 0.0)
            };

            // All exchanges now uniformly expect `phys_amount` for `place_order`.
            let target_qty_a = phys_qty;
            let target_qty_b = phys_qty;



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
            {
                let sig_key = format!("{}:{}:{}", row.pair, row.exchange_a, row.exchange_b);
                let throttle = signal_throttle();
                let last = throttle.get(&sig_key).map(|v| *v).unwrap_or(0);
                if now - last > 5_000 {
                    throttle.insert(sig_key, now);
                    tracing::info!(
                        "[scanner] SIGNAL {}: {} vs {} spread={:.4}% BE={:.4}% (fee_a={:.6} fee_b={:.6})",
                        row.pair, row.exchange_a, row.exchange_b, row.spread, break_even_pct,
                        fee_a, fee_b
                    );
                }
            }
            emitted_pairs.insert(row.pair.clone());
        }
    }
    signals
}
