use std::sync::atomic::{AtomicI64, Ordering};
use shared::{SpreadRow, TradingSettings, TechnicalSettings, TradeSignal};
use crate::state::{EngineContext, PriceMap};
use crate::engine::real::{now_ms, now_secs, ExchangeKeys};

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
        if now - cd_a < ts.exchange_cooldown_ms || now - cd_b < ts.exchange_cooldown_ms { 
            continue; 
        }

        let cd_pair = pair_cd.get(&row.pair).map(|v| *v).unwrap_or(0);
        if now - cd_pair < ts.pair_cooldown_ms { 
            continue; 
        }

        // ── BALANCE LOW GUARD (zero-latency) ──
        if e_ctx.balance_low.get(&row.exchange_a).map(|v| *v).unwrap_or(false) ||
           e_ctx.balance_low.get(&row.exchange_b).map(|v| *v).unwrap_or(false) {
            continue;
        }

        let is_a_higher = row.direction.starts_with(&row.exchange_a);
        let entry_a = if is_a_higher { row.bid_a } else { row.ask_a };
        let entry_b = if is_a_higher { row.ask_b } else { row.bid_b };

        // ── LIQUIDITY DEPTH FILTER ──
        let data_a = prices.get(&row.exchange_a).and_then(|m| m.get(&row.pair).cloned());
        let data_b = prices.get(&row.exchange_b).and_then(|m| m.get(&row.pair).cloned());
        let _depth_sell: f64;
        let _depth_buy: f64;

        // Check if this spread is worth diagnosing (exceeds any active profile's min)
        let worth_diag = log_diag && profiles.iter().any(|p| p.is_active && row.spread >= p.min_entry_spread && row.spread <= p.max_entry_spread);

        if let (Some(ref da), Some(ref db)) = (&data_a, &data_b) {
            _depth_sell = if is_a_higher { da.bid_size } else { db.bid_size };
            _depth_buy  = if is_a_higher { db.ask_size } else { da.ask_size };
            
            let intra_a = (da.ask - da.bid) / da.bid.max(1e-12) * 100.0;
            let intra_b = (db.ask - db.bid) / db.bid.max(1e-12) * 100.0;
            if intra_a > ts.intra_spread_max_pct || intra_b > ts.intra_spread_max_pct { 
                if worth_diag { tracing::info!("[scanner] REJECT {} spread={:.4}% -> intra_spread too wide ({:.2}%/{:.2}% > max {:.2}%)", row.pair, row.spread, intra_a, intra_b, ts.intra_spread_max_pct); }
                continue; 
            }
        } else {
            if worth_diag { tracing::info!("[scanner] REJECT {} spread={:.4}% -> no price data for {}/{}", row.pair, row.spread, row.exchange_a, row.exchange_b); }
            continue;
        }

        // ── BASE MODIFIERS ──
        let map = &e_ctx.multipliers;
        let mult_a = match map.get(&format!("{}:{}", row.exchange_a, row.pair)) { Some(v) => *v, None => {
            if worth_diag { tracing::info!("[scanner] REJECT {} spread={:.4}% -> no multiplier for {}:{}", row.pair, row.spread, row.exchange_a, row.pair); }
            continue;
        }};
        let mult_b = match map.get(&format!("{}:{}", row.exchange_b, row.pair)) { Some(v) => *v, None => {
            if worth_diag { tracing::info!("[scanner] REJECT {} spread={:.4}% -> no multiplier for {}:{}", row.pair, row.spread, row.exchange_b, row.pair); }
            continue;
        }};
        
        let has_tick_a = e_ctx.tick_sizes.contains_key(&format!("{}:{}", row.exchange_a, row.pair));
        let has_tick_b = e_ctx.tick_sizes.contains_key(&format!("{}:{}", row.exchange_b, row.pair));
        if !has_tick_a || !has_tick_b { 
            if worth_diag { tracing::info!("[scanner] REJECT {} spread={:.4}% -> missing tick_size (a={} b={})", row.pair, row.spread, has_tick_a, has_tick_b); }
            continue; 
        }
        
        let tick_a = e_ctx.tick_sizes.get(&format!("{}:{}", row.exchange_a, row.pair)).map(|v| *v).unwrap_or(0.000001);
        let tick_b = e_ctx.tick_sizes.get(&format!("{}:{}", row.exchange_b, row.pair)).map(|v| *v).unwrap_or(0.000001);

        let lmap = &e_ctx.launch_times;
        let min_age_secs = ts.min_listing_age_days * 24 * 3600;
        let age_a = lmap.get(&format!("{}:{}", row.exchange_a, row.pair)).map(|r| *r).unwrap_or(0);
        let age_b = lmap.get(&format!("{}:{}", row.exchange_b, row.pair)).map(|r| *r).unwrap_or(0);
        if (age_a > 0 && now_s - age_a < min_age_secs) || (age_b > 0 && now_s - age_b < min_age_secs) { 
            if worth_diag { tracing::info!("[scanner] REJECT {} spread={:.4}% -> listing too young", row.pair, row.spread); }
            continue; 
        }

        let fee_a = crate::engine::real::get_exchange_fee(&row.exchange_a, ts.default_fee_rate, e_ctx);
        let fee_b = crate::engine::real::get_exchange_fee(&row.exchange_b, ts.default_fee_rate, e_ctx);

        // (Qty basis is now handled universally by sending phys_amount to exchange connectors)

        // ── PROFILE EVALUATION ──
        for profile in profiles {
            if !profile.is_active { continue; }
            if profile.disabled_exchanges.contains(&row.exchange_a) || profile.disabled_exchanges.contains(&row.exchange_b) { continue; }
            if row.spread < profile.min_entry_spread || row.spread > profile.max_entry_spread { continue; }

            let diag_prefix = if log_diag && row.spread >= profile.min_entry_spread { Some(format!("[scanner] REJECT {} {}={:.4}%", row.pair, profile.id, row.spread)) } else { None };

            let structural_deviation = row.spread - row.structural_spread;
            let mut dev_threshold = if profile.min_structural_deviation > 0.0 {
                profile.min_structural_deviation
            } else {
                0.0 // CHANGED: 0.0 means filter disabled. Do not inherit min_entry_spread
            };

            // If the pairs' legs changed places within the last 24h, the spread is highly volatile.
            // Disable structural spread filter entirely so we only mathematically enter by min_entry_spread
            if !row.has_structural_spread {
                dev_threshold = -1000.0;
            }

            // Only enforce deviation if explicitly set (>0.0)
            if profile.min_structural_deviation > 0.0 && structural_deviation < dev_threshold { 
                if let Some(p) = &diag_prefix { tracing::info!("{} -> ema deviation {:.4}% < {:.4}% req (base: {:.4}%)", p, structural_deviation, dev_threshold, row.structural_spread); }
                continue; 
            }

            let slippage_frac = profile.slippage_percent / 100.0;
            let break_even_pct = (2.0 * (fee_a + fee_b) + 4.0 * slippage_frac) * 100.0;

            // ── ABSOLUTE STRUCTURAL GUARD ──
            // Active ONLY when structural spread is reliable (legs did NOT swap in 24h+).
            // If has_structural_spread=false (premium crossed 0 within 24h) — this is a
            // normal volatile pair, guard is disabled, evaluation is purely by raw spread.
            if row.has_structural_spread {
                let profit_potential = row.spread - row.structural_spread - break_even_pct;
                if profit_potential <= 0.0 {
                    if let Some(p) = &diag_prefix {
                        tracing::info!("{} -> structural_guard: profit impossible (spread={:.4}% - structural={:.4}% - BE={:.4}% = {:.4}%)",
                            p, row.spread, row.structural_spread, break_even_pct, profit_potential);
                    }
                    continue;
                }
            }

            // ── L1 DEPTH FILTER (Zero-Latency Fake Liquidity Protection) ──
            // Uses exact websocket Bid/Ask volume received in the same tick as the price.
            if profile.min_depth_percent > 0.0 {
                let min_depth_usd = profile.trade_volume * (profile.min_depth_percent / 100.0);
                
                // If A is higher, we SELL A (hit bid) and BUY B (hit ask).
                let depth_a_usd = if is_a_higher { data_a.as_ref().unwrap().bid_size } else { data_a.as_ref().unwrap().ask_size };
                let depth_b_usd = if is_a_higher { data_b.as_ref().unwrap().ask_size } else { data_b.as_ref().unwrap().bid_size };
                
                if depth_a_usd < min_depth_usd || depth_b_usd < min_depth_usd {
                    if let Some(p) = &diag_prefix { 
                        tracing::info!("{} -> L1 depth shallow: A_USD={:.2}, B_USD={:.2} < REQUIRED={:.2}", 
                            p, depth_a_usd, depth_b_usd, min_depth_usd); 
                    }
                    continue; // Skip spread, orderbook is too thin to absorb full IOC cleanly
                }
            }

            // ENSURE PERFECT PHYSICAL BALANCE (0.0% COIN DUST)
            let big_step = mult_a.max(mult_b);
            let target_phys = profile.trade_volume / entry_a.max(1e-12);
            let mut base_steps = (target_phys / big_step).round();
            if base_steps < 1.0 { base_steps = 1.0; } 

            let mut phys_qty = 0.0;
            let mut found_match = false;
            // Iterate gracefully to find the nearest perfect multiple for BOTH exchanges
            for delta in 0..=5 {
                for dir in [1.0, -1.0] {
                    let test_steps = base_steps + (delta as f64) * dir;
                    if test_steps < 1.0 { continue; }
                    let test_phys = test_steps * big_step;
                    
                    let c_a = (test_phys / mult_a).round();
                    let c_b = (test_phys / mult_b).round();
                    
                    if (c_a * mult_a - test_phys).abs() < 1e-8 && (c_b * mult_b - test_phys).abs() < 1e-8 {
                        phys_qty = test_phys;
                        found_match = true;
                        break;
                    }
                }
                if found_match { break; }
            }

            if !found_match { 
                if let Some(p) = &diag_prefix { tracing::info!("{} -> physically impossible to balance lot sizes", p); }
                continue; 
            } // Physically impossible to balance lot sizes

            let max_v = profile.trade_volume * (1.0 + ts.vol_tolerance_pct / 100.0);
            
            // Loop down scaling big_step until volumetric match fits inside the max_v limit
            while phys_qty * entry_a > max_v && phys_qty > big_step {
                phys_qty -= big_step;
            }
            if phys_qty <= 0.0 { continue; } // Couldn't fit

            let contracts_a = (phys_qty / mult_a).round();
            let contracts_b = (phys_qty / mult_b).round();
            

            
            let actual_vol_a = phys_qty * entry_a;
            let actual_vol_b = phys_qty * entry_b;

            if actual_vol_a > max_v { 
                if let Some(p) = &diag_prefix { tracing::info!("{} -> actual_vol exceeds max_v (${:.2} > ${:.2}) (mult_trap)", p, actual_vol_a, max_v); }
                continue; 
            }

            if (row.exchange_a == "binance" && actual_vol_a < ts.binance_min_notional) || 
               (row.exchange_b == "binance" && phys_qty * entry_b < ts.binance_min_notional) { 
                if let Some(p) = &diag_prefix { tracing::info!("{} -> below binance_min_notional (${:.2} < ${:.2})", p, actual_vol_a, ts.binance_min_notional); }
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

            // ABSOLUTE SANITY CHECK: 
            // In cases of WebSocket flash crashes (where entry_a flashes to 0.1 instead of 1.0)
            // the engine could miscalculate a 10x target_phys. This block explicitly prevents it.
            // Enforcing a strict limit above the requested trade volume using `vol_tolerance_pct`.
            let max_vol = profile.trade_volume * (1.0 + ts.vol_tolerance_pct / 100.0);
            if actual_vol_a > max_vol || actual_vol_b > max_vol {
                tracing::error!("[scanner] ❌ VOLUME BLOCK: Vol A ${:.2} Vol B ${:.2} exceeds Profile Vol ${:.2} by >{}%. Skipping due to market data anomaly.", actual_vol_a, actual_vol_b, profile.trade_volume, ts.vol_tolerance_pct);
                continue;
            }

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
        }
    }
    signals
}
