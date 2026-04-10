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
        let depth_sell: f64;
        let depth_buy: f64;

        if let (Some(ref da), Some(ref db)) = (&data_a, &data_b) {
            depth_sell = if is_a_higher { da.bid_size } else { db.bid_size };
            depth_buy  = if is_a_higher { db.ask_size } else { da.ask_size };
            
            let intra_a = (da.ask - da.bid) / da.bid.max(1e-12) * 100.0;
            let intra_b = (db.ask - db.bid) / db.bid.max(1e-12) * 100.0;
            if intra_a > ts.intra_spread_max_pct || intra_b > ts.intra_spread_max_pct { 
                continue; 
            }
        } else {
            continue;
        }

        // ── BASE MODIFIERS ──
        let map = &e_ctx.multipliers;
        let mult_a = match map.get(&format!("{}:{}", row.exchange_a, row.pair)) { Some(v) => *v, None => continue };
        let mult_b = match map.get(&format!("{}:{}", row.exchange_b, row.pair)) { Some(v) => *v, None => continue };
        
        let has_tick_a = e_ctx.tick_sizes.contains_key(&format!("{}:{}", row.exchange_a, row.pair));
        let has_tick_b = e_ctx.tick_sizes.contains_key(&format!("{}:{}", row.exchange_b, row.pair));
        if !has_tick_a || !has_tick_b { continue; }
        
        let tick_a = e_ctx.tick_sizes.get(&format!("{}:{}", row.exchange_a, row.pair)).map(|v| *v).unwrap_or(0.000001);
        let tick_b = e_ctx.tick_sizes.get(&format!("{}:{}", row.exchange_b, row.pair)).map(|v| *v).unwrap_or(0.000001);

        let lmap = &e_ctx.launch_times;
        let min_age_secs = ts.min_listing_age_days * 24 * 3600;
        let age_a = lmap.get(&format!("{}:{}", row.exchange_a, row.pair)).map(|r| *r).unwrap_or(0);
        let age_b = lmap.get(&format!("{}:{}", row.exchange_b, row.pair)).map(|r| *r).unwrap_or(0);
        if (age_a > 0 && now_s - age_a < min_age_secs) || (age_b > 0 && now_s - age_b < min_age_secs) { continue; }

        let fee_a = crate::engine::real::get_exchange_fee(&row.exchange_a, ts.default_fee_rate, e_ctx);
        let fee_b = crate::engine::real::get_exchange_fee(&row.exchange_b, ts.default_fee_rate, e_ctx);

        let is_qty_based_a = row.exchange_a == "binance" || row.exchange_a == "bybit";
        let is_qty_based_b = row.exchange_b == "binance" || row.exchange_b == "bybit";

        // ── PROFILE EVALUATION ──
        for profile in profiles {
            if !profile.is_active { continue; }
            if profile.disabled_exchanges.contains(&row.exchange_a) || profile.disabled_exchanges.contains(&row.exchange_b) { continue; }
            if row.spread < profile.min_entry_spread || row.spread > profile.max_entry_spread { continue; }

            let structural_deviation = row.spread - row.structural_spread;
            if structural_deviation < profile.min_entry_spread { continue; }

            let slippage_frac = profile.slippage_percent / 100.0;
            let break_even_pct = (2.0 * (fee_a + fee_b) + 4.0 * slippage_frac) * 100.0;

            let min_depth_usd = profile.trade_volume * ts.depth_multiplier;
            if depth_sell > 0.0 && depth_sell < min_depth_usd { continue; }
            if depth_buy > 0.0 && depth_buy < min_depth_usd { continue; }

            // Ensure strict adherence to LOT_SIZE / qtyStep ticks or integer contracts
            let contracts_a = ((profile.trade_volume / entry_a.max(1e-12)) / mult_a).round();
            if contracts_a < 1.0 { continue; }
            
            let actual_vol_a = contracts_a * mult_a * entry_a;
            let contracts_b = ((actual_vol_a / entry_b.max(1e-12)) / mult_b).round();
            if contracts_b < 1.0 { continue; }
            
            let actual_vol_b = contracts_b * mult_b * entry_b;
            let vol_diff_pct = (actual_vol_a - actual_vol_b).abs() / actual_vol_a * 100.0;
            if vol_diff_pct > ts.vol_diff_max_pct { continue; }

            let max_v = profile.trade_volume * (1.0 + ts.vol_tolerance_pct / 100.0);
            if actual_vol_a > max_v { continue; }

            if (row.exchange_a == "binance" && actual_vol_a < ts.binance_min_notional) || 
               (row.exchange_b == "binance" && actual_vol_b < ts.binance_min_notional) { 
                continue; 
            }

            let use_ioc = ts.entry_order_type == "ioc";
            let (limit_price_a, limit_price_b) = if use_ioc {
                let lpa = if is_a_higher { entry_a * (1.0 - slippage_frac) } else { entry_a * (1.0 + slippage_frac) };
                let lpb = if is_a_higher { entry_b * (1.0 + slippage_frac) } else { entry_b * (1.0 - slippage_frac) };
                
                let lpa_rounded = (lpa / tick_a).round() * tick_a;
                let lpb_rounded = (lpb / tick_b).round() * tick_b;
                (lpa_rounded, lpb_rounded)
            } else {
                (0.0, 0.0)
            };

            let target_qty_a = if is_qty_based_a { contracts_a * mult_a } else { contracts_a };
            let target_qty_b = if is_qty_based_b { contracts_b * mult_b } else { contracts_b };

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
