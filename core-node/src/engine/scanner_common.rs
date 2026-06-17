//! Common scanner helpers — shared between EMA scanner and HFT scanner.
//!
//! Common scanner helpers used by the EMA scanner.

/// Compute the optimal physical quantity that is an exact integer multiple
/// of BOTH exchange multipliers (so contracts_a * mult_a == contracts_b * mult_b == phys_qty).
///
/// Returns `None` if no valid lot size can be found within 5 steps.
///
/// # Arguments
/// * `trade_volume` — target USD volume for the trade
/// * `entry_price` — entry price of side A (used for USD→physical conversion)
/// * `mult_a` — contract multiplier for exchange A
/// * `mult_b` — contract multiplier for exchange B
/// * `vol_tolerance_pct` — maximum percentage above trade_volume allowed
pub fn compute_lot_size(
    trade_volume: f64,
    entry_price: f64,
    mult_a: f64,
    mult_b: f64,
    vol_tolerance_pct: f64,
) -> Option<LotSize> {
    let big_step = mult_a.max(mult_b);
    let target_phys = trade_volume / entry_price.max(1e-12);
    let mut base_steps = (target_phys / big_step).round();
    if base_steps < 1.0 {
        base_steps = 1.0;
    }

    let mut phys_qty = 0.0;
    let mut found_match = false;

    // Iterate gracefully to find the nearest perfect multiple for BOTH exchanges
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
        return None;
    }

    let max_v = trade_volume * (1.0 + vol_tolerance_pct / 100.0);

    // Scale down until volumetric match fits inside the max_v limit
    while phys_qty * entry_price > max_v && phys_qty > big_step {
        phys_qty -= big_step;
    }
    if phys_qty <= 0.0 {
        return None;
    }

    let contracts_a = (phys_qty / mult_a).round();
    let contracts_b = (phys_qty / mult_b).round();

    Some(LotSize {
        phys_qty,
        contracts_a,
        contracts_b,
    })
}

/// Result of lot size computation.
pub struct LotSize {
    /// Physical quantity (base asset) — identical for both legs.
    pub phys_qty: f64,
    /// Number of contracts for exchange A.
    pub contracts_a: f64,
    /// Number of contracts for exchange B.
    pub contracts_b: f64,
}

/// Check listing age across all exchanges. Returns the oldest elapsed seconds.
/// Returns `i64::MAX` if age data is missing for the spread legs (safe fallback).
///
/// # Arguments
/// * `exchange_a`, `exchange_b` — the two spread legs
/// * `pair` — the trading pair
/// * `e_ctx` — engine context with launch_times
/// * `now_s` — current time in seconds
pub fn check_listing_age(
    exchange_a: &str,
    exchange_b: &str,
    pair: &str,
    e_ctx: &crate::state::EngineContext,
    now_s: i64,
) -> i64 {
    let lmap = &e_ctx.launch_times;
    let mut oldest_elapsed = -1i64;
    let mut missing_date_for_legs = false;

    // 1. Check the active legs
    for ex in [exchange_a, exchange_b] {
        let age = lmap
            .get(&format!("{}:{}", ex, pair))
            .map(|r| *r)
            .unwrap_or(0);
        if age > 0 {
            let el = now_s - age;
            if el > oldest_elapsed {
                oldest_elapsed = el;
            }
        } else {
            missing_date_for_legs = true;
        }
    }

    // 2. Check all other potential exchanges for the absolute oldest listing
    for ex in ["binance", "bybit", "gate", "kucoin", "bitget", "bingx", "bitmex", "ascendex", "xt", "toobit"] {
        if ex == exchange_a || ex == exchange_b {
            continue;
        }
        let age = lmap
            .get(&format!("{}:{}", ex, pair))
            .map(|r| *r)
            .unwrap_or(0);
        if age > 0 {
            let el = now_s - age;
            if el > oldest_elapsed {
                oldest_elapsed = el;
            }
        }
    }

    // If active spread legs failed to report a launch time, fallback to i64::MAX to prevent false rejections
    if missing_date_for_legs || oldest_elapsed == -1 {
        i64::MAX
    } else {
        oldest_elapsed
    }
}
