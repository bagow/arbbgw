//! Common scanner helpers — shared between EMA scanner and HFT scanner.
//!
//! Common scanner helpers used by the EMA scanner.

/// Conservative expected execution cost used for profitability filtering.
///
/// `TradingSettings::slippage_percent` is an IOC price-limit ceiling, not an
/// estimate that every fill is expected to consume. The two controls must stay
/// independent: using that ceiling as a
/// guaranteed cost can make the configured entry window mathematically
/// unreachable (for example a 2% ceiling with a 2.5% maximum spread).
pub const EXPECTED_IOC_SLIPPAGE_PER_LEG_PCT: f64 = 0.02;

/// Fees for entry and exit on both venues, plus the expected cost of four IOC
/// legs. Fee rates are fractional (0.0005 = 0.05%); the result is a percent.
pub fn ioc_break_even_pct(fee_a: f64, fee_b: f64) -> f64 {
    2.0 * (fee_a + fee_b) * 100.0 + 4.0 * EXPECTED_IOC_SLIPPAGE_PER_LEG_PCT
}

/// Net PnL percent if both newly opened legs were immediately liquidated at
/// the current executable quotes. This uses the same leg-A entry-value
/// denominator as managed-trade mark-to-market accounting.
pub fn initial_liquidation_net_pct(
    is_a_higher: bool,
    entry_a: f64,
    entry_b: f64,
    exit_a: f64,
    exit_b: f64,
    quantity: f64,
    fee_a: f64,
    fee_b: f64,
) -> Option<f64> {
    if ![
        entry_a, entry_b, exit_a, exit_b, quantity, fee_a, fee_b,
    ]
    .iter()
    .all(|value| value.is_finite())
        || [entry_a, entry_b, exit_a, exit_b, quantity]
            .iter()
            .any(|value| *value <= 0.0)
        || fee_a < 0.0
        || fee_b < 0.0
    {
        return None;
    }
    let gross = if is_a_higher {
        entry_a - exit_a + exit_b - entry_b
    } else {
        exit_a - entry_a + entry_b - exit_b
    } * quantity;
    let fees = quantity
        * (entry_a * fee_a + entry_b * fee_b + exit_a * fee_a + exit_b * fee_b);
    let percent = (gross - fees) / (entry_a * quantity) * 100.0;
    percent.is_finite().then_some(percent)
}

/// A trade must not start on, or within the configured maximum movement of its
/// two opening IOC legs from, its own hard-stop boundary. A disabled hard stop
/// remains compatible by definition.
pub fn hard_stop_entry_is_compatible(
    initial_net_pct: f64,
    hard_stop_percent: f64,
    entry_slippage_limit_per_leg_pct: f64,
) -> bool {
    initial_net_pct.is_finite()
        && hard_stop_percent.is_finite()
        && entry_slippage_limit_per_leg_pct.is_finite()
        && entry_slippage_limit_per_leg_pct >= 0.0
        && (hard_stop_percent <= 0.0
            || initial_net_pct
                > -hard_stop_percent + 2.0 * entry_slippage_limit_per_leg_pct)
}

/// Return the verified structural reserve used by every EMA entry.
///
/// `min_structural_deviation` controls the additional deviation required by
/// the profile. Zero disables only that extra margin; it must not turn a
/// persistent venue premium into presumed profit or bypass historical warmup.
pub fn configured_structural_reserve(
    min_structural_deviation: f64,
    structural_verified: bool,
    observed_structural_spread: f64,
) -> Option<f64> {
    if !min_structural_deviation.is_finite()
        || !observed_structural_spread.is_finite()
        || min_structural_deviation < 0.0
    {
        return None;
    }
    structural_verified.then_some(observed_structural_spread.max(0.0))
}

/// Require the selected venue premium to have remained on the same side of
/// zero for the complete structural observation window.
///
/// A verified EMA alone is not enough: after a basis direction flip its mean
/// can be close to zero while the live spread is several percent. Treating that
/// regime change as a mean-reverting excursion creates a fictitious entry
/// budget. `has_structural_spread` becomes true only after roughly 24 hours in
/// one direction.
pub fn directional_structural_regime_is_stable(
    structural_verified: bool,
    has_structural_spread: bool,
    structural_min: f64,
    structural_max: f64,
) -> bool {
    structural_verified
        && has_structural_spread
        && structural_min.is_finite()
        && structural_max.is_finite()
        && structural_max >= structural_min
}

/// Remaining two-leg price budget after preserving the configured entry rules.
///
/// `min_structural_deviation` is the required net excursion beyond both the
/// structural baseline and round-trip execution costs. Treating it as a
/// substitute for those costs (`max(deviation, break_even)`) overstates the
/// available edge and can admit a trade that cannot retain the configured
/// deviation after fees.
pub fn entry_execution_budget(
    gross_spread: f64,
    min_entry_spread: f64,
    max_entry_spread: f64,
    structural_reserve: f64,
    min_structural_deviation: f64,
    break_even_pct: f64,
) -> Option<f64> {
    if ![
        gross_spread,
        min_entry_spread,
        max_entry_spread,
        structural_reserve,
        min_structural_deviation,
        break_even_pct,
    ]
    .iter()
    .all(|value| value.is_finite())
        || min_entry_spread < 0.0
        || max_entry_spread < min_entry_spread
        || structural_reserve < 0.0
        || min_structural_deviation < 0.0
        || break_even_pct < 0.0
        || gross_spread < min_entry_spread
        || gross_spread > max_entry_spread
    {
        return None;
    }
    let structural_floor = structural_reserve + break_even_pct + min_structural_deviation;
    let required = min_entry_spread.max(structural_floor);
    let budget = gross_spread - required;
    (budget > 0.0).then_some(budget)
}

/// Budget used only to decide whether an executable L2 stream is worth warming.
///
/// Keep the verified structural baseline and round-trip costs fully reserved;
/// discount only user-configured entry/deviation margins. This creates useful
/// headroom before a real signal without subscribing to a persistent venue
/// premium which cannot become profitable at the configured entry rules.
pub fn prewarm_execution_budget(
    gross_spread: f64,
    min_entry_spread: f64,
    max_entry_spread: f64,
    structural_reserve: f64,
    min_structural_deviation: f64,
    break_even_pct: f64,
    prewarm_ratio: f64,
) -> Option<f64> {
    if !prewarm_ratio.is_finite() || !(0.0..=1.0).contains(&prewarm_ratio) {
        return None;
    }
    entry_execution_budget(
        gross_spread,
        min_entry_spread * prewarm_ratio,
        max_entry_spread,
        structural_reserve,
        min_structural_deviation * prewarm_ratio,
        break_even_pct,
    )
}

/// Return the exact user-configured cumulative L2 depth requirement. Zero disables it.
pub fn required_depth_usd(trade_volume: f64, min_depth_percent: f64) -> Option<f64> {
    if trade_volume.is_finite()
        && min_depth_percent.is_finite()
        && trade_volume > 0.0
        && min_depth_percent > 0.0
    {
        Some(trade_volume * min_depth_percent / 100.0)
    } else {
        None
    }
}

/// Check both legs against the configured lower and upper order-volume bounds.
pub fn volumes_within_tolerance(
    target: f64,
    tolerance_pct: f64,
    actual_a: f64,
    actual_b: f64,
) -> bool {
    if ![target, tolerance_pct, actual_a, actual_b]
        .iter()
        .all(|value| value.is_finite())
        || target <= 0.0
        || tolerance_pct < 0.0
        || actual_a <= 0.0
        || actual_b <= 0.0
    {
        return false;
    }
    let lower = target * (1.0 - tolerance_pct / 100.0).max(0.0);
    let upper = target * (1.0 + tolerance_pct / 100.0);
    let epsilon = target * 1e-9;
    [actual_a, actual_b]
        .iter()
        .all(|actual| *actual >= lower - epsilon && *actual <= upper + epsilon)
}

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
    if ![trade_volume, entry_price, mult_a, mult_b, vol_tolerance_pct]
        .iter()
        .all(|x| x.is_finite())
        || trade_volume <= 0.0
        || entry_price <= 0.0
        || vol_tolerance_pct < 0.0
    {
        return None;
    }
    let step = common_quantity_step(mult_a, mult_b)?;
    let max_qty = trade_volume * (1.0 + vol_tolerance_pct / 100.0) / entry_price;
    let preferred = (trade_volume / entry_price / step).round().max(1.0);
    let steps = preferred.min((max_qty / step + 1e-10).floor());
    if steps < 1.0 {
        return None;
    }
    let phys_qty = steps * step;
    Some(LotSize {
        phys_qty,
        contracts_a: phys_qty / mult_a,
        contracts_b: phys_qty / mult_b,
    })
}

/// Exact common grid for decimal exchange quantity steps (up to 8 decimals).
/// Quantity step is not the size of a derivative contract.
pub fn common_quantity_step(a: f64, b: f64) -> Option<f64> {
    const SCALE: f64 = 100_000_000.0;
    fn units(x: f64) -> Option<u64> {
        let n = x * SCALE;
        if !x.is_finite()
            || x <= 0.0
            || n < 1.0
            || n > 9_000_000_000_000_000.0
            || (n - n.round()).abs() > 1e-5
        {
            return None;
        }
        Some(n.round() as u64)
    }
    let (a, b) = (units(a)?, units(b)?);
    let (mut x, mut y) = (a, b);
    while y != 0 {
        let r = x % y;
        x = y;
        y = r;
    }
    let lcm = (a / x).checked_mul(b)?;
    if lcm > 9_000_000_000_000_000 {
        return None;
    }
    Some(lcm as f64 / SCALE)
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
    for ex in [
        "binance", "bybit", "gate", "kucoin", "bitget", "bingx", "bitmex", "ascendex", "xt",
        "toobit",
    ] {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ioc_break_even_uses_expected_cost_not_price_limit_ceiling() {
        // Four 0.05% taker fees + four expected 0.02% slips = 0.28%.
        assert!((ioc_break_even_pct(0.0005, 0.0005) - 0.28).abs() < 1e-12);
    }

    #[test]
    fn entry_cannot_begin_inside_its_own_hard_stop() {
        let immediate = initial_liquidation_net_pct(
            true, 101.0, 100.0, 101.2, 99.8, 1.0, 0.0005, 0.0005,
        )
        .unwrap();
        assert!(immediate < -0.4);
        assert!(!hard_stop_entry_is_compatible(immediate, 0.4, 0.05));
        assert!(hard_stop_entry_is_compatible(immediate, 0.7, 0.05));
        assert!(hard_stop_entry_is_compatible(immediate, 0.0, 0.05));
        assert!(!hard_stop_entry_is_compatible(f64::NAN, 0.7, 0.05));
        assert!(!hard_stop_entry_is_compatible(immediate, 0.7, -0.01));
    }

    #[test]
    fn structural_baseline_is_mandatory_even_without_extra_deviation() {
        assert_eq!(configured_structural_reserve(0.0, false, 1.65), None);
        assert_eq!(configured_structural_reserve(0.0, true, 1.65), Some(1.65));
        assert_eq!(configured_structural_reserve(0.5, false, 1.65), None);
        assert_eq!(configured_structural_reserve(0.5, true, 1.65), Some(1.65));
    }

    #[test]
    fn direction_flip_cannot_be_treated_as_mean_reverting_edge() {
        assert!(!directional_structural_regime_is_stable(
            true, false, -5.1507, 4.5306
        ));
        assert!(directional_structural_regime_is_stable(
            true, true, 0.1409, 4.6924
        ));
        assert!(!directional_structural_regime_is_stable(
            false, true, 0.1409, 4.6924
        ));
        assert!(!directional_structural_regime_is_stable(
            true, true, f64::NAN, 4.6924
        ));
    }

    #[test]
    fn entry_budget_reserves_round_trip_costs_even_without_extra_deviation() {
        let budget = entry_execution_budget(0.5, 0.1, 2.5, 0.0, 0.0, 0.28)
            .expect("configured entry spread should pass");
        assert!((budget - 0.22).abs() < 1e-12);
        assert!(entry_execution_budget(2.6, 1.2, 2.5, 0.0, 0.0, 0.28).is_none());
    }

    #[test]
    fn entry_budget_adds_costs_to_enabled_structural_deviation() {
        let budget = entry_execution_budget(2.1, 1.2, 2.5, 1.0, 0.7, 0.28)
            .expect("raw and structural profile limits should pass");
        assert!((budget - 0.12).abs() < 1e-12);
        assert!(entry_execution_budget(1.98, 1.2, 2.5, 1.0, 0.7, 0.28).is_none());
    }

    #[test]
    fn one_canary_entry_is_rejected_after_costs_and_deviation_are_reserved() {
        // Corrected 24h baseline ~= 2.64%, actual fee/slippage reserve 0.30%,
        // configured extra deviation 0.50%; the 3.3217% fill had no such edge.
        assert!(entry_execution_budget(3.3217, 0.5, 4.0, 2.64, 0.5, 0.30).is_none());
    }

    #[test]
    fn prewarm_keeps_structural_baseline_and_costs_fully_reserved() {
        // Live ONEUSDT snapshot: raw 3.1652%, baseline 2.7613%. Even a 50%
        // prewarm margin cannot turn the persistent premium into usable edge.
        assert!(prewarm_execution_budget(3.1652, 0.5, 4.0, 2.7613, 0.5, 0.28, 0.5)
            .is_none());
        assert!(prewarm_execution_budget(3.45, 0.5, 4.0, 2.7613, 0.5, 0.28, 0.5)
            .is_some());

        // Prewarm still starts before the full 0.78% executable floor.
        assert!(entry_execution_budget(0.69, 0.5, 4.0, 0.0, 0.5, 0.28).is_none());
        assert!(prewarm_execution_budget(0.69, 0.5, 4.0, 0.0, 0.5, 0.28, 0.5)
            .is_some());
    }

    #[test]
    fn l2_depth_requirement_respects_profile_without_hidden_floor() {
        assert_eq!(required_depth_usd(6.0, 200.0), Some(12.0));
        assert_eq!(required_depth_usd(6.0, 0.0), None);
    }

    #[test]
    fn volume_tolerance_applies_lower_and_upper_bounds_to_both_legs() {
        assert!(volumes_within_tolerance(6.0, 15.0, 5.1, 6.9));
        assert!(!volumes_within_tolerance(6.0, 15.0, 5.09, 6.0));
        assert!(!volumes_within_tolerance(6.0, 15.0, 6.0, 6.91));
    }

    #[test]
    fn equal_multipliers_produce_exact_target_volume() {
        let lot = compute_lot_size(1000.0, 100.0, 1.0, 1.0, 15.0).expect("лот должен найтись");
        assert!((lot.phys_qty - 10.0).abs() < 1e-9);
        assert!((lot.contracts_a - 10.0).abs() < 1e-9);
        assert!((lot.contracts_b - 10.0).abs() < 1e-9);
    }

    #[test]
    fn different_multipliers_keep_both_legs_identical() {
        // Главный инвариант: физический объем обеих ног обязан совпадать,
        // иначе связка не дельта-нейтральна.
        let lot = compute_lot_size(1000.0, 100.0, 1.0, 0.1, 15.0).expect("лот должен найтись");
        assert!((lot.contracts_a * 1.0 - lot.phys_qty).abs() < 1e-8);
        assert!((lot.contracts_b * 0.1 - lot.phys_qty).abs() < 1e-8);
    }

    #[test]
    fn volume_never_exceeds_tolerance_ceiling() {
        // mult 4.0 -> ближайший лот 12 монет = $1200 > потолка +15% ($1150),
        // поэтому объем скейлится вниз до 8 монет = $800.
        let lot = compute_lot_size(1000.0, 100.0, 4.0, 4.0, 15.0).expect("лот должен найтись");
        assert!(lot.phys_qty * 100.0 <= 1000.0 * 1.15 + 1e-9);
        assert!((lot.phys_qty - 8.0).abs() < 1e-9);
        assert!((lot.contracts_a - 2.0).abs() < 1e-9);
        assert!((lot.contracts_b - 2.0).abs() < 1e-9);
    }

    #[test]
    fn non_divisible_steps_preserve_common_grid() {
        let l = compute_lot_size(10.0, 1.0, 3.0, 2.0, 0.0).unwrap();
        assert_eq!(l.phys_qty, 6.0);
        assert_eq!(l.contracts_a * 3.0, l.contracts_b * 2.0);
    }

    #[test]
    fn small_budget_is_rejected_instead_of_exceeded() {
        assert!(compute_lot_size(1.0, 100.0, 1.0, 1.0, 15.0).is_none());
    }

    #[test]
    fn invalid_steps_fail_closed() {
        for x in [0.0, -1.0, f64::NAN, f64::INFINITY, 0.000000001] {
            assert!(common_quantity_step(x, 1.0).is_none());
        }
    }

    #[test]
    fn grid_invariants_across_decimal_steps() {
        for a in [0.001, 0.02, 0.03, 0.13, 2.0, 3.0] {
            for b in [0.001, 0.02, 0.03, 0.13, 2.0, 3.0] {
                for v in [1.0, 10.0, 23.0, 100.0] {
                    if let Some(l) = compute_lot_size(v, 1.0, a, b, 0.0) {
                        assert!(l.phys_qty <= v + 1e-9);
                        assert!((l.contracts_a - l.contracts_a.round()).abs() < 1e-8);
                        assert!((l.contracts_b - l.contracts_b.round()).abs() < 1e-8);
                    }
                }
            }
        }
    }
}
