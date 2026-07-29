pub struct PnlResult {
    pub gross_pnl: f64,
    pub pnl_a: f64,
    pub pnl_b: f64,
}

/// Calculate current gross PnL for a position based on actual coin sizes
pub fn calculate_gross_pnl(
    is_a_higher: bool,
    entry_price_a: f64,
    entry_price_b: f64,
    current_price_a: f64,
    current_price_b: f64,
    coins_a: f64,
    coins_b: f64,
) -> PnlResult {
    let pnl_a = if is_a_higher {
        (entry_price_a - current_price_a) * coins_a
    } else {
        (current_price_a - entry_price_a) * coins_a
    };

    let pnl_b = if is_a_higher {
        (current_price_b - entry_price_b) * coins_b
    } else {
        (entry_price_b - current_price_b) * coins_b
    };

    PnlResult {
        gross_pnl: pnl_a + pnl_b,
        pnl_a,
        pnl_b,
    }
}

/// Calculate current spread based on bid/ask
pub fn calculate_current_spread(
    is_a_higher: bool,
    bid_a: f64,
    ask_a: f64,
    bid_b: f64,
    ask_b: f64,
) -> f64 {
    if is_a_higher {
        (bid_a - ask_b) / ask_b.max(1e-12) * 100.0
    } else {
        (bid_b - ask_a) / ask_a.max(1e-12) * 100.0
    }
}

/// Calculate pessimistic exit spread (for stop loss / take profit checks)
pub fn calculate_exit_spread(
    is_a_higher: bool,
    bid_a: f64,
    ask_a: f64,
    bid_b: f64,
    ask_b: f64,
) -> f64 {
    if is_a_higher {
        (ask_a - bid_b) / bid_b.max(1e-12) * 100.0
    } else {
        (ask_b - bid_a) / bid_a.max(1e-12) * 100.0
    }
}


/// Calculate incremental funding based on a dt fraction
pub fn calculate_funding_increment(
    is_a_higher: bool,
    entry_price_a: f64,
    entry_price_b: f64,
    coins_a: f64,
    coins_b: f64,
    fr_a: f64,
    fr_b: f64,
    dt_8h: f64,
) -> f64 {
    let notional_a = entry_price_a * coins_a;
    let notional_b = entry_price_b * coins_b;
    let long_a = !is_a_higher;
    let long_b = is_a_higher;
    
    let fund_incr_a = if long_a { notional_a * fr_a * dt_8h } else { -notional_a * fr_a * dt_8h };
    let fund_incr_b = if long_b { notional_b * fr_b * dt_8h } else { -notional_b * fr_b * dt_8h };
    
    fund_incr_a + fund_incr_b
}

/// Net PnL = Gross PnL - Commission - Funding
/// NOTE: slippage is NOT subtracted because gross_pnl already uses actual fill
/// prices (which reflect slippage). Subtracting slippage_cost would double-count it.
/// slippage_cost is stored in DB for diagnostics only.
pub fn calculate_net_pnl(
    gross_pnl: f64,
    commission_cost: f64,
    _slippage_cost: f64,
    funding_cost: f64,
) -> f64 {
    gross_pnl - commission_cost - funding_cost
}

pub struct LivePnlState {
    pub c_price_a: f64,
    pub c_price_b: f64,
    pub exit_price_a: f64,
    pub exit_price_b: f64,
    pub current_spread: f64,
    pub exit_spread: f64,
    pub live_pnl_a: f64,
    pub live_pnl_b: f64,
    pub live_gross_pnl: f64,
    pub exec_gross_pnl: f64,
    pub coins_a: f64,
    pub coins_b: f64,
    pub is_valid: bool,
}

pub fn calculate_live_pnl_state(
    trade: &crate::engine::real::Trade,
    prices: &crate::state::PriceMap,
    e_ctx: &crate::state::EngineContext,
    manual_close: bool,
    closing_retry: bool,
) -> LivePnlState {
    let is_a_higher = trade.direction.starts_with(&trade.exchange_a);
    let key_a = format!("{}:{}", trade.exchange_a, trade.pair);
    let key_b = format!("{}:{}", trade.exchange_b, trade.pair);

    let data_a = prices.get(&trade.exchange_a).and_then(|m| m.get(&trade.pair).cloned());
    let data_b = prices.get(&trade.exchange_b).and_then(|m| m.get(&trade.pair).cloned());

    if (data_a.is_none() || data_b.is_none()) && !manual_close && !closing_retry {
        return LivePnlState {
            c_price_a: 0.0, c_price_b: 0.0, exit_price_a: 0.0, exit_price_b: 0.0,
            current_spread: 0.0, exit_spread: 0.0, live_pnl_a: 0.0, live_pnl_b: 0.0,
            live_gross_pnl: 0.0, exec_gross_pnl: 0.0, coins_a: 0.0, coins_b: 0.0,
            is_valid: false,
        };
    }

    let mut exit_price_a = trade.entry_price_a.max(1e-12);
    let mut exit_price_b = trade.entry_price_b.max(1e-12);
    let mut c_price_a = exit_price_a;
    let mut c_price_b = exit_price_b;
    let mut current_spread = 0.0;
    let mut exit_spread = 0.0;
    let mut is_valid = true;

    if let (Some(da), Some(db)) = (&data_a, &data_b) {
        if da.bid <= 0.0 || da.ask <= 0.0 || db.bid <= 0.0 || db.ask <= 0.0 {
            c_price_a = if da.price > 0.0 { da.price } else { trade.entry_price_a };
            c_price_b = if db.price > 0.0 { db.price } else { trade.entry_price_b };
            exit_price_a = c_price_a;
            exit_price_b = c_price_b;
            if !manual_close {
                is_valid = false;
            }
        } else {
            c_price_a = da.price;
            c_price_b = db.price;
            exit_price_a = if is_a_higher { da.ask } else { da.bid };
            exit_price_b = if is_a_higher { db.bid } else { db.ask };

            current_spread = calculate_current_spread(is_a_higher, da.bid, da.ask, db.bid, db.ask);
            exit_spread = calculate_exit_spread(is_a_higher, da.bid, da.ask, db.bid, db.ask);
        }
    } else if !manual_close && !closing_retry {
        is_valid = false;
    }

    let mult_a_live = e_ctx.multipliers.get(&key_a).map(|v| *v).unwrap_or(1.0);
    let mult_b_live = e_ctx.multipliers.get(&key_b).map(|v| *v).unwrap_or(1.0);
    let coins_a = trade.contracts_a * mult_a_live;
    let coins_b = trade.contracts_b * mult_b_live;

    let live_pnl = calculate_gross_pnl(
        is_a_higher, trade.entry_price_a, trade.entry_price_b,
        c_price_a, c_price_b, coins_a, coins_b,
    );

    let exec_pnl = calculate_gross_pnl(
        is_a_higher, trade.entry_price_a, trade.entry_price_b,
        exit_price_a, exit_price_b, coins_a, coins_b,
    );

    LivePnlState {
        c_price_a,
        c_price_b,
        exit_price_a,
        exit_price_b,
        current_spread,
        exit_spread,
        live_pnl_a: live_pnl.pnl_a,
        live_pnl_b: live_pnl.pnl_b,
        live_gross_pnl: live_pnl.gross_pnl,
        exec_gross_pnl: exec_pnl.gross_pnl,
        coins_a,
        coins_b,
        is_valid,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn gross_pnl_converging_spread_is_profitable() {
        // is_a_higher = true -> шорт A, лонг B. Спред 101/100 схлопывается в 100.5/100.5.
        let r = calculate_gross_pnl(true, 101.0, 100.0, 100.5, 100.5, 1.0, 1.0);
        approx(r.pnl_a, 0.5);
        approx(r.pnl_b, 0.5);
        approx(r.gross_pnl, 1.0);
    }

    #[test]
    fn gross_pnl_widening_spread_is_a_loss() {
        let r = calculate_gross_pnl(true, 101.0, 100.0, 102.0, 99.0, 1.0, 1.0);
        approx(r.pnl_a, -1.0);
        approx(r.pnl_b, -1.0);
        approx(r.gross_pnl, -2.0);
    }

    #[test]
    fn gross_pnl_is_direction_symmetric() {
        let a_high = calculate_gross_pnl(true, 101.0, 100.0, 100.5, 100.5, 2.0, 2.0);
        let b_high = calculate_gross_pnl(false, 100.0, 101.0, 100.5, 100.5, 2.0, 2.0);
        approx(a_high.gross_pnl, b_high.gross_pnl);
    }

    #[test]
    fn gross_pnl_scales_linearly_with_coin_size() {
        let one = calculate_gross_pnl(true, 101.0, 100.0, 100.5, 100.5, 1.0, 1.0);
        let ten = calculate_gross_pnl(true, 101.0, 100.0, 100.5, 100.5, 10.0, 10.0);
        approx(ten.gross_pnl, one.gross_pnl * 10.0);
    }

    #[test]
    fn current_spread_uses_optimistic_quotes() {
        approx(calculate_current_spread(true, 101.0, 101.2, 99.8, 100.0), 1.0);
        approx(calculate_current_spread(false, 99.8, 100.0, 101.0, 101.2), 1.0);
    }

    #[test]
    fn exit_spread_is_pessimistic_vs_current_spread() {
        let cur = calculate_current_spread(true, 101.0, 101.2, 99.8, 100.0);
        let exit = calculate_exit_spread(true, 101.0, 101.2, 99.8, 100.0);
        assert!(
            exit > cur,
            "exit spread {exit} должен быть хуже текущего {cur}"
        );
    }

    #[test]
    fn spread_helpers_never_return_inf_or_nan() {
        assert!(calculate_current_spread(true, 0.0, 0.0, 0.0, 0.0).is_finite());
        assert!(calculate_current_spread(false, 0.0, 0.0, 0.0, 0.0).is_finite());
        assert!(calculate_exit_spread(true, 0.0, 0.0, 0.0, 0.0).is_finite());
        assert!(calculate_exit_spread(false, 0.0, 0.0, 0.0, 0.0).is_finite());
    }

    #[test]
    fn funding_cancels_out_when_both_rates_are_equal() {
        // Дельта-нейтральная связка при одинаковых ставках не должна стоить ничего.
        let f = calculate_funding_increment(true, 100.0, 100.0, 1.0, 1.0, 0.0001, 0.0001, 1.0);
        approx(f, 0.0);
    }

    #[test]
    fn funding_sign_follows_leg_direction() {
        // is_a_higher = true -> шорт A (получает funding), лонг B (платит).
        let long_leg_pays = calculate_funding_increment(true, 100.0, 100.0, 1.0, 1.0, 0.0, 0.0001, 1.0);
        assert!(long_leg_pays > 0.0, "лонг при положительном FR = расход");

        let short_leg_receives = calculate_funding_increment(true, 100.0, 100.0, 1.0, 1.0, 0.0001, 0.0, 1.0);
        assert!(short_leg_receives < 0.0, "шорт при положительном FR = доход");
    }

    #[test]
    fn funding_scales_linearly_with_time() {
        let full = calculate_funding_increment(true, 100.0, 100.0, 1.0, 1.0, 0.0, 0.0001, 1.0);
        let half = calculate_funding_increment(true, 100.0, 100.0, 1.0, 1.0, 0.0, 0.0001, 0.5);
        approx(half * 2.0, full);
    }

    #[test]
    fn net_pnl_ignores_slippage_to_avoid_double_counting() {
        let with_slippage = calculate_net_pnl(10.0, 1.0, 5.0, 2.0);
        let without_slippage = calculate_net_pnl(10.0, 1.0, 0.0, 2.0);
        approx(with_slippage, 7.0);
        approx(with_slippage, without_slippage);
    }
}
