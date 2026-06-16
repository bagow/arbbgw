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

/// Helper to get L1 prices or fallback to entry prices if missing/empty
#[allow(dead_code)]
pub fn get_fallback_prices(
    is_a_higher: bool,
    entry_price_a: f64,
    entry_price_b: f64,
    data_a: Option<&shared::TickerData>,
    data_b: Option<&shared::TickerData>,
) -> (f64, f64, f64, f64, f64, f64) {
    let (mut c_price_a, mut c_price_b, mut exit_price_a, mut exit_price_b) = (entry_price_a, entry_price_b, entry_price_a, entry_price_b);
    let mut current_spread = 0.0;
    let mut exit_spread = 0.0;

    if let (Some(da), Some(db)) = (data_a, data_b) {
        if da.bid <= 0.0 || da.ask <= 0.0 || db.bid <= 0.0 || db.ask <= 0.0 {
            // Empty orderbook, fallback
            c_price_a = if da.price > 0.0 { da.price } else { entry_price_a };
            c_price_b = if db.price > 0.0 { db.price } else { entry_price_b };
            exit_price_a = c_price_a;
            exit_price_b = c_price_b;
        } else {
            c_price_a = da.price;
            c_price_b = db.price;
            exit_price_a = if is_a_higher { da.ask } else { da.bid };
            exit_price_b = if is_a_higher { db.bid } else { db.ask };

            current_spread = calculate_current_spread(is_a_higher, da.bid, da.ask, db.bid, db.ask);
            exit_spread = calculate_exit_spread(is_a_higher, da.bid, da.ask, db.bid, db.ask);
        }
    }

    (c_price_a, c_price_b, exit_price_a, exit_price_b, current_spread, exit_spread)
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

/// Net PnL = Gross PnL - Commission - Slippage - Funding
pub fn calculate_net_pnl(
    gross_pnl: f64,
    commission_cost: f64,
    slippage_cost: f64,
    funding_cost: f64,
) -> f64 {
    gross_pnl - commission_cost - slippage_cost - funding_cost
}
