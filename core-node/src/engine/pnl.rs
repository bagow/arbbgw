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
