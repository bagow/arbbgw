//! Shared execution helpers — extracted from entry_manager.rs, exit_manager.rs, and hft_cycle.rs.
//!
//! Contains the low-level subroutines that are identical between EMA and HFT strategies:
//! - Fill verification (fast loop, slow loop, position fallback)
//! - Close execution (parallel close + error handling + PnL calculation)
//! - Volume matching (min_phys / step rounding)
//! - Entry spread calculation from actual fill prices

use std::sync::Arc;
use reqwest::Client;

use crate::engine::exchanges::traits::ExchangeConnector;
use crate::engine::real::ExchangeKeys;
use crate::engine::utils::is_position_missing_or_liquidated_error;

// ─── FILL VERIFICATION ──────────────────────────────────────────────────────

/// Configuration for fill verification behavior.
pub struct VerifyConfig {
    /// Number of fast verify retries
    pub fast_retries: i64,
    /// Delay between fast verify attempts (ms)
    pub fast_delay_ms: u64,
    /// Number of slow verify attempts (0 to skip slow path)
    pub slow_retries: usize,
    /// Delay between slow verify attempts (ms)
    pub slow_delay_ms: u64,
    /// Whether to attempt position fallback if all else fails
    pub position_fallback: bool,
    /// Contract multiplier for this leg
    pub mult: f64,
}

/// Result of fill verification.
pub struct VerifyResult {
    /// True if fill was confirmed
    #[allow(dead_code)]
    pub got_fill: bool,
    /// Physical volume filled (None if unconfirmed)
    pub phys_volume: Option<f64>,
    /// Fill price
    pub fill_price: f64,
    /// Fee paid
    pub fee: f64,
}

/// Verify that an order was filled. Tries fast loop → slow loop → position fallback.
///
/// This is the exact same algorithm used by both EMA (entry_manager.rs) and HFT (hft_cycle.rs).
pub async fn verify_fill(
    connector: &Arc<dyn ExchangeConnector>,
    http: &Client,
    keys: &Arc<ExchangeKeys>,
    pair: &str,
    order_id: &Option<String>,
    initial_fill_price: f64,
    initial_fee: f64,
    initial_phys: Option<f64>,
    expected_qty: f64,
    dry_run: bool,
    config: VerifyConfig,
    label: &str,
) -> VerifyResult {
    let mut fill_price = initial_fill_price;
    let mut fee = initial_fee;
    let mut phys = initial_phys;
    let mut got = dry_run || fill_price > 0.0;

    // Inline fill shortcut — skip verify if we already have price + volume
    if got && fill_price > 0.0 && phys.is_none() {
        phys = Some(expected_qty);
        tracing::info!("[{}] ⚡ Inline fill: price={:.6} fee={:.6} — skipping verify", label, fill_price, fee);
    }

    // ── Fast verify loop ──
    for attempt in 0..config.fast_retries {
        if got && phys.is_some() { break; }
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(config.fast_delay_ms)).await;
        }
        if let Some(ref oid) = order_id {
            if let Ok((p, f, ev)) = connector.fetch_order(http, keys, pair, oid, config.mult).await {
                if p > 0.0 { fill_price = p; }
                if f > 1e-9 { fee = f; }
                if ev > 1e-12 { phys = Some(ev); got = true; }
            }
        }
    }

    // ── Slow verify loop ──
    if !got && order_id.is_some() && !dry_run && config.slow_retries > 0 {
        tracing::warn!("[{}] ⏳ {}: orderId={} fill not confirmed. Extended wait...",
            label, pair, order_id.as_deref().unwrap_or("?"));
        for slow_attempt in 0..config.slow_retries {
            tokio::time::sleep(std::time::Duration::from_millis(config.slow_delay_ms)).await;
            if let Some(ref oid) = order_id {
                match connector.fetch_order(http, keys, pair, oid, config.mult).await {
                    Ok((p, f, ev)) => {
                        if ev > 1e-12 {
                            phys = Some(ev); got = true;
                            if p > 0.0 { fill_price = p; }
                            if f > 1e-9 { fee = f; }
                            tracing::info!("[{}] ✅ {}: fill confirmed on slow attempt {}", label, pair, slow_attempt + 1);
                            break;
                        }
                        if p == 0.0 && f == 0.0 && ev == 0.0 { break; }
                    }
                    Err(_) => {}
                }
            }
        }
    }

    // ── Position fallback ──
    if config.position_fallback && phys.unwrap_or(0.0) <= 1e-12 && order_id.is_some() && !dry_run {
        tracing::warn!("[{}] 🔍 {}: fill unverified. Checking position...", label, pair);
        if let Ok(positions) = connector.fetch_positions(http, keys).await {
            if let Some(pos) = positions.iter().find(|p| {
                crate::engine::real::symbols_match(&p.symbol, pair) && p.size.abs() > 1e-12
            }) {
                tracing::warn!("[{}] 🚨 {}: Position FOUND despite verify failure!", label, pair);
                phys = Some(pos.size.abs());
                got = true;
                if pos.entry_price > 0.0 { fill_price = pos.entry_price; }
            }
        }
    }

    VerifyResult {
        got_fill: got,
        phys_volume: phys,
        fill_price,
        fee,
    }
}

// ─── VOLUME MATCHING ─────────────────────────────────────────────────────────

/// Result of volume matching.
pub struct MatchedVolume {
    /// Matched physical quantity (rounded to valid lot sizes for both exchanges)
    pub matched_phys: f64,
    /// Excess on leg A that needs trimming
    pub delta_a: f64,
    /// Excess on leg B that needs trimming
    pub delta_b: f64,
}

/// Match fill volumes from two legs to the nearest valid lot size for both exchanges.
///
/// Returns the matched physical quantity and any deltas that need trimming.
pub fn match_volumes(phys_a: f64, phys_b: f64, mult_a: f64, mult_b: f64) -> MatchedVolume {
    let min_phys = phys_a.min(phys_b);
    let step_a = mult_a.max(1e-12);
    let step_b = mult_b.max(1e-12);
    let valid_a = (min_phys / step_a).floor() * step_a;
    let valid_b = (min_phys / step_b).floor() * step_b;
    let matched_phys = valid_a.min(valid_b);

    MatchedVolume {
        matched_phys,
        delta_a: phys_a - matched_phys,
        delta_b: phys_b - matched_phys,
    }
}

// ─── ENTRY SPREAD CALCULATION ────────────────────────────────────────────────

/// Compute the actual entry spread from fill prices.
/// Falls back to signal spread if fill prices are zero.
pub fn compute_entry_spread(fill_a: f64, fill_b: f64, is_a_higher: bool, fallback_spread: f64) -> f64 {
    if fill_a > 0.0 && fill_b > 0.0 {
        if is_a_higher {
            (fill_a - fill_b) / fill_b.max(1e-12) * 100.0
        } else {
            (fill_b - fill_a) / fill_a.max(1e-12) * 100.0
        }
    } else {
        fallback_spread
    }
}

// ─── CLOSE EXECUTION ─────────────────────────────────────────────────────────

/// Result of a close execution.
pub struct CloseResult {
    /// Real exit price for leg A
    pub real_exit_a: f64,
    /// Real exit price for leg B
    pub real_exit_b: f64,
    /// Total exit fees
    pub exit_fee: f64,
    /// True if leg A was externally closed/liquidated
    pub leg_a_external: bool,
    /// True if leg B was externally closed/liquidated
    pub leg_b_external: bool,
    /// Error message (empty if successful)
    pub error_msg: String,
    /// Gross PnL for leg A
    pub pnl_a: f64,
    /// Gross PnL for leg B
    pub pnl_b: f64,
}

/// Execute close orders on both legs in parallel, handle errors and compute PnL.
///
/// This is the exact same algorithm used by both EMA (exit_manager.rs) and HFT (hft_cycle.rs).
pub async fn execute_close(
    conn_a: &Arc<dyn ExchangeConnector>,
    conn_b: &Arc<dyn ExchangeConnector>,
    http: &Client,
    keys: &Arc<ExchangeKeys>,
    pair: &str,
    is_a_higher: bool,
    q_a: f64,
    q_b: f64,
    leverage: f64,
    exit_price_a: f64,
    exit_price_b: f64,
    use_ioc: bool,
    slippage_percent: f64,
    mult_a: f64,
    mult_b: f64,
    tick_a: f64,
    tick_b: f64,
    close_verify_retries: u32,
    close_verify_delay: u64,
    api_timeout_ms: u64,
    entry_a: f64,
    entry_b: f64,
    trade_coins_a: f64,
    trade_coins_b: f64,
) -> CloseResult {
    let (ra, rb) = tokio::join!(
        conn_a.close_position(
            http, keys, pair, is_a_higher, q_a, false, leverage,
            exit_price_a, use_ioc, slippage_percent, mult_a, tick_a,
            close_verify_retries, close_verify_delay, api_timeout_ms
        ),
        conn_b.close_position(
            http, keys, pair, !is_a_higher, q_b, false, leverage,
            exit_price_b, use_ioc, slippage_percent, mult_b, tick_b,
            close_verify_retries, close_verify_delay, api_timeout_ms
        )
    );

    let mut real_exit_a = 0.0;
    let mut real_exit_b = 0.0;
    let mut exit_fee = 0.0;
    let mut error_msg = String::new();
    let mut leg_a_external = false;
    let mut leg_b_external = false;

    match ra {
        Ok(rc) => { real_exit_a = rc.final_price; exit_fee += rc.total_fee; }
        Err(e) => {
            if is_position_missing_or_liquidated_error(&e) {
                leg_a_external = true;
                real_exit_a = entry_a;
            } else {
                error_msg.push_str(&format!("Leg A close failed: {} ", e));
            }
        }
    }
    match rb {
        Ok(rc) => { real_exit_b = rc.final_price; exit_fee += rc.total_fee; }
        Err(e) => {
            if is_position_missing_or_liquidated_error(&e) {
                leg_b_external = true;
                real_exit_b = entry_b;
            } else {
                error_msg.push_str(&format!("Leg B close failed: {}", e));
            }
        }
    }

    // 🛡️ EXIT PRICE SANITY CHECK: Protect against connector bugs returning wrong price units.
    if real_exit_a > 0.0 && exit_price_a > 0.0 && !leg_a_external {
        let ratio = real_exit_a / exit_price_a;
        if ratio > 2.0 || ratio < 0.5 {
            tracing::error!(
                "[close] 🚨 EXIT PRICE SANITY FAIL {}/A: real_exit={:.8} expected={:.8} ratio={:.2}. Using expected!",
                pair, real_exit_a, exit_price_a, ratio
            );
            real_exit_a = exit_price_a;
        }
    }
    if real_exit_b > 0.0 && exit_price_b > 0.0 && !leg_b_external {
        let ratio = real_exit_b / exit_price_b;
        if ratio > 2.0 || ratio < 0.5 {
            tracing::error!(
                "[close] 🚨 EXIT PRICE SANITY FAIL {}/B: real_exit={:.8} expected={:.8} ratio={:.2}. Using expected!",
                pair, real_exit_b, exit_price_b, ratio
            );
            real_exit_b = exit_price_b;
        }
    }

    // Compute real PnL from actual fill prices
    let pnl_result = crate::engine::pnl::calculate_gross_pnl(
        is_a_higher, entry_a, entry_b,
        real_exit_a, real_exit_b, trade_coins_a, trade_coins_b,
    );
    let pnl_a = pnl_result.pnl_a;
    let pnl_b = pnl_result.pnl_b;

    CloseResult {
        real_exit_a,
        real_exit_b,
        exit_fee,
        leg_a_external,
        leg_b_external,
        error_msg,
        pnl_a,
        pnl_b,
    }
}

// ─── VWAP L2 VERIFICATION ────────────────────────────────────────────────────

/// Simulates executing a market order against the L2 orderbook to find the VWAP
/// (Volume Weighted Average Price) of the fill.
/// Returns Some(vwap_price) if the orderbook has enough depth to fill `required_coins`.
/// Returns None if the orderbook is too thin.
pub fn verify_l2_vwap(
    book_res: Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), String>,
    is_buy: bool,
    required_coins: f64,
    multiplier: f64,
) -> Option<f64> {
    if let Ok((bids, asks)) = book_res {
        let levels = if is_buy { asks } else { bids };
        let mut cum_vol = 0.0;
        let mut cum_cost = 0.0;
        
        for (px, sz) in levels {
            // Some connectors might need multiplier adjustment here, depending on how they store L2 cache.
            // In the current codebase, we pass multiplier to handle cases where sizes aren't in coins.
            let base_sz = if multiplier > 0.0 && multiplier != 1.0 { sz * multiplier } else { sz };
            let remaining = required_coins - cum_vol;
            if remaining <= 0.0 { break; }
            
            let take = if base_sz > remaining { remaining } else { base_sz };
            cum_vol += take;
            cum_cost += take * px;
        }
        
        // We tolerate a tiny floating point gap (1%) for full fill check
        if cum_vol >= required_coins * 0.99 { 
            Some(cum_cost / cum_vol)
        } else {
            None 
        }
    } else {
        None
    }
}
