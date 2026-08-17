//! Shared execution helpers — extracted from entry_manager.rs and exit_manager.rs.
//!
//! Contains the low-level subroutines that are identical between EMA and HFT strategies:
//! - Fill verification (fast loop, slow loop, position fallback)
//! - Close execution (parallel close + error handling + PnL calculation)
//! - Volume matching (min_phys / step rounding)
//! - Entry spread calculation from actual fill prices

use std::sync::Arc;
use reqwest::Client;
use tokio_rusqlite::Connection;

use crate::engine::exchanges::traits::ExchangeConnector;
use crate::engine::real::ExchangeKeys;
use crate::engine::utils::is_position_missing_or_liquidated_error;
use crate::state::EngineContext;
use crate::engine::db::TradeRepository;
use shared::TechnicalSettings;

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
/// This is the exact same algorithm used by entry_manager.rs.
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
    let mut order_definitively_unfilled = false;
    for attempt in 0..config.fast_retries {
        if got && phys.is_some() { break; }
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(config.fast_delay_ms)).await;
        }
        if let Some(ref oid) = order_id {
            if let Ok((p, f, ev)) = connector.fetch_order(http, keys, pair, oid, config.mult).await {
                if p > 0.0 { fill_price = p; }
                if f > 1e-9 { fee = f; }
                if ev > 1e-12 {
                    phys = Some(ev);
                    got = true;
                    break;
                } else if attempt > 0 || (p == 0.0 && f == 0.0) {
                    // fetch_order returned Ok with 0 volume — order is confirmed EXPIRED/UNFILLED
                    order_definitively_unfilled = true;
                    break;
                }
            }
        }
    }

    // ── Slow verify loop (skipped if order confirmed EXPIRED/UNFILLED) ──
    if !got && !order_definitively_unfilled && order_id.is_some() && !dry_run && config.slow_retries > 0 {
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
                tracing::warn!("[{}] 🚨 {}: Position FOUND despite verify failure! size={:.6} entry_price={:.6}",
                    label, pair, pos.size.abs(), pos.entry_price);
                phys = Some(pos.size.abs());
                got = true;
                // IMPORTANT: Do NOT use pos.entry_price as fill_price!
                // entry_price from exchange position is the AVERAGE price of the ENTIRE position
                // (including old fills from previous trades), not the fill price of this specific order.
                // Leaving fill_price=0 lets compute_entry_spread use fallback signal spread,
                // which is much more accurate than a stale average position price.
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
/// This is the exact same algorithm used by exit_manager.rs.
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

// ─── NEW ABSTRACTED HELPERS ──────────────────────────────────────────────────

pub async fn trim_asymmetric_legs(
    http: &Client,
    keys: &Arc<ExchangeKeys>,
    pair: &str,
    exa: &str,
    exb: &str,
    is_a_higher: bool,
    delta_a: f64,
    delta_b: f64,
    mult_a: f64,
    mult_b: f64,
    fill_a: f64,
    fill_b: f64,
    api_timeout_ms: u64,
    e_ctx: &EngineContext,
) -> (f64, f64, bool) {
    let mut trimming_loss = 0.0;
    let mut trimming_fee = 0.0;
    let mut all_trimmed = true;
    let hc = http.clone();
    let kc = Arc::clone(keys);
    let fp = pair.to_string();
    let key_a = format!("{}:{}", exa, pair);
    let key_b = format!("{}:{}", exb, pair);
    const MAX_TRIM_RETRIES: u32 = 10;
    const TRIM_RETRY_DELAY_MS: u64 = 3000;

    if delta_a > 1e-12 {
        let phys_to_dump = delta_a;
        if phys_to_dump >= mult_a.min(1e-8) {
            // Verify position actually exists before attempting trim (guards against phantom WS fills)
            let conn_a = crate::engine::exchanges::get_connector(exa).unwrap();
            let pos_exists = match conn_a.fetch_positions(&hc, &kc).await {
                Ok(positions) => positions.iter().any(|p| {
                    crate::engine::real::symbols_match(&p.symbol, pair) && p.size.abs() > 1e-12
                }),
                Err(_) => true, // On error, assume position exists and try to trim
            };
            if !pos_exists {
                tracing::warn!("[real] ⚠️ TRIM {} LEG A skipped — no position found on {} (phantom fill from WS cache)", pair, exa);
            } else {
                let is_buy = is_a_higher;
                tracing::error!("[real] ⚠️ TRIMMING {} LEG A — Selling excess {:.6} phys", pair, phys_to_dump);
                let tick_for_close = e_ctx.tick_sizes.get(&key_a).map(|v| *v).unwrap_or(0.000001);
                let mut trimmed = false;
                for attempt in 0..MAX_TRIM_RETRIES {
                    match conn_a
                        .trim_position(&hc, &kc, &fp, is_buy, phys_to_dump, false, mult_a, tick_for_close, fill_a, api_timeout_ms).await {
                        Ok(rc) => {
                            trimming_fee += rc.total_fee;
                            let close_price = if rc.final_price > 1e-12 { rc.final_price } else { fill_a };
                            let gross_loss = if is_buy { (fill_a - close_price) * delta_a } else { (close_price - fill_a) * delta_a };
                            trimming_loss += gross_loss.min(0.0).abs();
                            trimmed = true;
                            break;
                        }
                        Err(e) => {
                            tracing::error!("[real] Failed to trim DELTA A (attempt {}/{}): {}", attempt + 1, MAX_TRIM_RETRIES, e);
                            // Re-verify position exists — if exchange confirms no position, stop retrying (phantom fill)
                            if let Ok(positions) = conn_a.fetch_positions(&hc, &kc).await {
                                if !positions.iter().any(|p| {
                                    crate::engine::real::symbols_match(&p.symbol, pair) && p.size.abs() > 1e-12
                                }) {
                                    tracing::warn!("[real] ⚠️ TRIM {} LEG A — position vanished on {} after attempt {}. Stopping retries (phantom fill).", pair, exa, attempt + 1);
                                    break;
                                }
                            }
                            if attempt + 1 < MAX_TRIM_RETRIES {
                                tokio::time::sleep(tokio::time::Duration::from_millis(TRIM_RETRY_DELAY_MS)).await;
                            }
                        }
                    }
                }
                if !trimmed {
                    tracing::error!("[real] 🚨 TRIM {} LEG A FAILED after {} retries — position still open on {}", pair, MAX_TRIM_RETRIES, exa);
                    all_trimmed = false;
                }
            }
        }
    }
    
    if delta_b > 1e-12 {
        let phys_to_dump = delta_b;
        if phys_to_dump >= mult_b.min(1e-8) {
            // Verify position actually exists before attempting trim (guards against phantom WS fills)
            let conn_b = crate::engine::exchanges::get_connector(exb).unwrap();
            let pos_exists = match conn_b.fetch_positions(&hc, &kc).await {
                Ok(positions) => positions.iter().any(|p| {
                    crate::engine::real::symbols_match(&p.symbol, pair) && p.size.abs() > 1e-12
                }),
                Err(_) => true,
            };
            if !pos_exists {
                tracing::warn!("[real] ⚠️ TRIM {} LEG B skipped — no position found on {} (phantom fill from WS cache)", pair, exb);
            } else {
                let is_buy = !is_a_higher;
                tracing::error!("[real] ⚠️ TRIMMING {} LEG B — Selling excess {:.6} phys", pair, phys_to_dump);
                let tick_for_close = e_ctx.tick_sizes.get(&key_b).map(|v| *v).unwrap_or(0.000001);
                let mut trimmed = false;
                for attempt in 0..MAX_TRIM_RETRIES {
                    match conn_b
                        .trim_position(&hc, &kc, &fp, is_buy, phys_to_dump, false, mult_b, tick_for_close, fill_b, api_timeout_ms).await {
                        Ok(rc) => {
                            trimming_fee += rc.total_fee;
                            let close_price = if rc.final_price > 1e-12 { rc.final_price } else { fill_b };
                            let gross_loss = if is_buy { (fill_b - close_price) * delta_b } else { (close_price - fill_b) * delta_b };
                            trimming_loss += gross_loss.min(0.0).abs();
                            trimmed = true;
                            break;
                        }
                        Err(e) => {
                            tracing::error!("[real] Failed to trim DELTA B (attempt {}/{}): {}", attempt + 1, MAX_TRIM_RETRIES, e);
                            // Re-verify position exists — if exchange confirms no position, stop retrying (phantom fill)
                            if let Ok(positions) = conn_b.fetch_positions(&hc, &kc).await {
                                if !positions.iter().any(|p| {
                                    crate::engine::real::symbols_match(&p.symbol, pair) && p.size.abs() > 1e-12
                                }) {
                                    tracing::warn!("[real] ⚠️ TRIM {} LEG B — position vanished on {} after attempt {}. Stopping retries (phantom fill).", pair, exb, attempt + 1);
                                    break;
                                }
                            }
                            if attempt + 1 < MAX_TRIM_RETRIES {
                                tokio::time::sleep(tokio::time::Duration::from_millis(TRIM_RETRY_DELAY_MS)).await;
                            }
                        }
                    }
                }
                if !trimmed {
                    tracing::error!("[real] 🚨 TRIM {} LEG B FAILED after {} retries — position still open on {}", pair, MAX_TRIM_RETRIES, exb);
                    all_trimmed = false;
                }
            }
        }
    }
    (trimming_fee, trimming_loss, all_trimmed)

}


pub struct SequentialEntryContext {
    pub pair: String,
    pub exa: String,
    pub exb: String,
    pub is_a_higher: bool,
    pub qty_a: f64,
    pub qty_b: f64,
    pub limit_price_a: f64,
    pub limit_price_b: f64,
    pub mult_a: f64,
    pub mult_b: f64,
    pub tick_a: f64,
    pub tick_b: f64,
    pub ioc_slippage_pct: f64,
    pub leverage: f64,
    pub dry_run: bool,
    pub sig_spread: f64,
    pub sig_structural_spread: f64,
    pub sig_direction: String,
    pub sig_entry_a: f64,
    pub sig_entry_b: f64,
    pub profile_id: String,
    pub break_even_pct: f64,
}

pub async fn execute_sequential_entry(
    ctx: SequentialEntryContext,
    http: &Client,
    keys: &Arc<ExchangeKeys>,
    db: &Connection,
    e_ctx: &Arc<EngineContext>,
    ts: &TechnicalSettings,
    sort_by_liquidity: bool,
) -> bool {
    let now = crate::engine::utils::now_ms();
    let conn_ensure_a = crate::engine::exchanges::get_connector(&ctx.exa).unwrap();
    let conn_ensure_b = crate::engine::exchanges::get_connector(&ctx.exb).unwrap();
    
    let rank_a = crate::engine::utils::liquidity_rank(&ctx.exa);
    let rank_b = crate::engine::utils::liquidity_rank(&ctx.exb);
    let a_is_less_liquid = if sort_by_liquidity { rank_a >= rank_b } else { true };

    let (first_label, second_label) = if a_is_less_liquid {
        (ctx.exa.as_str(), ctx.exb.as_str())
    } else {
        (ctx.exb.as_str(), ctx.exa.as_str())
    };

    if sort_by_liquidity {
        tracing::info!(
            "[real] 🔄 Последовательное исполнение: сначала {} (менее ликв.), затем {} для {}...",
            first_label, second_label, ctx.pair
        );
    }

    let t0_exec = std::time::Instant::now();

    // ── PHASE 1: Less liquid leg (or Leg A for HFT) ─────────────────────────
    let (first_conn, first_is_buy, first_qty, first_limit, first_mult, first_tick) = if a_is_less_liquid {
        (&conn_ensure_a, !ctx.is_a_higher, ctx.qty_a, ctx.limit_price_a, ctx.mult_a, ctx.tick_a)
    } else {
        (&conn_ensure_b, ctx.is_a_higher, ctx.qty_b, ctx.limit_price_b, ctx.mult_b, ctx.tick_b)
    };

    let res_first = first_conn.place_order(
        http, keys, &ctx.pair, first_is_buy, first_qty, ctx.dry_run,
        first_limit, first_limit > 0.0,
        ctx.leverage, first_mult, ctx.ioc_slippage_pct, first_tick, ts.api_timeout_ms
    ).await;

    let (oid_first, fill_first, fee_first) = match res_first {
        Ok(t) => {
            // Successful order — clear any unhealthy marker
            e_ctx.ws_order_unhealthy_until.remove(&first_label.to_string());
            (t.0, t.1, t.2)
        }
        Err(e) => {
            tracing::error!("[real] LEG_FIRST reject {}/{}: {}", first_label, ctx.pair, e);
            let esl = e.to_string().to_lowercase();
            if esl.contains("insufficient") || esl.contains("balance") || esl.contains("margin") || esl.contains("exceed") {
                crate::engine::risk_manager::mark_balance_low(first_label, e_ctx);
            }
            // Mark WS unhealthy for 60s on timeout/WS failures (auto-recovers via TTL)
            if esl.contains("timeout") || esl.contains("ws ") || esl.contains("websocket") || esl.contains("connect") {
                let unhealthy_until = now + 60_000;
                e_ctx.ws_order_unhealthy_until.insert(first_label.to_string(), unhealthy_until);
                tracing::warn!("[real] 📡 {} WS marked UNHEALTHY for 60s (auto-recovery)", first_label);
            }
            (None, 0.0, 0.0)
        }
    };

    let first_phys_init = if fill_first > 0.0 { Some(first_qty) } else { None };
    let first_max_retries = if fill_first <= 0.0 { 5_i64 } else { ts.fill_verify_retries };

    let ver_first = verify_fill(
        first_conn, http, keys, &ctx.pair, &oid_first,
        fill_first, fee_first, first_phys_init, first_qty, ctx.dry_run,
        VerifyConfig {
            fast_retries: first_max_retries,
            fast_delay_ms: ts.fill_verify_delay_ms as u64,
            slow_retries: 8,
            slow_delay_ms: 500,
            // CRITICAL: Do NOT use position_fallback for IOC entry orders!
            // fetch_positions can find stale positions from previous trades
            // (e.g. failed trims) and falsely confirm a phantom fill.
            position_fallback: false,
            mult: first_mult,
        },
        "real",
    ).await;

    let first_phys = ver_first.phys_volume.unwrap_or(0.0);

    // Guard: if phys > 0 but fill_price = 0, this is a phantom fill (position_fallback found stale position)
    if first_phys > 1e-12 && ver_first.fill_price <= 0.0 {
        tracing::error!(
            "[real] 🚨 PHANTOM FILL {} on {}: phys={:.6} but fill_price=0 — treating as unfilled",
            ctx.pair, first_label, first_phys
        );
        e_ctx.pair_cooldown.insert(ctx.pair.clone(), now + 30_000);
        return true;
    }

    if first_phys <= 1e-12 {
        tracing::info!(
            "[real] ⏭️ {} IOC не заполнен для {} — нет экспозиции, пропускаем ({}ms)",
            first_label, ctx.pair, t0_exec.elapsed().as_millis()
        );
        e_ctx.pair_cooldown.insert(ctx.pair.clone(), now + 10_000);
        return true;
    }

    if sort_by_liquidity {
        tracing::info!(
            "[real] ✅ Первая нога {} заполнена: price={:.6} qty={:.6}. Открываем {}...",
            first_label, ver_first.fill_price, first_phys, second_label
        );
    }

    // ── PHASE 2: More liquid leg (or Leg B for HFT) ─────────────────────────
    let final_qty_second = if ctx.dry_run { 
        if a_is_less_liquid { ctx.qty_b } else { ctx.qty_a } 
    } else { first_phys };

    let (second_conn, second_is_buy, second_limit, second_mult, second_tick) = if a_is_less_liquid {
        (&conn_ensure_b, ctx.is_a_higher, ctx.limit_price_b, ctx.mult_b, ctx.tick_b)
    } else {
        (&conn_ensure_a, !ctx.is_a_higher, ctx.limit_price_a, ctx.mult_a, ctx.tick_a)
    };

    // After first leg is filled, we're already committed/exposed.
    // Use aggressive slippage on second leg: 80% of (spread - BE).
    // This keeps at least 20% of expected profit while maximizing fill probability.
    // A failed second leg costs MORE than slippage (full trim loss + fees).
    let profit_margin = (ctx.sig_spread - ctx.break_even_pct).max(0.0);
    let aggressive_slip = (profit_margin * 0.80)
        .max(ctx.ioc_slippage_pct)  // never less than base slippage
        .min(2.5);                   // safety cap 2.5% (trim loss is always worse)

    tracing::info!(
        "[real] 🎯 Second leg {} slippage: base={:.3}% aggressive={:.3}% (spread={:.3}% BE={:.3}% profit_margin={:.3}%)",
        second_label, ctx.ioc_slippage_pct, aggressive_slip, ctx.sig_spread, ctx.break_even_pct, profit_margin
    );

    let res_second = second_conn.place_order(
        http, keys, &ctx.pair, second_is_buy, final_qty_second, ctx.dry_run,
        second_limit, second_limit > 0.0,
        ctx.leverage, second_mult, aggressive_slip, second_tick, ts.api_timeout_ms
    ).await;

    let (mut oid_second, mut fill_second, mut fee_second) = match res_second {
        Ok(t) => {
            e_ctx.ws_order_unhealthy_until.remove(&second_label.to_string());
            (t.0, t.1, t.2)
        }
        Err(e) => {
            tracing::error!("[real] LEG_SECOND reject {}/{}: {}", second_label, ctx.pair, e);
            let esl = e.to_string().to_lowercase();
            if esl.contains("insufficient") || esl.contains("balance") || esl.contains("margin") || esl.contains("exceed") {
                crate::engine::risk_manager::mark_balance_low(second_label, e_ctx);
            }
            if esl.contains("timeout") || esl.contains("ws ") || esl.contains("websocket") || esl.contains("connect") {
                let unhealthy_until = now + 60_000;
                e_ctx.ws_order_unhealthy_until.insert(second_label.to_string(), unhealthy_until);
                tracing::warn!("[real] 📡 {} WS marked UNHEALTHY for 60s (auto-recovery)", second_label);
            }
            // Cooldown the specific ticker on this exchange (not the whole exchange)
            let ticker_key = format!("{}:{}", second_label, ctx.pair);
            e_ctx.pair_cooldown.insert(ctx.pair.clone(), now + 300_000);
            tracing::error!(
                "[real] 🚫 {} pair cooldown 5min after LEG_SECOND reject ({})",
                ticker_key, ctx.pair
            );
            (None, 0.0, 0.0)
        }
    };

    let mut second_phys_init = if fill_second > 0.0 { Some(final_qty_second) } else { None };
    let second_max_retries = if fill_second <= 0.0 { 5_i64 } else { ts.fill_verify_retries };

    let mut ver_second = verify_fill(
        second_conn, http, keys, &ctx.pair, &oid_second,
        fill_second, fee_second, second_phys_init, final_qty_second, ctx.dry_run,
        VerifyConfig {
            fast_retries: second_max_retries,
            fast_delay_ms: ts.fill_verify_delay_ms as u64,
            slow_retries: 8,
            slow_delay_ms: 500,
            position_fallback: false,
            mult: second_mult,
        },
        "real",
    ).await;

    // ── RETRY SECOND LEG: If IOC didn't fill, retry once with maximum slippage ──
    // A failed second leg means full ASYMMETRIC_CLOSE (trim loss), which is ALWAYS
    // worse than any slippage. So on retry, use the full profit margin.
    if ver_second.phys_volume.unwrap_or(0.0) <= 1e-12 && !ctx.dry_run && oid_second.is_some() {
        let retry_slip = profit_margin.max(ctx.ioc_slippage_pct).min(3.0);
        tracing::warn!(
            "[real] 🔄 RETRY second leg {} for {} with max slippage {:.3}% (was {:.3}%)",
            second_label, ctx.pair, retry_slip, aggressive_slip
        );

        let res_retry = second_conn.place_order(
            http, keys, &ctx.pair, second_is_buy, final_qty_second, ctx.dry_run,
            second_limit, second_limit > 0.0,
            ctx.leverage, second_mult, retry_slip, second_tick, ts.api_timeout_ms
        ).await;

        match res_retry {
            Ok(t) => {
                oid_second = t.0;
                fill_second = t.1;
                fee_second = t.2;
                second_phys_init = if fill_second > 0.0 { Some(final_qty_second) } else { None };

                ver_second = verify_fill(
                    second_conn, http, keys, &ctx.pair, &oid_second,
                    fill_second, fee_second, second_phys_init, final_qty_second, ctx.dry_run,
                    VerifyConfig {
                        fast_retries: 5,
                        fast_delay_ms: ts.fill_verify_delay_ms as u64,
                        slow_retries: 5,
                        slow_delay_ms: 500,
                        position_fallback: false,
                        mult: second_mult,
                    },
                    "real",
                ).await;

                if ver_second.phys_volume.unwrap_or(0.0) > 1e-12 {
                    tracing::info!("[real] ✅ RETRY SUCCESS: second leg {} filled on retry!", second_label);
                } else {
                    tracing::error!("[real] ❌ RETRY FAILED: second leg {} still unfilled after retry", second_label);
                }
            }
            Err(e) => {
                tracing::error!("[real] ❌ RETRY second leg reject {}/{}: {}", second_label, ctx.pair, e);
            }
        }
    }

    let total_exec_ms = t0_exec.elapsed().as_millis();

    if total_exec_ms > 800 {
        let mut strikes_a = e_ctx.high_latency_strikes.entry(ctx.exa.clone()).or_insert(0);
        *strikes_a += 1;
        if *strikes_a >= 3 {
            e_ctx.exchange_cooldown.insert(ctx.exa.clone(), now + 60_000);
            tracing::error!("[real] 🐢 LATENCY ALERT: {} RTT >800ms. Cooldown 1m.", ctx.exa);
            *strikes_a = 0;
        }

        let mut strikes_b = e_ctx.high_latency_strikes.entry(ctx.exb.clone()).or_insert(0);
        *strikes_b += 1;
        if *strikes_b >= 3 {
            e_ctx.exchange_cooldown.insert(ctx.exb.clone(), now + 60_000);
            tracing::error!("[real] 🐢 LATENCY ALERT: {} RTT >800ms. Cooldown 1m.", ctx.exb);
            *strikes_b = 0;
        }
    } else {
        e_ctx.high_latency_strikes.insert(ctx.exa.clone(), 0);
        e_ctx.high_latency_strikes.insert(ctx.exb.clone(), 0);
    }

    let (oid_a, oid_b);
    let (mut fill_a, fee_a_val, actual_vol_phys_a);
    let (mut fill_b, fee_b_val, actual_vol_phys_b);
    if a_is_less_liquid {
        oid_a = oid_first;
        fill_a = ver_first.fill_price;
        fee_a_val = ver_first.fee;
        actual_vol_phys_a = ver_first.phys_volume;
        oid_b = oid_second;
        fill_b = ver_second.fill_price;
        fee_b_val = ver_second.fee;
        actual_vol_phys_b = ver_second.phys_volume;
    } else {
        oid_a = oid_second;
        fill_a = ver_second.fill_price;
        fee_a_val = ver_second.fee;
        actual_vol_phys_a = ver_second.phys_volume;
        oid_b = oid_first;
        fill_b = ver_first.fill_price;
        fee_b_val = ver_first.fee;
        actual_vol_phys_b = ver_first.phys_volume;
    }

    let mut phys_a = actual_vol_phys_a.unwrap_or(0.0);
    let mut phys_b = actual_vol_phys_b.unwrap_or(0.0);

    if ctx.dry_run {
        fill_a = ctx.sig_entry_a;
        fill_b = ctx.sig_entry_b;
    }

    let a_filled = oid_a.is_some() && phys_a > 0.0;
    let b_filled = oid_b.is_some() && phys_b > 0.0;

    tracing::info!("[real] RESULT {} A={} B={} phys_a={:.6} phys_b={:.6} fill_a={:.6} fill_b={:.6} latency={}ms",
        ctx.pair, a_filled, b_filled, phys_a, phys_b, fill_a, fill_b, total_exec_ms);

    if !a_filled && !b_filled {
        tracing::info!("[real] Both IOC unfilled {} — no exposure ({}ms)", ctx.pair, total_exec_ms);
        e_ctx.pair_cooldown.insert(ctx.pair.clone(), now + 10_000);
        return true;
    }

    let mv = match_volumes(phys_a, phys_b, ctx.mult_a, ctx.mult_b);
    let matched_phys = mv.matched_phys;
    let delta_a = mv.delta_a;
    let delta_b = mv.delta_b;

    // ── DELTA TRIMMING (Phase 3) ──
    let (trimming_fee, trimming_loss, all_trimmed) = if !ctx.dry_run {
        trim_asymmetric_legs(
            http, keys, &ctx.pair, &ctx.exa, &ctx.exb, ctx.is_a_higher,
            delta_a, delta_b, ctx.mult_a, ctx.mult_b, fill_a, fill_b, ts.api_timeout_ms, e_ctx
        ).await
    } else {
        (0.0, 0.0, true)
    };

    // ── TRADE BOOKING (Phase 4) ──
    let repo = TradeRepository::new(db.clone());
    
    if matched_phys < 1e-12 {
        // ── PER-TICKER-PER-EXCHANGE COOLDOWN (replaces old global kill-switch) ──
        // Instead of blocking the ENTIRE engine after 3 trims, we ban the specific
        // ticker on the specific exchange that failed to fill.
        // This allows other pairs and exchanges to keep trading.
        let failed_exchange = if a_filled { second_label } else { first_label };
        let ticker_ban_key = format!("{}:{}", failed_exchange, ctx.pair);
        {
            let now_ban = crate::engine::utils::now_ms();
            let mut entry = e_ctx.asymmetric_fail_counter.entry(ticker_ban_key.clone()).or_insert((0, now_ban));
            let (count, first_ts) = entry.value_mut();
            // Reset counter if first failure was > 1 hour ago
            if now_ban - *first_ts > 3_600_000 {
                *count = 0;
                *first_ts = now_ban;
            }
            *count += 1;
            if *count >= 3 {
                let ban_ms = 300_000_i64; // 5 minutes
                e_ctx.pair_cooldown.insert(ctx.pair.clone(), now_ban + ban_ms);
                tracing::error!(
                    "[real] 🚫 TICKER BANNED {}: {} asymmetric failures in 1h on this exchange. Cooldown {}s.",
                    ticker_ban_key, count, ban_ms / 1000
                );
                *count = 0;
                *first_ts = now_ban;
            }
        }

        // Also track global consecutive_trims for the safety-net kill-switch (raised to 10)
        let trims = e_ctx.consecutive_trims.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if trims >= 10 {
            e_ctx.is_killed.store(true, std::sync::atomic::Ordering::Relaxed);
            tracing::error!("[real] 💀 KILL-SWITCH TRIGGERED: {} consecutive FULL TRIMS (total asymmetry). Suspending new entries. Use Resume button on frontend to reset.", trims);
            if let Some(ref pdb) = e_ctx.profiles_db {
                let pdb = pdb.clone();
                tokio::spawn(async move {
                    let _ = pdb.call(move |conn| {
                        conn.execute(
                            "INSERT INTO server_cache(key,value,expires_at) VALUES('kill_switch_active','1',0) \
                             ON CONFLICT(key) DO UPDATE SET value='1'",
                            [],
                        )?;
                        Ok(())
                    }).await;
                });
            }
        }

        if !all_trimmed && !ctx.dry_run {
            // ── TRIM FAILED after 10 retries (30s) ──
            // Record as ASYMMETRIC_CLOSE anyway. Position is an orphan on exchange,
            // but a broken one-legged OPEN trade in DB is worse (exit_manager can't
            // close it, it spams logs and blocks UI).
            // The orphan position will be caught by position_sync's exchange scan.
            let filled_ex = if a_filled { &ctx.exa } else { &ctx.exb };
            tracing::error!(
                "[real] 💀 CRITICAL: TRIM FAILED for {} after 10 retries — orphan position on {}! Recording ASYMMETRIC_CLOSE.",
                ctx.pair, filled_ex
            );
            // Fall through to ASYMMETRIC_CLOSE below
        }

        // Trim succeeded — record as ASYMMETRIC_CLOSE (position is already closed)
        let total_fee = fee_a_val + fee_b_val + trimming_fee;
        tracing::error!("[real] ❌ FULL ASYMMETRY DUMPED {} on {}. Trade canceled. Trimming Loss: {:.4}, Trimming Fee: {:.4}", ctx.pair, failed_exchange, trimming_loss, trimming_fee);
        e_ctx.pair_cooldown.insert(ctx.pair.clone(), now + ts.asymmetric_cooldown_ms);

        let (ent_a, ent_b) = if a_filled { (fill_a, 0.0) } else { (0.0, fill_b) };
        let fv_usd = if a_filled { delta_a * fill_a } else { delta_b * fill_b };
        let gross_pnl = -trimming_loss;
        let net_pnl = gross_pnl - total_fee;

        let pnl_a_val = if a_filled { gross_pnl } else { 0.0 };
        let pnl_b_val = if !a_filled { gross_pnl } else { 0.0 };

        repo.append_asymmetric_close(
            ctx.pair.clone(), ctx.exa.clone(), ctx.exb.clone(), ctx.sig_direction.clone(),
            fv_usd, ent_a, ent_b, ctx.sig_spread, crate::engine::utils::now_ms(),
            pnl_a_val, pnl_b_val, ctx.profile_id.clone(),
            total_fee, 0.0, net_pnl, gross_pnl, 0.0, 0.0,
        ).await;
        return true;
    }

    e_ctx.consecutive_failures.store(0, std::sync::atomic::Ordering::Relaxed);
    e_ctx.consecutive_trims.store(0, std::sync::atomic::Ordering::Relaxed);
    
    let final_vol_usd = matched_phys * fill_a;
    crate::engine::risk_manager::deduct_cached_balance(&ctx.exa, final_vol_usd, ctx.leverage, e_ctx);
    crate::engine::risk_manager::deduct_cached_balance(&ctx.exb, final_vol_usd, ctx.leverage, e_ctx);

    e_ctx.pair_cooldown.insert(ctx.pair.clone(), now + ts.pair_cooldown_ms);

    let actual_entry_spread = compute_entry_spread(fill_a, fill_b, ctx.is_a_higher, ctx.sig_spread);

    // ── POST-FILL SPREAD GUARD (sequential) ──
    // Reject only if slippage caused realized spread to drop below break-even fees (negative PnL).
    let min_acceptable = ctx.break_even_pct;
    if a_filled && b_filled && actual_entry_spread < min_acceptable && !ctx.dry_run {
        tracing::warn!(
            "[real] ⛔ POST-FILL SPREAD GUARD: {} realized spread {:.4}% < break_even {:.4}% (signal was {:.4}%). Closing BOTH positions.",
            ctx.pair, actual_entry_spread, min_acceptable, ctx.sig_spread
        );
        let conn_a_trim = crate::engine::exchanges::get_connector(&ctx.exa).unwrap();
        let conn_b_trim = crate::engine::exchanges::get_connector(&ctx.exb).unwrap();
        let (tr_a, tr_b) = tokio::join!(
            conn_a_trim.trim_position(
                http, keys, &ctx.pair, ctx.is_a_higher, phys_a, false,
                ctx.mult_a, ctx.tick_a, fill_a, ts.api_timeout_ms,
            ),
            conn_b_trim.trim_position(
                http, keys, &ctx.pair, !ctx.is_a_higher, phys_b, false,
                ctx.mult_b, ctx.tick_b, fill_b, ts.api_timeout_ms,
            )
        );
        if let Err(e) = tr_a { tracing::error!("[real] Failed to close spread-guard leg A {}: {}", ctx.exa, e); }
        if let Err(e) = tr_b { tracing::error!("[real] Failed to close spread-guard leg B {}: {}", ctx.exb, e); }

        let total_fee = fee_a_val + fee_b_val;
        e_ctx.pair_cooldown.insert(ctx.pair.clone(), now + ts.asymmetric_cooldown_ms);
        let repo = TradeRepository::new(db.clone());
        repo.append_asymmetric_close(
            ctx.pair.clone(), ctx.exa.clone(), ctx.exb.clone(), ctx.sig_direction.clone(),
            matched_phys * fill_a, fill_a, fill_b, ctx.sig_spread, crate::engine::utils::now_ms(),
            0.0, 0.0, ctx.profile_id.clone(),
            total_fee, 0.0, -total_fee, 0.0, 0.0, 0.0,
        ).await;
        return true;
    }

    let entry_comm = fee_a_val + fee_b_val + trimming_fee + trimming_loss;
    let slip_a = if fill_a > 0.0 {
        if ctx.is_a_higher { ((ctx.sig_entry_a - fill_a) * matched_phys).max(0.0) } else { ((fill_a - ctx.sig_entry_a) * matched_phys).max(0.0) }
    } else { 0.0 };
    let slip_b = if fill_b > 0.0 {
        if !ctx.is_a_higher { ((ctx.sig_entry_b - fill_b) * matched_phys).max(0.0) } else { ((fill_b - ctx.sig_entry_b) * matched_phys).max(0.0) }
    } else { 0.0 };

    let entry_slip = slip_a + slip_b;
    // If trim failed (partial asymmetry), store ACTUAL exchange-side quantities
    // instead of matched_phys. This ensures DB always reflects what's on the exchange.
    // Exit_manager/position_sync will then close the correct quantity.
    let (contracts_a_db, contracts_b_db) = if !all_trimmed && !ctx.dry_run {
        tracing::warn!(
            "[real] ⚠️ PARTIAL TRIM FAILED for {} — storing actual exchange quantities (a={:.6} b={:.6}) instead of matched ({:.6})",
            ctx.pair, phys_a, phys_b, matched_phys
        );
        (phys_a / ctx.mult_a, phys_b / ctx.mult_b)
    } else {
        (matched_phys / ctx.mult_a, matched_phys / ctx.mult_b)
    };

    if fill_a > 0.0 && ctx.sig_entry_a > 0.0 {
        let ratio_a = fill_a / ctx.sig_entry_a;
        if ratio_a > 2.0 || ratio_a < 0.5 {
            tracing::error!("[real] 🚨 PRICE SANITY FAIL {}/{}: fill_a={:.8} entry_a={:.8} ratio={:.2}. Using L1 fallback!",
                ctx.pair, ctx.exa, fill_a, ctx.sig_entry_a, ratio_a);
            fill_a = ctx.sig_entry_a;
        }
    }
    if fill_b > 0.0 && ctx.sig_entry_b > 0.0 {
        let ratio_b = fill_b / ctx.sig_entry_b;
        if ratio_b > 2.0 || ratio_b < 0.5 {
            tracing::error!("[real] 🚨 PRICE SANITY FAIL {}/{}: fill_b={:.8} entry_b={:.8} ratio={:.2}. Using L1 fallback!",
                ctx.pair, ctx.exb, fill_b, ctx.sig_entry_b, ratio_b);
            fill_b = ctx.sig_entry_b;
        }
    }

    // 🛡️ CROSS-LEG PRICE SANITY: Detect anomalies where fill_a/fill_b ratio
    // diverges wildly from signal ratio (e.g. 900%+ spread from unit mismatch).
    if fill_a > 0.0 && fill_b > 0.0 && ctx.sig_entry_a > 0.0 && ctx.sig_entry_b > 0.0 {
        let fill_ratio = fill_a / fill_b;
        let sig_ratio = ctx.sig_entry_a / ctx.sig_entry_b;
        let cross_ratio = fill_ratio / sig_ratio;
        if cross_ratio > 5.0 || cross_ratio < 0.2 {
            tracing::error!(
                "[real] 🚨 CROSS-LEG SANITY FAIL {}: fill_a/fill_b={:.6} sig_a/sig_b={:.6} cross_ratio={:.2}. Aborting trade + trimming.",
                ctx.pair, fill_ratio, sig_ratio, cross_ratio
            );
            // Force matched_phys to 0 so the trade is fully trimmed below
            phys_a = 0.0;
            phys_b = 0.0;
        }
    }

    tracing::info!("[real] [EXCHANGE_VERIFIED] ✅ OPEN {} phys={:.6} (Trimming: -${:.4}) A_price={:.6} B_price={:.6} fees={:.4} slip={:.4} actual_sp={:.4}%",
        ctx.pair, matched_phys, trimming_loss, fill_a, fill_b, entry_comm, entry_slip, actual_entry_spread);

    repo.insert_new_entry(
        ctx.pair.clone(), ctx.exa.clone(), ctx.exb.clone(), ctx.sig_direction.clone(),
        final_vol_usd, fill_a, fill_b, actual_entry_spread, crate::engine::utils::now_ms(),
        ctx.profile_id.clone(), entry_comm, entry_slip,
        oid_a.unwrap_or_default(), oid_b.unwrap_or_default(),
        contracts_a_db, contracts_b_db, ctx.sig_structural_spread,
    ).await;
    
    true
}

// ═══════════════════════════════════════════════════════════════════════════════
// PARALLEL ENTRY — both legs fire simultaneously via tokio::join!
// ═══════════════════════════════════════════════════════════════════════════════

pub async fn execute_parallel_entry(
    ctx: SequentialEntryContext,
    http: &Client,
    keys: &Arc<ExchangeKeys>,
    db: &Connection,
    e_ctx: &Arc<EngineContext>,
    ts: &TechnicalSettings,
) -> bool {
    let now = crate::engine::utils::now_ms();
    let conn_a = crate::engine::exchanges::get_connector(&ctx.exa).unwrap();
    let conn_b = crate::engine::exchanges::get_connector(&ctx.exb).unwrap();

    let t0_exec = std::time::Instant::now();

    // A is higher → sell A (hit bid), buy B (hit ask)
    let is_buy_a = !ctx.is_a_higher;
    let is_buy_b = ctx.is_a_higher;

    tracing::info!(
        "[real] ⚡ PARALLEL execution: {} + {} for {} (qty_a={:.6} qty_b={:.6}) is_a_higher={} buy_a={} buy_b={}",
        ctx.exa, ctx.exb, ctx.pair, ctx.qty_a, ctx.qty_b, ctx.is_a_higher, is_buy_a, is_buy_b
    );

    // ── PHASE 1: Fire BOTH legs simultaneously ──────────────────────────────
    let (res_a, res_b) = tokio::join!(
        conn_a.place_order(
            http, keys, &ctx.pair, is_buy_a, ctx.qty_a, ctx.dry_run,
            ctx.limit_price_a, ctx.limit_price_a > 0.0,
            ctx.leverage, ctx.mult_a, ctx.ioc_slippage_pct, ctx.tick_a, ts.api_timeout_ms
        ),
        conn_b.place_order(
            http, keys, &ctx.pair, is_buy_b, ctx.qty_b, ctx.dry_run,
            ctx.limit_price_b, ctx.limit_price_b > 0.0,
            ctx.leverage, ctx.mult_b, ctx.ioc_slippage_pct, ctx.tick_b, ts.api_timeout_ms
        )
    );

    let fire_ms = t0_exec.elapsed().as_millis();
    tracing::info!("[real] ⚡ PARALLEL orders sent in {}ms", fire_ms);

    // Parse results
    let (oid_a, fill_a_inline, fee_a_inline) = match res_a {
        Ok(t) => {
            e_ctx.ws_order_unhealthy_until.remove(&ctx.exa);
            t
        }
        Err(e) => {
            tracing::error!("[real] LEG_A reject {}/{}: {}", ctx.exa, ctx.pair, e);
            let esl = e.to_string().to_lowercase();
            if esl.contains("insufficient") || esl.contains("balance") || esl.contains("margin") || esl.contains("exceed") {
                crate::engine::risk_manager::mark_balance_low(&ctx.exa, e_ctx);
            }
            if esl.contains("timeout") || esl.contains("ws ") || esl.contains("websocket") || esl.contains("connect") {
                e_ctx.ws_order_unhealthy_until.insert(ctx.exa.clone(), now + 60_000);
                tracing::warn!("[real] 📡 {} WS marked UNHEALTHY for 60s", ctx.exa);
            }
            (None, 0.0, 0.0)
        }
    };

    let (oid_b, fill_b_inline, fee_b_inline) = match res_b {
        Ok(t) => {
            e_ctx.ws_order_unhealthy_until.remove(&ctx.exb);
            t
        }
        Err(e) => {
            tracing::error!("[real] LEG_B reject {}/{}: {}", ctx.exb, ctx.pair, e);
            let esl = e.to_string().to_lowercase();
            if esl.contains("insufficient") || esl.contains("balance") || esl.contains("margin") || esl.contains("exceed") {
                crate::engine::risk_manager::mark_balance_low(&ctx.exb, e_ctx);
            }
            if esl.contains("timeout") || esl.contains("ws ") || esl.contains("websocket") || esl.contains("connect") {
                e_ctx.ws_order_unhealthy_until.insert(ctx.exb.clone(), now + 60_000);
                tracing::warn!("[real] 📡 {} WS marked UNHEALTHY for 60s", ctx.exb);
            }
            (None, 0.0, 0.0)
        }
    };

    // ── PHASE 2: Verify fills (in parallel too) ─────────────────────────────
    let phys_a_init = if fill_a_inline > 0.0 { Some(ctx.qty_a) } else { None };
    let phys_b_init = if fill_b_inline > 0.0 { Some(ctx.qty_b) } else { None };
    let retries_a = if fill_a_inline <= 0.0 { 5_i64 } else { ts.fill_verify_retries };
    let retries_b = if fill_b_inline <= 0.0 { 5_i64 } else { ts.fill_verify_retries };

    let (ver_a, ver_b) = tokio::join!(
        verify_fill(
            &conn_a, http, keys, &ctx.pair, &oid_a,
            fill_a_inline, fee_a_inline, phys_a_init, ctx.qty_a, ctx.dry_run,
            VerifyConfig {
                fast_retries: retries_a,
                fast_delay_ms: ts.fill_verify_delay_ms as u64,
                slow_retries: 2,
                slow_delay_ms: 200,
                position_fallback: false,
                mult: ctx.mult_a,
            },
            "real",
        ),
        verify_fill(
            &conn_b, http, keys, &ctx.pair, &oid_b,
            fill_b_inline, fee_b_inline, phys_b_init, ctx.qty_b, ctx.dry_run,
            VerifyConfig {
                fast_retries: retries_b,
                fast_delay_ms: ts.fill_verify_delay_ms as u64,
                slow_retries: 2,
                slow_delay_ms: 200,
                position_fallback: false,
                mult: ctx.mult_b,
            },
            "real",
        )
    );

    let mut phys_a = ver_a.phys_volume.unwrap_or(0.0);
    let mut phys_b = ver_b.phys_volume.unwrap_or(0.0);
    let mut fill_a = ver_a.fill_price;
    let mut fill_b = ver_b.fill_price;
    let fee_a_val = ver_a.fee;
    let fee_b_val = ver_b.fee;

    // Guard phantom fills
    if phys_a > 1e-12 && fill_a <= 0.0 {
        tracing::error!("[real] 🚨 PHANTOM FILL {} on {}: phys={:.6} but fill_price=0", ctx.pair, ctx.exa, phys_a);
        phys_a = 0.0;
    }
    if phys_b > 1e-12 && fill_b <= 0.0 {
        tracing::error!("[real] 🚨 PHANTOM FILL {} on {}: phys={:.6} but fill_price=0", ctx.pair, ctx.exb, phys_b);
        phys_b = 0.0;
    }

    if ctx.dry_run {
        fill_a = ctx.sig_entry_a;
        fill_b = ctx.sig_entry_b;
    }

    let total_exec_ms = t0_exec.elapsed().as_millis();

    let mut a_filled = oid_a.is_some() && phys_a > 0.0;
    let mut b_filled = oid_b.is_some() && phys_b > 0.0;

    tracing::info!("[real] RESULT {} A={} B={} phys_a={:.6} phys_b={:.6} fill_a={:.6} fill_b={:.6} latency={}ms",
        ctx.pair, a_filled, b_filled, phys_a, phys_b, fill_a, fill_b, total_exec_ms);

    // ── RETRY UNFILLED LEG (parallel entry recovery) ────────────────────────
    // If one leg filled and the other didn't, retry the failed leg with
    // aggressive slippage before falling through to expensive ASYMMETRIC_CLOSE.
    // This mirrors sequential entry's retry logic (L706-753).
    if (a_filled && !b_filled) || (!a_filled && b_filled) {
        if !ctx.dry_run {
            let profit_margin = (ctx.sig_spread - ctx.break_even_pct).max(0.0);
            let retry_slip = profit_margin.max(ctx.ioc_slippage_pct).min(3.0);

            let (retry_conn, retry_is_buy, retry_qty, retry_limit, retry_mult, retry_tick, retry_label) =
                if a_filled && !b_filled {
                    (&conn_b, is_buy_b, ctx.qty_b, ctx.limit_price_b, ctx.mult_b, ctx.tick_b, ctx.exb.as_str())
                } else {
                    (&conn_a, is_buy_a, ctx.qty_a, ctx.limit_price_a, ctx.mult_a, ctx.tick_a, ctx.exa.as_str())
                };

            tracing::warn!(
                "[real] 🔄 PARALLEL RETRY {} leg {} for {} with aggressive slippage {:.3}% (was {:.3}%)",
                if a_filled { "B" } else { "A" }, retry_label, ctx.pair, retry_slip, ctx.ioc_slippage_pct
            );

            let res_retry = retry_conn.place_order(
                http, keys, &ctx.pair, retry_is_buy, retry_qty, false,
                retry_limit, retry_limit > 0.0,
                ctx.leverage, retry_mult, retry_slip, retry_tick, ts.api_timeout_ms
            ).await;

            match res_retry {
                Ok((retry_oid, retry_fill, retry_fee)) => {
                    let retry_phys_init = if retry_fill > 0.0 { Some(retry_qty) } else { None };
                    let ver_retry = verify_fill(
                        retry_conn, http, keys, &ctx.pair, &retry_oid,
                        retry_fill, retry_fee, retry_phys_init, retry_qty, false,
                        VerifyConfig {
                            fast_retries: 5,
                            fast_delay_ms: ts.fill_verify_delay_ms as u64,
                            slow_retries: 5,
                            slow_delay_ms: 500,
                            position_fallback: false,
                            mult: retry_mult,
                        },
                        "real",
                    ).await;

                    let retry_phys = ver_retry.phys_volume.unwrap_or(0.0);
                    if retry_phys > 1e-12 && ver_retry.fill_price > 0.0 {
                        // ── SPREAD GUARD: verify retry fill doesn't create a losing entry ──
                        let (candidate_fill_a, candidate_fill_b) = if a_filled && !b_filled {
                            (fill_a, ver_retry.fill_price)
                        } else {
                            (ver_retry.fill_price, fill_b)
                        };

                        // A is the higher-priced exchange (SELL side), B is lower (BUY side)
                        // For profit: sell_price > buy_price, i.e. spread > 0
                        let realized_spread = if ctx.is_a_higher {
                            (candidate_fill_a - candidate_fill_b) / candidate_fill_b
                        } else {
                            (candidate_fill_b - candidate_fill_a) / candidate_fill_a
                        };

                        let min_acceptable_spread = ctx.break_even_pct / 100.0;
                        if realized_spread < min_acceptable_spread {
                            tracing::warn!(
                                "[real] ⛔ PARALLEL RETRY REJECTED: {} filled but realized spread {:.4}% < break_even {:.4}% (fill_a={:.6} fill_b={:.6}). Closing retry position and proceeding to trim.",
                                retry_label, realized_spread * 100.0, ctx.break_even_pct, candidate_fill_a, candidate_fill_b
                            );
                            // Close the retry position immediately to avoid orphan
                            let close_is_buy = !retry_is_buy; // reverse direction to close
                            if let Err(ce) = retry_conn.trim_position(
                                http, keys, &ctx.pair, close_is_buy, retry_phys, false,
                                retry_mult, retry_tick, ver_retry.fill_price, ts.api_timeout_ms,
                            ).await {
                                tracing::error!("[real] ⚠️ Failed to close rejected retry position on {}: {}", retry_label, ce);
                            } else {
                                tracing::info!("[real] ✅ Rejected retry position on {} closed successfully", retry_label);
                            }
                        } else {
                            tracing::info!("[real] ✅ PARALLEL RETRY SUCCESS: {} filled on retry! price={:.6} qty={:.6} realized_spread={:.4}%",
                                retry_label, ver_retry.fill_price, retry_phys, realized_spread * 100.0);
                            if a_filled && !b_filled {
                                phys_b = retry_phys;
                                fill_b = ver_retry.fill_price;
                                b_filled = true;
                            } else {
                                phys_a = retry_phys;
                                fill_a = ver_retry.fill_price;
                                a_filled = true;
                            }
                        }
                    } else {
                        tracing::error!("[real] ❌ PARALLEL RETRY FAILED: {} still unfilled after retry", retry_label);
                    }
                }
                Err(e) => {
                    tracing::error!("[real] ❌ PARALLEL RETRY reject {}/{}: {}", retry_label, ctx.pair, e);
                }
            }
        }
    }

    // ── From here: identical to sequential_entry post-fill logic ────────────

    if !a_filled && !b_filled {
        tracing::info!("[real] Both IOC unfilled {} — no exposure ({}ms)", ctx.pair, total_exec_ms);
        e_ctx.pair_cooldown.insert(ctx.pair.clone(), now + 10_000);
        return true;
    }

    let mv = match_volumes(phys_a, phys_b, ctx.mult_a, ctx.mult_b);
    let matched_phys = mv.matched_phys;
    let delta_a = mv.delta_a;
    let delta_b = mv.delta_b;

    // ── DELTA TRIMMING (Phase 3) ──
    let (trimming_fee, trimming_loss, all_trimmed) = if !ctx.dry_run {
        trim_asymmetric_legs(
            http, keys, &ctx.pair, &ctx.exa, &ctx.exb, ctx.is_a_higher,
            delta_a, delta_b, ctx.mult_a, ctx.mult_b, fill_a, fill_b, ts.api_timeout_ms, e_ctx
        ).await
    } else {
        (0.0, 0.0, true)
    };

    // ── TRADE BOOKING (Phase 4) ──
    let repo = TradeRepository::new(db.clone());

    if matched_phys < 1e-12 {
        let failed_exchange = if a_filled { ctx.exb.as_str() } else { ctx.exa.as_str() };
        let ticker_ban_key = format!("{}:{}", failed_exchange, ctx.pair);
        {
            let now_ban = crate::engine::utils::now_ms();
            let mut entry = e_ctx.asymmetric_fail_counter.entry(ticker_ban_key.clone()).or_insert((0, now_ban));
            let (count, first_ts) = entry.value_mut();
            if now_ban - *first_ts > 3_600_000 {
                *count = 0;
                *first_ts = now_ban;
            }
            *count += 1;
            if *count >= 3 {
                let ban_ms = 300_000_i64;
                e_ctx.pair_cooldown.insert(ctx.pair.clone(), now_ban + ban_ms);
                tracing::error!(
                    "[real] 🚫 TICKER BANNED {}: {} asymmetric failures in 1h. Cooldown {}s.",
                    ticker_ban_key, count, ban_ms / 1000
                );
                *count = 0;
                *first_ts = now_ban;
            }
        }

        let trims = e_ctx.consecutive_trims.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if trims >= 10 {
            e_ctx.is_killed.store(true, std::sync::atomic::Ordering::Relaxed);
            tracing::error!("[real] 💀 KILL-SWITCH TRIGGERED: {} consecutive FULL TRIMS. Suspending new entries.", trims);
            if let Some(ref pdb) = e_ctx.profiles_db {
                let pdb = pdb.clone();
                tokio::spawn(async move {
                    let _ = pdb.call(move |conn| {
                        conn.execute(
                            "INSERT INTO server_cache(key,value,expires_at) VALUES('kill_switch_active','1',0) \
                             ON CONFLICT(key) DO UPDATE SET value='1'",
                            [],
                        )?;
                        Ok(())
                    }).await;
                });
            }
        }

        if !all_trimmed && !ctx.dry_run {
            let filled_ex = if a_filled { &ctx.exa } else { &ctx.exb };
            tracing::error!(
                "[real] 💀 CRITICAL: TRIM FAILED for {} — orphan position on {}! Recording ASYMMETRIC_CLOSE.",
                ctx.pair, filled_ex
            );
        }

        let total_fee = fee_a_val + fee_b_val + trimming_fee;
        tracing::error!("[real] ❌ FULL ASYMMETRY DUMPED {} on {}. Trade canceled. Trimming Loss: {:.4}, Trimming Fee: {:.4}", ctx.pair, failed_exchange, trimming_loss, trimming_fee);
        e_ctx.pair_cooldown.insert(ctx.pair.clone(), now + ts.asymmetric_cooldown_ms);

        let (ent_a, ent_b) = if a_filled { (fill_a, 0.0) } else { (0.0, fill_b) };
        let fv_usd = if a_filled { delta_a * fill_a } else { delta_b * fill_b };
        let gross_pnl = -trimming_loss;
        let net_pnl = gross_pnl - total_fee;

        let pnl_a_val = if a_filled { gross_pnl } else { 0.0 };
        let pnl_b_val = if !a_filled { gross_pnl } else { 0.0 };

        repo.append_asymmetric_close(
            ctx.pair.clone(), ctx.exa.clone(), ctx.exb.clone(), ctx.sig_direction.clone(),
            fv_usd, ent_a, ent_b, ctx.sig_spread, crate::engine::utils::now_ms(),
            pnl_a_val, pnl_b_val, ctx.profile_id.clone(),
            total_fee, 0.0, net_pnl, gross_pnl, 0.0, 0.0,
        ).await;
        return true;
    }

    e_ctx.consecutive_failures.store(0, std::sync::atomic::Ordering::Relaxed);
    e_ctx.consecutive_trims.store(0, std::sync::atomic::Ordering::Relaxed);

    let final_vol_usd = matched_phys * fill_a;
    crate::engine::risk_manager::deduct_cached_balance(&ctx.exa, final_vol_usd, ctx.leverage, e_ctx);
    crate::engine::risk_manager::deduct_cached_balance(&ctx.exb, final_vol_usd, ctx.leverage, e_ctx);

    e_ctx.pair_cooldown.insert(ctx.pair.clone(), now + ts.pair_cooldown_ms);

    let actual_entry_spread = compute_entry_spread(fill_a, fill_b, ctx.is_a_higher, ctx.sig_spread);

    // ── POST-FILL SPREAD GUARD: reject if slippage ate the profit margin below break-even ──
    let min_acceptable = ctx.break_even_pct;
    if a_filled && b_filled && actual_entry_spread < min_acceptable && !ctx.dry_run {
        tracing::warn!(
            "[real] ⛔ POST-FILL SPREAD GUARD: {} realized spread {:.4}% < break_even {:.4}% (signal was {:.4}%). Closing BOTH positions.",
            ctx.pair, actual_entry_spread, min_acceptable, ctx.sig_spread
        );

        // Close both legs immediately
        // Note: is_buy_to_close is opposite of entry: Leg A entry was !is_a_higher so close is is_a_higher. Leg B entry was is_a_higher so close is !is_a_higher.
        let conn_a_trim = crate::engine::exchanges::get_connector(&ctx.exa).unwrap();
        let conn_b_trim = crate::engine::exchanges::get_connector(&ctx.exb).unwrap();
        let (tr_a, tr_b) = tokio::join!(
            conn_a_trim.trim_position(
                http, keys, &ctx.pair, ctx.is_a_higher, phys_a, false,
                ctx.mult_a, ctx.tick_a, fill_a, ts.api_timeout_ms,
            ),
            conn_b_trim.trim_position(
                http, keys, &ctx.pair, !ctx.is_a_higher, phys_b, false,
                ctx.mult_b, ctx.tick_b, fill_b, ts.api_timeout_ms,
            )
        );
        if let Err(e) = tr_a { tracing::error!("[real] Failed to close spread-guard leg A {}: {}", ctx.exa, e); }
        if let Err(e) = tr_b { tracing::error!("[real] Failed to close spread-guard leg B {}: {}", ctx.exb, e); }

        let total_fee = fee_a_val + fee_b_val;
        e_ctx.pair_cooldown.insert(ctx.pair.clone(), now + ts.asymmetric_cooldown_ms);

        repo.append_asymmetric_close(
            ctx.pair.clone(), ctx.exa.clone(), ctx.exb.clone(), ctx.sig_direction.clone(),
            matched_phys * fill_a, fill_a, fill_b, ctx.sig_spread, crate::engine::utils::now_ms(),
            0.0, 0.0, ctx.profile_id.clone(),
            total_fee, 0.0, -total_fee, 0.0, 0.0, 0.0,
        ).await;
        tracing::info!("[real] ⛔ Both legs closed, trade rejected. Slippage cost: fees only ({:.4})", total_fee);
        return true;
    }

    let entry_comm = fee_a_val + fee_b_val + trimming_fee + trimming_loss;
    let slip_a = if fill_a > 0.0 {
        if ctx.is_a_higher { ((ctx.sig_entry_a - fill_a) * matched_phys).max(0.0) } else { ((fill_a - ctx.sig_entry_a) * matched_phys).max(0.0) }
    } else { 0.0 };
    let slip_b = if fill_b > 0.0 {
        if !ctx.is_a_higher { ((ctx.sig_entry_b - fill_b) * matched_phys).max(0.0) } else { ((fill_b - ctx.sig_entry_b) * matched_phys).max(0.0) }
    } else { 0.0 };

    let entry_slip = slip_a + slip_b;
    let (contracts_a_db, contracts_b_db) = if !all_trimmed && !ctx.dry_run {
        tracing::warn!(
            "[real] ⚠️ PARTIAL TRIM FAILED for {} — storing actual quantities (a={:.6} b={:.6}) instead of matched ({:.6})",
            ctx.pair, phys_a, phys_b, matched_phys
        );
        (phys_a / ctx.mult_a, phys_b / ctx.mult_b)
    } else {
        (matched_phys / ctx.mult_a, matched_phys / ctx.mult_b)
    };

    if fill_a > 0.0 && ctx.sig_entry_a > 0.0 {
        let ratio_a = fill_a / ctx.sig_entry_a;
        if ratio_a > 2.0 || ratio_a < 0.5 {
            tracing::error!("[real] 🚨 PRICE SANITY FAIL {}/{}: fill_a={:.8} entry_a={:.8} ratio={:.2}. Using L1 fallback!",
                ctx.pair, ctx.exa, fill_a, ctx.sig_entry_a, ratio_a);
            fill_a = ctx.sig_entry_a;
        }
    }
    if fill_b > 0.0 && ctx.sig_entry_b > 0.0 {
        let ratio_b = fill_b / ctx.sig_entry_b;
        if ratio_b > 2.0 || ratio_b < 0.5 {
            tracing::error!("[real] 🚨 PRICE SANITY FAIL {}/{}: fill_b={:.8} entry_b={:.8} ratio={:.2}. Using L1 fallback!",
                ctx.pair, ctx.exb, fill_b, ctx.sig_entry_b, ratio_b);
            fill_b = ctx.sig_entry_b;
        }
    }

    if fill_a > 0.0 && fill_b > 0.0 && ctx.sig_entry_a > 0.0 && ctx.sig_entry_b > 0.0 {
        let fill_ratio = fill_a / fill_b;
        let sig_ratio = ctx.sig_entry_a / ctx.sig_entry_b;
        let cross_ratio = fill_ratio / sig_ratio;
        if cross_ratio > 5.0 || cross_ratio < 0.2 {
            tracing::error!(
                "[real] 🚨 CROSS-LEG SANITY FAIL {}: fill_a/fill_b={:.6} sig_a/sig_b={:.6} cross_ratio={:.2}. Aborting trade + trimming.",
                ctx.pair, fill_ratio, sig_ratio, cross_ratio
            );
            phys_a = 0.0;
            phys_b = 0.0;
        }
    }

    tracing::info!("[real] [EXCHANGE_VERIFIED] ✅ OPEN {} phys={:.6} (Trimming: -${:.4}) A_price={:.6} B_price={:.6} fees={:.4} slip={:.4} actual_sp={:.4}%",
        ctx.pair, matched_phys, trimming_loss, fill_a, fill_b, entry_comm, entry_slip, actual_entry_spread);

    repo.insert_new_entry(
        ctx.pair.clone(), ctx.exa.clone(), ctx.exb.clone(), ctx.sig_direction.clone(),
        final_vol_usd, fill_a, fill_b, actual_entry_spread, crate::engine::utils::now_ms(),
        ctx.profile_id.clone(), entry_comm, entry_slip,
        oid_a.unwrap_or_default(), oid_b.unwrap_or_default(),
        contracts_a_db, contracts_b_db, ctx.sig_structural_spread,
    ).await;

    true
}

pub struct CloseTaskContext {
    pub trade_id: i64,
    pub pair: String,
    pub exa: String,
    pub exb: String,
    pub is_a_higher: bool,
    pub q_a: f64,
    pub q_b: f64,
    pub mult_a: f64,
    pub mult_b: f64,
    pub tick_a: f64,
    pub tick_b: f64,
    pub leverage: f64,
    pub exit_price_a: f64,
    pub exit_price_b: f64,
    pub use_ioc: bool,
    pub slippage_percent: f64,
    pub entry_a: f64,
    pub entry_b: f64,
    pub trade_coins_a: f64,
    pub trade_coins_b: f64,
    pub close_verify_retries: u32,
    pub close_verify_delay: u64,
    pub api_timeout_ms: u64,
    pub reason: String,
    pub fund_cost: f64,
    pub exit_spread: f64,
}

pub fn spawn_close_task(
    ctx: CloseTaskContext,
    http: Client,
    keys: Arc<ExchangeKeys>,
    db: Connection,
    e_ctx: Arc<EngineContext>,
) {
    let closing_guard = Arc::clone(&e_ctx.closing_in_flight);
    let trade_id_for_guard = ctx.trade_id;

    tokio::spawn(async move {
        let conn_a = crate::engine::exchanges::get_connector(&ctx.exa).unwrap();
        let conn_b = crate::engine::exchanges::get_connector(&ctx.exb).unwrap();

        let cr = execute_close(
            &conn_a, &conn_b, &http, &keys, &ctx.pair,
            ctx.is_a_higher, ctx.q_a, ctx.q_b, ctx.leverage,
            ctx.exit_price_a, ctx.exit_price_b,
            ctx.use_ioc, ctx.slippage_percent, ctx.mult_a, ctx.mult_b, ctx.tick_a, ctx.tick_b,
            ctx.close_verify_retries, ctx.close_verify_delay, ctx.api_timeout_ms,
            ctx.entry_a, ctx.entry_b, ctx.trade_coins_a, ctx.trade_coins_b,
        ).await;

        let repo_close = TradeRepository::new(db.clone());

        if cr.real_exit_a <= 0.0 || cr.real_exit_b <= 0.0 {
            tracing::error!("[real] ⚠️ CLOSE FAILED OR UNVERIFIED id={} {}. Error: {}", ctx.trade_id, ctx.pair, cr.error_msg);
            let keep_closing = cr.leg_a_external || cr.leg_b_external || ctx.reason == "MANUAL";
            repo_close.record_close_failure(ctx.trade_id, &cr.error_msg, keep_closing).await;
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            closing_guard.remove(&trade_id_for_guard);
            return;
        }

        let close_reason_db = if cr.leg_a_external && cr.leg_b_external {
            "EXTERNAL_CLOSE".to_string()
        } else if cr.leg_a_external || cr.leg_b_external {
            "LEG_MISSING_ON_CLOSE".to_string()
        } else {
            ctx.reason.clone()
        };

        let real_gp = cr.pnl_a + cr.pnl_b;

        let slip_cost_exit_a = if cr.real_exit_a > 0.0 {
            if ctx.is_a_higher { ((cr.real_exit_a - ctx.exit_price_a) * ctx.trade_coins_a).max(0.0) } else { ((ctx.exit_price_a - cr.real_exit_a) * ctx.trade_coins_a).max(0.0) }
        } else { 0.0 };
        let slip_cost_exit_b = if cr.real_exit_b > 0.0 {
            if !ctx.is_a_higher { ((cr.real_exit_b - ctx.exit_price_b) * ctx.trade_coins_b).max(0.0) } else { ((ctx.exit_price_b - cr.real_exit_b) * ctx.trade_coins_b).max(0.0) }
        } else { 0.0 };
        let exit_slip = slip_cost_exit_a + slip_cost_exit_b;

        tracing::info!("[real] [EXCHANGE_VERIFIED] PnL id={} pna={:.4} pnb={:.4} gross={:.4} exit_fee={:.4} exit_slip={:.4}", 
            ctx.trade_id, cr.pnl_a, cr.pnl_b, real_gp, cr.exit_fee, exit_slip);

        let both_external = cr.leg_a_external && cr.leg_b_external;

        // Если close вернул валидные цены для обоих ног и нет external/missing —
        // доверяем результату, не перепроверяем через fetch_positions.
        // Stale API кэш может показать позицию ещё 1-3с после закрытия,
        // что вызывает ложный retry → на повторе одна нога уже закрыта →
        // exit_price = entry_price → PnL обнуляется. Это критический баг учёта.
        let both_legs_confirmed = cr.real_exit_a > 0.0 && cr.real_exit_b > 0.0
            && !cr.leg_a_external && !cr.leg_b_external
            && cr.error_msg.is_empty();

        if both_legs_confirmed {
            tracing::info!(
                "[real] ✅ Both legs confirmed by exchange for id={} {} — skipping fetch_positions verification",
                ctx.trade_id, ctx.pair
            );
        } else {
            // Есть сомнения (external leg, ошибка) — проверяем через fetch_positions
            let pos_check_a = conn_a.fetch_positions(&http, &keys).await;
            let pos_check_b = conn_b.fetch_positions(&http, &keys).await;
            let still_a = pos_check_a.as_ref().ok().map(|ps| ps.iter().any(|p| crate::engine::utils::symbols_match(&p.symbol, &ctx.pair) && p.size.abs() > 1e-12)).unwrap_or(true);
            let still_b = pos_check_b.as_ref().ok().map(|ps| ps.iter().any(|p| crate::engine::utils::symbols_match(&p.symbol, &ctx.pair) && p.size.abs() > 1e-12)).unwrap_or(true);
            if (still_a || still_b) && !both_external {
                tracing::error!("[real] ⚠️ POSITION STILL EXISTS after close id={} {} still_a={} still_b={} — keeping trade OPEN for retry/recovery",
                    ctx.trade_id, ctx.pair, still_a, still_b);
                repo_close.record_close_failure(ctx.trade_id, &format!("post-close verification failed still_a={} still_b={}", still_a, still_b), true).await;
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                closing_guard.remove(&trade_id_for_guard);
                return;
            }
            if both_external && (still_a || still_b) {
                tracing::warn!("[real] ⚠️ EXTERNAL_CLOSE id={} {} — both legs reported 'no position' but still_a={} still_b={}. Finalizing anyway (dust/phantom).",
                    ctx.trade_id, ctx.pair, still_a, still_b);
            }
        }

        repo_close.mark_trade_closed_confirmed(
            ctx.trade_id, cr.real_exit_a, cr.real_exit_b, ctx.exit_spread, real_gp,
            cr.pnl_a, cr.pnl_b, &close_reason_db, ctx.fund_cost, cr.exit_fee, exit_slip,
        ).await;
        closing_guard.remove(&trade_id_for_guard);
    });
}
