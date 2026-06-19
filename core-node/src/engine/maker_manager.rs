//! Maker Manager — центральная логика квази-Maker схемы
//!
//! Управляет жизненным циклом pending Post-Only ордеров:
//! 1. submit_maker_entry() — выставляет Post-Only на «медленной» бирже
//! 2. process_pending_makers() — вызывается каждый цикл:
//!    - Проверяет WS private cache на fills
//!    - При fill → IOC hedge на «быстрой» бирже → book trade
//!    - Проверяет спред: если вышел за рамки → cancel
//!    - Health check: exchange banned/unhealthy → cancel

use std::sync::Arc;
use reqwest::Client;
use tokio_rusqlite::Connection;
use tracing::{info, warn, error};

use crate::engine::db::TradeRepository;
use crate::engine::real::ExchangeKeys;
use crate::engine::exchanges;
use crate::engine::utils::{liquidity_rank, now_ms};
use crate::state::{EngineContext, PriceMap};
use shared::{TradeSignal, TechnicalSettings, TradingSettings};

/// A pending maker order sitting in an exchange's orderbook.
#[derive(Clone, Debug)]
pub struct PendingMakerOrder {
    pub pair: String,
    pub maker_exchange: String,     // "gate", "kucoin", etc.
    pub hedge_exchange: String,     // "binance", "bybit"
    pub order_id: String,
    pub is_a_higher: bool,
    pub maker_is_a: bool,           // true = maker is leg A
    pub maker_is_buy: bool,         // true = maker order is BUY
    pub qty: f64,                   // physical qty (coins)
    pub maker_price: f64,           // price the maker order was placed at
    pub hedge_limit_price: f64,     // target price for hedge IOC
    pub placed_at_ms: i64,
    // Signal context for DB booking
    pub sig_spread: f64,
    pub sig_structural_spread: f64,
    pub sig_direction: String,
    pub sig_entry_a: f64,
    pub sig_entry_b: f64,
    pub profile_id: String,
    pub break_even_pct: f64,
    pub ioc_slippage_pct: f64,
    pub leverage: f64,
    pub mult_a: f64,
    pub mult_b: f64,
    pub tick_a: f64,
    pub tick_b: f64,
    pub exa: String,
    pub exb: String,
    pub contracts_a: f64,
    pub contracts_b: f64,
}

/// Submit a maker entry for a trade signal.
/// Places a Post-Only order on the less-liquid exchange and registers it as pending.
/// Returns immediately — fill is detected asynchronously via WS private stream.
pub async fn submit_maker_entry(
    sig: &TradeSignal,
    _prices: &PriceMap,
    settings: &TradingSettings,
    http: &Client,
    keys: &Arc<ExchangeKeys>,
    ts: &TechnicalSettings,
    e_ctx: &Arc<EngineContext>,
) {
    let pair = &sig.pair;
    let exa = &sig.exchange_a;
    let exb = &sig.exchange_b;

    // Already have a pending maker for this pair
    if e_ctx.pending_makers.contains_key(pair) {
        return;
    }

    // Determine maker (less liquid) and hedge (more liquid) exchanges
    let rank_a = liquidity_rank(exa);
    let rank_b = liquidity_rank(exb);

    // Maker = higher rank number (less liquid)
    // Hedge = lower rank number (more liquid)
    let (maker_ex, hedge_ex, maker_is_a) = if rank_a >= rank_b {
        (exa.as_str(), exb.as_str(), true)
    } else {
        (exb.as_str(), exa.as_str(), false)
    };

    let maker_conn = match exchanges::get_connector(maker_ex) {
        Some(c) => c,
        None => {
            error!("[maker] No connector for maker exchange: {}", maker_ex);
            return;
        }
    };

    // Determine maker order direction:
    // If A is higher-priced:
    //   - Sell A (bid side), Buy B (ask side)
    //   - If maker=A → we SELL (maker_is_buy=false)
    //   - If maker=B → we BUY (maker_is_buy=true)
    let maker_is_buy = if sig.is_a_higher {
        !maker_is_a  // A sells, B buys
    } else {
        maker_is_a   // B sells, A buys
    };

    // Calculate maker price: inside the spread with offset
    // For BUY: slightly above best ask → but still inside spread (will be maker)
    // Actually for Post-Only BUY: price must be <= current best bid (otherwise it crosses)
    // For Post-Only SELL: price must be >= current best ask (otherwise it crosses)
    //
    // Real strategy: place at the BEST price on our side with a tiny offset
    let maker_entry = if maker_is_a { sig.entry_a } else { sig.entry_b };
    let offset = maker_entry * ts.maker_price_offset_pct / 100.0;

    let maker_price = if maker_is_buy {
        // BUY → place at bid + offset (inside spread, still maker)
        maker_entry + offset
    } else {
        // SELL → place at ask - offset (inside spread, still maker)
        maker_entry - offset
    };

    // Hedge price (the other leg's entry price, with IOC slippage)
    let hedge_entry = if maker_is_a { sig.entry_b } else { sig.entry_a };

    let qty = sig.target_qty_a; // Both legs have same physical qty
    let mult = if maker_is_a { sig.mult_a } else { sig.mult_b };
    let tick = if maker_is_a { sig.tick_a } else { sig.tick_b };

    // Ensure leverage is set (pre-warmed, just ensure via WS)
    let _ = maker_conn.ensure_leverage(http, keys, pair, settings.leverage as f64).await;

    // Place the Post-Only order
    info!(
        "[maker] Placing POST-ONLY: pair={} exchange={} side={} qty={:.6} price={:.8} (entry={:.8} offset={:.6}%)",
        pair, maker_ex, if maker_is_buy { "BUY" } else { "SELL" },
        qty, maker_price, maker_entry, ts.maker_price_offset_pct
    );

    match maker_conn.place_maker_order(
        http, keys, pair, maker_is_buy, qty,
        maker_price, settings.leverage as f64,
        mult, tick, ts.api_timeout_ms,
    ).await {
        Ok((order_id, placed_price)) => {
            info!(
                "[maker] ✅ POST-ONLY placed: pair={} exchange={} oid={} price={:.8}",
                pair, maker_ex, order_id, placed_price
            );

            let ioc_slippage_pct = ((sig.spread - sig.break_even_pct) / 2.0)
                .max(0.05)
                .min(0.5);

            let pending = PendingMakerOrder {
                pair: pair.clone(),
                maker_exchange: maker_ex.to_string(),
                hedge_exchange: hedge_ex.to_string(),
                order_id,
                is_a_higher: sig.is_a_higher,
                maker_is_a,
                maker_is_buy,
                qty,
                maker_price: placed_price,
                hedge_limit_price: hedge_entry,
                placed_at_ms: now_ms(),
                sig_spread: sig.spread,
                sig_structural_spread: sig.structural_spread,
                sig_direction: sig.direction.clone(),
                sig_entry_a: sig.entry_a,
                sig_entry_b: sig.entry_b,
                profile_id: sig.profile_id.clone(),
                break_even_pct: sig.break_even_pct,
                ioc_slippage_pct,
                leverage: settings.leverage as f64,
                mult_a: sig.mult_a,
                mult_b: sig.mult_b,
                tick_a: sig.tick_a,
                tick_b: sig.tick_b,
                exa: sig.exchange_a.clone(),
                exb: sig.exchange_b.clone(),
                contracts_a: sig.contracts_a,
                contracts_b: sig.contracts_b,
            };

            e_ctx.pending_makers.insert(pair.clone(), pending);
        }
        Err(e) => {
            warn!("[maker] Post-Only rejected for {} on {}: {}", pair, maker_ex, e);
            // Brief cooldown on rejection
            e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + 3_000);
        }
    }
}

/// Process all pending maker orders. Called every cycle from cycle.rs.
/// - Checks for fills in WS private caches
/// - Validates spread conditions
/// - Cancels stale/invalid orders
pub async fn process_pending_makers(
    prices: &PriceMap,
    settings: &TradingSettings,
    http: &Client,
    db: &Connection,
    keys: &Arc<ExchangeKeys>,
    ts: &TechnicalSettings,
    e_ctx: &Arc<EngineContext>,
) {
    // Collect keys to avoid holding DashMap ref during async ops
    let pending_keys: Vec<String> = e_ctx.pending_makers.iter()
        .map(|entry| entry.key().clone())
        .collect();

    if pending_keys.is_empty() {
        return;
    }

    for pair_key in pending_keys {
        let pending = match e_ctx.pending_makers.get(&pair_key) {
            Some(p) => p.clone(),
            None => continue,
        };

        // ── 1. CHECK FOR FILL IN WS CACHE ──────────────────────────────────
        let fill_result = check_maker_fill(&pending);

        if let Some((fill_price, fill_fee, filled_qty_contracts)) = fill_result {
            info!(
                "[maker] 🔔 FILL DETECTED: pair={} exchange={} oid={} price={:.8} fee={:.6} contracts={}",
                pending.pair, pending.maker_exchange, pending.order_id,
                fill_price, fill_fee, filled_qty_contracts
            );

            // Remove from pending BEFORE firing hedge (prevents re-processing)
            e_ctx.pending_makers.remove(&pair_key);

            // Fire IOC hedge
            execute_hedge(
                &pending, fill_price, fill_fee, filled_qty_contracts,
                http, db, keys, ts, e_ctx, settings,
            ).await;

            continue;
        }

        // ── 2. SPREAD VALIDITY CHECK ────────────────────────────────────────
        // If spread has collapsed below min_entry_spread → cancel
        let current_spread = calculate_current_spread(
            prices, &pending.pair, &pending.exa, &pending.exb, pending.is_a_higher,
        );

        if let Some(spread) = current_spread {
            if spread < settings.min_entry_spread as f64 {
                info!(
                    "[maker] Spread collapsed: pair={} current={:.4}% < min={:.2}% → CANCEL",
                    pending.pair, spread, settings.min_entry_spread
                );
                cancel_pending_maker(&pending, http, keys, e_ctx).await;
                continue;
            }

            // Check spread is still within max_entry_spread
            if spread > settings.max_entry_spread as f64 {
                warn!(
                    "[maker] Spread exceeded max: pair={} current={:.4}% > max={:.2}% → CANCEL",
                    pending.pair, spread, settings.max_entry_spread
                );
                cancel_pending_maker(&pending, http, keys, e_ctx).await;
                continue;
            }
        }

        // ── 3. HEALTH CHECK ─────────────────────────────────────────────────
        if !crate::engine::risk_manager::verify_trade_health(
            &pending.maker_exchange, &pending.hedge_exchange, e_ctx,
        ) {
            warn!(
                "[maker] Health check failed for {} ({}/{}). Cancelling maker.",
                pending.pair, pending.maker_exchange, pending.hedge_exchange
            );
            cancel_pending_maker(&pending, http, keys, e_ctx).await;
            continue;
        }

        // ── 4. KILL SWITCH ──────────────────────────────────────────────────
        if e_ctx.is_killed.load(std::sync::atomic::Ordering::Relaxed) {
            warn!("[maker] Kill switch active. Cancelling maker for {}.", pending.pair);
            cancel_pending_maker(&pending, http, keys, e_ctx).await;
            continue;
        }
    }
}

/// Check WS private cache for a fill on the maker order.
/// Returns Some((fill_price, fee, filled_contracts)) if filled.
fn check_maker_fill(pending: &PendingMakerOrder) -> Option<(f64, f64, i64)> {
    match pending.maker_exchange.as_str() {
        "gate" => {
            let cache = crate::engine::exchanges::ws_gate_private::get_order_cache();
            if let Some(state) = cache.get(&pending.order_id) {
                if state.filled_contracts > 0 && state.fill_price > 0.0 {
                    return Some((state.fill_price, state.fee, state.filled_contracts));
                }
                // Check if order was cancelled externally
                if state.status == "finished" && state.finish_as != "filled" {
                    tracing::info!(
                        "[maker] Gate order {} finished as '{}' — removing from pending",
                        pending.order_id, state.finish_as
                    );
                    // Return None — will be cleaned up by caller checking finish_as
                }
            }
            None
        }
        // Future: "bitget", "kucoin", "bybit", etc.
        _ => None,
    }
}

/// Calculate current spread from live prices.
fn calculate_current_spread(
    prices: &PriceMap,
    pair: &str,
    exa: &str,
    exb: &str,
    is_a_higher: bool,
) -> Option<f64> {
    let data_a = prices.get(exa).and_then(|m| m.get(pair).cloned())?;
    let data_b = prices.get(exb).and_then(|m| m.get(pair).cloned())?;

    let spread = if is_a_higher {
        // A higher: sell A (bid), buy B (ask)
        (data_a.bid - data_b.ask) / data_b.ask.max(1e-12) * 100.0
    } else {
        // B higher: sell B (bid), buy A (ask)
        (data_b.bid - data_a.ask) / data_a.ask.max(1e-12) * 100.0
    };

    Some(spread)
}

/// Cancel a pending maker order and remove from state.
async fn cancel_pending_maker(
    pending: &PendingMakerOrder,
    http: &Client,
    keys: &Arc<ExchangeKeys>,
    e_ctx: &Arc<EngineContext>,
) {
    if let Some(conn) = exchanges::get_connector(&pending.maker_exchange) {
        match conn.cancel_order(http, keys, &pending.pair, &pending.order_id).await {
            Ok(true) => {
                info!("[maker] Cancelled maker order {} on {}", pending.order_id, pending.maker_exchange);
            }
            Ok(false) => {
                // Already filled or gone — will be handled by fill check next cycle
                warn!("[maker] Order {} already filled/gone on {}", pending.order_id, pending.maker_exchange);
            }
            Err(e) => {
                error!("[maker] Failed to cancel order {} on {}: {}", pending.order_id, pending.maker_exchange, e);
            }
        }
    }
    e_ctx.pending_makers.remove(&pending.pair);
}

/// Execute the hedge leg after maker fill is confirmed.
async fn execute_hedge(
    pending: &PendingMakerOrder,
    maker_fill_price: f64,
    maker_fee: f64,
    _filled_contracts: i64,
    http: &Client,
    db: &Connection,
    keys: &Arc<ExchangeKeys>,
    ts: &TechnicalSettings,
    e_ctx: &Arc<EngineContext>,
    settings: &TradingSettings,
) {
    let hedge_conn = match exchanges::get_connector(&pending.hedge_exchange) {
        Some(c) => c,
        None => {
            error!("[maker] No connector for hedge exchange: {}", pending.hedge_exchange);
            // CRITICAL: We have an open position on maker exchange with no hedge!
            // Must trim the maker position
            trim_unhedged_maker(pending, maker_fill_price, maker_fee, http, db, keys, ts, e_ctx, settings).await;
            return;
        }
    };

    // Hedge direction is OPPOSITE of maker
    let hedge_is_buy = !pending.maker_is_buy;

    // Hedge price with aggressive slippage
    let hedge_target_price = pending.hedge_limit_price;
    let hedge_mult = if pending.maker_is_a { pending.mult_b } else { pending.mult_a };
    let hedge_tick = if pending.maker_is_a { pending.tick_b } else { pending.tick_a };

    info!(
        "[maker] 🚀 Firing IOC hedge: pair={} exchange={} side={} qty={:.6} price={:.8} slip={:.3}%",
        pending.pair, pending.hedge_exchange,
        if hedge_is_buy { "BUY" } else { "SELL" },
        pending.qty, hedge_target_price, pending.ioc_slippage_pct
    );

    // Ensure leverage on hedge exchange
    let _ = hedge_conn.ensure_leverage(http, keys, &pending.pair, pending.leverage).await;

    let hedge_result = hedge_conn.place_order(
        http,
        keys,
        &pending.pair,
        hedge_is_buy,
        pending.qty,
        false, // not dry run
        hedge_target_price,
        true,  // use IOC
        pending.leverage,
        hedge_mult,
        pending.ioc_slippage_pct,
        hedge_tick,
        ts.api_timeout_ms,
    ).await;

    match hedge_result {
        Ok((Some(hedge_oid), hedge_fill_price, hedge_fee)) if hedge_fill_price > 0.0 => {
            info!(
                "[maker] ✅ HEDGE FILLED: pair={} exchange={} oid={} price={:.8} fee={:.6}",
                pending.pair, pending.hedge_exchange, hedge_oid, hedge_fill_price, hedge_fee
            );

            // Book the trade in DB
            book_maker_trade(
                pending, maker_fill_price, maker_fee,
                &hedge_oid, hedge_fill_price, hedge_fee,
                db, e_ctx, ts,
            ).await;
        }
        Ok((_, hedge_fill_price, _)) => {
            error!(
                "[maker] ❌ HEDGE NOT FILLED: pair={} exchange={} fill_price={:.8}. Trimming maker leg!",
                pending.pair, pending.hedge_exchange, hedge_fill_price
            );
            trim_unhedged_maker(pending, maker_fill_price, maker_fee, http, db, keys, ts, e_ctx, settings).await;
        }
        Err(e) => {
            error!(
                "[maker] ❌ HEDGE FAILED: pair={} exchange={}: {}. Trimming maker leg!",
                pending.pair, pending.hedge_exchange, e
            );
            trim_unhedged_maker(pending, maker_fill_price, maker_fee, http, db, keys, ts, e_ctx, settings).await;
        }
    }
}

/// Book a completed maker+hedge trade to the database.
async fn book_maker_trade(
    pending: &PendingMakerOrder,
    maker_fill_price: f64,
    maker_fee: f64,
    hedge_oid: &str,
    hedge_fill_price: f64,
    hedge_fee: f64,
    db: &Connection,
    e_ctx: &Arc<EngineContext>,
    ts: &TechnicalSettings,
) {
    let repo = TradeRepository::new(db.clone());

    // Determine which leg is A and which is B
    let (fill_a, fill_b, oid_a, oid_b, fee_total) = if pending.maker_is_a {
        (maker_fill_price, hedge_fill_price, pending.order_id.clone(), hedge_oid.to_string(), maker_fee + hedge_fee)
    } else {
        (hedge_fill_price, maker_fill_price, hedge_oid.to_string(), pending.order_id.clone(), maker_fee + hedge_fee)
    };

    let entry_spread = if pending.is_a_higher {
        (fill_a - fill_b) / fill_b.max(1e-12) * 100.0
    } else {
        (fill_b - fill_a) / fill_a.max(1e-12) * 100.0
    };

    let now = now_ms();

    info!(
        "[maker] 📊 BOOKING TRADE: pair={} dir={} fill_a={:.8} fill_b={:.8} entry_spread={:.4}% fee={:.6}",
        pending.pair, pending.sig_direction, fill_a, fill_b, entry_spread, fee_total
    );

    repo.insert_new_entry(
        pending.pair.clone(),
        pending.exa.clone(),
        pending.exb.clone(),
        pending.sig_direction.clone(),
        pending.qty * fill_a.max(fill_b), // volume in USD
        fill_a,
        fill_b,
        entry_spread,
        now,
        pending.profile_id.clone(),
        fee_total,
        0.0, // slippage (maker = 0 slippage on maker leg)
        oid_a,
        oid_b,
        pending.contracts_a,
        pending.contracts_b,
        pending.sig_structural_spread,
    ).await;

    // Reset consecutive failure counters
    e_ctx.consecutive_failures.store(0, std::sync::atomic::Ordering::Relaxed);
    e_ctx.consecutive_trims.store(0, std::sync::atomic::Ordering::Relaxed);

    // Set pair cooldown
    e_ctx.pair_cooldown.insert(pending.pair.clone(), now + ts.pair_cooldown_ms);

    info!(
        "[maker] ✅ TRADE BOOKED: pair={} dir={} spread={:.4}%",
        pending.pair, pending.sig_direction, entry_spread
    );
}

/// Trim an unhedged maker position (maker filled but hedge failed).
/// Closes the maker leg immediately to avoid directional exposure.
async fn trim_unhedged_maker(
    pending: &PendingMakerOrder,
    maker_fill_price: f64,
    maker_fee: f64,
    http: &Client,
    db: &Connection,
    keys: &Arc<ExchangeKeys>,
    ts: &TechnicalSettings,
    e_ctx: &Arc<EngineContext>,
    settings: &TradingSettings,
) {
    error!(
        "[maker] ⚠️ TRIMMING unhedged maker: pair={} exchange={} oid={} price={:.8}",
        pending.pair, pending.maker_exchange, pending.order_id, maker_fill_price
    );

    let maker_conn = match exchanges::get_connector(&pending.maker_exchange) {
        Some(c) => c,
        None => {
            error!("[maker] Cannot trim — no connector for {}", pending.maker_exchange);
            return;
        }
    };

    // Close the maker position (opposite direction)
    let close_is_buy = !pending.maker_is_buy;
    let maker_mult = if pending.maker_is_a { pending.mult_a } else { pending.mult_b };
    let maker_tick = if pending.maker_is_a { pending.tick_a } else { pending.tick_b };

    let trim_result = maker_conn.close_position(
        http, keys, &pending.pair,
        close_is_buy,
        pending.qty,
        false,
        pending.leverage,
        maker_fill_price, // use fill price as target
        true,  // IOC
        0.5,   // aggressive slippage for emergency close
        maker_mult,
        maker_tick,
        3, 100, // retries / delay
        ts.api_timeout_ms,
    ).await;

    let trim_fee = match &trim_result {
        Ok(receipt) => receipt.total_fee,
        Err(_) => 0.0,
    };

    // Record as asymmetric close in DB
    let repo = TradeRepository::new(db.clone());
    let total_fee = maker_fee + trim_fee;
    let trim_price = trim_result.as_ref().map(|r| r.final_price).unwrap_or(maker_fill_price);

    // PnL from the round-trip on maker exchange
    let gross_pnl = if pending.maker_is_buy {
        (trim_price - maker_fill_price) * pending.qty
    } else {
        (maker_fill_price - trim_price) * pending.qty
    };
    let net_pnl = gross_pnl - total_fee;

    let (fill_a, fill_b, ext_a, ext_b) = if pending.maker_is_a {
        (maker_fill_price, 0.0, trim_price, 0.0)
    } else {
        (0.0, maker_fill_price, 0.0, trim_price)
    };

    repo.append_asymmetric_close(
        pending.pair.clone(),
        pending.exa.clone(),
        pending.exb.clone(),
        pending.sig_direction.clone(),
        pending.qty * maker_fill_price,
        fill_a,
        fill_b,
        pending.sig_spread,
        now_ms(),
        gross_pnl, 0.0,
        pending.profile_id.clone(),
        total_fee, 0.0,
        net_pnl,
        gross_pnl,
        ext_a, ext_b,
    ).await;

    // Increment trim counter for risk management
    let trims = e_ctx.consecutive_trims.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    warn!("[maker] Trim #{} — unhedged maker closed. net_pnl={:.4}", trims, net_pnl);

    // Set exchange + pair cooldowns
    e_ctx.pair_cooldown.insert(pending.pair.clone(), now_ms() + ts.pair_cooldown_ms);
    e_ctx.exchange_cooldown.insert(pending.hedge_exchange.clone(), now_ms() + ts.exchange_cooldown_ms);
}


