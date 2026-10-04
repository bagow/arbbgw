use crate::engine::real::ExchangeKeys;
use crate::engine::utils::{
    cooldown_active, get_exchange_fee, local_timestamp_is_fresh, now_ms, now_secs,
};
use crate::state::{EngineContext, PriceMap};
use dashmap::DashMap;
use shared::{SpreadRow, TechnicalSettings, TradeSignal, TradingSettings};
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;

/// Throttle map for SIGNAL log messages: key = "pair:exa:exb" → last_log_ms
static SIGNAL_LOG_THROTTLE: OnceLock<DashMap<String, i64>> = OnceLock::new();
fn signal_throttle() -> &'static DashMap<String, i64> {
    SIGNAL_LOG_THROTTLE.get_or_init(|| DashMap::new())
}

/// A profile-window L1 excursion can disappear between the 100 ms shadow
/// samples.  Keep the first stale-L2 observation visible in the service log,
/// but bound repeated messages while the same route is waiting for depth.
static L2_PENDING_LOG_THROTTLE: OnceLock<DashMap<String, i64>> = OnceLock::new();
const L2_PENDING_LOG_THROTTLE_MS: i64 = 10_000;

fn should_log_l2_pending(route: &str, now_ms: i64) -> bool {
    let throttle = L2_PENDING_LOG_THROTTLE.get_or_init(DashMap::new);
    if let Some(mut last) = throttle.get_mut(route) {
        if now_ms.saturating_sub(*last) < L2_PENDING_LOG_THROTTLE_MS {
            return false;
        }
        *last = now_ms;
        true
    } else {
        throttle.insert(route.to_string(), now_ms);
        true
    }
}

static LAST_DIAG_LOG: AtomicI64 = AtomicI64::new(0);

/// An entry must remain executable while both venue books advance.  Keeping
/// this state in the event-driven scanner avoids sleeping on the hot path: the
/// confirming market event carries a fresh quote and can be dispatched
/// immediately.
const ENTRY_CONFIRMATION_MS: i64 = 500;
const ENTRY_WIDENING_EPSILON_PCT: f64 = 0.000001;

#[derive(Clone, Copy, Debug, PartialEq)]
struct EntryConfirmation {
    first_seen_ms: i64,
    initial_spread: f64,
    initial_sell_book_ms: i64,
    initial_buy_book_ms: i64,
}

static ENTRY_CONFIRMATIONS: OnceLock<DashMap<String, EntryConfirmation>> = OnceLock::new();

fn advance_entry_confirmation(
    previous: Option<EntryConfirmation>,
    now_ms: i64,
    spread: f64,
    sell_book_ms: i64,
    buy_book_ms: i64,
) -> (EntryConfirmation, bool) {
    let reset = || EntryConfirmation {
        first_seen_ms: now_ms,
        initial_spread: spread,
        initial_sell_book_ms: sell_book_ms,
        initial_buy_book_ms: buy_book_ms,
    };
    let Some(state) = previous else {
        return (reset(), false);
    };
    if now_ms < state.first_seen_ms
        || sell_book_ms < state.initial_sell_book_ms
        || buy_book_ms < state.initial_buy_book_ms
    {
        return (reset(), false);
    }
    let elapsed = now_ms - state.first_seen_ms;
    if elapsed >= ENTRY_CONFIRMATION_MS
        && spread > state.initial_spread + ENTRY_WIDENING_EPSILON_PCT
    {
        return (reset(), false);
    }
    let both_books_advanced = sell_book_ms > state.initial_sell_book_ms
        && buy_book_ms > state.initial_buy_book_ms;
    (
        state,
        elapsed >= ENTRY_CONFIRMATION_MS && both_books_advanced,
    )
}

fn confirmation_route(signal: &TradeSignal) -> (String, String, String) {
    let (sell_exchange, buy_exchange) = if signal.is_a_higher {
        (&signal.exchange_a, &signal.exchange_b)
    } else {
        (&signal.exchange_b, &signal.exchange_a)
    };
    (
        format!(
            "{}:{}:{}>{}",
            signal.profile_id, signal.pair, sell_exchange, buy_exchange
        ),
        format!("{}:{}", sell_exchange, signal.pair),
        format!("{}:{}", buy_exchange, signal.pair),
    )
}

/// A pair of individually fresh books can still describe different market
/// moments. With 100 ms venue feeds, 250 ms leaves room for normal phase
/// offset and network jitter while rejecting a quote assembled from a moving
/// market and a lagging peer.
pub(crate) const MAX_EXECUTION_BOOK_SKEW_MS: i64 = 250;

pub(crate) fn execution_books_are_fresh(
    e_ctx: &EngineContext,
    key_a: &str,
    key_b: &str,
    now_ms: i64,
) -> bool {
    let valid = |key: &str| {
        e_ctx.l2_books.get(key).is_some_and(|book| {
            let top = book.bids.first().zip(book.asks.first());
            local_timestamp_is_fresh(now_ms, book.updated_ms, 750)
                && top.is_some_and(|(bid, ask)| {
                    [bid.0, bid.1, ask.0, ask.1]
                        .iter()
                        .all(|value| value.is_finite() && *value > 0.0)
                        && bid.0 < ask.0
                })
        })
    };
    if !valid(key_a) || !valid(key_b) {
        return false;
    }
    let Some(updated_a) = e_ctx.l2_books.get(key_a).map(|book| book.updated_ms) else {
        return false;
    };
    let Some(updated_b) = e_ctx.l2_books.get(key_b).map(|book| book.updated_ms) else {
        return false;
    };
    updated_a.abs_diff(updated_b) <= MAX_EXECUTION_BOOK_SKEW_MS as u64
}

/// Price the complete physical quantity against both executable books.
/// L1 quotes are only candidate discovery; a signal must be profitable at L2
/// VWAP in the same direction before any private REST preflight is attempted.
fn executable_l2_spread(
    e_ctx: &EngineContext,
    key_a: &str,
    key_b: &str,
    is_a_higher: bool,
    quantity: f64,
    now_ms: i64,
) -> Option<(f64, f64, f64)> {
    if !execution_books_are_fresh(e_ctx, key_a, key_b, now_ms) {
        return None;
    }
    let book_a = e_ctx.l2_books.get(key_a)?;
    let book_b = e_ctx.l2_books.get(key_b)?;
    let quote_a = crate::engine::book::quote(
        &book_a.bids,
        &book_a.asks,
        !is_a_higher,
        quantity,
    )?;
    let quote_b = crate::engine::book::quote(
        &book_b.bids,
        &book_b.asks,
        is_a_higher,
        quantity,
    )?;
    let spread = if is_a_higher {
        (quote_a.vwap - quote_b.vwap) / quote_b.vwap * 100.0
    } else {
        (quote_b.vwap - quote_a.vwap) / quote_a.vwap * 100.0
    };
    spread
        .is_finite()
        .then_some((quote_a.vwap, quote_b.vwap, spread))
}

/// Price the immediate reduce-only unwind that managed risk evaluates after
/// entry. The scanner rejects a signal if ordinary crossing costs would put a
/// new position on its own hard-stop boundary before any adverse move.
fn initial_l2_liquidation_net_pct(
    e_ctx: &EngineContext,
    key_a: &str,
    key_b: &str,
    is_a_higher: bool,
    quantity: f64,
    fee_a: f64,
    fee_b: f64,
    now_ms: i64,
) -> Option<f64> {
    if !execution_books_are_fresh(e_ctx, key_a, key_b, now_ms) {
        return None;
    }
    let book_a = e_ctx.l2_books.get(key_a)?;
    let book_b = e_ctx.l2_books.get(key_b)?;
    let entry_a = crate::engine::book::quote(
        &book_a.bids,
        &book_a.asks,
        !is_a_higher,
        quantity,
    )?;
    let entry_b = crate::engine::book::quote(
        &book_b.bids,
        &book_b.asks,
        is_a_higher,
        quantity,
    )?;
    let exit_a = crate::engine::book::quote(
        &book_a.bids,
        &book_a.asks,
        is_a_higher,
        quantity,
    )?;
    let exit_b = crate::engine::book::quote(
        &book_b.bids,
        &book_b.asks,
        !is_a_higher,
        quantity,
    )?;
    crate::engine::scanner_common::initial_liquidation_net_pct(
        is_a_higher,
        entry_a.vwap,
        entry_b.vwap,
        exit_a.vwap,
        exit_b.vwap,
        quantity,
        fee_a,
        fee_b,
    )
}

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
    let mut emitted_pairs: HashSet<String> = HashSet::new();
    let mut l2_pending_rows = 0u64;
    let mut profile_l2_pending_rows = 0u64;
    let mut structural_rejections = 0u64;
    let mut lot_grid_rejections = 0u64;
    let mut depth_rejections = 0u64;
    let mut l2_budget_rejections = 0u64;
    let mut hard_stop_compatibility_rejections = 0u64;
    let mut volume_rejections = 0u64;
    let now = now_ms();
    let now_s = now_secs();

    let ex_cd = &e_ctx.exchange_cooldown;
    let pair_cd = &e_ctx.pair_cooldown;

    // Log diagnostics throttled
    let last_diag = LAST_DIAG_LOG.load(Ordering::Relaxed);
    let log_diag = (now - last_diag) > (ts.diag_log_interval as i64);
    if log_diag {
        LAST_DIAG_LOG.store(now, Ordering::Relaxed);
    }

    for row in spreads.iter().take(ts.max_spreads_per_cycle as usize) {
        if log_diag {
            tracing::debug!(
                "[scanner] TRACE {} ({} vs {}) spread={:.4}% verified={} dir={}",
                row.pair, row.exchange_a, row.exchange_b, row.spread,
                row.structural_verified, row.direction
            );
        }
        if [&row.exchange_a,&row.exchange_b].iter().any(|ex|!crate::engine::exchanges::get_connector(ex).map(|c|c.supports_managed_orders()).unwrap_or(false)){continue;}
        if !keys.supports(&row.exchange_a) || !keys.supports(&row.exchange_b) {
            if log_diag {
                tracing::debug!("[scanner] TRACE {} → SKIP: keys not supported ({} or {})", row.pair, row.exchange_a, row.exchange_b);
            }
            continue;
        }

        let cd_a = ex_cd.get(&row.exchange_a).map(|v| *v).unwrap_or(0);
        let cd_b = ex_cd.get(&row.exchange_b).map(|v| *v).unwrap_or(0);
        if cooldown_active(cd_a, now, ts.exchange_cooldown_ms)
            || cooldown_active(cd_b, now, ts.exchange_cooldown_ms)
        {
            if log_diag {
                tracing::debug!("[scanner] TRACE {} → SKIP: exchange cooldown", row.pair);
            }
            continue;
        }

        let cd_pair = pair_cd.get(&row.pair).map(|v| *v).unwrap_or(0);
        if cooldown_active(cd_pair, now, ts.pair_cooldown_ms) {
            if log_diag {
                tracing::debug!("[scanner] TRACE {} → SKIP: pair cooldown", row.pair);
            }
            continue;
        }

        // ── HEALTH & RISK GUARD ──
        if !crate::engine::risk_manager::verify_trade_health(&row.exchange_a, &row.exchange_b, e_ctx) {
            if log_diag {
                tracing::debug!("[scanner] TRACE {} → SKIP: health check failed ({} or {})", row.pair, row.exchange_a, row.exchange_b);
            }
            continue;
        }

        // ── PRE-FORMAT CACHE KEYS (Optimization) ──
        let key_a = format!("{}:{}", row.exchange_a, row.pair);
        let key_b = format!("{}:{}", row.exchange_b, row.pair);

        let is_a_higher = row.direction.starts_with(&row.exchange_a);
        let entry_a = if is_a_higher { row.bid_a } else { row.ask_a };

        // ── LIQUIDITY DEPTH FILTER ──
        let data_a = prices
            .get(&row.exchange_a)
            .and_then(|m| m.get(&row.pair).cloned());
        let data_b = prices
            .get(&row.exchange_b)
            .and_then(|m| m.get(&row.pair).cloned());


        // Check if this spread is worth diagnosing (exceeds any active profile's min)
        let in_profile_window = profiles.iter().any(|p| {
            p.is_active && row.spread >= p.min_entry_spread && row.spread <= p.max_entry_spread
        });
        let matching_profile = profiles.iter().find(|profile| {
            profile.is_active
                && !profile.disabled_exchanges.contains(&row.exchange_a)
                && !profile.disabled_exchanges.contains(&row.exchange_b)
                && row.spread >= profile.min_entry_spread
                && row.spread <= profile.max_entry_spread
                && row.vol_a >= profile.min_volume_millions * 1_000_000.0
                && row.vol_b >= profile.min_volume_millions * 1_000_000.0
        });
        let worth_diag = log_diag && in_profile_window;

        // Discovery quotes only nominate candidates. Do not create an order
        // signal until both venues have a fresh executable-depth snapshot. In
        // particular, this gives Binance's demand-driven L2 subscription one
        // update cycle to replace an internally stale all-market book ticker.
        if !execution_books_are_fresh(e_ctx, &key_a, &key_b, now) {
            if in_profile_window {
                l2_pending_rows += 1;
            }
            if let Some(profile) = matching_profile {
                profile_l2_pending_rows += 1;
                let route = format!(
                    "{}:{}:{}:{}",
                    profile.id, row.pair, row.exchange_a, row.exchange_b,
                );
                if should_log_l2_pending(&route, now) {
                    let age_a = e_ctx
                        .l2_books
                        .get(&key_a)
                        .map(|book| now.saturating_sub(book.updated_ms));
                    let age_b = e_ctx
                        .l2_books
                        .get(&key_b)
                        .map(|book| now.saturating_sub(book.updated_ms));
                    tracing::info!(
                        "[scanner] PROFILE_L2_PENDING pair={} direction={} profile={} l1={:.4}% vol_a=${:.0} vol_b=${:.0} structural={:.4}% verified={} persistent={} age_a_ms={:?} age_b_ms={:?} wanted_a={} wanted_b={}",
                        row.pair,
                        row.direction,
                        profile.id,
                        row.spread,
                        row.vol_a,
                        row.vol_b,
                        row.structural_spread,
                        row.structural_verified,
                        row.has_structural_spread,
                        age_a,
                        age_b,
                        e_ctx.l2_wanted.contains(&key_a),
                        e_ctx.l2_wanted.contains(&key_b),
                    );
                }
            }
            if worth_diag {
                tracing::debug!(
                    "[scanner] REJECT {} spread={:.4}% -> executable L2 pending or stale",
                    row.pair,
                    row.spread,
                );
            }
            continue;
        }

        if let (Some(ref da), Some(ref db)) = (&data_a, &data_b) {

            let intra_a = (da.ask - da.bid) / da.bid.max(1e-12) * 100.0;
            let intra_b = (db.ask - db.bid) / db.bid.max(1e-12) * 100.0;
            if intra_a > ts.intra_spread_max_pct || intra_b > ts.intra_spread_max_pct {
                if worth_diag {
                    tracing::debug!("[scanner] REJECT {} spread={:.4}% -> intra_spread too wide ({:.2}%/{:.2}% > max {:.2}%)", row.pair, row.spread, intra_a, intra_b, ts.intra_spread_max_pct);
                }
                continue;
            }
        } else {
            if worth_diag {
                tracing::debug!(
                    "[scanner] REJECT {} spread={:.4}% -> no price data for {}/{}",
                    row.pair,
                    row.spread,
                    row.exchange_a,
                    row.exchange_b
                );
            }
            continue;
        }

        // ── BASE MODIFIERS ──
        let map = &e_ctx.multipliers;
        let mult_a = match map.get(&key_a) {
            Some(v) => *v,
            None => {
                if worth_diag {
                    tracing::debug!(
                        "[scanner] REJECT {} spread={:.4}% -> no multiplier for {}",
                        row.pair,
                        row.spread,
                        key_a
                    );
                }
                continue;
            }
        };
        let mult_b = match map.get(&key_b) {
            Some(v) => *v,
            None => {
                if worth_diag {
                    tracing::debug!(
                        "[scanner] REJECT {} spread={:.4}% -> no multiplier for {}",
                        row.pair,
                        row.spread,
                        key_b
                    );
                }
                continue;
            }
        };

        let has_tick_a = e_ctx.tick_sizes.contains_key(&key_a);
        let has_tick_b = e_ctx.tick_sizes.contains_key(&key_b);
        if !has_tick_a || !has_tick_b {
            if worth_diag {
                tracing::debug!(
                    "[scanner] REJECT {} spread={:.4}% -> missing tick_size (a={} b={})",
                    row.pair,
                    row.spread,
                    has_tick_a,
                    has_tick_b
                );
            }
            continue;
        }

        let tick_a = e_ctx.tick_sizes.get(&key_a).map(|v| *v).unwrap_or(0.000001);
        let tick_b = e_ctx.tick_sizes.get(&key_b).map(|v| *v).unwrap_or(0.000001);

        let min_age_secs = ts.min_listing_age_days * 24 * 3600;
        let final_oldest_elapsed = crate::engine::scanner_common::check_listing_age(
            &row.exchange_a, &row.exchange_b, &row.pair, e_ctx, now_s,
        );

        if final_oldest_elapsed < min_age_secs {
            if worth_diag {
                tracing::debug!(
                    "[scanner] REJECT {} ({} vs {}) spread={:.4}% -> listing too young",
                    row.pair,
                    row.exchange_a,
                    row.exchange_b,
                    row.spread
                );
            }
            continue;
        }

        let fee_a = get_exchange_fee(&row.exchange_a, &row.pair, ts.default_fee_rate, e_ctx);
        let fee_b = get_exchange_fee(&row.exchange_b, &row.pair, ts.default_fee_rate, e_ctx);
        let break_even_pct = crate::engine::scanner_common::ioc_break_even_pct(fee_a, fee_b);

        // (Qty basis is now handled universally by sending phys_amount to exchange connectors)

        // ── PROFILE EVALUATION ──
        for profile in profiles {
            if !profile.is_active {
                continue;
            }
            if profile.disabled_exchanges.contains(&row.exchange_a)
                || profile.disabled_exchanges.contains(&row.exchange_b)
            {
                continue;
            }
            // Min/max entry spread is the raw executable spread shown in the UI.
            // Structural deviation is an independent filter with its own off switch.
            if row.spread < profile.min_entry_spread || row.spread > profile.max_entry_spread {
                continue;
            }
            // Intentional product rule: listing age is treated as token age from the
            // oldest known listing venue, but liquidity threshold is profile-specific.
            if row.vol_a < profile.min_volume_millions * 1_000_000.0
                || row.vol_b < profile.min_volume_millions * 1_000_000.0
            {
                continue;
            }

            let diag_prefix = if log_diag && row.spread >= profile.min_entry_spread {
                Some(format!(
                    "[scanner] REJECT {} ({} vs {}) {}={:.4}%",
                    row.pair, row.exchange_a, row.exchange_b, profile.id, row.spread
                ))
            } else {
                None
            };

            let stable_regime =
                crate::engine::scanner_common::directional_structural_regime_is_stable(
                    row.structural_verified,
                    row.has_structural_spread,
                    row.structural_min,
                    row.structural_max,
                );
            let structural_reserve = match stable_regime.then(|| {
                crate::engine::scanner_common::configured_structural_reserve(
                    profile.min_structural_deviation,
                    row.structural_verified,
                    row.structural_spread,
                )
            }) {
                Some(Some(value)) => value,
                _ => {
                    structural_rejections += 1;
                    if let Some(p) = &diag_prefix {
                        tracing::debug!(
                            "{} -> unstable structural regime (verified={}, persistent={}, range={:.4}%..{:.4}%)",
                            p,
                            row.structural_verified,
                            row.has_structural_spread,
                            row.structural_min,
                            row.structural_max,
                        );
                    }
                    continue;
                }
            };
            let structural_deviation = row.spread - structural_reserve - break_even_pct;

            // The configured deviation is net of the verified structural
            // baseline and round-trip costs; it is not a replacement for fees.
            if profile.min_structural_deviation > 0.0
                && structural_deviation < profile.min_structural_deviation
            {
                structural_rejections += 1;
                if let Some(p) = &diag_prefix {
                    tracing::info!(
                        "{} -> net ema deviation {:.4}% < {:.4}% req (base: {:.4}%, costs: {:.4}%)",
                        p,
                        structural_deviation,
                        profile.min_structural_deviation,
                        row.structural_spread,
                        break_even_pct,
                    );
                }
                continue;
            }

            // ── ABSOLUTE STRUCTURAL GUARD (ALWAYS ACTIVE) ──
            // Blocks entry when spread - structural_spread < break_even (fees).
            // structural_spread is the EMA baseline — what the spread will converge TO.
            // If structural_spread ≈ 0 (no premium), this simply checks spread > BE.
            // If structural_spread is high (persistent premium), this blocks entry
            // because the spread will never converge enough to cover fees.
            {
                let profit_potential = row.spread - structural_reserve - break_even_pct;
                if profit_potential <= 0.0 {
                    structural_rejections += 1;
                    if let Some(p) = &diag_prefix {
                        tracing::debug!("{} -> structural_guard: profit impossible (spread={:.4}% - structural={:.4}% - BE={:.4}% = {:.4}%)",
                            p, row.spread, structural_reserve, break_even_pct, profit_potential);
                    }
                    continue;
                }
            }

            // ENSURE PERFECT PHYSICAL BALANCE (0.0% COIN DUST)
            let lot = match crate::engine::scanner_common::compute_lot_size(
                profile.trade_volume, entry_a, mult_a, mult_b, ts.vol_tolerance_pct,
            ) {
                Some(l) => l,
                None => {
                    lot_grid_rejections += 1;
                    if let Some(p) = &diag_prefix {
                        tracing::info!("{} -> physically impossible to balance lot sizes", p);
                    }
                    continue;
                }
            };

            let phys_qty = lot.phys_qty;
            let contracts_a = lot.contracts_a;
            let contracts_b = lot.contracts_b;

            // The configured liquidity cushion must be present cumulatively in
            // executable L2, not merely at a possibly misleading top level.
            if let Some(min_depth_usd) = crate::engine::scanner_common::required_depth_usd(
                profile.trade_volume,
                profile.min_depth_percent,
            ) {
                let reserve_qty = min_depth_usd / entry_a;
                if executable_l2_spread(
                    e_ctx,
                    &key_a,
                    &key_b,
                    is_a_higher,
                    reserve_qty,
                    now,
                )
                .is_none()
                {
                    depth_rejections += 1;
                    if let Some(p) = &diag_prefix {
                        tracing::info!(
                            "{} -> cumulative L2 depth cannot cover ${:.2}",
                            p,
                            min_depth_usd,
                        );
                    }
                    continue;
                }
            }

            let (entry_a, entry_b, executable_spread) = match executable_l2_spread(
                e_ctx,
                &key_a,
                &key_b,
                is_a_higher,
                phys_qty,
                now,
            ) {
                Some(quote) => quote,
                None => {
                    depth_rejections += 1;
                    if let Some(p) = &diag_prefix {
                        tracing::info!("{} -> no full-quantity executable L2 quote", p);
                    }
                    continue;
                }
            };
            if crate::engine::scanner_common::entry_execution_budget(
                executable_spread,
                profile.min_entry_spread,
                profile.max_entry_spread,
                structural_reserve,
                profile.min_structural_deviation,
                break_even_pct,
            )
            .is_none()
            {
                l2_budget_rejections += 1;
                if let Some(p) = &diag_prefix {
                    let required = profile.min_entry_spread.max(
                        structural_reserve + break_even_pct + profile.min_structural_deviation,
                    );
                    tracing::info!(
                        "{} -> L2 VWAP spread {:.4}% below executable budget {:.4}% (L1 {:.4}%)",
                        p,
                        executable_spread,
                        required,
                        row.spread,
                    );
                }
                continue;
            }

            if profile.hard_stop_percent > 0.0 {
                let initial_net_pct = initial_l2_liquidation_net_pct(
                    e_ctx,
                    &key_a,
                    &key_b,
                    is_a_higher,
                    phys_qty,
                    fee_a,
                    fee_b,
                    now,
                );
                if !initial_net_pct.is_some_and(|value| {
                    crate::engine::scanner_common::hard_stop_entry_is_compatible(
                        value,
                        profile.hard_stop_percent,
                        profile.slippage_percent,
                    )
                }) {
                    hard_stop_compatibility_rejections += 1;
                    if let Some(p) = &diag_prefix {
                        tracing::info!(
                            "{} -> initial liquidation net {:?}% conflicts with {:.4}% hard stop",
                            p,
                            initial_net_pct.map(|value| (value * 10_000.0).round() / 10_000.0),
                            profile.hard_stop_percent,
                        );
                    }
                    continue;
                }
            }

            let actual_vol_a = phys_qty * entry_a;
            let actual_vol_b = phys_qty * entry_b;
            if !crate::engine::scanner_common::volumes_within_tolerance(
                profile.trade_volume,
                ts.vol_tolerance_pct,
                actual_vol_a,
                actual_vol_b,
            ) {
                volume_rejections += 1;
                let min_v =
                    profile.trade_volume * (1.0 - ts.vol_tolerance_pct / 100.0).max(0.0);
                let max_v = profile.trade_volume * (1.0 + ts.vol_tolerance_pct / 100.0);
                if let Some(p) = &diag_prefix {
                    tracing::info!(
                        "{} -> actual volume outside configured range: A=${:.2} B=${:.2}, allowed=${:.2}..${:.2}",
                        p,
                        actual_vol_a,
                        actual_vol_b,
                        min_v,
                        max_v
                    );
                }
                continue;
            }

            // Universal minimum notional check — all major CEXes reject orders below ~$5
            let min_notional = ts.binance_min_notional; // reuse setting as universal floor
            if actual_vol_a < min_notional || actual_vol_b < min_notional {
                volume_rejections += 1;
                if let Some(p) = &diag_prefix {
                    tracing::info!(
                        "{} -> below min_notional (A=${:.2} B=${:.2} < ${:.2})",
                        p,
                        actual_vol_a,
                        actual_vol_b,
                        min_notional
                    );
                }
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
                spread: executable_spread,
                // Carry only the baseline that this profile explicitly enabled.
                structural_spread: structural_reserve,
                is_a_higher,

                tick_a,
                tick_b,
                mult_a,
                mult_b,
            });
        }
    }
    // Rank by executable net edge before retaining one route per profile/pair.
    let signals_before_pair_dedupe = signals.len() as u64;
    signals.sort_by(|a, b| {
        let edge_a = a.spread - a.structural_spread - a.break_even_pct;
        let edge_b = b.spread - b.structural_spread - b.break_even_pct;
        edge_b.total_cmp(&edge_a)
    });
    signals.retain(|signal| emitted_pairs.insert(format!("{}:{}", signal.profile_id, signal.pair)));
    let confirmations = ENTRY_CONFIRMATIONS.get_or_init(DashMap::new);
    let mut observed_confirmation_keys = HashSet::new();
    let mut signals_awaiting_confirmation = 0u64;
    signals.retain(|signal| {
        let (route_key, sell_book_key, buy_book_key) = confirmation_route(signal);
        observed_confirmation_keys.insert(route_key.clone());
        let Some((sell_book_ms, buy_book_ms)) = e_ctx
            .l2_books
            .get(&sell_book_key)
            .map(|book| book.updated_ms)
            .zip(e_ctx.l2_books.get(&buy_book_key).map(|book| book.updated_ms))
        else {
            confirmations.remove(&route_key);
            return false;
        };
        let previous = confirmations.get(&route_key).map(|state| *state);
        let (next, ready) = advance_entry_confirmation(
            previous,
            now,
            signal.spread,
            sell_book_ms,
            buy_book_ms,
        );
        if ready {
            confirmations.remove(&route_key);
            let throttle = signal_throttle();
            let last = throttle.get(&route_key).map(|value| *value).unwrap_or(0);
            if now - last > 5_000 {
                throttle.insert(route_key, now);
                tracing::info!(
                    "[scanner] CONFIRMED SIGNAL {}: {} vs {} L2_VWAP={:.4}% BE={:.4}% after {}ms",
                    signal.pair,
                    signal.exchange_a,
                    signal.exchange_b,
                    signal.spread,
                    signal.break_even_pct,
                    ENTRY_CONFIRMATION_MS,
                );
            }
        } else {
            confirmations.insert(route_key, next);
            signals_awaiting_confirmation += 1;
        }
        ready
    });
    confirmations.retain(|key, _| observed_confirmation_keys.contains(key));
    let considered = spreads.len().min(ts.max_spreads_per_cycle.max(0) as usize);
    let profile_window_rows = spreads
        .iter()
        .take(considered)
        .filter(|row| {
            profiles.iter().any(|profile| {
                profile.is_active
                    && !profile.disabled_exchanges.contains(&row.exchange_a)
                    && !profile.disabled_exchanges.contains(&row.exchange_b)
                    && row.spread >= profile.min_entry_spread
                    && row.spread <= profile.max_entry_spread
                    && row.vol_a >= profile.min_volume_millions * 1_000_000.0
                    && row.vol_b >= profile.min_volume_millions * 1_000_000.0
            })
        })
        .count();
    // Candidate-depth fetches finish asynchronously and publish their own
    // cumulative diagnostics into this map. Keep only those external metrics
    // while replacing every scanner-owned value below; a wholesale clear can
    // otherwise erase a completed prewarm between its cache update and the
    // next `/health` sample.
    e_ctx
        .scanner_status
        .retain(|key, _| {
            key.starts_with("kucoin_depth_prewarm_")
                || key.ends_with("_observations_total")
        });
    let cycle_metrics = [
        ("updated_ms", now.max(0) as u64),
        ("spreads_available", spreads.len() as u64),
        ("spreads_considered", considered as u64),
        (
            "active_profiles",
            profiles.iter().filter(|profile| profile.is_active).count() as u64,
        ),
        ("profile_window_rows", profile_window_rows as u64),
        ("signals_emitted", signals.len() as u64),
        ("signals_before_pair_dedupe", signals_before_pair_dedupe),
        ("signals_awaiting_confirmation", signals_awaiting_confirmation),
        ("l2_pending_rows", l2_pending_rows),
        ("profile_l2_pending_rows", profile_l2_pending_rows),
        ("structural_rejections", structural_rejections),
        ("lot_grid_rejections", lot_grid_rejections),
        ("depth_rejections", depth_rejections),
        ("l2_budget_rejections", l2_budget_rejections),
        (
            "hard_stop_compatibility_rejections",
            hard_stop_compatibility_rejections,
        ),
        ("volume_rejections", volume_rejections),
    ];
    for (key, value) in cycle_metrics {
        e_ctx.scanner_status.insert(key.to_string(), value);
    }
    for (key, value) in [
        ("profile_window_rows", profile_window_rows as u64),
        ("profile_l2_pending_rows", profile_l2_pending_rows),
        ("signals_emitted", signals.len() as u64),
        ("structural_rejections", structural_rejections),
        ("lot_grid_rejections", lot_grid_rejections),
        ("depth_rejections", depth_rejections),
        ("l2_budget_rejections", l2_budget_rejections),
        (
            "hard_stop_compatibility_rejections",
            hard_stop_compatibility_rejections,
        ),
        ("volume_rejections", volume_rejections),
    ] {
        if value == 0 {
            continue;
        }
        let total_key = format!("{key}_observations_total");
        e_ctx
            .scanner_status
            .entry(total_key)
            .and_modify(|total| *total = total.saturating_add(value))
            .or_insert(value);
    }
    signals
}

#[cfg(test)]
mod l2_gate_tests {
    use super::*;
    use crate::state::L2Snapshot;

    fn book(updated_ms: i64) -> L2Snapshot {
        L2Snapshot {
            bids: vec![(100.0, 1.0)],
            asks: vec![(101.0, 1.0)],
            updated_ms,
        }
    }

    #[test]
    fn pending_profile_log_is_immediate_and_route_throttled() {
        let route = "test-profile:TESTUSDT:binance:kucoin";
        assert!(should_log_l2_pending(route, 10_000));
        assert!(!should_log_l2_pending(route, 19_999));
        assert!(should_log_l2_pending(route, 20_000));
    }

    #[test]
    fn entry_confirmation_is_non_blocking_and_requires_both_books_to_advance() {
        let (initial, ready) = advance_entry_confirmation(None, 1_000, 1.0, 990, 995);
        assert!(!ready);

        let (_, ready) = advance_entry_confirmation(Some(initial), 1_600, 0.9, 1_100, 995);
        assert!(!ready, "one unchanged venue book cannot confirm entry");

        let (_, ready) = advance_entry_confirmation(Some(initial), 1_499, 0.9, 1_100, 1_105);
        assert!(!ready, "fresh books cannot bypass the duration guard");

        let (confirmed, ready) =
            advance_entry_confirmation(Some(initial), 1_500, 0.9, 1_100, 1_105);
        assert!(ready);
        assert_eq!(confirmed, initial);
    }

    #[test]
    fn widening_at_the_confirmation_boundary_resets_the_window() {
        let (initial, _) = advance_entry_confirmation(None, 1_000, 1.0, 990, 995);
        let (before_boundary, ready) =
            advance_entry_confirmation(Some(initial), 1_400, 1.1, 1_100, 1_105);
        assert!(!ready);
        assert_eq!(before_boundary, initial);

        let (reset, ready) =
            advance_entry_confirmation(Some(before_boundary), 1_500, 1.1, 1_200, 1_205);
        assert!(!ready);
        assert_eq!(reset.first_seen_ms, 1_500);
        assert_eq!(reset.initial_spread, 1.1);

        let (_, ready) = advance_entry_confirmation(Some(reset), 1_999, 1.0, 1_300, 1_305);
        assert!(!ready);
        let (_, ready) = advance_entry_confirmation(Some(reset), 2_000, 1.0, 1_300, 1_305);
        assert!(ready);
    }

    #[test]
    fn signals_wait_for_two_fresh_execution_books() {
        let ctx = EngineContext::default();
        let now = 10_000;
        assert!(!execution_books_are_fresh(
            &ctx,
            "binance:TESTUSDT",
            "kucoin:TESTUSDT",
            now,
        ));

        ctx.l2_books
            .insert("binance:TESTUSDT".into(), book(now - 750));
        ctx.l2_books
            .insert("kucoin:TESTUSDT".into(), book(now + 1));
        assert!(!execution_books_are_fresh(
            &ctx,
            "binance:TESTUSDT",
            "kucoin:TESTUSDT",
            now,
        ));

        ctx.l2_books
            .insert("binance:TESTUSDT".into(), book(now - 249));
        ctx.l2_books
            .insert("kucoin:TESTUSDT".into(), book(now));
        assert!(execution_books_are_fresh(
            &ctx,
            "binance:TESTUSDT",
            "kucoin:TESTUSDT",
            now,
        ));

        ctx.l2_books
            .insert("binance:TESTUSDT".into(), book(now - 251));
        assert!(!execution_books_are_fresh(
            &ctx,
            "binance:TESTUSDT",
            "kucoin:TESTUSDT",
            now,
        ));

        ctx.l2_books
            .insert("kucoin:TESTUSDT".into(), book(now + 1_001));
        assert!(!execution_books_are_fresh(
            &ctx,
            "binance:TESTUSDT",
            "kucoin:TESTUSDT",
            now,
        ));

        ctx.l2_books
            .insert("binance:TESTUSDT".into(), book(now));
        ctx.l2_books
            .insert("kucoin:TESTUSDT".into(), book(now));
        assert!(execution_books_are_fresh(
            &ctx,
            "binance:TESTUSDT",
            "kucoin:TESTUSDT",
            now,
        ));

        ctx.l2_books
            .insert("binance:TESTUSDT".into(), book(now - 751));
        assert!(!execution_books_are_fresh(
            &ctx,
            "binance:TESTUSDT",
            "kucoin:TESTUSDT",
            now,
        ));
    }

    #[test]
    fn executable_spread_uses_full_depth_instead_of_top_quotes() {
        let ctx = EngineContext::default();
        let now = 10_000;
        ctx.l2_books.insert(
            "binance:TESTUSDT".into(),
            L2Snapshot {
                bids: vec![(110.0, 0.02), (99.0, 1.0)],
                asks: vec![(111.0, 1.0)],
                updated_ms: now,
            },
        );
        ctx.l2_books.insert(
            "kucoin:TESTUSDT".into(),
            L2Snapshot {
                bids: vec![(98.0, 1.0)],
                asks: vec![(100.0, 0.02), (101.0, 1.0)],
                updated_ms: now,
            },
        );

        let (sell_a, buy_b, spread) = executable_l2_spread(
            &ctx,
            "binance:TESTUSDT",
            "kucoin:TESTUSDT",
            true,
            1.0,
            now,
        )
        .unwrap();
        assert!(sell_a < buy_b);
        assert!(spread < 0.0, "full-depth spread must reject fake L1 edge");
        assert!(executable_l2_spread(
            &ctx,
            "binance:TESTUSDT",
            "kucoin:TESTUSDT",
            true,
            2.0,
            now,
        )
        .is_none());
    }

    #[test]
    fn round_trip_l2_quote_exposes_immediate_hard_stop_conflict() {
        let ctx = EngineContext::default();
        let now = 10_000;
        ctx.l2_books.insert(
            "binance:TESTUSDT".into(),
            L2Snapshot {
                bids: vec![(101.0, 2.0)],
                asks: vec![(101.2, 2.0)],
                updated_ms: now,
            },
        );
        ctx.l2_books.insert(
            "kucoin:TESTUSDT".into(),
            L2Snapshot {
                bids: vec![(99.8, 2.0)],
                asks: vec![(100.0, 2.0)],
                updated_ms: now,
            },
        );
        let immediate = initial_l2_liquidation_net_pct(
            &ctx,
            "binance:TESTUSDT",
            "kucoin:TESTUSDT",
            true,
            1.0,
            0.0005,
            0.0005,
            now,
        )
        .unwrap();
        assert!(immediate < -0.4);
        assert!(!crate::engine::scanner_common::hard_stop_entry_is_compatible(
            immediate, 0.4, 0.05
        ));

        ctx.l2_books.insert(
            "binance:TESTUSDT".into(),
            L2Snapshot {
                bids: vec![(101.0, 2.0)],
                asks: vec![(101.02, 2.0)],
                updated_ms: now,
            },
        );
        ctx.l2_books.insert(
            "kucoin:TESTUSDT".into(),
            L2Snapshot {
                bids: vec![(99.98, 2.0)],
                asks: vec![(100.0, 2.0)],
                updated_ms: now,
            },
        );
        let tight = initial_l2_liquidation_net_pct(
            &ctx,
            "binance:TESTUSDT",
            "kucoin:TESTUSDT",
            true,
            1.0,
            0.0005,
            0.0005,
            now,
        )
        .unwrap();
        assert!(crate::engine::scanner_common::hard_stop_entry_is_compatible(
            tight, 0.4, 0.05
        ));
    }
}
