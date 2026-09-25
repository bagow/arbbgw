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

static LAST_DIAG_LOG: AtomicI64 = AtomicI64::new(0);

fn execution_books_are_fresh(
    e_ctx: &EngineContext,
    key_a: &str,
    key_b: &str,
    now_ms: i64,
) -> bool {
    [key_a, key_b].iter().all(|key| {
        e_ctx.l2_books.get(*key).is_some_and(|book| {
            let top = book.bids.first().zip(book.asks.first());
            local_timestamp_is_fresh(now_ms, book.updated_ms, 750)
                && top.is_some_and(|(bid, ask)| {
                    [bid.0, bid.1, ask.0, ask.1]
                        .iter()
                        .all(|value| value.is_finite() && *value > 0.0)
                        && bid.0 < ask.0
                })
        })
    })
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
        let worth_diag = log_diag
            && profiles.iter().any(|p| {
                p.is_active && row.spread >= p.min_entry_spread && row.spread <= p.max_entry_spread
            });

        // Discovery quotes only nominate candidates. Do not create an order
        // signal until both venues have a fresh executable-depth snapshot. In
        // particular, this gives Binance's demand-driven L2 subscription one
        // update cycle to replace an internally stale all-market book ticker.
        if !execution_books_are_fresh(e_ctx, &key_a, &key_b, now) {
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

            let structural_reserve =
                match crate::engine::scanner_common::configured_structural_reserve(
                    profile.min_structural_deviation,
                    row.structural_verified,
                    row.structural_spread,
                ) {
                    Some(value) => value,
                    None => {
                        if let Some(p) = &diag_prefix {
                            tracing::debug!(
                                "{} -> structural filter enabled but baseline is not verified",
                                p
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

            let actual_vol_a = phys_qty * entry_a;
            let actual_vol_b = phys_qty * entry_b;
            if !crate::engine::scanner_common::volumes_within_tolerance(
                profile.trade_volume,
                ts.vol_tolerance_pct,
                actual_vol_a,
                actual_vol_b,
            ) {
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
            {
                let sig_key = format!("{}:{}:{}", row.pair, row.exchange_a, row.exchange_b);
                let throttle = signal_throttle();
                let last = throttle.get(&sig_key).map(|v| *v).unwrap_or(0);
                if now - last > 5_000 {
                    throttle.insert(sig_key, now);
                    tracing::info!(
                        "[scanner] SIGNAL {}: {} vs {} L2_VWAP={:.4}% L1={:.4}% BE={:.4}% (fee_a={:.6} fee_b={:.6})",
                        row.pair, row.exchange_a, row.exchange_b, executable_spread, row.spread, break_even_pct,
                        fee_a, fee_b
                    );
                }
            }
        }
    }
    // Rank by executable net edge before retaining one route per profile/pair.
    signals.sort_by(|a, b| {
        let edge_a = a.spread - a.structural_spread - a.break_even_pct;
        let edge_b = b.spread - b.structural_spread - b.break_even_pct;
        edge_b.total_cmp(&edge_a)
    });
    signals.retain(|signal| emitted_pairs.insert(format!("{}:{}", signal.profile_id, signal.pair)));
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
    e_ctx.scanner_status.clear();
    for (key, value) in [
        ("updated_ms", now.max(0) as u64),
        ("spreads_available", spreads.len() as u64),
        ("spreads_considered", considered as u64),
        (
            "active_profiles",
            profiles.iter().filter(|profile| profile.is_active).count() as u64,
        ),
        ("profile_window_rows", profile_window_rows as u64),
        ("signals_emitted", signals.len() as u64),
    ] {
        e_ctx.scanner_status.insert(key.to_string(), value);
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
        assert!(execution_books_are_fresh(
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
}
