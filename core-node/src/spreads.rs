//! Spread computation — ported from rust-collector, runs in-process.
//! No HTTP serialization overhead; reads PriceMap directly from memory.

use crate::state::PriceMap;
use dashmap::{DashMap, DashSet};
use shared::{SpreadRow, TechnicalSettings};
use std::collections::HashMap;
use std::sync::OnceLock;

/// (ema_ab, ema_ba, max_ab, min_ab, max_ba, min_ba, last_cross_ab, last_cross_ba, last_ts_secs)
static STRUCTURAL_EMA: OnceLock<DashMap<String, (f64, f64, f64, f64, f64, f64, f64, f64, f64)>> =
    OnceLock::new();

/// Set of EMA keys that have been verified from 24h historical warmup data.
/// Pairs NOT in this set are "on probation" — entry is blocked.
static VERIFIED_KEYS: OnceLock<DashSet<String>> = OnceLock::new();

/// Tracks when each pair (ema_key) was FIRST observed with a valid spread.
/// Used for auto-verification: verify if pair has had consistent spread for 5+ min.
/// Independent of zero-cross timer so WS reconnects don't reset the clock.
static FIRST_SEEN: OnceLock<DashMap<String, f64>> = OnceLock::new();

/// Compute spread opportunities from PriceMap.
/// Called directly by the engine loop — zero serialization/HTTP overhead.
///
/// Snapshots the entire DashMap into plain HashMaps first to minimize
/// lock hold time and avoid blocking WS collector writes.
pub fn compute_spreads(
    price_map: &PriceMap,
    min_vol_m: f64,
    ts: &TechnicalSettings,
) -> Vec<SpreadRow> {
    use std::sync::atomic::{AtomicI64, Ordering};
    static LAST_DIAG: AtomicI64 = AtomicI64::new(0);

    let now_f64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let now_secs = now_f64 as u64;
    let now_ms = (now_f64 * 1000.0) as i64;

    // Diagnostic logging — throttled to once per 30s
    let last_d = LAST_DIAG.load(Ordering::Relaxed);
    let log_diag = (now_ms - last_d) > 30_000;
    if log_diag {
        LAST_DIAG.store(now_ms, Ordering::Relaxed);
    }

    // Filter-stage counters
    let mut cnt_total_pairs: u32 = 0;
    let mut cnt_blacklist: u32 = 0;
    let mut cnt_stale: u32 = 0;
    let mut cnt_invalid_price: u32 = 0;
    let mut cnt_intra_spread: u32 = 0;
    let mut cnt_volume: u32 = 0;
    let mut cnt_spread_range: u32 = 0;
    let mut cnt_passed: u32 = 0;

    // Snapshot: take a quick read of each exchange shard into owned HashMaps.
    // This releases the DashMap refs immediately so collectors are never blocked.
    let snapshot: Vec<(String, HashMap<String, shared::TickerData>)> = price_map
        .iter()
        .map(|entry| (entry.key().clone(), entry.value().clone()))
        .collect();

    let min_v = min_vol_m * 1_000_000.0;
    let ema_map = STRUCTURAL_EMA.get_or_init(|| DashMap::new());
    let verified = VERIFIED_KEYS.get_or_init(|| DashSet::new());
    let first_seen_map = FIRST_SEEN.get_or_init(|| DashMap::new());
    let blacklist = ts.blacklist_vec();
    let mut rows = Vec::with_capacity(512);

    for i in 0..snapshot.len() {
        for j in (i + 1)..snapshot.len() {
            let (name_a, map_a) = &snapshot[i];
            let (name_b, map_b) = &snapshot[j];

            for (sym, data_a) in map_a.iter() {
                let data_b = match map_b.get(sym.as_str()) {
                    Some(d) => d,
                    None => continue,
                };

                cnt_total_pairs += 1;

                if blacklist.iter().any(|bl| sym.eq_ignore_ascii_case(bl)) {
                    cnt_blacklist += 1;
                    continue;
                }

                if data_a.last_updated > 0
                    && now_secs.saturating_sub(data_a.last_updated) > ts.stale_secs
                {
                    cnt_stale += 1;
                    continue;
                }
                if data_b.last_updated > 0
                    && now_secs.saturating_sub(data_b.last_updated) > ts.stale_secs
                {
                    cnt_stale += 1;
                    continue;
                }

                if data_a.bid <= 0.0 || data_a.ask <= 0.0 || data_b.bid <= 0.0 || data_b.ask <= 0.0
                {
                    cnt_invalid_price += 1;
                    continue;
                }
                if data_a.ask > data_a.bid * ts.intra_spread_limit
                    || data_b.ask > data_b.bid * ts.intra_spread_limit
                {
                    cnt_intra_spread += 1;
                    continue;
                }

                if data_a.vol <= 0.0
                    || data_a.vol < min_v
                    || data_b.vol <= 0.0
                    || data_b.vol < min_v
                {
                    cnt_volume += 1;
                    continue;
                }

                // Removed max_funding_diff check per user request

                // ── DIRECTIONAL ARBITRAGE SPREAD ──
                let a_over_b = (data_a.bid - data_b.ask) / data_b.ask * 100.0;
                let b_over_a = (data_b.bid - data_a.ask) / data_a.ask * 100.0;
                let (spread, direction) = if a_over_b >= b_over_a {
                    (a_over_b, format!("{name_a}>{name_b}"))
                } else {
                    (b_over_a, format!("{name_b}>{name_a}"))
                };

                if spread < ts.spread_threshold || spread > ts.spread_max {
                    cnt_spread_range += 1;
                    continue;
                }

                cnt_passed += 1;

                // ── STRUCTURAL SPREAD (MID-PRICE EMA) ──
                let is_a_first = name_a < name_b;
                let (name1, name2) = if is_a_first {
                    (name_a.clone(), name_b.clone())
                } else {
                    (name_b.clone(), name_a.clone())
                };
                // mid1 corresponds to name1, mid2 corresponds to name2
                let (data1, data2) = if is_a_first {
                    (&data_a, &data_b)
                } else {
                    (&data_b, &data_a)
                };

                let mid1 = (data1.bid + data1.ask) / 2.0;
                let mid2 = (data2.bid + data2.ask) / 2.0;

                // Track mathematically symmetric directional premiums based on Mid price
                let premium_1_over_2 = (mid1 - mid2) / mid2 * 100.0;
                let premium_2_over_1 = (mid2 - mid1) / mid1 * 100.0;

                let ema_key = format!("{}|{}|{}", sym, name1, name2);
                let is_verified = verified.contains(&ema_key);

                let (ema_12, ema_21, max_12, min_12, max_21, min_21, cross_12, cross_21) =
                    if let Some(mut entry) = ema_map.get_mut(&ema_key) {
                        let (
                            old_12,
                            old_21,
                            old_max_12,
                            old_min_12,
                            old_max_21,
                            old_min_21,
                            mut old_cross_12,
                            mut old_cross_21,
                            last_updated,
                        ) = *entry;
                        let dt = (now_f64 - last_updated).max(0.0);
                        let alpha = if dt > 0.0 {
                            1.0 - (-dt / ts.ema_tau_secs).exp()
                        } else {
                            0.0
                        };

                        let new_12 = old_12 + alpha * (premium_1_over_2 - old_12);
                        let new_21 = old_21 + alpha * (premium_2_over_1 - old_21);

                        let new_max_12 = old_max_12.max(premium_1_over_2);
                        let new_min_12 = old_min_12.min(premium_1_over_2);
                        let new_max_21 = old_max_21.max(premium_2_over_1);
                        let new_min_21 = old_min_21.min(premium_2_over_1);

                        if premium_1_over_2 <= 0.0 {
                            old_cross_12 = now_f64;
                        }
                        if premium_2_over_1 <= 0.0 {
                            old_cross_21 = now_f64;
                        }

                        *entry = (
                            new_12,
                            new_21,
                            new_max_12,
                            new_min_12,
                            new_max_21,
                            new_min_21,
                            old_cross_12,
                            old_cross_21,
                            now_f64,
                        );
                        (
                            new_12,
                            new_21,
                            new_max_12,
                            new_min_12,
                            new_max_21,
                            new_min_21,
                            old_cross_12,
                            old_cross_21,
                        )
                    } else {
                        // Default cross to NOW — conservative: assume NO structural spread
                        // until proven by 24h of no zero-crossing.
                        let cross_12 = now_f64;
                        let cross_21 = now_f64;
                        ema_map.insert(
                            ema_key.clone(),
                            (
                                premium_1_over_2,
                                premium_2_over_1,
                                premium_1_over_2,
                                premium_1_over_2,
                                premium_2_over_1,
                                premium_2_over_1,
                                cross_12,
                                cross_21,
                                now_f64,
                            ),
                        );
                        (
                            premium_1_over_2,
                            premium_2_over_1,
                            premium_1_over_2,
                            premium_1_over_2,
                            premium_2_over_1,
                            premium_2_over_1,
                            cross_12,
                            cross_21,
                        )
                    };

                // Map values depending on trade direction.
                // name1 is ALWAYS alphabetically first. name2 is ALWAYS alphabetically second.
                let is_dir_1_over_2 = direction == format!("{name1}>{name2}");

                let (
                    ema_mid_spread,
                    current_mid_spread,
                    structural_max,
                    structural_min,
                    last_cross,
                ) = if is_dir_1_over_2 {
                    (ema_12, premium_1_over_2, max_12, min_12, cross_12)
                } else {
                    (ema_21, premium_2_over_1, max_21, min_21, cross_21)
                };

                // Isolating the penalty: calculate the pure deviation mathematically.
                // mid_deviation = Current Mid Premium - Historical Mid EMA.
                let mid_deviation = current_mid_spread - ema_mid_spread;

                // Create a "Synthetic Base Executable Spread" such that:
                // spread - structural_spread = mid_deviation.
                // Therefore, structural_spread = spread - mid_deviation.
                // This tracks the historical behavior of the actual executable spread cleanly!
                let structural_spread = spread - mid_deviation;

                let has_structural_spread = (now_f64 - last_cross) > 86000.0;

                // ── AUTO-VERIFY via first_seen timer ──────────────────────────
                // Pairs without historical candle data (new listings, API rate limits)
                // are auto-verified once they've been observed for 5+ minutes AND the
                // EMA mid-spread is still confirming the premium (> 0.1%).
                //
                // Uses FIRST_SEEN (not last_cross) so Bybit/BingX WS reconnects can't
                // reset the clock — stale prices briefly crossing zero must not block.
                if !is_verified {
                    first_seen_map.entry(ema_key.clone()).or_insert(now_f64);
                    let first_ts = *first_seen_map.get(&ema_key).unwrap();
                    let observation_age = now_f64 - first_ts;
                    // Auto-verify: 2min observation (historical warmup fallback)
                    if observation_age > 120.0 {
                        let verified = VERIFIED_KEYS.get_or_init(|| DashSet::new());
                        verified.insert(ema_key.clone());
                        tracing::info!(
                            "[spreads] Auto-verified {} after {:.0}s observation, EMA={:.3}%",
                            ema_key, observation_age, ema_mid_spread
                        );
                    }
                }
                let is_verified = verified.contains(&ema_key);

                rows.push(SpreadRow {
                    pair: sym.clone(),
                    exchange_a: name_a.clone(),
                    exchange_b: name_b.clone(),
                    price_a: data_a.price,
                    price_b: data_b.price,
                    bid_a: data_a.bid,
                    ask_a: data_a.ask,
                    bid_b: data_b.bid,
                    ask_b: data_b.ask,
                    vol_a: data_a.vol,
                    vol_b: data_b.vol,
                    funding_a: data_a.funding_rate,
                    funding_b: data_b.funding_rate,
                    structural_spread: (structural_spread * 10000.0).round() / 10000.0,
                    structural_max,
                    structural_min,
                    spread: (spread * 10000.0).round() / 10000.0,
                    direction,
                    has_structural_spread,
                    structural_verified: is_verified,
                });
            }
        }
    }

    // ── PERIODIC DIAGNOSTICS ──
    if log_diag {
        // Per-exchange freshness: count non-stale tickers with valid bid/ask
        let mut ex_fresh: Vec<(&str, usize, usize)> = Vec::new();
        for (name, map) in &snapshot {
            let total = map.len();
            let fresh = map.values().filter(|d| {
                d.bid > 0.0 && d.ask > 0.0
                    && (d.last_updated == 0 || now_secs.saturating_sub(d.last_updated) <= ts.stale_secs)
            }).count();
            ex_fresh.push((name.as_str(), fresh, total));
        }
        ex_fresh.sort_by(|a, b| b.1.cmp(&a.1));
        let ex_summary: String = ex_fresh.iter()
            .map(|(n, f, t)| format!("{}:{}/{}", n, f, t))
            .collect::<Vec<_>>()
            .join(" ");

        tracing::info!(
            "[spreads] DIAG: exchanges={} total_pairs={} → blacklist={} stale={} invalid_price={} intra_spread={} volume={} spread_range={} → PASSED={} | min_vol={:.1}M spread_thr={:.2} stale={}s",
            snapshot.len(),
            cnt_total_pairs,
            cnt_blacklist,
            cnt_stale,
            cnt_invalid_price,
            cnt_intra_spread,
            cnt_volume,
            cnt_spread_range,
            cnt_passed,
            min_vol_m,
            ts.spread_threshold,
            ts.stale_secs,
        );
        tracing::info!("[spreads] FRESH: {}", ex_summary);
    }

    // Partial sort: we only need top spreads, but full sort is fine for ~1-5k rows
    rows.sort_unstable_by(|a, b| {
        b.spread
            .partial_cmp(&a.spread)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

/// Pre-seed the STRUCTURAL_EMA from historical canonical premiums.
/// Tuple: (timestamp_secs, premium_12, premium_21, pair, ex1_canonical, ex2_canonical)
/// All keys seeded here are marked as VERIFIED — they have real 24h history.
pub fn warmup_ema_from_premiums(
    history: &[(f64, f64, f64, String, String, String)],
    ts: &TechnicalSettings,
) {
    let ema_map = STRUCTURAL_EMA.get_or_init(|| DashMap::new());
    let verified = VERIFIED_KEYS.get_or_init(|| DashSet::new());
    let tau = ts.ema_tau_secs;

    let mut sorted = history.to_vec();
    sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    for (ts_val, premium_12, premium_21, pair, name1, name2) in &sorted {
        let key = format!("{}|{}|{}", pair, name1, name2);
        // Default cross to first candle ts (not 0.0) — prevents false has_structural_spread
        let mut entry = ema_map.entry(key.clone()).or_insert((
            *premium_12,
            *premium_21,
            *premium_12,
            *premium_12,
            *premium_21,
            *premium_21,
            *ts_val,
            *ts_val,
            *ts_val,
        ));
        let (
            old_12,
            old_21,
            old_max_12,
            old_min_12,
            old_max_21,
            old_min_21,
            mut old_cross_12,
            mut old_cross_21,
            last_ts,
        ) = *entry;
        let dt = (ts_val - last_ts).max(0.0);
        let alpha = if dt > 0.0 {
            1.0 - (-dt / tau).exp()
        } else {
            0.0
        };

        let new_12 = old_12 + alpha * (premium_12 - old_12);
        let new_21 = old_21 + alpha * (premium_21 - old_21);

        let new_max_12 = old_max_12.max(*premium_12);
        let new_min_12 = old_min_12.min(*premium_12);
        let new_max_21 = old_max_21.max(*premium_21);
        let new_min_21 = old_min_21.min(*premium_21);

        if *premium_12 <= 0.0 {
            old_cross_12 = *ts_val;
        }
        if *premium_21 <= 0.0 {
            old_cross_21 = *ts_val;
        }

        *entry = (
            new_12,
            new_21,
            new_max_12,
            new_min_12,
            new_max_21,
            new_min_21,
            old_cross_12,
            old_cross_21,
            *ts_val,
        );

        // Mark this pair as VERIFIED — it has real historical data
        verified.insert(key);
    }
}
/// Returns all EMA keys that are being tracked but not yet verified.
/// Used by the background re-warmup task to retry candle fetches for new listings.
pub fn unverified_ema_keys() -> Vec<String> {
    let ema_map = STRUCTURAL_EMA.get_or_init(|| DashMap::new());
    let verified = VERIFIED_KEYS.get_or_init(|| DashSet::new());
    ema_map
        .iter()
        .filter_map(|e| {
            let key = e.key().clone();
            if verified.contains(&key) { None } else { Some(key) }
        })
        .collect()
}
