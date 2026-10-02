//! Spread computation — ported from rust-collector, runs in-process.
//! No HTTP serialization overhead; reads PriceMap directly from memory.

use crate::state::PriceMap;
use dashmap::{DashMap, DashSet};
use shared::{SpreadRow, TechnicalSettings, TradingSettings};
use std::collections::HashMap;
use std::sync::OnceLock;

/// Named struct replacing the previous 9-element tuple for safety and readability.
#[derive(Clone, Copy)]
struct EmaEntry {
    ema_12: f64,
    ema_21: f64,
    max_12: f64,
    min_12: f64,
    max_21: f64,
    min_21: f64,
    last_cross_12: f64,
    last_cross_21: f64,
    last_ts: f64,
}

static STRUCTURAL_EMA: OnceLock<DashMap<String, EmaEntry>> = OnceLock::new();

/// Set of EMA keys that have been verified from 24h historical warmup data.
/// Pairs NOT in this set are "on probation" — entry is blocked.
static VERIFIED_KEYS: OnceLock<DashSet<String>> = OnceLock::new();

/// Subset of VERIFIED_KEYS whose baseline was reconstructed from a complete
/// historical window. A transient live-feed gap must not revoke this evidence.
static HISTORICALLY_VERIFIED_KEYS: OnceLock<DashSet<String>> = OnceLock::new();

/// Tracks when each pair (ema_key) was FIRST observed with a valid spread.
/// Live verification requires distinct fresh observations over at least ten minutes.
/// Independent of zero-cross timer so WS reconnects don't reset the clock.
#[derive(Clone, Copy, Default)]
struct ObservationQuality {
    first: f64,
    last: f64,
    count: u64,
}
impl ObservationQuality {
    fn observe(&mut self, stamp: f64) -> bool {
        if stamp > self.last {
            if self.count == 0 || stamp - self.last > 5.0 {
                self.first = stamp;
                self.count = 0;
            }
            self.last = stamp;
            self.count += 1;
        }
        self.count >= 300 && self.last - self.first >= 600.0
    }
}
static FIRST_SEEN: OnceLock<DashMap<String, ObservationQuality>> = OnceLock::new();

/// Subscribe executable books before a raw spread reaches the trading window.
/// This is discovery-only: the scanner still enforces the profile's full entry
/// threshold and all structural, fee, freshness, and depth budgets.
pub const DISCOVERY_PREWARM_RATIO: f64 = 0.4;
const MAX_DISCOVERY_SOURCE_SKEW_MS: i64 = 3_000;

fn exact_discovery_sources_are_fresh(
    source_ms_a: i64,
    source_ms_b: i64,
    now_ms: i64,
    max_age_ms: i64,
) -> bool {
    if source_ms_a <= 0 || source_ms_b <= 0 || max_age_ms < 0 {
        return false;
    }
    let age_a = now_ms - source_ms_a;
    let age_b = now_ms - source_ms_b;
    (0..=max_age_ms).contains(&age_a)
        && (0..=max_age_ms).contains(&age_b)
        && source_ms_a.abs_diff(source_ms_b) <= MAX_DISCOVERY_SOURCE_SKEW_MS as u64
}

pub fn discovery_spread_threshold(configured_threshold: f64, profiles: &[TradingSettings]) -> f64 {
    profiles
        .iter()
        .filter(|profile| profile.is_active && profile.strategy_type == "ema")
        .filter_map(|profile| {
            let threshold = profile.min_entry_spread * DISCOVERY_PREWARM_RATIO;
            (threshold.is_finite() && threshold >= 0.0).then_some(threshold)
        })
        .fold(configured_threshold, f64::min)
}

fn reset_live_verification_after_gap(
    key: &str,
    verified: &DashSet<String>,
    historically_verified: &DashSet<String>,
    observations: &DashMap<String, ObservationQuality>,
) {
    observations.remove(key);
    if !historically_verified.contains(key) {
        verified.remove(key);
    }
}

/// Estimate the EMA represented by a finite historical window without giving
/// the oldest observation an artificial pre-history. Seeding the ordinary EMA
/// recurrence with the first candle leaves it with `exp(-window / tau)` weight
/// (36.8% for a 24h window and 24h tau), which can dominate the baseline during
/// a regime change. Normalising the exponential weights conditions the estimate
/// on the history we actually observed.
fn normalized_finite_ema(rows: &[(f64, f64, f64)], tau_secs: f64) -> Option<(f64, f64)> {
    let last = rows.last()?.0;
    if !last.is_finite() || !tau_secs.is_finite() || tau_secs <= 0.0 {
        return None;
    }
    let mut weight_sum = 0.0;
    let mut weighted_12 = 0.0;
    let mut weighted_21 = 0.0;
    for (stamp, premium_12, premium_21) in rows {
        if ![stamp, premium_12, premium_21]
            .iter()
            .all(|value| value.is_finite())
            || *stamp > last
        {
            return None;
        }
        let weight = (-((last - stamp) / tau_secs)).exp();
        weight_sum += weight;
        weighted_12 += weight * premium_12;
        weighted_21 += weight * premium_21;
    }
    (weight_sum.is_finite() && weight_sum > 0.0)
        .then_some((weighted_12 / weight_sum, weighted_21 / weight_sum))
}

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
    let historically_verified = HISTORICALLY_VERIFIED_KEYS.get_or_init(|| DashSet::new());
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

                if data_a.last_updated == 0
                    || data_b.last_updated == 0
                    || data_a.last_updated > now_secs
                    || data_b.last_updated > now_secs
                    || ![
                        data_a.bid, data_a.ask, data_b.bid, data_b.ask, data_a.vol, data_b.vol,
                    ]
                    .iter()
                    .all(|v| v.is_finite())
                    || data_a.bid <= 0.0
                    || data_b.bid <= 0.0
                    || data_a.bid >= data_a.ask
                    || data_b.bid >= data_b.ask
                {
                    continue;
                }
                let max_stale = ts.stale_secs.min(5);
                if data_a.last_updated > 0
                    && now_secs.saturating_sub(data_a.last_updated) > max_stale
                {
                    cnt_stale += 1;
                    continue;
                }
                if data_b.last_updated > 0
                    && now_secs.saturating_sub(data_b.last_updated) > max_stale
                {
                    cnt_stale += 1;
                    continue;
                }

                let exact_source_ms_a = match name_a.as_str() {
                    "binance" => crate::collector::binance::ticker_source_ms(sym),
                    "kucoin" => crate::collector::kucoin::ticker_source_ms(sym),
                    _ => None,
                };
                let exact_source_ms_b = match name_b.as_str() {
                    "binance" => crate::collector::binance::ticker_source_ms(sym),
                    "kucoin" => crate::collector::kucoin::ticker_source_ms(sym),
                    _ => None,
                };
                if let (Some(source_ms_a), Some(source_ms_b)) =
                    (exact_source_ms_a, exact_source_ms_b)
                {
                    let max_age_ms = (max_stale as i64).saturating_mul(1_000);
                    if !exact_discovery_sources_are_fresh(
                        source_ms_a,
                        source_ms_b,
                        now_ms,
                        max_age_ms,
                    ) {
                        cnt_stale += 1;
                        continue;
                    }
                }

                // ── CROSS-EXCHANGE SYNCHRONIZATION GUARD ──
                // If ticker feeds differ by >3s in age, they represent different market moments
                // (e.g. one exchange lagged while price moved -> creates fake phantom spread).
                if data_a.last_updated > 0 && data_b.last_updated > 0 {
                    let desync = (data_a.last_updated as i64 - data_b.last_updated as i64).abs();
                    if desync > 3 {
                        cnt_stale += 1;
                        continue;
                    }
                }

                if data_a.bid <= 0.0 || data_a.ask <= 0.0 || data_b.bid <= 0.0 || data_b.ask <= 0.0
                {
                    cnt_invalid_price += 1;
                    continue;
                }
                let max_intra_mult = 1.0 + ts.intra_spread_max_pct / 100.0;
                if data_a.ask > data_a.bid * max_intra_mult
                    || data_b.ask > data_b.bid * max_intra_mult
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

                // ── PRICE SANITY CHECK ──
                // Reject pairs where prices differ > 50% between exchanges.
                // This catches different tokens with identical tickers
                // (e.g. COAIUSDT: $0.32 on Bybit vs $0.03 on KuCoin = different tokens).
                let mid_a = (data_a.bid + data_a.ask) / 2.0;
                let mid_b = (data_b.bid + data_b.ask) / 2.0;
                let price_ratio = mid_a / mid_b.max(1e-15);
                if price_ratio > 1.5 || price_ratio < 0.667 {
                    cnt_spread_range += 1;
                    continue;
                }

                // ── DIRECTIONAL ARBITRAGE SPREAD ──
                let a_over_b = (data_a.bid - data_b.ask) / data_b.ask * 100.0;
                let b_over_a = (data_b.bid - data_a.ask) / data_a.ask * 100.0;
                let (spread, direction) = if a_over_b >= b_over_a {
                    (a_over_b, format!("{name_a}>{name_b}"))
                } else {
                    (b_over_a, format!("{name_b}>{name_a}"))
                };

                // ── STRUCTURAL SPREAD (EXECUTABLE SPREAD EMA) ──
                let is_a_first = name_a < name_b;
                let (name1, name2) = if is_a_first {
                    (name_a.clone(), name_b.clone())
                } else {
                    (name_b.clone(), name_a.clone())
                };
                let (data1, data2) = if is_a_first {
                    (&data_a, &data_b)
                } else {
                    (&data_b, &data_a)
                };

                // Signed mid-price premiums in canonical order; executable entry is calculated separately.
                let exec_12 = ((data1.bid + data1.ask) / (data2.bid + data2.ask) - 1.0) * 100.0; // sell 1, buy 2
                let exec_21 = ((data2.bid + data2.ask) / (data1.bid + data1.ask) - 1.0) * 100.0; // sell 2, buy 1

                // Mid premiums — used ONLY for zero-crossing detection (direction stability)
                let mid1 = (data1.bid + data1.ask) / 2.0;
                let mid2 = (data2.bid + data2.ask) / 2.0;
                let premium_1_over_2 = (mid1 - mid2) / mid2 * 100.0;
                let premium_2_over_1 = (mid2 - mid1) / mid1 * 100.0;

                let ema_key = format!("{}|{}|{}", sym, name1, name2);
                let gap = ema_map
                    .get(&ema_key)
                    .map(|e| now_f64 - e.last_ts > 5.0)
                    .unwrap_or(false);
                if gap {
                    reset_live_verification_after_gap(
                        &ema_key,
                        verified,
                        historically_verified,
                        first_seen_map,
                    );
                }

                let (ema_12, ema_21, max_12, min_12, max_21, min_21, cross_12, cross_21) =
                    if let Some(mut entry) = ema_map.get_mut(&ema_key) {
                        let e = *entry;
                        let dt = (now_f64 - e.last_ts).max(0.0);
                        let alpha = if dt > 0.0 {
                            1.0 - (-dt / ts.ema_tau_secs).exp()
                        } else {
                            0.0
                        };

                        // EMA tracks the same signed mid-price premium as historical warmup
                        let new_12 = e.ema_12 + alpha * (exec_12 - e.ema_12);
                        let new_21 = e.ema_21 + alpha * (exec_21 - e.ema_21);

                        let new_max_12 = e.max_12.max(exec_12);
                        let new_min_12 = e.min_12.min(exec_12);
                        let new_max_21 = e.max_21.max(exec_21);
                        let new_min_21 = e.min_21.min(exec_21);

                        // Zero-crossing still based on MID premium (direction stability check)
                        let new_cross_12 = if premium_1_over_2 <= 0.0 {
                            now_f64
                        } else {
                            e.last_cross_12
                        };
                        let new_cross_21 = if premium_2_over_1 <= 0.0 {
                            now_f64
                        } else {
                            e.last_cross_21
                        };

                        *entry = EmaEntry {
                            ema_12: new_12,
                            ema_21: new_21,
                            max_12: new_max_12,
                            min_12: new_min_12,
                            max_21: new_max_21,
                            min_21: new_min_21,
                            last_cross_12: new_cross_12,
                            last_cross_21: new_cross_21,
                            last_ts: now_f64,
                        };
                        (
                            new_12,
                            new_21,
                            new_max_12,
                            new_min_12,
                            new_max_21,
                            new_min_21,
                            new_cross_12,
                            new_cross_21,
                        )
                    } else {
                        let cross_12 = now_f64;
                        let cross_21 = now_f64;
                        ema_map.insert(
                            ema_key.clone(),
                            EmaEntry {
                                ema_12: exec_12,
                                ema_21: exec_21,
                                max_12: exec_12,
                                min_12: exec_12,
                                max_21: exec_21,
                                min_21: exec_21,
                                last_cross_12: cross_12,
                                last_cross_21: cross_21,
                                last_ts: now_f64,
                            },
                        );
                        (
                            exec_12, exec_21, exec_12, exec_12, exec_21, exec_21, cross_12,
                            cross_21,
                        )
                    };

                // Map EMA values to trade direction
                let is_dir_1_over_2 = direction == format!("{name1}>{name2}");

                let (structural_max, structural_min, last_cross) = if is_dir_1_over_2 {
                    (max_12, min_12, cross_12)
                } else {
                    (max_21, min_21, cross_21)
                };

                // Expected directional exit premium includes crossing both bid/ask spreads.
                let close_cross_cost =
                    ((data1.ask - data1.bid) / mid1 + (data2.ask - data2.bid) / mid2) * 50.0;
                let structural_spread =
                    (if is_dir_1_over_2 { ema_12 } else { ema_21 }).max(0.0) + close_cross_cost;

                let has_structural_spread = (now_f64 - last_cross) > 86000.0;

                let stamp = data_a.last_updated.min(data_b.last_updated) as f64;
                if first_seen_map
                    .entry(ema_key.clone())
                    .or_default()
                    .observe(stamp)
                {
                    verified.insert(ema_key.clone());
                }
                let is_verified = verified.contains(&ema_key);

                if spread < ts.spread_threshold || spread > ts.spread_max {
                    cnt_spread_range += 1;
                    continue;
                }

                cnt_passed += 1;

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
                    bid_size_a_usd: data_a.bid_size,
                    ask_size_a_usd: data_a.ask_size,
                    bid_size_b_usd: data_b.bid_size,
                    ask_size_b_usd: data_b.ask_size,
                    source_ms_a: exact_source_ms_a
                        .unwrap_or((data_a.last_updated as i64).saturating_mul(1_000)),
                    source_ms_b: exact_source_ms_b
                        .unwrap_or((data_b.last_updated as i64).saturating_mul(1_000)),
                    vol_a: data_a.vol,
                    vol_b: data_b.vol,
                    funding_a: data_a.funding_rate,
                    funding_b: data_b.funding_rate,
                    // Negative directional premiums are conservatively floored before reserving crossing costs.
                    // Negative EMA = no premium in this direction → structural = 0.
                    structural_spread: (structural_spread.max(0.0) * 10000.0).round() / 10000.0,
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
            let fresh = map
                .values()
                .filter(|d| {
                    d.bid > 0.0
                        && d.ask > 0.0
                        && (d.last_updated == 0
                            || now_secs.saturating_sub(d.last_updated) <= ts.stale_secs)
                })
                .count();
            ex_fresh.push((name.as_str(), fresh, total));
        }
        ex_fresh.sort_by(|a, b| b.1.cmp(&a.1));
        let ex_summary: String = ex_fresh
            .iter()
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
/// Returns the number of keys that passed validation and were newly seeded.
pub fn warmup_ema_from_premiums(
    history: &[(f64, f64, f64, String, String, String)],
    ts: &TechnicalSettings,
) -> usize {
    let ema_map = STRUCTURAL_EMA.get_or_init(DashMap::new);
    let verified = VERIFIED_KEYS.get_or_init(DashSet::new);
    let historically_verified = HISTORICALLY_VERIFIED_KEYS.get_or_init(DashSet::new);
    if !ts.ema_tau_secs.is_finite() || ts.ema_tau_secs <= 0.0 {
        return 0;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let mut grouped: HashMap<String, Vec<(f64, f64, f64)>> = HashMap::new();
    for (stamp, a, b, pair, ex1, ex2) in history {
        if [*stamp, *a, *b].iter().all(|n| n.is_finite()) && *stamp <= now {
            grouped
                .entry(format!("{pair}|{ex1}|{ex2}"))
                .or_default()
                .push((*stamp, *a, *b));
        }
    }
    let mut verified_count = 0usize;
    for (key, mut rows) in grouped {
        if historically_verified.contains(&key) {
            continue;
        }
        rows.sort_by(|a, b| a.0.total_cmp(&b.0));
        rows.dedup_by(|a, b| a.0 == b.0);
        if rows.len() < 80
            || rows.last().unwrap().0 - rows[0].0 < 20.0 * 3600.0
            || now - rows.last().unwrap().0 > 1800.0
            || rows.windows(2).any(|r| r[1].0 - r[0].0 > 1800.0)
        {
            continue;
        }
        let Some((warm_12, warm_21)) = normalized_finite_ema(&rows, ts.ema_tau_secs) else {
            continue;
        };
        let (first, a, b) = rows[0];
        let mut e = EmaEntry {
            ema_12: warm_12,
            ema_21: warm_21,
            max_12: a,
            min_12: a,
            max_21: b,
            min_21: b,
            last_cross_12: first,
            last_cross_21: first,
            last_ts: first,
        };
        for (stamp, a, b) in rows.into_iter().skip(1) {
            e.max_12 = e.max_12.max(a);
            e.min_12 = e.min_12.min(a);
            e.max_21 = e.max_21.max(b);
            e.min_21 = e.min_21.min(b);
            if a <= 0.0 {
                e.last_cross_12 = stamp;
            }
            if b <= 0.0 {
                e.last_cross_21 = stamp;
            }
            e.last_ts = stamp;
        }
        if let Some(live) = ema_map.get(&key) {
            let alpha = 1.0 - (-(now - e.last_ts).max(0.0) / ts.ema_tau_secs).exp();
            e.ema_12 += alpha * (live.ema_12 - e.ema_12);
            e.ema_21 += alpha * (live.ema_21 - e.ema_21);
        }
        e.last_ts = now;
        ema_map.insert(key.clone(), e);
        verified.insert(key.clone());
        historically_verified.insert(key);
        verified_count += 1;
    }
    verified_count
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
            if verified.contains(&key) {
                None
            } else {
                Some(key)
            }
        })
        .collect()
}

#[cfg(test)]
mod quality_tests {
    use super::*;

    #[test]
    fn active_profile_prewarms_below_entry_without_lowering_entry_setting() {
        let mut profile = TradingSettings::default();
        profile.is_active = true;
        profile.min_entry_spread = 0.5;

        let threshold = discovery_spread_threshold(0.5, &[profile.clone()]);
        assert!((threshold - 0.20).abs() < 1e-12);
        assert_eq!(profile.min_entry_spread, 0.5);

        profile.is_active = false;
        assert_eq!(discovery_spread_threshold(0.5, &[profile]), 0.5);
    }

    #[test]
    fn more_permissive_diagnostic_threshold_is_preserved() {
        let mut profile = TradingSettings::default();
        profile.is_active = true;
        profile.min_entry_spread = 0.5;

        assert_eq!(discovery_spread_threshold(-1.0, &[profile]), -1.0);
    }

    #[test]
    fn exact_discovery_freshness_rejects_subsecond_rounding_loopholes() {
        let now = 10_000;
        assert!(exact_discovery_sources_are_fresh(5_000, 8_000, now, 5_000));
        assert!(!exact_discovery_sources_are_fresh(4_999, 8_000, now, 5_000));
        assert!(!exact_discovery_sources_are_fresh(5_000, 8_001, now, 5_000));
        assert!(!exact_discovery_sources_are_fresh(
            10_001, 10_000, now, 5_000
        ));
        assert!(!exact_discovery_sources_are_fresh(0, 10_000, now, 5_000));
    }

    #[test]
    fn repeated_cached_quote_cannot_warm_up_ema() {
        let mut q = ObservationQuality::default();
        for _ in 0..1000 {
            assert!(!q.observe(100.0));
        }
        assert_eq!(q.count, 1);
    }
    #[test]
    fn observation_gap_requires_new_continuous_history() {
        let mut q = ObservationQuality::default();
        for t in 1..=601 {
            q.observe(t as f64);
        }
        assert!(q.observe(602.0));
        assert!(!q.observe(700.0));
        assert_eq!(q.count, 1);
    }

    #[test]
    fn feed_gap_preserves_historical_verification_only() {
        let verified = DashSet::new();
        let historical = DashSet::new();
        let observations = DashMap::new();
        verified.insert("history".to_string());
        verified.insert("live".to_string());
        historical.insert("history".to_string());
        observations.insert("history".to_string(), ObservationQuality::default());
        observations.insert("live".to_string(), ObservationQuality::default());

        reset_live_verification_after_gap("history", &verified, &historical, &observations);
        reset_live_verification_after_gap("live", &verified, &historical, &observations);

        assert!(verified.contains("history"));
        assert!(!verified.contains("live"));
        assert!(!observations.contains_key("history"));
        assert!(!observations.contains_key("live"));
    }

    #[test]
    fn finite_ema_normalizes_away_the_oldest_sample_seed_bias() {
        let rows = vec![(0.0, -3.0, 3.0), (10.0, 3.0, -3.0)];
        let (a, b) = normalized_finite_ema(&rows, 10.0).unwrap();
        let expected = (-3.0 * (-1.0_f64).exp() + 3.0) / ((-1.0_f64).exp() + 1.0);
        assert!((a - expected).abs() < 1e-12);
        assert!((b + expected).abs() < 1e-12);
        // A first-sample-seeded recurrence would be about 0.79 instead of 1.39.
        assert!(a > 1.3);
    }

    #[test]
    fn finite_ema_preserves_a_constant_baseline() {
        let rows = vec![(0.0, 2.5, -2.5), (10.0, 2.5, -2.5), (20.0, 2.5, -2.5)];
        let (a, b) = normalized_finite_ema(&rows, 86_400.0).unwrap();
        assert!((a - 2.5).abs() < 1e-12);
        assert!((b + 2.5).abs() < 1e-12);
    }
}
