//! Spread computation — ported from rust-collector, runs in-process.
//! No HTTP serialization overhead; reads PriceMap directly from memory.

use crate::state::PriceMap;
use shared::{SpreadRow, TechnicalSettings};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::OnceLock;

/// (ema_ab, ema_ba, max_ab, min_ab, max_ba, min_ba, last_cross_ab, last_cross_ba, last_ts_secs)
static STRUCTURAL_EMA: OnceLock<DashMap<String, (f64, f64, f64, f64, f64, f64, f64, f64, f64)>> = OnceLock::new();

/// Compute spread opportunities from PriceMap.
/// Called directly by the engine loop — zero serialization/HTTP overhead.
///
/// Snapshots the entire DashMap into plain HashMaps first to minimize
/// lock hold time and avoid blocking WS collector writes.
pub fn compute_spreads(price_map: &PriceMap, min_vol_m: f64, ts: &TechnicalSettings) -> Vec<SpreadRow> {
    let now_f64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs_f64();
    let now_secs = now_f64 as u64;

    // Snapshot: take a quick read of each exchange shard into owned HashMaps.
    // This releases the DashMap refs immediately so collectors are never blocked.
    let snapshot: Vec<(String, HashMap<String, shared::TickerData>)> = price_map
        .iter()
        .map(|entry| (entry.key().clone(), entry.value().clone()))
        .collect();

    let min_v = min_vol_m * 1_000_000.0;
    let ema_map = STRUCTURAL_EMA.get_or_init(|| DashMap::new());
    let blacklist = ts.blacklist_vec();
    let mut rows = Vec::with_capacity(512);

    for i in 0..snapshot.len() {
        for j in (i + 1)..snapshot.len() {
            let (name_a, map_a) = &snapshot[i];
            let (name_b, map_b) = &snapshot[j];

            for (sym, data_a) in map_a.iter() {
                let data_b = match map_b.get(sym.as_str()) { Some(d) => d, None => continue };

                if blacklist.iter().any(|bl| sym.eq_ignore_ascii_case(bl)) { continue; }

                if data_a.last_updated > 0 && now_secs.saturating_sub(data_a.last_updated) > ts.stale_secs { continue; }
                if data_b.last_updated > 0 && now_secs.saturating_sub(data_b.last_updated) > ts.stale_secs { continue; }

                if data_a.bid <= 0.0 || data_a.ask <= 0.0 || data_b.bid <= 0.0 || data_b.ask <= 0.0 { continue; }
                if data_a.ask > data_a.bid * ts.intra_spread_limit || data_b.ask > data_b.bid * ts.intra_spread_limit { continue; }

                if data_a.vol <= 0.0 || data_a.vol < min_v || data_b.vol <= 0.0 || data_b.vol < min_v { continue; }

                // Removed max_funding_diff check per user request

                // ── DIRECTIONAL ARBITRAGE SPREAD ──
                let a_over_b = (data_a.bid - data_b.ask) / data_b.ask * 100.0;
                let b_over_a = (data_b.bid - data_a.ask) / data_a.ask * 100.0;
                let (spread, direction) = if a_over_b >= b_over_a {
                    (a_over_b, format!("{name_a}>{name_b}"))
                } else {
                    (b_over_a, format!("{name_b}>{name_a}"))
                };

                if spread < ts.spread_threshold || spread > ts.spread_max { continue; }

                // ── STRUCTURAL SPREAD (MID-PRICE EMA) ──
                let is_a_first = name_a < name_b;
                let (name1, name2) = if is_a_first { (name_a.clone(), name_b.clone()) } else { (name_b.clone(), name_a.clone()) };
                // mid1 corresponds to name1, mid2 corresponds to name2
                let (data1, data2) = if is_a_first { (&data_a, &data_b) } else { (&data_b, &data_a) };
                
                let mid1 = (data1.bid + data1.ask) / 2.0;
                let mid2 = (data2.bid + data2.ask) / 2.0;
                
                // Track mathematically symmetric directional premiums based on Mid price
                let premium_1_over_2 = (mid1 - mid2) / mid2 * 100.0;
                let premium_2_over_1 = (mid2 - mid1) / mid1 * 100.0;
                
                let ema_key = format!("{}|{}|{}", sym, name1, name2);
                let (ema_12, ema_21, max_12, min_12, max_21, min_21, cross_12, cross_21) =
                    if let Some(mut entry) = ema_map.get_mut(&ema_key) {
                        let (old_12, old_21, old_max_12, old_min_12, old_max_21, old_min_21, mut old_cross_12, mut old_cross_21, last_updated) = *entry;
                        let dt = (now_f64 - last_updated).max(0.0);
                        let alpha = if dt > 0.0 { 1.0 - (-dt / ts.ema_tau_secs).exp() } else { 0.0 };
                        
                        let new_12 = old_12 + alpha * (premium_1_over_2 - old_12);
                        let new_21 = old_21 + alpha * (premium_2_over_1 - old_21);
                        
                        let new_max_12 = old_max_12.max(premium_1_over_2);
                        let new_min_12 = old_min_12.min(premium_1_over_2);
                        let new_max_21 = old_max_21.max(premium_2_over_1);
                        let new_min_21 = old_min_21.min(premium_2_over_1);
                        
                        if premium_1_over_2 <= 0.0 { old_cross_12 = now_f64; }
                        if premium_2_over_1 <= 0.0 { old_cross_21 = now_f64; }

                        *entry = (new_12, new_21, new_max_12, new_min_12, new_max_21, new_min_21, old_cross_12, old_cross_21, now_f64);
                        (new_12, new_21, new_max_12, new_min_12, new_max_21, new_min_21, old_cross_12, old_cross_21)
                    } else {
                        // Default cross to NOW — conservative: assume NO structural spread
                        // until proven by 24h of no zero-crossing. Old default (0.0) caused
                        // every new pair to immediately have has_structural_spread=true.
                        let cross_12 = now_f64;
                        let cross_21 = now_f64;
                        ema_map.insert(ema_key, (premium_1_over_2, premium_2_over_1, premium_1_over_2, premium_1_over_2, premium_2_over_1, premium_2_over_1, cross_12, cross_21, now_f64));
                        (premium_1_over_2, premium_2_over_1, premium_1_over_2, premium_1_over_2, premium_2_over_1, premium_2_over_1, cross_12, cross_21)
                    };

                // Map values depending on trade direction.
                // name1 is ALWAYS alphabetically first. name2 is ALWAYS alphabetically second.
                let is_dir_1_over_2 = direction == format!("{name1}>{name2}");
                
                let (ema_mid_spread, current_mid_spread, structural_max, structural_min, last_cross) = if is_dir_1_over_2 {
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

                let has_structural_spread = (now_f64 - last_cross) > 86400.0;

                rows.push(SpreadRow {
                    pair: sym.clone(), exchange_a: name_a.clone(), exchange_b: name_b.clone(),
                    price_a: data_a.price, price_b: data_b.price, bid_a: data_a.bid, ask_a: data_a.ask, bid_b: data_b.bid, ask_b: data_b.ask,
                    vol_a: data_a.vol, vol_b: data_b.vol, funding_a: data_a.funding_rate, funding_b: data_b.funding_rate,
                    structural_spread: (structural_spread * 10000.0).round() / 10000.0,
                    structural_max,
                    structural_min,
                    spread: (spread * 10000.0).round() / 10000.0,
                    direction,
                    has_structural_spread,
                });
            }
        }
    }

    // Partial sort: we only need top spreads, but full sort is fine for ~1-5k rows
    rows.sort_unstable_by(|a, b| b.spread.partial_cmp(&a.spread).unwrap_or(std::cmp::Ordering::Equal));
    rows
}

/// Pre-seed the STRUCTURAL_EMA from historical canonical premiums.
/// Tuple: (timestamp_secs, premium_12, premium_21, pair, ex1_canonical, ex2_canonical)
pub fn warmup_ema_from_premiums(history: &[(f64, f64, f64, String, String, String)], ts: &TechnicalSettings) {
    let ema_map = STRUCTURAL_EMA.get_or_init(|| DashMap::new());
    let tau = ts.ema_tau_secs;

    let mut sorted = history.to_vec();
    sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    for (ts_val, premium_12, premium_21, pair, name1, name2) in &sorted {
        let key = format!("{}|{}|{}", pair, name1, name2);
        // Default cross to first candle ts (not 0.0) — prevents false has_structural_spread
        let mut entry = ema_map.entry(key).or_insert((*premium_12, *premium_21, *premium_12, *premium_12, *premium_21, *premium_21, *ts_val, *ts_val, *ts_val));
        let (old_12, old_21, old_max_12, old_min_12, old_max_21, old_min_21, mut old_cross_12, mut old_cross_21, last_ts) = *entry;
        let dt = (ts_val - last_ts).max(0.0);
        let alpha = if dt > 0.0 { 1.0 - (-dt / tau).exp() } else { 0.0 };
        
        let new_12 = old_12 + alpha * (premium_12 - old_12);
        let new_21 = old_21 + alpha * (premium_21 - old_21);
        
        let new_max_12 = old_max_12.max(*premium_12);
        let new_min_12 = old_min_12.min(*premium_12);
        let new_max_21 = old_max_21.max(*premium_21);
        let new_min_21 = old_min_21.min(*premium_21);
        
        if *premium_12 <= 0.0 { old_cross_12 = *ts_val; }
        if *premium_21 <= 0.0 { old_cross_21 = *ts_val; }

        *entry = (new_12, new_21, new_max_12, new_min_12, new_max_21, new_min_21, old_cross_12, old_cross_21, *ts_val);
    }
}
