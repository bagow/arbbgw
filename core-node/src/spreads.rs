//! Spread computation — ported from rust-collector, runs in-process.
//! No HTTP serialization overhead; reads PriceMap directly from memory.

use crate::state::PriceMap;
use shared::SpreadRow;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::OnceLock;

static STRUCTURAL_EMA: OnceLock<DashMap<String, (f64, f64)>> = OnceLock::new();

const SPREAD_THRESHOLD: f64 = -1.0;
const SPREAD_MAX:       f64 = 50.0;
const STALE_SECS:       u64 = 30;
const INTRA_SPREAD_LIMIT: f64 = 1.01;
const MAX_FUNDING_DIFF: f64 = 0.005;   // 0.5% per 8h

const BLACKLIST: &[&str] = &[
    "USOILUSDT", "XMRUSDT", "TRUMPUSDT", "LUNAUSDT", "BTTUSDT", "BTCSTUSDT",
];

/// Compute spread opportunities from PriceMap.
/// Called directly by the engine loop — zero serialization/HTTP overhead.
///
/// Snapshots the entire DashMap into plain HashMaps first to minimize
/// lock hold time and avoid blocking WS collector writes.
pub fn compute_spreads(price_map: &PriceMap, min_vol_m: f64) -> Vec<SpreadRow> {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    let now_f64 = now_secs as f64;

    // Snapshot: take a quick read of each exchange shard into owned HashMaps.
    // This releases the DashMap refs immediately so collectors are never blocked.
    let snapshot: Vec<(String, HashMap<String, shared::TickerData>)> = price_map
        .iter()
        .map(|entry| (entry.key().clone(), entry.value().clone()))
        .collect();

    let min_v = min_vol_m * 1_000_000.0;
    let ema_map = STRUCTURAL_EMA.get_or_init(|| DashMap::new());
    let mut rows = Vec::with_capacity(512);

    for i in 0..snapshot.len() {
        for j in (i + 1)..snapshot.len() {
            let (name_a, map_a) = &snapshot[i];
            let (name_b, map_b) = &snapshot[j];

            for (sym, data_a) in map_a.iter() {
                let data_b = match map_b.get(sym.as_str()) { Some(d) => d, None => continue };

                if BLACKLIST.iter().any(|bl| sym.eq_ignore_ascii_case(bl)) { continue; }

                if data_a.last_updated > 0 && now_secs.saturating_sub(data_a.last_updated) > STALE_SECS { continue; }
                if data_b.last_updated > 0 && now_secs.saturating_sub(data_b.last_updated) > STALE_SECS { continue; }

                if data_a.bid <= 0.0 || data_a.ask <= 0.0 || data_b.bid <= 0.0 || data_b.ask <= 0.0 { continue; }
                if data_a.ask > data_a.bid * INTRA_SPREAD_LIMIT || data_b.ask > data_b.bid * INTRA_SPREAD_LIMIT { continue; }

                if data_a.vol <= 0.0 || data_a.vol < min_v || data_b.vol <= 0.0 || data_b.vol < min_v { continue; }

                if data_a.funding_rate != 0.0 && data_b.funding_rate != 0.0 {
                    if (data_a.funding_rate - data_b.funding_rate).abs() > MAX_FUNDING_DIFF { continue; }
                }

                let a_over_b = (data_a.bid - data_b.ask) / data_b.ask * 100.0;
                let b_over_a = (data_b.bid - data_a.ask) / data_a.ask * 100.0;
                let (spread, direction) = if a_over_b >= b_over_a {
                    (a_over_b, format!("{name_a}>{name_b}"))
                } else {
                    (b_over_a, format!("{name_b}>{name_a}"))
                };

                if spread < SPREAD_THRESHOLD || spread > SPREAD_MAX { continue; }

                let ema_key = format!("{}|{}|{}|{}", sym, name_a, name_b, direction);
                let structural_spread = if let Some(mut entry) = ema_map.get_mut(&ema_key) {
                    let (old_ema, last_updated) = *entry;
                    let dt = (now_f64 - last_updated).max(0.0);
                    let tau = 86400.0; // 24 hours
                    let alpha = 1.0 - (-dt / tau).exp();
                    let new_ema = old_ema + alpha * (spread - old_ema);
                    *entry = (new_ema, now_f64);
                    new_ema
                } else {
                    ema_map.insert(ema_key, (spread, now_f64));
                    spread
                };

                rows.push(SpreadRow {
                    pair: sym.clone(), exchange_a: name_a.clone(), exchange_b: name_b.clone(),
                    price_a: data_a.price, price_b: data_b.price, bid_a: data_a.bid, ask_a: data_a.ask, bid_b: data_b.bid, ask_b: data_b.ask,
                    vol_a: data_a.vol, vol_b: data_b.vol, funding_a: data_a.funding_rate, funding_b: data_b.funding_rate,
                    structural_spread,
                    spread: (spread * 10000.0).round() / 10000.0,
                    direction,
                });
            }
        }
    }

    // Partial sort: we only need top spreads, but full sort is fine for ~1-5k rows
    rows.sort_unstable_by(|a, b| b.spread.partial_cmp(&a.spread).unwrap_or(std::cmp::Ordering::Equal));
    rows
}

/// Pre-seed the STRUCTURAL_EMA from historical (timestamp, spread, direction, pair, ex_a, ex_b) data.
/// Replays the history in chronological order through the same Time-Decay EMA formula,
/// so the engine starts with a realistic 24h baseline instead of waiting for live accumulation.
pub fn warmup_ema_from_history(history: &[(f64, f64, String, String, String, String)]) {
    let ema_map = STRUCTURAL_EMA.get_or_init(|| DashMap::new());
    let tau = 86400.0_f64;

    // Sort chronologically (history from API may be newest-first)
    let mut sorted = history.to_vec();
    sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    for (ts, spread, direction, pair, ex_a, ex_b) in &sorted {
        let key = format!("{}|{}|{}|{}", pair, ex_a, ex_b, direction);
        let mut entry = ema_map.entry(key).or_insert((*spread, *ts));
        let (old_ema, last_ts) = *entry;
        let dt = (ts - last_ts).max(0.0);
        let alpha = 1.0 - (-dt / tau).exp();
        let new_ema = old_ema + alpha * (spread - old_ema);
        *entry = (new_ema, *ts);
    }
}
