//! Spread computation — ported from rust-collector, runs in-process.
//! No HTTP serialization overhead; reads PriceMap directly from memory.

use std::collections::HashMap;
use crate::state::{PriceMap, TickerData};
use shared::SpreadRow;

const SPREAD_THRESHOLD: f64 = -1.0;
const SPREAD_MAX:       f64 = 50.0;
const TOP_N:            usize = 1000;
const STALE_SECS:       u64 = 30;
const INTRA_SPREAD_LIMIT: f64 = 1.02;
const MAX_FUNDING_DIFF: f64 = 0.005;   // 0.5% per 8h

const BLACKLIST: &[&str] = &[
    "USOILUSDT", "XMRUSDT", "TRUMPUSDT", "LUNAUSDT", "BTTUSDT", "BTCSTUSDT",
];

/// Compute spread opportunities from PriceMap.
/// Called directly by the engine loop — zero serialization/HTTP overhead.
pub fn compute_spreads(price_map: &PriceMap, min_vol_m: f64) -> Vec<SpreadRow> {
    let exchanges: Vec<String> = price_map.iter().map(|e| e.key().clone()).collect();
    let mut rows = Vec::new();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

    for i in 0..exchanges.len() {
        for j in (i + 1)..exchanges.len() {
            let name_a = &exchanges[i];
            let name_b = &exchanges[j];
            let map_a = match price_map.get(name_a) { Some(m) => m, None => continue };
            let map_b = match price_map.get(name_b) { Some(m) => m, None => continue };

            for (sym, data_a) in map_a.iter() {
                let data_b = match map_b.get(sym.as_str()) { Some(d) => d, None => continue };

                if BLACKLIST.iter().any(|bl| sym.eq_ignore_ascii_case(bl)) { continue; }

                if data_a.last_updated > 0 && now_secs.saturating_sub(data_a.last_updated) > STALE_SECS { continue; }
                if data_b.last_updated > 0 && now_secs.saturating_sub(data_b.last_updated) > STALE_SECS { continue; }

                if data_a.bid <= 0.0 || data_a.ask <= 0.0 || data_b.bid <= 0.0 || data_b.ask <= 0.0 { continue; }
                if data_a.ask > data_a.bid * INTRA_SPREAD_LIMIT || data_b.ask > data_b.bid * INTRA_SPREAD_LIMIT { continue; }

                let min_v = min_vol_m * 1_000_000.0;
                if data_a.vol <= 0.0 || data_a.vol < min_v || data_b.vol <= 0.0 || data_b.vol < min_v { continue; }

                if data_a.funding_rate != 0.0 && data_b.funding_rate != 0.0 {
                    if (data_a.funding_rate - data_b.funding_rate).abs() > MAX_FUNDING_DIFF { continue; }
                }

                let a_over_b = (data_a.bid - data_b.ask) / data_b.ask * 100.0;
                let b_over_a = (data_b.bid - data_a.ask) / data_a.ask * 100.0;
                let (spread, direction, exchange_a, exchange_b, price_a, price_b,
                     bid_a, ask_a, bid_b, ask_b, vol_a, vol_b, funding_a, funding_b) =
                    if a_over_b >= b_over_a {
                        (a_over_b, format!("{name_a}>{name_b}"),
                         name_a.clone(), name_b.clone(),
                         data_a.price, data_b.price,
                         data_a.bid, data_a.ask, data_b.bid, data_b.ask,
                         data_a.vol, data_b.vol, data_a.funding_rate, data_b.funding_rate)
                    } else {
                        (b_over_a, format!("{name_b}>{name_a}"),
                         name_b.clone(), name_a.clone(),
                         data_b.price, data_a.price,
                         data_b.bid, data_b.ask, data_a.bid, data_a.ask,
                         data_b.vol, data_a.vol, data_b.funding_rate, data_a.funding_rate)
                    };

                if spread < SPREAD_THRESHOLD || spread > SPREAD_MAX { continue; }

                rows.push(SpreadRow {
                    pair: sym.clone(), exchange_a, exchange_b,
                    price_a, price_b, bid_a, ask_a, bid_b, ask_b,
                    vol_a, vol_b, funding_a, funding_b,
                    spread: (spread * 10000.0).round() / 10000.0,
                    direction,
                });
            }
        }
    }

    rows.sort_by(|a, b| b.spread.partial_cmp(&a.spread).unwrap_or(std::cmp::Ordering::Equal));
    rows.truncate(TOP_N);
    rows
}
