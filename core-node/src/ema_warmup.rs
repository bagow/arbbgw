//! EMA Warmup — fetches 24h of historical 1-min candles from Gate, KuCoin and HTX
//! on startup, reconstructs historical spreads for every tradeable pair combination,
//! and pre-initialises the STRUCTURAL_EMA cache so the engine can trade immediately
//! without waiting for a cold-start accumulation period.

use reqwest::Client;
use serde_json::Value;
use tracing::info;

use crate::state::PriceMap;
use crate::spreads::warmup_ema_from_history;

const LOOKBACK_SECS: u64 = 86_400; // 24 hours

// ─── Gate.io: fetch 1-min candles for a USDT perp ────────────────────────────

async fn fetch_gate_candles(http: &Client, pair: &str) -> Vec<(f64, f64)> {
    // Gate contract name: BTCUSDT → BTC_USDT
    let contract = pair.replace("USDT", "_USDT");
    let from = now_secs() - LOOKBACK_SECS;
    let url = format!(
        "https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={}&interval=1m&from={}&limit=1440",
        contract, from
    );
    let v: Value = match http.get(&url).timeout(std::time::Duration::from_secs(15)).send().await {
        Ok(r) => match r.json().await { Ok(j) => j, Err(_) => return vec![] },
        Err(_) => return vec![],
    };
    let arr = match v.as_array() { Some(a) => a, None => return vec![] };
    arr.iter().filter_map(|c| {
        // Gate candles: [{"t":ts,"o":open,"c":close,...}]
        let ts = c["t"].as_f64()?;
        let close = c["c"].as_str().and_then(|s| s.parse().ok()).or_else(|| c["c"].as_f64())?;
        Some((ts, close))
    }).collect()
}

// ─── KuCoin Futures: fetch 1-min klines ──────────────────────────────────────

async fn fetch_kucoin_candles(http: &Client, pair: &str) -> Vec<(f64, f64)> {
    // KuCoin symbol: XBTUSDTM or BTCUSDTM pattern — pair ends with USDT → add M
    let sym = format!("{}M", pair);
    let from = now_secs() - LOOKBACK_SECS;
    let url = format!(
        "https://api-futures.kucoin.com/api/v1/kline/query?symbol={}&granularity=1&from={}&to={}",
        sym, from * 1000, now_secs() * 1000
    );
    let v: Value = match http.get(&url).timeout(std::time::Duration::from_secs(15)).send().await {
        Ok(r) => match r.json().await { Ok(j) => j, Err(_) => return vec![] },
        Err(_) => return vec![],
    };
    // KuCoin response: { "data": [[ts_ms, open, high, low, close, vol], ...] }
    let arr = match v["data"].as_array() { Some(a) => a, None => return vec![] };
    arr.iter().filter_map(|c| {
        let ts = c[0].as_f64()? / 1000.0; // ms → secs
        let close = c[4].as_f64()?;
        Some((ts, close))
    }).collect()
}

// ─── HTX: fetch 1-min klines ─────────────────────────────────────────────────

async fn fetch_htx_candles(http: &Client, pair: &str) -> Vec<(f64, f64)> {
    // HTX linear-swap symbol: BTCUSDT → BTC-USDT
    let sym = format!("{}-{}", &pair[..pair.len()-4], &pair[pair.len()-4..]);
    let url = format!(
        "https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code={}&period=1min&size=1440",
        sym
    );
    let v: Value = match http.get(&url).timeout(std::time::Duration::from_secs(15)).send().await {
        Ok(r) => match r.json().await { Ok(j) => j, Err(_) => return vec![] },
        Err(_) => return vec![],
    };
    let arr = match v["data"].as_array() { Some(a) => a, None => return vec![] };
    arr.iter().filter_map(|c| {
        let ts = c["id"].as_f64()?;
        let close = c["close"].as_f64()?;
        Some((ts, close))
    }).collect()
}

// ─── Main warmup function — called from main.rs after WS collectors start ────

pub async fn run(http: Client, prices: PriceMap) {
    // Wait for collectors to populate the price map (up to 15s)
    for _ in 0..30 {
        let total: usize = prices.iter().map(|e| e.value().len()).sum();
        if total > 100 { break; }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    // Collect all pairs available on each exchange
    let exchanges: Vec<String> = prices.iter().map(|e| e.key().clone()).collect();
    info!("[ema_warmup] Starting EMA warmup with {} exchanges", exchanges.len());

    let mut warmed = 0usize;

    for i in 0..exchanges.len() {
        for j in (i + 1)..exchanges.len() {
            let ex_a = &exchanges[i];
            let ex_b = &exchanges[j];

            let map_a = match prices.get(ex_a.as_str()) { Some(m) => m, None => continue };
            let map_b = match prices.get(ex_b.as_str()) { Some(m) => m, None => continue };

            // Only warm pairs present on BOTH exchanges
            let pairs: Vec<String> = map_a.keys()
                .filter(|k| map_b.contains_key(k.as_str()))
                .cloned()
                .collect();

            for pair in &pairs {
                // Fetch candles from both exchanges
                let candles_a = fetch_candles_for(&http, ex_a, pair).await;
                let candles_b = fetch_candles_for(&http, ex_b, pair).await;

                if candles_a.is_empty() || candles_b.is_empty() { continue; }

                // Merge by timestamp (align to 1-min buckets)
                let merged = merge_candles(&candles_a, &candles_b);
                if merged.len() < 10 { continue; }

                // Calculate spread at each historical point and feed EMA
                let mut history = Vec::with_capacity(merged.len());
                for (ts, price_a, price_b) in &merged {
                    if *price_a <= 0.0 || *price_b <= 0.0 { continue; }
                    let spread_ab = (price_a - price_b) / price_b * 100.0;
                    let spread_ba = (price_b - price_a) / price_a * 100.0;
                    let (spread, direction) = if spread_ab >= spread_ba {
                        (spread_ab, format!("{ex_a}>{ex_b}"))
                    } else {
                        (spread_ba, format!("{ex_b}>{ex_a}"))
                    };
                    history.push((*ts, spread, direction, pair.to_string(), ex_a.to_string(), ex_b.to_string()));
                }

                warmup_ema_from_history(&history);
                warmed += 1;
            }
        }
    }

    info!("[ema_warmup] ✅ Warmed EMA for {} pair combinations from 24h history", warmed);
}

// ─── Dispatch to the right exchange API ──────────────────────────────────────

async fn fetch_candles_for(http: &Client, exchange: &str, pair: &str) -> Vec<(f64, f64)> {
    match exchange {
        "gate"    => fetch_gate_candles(http, pair).await,
        "kucoin"  => fetch_kucoin_candles(http, pair).await,
        "htx"     => fetch_htx_candles(http, pair).await,
        _         => vec![],
    }
}

// ─── Merge two (timestamp, price) series by nearest minute ──────────────────

fn merge_candles(a: &[(f64, f64)], b: &[(f64, f64)]) -> Vec<(f64, f64, f64)> {
    use std::collections::HashMap;
    let map_b: HashMap<i64, f64> = b.iter()
        .map(|(ts, p)| ((*ts / 60.0).round() as i64, *p))
        .collect();

    a.iter().filter_map(|(ts, pa)| {
        let bucket = (*ts / 60.0).round() as i64;
        map_b.get(&bucket).map(|pb| (*ts, *pa, *pb))
    }).collect()
}

fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
}
