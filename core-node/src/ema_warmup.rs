//! EMA Warmup — fetches 24h of historical 15-min candles from exchanges
//! on startup, reconstructs historical spreads for every tradeable pair combination,
//! and pre-initialises the STRUCTURAL_EMA cache so the engine can trade immediately
//! without waiting for a cold-start accumulation period.
//!
//! Uses parallel fetching (~20 concurrent requests) to complete warmup quickly.

use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::info;

use crate::spreads::warmup_ema_from_premiums;
use crate::state::PriceMap;

const LOOKBACK_SECS: u64 = 86_400; // 24 hours
const MAX_CONCURRENT: usize = 20; // Max parallel REST requests (avoid rate limits)

// ─── Gate.io: fetch 15-min candles for a USDT perp ────────────────────────────

async fn fetch_gate_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    // Gate contract name: BTCUSDT → BTC_USDT
    let contract = pair.replace("USDT", "_USDT");
    let from = now_secs() - LOOKBACK_SECS;
    let url = format!(
        "https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={}&interval=15m&from={}&limit=100",
        contract, from
    );
    let v: Value = match http
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(j) => j,
            Err(_) => return None,
        },
        _ => return None,
    };
    let arr = match v.as_array() {
        Some(a) => a,
        None => return Some(vec![]),
    };
    let res: Vec<(f64, f64)> = arr
        .iter()
        .filter_map(|c| {
            // Gate candles: [{"t":ts,"o":open,"c":close,...}]
            let ts = c["t"].as_f64()?;
            let close = c["c"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| c["c"].as_f64())?;
            Some((ts, close))
        })
        .collect();
    Some(res)
}

// ─── KuCoin Futures: fetch 15-min klines ──────────────────────────────────────

async fn fetch_kucoin_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    // KuCoin symbol: XBTUSDTM or BTCUSDTM pattern — pair ends with USDT → add M
    let sym = format!("{}M", pair);
    let from = now_secs() - LOOKBACK_SECS;
    let url =
        format!(
        "https://api-futures.kucoin.com/api/v1/kline/query?symbol={}&granularity=15&from={}&to={}",
        sym, from * 1000, now_secs() * 1000
    );
    let v: Value = match http
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(j) => j,
            Err(_) => return None,
        },
        _ => return None,
    };
    // KuCoin response: { "data": [[ts_ms, open, high, low, close, vol], ...] }
    let arr = match v["data"].as_array() {
        Some(a) => a,
        None => return Some(vec![]),
    };
    let res: Vec<(f64, f64)> = arr
        .iter()
        .filter_map(|c| {
            let ts = c[0].as_f64()? / 1000.0; // ms → secs
            let close = c[4].as_f64()?;
            Some((ts, close))
        })
        .collect();
    Some(res)
}

// ─── HTX: fetch 15-min klines ─────────────────────────────────────────────────

async fn fetch_htx_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    // HTX linear-swap symbol: BTCUSDT → BTC-USDT
    let sym = format!("{}-{}", &pair[..pair.len() - 4], &pair[pair.len() - 4..]);
    let url = format!(
        "https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code={}&period=15min&size=100",
        sym
    );
    let v: Value = match http
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(j) => j,
            Err(_) => return None,
        },
        _ => return None,
    };
    let arr = match v["data"].as_array() {
        Some(a) => a,
        None => return Some(vec![]),
    };
    let res: Vec<(f64, f64)> = arr
        .iter()
        .filter_map(|c| {
            let ts = c["id"].as_f64()?;
            let close = c["close"].as_f64()?;
            Some((ts, close))
        })
        .collect();
    Some(res)
}

// ─── Binance: fetch 15-min klines ─────────────────────────────────────────────

async fn fetch_binance_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    let from = now_secs() - LOOKBACK_SECS;
    let url = format!(
        "https://fapi.binance.com/fapi/v1/klines?symbol={}&interval=15m&startTime={}&limit=100",
        pair,
        from * 1000
    );
    let v: Value = match http
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(j) => j,
            Err(_) => return None,
        },
        _ => return None,
    };
    let arr = match v.as_array() {
        Some(a) => a,
        None => return Some(vec![]),
    };
    let res: Vec<(f64, f64)> = arr
        .iter()
        .filter_map(|c| {
            // Binance klines: [openTime, open, high, low, close, vol, closeTime, ...]
            let ts = c[0].as_f64()? / 1000.0; // ms → secs
            let close = c[4]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| c[4].as_f64())?;
            Some((ts, close))
        })
        .collect();
    Some(res)
}

// ─── Bybit: fetch 15-min klines ───────────────────────────────────────────────

async fn fetch_bybit_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    let from = now_secs() - LOOKBACK_SECS;
    let url = format!(
        "https://api.bybit.com/v5/market/kline?category=linear&symbol={}&interval=15&start={}&limit=100",
        pair, from * 1000
    );
    let v: Value = match http
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(j) => j,
            Err(_) => return None,
        },
        _ => return None,
    };
    let arr = match v["result"]["list"].as_array() {
        Some(a) => a,
        None => return Some(vec![]),
    };
    let res: Vec<(f64, f64)> = arr
        .iter()
        .filter_map(|c| {
            // Bybit klines: [startTime, open, high, low, close, vol, turnover]
            let ts = c[0].as_str().and_then(|s| s.parse::<f64>().ok())? / 1000.0;
            let close = c[4].as_str().and_then(|s| s.parse().ok())?;
            Some((ts, close))
        })
        .collect();
    Some(res)
}

// ─── Main warmup function — called from main.rs after WS collectors start ────

pub async fn run(http: Client, prices: PriceMap, ts: shared::TechnicalSettings) {
    // Wait for collectors to populate the price map (full 15s to ensure all exchanges connect)
    for _ in 0..30 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // Don't break early, otherwise slower exchanges like Gate/Kucoin miss historical warmup!
    }

    // Collect all pairs available on each exchange
    let exchanges: Vec<String> = prices.iter().map(|e| e.key().clone()).collect();
    info!(
        "[ema_warmup] Starting EMA warmup with {} exchanges (tau={}s)",
        exchanges.len(),
        ts.ema_tau_secs
    );

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let mut handles = Vec::new();

    for i in 0..exchanges.len() {
        for j in (i + 1)..exchanges.len() {
            let ex_a = exchanges[i].clone();
            let ex_b = exchanges[j].clone();

            let pairs: Vec<String> = {
                let map_a = match prices.get(ex_a.as_str()) {
                    Some(m) => m,
                    None => continue,
                };
                let map_b = match prices.get(ex_b.as_str()) {
                    Some(m) => m,
                    None => continue,
                };

                // Only warm pairs present on BOTH exchanges
                map_a
                    .keys()
                    .filter(|k| map_b.contains_key(k.as_str()))
                    .cloned()
                    .collect()
            };

            for pair in pairs {
                let http = http.clone();
                let ex_a = ex_a.clone();
                let ex_b = ex_b.clone();
                let ts = ts.clone();
                let sem = semaphore.clone();

                handles.push(tokio::spawn(async move {
                    // Acquire semaphore permit — limits concurrent REST requests
                    let _permit = sem.acquire().await.unwrap();

                    let candles_a = fetch_candles_for_with_retry(&http, &ex_a, &pair).await;
                    let candles_b = fetch_candles_for_with_retry(&http, &ex_b, &pair).await;

                    if candles_a.is_empty() || candles_b.is_empty() {
                        return 0usize;
                    }

                    // Merge by timestamp (align to 15-min buckets)
                    let merged = merge_candles(&candles_a, &candles_b);
                    if merged.len() < 2 {
                        return 0;
                    }

                    // Canonical order: name1 < name2 (matching live compute_spreads)
                    let is_a_first = ex_a < ex_b;
                    let (name1, name2) = if is_a_first {
                        (ex_a.clone(), ex_b.clone())
                    } else {
                        (ex_b.clone(), ex_a.clone())
                    };

                    // Calculate canonical premium at each historical point.
                    // Treat close price as mid-price proxy (candles don't have bid/ask).
                    let mut history = Vec::with_capacity(merged.len());
                    for (ts_val, price_a, price_b) in &merged {
                        if *price_a <= 0.0 || *price_b <= 0.0 {
                            continue;
                        }
                        let (close1, close2) = if is_a_first {
                            (*price_a, *price_b)
                        } else {
                            (*price_b, *price_a)
                        };
                        let premium_12 = (close1 - close2) / close2 * 100.0;
                        let premium_21 = (close2 - close1) / close1 * 100.0;
                        history.push((
                            *ts_val,
                            premium_12,
                            premium_21,
                            pair.to_string(),
                            name1.clone(),
                            name2.clone(),
                        ));
                    }

                    warmup_ema_from_premiums(&history, &ts);
                    1usize
                }));
            }
        }
    }

    // Wait for all tasks to complete
    let mut warmed = 0usize;
    for h in handles {
        if let Ok(count) = h.await {
            warmed += count;
        }
    }

    info!(
        "[ema_warmup] ✅ Warmed EMA for {} pair combinations from 24h history",
        warmed
    );
}

// ─── Dispatch to the right exchange API ──────────────────────────────────────

async fn fetch_candles_for(http: &Client, exchange: &str, pair: &str) -> Option<Vec<(f64, f64)>> {
    match exchange {
        "gate" => fetch_gate_candles(http, pair).await,
        "kucoin" => fetch_kucoin_candles(http, pair).await,
        "htx" => fetch_htx_candles(http, pair).await,
        "binance" => fetch_binance_candles(http, pair).await,
        "bybit" => fetch_bybit_candles(http, pair).await,
        _ => Some(vec![]),
    }
}

async fn fetch_candles_for_with_retry(
    http: &Client,
    exchange: &str,
    pair: &str,
) -> Vec<(f64, f64)> {
    // Limited retries to avoid semaphore deadlocks during API rate limits or IP bans.
    for attempt in 1..=3 {
        if let Some(res) = fetch_candles_for(http, exchange, pair).await {
            // Reached HTTP 200 OK. Even if empty, it's valid.
            return res;
        }
        // If None, it is a Rate Limit or Server Error. Sleep and retry.
        tokio::time::sleep(std::time::Duration::from_millis(500 * attempt)).await;
    }
    vec![]
}

// ─── Merge two (timestamp, price) series by nearest 15-minute ──────────────────

fn merge_candles(a: &[(f64, f64)], b: &[(f64, f64)]) -> Vec<(f64, f64, f64)> {
    use std::collections::HashMap;
    let map_b: HashMap<i64, f64> = b
        .iter()
        .map(|(ts, p)| ((*ts / 900.0).round() as i64, *p))
        .collect();

    a.iter()
        .filter_map(|(ts, pa)| {
            let bucket = (*ts / 900.0).round() as i64;
            map_b.get(&bucket).map(|pb| (*ts, *pa, *pb))
        })
        .collect()
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
