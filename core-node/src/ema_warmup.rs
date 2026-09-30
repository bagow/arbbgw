//! EMA Warmup — fetches 24h of historical 15-min candles from exchanges
//! on startup, reconstructs historical spreads for every tradeable pair combination,
//! and pre-initialises the STRUCTURAL_EMA cache so the engine can trade immediately
//! without waiting for a cold-start accumulation period.
//!
//! Uses bounded parallel fetching so history cannot starve live sockets.

use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::info;

use crate::spreads::{warmup_ema_from_premiums, unverified_ema_keys};
use crate::state::{L2OrderbookMap, PriceMap};

const LOOKBACK_SECS: u64 = 86_400; // 24 hours
const MAX_CONCURRENT: usize = 2;

async fn wait_for_market_headroom(books: &L2OrderbookMap) {
    loop {
        let now = crate::engine::utils::now_ms();
        let binance_fresh = books.get("binance:BTCUSDT")
            .is_some_and(|book| (0..=750).contains(&(now - book.updated_ms)));
        let kucoin_fresh = books.iter().any(|book| book.key().starts_with("kucoin:")
            && (0..=750).contains(&(now - book.updated_ms)));
        if binance_fresh && kucoin_fresh { return; }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

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

// ─── BingX: fetch 15-min klines ───────────────────────────────────────────────

async fn fetch_bingx_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    let sym = pair.replace("USDT", "-USDT");
    let url = format!(
        "https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol={}&interval=15m&limit=100",
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
            let ts = c["time"].as_f64()? / 1000.0;
            let close = c["close"].as_f64()?;
            Some((ts, close))
        })
        .collect();
    Some(res)
}

// ─── Bitget: fetch 15-min klines ──────────────────────────────────────────────

async fn fetch_bitget_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    let url = format!(
        "https://api.bitget.com/api/v2/mix/market/candles?symbol={}&productType=USDT-FUTURES&granularity=15m&limit=100",
        pair
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
            let ts = c[0].as_str().and_then(|s| s.parse::<f64>().ok())? / 1000.0;
            let close = c[4].as_str().and_then(|s| s.parse::<f64>().ok())?;
            Some((ts, close))
        })
        .collect();
    Some(res)
}

// ─── XT.COM: fetch 15-min klines ──────────────────────────────────────────────

async fn fetch_xt_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    let sym = pair.to_lowercase().replace("usdt", "_usdt");
    let url = format!(
        "https://fapi.xt.com/fapi/v1/kline?symbol={}&interval=15m&limit=100",
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
    let arr = match v["result"].as_array() {
        Some(a) => a,
        None => return Some(vec![]),
    };
    let res: Vec<(f64, f64)> = arr
        .iter()
        .filter_map(|c| {
            let ts = c["t"].as_f64()? / 1000.0;
            let close = c["c"].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| c["c"].as_f64())?;
            Some((ts, close))
        })
        .collect();
    Some(res)
}

// ─── Binance: fetch 15-min klines ─────────────────────────────────────────────

async fn fetch_binance_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    // Check ban + weight before REST request (each kline = weight 5)
    if crate::engine::exchanges::binance::check_before_binance_rest().is_err() {
        return None;
    }
    // Throttle if weight > 75% — sleep to let window reset
    if crate::engine::exchanges::binance::binance_remaining_weight() < 600 {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        if crate::engine::exchanges::binance::check_before_binance_rest().is_err() {
            return None;
        }
    }

    let from = now_secs() - LOOKBACK_SECS;
    let url = format!(
        "https://fapi.binance.com/fapi/v1/klines?symbol={}&interval=15m&startTime={}&limit=100",
        pair,
        from * 1000
    );
    let r = match http
        .get(&url)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => return None,
    };

    // Record response for weight tracking
    crate::engine::exchanges::binance::record_binance_response(
        r.status().as_u16(), r.headers(),
    );

    if !r.status().is_success() { return None; }
    let v: Value = match r.json().await {
        Ok(j) => j,
        Err(_) => return None,
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

pub async fn run(http: Client, prices: PriceMap, books: L2OrderbookMap, ts: shared::TechnicalSettings) {
    // Give the websocket routes a full stabilization window before background REST.
    for _ in 0..30 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    // Histories are useful only for symbols shared by at least two venues.
    let exchanges: Vec<String> = prices.iter().map(|e| e.key().clone()).collect();
    let mut venue_counts = std::collections::HashMap::<String, usize>::new();
    for ex in &exchanges {
        if let Some(map) = prices.get(ex.as_str()) {
            for pair in map.keys() { *venue_counts.entry(pair.clone()).or_default() += 1; }
        }
    }
    let mut unique_ex_pairs = std::collections::HashSet::new();
    for ex in &exchanges {
        if let Some(map) = prices.get(ex.as_str()) {
            for pair in map.keys() {
                if venue_counts.get(pair).copied().unwrap_or(0) >= 2 {
                    unique_ex_pairs.insert((ex.clone(), pair.clone()));
                }
            }
        }
    }

    info!(
        "[ema_warmup] Starting EMA warmup: fetching candles for {} unique (exchange, pair) combos (tau={}s)",
        unique_ex_pairs.len(),
        ts.ema_tau_secs
    );

    use dashmap::DashMap;
    let candle_cache: Arc<DashMap<String, Vec<(f64, f64)>>> = Arc::new(DashMap::new());
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let mut fetch_handles = Vec::new();

    for (ex, pair) in unique_ex_pairs {
        let http = http.clone();
        let sem = semaphore.clone();
        let cache = candle_cache.clone();
        let books = books.clone();
        fetch_handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            wait_for_market_headroom(&books).await;
            let candles = fetch_candles_for_with_retry(&http, &ex, &pair).await;
            if !candles.is_empty() {
                cache.insert(format!("{}:{}", ex, pair), candles);
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }));
    }

    for h in fetch_handles {
        let _ = h.await;
    }

    info!("[ema_warmup] Fetched candles. Reconstructing cross-exchange historical premiums...");

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
                let ex_a = ex_a.clone();
                let ex_b = ex_b.clone();
                let ts = ts.clone();
                let cache = candle_cache.clone();

                handles.push(tokio::spawn(async move {
                    let candles_a = cache.get(&format!("{}:{}", ex_a, pair)).map(|v| v.clone()).unwrap_or_default();
                    let candles_b = cache.get(&format!("{}:{}", ex_b, pair)).map(|v| v.clone()).unwrap_or_default();

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

                    warmup_ema_from_premiums(&history, &ts)
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

    // Spawn background re-warmup task — retries unverified pairs every 10 minutes.
    // New listings (e.g. FOLKSUSDT) that had no candle history at startup
    // will be verified as soon as they accumulate 2+ candles on both exchanges.
    let http2 = http.clone();
    let prices2 = prices.clone();
    let books2 = books.clone();
    let ts2 = ts.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(600)).await; // 10 min
            let verified = run_retry_unverified(&http2, &prices2, &books2, &ts2).await;
            if verified > 0 {
                info!("[ema_warmup] 🔄 Background re-warmup verified {} new pair(s)", verified);
            }
        }
    });
}

/// Retry warmup for pairs in STRUCTURAL_EMA that are not yet VERIFIED.
/// Called every 10 minutes so new listings get verified as candle data accumulates.
async fn run_retry_unverified(
    http: &Client,
    prices: &PriceMap,
    books: &L2OrderbookMap,
    ts: &shared::TechnicalSettings,
) -> usize {
    let unverified = unverified_ema_keys();
    if unverified.is_empty() {
        return 0;
    }

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let mut handles = Vec::new();

    for key in unverified {
        // Key format: "SYMBOL|exchange1|exchange2"
        let parts: Vec<&str> = key.splitn(3, '|').collect();
        if parts.len() != 3 { continue; }
        let pair = parts[0].to_string();
        let ex_a = parts[1].to_string();
        let ex_b = parts[2].to_string();

        // Only retry if pair is still present on both exchanges
        let pair_exists = {
            let has_a = prices.get(ex_a.as_str()).map(|m| m.contains_key(pair.as_str())).unwrap_or(false);
            let has_b = prices.get(ex_b.as_str()).map(|m| m.contains_key(pair.as_str())).unwrap_or(false);
            has_a && has_b
        };
        if !pair_exists { continue; }

        let http = http.clone();
        let ts = ts.clone();
        let sem = semaphore.clone();
        let books = books.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            wait_for_market_headroom(&books).await;
            let candles_a = fetch_candles_for_with_retry(&http, &ex_a, &pair).await;
            let candles_b = fetch_candles_for_with_retry(&http, &ex_b, &pair).await;
            if candles_a.is_empty() || candles_b.is_empty() { return 0usize; }
            let merged = merge_candles(&candles_a, &candles_b);
            if merged.len() < 2 { return 0; }
            let is_a_first = ex_a < ex_b;
            let (name1, name2) = if is_a_first { (ex_a.clone(), ex_b.clone()) } else { (ex_b.clone(), ex_a.clone()) };
            let mut history = Vec::with_capacity(merged.len());
            for (ts_val, price_a, price_b) in &merged {
                if *price_a <= 0.0 || *price_b <= 0.0 { continue; }
                let (c1, c2) = if is_a_first { (*price_a, *price_b) } else { (*price_b, *price_a) };
                let p12 = (c1 - c2) / c2 * 100.0;
                let p21 = (c2 - c1) / c1 * 100.0;
                history.push((*ts_val, p12, p21, pair.to_string(), name1.clone(), name2.clone()));
            }
            warmup_ema_from_premiums(&history, &ts)
        }));
    }

    let mut count = 0usize;
    for h in handles {
        if let Ok(n) = h.await { count += n; }
    }
    count
}

// ─── Toobit: fetch 15-min klines (Binance-compatible format) ──────────────────

async fn fetch_toobit_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    let from = now_secs() - LOOKBACK_SECS;
    let url = format!(
        "https://api.toobit.com/quote/v1/klines?symbol={}&interval=15m&startTime={}&limit=100",
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
    // Toobit klines = Binance format: [openTime, open, high, low, close, vol, ...]
    let res: Vec<(f64, f64)> = arr
        .iter()
        .filter_map(|c| {
            let ts = c[0].as_f64()? / 1000.0;
            let close = c[4]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| c[4].as_f64())?;
            Some((ts, close))
        })
        .collect();
    Some(res)
}

// ─── AscendEX: fetch 15-min klines ───────────────────────────────────────────

async fn fetch_ascendex_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    // AscendEX symbol: BTCUSDT → BTC-PERP
    let sym = pair.replace("USDT", "-PERP");
    let from = (now_secs() - LOOKBACK_SECS) * 1000;
    let to = now_secs() * 1000;
    let url = format!(
        "https://ascendex.com/api/pro/v1/barhist?symbol={}&interval=15&from={}&to={}",
        sym, from, to
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
    // AscendEX response: { "code": 0, "data": [{ "m":"bar", "s":"BTC-PERP", "data": { "ts":..., "c":"...", ... } }, ...] }
    let arr = match v["data"].as_array() {
        Some(a) => a,
        None => return Some(vec![]),
    };
    let res: Vec<(f64, f64)> = arr
        .iter()
        .filter_map(|c| {
            let d = &c["data"];
            let ts = d["ts"].as_i64()? as f64 / 1000.0;
            let close = d["c"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| d["c"].as_f64())?;
            Some((ts, close))
        })
        .collect();
    Some(res)
}

// ─── BitMEX: fetch 5-min klines (no 15m available) ───────────────────────────

async fn fetch_bitmex_candles(http: &Client, pair: &str) -> Option<Vec<(f64, f64)>> {
    // BitMEX symbol: BTCUSDT → XBTUSDT (only for BTC)
    let sym = if pair.starts_with("BTC") {
        pair.replacen("BTC", "XBT", 1)
    } else {
        pair.to_string()
    };
    let start_time = chrono_format_secs(now_secs() - LOOKBACK_SECS);
    let url = format!(
        "https://www.bitmex.com/api/v1/trade/bucketed?binSize=5m&symbol={}&count=100&startTime={}&reverse=false",
        sym, start_time
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
    // BitMEX response: [{ "timestamp": "2026-...", "close": 65600.1, ... }]
    let res: Vec<(f64, f64)> = arr
        .iter()
        .filter_map(|c| {
            let ts_str = c["timestamp"].as_str()?;
            let ts = parse_iso_timestamp(ts_str)?;
            let close = c["close"].as_f64()?;
            if close <= 0.0 { return None; }
            Some((ts, close))
        })
        .collect();
    Some(res)
}

/// Format epoch secs as ISO 8601 string for BitMEX API
fn chrono_format_secs(secs: u64) -> String {
    // Simple ISO 8601 without chrono dependency
    let s = secs;
    let days = s / 86400;
    let rem = s % 86400;
    let h = rem / 3600;
    let m = (rem % 3600) / 60;
    let sec = rem % 60;
    // Epoch = 1970-01-01. Simple conversion for recent dates.
    let mut y = 1970i64;
    let mut d = days as i64;
    loop {
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let yd = if leap { 366 } else { 365 };
        if d < yd { break; }
        d -= yd;
        y += 1;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let mdays = [31, if leap {29} else {28}, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut mo = 0usize;
    for i in 0..12 {
        if d < mdays[i] { mo = i; break; }
        d -= mdays[i];
    }
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.000Z", y, mo + 1, d + 1, h, m, sec)
}

/// Parse ISO 8601 timestamp string to epoch seconds
fn parse_iso_timestamp(s: &str) -> Option<f64> {
    // Expected: "2026-06-17T07:00:00.000Z"
    let y: i64 = s.get(0..4)?.parse().ok()?;
    let mo: i64 = s.get(5..7)?.parse().ok()?;
    let d: i64 = s.get(8..10)?.parse().ok()?;
    let h: i64 = s.get(11..13)?.parse().ok()?;
    let mi: i64 = s.get(14..16)?.parse().ok()?;
    let sec: i64 = s.get(17..19)?.parse().ok()?;

    // Days from epoch to year start
    let mut days: i64 = 0;
    for yr in 1970..y {
        let leap = yr % 4 == 0 && (yr % 100 != 0 || yr % 400 == 0);
        days += if leap { 366 } else { 365 };
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let mdays = [31, if leap {29} else {28}, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for i in 0..(mo - 1) as usize {
        days += mdays[i];
    }
    days += d - 1;

    Some((days * 86400 + h * 3600 + mi * 60 + sec) as f64)
}

// ─── Dispatch to the right exchange API ──────────────────────────────────────

async fn fetch_candles_for(http: &Client, exchange: &str, pair: &str) -> Option<Vec<(f64, f64)>> {
    match exchange {
        "gate" => fetch_gate_candles(http, pair).await,
        "kucoin" => fetch_kucoin_candles(http, pair).await,
        "htx" => fetch_htx_candles(http, pair).await,
        "binance" => fetch_binance_candles(http, pair).await,
        "bybit" => fetch_bybit_candles(http, pair).await,
        "bingx" => fetch_bingx_candles(http, pair).await,
        "bitget" => fetch_bitget_candles(http, pair).await,
        "xt" => fetch_xt_candles(http, pair).await,
        "toobit" => fetch_toobit_candles(http, pair).await,
        "ascendex" => fetch_ascendex_candles(http, pair).await,
        "bitmex" => fetch_bitmex_candles(http, pair).await,
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
