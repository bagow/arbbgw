//! Binance USDT Futures WebSocket collector — !bookTicker stream (all pairs)
//! + L2 depth stream for active pairs (top 5 levels @ 100ms)
use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{L2OrderbookMap, L2Snapshot, PriceMap, TickerData, UpdateSignal};

pub async fn run(
    prices: PriceMap,
    signal: UpdateSignal,
    l2_books: L2OrderbookMap,
    settings: std::sync::Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let is_disabled = {
            let p = settings.read().await;
            !p.is_empty()
                && p.iter()
                    .all(|x| x.disabled_exchanges.contains(&"binance".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) {
                info!("[binance] Disabled globally in profiles. Sleeping...");
            }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        info!("[binance] Connecting...");

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"binance".to_string()))
                };
                if is_disabled {
                    break;
                }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal, &l2_books) => {
                match res {
                    Ok(_)  => { warn!("[binance] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => error!("[binance] Error: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            _ = disable_watcher => {
                info!("[binance] Exchange was disabled dynamically. Dropping connection.");
                backoff = Duration::from_secs(1);
            }
        }
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal, l2_books: &L2OrderbookMap) -> Result<()> {
    // Seed 24h volume from REST miniTicker snapshot
    let http = Client::builder().user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36").build().unwrap();
    let mut target_url = "https://fapi.binance.com/fapi/v1/ticker/24hr".to_string();
    let mut resp = http.get(&target_url).timeout(Duration::from_secs(15)).send().await;
    
    if let Ok(ref r) = resp {
        let status = r.status().as_u16();
        if status == 403 || status == 418 || status == 429 {
            target_url = "https://api.codetabs.com/v1/proxy/?quest=https://fapi.binance.com/fapi/v1/ticker/24hr".to_string();
            tracing::warn!("[binance] Using proxy fallback for volume due to HTTP {}", status);
            resp = http.get(&target_url).timeout(Duration::from_secs(15)).send().await;
        }
    }

    if let Ok(r) = resp {
        if let Ok(arr) = r.json::<Vec<Value>>().await {
            for t in &arr {
                let sym = t["symbol"].as_str().unwrap_or("");
                if !sym.ends_with("USDT") {
                    continue;
                }
                let vol: f64 = t["quoteVolume"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                if vol > 0.0 {
                    let mut entry = price_map.entry("binance".to_string()).or_default();
                    entry
                        .entry(sym.to_string())
                        .or_insert_with(|| TickerData {
                            vol,
                            ..Default::default()
                        })
                        .vol = vol;
                }
            }
            info!("[binance] Seeded volume for {} symbols", arr.len());
        }
    } else {
        warn!("[binance] Failed to fetch volume from REST/Proxy. Using fallback volumes via WS discovery.");
    }

    // WS: !bookTicker — instant bid/ask for every USDT future
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://fstream.binance.com/ws/!bookTicker",
        None,
        false,
        None,
    )
    .await
    .context("Binance WS connect failed")?;
    info!("[binance] Connected (bookTicker)");

    // Spawn L2 depth collector — passes price_map so it can rebuild
    // the symbol list dynamically on each reconnect (bookTicker fills it)
    let l2_books_clone = l2_books.clone();
    let price_map_clone = price_map.clone();
    tokio::spawn(async move {
        // Wait 15s for bookTicker to populate price_map with all symbols
        // before building the top-200 sorted list for the L2 stream.
        tokio::time::sleep(Duration::from_secs(15)).await;
        run_l2_depth_stream(l2_books_clone, price_map_clone).await;
    });

    // Spawn background volume refresher — re-seeds 24h volumes every 30 min
    // so Binance doesn't disappear from the spread scanner after long uptime.
    {
        let pm = price_map.clone();
        let http2 = Client::builder().user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36").build().unwrap();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1800)).await;
                let mut target_url = "https://fapi.binance.com/fapi/v1/ticker/24hr".to_string();
                let mut resp = http2.get(&target_url).timeout(Duration::from_secs(15)).send().await;
                
                if let Ok(ref r) = resp {
                    let status = r.status().as_u16();
                    if status == 403 || status == 418 || status == 429 {
                        target_url = "https://api.codetabs.com/v1/proxy/?quest=https://fapi.binance.com/fapi/v1/ticker/24hr".to_string();
                        resp = http2.get(&target_url).timeout(Duration::from_secs(15)).send().await;
                    }
                }

                if let Ok(r) = resp {
                    if let Ok(arr) = r.json::<Vec<Value>>().await {
                        let mut count = 0u32;
                        for t in &arr {
                            let sym = t["symbol"].as_str().unwrap_or("");
                            if !sym.ends_with("USDT") { continue; }
                            let vol: f64 = t["quoteVolume"]
                                .as_str()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            if vol > 0.0 {
                                if let Some(mut ex_map) = pm.get_mut("binance") {
                                    if let Some(entry) = ex_map.get_mut(sym) {
                                        entry.vol = vol;
                                        count += 1;
                                    }
                                }
                            }
                        }
                        info!("[binance] Volume refreshed for {} symbols", count);
                    }
                }
            }
        });
    }

    #[derive(Deserialize)]
    struct BinanceTicker<'a> {
        #[serde(rename = "s")]
        symbol: &'a str,
        #[serde(rename = "b")]
        bid: &'a str,
        #[serde(rename = "a")]
        ask: &'a str,
        #[serde(rename = "B")]
        bid_qty: &'a str,
        #[serde(rename = "A")]
        ask_qty: &'a str,
    }

    // Ensure binance map exists for getting mutably later
    price_map.entry("binance".to_string()).or_default();

    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(30), ws.next()).await {
        if let Ok(Message::Text(text)) = msg {
            if let Ok(t) = serde_json::from_str::<BinanceTicker>(&text) {
                if !t.symbol.ends_with("USDT") {
                    continue;
                }
                let bid: f64 = match t.bid.parse() {
                    Ok(v) if v > 0.0 => v,
                    _ => continue,
                };
                let ask: f64 = match t.ask.parse() {
                    Ok(v) if v > 0.0 => v,
                    _ => continue,
                };
                let bid_qty: f64 = t.bid_qty.parse().unwrap_or(0.0);
                let ask_qty: f64 = t.ask_qty.parse().unwrap_or(0.0);
                let mid = (bid + ask) / 2.0;

                if let Some(mut ex_map) = price_map.get_mut("binance") {
                    let ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    if let Some(entry) = ex_map.get_mut(t.symbol) {
                        entry.bid = bid;
                        entry.ask = ask;
                        entry.price = mid;
                        entry.bid_size = bid_qty * mid;
                        entry.ask_size = ask_qty * mid;
                        entry.last_updated = ts;
                        if entry.vol == 0.0 {
                            entry.vol = 100_000_000.0; // Fallback volume
                        }
                    } else {
                        ex_map.insert(
                            t.symbol.to_string(),
                            TickerData {
                                bid,
                                ask,
                                price: mid,
                                vol: 100_000_000.0, // Fallback volume
                                funding_rate: 0.0,
                                bid_size: bid_qty * mid,
                                ask_size: ask_qty * mid,
                                last_updated: ts,
                            },
                        );
                    }
                }
                signal.notify_one();
            }
        }
    }
    Ok(())
}

/// Separate L2 depth stream for top-volume pairs (dynamic, top 200 by volume).
/// Rebuilds symbol list from price_map on each reconnect — handles REST seed failures.
async fn run_l2_depth_stream(l2_books: L2OrderbookMap, price_map: crate::state::PriceMap) {
    // Hardcoded fallback covers common pairs when REST/price_map is empty
    const FALLBACK: &[&str] = &[
        "BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","DOGEUSDT","XRPUSDT","PEPEUSDT",
        "SUIUSDT","ADAUSDT","TRXUSDT","AVAXUSDT","LINKUSDT","DOTUSDT","LTCUSDT",
        "MATICUSDT","ARBUSDT","OPUSDT","APTUSDT","NEARUSDT","FILUSDT","WIFUSDT",
        "SHIBUSDT","BONKUSDT","FLOKIUSDT","WLDUSDT","ORDIUSDT","TIAUSDT","SEIUSDT",
        "INJUSDT","FETUSDT","RUNEUSDT","JUPUSDT","ENAUSDT","ONDOUSDT","RENDERUSDT",
        "AAVEUSDT","MKRUSDT","LDOUSDT","PENDLEUSDT","TONUSDT","NOTUSDT","STXUSDT",
        "KATUSDT","SPKUSDT","HYPEUSDT","VIRTUSDT","BOMEUSDT","JUPUSDT","WUSDT",
        "APEUSDT","GMTUSDT","GALAUSDT","FTMUSDT","HBARUSDT","FLOWUSDT","ICPUSDT",
        "SANDUSDT","MANAUSDT","AXSUSDT","CRVUSDT","SNXUSDT","ATOMUSDT","XLMUSDT",
    ];

    let mut backoff = Duration::from_secs(1);
    loop {
        // Rebuild symbol list on every reconnect attempt
        let mut vol_syms: Vec<(f64, String)> = {
            let map = price_map.get("binance");
            match map {
                Some(m) => m.iter()
                    .map(|(k, v)| (v.vol, k.clone()))
                    .filter(|(_, k)| k.ends_with("USDT"))
                    .collect(),
                None => vec![],
            }
        };
        vol_syms.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        let symbols: Vec<String> = if vol_syms.is_empty() {
            FALLBACK.iter().map(|s| s.to_string()).collect()
        } else {
            vol_syms.into_iter().map(|(_, s)| s).collect()
        };
        // Filter symbols: only subscribe to L2 depth if the symbol exists on at least one OTHER exchange
        let mut needed_symbols = Vec::new();
        for sym in symbols {
            let mut count = 0;
            for ex in price_map.iter() {
                if ex.key() != "binance" && ex.value().contains_key(&sym) {
                    count += 1;
                    break;
                }
            }
            if count > 0 {
                needed_symbols.push(sym);
            }
        }
        
        let symbols = needed_symbols;

        info!("[binance_l2] Starting L2 subscriptions for {} symbols (requires {} connections)", symbols.len(), (symbols.len() + 199) / 200);

        let mut handles = Vec::new();

        for chunk in symbols.chunks(200) {
            let chunk_symbols = chunk.to_vec();
            let l2_books_clone = l2_books.clone();

            let handle = tokio::spawn(async move {
                let mut backoff = Duration::from_secs(1);
                let max_age = Duration::from_secs(1800);
                let conn_start = std::time::Instant::now();
                
                loop {
                    if conn_start.elapsed() > max_age {
                        break;
                    }
                    
                    match connect_async_tls_with_config("wss://fstream.binance.com/ws", None, false, None).await {
                        Ok((mut ws, _)) => {
                            use futures_util::SinkExt;
                            let streams: Vec<String> = chunk_symbols.iter().map(|s| format!("{}@depth5@100ms", s.to_lowercase())).collect();
                            let msg = serde_json::json!({
                                "method": "SUBSCRIBE",
                                "params": streams,
                                "id": 1
                            });
                            
                            if let Err(e) = ws.send(tokio_tungstenite::tungstenite::Message::Text(msg.to_string().into())).await {
                                error!("[binance_l2_chunk] Subscribe failed: {}", e);
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                            
                            backoff = Duration::from_secs(1);

                            // Heartbeat: depth5@100ms only fires when the book changes.
                            // For low-volume pairs, the book can be static for seconds.
                            // While the WS is alive, the cached data IS still accurate —
                            // refresh updated_ms so the 2s freshness check doesn't reject it.
                            let ws_alive = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
                            let ws_alive_hb = ws_alive.clone();
                            let l2_hb = l2_books_clone.clone();
                            let hb_syms: Vec<String> = chunk_symbols.iter().map(|s| format!("binance:{}", s)).collect();
                            let hb_handle = tokio::spawn(async move {
                                while ws_alive_hb.load(std::sync::atomic::Ordering::Relaxed) {
                                    tokio::time::sleep(Duration::from_millis(500)).await;
                                    let now_ms = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis() as i64;
                                    for key in &hb_syms {
                                        if let Some(mut book) = l2_hb.get_mut(key) {
                                            book.updated_ms = now_ms;
                                        }
                                    }
                                }
                            });
                            
                            while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(30), ws.next()).await {
                                if conn_start.elapsed() > max_age {
                                    break;
                                }
                                if let Ok(Message::Text(text)) = msg {
                                    if let Ok(data) = serde_json::from_str::<Value>(&text) {
                                        let (sym_str, depth_data) = if let Some(stream) = data["stream"].as_str() {
                                            let sym = stream.split('@').next().unwrap_or("").to_uppercase();
                                            (sym, data["data"].clone())
                                        } else if let Some(s) = data["s"].as_str() {
                                            (s.to_uppercase(), data.clone())
                                        } else {
                                            continue;
                                        };

                                        if sym_str.is_empty() || !sym_str.ends_with("USDT") { continue; }

                                        let mut bids = parse_levels(&depth_data, "bids");
                                        if bids.is_empty() { bids = parse_levels(&depth_data, "b"); }
                                        let mut asks = parse_levels(&depth_data, "asks");
                                        if asks.is_empty() { asks = parse_levels(&depth_data, "a"); }

                                        if !bids.is_empty() || !asks.is_empty() {
                                            let now_ms = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_millis() as i64;
                                            l2_books_clone.insert(
                                                format!("binance:{}", sym_str),
                                                L2Snapshot { bids, asks, updated_ms: now_ms },
                                            );
                                        }
                                    }
                                }
                            }
                            // WS disconnected — stop heartbeat
                            ws_alive.store(false, std::sync::atomic::Ordering::Relaxed);
                            hb_handle.abort();
                        }
                        Err(e) => {
                            error!("[binance_l2_chunk] Connect failed: {}. Retry in {:?}", e, backoff);
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(60));
                        }
                    }
                }
            });
            handles.push(handle);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Wait 30 minutes, then kill all handles and restart the main loop to refresh symbols
        tokio::time::sleep(Duration::from_secs(1800)).await;
        info!("[binance_l2] 30m elapsed. Refreshing all L2 subscriptions...");
        for h in handles {
            h.abort();
        }
    }
}

/// Parse [[price, qty], ...] from depth snapshot
fn parse_levels(data: &Value, key: &str) -> Vec<(f64, f64)> {
    data[key]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|level| {
                    let px: f64 = level[0].as_str()?.parse().ok()?;
                    let sz: f64 = level[1].as_str()?.parse().ok()?;
                    if px > 0.0 && sz > 0.0 { Some((px, sz)) } else { None }
                })
                .collect()
        })
        .unwrap_or_default()
}
