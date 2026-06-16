//! AscendEX WebSocket collector
//! Public WS: wss://ascendex.com/api/pro/v2/stream
//! Subscribes to bbo (best bid/offer) channel for USDT perp pairs
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{L2OrderbookMap, PriceMap, TickerData, UpdateSignal};

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
            !p.is_empty() && p.iter().all(|x| x.disabled_exchanges.contains(&"ascendex".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) { info!("[ascendex] Disabled globally. Sleeping..."); }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let dis = settings.read().await;
                if !dis.is_empty() && dis.iter().all(|x| x.disabled_exchanges.contains(&"ascendex".to_string())) { break; }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal, &l2_books) => {
                match res {
                    Ok(_) => { warn!("[ascendex] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => error!("[ascendex] Error: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
            _ = disable_watcher => { info!("[ascendex] disabled dynamically."); backoff = Duration::from_secs(1); }
        }
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal, l2_books: &L2OrderbookMap) -> Result<()> {
    let http = Client::builder().user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36").build().unwrap();

    // Seed from REST: get all futures contracts
    // API returns: symbol="BTC-PERP", displayName="BTCUSDT", settlementAsset="USDT"
    let mut ws_symbols: Vec<String> = Vec::new(); // "BTC-PERP" format for WS
    let mut sym_map: std::collections::HashMap<String, String> = std::collections::HashMap::new(); // "BTC-PERP" → "BTCUSDT"
    if let Ok(resp) = http
        .get("https://ascendex.com/api/pro/v2/futures/contract")
        .timeout(Duration::from_secs(15))
        .send().await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(arr) = body["data"].as_array() {
                let mut seeded = 0usize;
                for c in arr {
                    let ws_sym = c["symbol"].as_str().unwrap_or(""); // "BTC-PERP"
                    let std_sym = c["displayName"].as_str().unwrap_or(""); // "BTCUSDT"
                    let settlement = c["settlementAsset"].as_str().unwrap_or("");
                    if settlement != "USDT" || std_sym.is_empty() || ws_sym.is_empty() { continue; }
                    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                    price_map.entry("ascendex".to_string()).or_default().entry(std_sym.to_string()).or_insert(TickerData {
                        bid: 0.0, ask: 0.0, price: 0.0, vol: 0.0, funding_rate: 0.0,
                        bid_size: 0.0, ask_size: 0.0, last_updated: ts,
                    });
                    sym_map.insert(ws_sym.to_string(), std_sym.to_string());
                    ws_symbols.push(ws_sym.to_string());
                    seeded += 1;
                }
                info!("[ascendex] Seeded {} futures symbols from REST", seeded);
            }
        }
    }

    // Seed 24h volume from REST tickers
    if let Ok(resp) = http
        .get("https://ascendex.com/api/pro/v1/futures/ticker")
        .timeout(Duration::from_secs(15))
        .send().await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(arr) = body["data"].as_array() {
                let mut vol_seeded = 0usize;
                for t in arr {
                    let ws_sym = t["symbol"].as_str().unwrap_or(""); // "BTC-PERP"
                    let std_sym = match sym_map.get(ws_sym) {
                        Some(s) => s.clone(),
                        None => continue,
                    };
                    let base_vol: f64 = t["baseVol"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let close: f64 = t["close"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let bid: f64 = t["bid"].as_array().and_then(|a| a.first()).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let ask: f64 = t["ask"].as_array().and_then(|a| a.first()).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let bid_sz: f64 = t["bid"].as_array().and_then(|a| a.get(1)).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let ask_sz: f64 = t["ask"].as_array().and_then(|a| a.get(1)).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let quote_vol = base_vol * close;
                    if quote_vol > 0.0 {
                        let mid = if bid > 0.0 && ask > 0.0 { (bid + ask) / 2.0 } else { close };
                        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                        if let Some(mut ex_map) = price_map.get_mut("ascendex") {
                            if let Some(entry) = ex_map.get_mut(&std_sym) {
                                entry.vol = quote_vol;
                                if bid > 0.0 && ask > 0.0 {
                                    entry.bid = bid; entry.ask = ask; entry.price = mid;
                                    entry.bid_size = bid_sz * mid; entry.ask_size = ask_sz * mid;
                                    entry.last_updated = ts;
                                }
                            }
                        }
                        vol_seeded += 1;
                    }
                }
                info!("[ascendex] Seeded 24h volume for {} symbols", vol_seeded);
            }
        }
    }

    if ws_symbols.is_empty() { return Err(anyhow::anyhow!("No AscendEX USDT futures symbols")); }

    let (mut ws, _) = connect_async_tls_with_config(
        "wss://ascendex.com/api/pro/v2/stream",
        None, false, None,
    ).await.context("AscendEX WS connect failed")?;
    info!("[ascendex] Connected, subscribing {} symbols...", ws_symbols.len());

    // Subscribe to bbo channel in chunks
    for chunk in ws_symbols.chunks(20) {
        let args: Vec<String> = chunk.iter().map(|s| format!("futures/bbo:{}", s)).collect();
        ws.send(Message::Text(json!({"op":"sub","ch": args}).to_string().into())).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    price_map.entry("ascendex".to_string()).or_default();

    let mut ping = tokio::time::interval(Duration::from_secs(25));

    loop {
        tokio::select! {
            msg = tokio::time::timeout(Duration::from_secs(35), ws.next()) => match msg.ok().flatten() {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        let m = v["m"].as_str().unwrap_or("");
                        if m == "futures/bbo" {
                            if let Some(data) = v["data"].as_object() {
                                let sym_raw = data["s"].as_str().unwrap_or(""); // "BTC-PERP"
                                let std_sym = match sym_map.get(sym_raw) {
                                    Some(s) => s.clone(),
                                    None => continue, // unknown symbol
                                };
                                let bid: f64 = data["b"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                let ask: f64 = data["a"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                let bid_sz: f64 = data["bq"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                let ask_sz: f64 = data["aq"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                if bid <= 0.0 || ask <= 0.0 { continue; }
                                let mid = (bid + ask) / 2.0;
                                let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                                if let Some(mut ex_map) = price_map.get_mut("ascendex") {
                                    if let Some(entry) = ex_map.get_mut(&std_sym) {
                                        entry.bid = bid; entry.ask = ask; entry.price = mid;
                                        entry.bid_size = bid_sz * mid; entry.ask_size = ask_sz * mid;
                                        entry.last_updated = ts;
                                    } else {
                                        ex_map.insert(std_sym.clone(), TickerData {
                                            bid, ask, price: mid, vol: 0.0, funding_rate: 0.0,
                                            bid_size: bid_sz * mid, ask_size: ask_sz * mid, last_updated: ts,
                                        });
                                    }
                                }
                                let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
                                l2_books.insert(
                                    format!("ascendex:{}", std_sym),
                                    crate::state::L2Snapshot {
                                        bids: vec![(bid, bid_sz)],
                                        asks: vec![(ask, ask_sz)],
                                        updated_ms: now_ms,
                                    }
                                );
                                signal.notify_one();
                            }
                        }
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => {
                ws.send(Message::Text(json!({"op":"ping"}).to_string().into())).await?;
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default().as_secs();
                if let Some(mut ex_map) = price_map.get_mut("ascendex") {
                    for (_, entry) in ex_map.iter_mut() {
                        if entry.bid > 0.0 && entry.ask > 0.0 {
                            entry.last_updated = now_ts;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
