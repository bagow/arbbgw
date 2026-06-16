//! BingX USDT-M Perpetual WebSocket collector
//! Public WS: wss://open-api-swap.bingx.com/swap-market
//! Subscribes to ticker and depth channels for all USDT pairs
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
            !p.is_empty() && p.iter().all(|x| x.disabled_exchanges.contains(&"bingx".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) { info!("[bingx] Disabled globally. Sleeping..."); }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let dis = settings.read().await;
                if !dis.is_empty() && dis.iter().all(|x| x.disabled_exchanges.contains(&"bingx".to_string())) { break; }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal, &l2_books) => {
                match res {
                    Ok(_) => { warn!("[bingx] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => error!("[bingx] Error: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            _ = disable_watcher => { info!("[bingx] disabled dynamically."); backoff = Duration::from_secs(1); }
        }
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal, l2_books: &L2OrderbookMap) -> Result<()> {
    let http = Client::builder().user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36").build().unwrap();

    // Seed from REST snapshot
    if let Ok(resp) = http
        .get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts")
        .timeout(Duration::from_secs(15))
        .send().await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(list) = body["data"].as_array() {
                let mut seeded = 0usize;
                for t in list {
                    let sym = t["symbol"].as_str().unwrap_or("").replace('-', "");
                    if !sym.ends_with("USDT") { continue; }
                    price_map.entry("bingx".to_string()).or_default().entry(sym).or_default();
                    seeded += 1;
                }
                info!("[bingx] Seeded {} symbols from REST", seeded);
            }
        }
    }

    // Seed ticker prices from REST
    if let Ok(resp) = http
        .get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker")
        .timeout(Duration::from_secs(15))
        .send().await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(list) = body["data"].as_array() {
                for t in list {
                    let sym = t["symbol"].as_str().unwrap_or("").replace('-', "");
                    if !sym.ends_with("USDT") { continue; }
                    let bid: f64 = t["bestBidPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let ask: f64 = t["bestAskPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let price: f64 = t["lastPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let vol: f64 = t["quoteVolume"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    if price > 0.0 {
                        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                        price_map.entry("bingx".to_string()).or_default().insert(sym, TickerData {
                            bid, ask, price, vol, funding_rate: 0.0,
                            bid_size: 0.0, ask_size: 0.0, last_updated: ts,
                        });
                    }
                }
            }
        }
    }

    let symbols: Vec<String> = {
        let map = price_map.get("bingx");
        match map { Some(m) => m.keys().cloned().collect(), None => vec![] }
    };

    if symbols.is_empty() { return Err(anyhow::anyhow!("No BingX symbols loaded")); }

    let (mut ws, _) = connect_async_tls_with_config(
        "wss://open-api-swap.bingx.com/swap-market",
        None, false, None,
    ).await.context("BingX WS connect failed")?;
    info!("[bingx] Connected, subscribing {} symbols...", symbols.len());

    price_map.entry("bingx".to_string()).or_default();



    // Subscribe in chunks of 30
    for chunk in symbols.chunks(30) {
        let subs: Vec<Value> = chunk.iter().flat_map(|s| {
            let ws_sym = if s.ends_with("USDT") && !s.contains('-') {
                format!("{}-USDT", &s[..s.len()-4])
            } else { s.clone() };
            let mut res = vec![json!({"id": uuid_lite(), "dataType": format!("{}.{}","MARKET.PRICE_FEED", ws_sym)})];
            res.push(json!({"id": uuid_lite(), "dataType": format!("{}.{}","MARKET.DEPTH.step0", ws_sym)}));
            res
        }).collect();
        for sub in subs {
            ws.send(Message::Text(sub.to_string().into())).await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    info!("[bingx] Subscribed L1 tickers & L2 depth for {} symbols", symbols.len());

    let mut ping = tokio::time::interval(Duration::from_secs(15));
    
    // Reconnect after 30 min to refresh L2 subscription list (new cross-exchange pairs may appear)
    let conn_start = std::time::Instant::now();
    let max_conn_age = Duration::from_secs(1800);

    loop {
        if conn_start.elapsed() > max_conn_age {
            info!("[bingx] Refreshing L2 subscriptions after 30min...");
            break;
        }
        tokio::select! {
            msg = tokio::time::timeout(Duration::from_secs(35), ws.next()) => match msg.ok().flatten() {
                Some(Ok(msg)) => {
                    let mut text = String::new();
                    match msg {
                        Message::Text(t) => text = t,
                        Message::Binary(b) => {
                            use std::io::Read;
                            let mut decoder = flate2::read::GzDecoder::new(&b[..]);
                            if let Err(_) = decoder.read_to_string(&mut text) {
                                continue;
                            }
                        }
                        Message::Close(_) => break,
                        _ => continue,
                    }
                    if text == "Ping" {
                        let _ = ws.send(Message::Text("Pong".into())).await;
                        continue;
                    }
                    if text.contains("\"Ping\"") || text.contains("\"ping\"") {
                        let _ = ws.send(Message::Text("Pong".into())).await;
                        continue;
                    }
                    if text.is_empty() { continue; }
                    if text == "Pong" { continue; }
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        let data_type = v["dataType"].as_str().unwrap_or("");
                        // MARKET.PRICE_FEED.BTC-USDT
                        if data_type.starts_with("MARKET.DEPTH.step0.") {
                            let sym_raw = data_type.trim_start_matches("MARKET.DEPTH.step0.").replace('-', "");
                            if sym_raw.ends_with("USDT") {
                                let bids: Vec<(f64, f64)> = v["data"]["bids"].as_array().map(|arr| {
                                    arr.iter().filter_map(|x| {
                                        let p = x[0].as_str()?.parse().ok()?;
                                        let q = x[1].as_str()?.parse().ok()?;
                                        Some((p, q))
                                    }).collect()
                                }).unwrap_or_default();
                                let asks: Vec<(f64, f64)> = v["data"]["asks"].as_array().map(|arr| {
                                    arr.iter().filter_map(|x| {
                                        let p = x[0].as_str()?.parse().ok()?;
                                        let q = x[1].as_str()?.parse().ok()?;
                                        Some((p, q))
                                    }).collect()
                                }).unwrap_or_default();
                                if !bids.is_empty() || !asks.is_empty() {
                                    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
                                    // BUG-3 FIX: Update L1 depth (bid_size/ask_size) from L2 data
                                    // so scanner depth filter sees real BingX liquidity.
                                    // Other collectors do this inline; BingX was missing it entirely.
                                    if let Some(mut ex_map) = price_map.get_mut("bingx") {
                                        if let Some(entry) = ex_map.get_mut(&sym_raw) {
                                            if let Some(&(bp, bq)) = bids.first() {
                                                entry.bid_size = bq * bp; // USD depth at best bid
                                            }
                                            if let Some(&(ap, aq)) = asks.first() {
                                                entry.ask_size = aq * ap; // USD depth at best ask
                                            }
                                        }
                                    }
                                    l2_books.insert(format!("bingx:{}", sym_raw), crate::state::L2Snapshot { bids, asks, updated_ms: now_ms });
                                }
                            }
                        }
                        if data_type.starts_with("MARKET.PRICE_FEED.") {
                            let sym_raw = data_type.trim_start_matches("MARKET.PRICE_FEED.").replace('-', "");
                            if sym_raw.ends_with("USDT") {
                                let bid: f64 = v["data"]["bidPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                let ask: f64 = v["data"]["askPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                let last: f64 = v["data"]["lastPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                if bid <= 0.0 || ask <= 0.0 { continue; }
                                let mid = (bid + ask) / 2.0;
                                let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                                if let Some(mut ex_map) = price_map.get_mut("bingx") {
                                    if let Some(entry) = ex_map.get_mut(&sym_raw) {
                                        entry.bid = bid; entry.ask = ask;
                                        entry.price = if last > 0.0 { last } else { mid };
                                        entry.last_updated = ts;
                                    } else {
                                        ex_map.insert(sym_raw.clone(), TickerData {
                                            bid, ask, price: if last > 0.0 { last } else { mid },
                                            vol: 0.0, funding_rate: 0.0,
                                            bid_size: 0.0, ask_size: 0.0, last_updated: ts,
                                        });
                                    }
                                }
                                signal.notify_one();
                            }
                        }
                    }
                }
                None | Some(Err(_)) => break,
                _ => {}
            },
            _ = ping.tick() => {
                let _ = ws.send(Message::Ping(vec![])).await;
                // WS heartbeat: update last_updated for all pairs with valid prices.
                // If the WS is alive, the exchange would notify us of price changes —
                // so existing bid/ask data is as current as the last received update.
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if let Some(mut ex_map) = price_map.get_mut("bingx") {
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

fn uuid_lite() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    format!("{:x}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos())
}
