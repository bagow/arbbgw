//! BingX USDT-M Perpetual WebSocket collector
//! Public WS: wss://open-api-swap.bingx.com/swap-market
//! Subscribes to ticker and depth channels for all USDT pairs
//! Uses sharding to avoid WS connection limits (~100 symbols per shard)
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{L2OrderbookMap, PriceMap, TickerData, UpdateSignal};

const SHARD_SIZE: usize = 100;

pub async fn run(
    prices: PriceMap,
    signal: UpdateSignal,
    l2_books: L2OrderbookMap,
    settings: std::sync::Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    loop {
        let is_disabled = {
            let p = settings.read().await;
            !p.is_empty() && p.iter().all(|x| x.disabled_exchanges.contains(&"bingx".to_string()))
        };
        if is_disabled {
            info!("[bingx] Disabled globally. Sleeping...");
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }

        // Seed symbols from REST
        let symbols = seed_symbols(&prices).await;
        if symbols.is_empty() {
            error!("[bingx] No symbols loaded from REST. Retrying in 30s...");
            tokio::time::sleep(Duration::from_secs(30)).await;
            continue;
        }

        // Split into shards
        let shards: Vec<Vec<String>> = symbols.chunks(SHARD_SIZE).map(|c| c.to_vec()).collect();
        info!("[bingx] Starting {} shards for {} symbols", shards.len(), symbols.len());

        // Spawn all shards as tasks
        let mut handles = Vec::new();
        for (idx, shard_symbols) in shards.into_iter().enumerate() {
            let pm = prices.clone();
            let sig = signal.clone();
            let l2 = l2_books.clone();
            let stgs = settings.clone();
            handles.push(tokio::spawn(async move {
                run_shard(idx, shard_symbols, pm, sig, l2, stgs).await;
            }));
        }

        // Wait for any shard to exit (means we need full restart)
        // Use a timeout to periodically re-check disabled status
        tokio::select! {
            _ = futures_util::future::join_all(handles) => {
                warn!("[bingx] All shards exited. Restarting...");
            }
            _ = async {
                loop {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    let dis = settings.read().await;
                    if !dis.is_empty() && dis.iter().all(|x| x.disabled_exchanges.contains(&"bingx".to_string())) {
                        break;
                    }
                }
            } => {
                info!("[bingx] Disabled dynamically.");
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_shard(
    shard_idx: usize,
    symbols: Vec<String>,
    prices: PriceMap,
    signal: UpdateSignal,
    l2_books: L2OrderbookMap,
    _settings: std::sync::Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        match shard_handler(shard_idx, &symbols, &prices, &signal, &l2_books).await {
            Ok(_) => {
                warn!("[bingx] Shard {} WS closed. Reconnecting...", shard_idx);
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                error!("[bingx] Shard {} error: {}. Reconnecting in {:?}", shard_idx, e, backoff);
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

async fn seed_symbols(price_map: &PriceMap) -> Vec<String> {
    let http = Client::builder()
        .user_agent("Mozilla/5.0")
        .build()
        .unwrap();

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
                info!("[bingx] Seeded {} symbols from REST contracts", seeded);
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

    let map = price_map.get("bingx");
    match map { Some(m) => m.keys().cloned().collect(), None => vec![] }
}

async fn shard_handler(
    shard_idx: usize,
    symbols: &[String],
    price_map: &PriceMap,
    signal: &UpdateSignal,
    l2_books: &L2OrderbookMap,
) -> Result<()> {
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://open-api-swap.bingx.com/swap-market",
        None, false, None,
    ).await.context("BingX WS connect failed")?;
    info!("[bingx] Shard {} connected, subscribing to {} symbols", shard_idx, symbols.len());

    // Subscribe in chunks of 20
    for chunk in symbols.chunks(20) {
        let subs: Vec<Value> = chunk.iter().flat_map(|s| {
            let ws_sym = if s.ends_with("USDT") && !s.contains('-') {
                format!("{}-USDT", &s[..s.len()-4])
            } else { s.clone() };
            vec![
                json!({"id": uuid_lite(), "dataType": format!("MARKET.PRICE_FEED.{}", ws_sym)}),
                json!({"id": uuid_lite(), "dataType": format!("MARKET.DEPTH.step0.{}", ws_sym)}),
            ]
        }).collect();
        for sub in subs {
            ws.send(Message::Text(sub.to_string().into())).await?;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    info!("[bingx] Shard {} subscribed L1+L2 for {} symbols", shard_idx, symbols.len());

    let mut ping = tokio::time::interval(Duration::from_secs(15));

    // Reconnect after 30 min to refresh subscriptions
    let conn_start = std::time::Instant::now();
    let max_conn_age = Duration::from_secs(1800);

    loop {
        if conn_start.elapsed() > max_conn_age {
            info!("[bingx] Shard {} refreshing after 30min...", shard_idx);
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
                    if text == "Ping" || text.contains("\"Ping\"") || text.contains("\"ping\"") {
                        let _ = ws.send(Message::Text("Pong".into())).await;
                        continue;
                    }
                    if text.is_empty() || text == "Pong" { continue; }
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        let data_type = v["dataType"].as_str().unwrap_or("");
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
                                    if let Some(mut ex_map) = price_map.get_mut("bingx") {
                                        if let Some(entry) = ex_map.get_mut(&sym_raw) {
                                            if let Some(&(bp, bq)) = bids.first() {
                                                entry.bid_size = bq * bp;
                                            }
                                            if let Some(&(ap, aq)) = asks.first() {
                                                entry.ask_size = aq * ap;
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
            },
            _ = ping.tick() => {
                let _ = ws.send(Message::Ping(vec![])).await;
                // Heartbeat: refresh last_updated for symbols with valid prices
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if let Some(mut ex_map) = price_map.get_mut("bingx") {
                    for sym in symbols {
                        if let Some(entry) = ex_map.get_mut(sym) {
                            if entry.bid > 0.0 && entry.ask > 0.0 {
                                entry.last_updated = now_ts;
                            }
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
