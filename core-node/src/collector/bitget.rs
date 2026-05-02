//! Bitget USDT-FUTURES WebSocket collector
//! Uses v2 API: ticker channel for L1 + books5 for L2 depth
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
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
                    .all(|x| x.disabled_exchanges.contains(&"bitget".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) {
                info!("[bitget] Disabled globally in profiles. Sleeping...");
            }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        info!("[bitget] Connecting...");

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"bitget".to_string()))
                };
                if is_disabled {
                    break;
                }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal, &l2_books) => {
                match res {
                    Ok(_)  => { warn!("[bitget] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => error!("[bitget] Error: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            _ = disable_watcher => {
                info!("[bitget] Exchange was disabled dynamically. Dropping connection.");
                backoff = Duration::from_secs(1);
            }
        }
    }
}

async fn handler(
    price_map: &PriceMap,
    signal: &UpdateSignal,
    l2_books: &L2OrderbookMap,
) -> Result<()> {
    let http = Client::new();
    let mut sym_map = std::collections::HashMap::new();

    if let Ok(resp) = http
        .get("https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES")
        .timeout(Duration::from_secs(15))
        .send()
        .await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(list) = body["data"].as_array() {
                for t in list {
                    let sym = t["symbol"].as_str().unwrap_or("");
                    if sym.ends_with("USDT") {
                        let mult = t["sizeMultiplier"].as_str().and_then(|s| s.parse().ok()).unwrap_or(1.0);
                        sym_map.insert(sym.to_string(), mult);
                    }
                }
            }
        }
    }

    // Seed 24h volume from REST tickers
    if let Ok(resp) = http
        .get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")
        .timeout(Duration::from_secs(15))
        .send()
        .await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(list) = body["data"].as_array() {
                let mut seeded = 0usize;
                for t in list {
                    let sym = t["symbol"].as_str().unwrap_or("");
                    if !sym.ends_with("USDT") {
                        continue;
                    }
                    let vol: f64 = t["usdtVolume"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    let bid: f64 = t["bestBid"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    let ask: f64 = t["bestAsk"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    let price: f64 = t["lastPr"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    if vol > 0.0 || price > 0.0 {
                        let mid = if bid > 0.0 && ask > 0.0 {
                            (bid + ask) / 2.0
                        } else {
                            price
                        };
                        let ts_now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        price_map
                            .entry("bitget".to_string())
                            .or_default()
                            .insert(
                                sym.to_string(),
                                TickerData {
                                    bid,
                                    ask,
                                    price: mid,
                                    vol,
                                    funding_rate: 0.0,
                                    bid_size: 0.0,
                                    ask_size: 0.0,
                                    last_updated: ts_now,
                                },
                            );
                        seeded += 1;
                    }
                }
                info!("[bitget] Seeded {} tickers from REST", seeded);
            }
        }
    }

    // Collect all USDT-FUTURES symbols for subscription
    let symbols: Vec<String> = {
        let map = price_map.get("bitget");
        match map {
            Some(m) => m.keys().cloned().collect(),
            None => vec![],
        }
    };
    if symbols.is_empty() {
        return Err(anyhow::anyhow!("No Bitget symbols loaded"));
    }

    // Connect WS
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://ws.bitget.com/v2/ws/public",
        None,
        false,
        None,
    )
    .await
    .context("Bitget WS connect failed")?;
    info!("[bitget] Connected, subscribing to {} symbols...", symbols.len());

    // Subscribe to ticker (L1) for all symbols in chunks
    for chunk in symbols.chunks(50) {
        let args: Vec<Value> = chunk
            .iter()
            .map(|s| {
                json!({
                    "instType": "USDT-FUTURES",
                    "channel": "ticker",
                    "instId": s
                })
            })
            .collect();
        ws.send(Message::Text(
            json!({"op": "subscribe", "args": args}).to_string().into(),
        ))
        .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Subscribe to books5 (L2 depth, top 5 levels) for top symbols
    let l2_syms: Vec<&String> = symbols.iter().take(60).collect();
    for chunk in l2_syms.chunks(50) {
        let args: Vec<Value> = chunk
            .iter()
            .map(|s| {
                json!({
                    "instType": "USDT-FUTURES",
                    "channel": "books5",
                    "instId": s
                })
            })
            .collect();
        ws.send(Message::Text(
            json!({"op": "subscribe", "args": args}).to_string().into(),
        ))
        .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    info!("[bitget] Subscribed L2 depth for {} symbols", l2_syms.len());

    // Ensure bitget map exists
    price_map.entry("bitget".to_string()).or_default();

    let mut ping = tokio::time::interval(Duration::from_secs(25));

    loop {
        tokio::select! {
            msg = tokio::time::timeout(Duration::from_secs(35), ws.next()) => match msg.ok().flatten() {
                Some(Ok(Message::Text(text))) => {
                    // Bitget sends "pong" as plain text
                    if text.as_str() == "pong" { continue; }

                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        let action = v["action"].as_str().unwrap_or("");
                        let channel = v["arg"]["channel"].as_str().unwrap_or("");
                        let inst_id = v["arg"]["instId"].as_str().unwrap_or("");

                        if inst_id.is_empty() || !inst_id.ends_with("USDT") { continue; }

                        if channel == "ticker" {
                            // L1 ticker update
                            if let Some(data_arr) = v["data"].as_array() {
                                for d in data_arr {
                                    let bid: f64 = d["bestBid"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    let ask: f64 = d["bestAsk"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    let bid_sz: f64 = d["bestBidSz"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    let ask_sz: f64 = d["bestAskSz"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    let last: f64 = d["lastPr"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    if bid <= 0.0 || ask <= 0.0 { continue; }
                                    let mult = sym_map.get(inst_id).copied().unwrap_or(1.0);
                                    let mid = (bid + ask) / 2.0;
                                    let ts = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs();
                                    if let Some(mut ex_map) = price_map.get_mut("bitget") {
                                        if let Some(entry) = ex_map.get_mut(inst_id) {
                                            entry.bid = bid;
                                            entry.ask = ask;
                                            entry.price = if last > 0.0 { last } else { mid };
                                            entry.bid_size = bid_sz * mult * mid;
                                            entry.ask_size = ask_sz * mult * mid;
                                            entry.last_updated = ts;
                                        } else {
                                            ex_map.insert(inst_id.to_string(), TickerData {
                                                bid, ask, price: if last > 0.0 { last } else { mid },
                                                vol: 0.0, funding_rate: 0.0,
                                                bid_size: bid_sz * mult * mid, ask_size: ask_sz * mult * mid,
                                                last_updated: ts,
                                            });
                                        }
                                    }
                                    signal.notify_one();
                                }
                            }
                        } else if channel == "books5" && (action == "snapshot" || action == "update" || action.is_empty()) {
                            // L2 depth update (top 5 levels)
                            if let Some(data_arr) = v["data"].as_array() {
                                for d in data_arr {
                                    let mult = sym_map.get(inst_id).copied().unwrap_or(1.0);
                                    let bids = parse_levels(&d["bids"], mult);
                                    let asks = parse_levels(&d["asks"], mult);
                                    if !bids.is_empty() || !asks.is_empty() {
                                        let now_ms = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_millis() as i64;
                                        l2_books.insert(
                                            format!("bitget:{}", inst_id),
                                            L2Snapshot { bids, asks, updated_ms: now_ms },
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => {
                ws.send(Message::Text("ping".into())).await?;
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default().as_secs();
                if let Some(mut ex_map) = price_map.get_mut("bitget") {
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

/// Parse [[price, qty, ...], ...] depth levels
fn parse_levels(val: &Value, mult: f64) -> Vec<(f64, f64)> {
    val.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|level| {
                    let px: f64 = level[0].as_str()?.parse().ok()?;
                    let sz: f64 = level[1].as_str()?.parse().ok()?;
                    if px > 0.0 && sz > 0.0 {
                        Some((px, sz * mult))
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}
