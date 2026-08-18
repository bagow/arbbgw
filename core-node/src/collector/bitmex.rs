//! BitMEX WebSocket collector
//! Public WS: wss://www.bitmex.com/realtime
//! Subscribes to quoteBin1m (book ticker) for all USDT instruments
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
    _l2_wanted: std::sync::Arc<dashmap::DashSet<String>>,
    settings: std::sync::Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let is_disabled = {
            let p = settings.read().await;
            !p.is_empty() && p.iter().all(|x| x.disabled_exchanges.contains(&"bitmex".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) { info!("[bitmex] Disabled globally. Sleeping..."); }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let dis = settings.read().await;
                if !dis.is_empty() && dis.iter().all(|x| x.disabled_exchanges.contains(&"bitmex".to_string())) { break; }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal, &l2_books) => {
                match res {
                    Ok(_) => { warn!("[bitmex] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => warn!("[bitmex] WS disconnected: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            _ = disable_watcher => { info!("[bitmex] disabled dynamically."); backoff = Duration::from_secs(1); }
        }
    }
}

fn normalize_bitmex(sym: &str) -> String {
    // XBTUSDT → BTCUSDT, others: strip and uppercase
    if sym == "XBTUSDT" { "BTCUSDT".to_string() }
    else { sym.replace('_', "") }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal, _l2_books: &L2OrderbookMap) -> Result<()> {
    let http = Client::builder().user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36").build().unwrap();

    // Seed symbols and prices from REST
    if let Ok(resp) = http
        .get("https://www.bitmex.com/api/v1/instrument/active")
        .timeout(Duration::from_secs(15))
        .send().await
    {
        if let Ok(arr) = resp.json::<Vec<Value>>().await {
            let mut seeded = 0usize;
            for inst in &arr {
                let raw_sym = inst["symbol"].as_str().unwrap_or("");
                let settle = inst["settlCurrency"].as_str().unwrap_or("");
                // Only USDT-settled instruments
                if settle != "USDt" && !raw_sym.ends_with("USDT") { continue; }
                let std_sym = normalize_bitmex(raw_sym);
                if !std_sym.ends_with("USDT") { continue; }
                let bid: f64 = inst["bidPrice"].as_f64().unwrap_or(0.0);
                let ask: f64 = inst["askPrice"].as_f64().unwrap_or(0.0);
                let last: f64 = inst["lastPrice"].as_f64().unwrap_or(0.0);
                let vol: f64 = inst["foreignNotional24h"].as_f64().unwrap_or(0.0);
                let mid = if bid > 0.0 && ask > 0.0 { (bid + ask) / 2.0 } else { last };
                let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                price_map.entry("bitmex".to_string()).or_default().insert(std_sym.clone(), TickerData {
                    bid, ask, price: if mid > 0.0 { mid } else { last },
                    vol, funding_rate: 0.0, bid_size: 0.0, ask_size: 0.0, last_updated: ts,
                });
                seeded += 1;
            }
            info!("[bitmex] Seeded {} USDT instruments from REST", seeded);
        }
    }

    let mut sym_map = std::collections::HashMap::new();

    // Collect raw symbols for subscription
    let raw_syms: Vec<String> = {
        // Re-fetch to get the raw BitMEX symbols for WS subscription
        let resp = http.get("https://www.bitmex.com/api/v1/instrument/active")
            .timeout(Duration::from_secs(15)).send().await.context("BitMEX REST failed")?;
        let arr: Vec<Value> = resp.json().await.context("BitMEX JSON parse failed")?;
        arr.iter().filter_map(|inst| {
            let sym = inst["symbol"].as_str()?;
            let settle = inst["settlCurrency"].as_str().unwrap_or("");
            let std_sym = normalize_bitmex(sym);
            if (settle == "USDt" || sym.ends_with("USDT")) && std_sym.ends_with("USDT") {
                let mult = inst["lotSize"].as_f64().unwrap_or(1.0);
                sym_map.insert(sym.to_string(), mult);
                Some(sym.to_string())
            } else { None }
        }).collect()
    };

    if raw_syms.is_empty() { return Err(anyhow::anyhow!("No BitMEX USDT symbols")); }

    let (mut ws, _) = connect_async_tls_with_config(
        "wss://www.bitmex.com/realtime",
        None, false, None,
    ).await.context("BitMEX WS connect failed")?;
    info!("[bitmex] Connected, subscribing {} symbols...", raw_syms.len());

    // Subscribe to quote (best bid/ask) for each symbol
    let topics: Vec<String> = raw_syms.iter().map(|s| format!("quote:{}", s)).collect();
    for chunk in topics.chunks(20) {
        ws.send(Message::Text(json!({"op":"subscribe","args": chunk}).to_string().into())).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    price_map.entry("bitmex".to_string()).or_default();

    let mut ping = tokio::time::interval(Duration::from_secs(25));

    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    if text.as_str() == "pong" { continue; }
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        if v["table"].as_str() == Some("quote") {
                            if let Some(arr) = v["data"].as_array() {
                                for d in arr {
                                    let raw_sym = d["symbol"].as_str().unwrap_or("");
                                    let std_sym = normalize_bitmex(raw_sym);
                                    if !std_sym.ends_with("USDT") { continue; }
                                    let bid: f64 = d["bidPrice"].as_f64().unwrap_or(0.0);
                                    let ask: f64 = d["askPrice"].as_f64().unwrap_or(0.0);
                                    let bid_sz: f64 = d["bidSize"].as_f64().unwrap_or(0.0);
                                    let ask_sz: f64 = d["askSize"].as_f64().unwrap_or(0.0);
                                    if bid <= 0.0 || ask <= 0.0 { continue; }
                                    let mult = sym_map.get(raw_sym).copied().unwrap_or(1.0);
                                    let mid = (bid + ask) / 2.0;
                                    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                                    if let Some(mut ex_map) = price_map.get_mut("bitmex") {
                                        if let Some(entry) = ex_map.get_mut(&std_sym) {
                                            entry.bid = bid; entry.ask = ask; entry.price = mid;
                                            entry.bid_size = bid_sz * mult * mid; entry.ask_size = ask_sz * mult * mid;
                                            entry.last_updated = ts;
                                        } else {
                                            ex_map.insert(std_sym.clone(), TickerData {
                                                bid, ask, price: mid, vol: 0.0, funding_rate: 0.0,
                                                bid_size: bid_sz * mult * mid, ask_size: ask_sz * mult * mid, last_updated: ts,
                                            });
                                        }
                                    }
                                    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
                                    _l2_books.insert(
                                        format!("bitmex:{}", std_sym),
                                        crate::state::L2Snapshot {
                                            bids: vec![(bid, bid_sz * mult)],
                                            asks: vec![(ask, ask_sz * mult)],
                                            updated_ms: now_ms,
                                        }
                                    );
                                    signal.notify_one();
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
                // WS heartbeat: update last_updated for all pairs with valid prices.
                // BitMEX quote stream pushes only on change — heartbeat confirms WS is alive.
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default().as_secs();
                if let Some(mut ex_map) = price_map.get_mut("bitmex") {
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
