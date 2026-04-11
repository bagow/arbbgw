//! Bybit USDT perpetual futures WebSocket collector
use std::time::Duration;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{PriceMap, TickerData, UpdateSignal};

pub async fn run(prices: PriceMap, signal: UpdateSignal) {
    let mut backoff = Duration::from_secs(1);
    loop {
        info!("[bybit] Connecting...");
        match handler(&prices, &signal).await {
            Ok(_)  => { warn!("[bybit] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
            Err(e) => error!("[bybit] Error: {}. Reconnecting in {:?}", e, backoff),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    // Fetch all linear USDT perpetual tickers (includes volume)
    let resp: Value = http.get("https://api.bybit.com/v5/market/tickers?category=linear")
        .timeout(Duration::from_secs(15)).send().await?.json().await?;

    let mut symbols = Vec::new();
    if let Some(list) = resp["result"]["list"].as_array() {
        for t in list {
            let sym = t["symbol"].as_str().unwrap_or("");
            if !sym.ends_with("USDT") { continue; }
            let vol: f64 = t["turnover24h"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let bid: f64 = t["bid1Price"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let ask: f64 = t["ask1Price"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let bid_qty: f64 = t["bid1Size"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let ask_qty: f64 = t["ask1Size"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let price: f64 = t["lastPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
            if bid > 0.0 && ask > 0.0 {
                upsert(price_map, signal, sym.to_string(), TickerData { bid, ask, price, vol, funding_rate: 0.0, bid_size: bid_qty * price, ask_size: ask_qty * price, last_updated: 0 });
            }
            symbols.push(sym.to_string());
        }
    }
    if symbols.is_empty() {
        return Err(anyhow::anyhow!("Failed to fetch symbols or list is empty"));
    }
    info!("[bybit] Seeded {} USDT linear tickers", symbols.len());

    // WS: subscribe to tickers in batches of 10
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://stream.bybit.com/v5/public/linear", None, false, None,
    ).await.context("Bybit WS connect failed")?;
    info!("[bybit] Connected, subscribing...");

    for chunk in symbols.chunks(10) {
        let args: Vec<String> = chunk.iter().map(|s| format!("tickers.{}", s)).collect();
        ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string().into())).await?;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    let mut ping = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    if v["topic"].as_str().map(|t| !t.starts_with("tickers.")).unwrap_or(true) { continue; }
                    let d = &v["data"];
                    let sym = d["symbol"].as_str().unwrap_or("").to_string();
                    if sym.is_empty() || !sym.ends_with("USDT") { continue; }
                    let bid: f64 = d["bid1Price"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let ask: f64 = d["ask1Price"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let bid_qty: f64 = d["bid1Size"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let ask_qty: f64 = d["ask1Size"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let price: f64 = d["lastPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
                    let ev = price_map.get("bybit").and_then(|m| m.get(&sym).map(|d| d.vol)).unwrap_or(0.0);
                    if bid > 0.0 && ask > 0.0 {
                        upsert(price_map, signal, sym, TickerData { bid, ask, price, vol: ev, funding_rate: 0.0, bid_size: bid_qty * price, ask_size: ask_qty * price, last_updated: 0 });
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => { ws.send(Message::Text(json!({"op":"ping"}).to_string().into())).await?; }
        }
    }
    Ok(())
}

fn upsert(price_map: &PriceMap, signal: &UpdateSignal, sym: String, mut data: TickerData) {
    data.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    price_map.entry("bybit".to_string()).or_default().insert(sym, data);
    signal.notify_one();
}
