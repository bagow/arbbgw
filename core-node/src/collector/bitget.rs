//! Bitget USDT futures WebSocket collector
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
        info!("[bitget] Connecting...");
        match handler(&prices, &signal).await {
            Ok(_)  => { warn!("[bitget] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
            Err(e) => error!("[bitget] Error: {}. Reconnecting in {:?}", e, backoff),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    // Fetch all USDT-FUTURES tickers from REST
    let resp: Value = http.get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")
        .timeout(Duration::from_secs(15)).send().await?.json().await?;

    let mut symbols = Vec::new();
    if let Some(list) = resp["data"].as_array() {
        for t in list {
            let sym = t["symbol"].as_str().unwrap_or("").to_string();
            if !sym.ends_with("USDT") { continue; }
            let bid: f64 = t["bidPr"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let ask: f64 = t["askPr"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let bid_qty: f64 = t["bidSz"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let ask_qty: f64 = t["askSz"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let price: f64 = t["lastPr"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
            let vol: f64 = t["usdtVolume"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            if bid > 0.0 && ask > 0.0 {
                upsert(price_map, signal, sym.clone(), TickerData { bid, ask, price, vol, funding_rate: 0.0, bid_size: bid_qty * price, ask_size: ask_qty * price, last_updated: 0 });
            }
            symbols.push(sym);
        }
    }
    info!("[bitget] Seeded {} USDT futures tickers", symbols.len());

    // WS: subscribe to ticker channel in batches of 100
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://ws.bitget.com/v2/ws/public", None, false, None,
    ).await.context("Bitget WS connect failed")?;
    info!("[bitget] Connected, subscribing...");

    for chunk in symbols.chunks(100) {
        let args: Vec<Value> = chunk.iter()
            .map(|sym| json!({"instType":"USDT-FUTURES","channel":"ticker","instId":sym})).collect();
        ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string().into())).await?;
    }

    let mut ping = tokio::time::interval(Duration::from_secs(25));
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    if v["arg"]["channel"].as_str() != Some("ticker") { continue; }
                    for d in v["data"].as_array().unwrap_or(&vec![]) {
                        let sym = d["instId"].as_str().unwrap_or("").to_string();
                        if !sym.ends_with("USDT") { continue; }
                        let bid: f64 = d["bidPr"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let ask: f64 = d["askPr"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let bid_qty: f64 = d["bidSz"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let ask_qty: f64 = d["askSz"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let price: f64 = d["lastPr"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
                        let ev = price_map.get("bitget").and_then(|m| m.get(&sym).map(|d| d.vol)).unwrap_or(0.0);
                        if bid > 0.0 && ask > 0.0 {
                            upsert(price_map, signal, sym, TickerData { bid, ask, price, vol: ev, funding_rate: 0.0, bid_size: bid_qty * price, ask_size: ask_qty * price, last_updated: 0 });
                        }
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
    price_map.entry("bitget".to_string()).or_default().insert(sym, data);
    signal.notify_one();
}
