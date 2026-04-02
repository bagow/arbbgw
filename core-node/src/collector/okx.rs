//! OKX USDT perpetual swap WebSocket collector
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
        info!("[okx] Connecting...");
        match handler(&prices, &signal).await {
            Ok(_)  => warn!("[okx] WS closed, reconnecting..."),
            Err(e) => error!("[okx] Error: {}. Reconnecting in {:?}", e, backoff),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    // Seed all SWAP tickers from REST in one call
    let resp: Value = http.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP")
        .timeout(Duration::from_secs(15)).send().await?.json().await?;

    let mut inst_ids = Vec::new();
    if let Some(list) = resp["data"].as_array() {
        for t in list {
            let inst = t["instId"].as_str().unwrap_or(""); // BTC-USDT-SWAP
            if !inst.ends_with("-USDT-SWAP") { continue; }
            // Normalize: BTC-USDT-SWAP → BTCUSDT
            let sym = inst.replace("-USDT-SWAP", "USDT");
            let bid: f64 = t["bidPx"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let ask: f64 = t["askPx"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let price: f64 = t["last"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
            let vol: f64 = t["volCcy24h"].as_str().and_then(|s| s.parse::<f64>().ok())
                .map(|v| v * price).unwrap_or(0.0); // volCcy24h is in contracts, * price = USD
            if bid > 0.0 && ask > 0.0 {
                upsert(price_map, signal, sym, TickerData { bid, ask, price, vol, funding_rate: 0.0, last_updated: 0 });
            }
            inst_ids.push(inst.to_string());
        }
    }
    info!("[okx] Seeded {} USDT-SWAP tickers", inst_ids.len());

    // WS: subscribe to tickers in batches of 100
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://ws.okx.com:8443/ws/v5/public", None, false, None,
    ).await.context("OKX WS connect failed")?;
    info!("[okx] Connected, subscribing...");

    for chunk in inst_ids.chunks(100) {
        let args: Vec<Value> = chunk.iter()
            .map(|id| json!({"channel":"tickers","instId":id})).collect();
        ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string().into())).await?;
    }

    let mut ping = tokio::time::interval(Duration::from_secs(25));
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    if v["arg"]["channel"].as_str() != Some("tickers") { continue; }
                    for d in v["data"].as_array().unwrap_or(&vec![]) {
                        let inst = d["instId"].as_str().unwrap_or("");
                        if !inst.ends_with("-USDT-SWAP") { continue; }
                        let sym = inst.replace("-USDT-SWAP", "USDT");
                        let bid: f64 = d["bidPx"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let ask: f64 = d["askPx"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let price: f64 = d["last"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
                        let ev = price_map.get("okx").and_then(|m| m.get(&sym).map(|d| d.vol)).unwrap_or(0.0);
                        if bid > 0.0 && ask > 0.0 {
                            upsert(price_map, signal, sym, TickerData { bid, ask, price, vol: ev, funding_rate: 0.0, last_updated: 0 });
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
    price_map.entry("okx".to_string()).or_default().insert(sym, data);
    signal.notify_one();
}
