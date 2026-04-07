//! Binance USDT Futures WebSocket collector — !bookTicker stream (all pairs)
use std::time::Duration;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::Value;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{PriceMap, TickerData, UpdateSignal};

pub async fn run(prices: PriceMap, signal: UpdateSignal) {
    let mut backoff = Duration::from_secs(1);
    loop {
        info!("[binance] Connecting...");
        match handler(&prices, &signal).await {
            Ok(_)  => { warn!("[binance] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
            Err(e) => error!("[binance] Error: {}. Reconnecting in {:?}", e, backoff),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    // Seed 24h volume from REST miniTicker snapshot
    let http = Client::new();
    if let Ok(resp) = http.get("https://fapi.binance.com/fapi/v1/ticker/24hr")
        .timeout(Duration::from_secs(15)).send().await {
        if let Ok(arr) = resp.json::<Vec<Value>>().await {
            for t in &arr {
                let sym = t["symbol"].as_str().unwrap_or("");
                if !sym.ends_with("USDT") { continue; }
                let vol: f64 = t["quoteVolume"].as_str()
                    .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if vol > 0.0 {
                    let mut entry = price_map.entry("binance".to_string()).or_default();
                    entry.entry(sym.to_string())
                        .or_insert_with(|| TickerData { vol, ..Default::default() }).vol = vol;
                }
            }
            info!("[binance] Seeded volume for {} symbols", arr.len());
        }
    }

    // WS: !bookTicker — instant bid/ask for every USDT future
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://fstream.binance.com/ws/!bookTicker", None, false, None,
    ).await.context("Binance WS connect failed")?;
    info!("[binance] Connected");

    while let Some(msg) = ws.next().await {
        if let Ok(Message::Text(text)) = msg {
            let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            let sym = v["s"].as_str().unwrap_or("");
            if !sym.ends_with("USDT") { continue; }
            let bid: f64 = match v["b"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
            let ask: f64 = match v["a"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
            let bid_qty: f64 = v["B"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let ask_qty: f64 = v["A"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let vol = price_map.get("binance").and_then(|m| m.get(sym).map(|d| d.vol)).unwrap_or(0.0);
            let mid = (bid + ask) / 2.0;
            upsert(price_map, signal, sym.to_string(), TickerData { bid, ask, price: mid, vol, funding_rate: 0.0, bid_size: bid_qty * mid, ask_size: ask_qty * mid, last_updated: 0 });
        }
    }
    Ok(())
}

fn upsert(price_map: &PriceMap, signal: &UpdateSignal, sym: String, mut data: TickerData) {
    data.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    price_map.entry("binance".to_string()).or_default().insert(sym, data);
    signal.notify_one();
}
