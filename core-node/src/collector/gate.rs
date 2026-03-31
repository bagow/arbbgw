//! Gate.io Futures WebSocket collector
use std::{sync::Arc, time::Duration};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{PriceMap, TickerData, UpdateSignal};

pub async fn run(prices: PriceMap, signal: UpdateSignal) {
    let mut backoff = Duration::from_secs(1);
    loop {
        info!("[gate] Connecting...");
        match handler(&prices, &signal).await {
            Ok(_)  => warn!("[gate] WS closed, reconnecting..."),
            Err(e) => error!("[gate] Error: {}. Reconnecting in {:?}", e, backoff),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://fx-ws.gateio.ws/v4/ws/usdt", None, false, None,
    ).await.context("Gate.io WS connect failed")?;

    info!("[gate] Connected");
    ws.send(Message::Text(
        json!({"time":0,"channel":"futures.tickers","event":"subscribe","payload":["!all"]})
            .to_string().into()
    )).await?;

    while let Some(msg) = ws.next().await {
        if let Ok(Message::Text(text)) = msg {
            let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            if v["channel"].as_str() != Some("futures.tickers") { continue; }
            for item in v["result"].as_array().unwrap_or(&vec![]) {
                let sym = item["contract"].as_str().unwrap_or("").replace('_', "");
                if !sym.ends_with("USDT") { continue; }
                let bid: f64 = match item["highest_bid"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                let ask: f64 = match item["lowest_ask"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                let price   = item["last"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
                let vol     = item["volume_24h_quote"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let funding = item["funding_rate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                upsert(price_map, signal, sym, TickerData { bid, ask, price, vol, funding_rate: funding, last_updated: 0 });
            }
        }
    }
    Ok(())
}

fn upsert(price_map: &PriceMap, signal: &UpdateSignal, sym: String, mut data: TickerData) {
    data.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    price_map.entry("gate".to_string()).or_default().insert(sym, data);
    signal.notify_one();
}
