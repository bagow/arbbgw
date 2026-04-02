//! Gate.io Futures WebSocket collector
use std::time::Duration;
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
    let client = reqwest::Client::new();
    let res = client.get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
        .send().await.context("Gate REST failed")?
        .json::<Vec<serde_json::Value>>().await.context("Gate JSON parse failed")?;
    
    let mut symbols = Vec::new();
    for c in res {
        if let Some(name) = c["name"].as_str() {
            if c["in_delisting"].as_bool().unwrap_or(false) { continue; }
            symbols.push(name.to_string());
        }
    }
    info!("[gate] Fetched {} symbols from REST", symbols.len());

    let (mut ws, _) = connect_async_tls_with_config(
        "wss://fx-ws.gateio.ws/v4/ws/usdt", None, false, None,
    ).await.context("Gate.io WS connect failed")?;

    info!("[gate] Connected");
    
    // Subscribe in chunks to avoid websocket payload limits
    for chunk in symbols.chunks(50) {
        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        ws.send(Message::Text(
            json!({"time":ts,"channel":"futures.tickers","event":"subscribe","payload": chunk})
                .to_string().into()
        )).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let mut ping = tokio::time::interval(Duration::from_secs(20));

    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    if v.get("error").is_some() {
                        warn!("[gate] WS Error: {}", text);
                    }
                    let ch = v["channel"].as_str().unwrap_or("");
                    
                    if ch == "futures.tickers" {
                        let event = v["event"].as_str().unwrap_or("");
                        if event == "subscribe" {
                            info!("[gate] Subscribe OK: {}", text);
                            continue;
                        }
                        let items = if v["result"].is_array() {
                            v["result"].as_array().unwrap().clone()
                        } else if v["result"].is_object() {
                            vec![v["result"].clone()]
                        } else {
                            vec![]
                        };

                        for item in &items {
                            let sym = item["contract"].as_str().unwrap_or("").replace('_', "");
                            if !sym.ends_with("USDT") { continue; }
                            
                            let price   = item["last"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            if price == 0.0 { continue; }
                            
                            let bid = item["highest_bid"].as_str().and_then(|s| s.parse().ok()).unwrap_or(price);
                            let ask = item["lowest_ask"].as_str().and_then(|s| s.parse().ok()).unwrap_or(price);
                            let vol     = item["volume_24h_quote"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let funding = item["funding_rate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            
                            upsert(price_map, signal, sym, TickerData { bid, ask, price, vol, funding_rate: funding, last_updated: 0 });
                        }
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => {
                let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                ws.send(Message::Text(json!({"time":ts, "channel":"futures.ping"}).to_string().into())).await?;
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
