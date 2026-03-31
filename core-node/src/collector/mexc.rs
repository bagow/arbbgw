//! MEXC Futures WebSocket collector — sub.ticker.all
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
        info!("[mexc] Connecting...");
        match handler(&prices, &signal).await {
            Ok(_)  => warn!("[mexc] WS closed, reconnecting..."),
            Err(e) => error!("[mexc] Error: {}. Reconnecting in {:?}", e, backoff),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://contract.mexc.com/edge", None, false, None,
    ).await.context("MEXC WS connect failed")?;
    info!("[mexc] Connected, subscribing to all tickers...");

    // Subscribe to all tickers at once
    ws.send(Message::Text(json!({"method":"sub.ticker.all","param":{}}).to_string().into())).await?;

    let mut ping = tokio::time::interval(Duration::from_secs(15));
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    if v["channel"].as_str() != Some("push.ticker") { continue; }
                    let d = &v["data"];
                    // MEXC symbol format: BTC_USDT → normalize to BTCUSDT
                    let raw = d["symbol"].as_str().unwrap_or("");
                    let sym = raw.replace('_', "");
                    if !sym.ends_with("USDT") { continue; }
                    let bid: f64 = d["bid1"].as_f64().unwrap_or(0.0);
                    let ask: f64 = d["ask1"].as_f64().unwrap_or(0.0);
                    let price: f64 = d["lastPrice"].as_f64().unwrap_or(0.0);
                    let vol: f64 = d["amount24"].as_f64().unwrap_or(0.0);
                    if bid > 0.0 && ask > 0.0 {
                        upsert(price_map, signal, sym, TickerData { bid, ask, price, vol, funding_rate: 0.0, last_updated: 0 });
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => { ws.send(Message::Text(json!({"method":"ping"}).to_string().into())).await?; }
        }
    }
    Ok(())
}

fn upsert(price_map: &PriceMap, signal: &UpdateSignal, sym: String, mut data: TickerData) {
    data.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    price_map.entry("mexc".to_string()).or_default().insert(sym, data);
    signal.notify_one();
}
