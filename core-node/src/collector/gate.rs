//! Gate.io Futures WebSocket collector
use std::time::Duration;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{PriceMap, UpdateSignal};

pub async fn run(prices: PriceMap, signal: UpdateSignal) {
    let mut backoff = Duration::from_secs(1);
    loop {
        info!("[gate] Connecting...");
        match handler(&prices, &signal).await {
            Ok(_)  => { warn!("[gate] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
            Err(e) => error!("[gate] Error: {}. Reconnecting in {:?}", e, backoff),
        }
        tokio::time::sleep(backoff).await;
        // Cap backoff at 10s (was 60s) — stale Gate prices cause false spread signals
        backoff = (backoff * 2).min(Duration::from_secs(10));
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
            
            let sym = name.replace('_', "");
            if sym.ends_with("USDT") {
                price_map.entry("gate".to_string()).or_default().entry(sym).or_default();
            }
        }
    }
    info!("[gate] Fetched {} symbols from REST", symbols.len());

    // Seed volume + funding from REST tickers (so we don't need futures.tickers WS channel)
    if let Ok(tickers_resp) = client.get("https://api.gateio.ws/api/v4/futures/usdt/tickers")
        .timeout(Duration::from_secs(15))
        .send().await
    {
        if let Ok(tickers) = tickers_resp.json::<Vec<Value>>().await {
            let mut seeded = 0usize;
            for t in &tickers {
                let name = t["contract"].as_str().unwrap_or("").replace('_', "");
                if !name.ends_with("USDT") { continue; }
                let vol: f64 = t["volume_24h_quote"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let funding: f64 = t["funding_rate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let price: f64 = t["last"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if vol > 0.0 || price > 0.0 {
                    upsert_tickers(price_map, signal, name, price, vol, funding);
                    seeded += 1;
                }
            }
            info!("[gate] Seeded {}/{} tickers from REST (vol+funding)", seeded, tickers.len());
        }
    }

    let (mut ws, _) = connect_async_tls_with_config(
        "wss://fx-ws.gateio.ws/v4/ws/usdt", None, false, None,
    ).await.context("Gate.io WS connect failed")?;

    info!("[gate] Connected");
    
    // Subscribe ONLY to book_ticker (bid/ask) — NOT futures.tickers.
    // This halves subscription count (665 instead of 1330) and avoids Gate
    // resetting the connection due to too many subscriptions.
    // Volume and funding are seeded from REST above.
    for chunk in symbols.chunks(50) {
        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        ws.send(Message::Text(
            json!({"time":ts,"channel":"futures.book_ticker","event":"subscribe","payload": chunk})
                .to_string().into()
        )).await?;
        // 500ms between chunks (was 200ms) — reduces subscribe burst rate
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    info!("[gate] Subscribed to book_ticker for {} symbols ({} chunks)", symbols.len(), (symbols.len() + 49) / 50);

    let mut ping = tokio::time::interval(Duration::from_secs(20));
    // Periodic REST refresh for volume/funding every 120s
    let mut ticker_refresh = tokio::time::interval(Duration::from_secs(120));
    ticker_refresh.tick().await; // skip immediate tick

    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    if v.get("error").is_some() {
                        warn!("[gate] WS Error: {}", text);
                    }
                    let ch = v["channel"].as_str().unwrap_or("");
                    
                    if ch == "futures.book_ticker" {
                        let event = v["event"].as_str().unwrap_or("");
                        if event == "subscribe" {
                            // Don't log every subscribe OK — too noisy with 14 chunks
                            continue;
                        }
                        let item = &v["result"];
                        if !item.is_object() { continue; }
                        let sym = item["s"].as_str().unwrap_or("").replace('_', "");
                        if !sym.ends_with("USDT") { continue; }
                        
                        let bid: f64 = match item["b"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                        let ask: f64 = match item["a"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                        let bid_size: f64 = item["B"].as_i64().unwrap_or(0) as f64;
                        let ask_size: f64 = item["A"].as_i64().unwrap_or(0) as f64;
                        let mid = (bid + ask) / 2.0;
                        
                        upsert_book(price_map, signal, sym, bid, ask, bid_size * mid, ask_size * mid);
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => {
                let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                ws.send(Message::Text(json!({"time":ts, "channel":"futures.ping"}).to_string().into())).await?;
            },
            _ = ticker_refresh.tick() => {
                // Refresh volume + funding from REST every 120s
                let c = reqwest::Client::new();
                if let Ok(resp) = c.get("https://api.gateio.ws/api/v4/futures/usdt/tickers")
                    .timeout(Duration::from_secs(10))
                    .send().await
                {
                    if let Ok(tickers) = resp.json::<Vec<Value>>().await {
                        for t in &tickers {
                            let name = t["contract"].as_str().unwrap_or("").replace('_', "");
                            if !name.ends_with("USDT") { continue; }
                            let vol: f64 = t["volume_24h_quote"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let funding: f64 = t["funding_rate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let price: f64 = t["last"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            if vol > 0.0 || price > 0.0 {
                                upsert_tickers(price_map, signal, name, price, vol, funding);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn upsert_tickers(price_map: &PriceMap, signal: &UpdateSignal, sym: String, price: f64, vol: f64, funding: f64) {
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    {
        let mut gate_map = price_map.entry("gate".to_string()).or_default();
        let entry = gate_map.entry(sym).or_default();
        entry.price = price;
        entry.vol = vol;
        entry.funding_rate = funding;
        entry.last_updated = ts;
    }
    signal.notify_one();
}

fn upsert_book(price_map: &PriceMap, signal: &UpdateSignal, sym: String, bid: f64, ask: f64, bid_size_usd: f64, ask_size_usd: f64) {
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    {
        let mut gate_map = price_map.entry("gate".to_string()).or_default();
        let entry = gate_map.entry(sym).or_default();
        entry.bid = bid;
        entry.ask = ask;
        entry.bid_size = bid_size_usd;
        entry.ask_size = ask_size_usd;
        if entry.price == 0.0 { entry.price = (bid + ask) / 2.0; }
        entry.last_updated = ts;
    }
    signal.notify_one();
}
