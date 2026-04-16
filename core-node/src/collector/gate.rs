//! Gate.io Futures WebSocket collector
//! Uses multiple parallel WS connections (~100 symbols each) to avoid
//! Gate resetting connections due to too many subscriptions per socket.
use std::time::Duration;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{PriceMap, UpdateSignal};

/// Max symbols per single WS connection — keeps well under Gate's implicit limit.
const SYMBOLS_PER_WS: usize = 100;

pub async fn run(prices: PriceMap, signal: UpdateSignal, settings: std::sync::Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let is_disabled = {
            let p = settings.read().await;
            !p.is_empty() && p.iter().all(|x| x.disabled_exchanges.contains(&"gate".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) {
                info!("[gate] Disabled globally in profiles. Sleeping...");
            }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        info!("[gate] Starting...");
        
        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty() && p.iter().all(|x| x.disabled_exchanges.contains(&"gate".to_string()))
                };
                if is_disabled { break; }
            }
        };

        tokio::select! {
            res = orchestrator(&prices, &signal) => {
                match res {
                    Ok(_)  => { warn!("[gate] orchestrator returned, restarting..."); backoff = Duration::from_secs(1); }
                    Err(e) => error!("[gate] Error: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
            _ = disable_watcher => {
                info!("[gate] Exchange was disabled dynamically. Dropping connections.");
                backoff = Duration::from_secs(1);
            }
        }
    }
}

/// Fetches symbol list + seeds tickers from REST, then spawns N parallel WS
/// connections (each handling ~100 symbols). If ANY connection dies, all are
/// cancelled and the whole orchestrator returns (triggering full reconnect).
async fn orchestrator(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
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

    // Seed volume + funding from REST tickers
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

    // Split symbols into chunks for parallel WS connections
    let sym_chunks: Vec<Vec<String>> = symbols.chunks(SYMBOLS_PER_WS)
        .map(|c| c.to_vec())
        .collect();
    let n_conns = sym_chunks.len();
    info!("[gate] Spawning {} parallel WS connections (~{} symbols each)", n_conns, SYMBOLS_PER_WS);

    // Spawn one WS handler per chunk. If any one dies, we cancel all and restart.
    let mut handles = Vec::with_capacity(n_conns);
    for (idx, chunk) in sym_chunks.into_iter().enumerate() {
        let pm = price_map.clone();
        let sig = signal.clone();
        handles.push(tokio::spawn(async move {
            ws_shard(idx, chunk, &pm, &sig).await
        }));
    }

    // Also spawn periodic REST ticker refresh
    let pm_refresh = price_map.clone();
    let sig_refresh = signal.clone();
    let refresh_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(120));
        interval.tick().await; // skip immediate
        loop {
            interval.tick().await;
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
                            upsert_tickers(&pm_refresh, &sig_refresh, name, price, vol, funding);
                        }
                    }
                }
            }
        }
    });

    // Wait for ANY handle to complete (= die). Then cancel all others.
    let (result, _idx, remaining) = futures_util::future::select_all(handles).await;
    refresh_handle.abort();
    for h in remaining { h.abort(); }

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow::anyhow!("WS shard task panicked: {}", e)),
    }
}

/// A single WS shard handling a subset of symbols.
async fn ws_shard(idx: usize, symbols: Vec<String>, price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://fx-ws.gateio.ws/v4/ws/usdt", None, false, None,
    ).await.context(format!("Gate WS shard {} connect failed", idx))?;

    info!("[gate] Shard {} connected, subscribing to {} symbols", idx, symbols.len());

    // Subscribe in chunks of 50 (Gate protocol limit per message)
    for chunk in symbols.chunks(50) {
        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        ws.send(Message::Text(
            json!({"time":ts,"channel":"futures.book_ticker","event":"subscribe","payload": chunk})
                .to_string().into()
        )).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    info!("[gate] Shard {} subscribed OK", idx);

    let mut ping = tokio::time::interval(Duration::from_secs(20));

    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    if v.get("error").is_some() {
                        warn!("[gate] Shard {} WS Error: {}", idx, text);
                    }
                    let ch = v["channel"].as_str().unwrap_or("");
                    
                    if ch == "futures.book_ticker" {
                        let event = v["event"].as_str().unwrap_or("");
                        if event == "subscribe" { continue; }
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
