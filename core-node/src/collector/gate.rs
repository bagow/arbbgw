//! Gate.io Futures WebSocket collector
//! Uses multiple parallel WS connections (~100 symbols each) to avoid
//! Gate resetting connections due to too many subscriptions per socket.
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{L2OrderbookMap, L2Snapshot, PriceMap, UpdateSignal};

/// Max symbols per single WS connection — keeps well under Gate's implicit limit.
const SYMBOLS_PER_WS: usize = 100;

pub async fn run(
    prices: PriceMap,
    signal: UpdateSignal,
    l2_books: L2OrderbookMap,
    settings: std::sync::Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let is_disabled = {
            let p = settings.read().await;
            !p.is_empty()
                && p.iter()
                    .all(|x| x.disabled_exchanges.contains(&"gate".to_string()))
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
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"gate".to_string()))
                };
                if is_disabled {
                    break;
                }
            }
        };

        tokio::select! {
            res = orchestrator(&prices, &signal, &l2_books) => {
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
async fn orchestrator(price_map: &PriceMap, signal: &UpdateSignal, l2_books: &L2OrderbookMap) -> Result<()> {
    let client = reqwest::Client::new();
    let res = client
        .get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
        .send()
        .await
        .context("Gate REST failed")?
        .json::<Vec<serde_json::Value>>()
        .await
        .context("Gate JSON parse failed")?;

    let mut symbols = Vec::new();
    let mut sym_map = std::collections::HashMap::new();
    for c in res {
        if let Some(name) = c["name"].as_str() {
            if c["in_delisting"].as_bool().unwrap_or(false) {
                continue;
            }
            symbols.push(name.to_string());

            let sym = name.replace('_', "");
            if sym.ends_with("USDT") {
                let mult: f64 = c["quanto_multiplier"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1.0);
                sym_map.insert(name.to_string(), (sym.clone(), mult));
                price_map
                    .entry("gate".to_string())
                    .or_default()
                    .entry(sym)
                    .or_default();
            }
        }
    }
    let sym_map_arc = Arc::new(sym_map);
    info!("[gate] Fetched {} symbols from REST", symbols.len());

    // Seed volume + funding from REST tickers
    if let Ok(tickers_resp) = client
        .get("https://api.gateio.ws/api/v4/futures/usdt/tickers")
        .timeout(Duration::from_secs(15))
        .send()
        .await
    {
        if let Ok(tickers) = tickers_resp.json::<Vec<Value>>().await {
            let mut seeded = 0usize;
            for t in &tickers {
                let name = t["contract"].as_str().unwrap_or("").replace('_', "");
                if !name.ends_with("USDT") {
                    continue;
                }
                let vol: f64 = t["volume_24h_quote"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let funding: f64 = t["funding_rate"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let price: f64 = t["last"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                if vol > 0.0 || price > 0.0 {
                    upsert_tickers(price_map, signal, name, price, vol, funding);
                    seeded += 1;
                }
            }
            info!(
                "[gate] Seeded {}/{} tickers from REST (vol+funding)",
                seeded,
                tickers.len()
            );
        }
    }

    // Split symbols into chunks for parallel WS connections
    let sym_chunks: Vec<Vec<String>> = symbols.chunks(SYMBOLS_PER_WS).map(|c| c.to_vec()).collect();
    let n_conns = sym_chunks.len();
    info!(
        "[gate] Spawning {} parallel WS connections (~{} symbols each)",
        n_conns, SYMBOLS_PER_WS
    );

    // Spawn one WS handler per chunk. If any one dies, we cancel all and restart.
    let mut handles = Vec::with_capacity(n_conns);
    for (idx, chunk) in sym_chunks.into_iter().enumerate() {
        let pm = price_map.clone();
        let sig = signal.clone();
        let sm = sym_map_arc.clone();
        let l2 = l2_books.clone();
        handles.push(tokio::spawn(async move {
            ws_shard(idx, chunk, &pm, &sig, sm, &l2).await
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
            if let Ok(resp) = c
                .get("https://api.gateio.ws/api/v4/futures/usdt/tickers")
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                if let Ok(tickers) = resp.json::<Vec<Value>>().await {
                    for t in &tickers {
                        let name = t["contract"].as_str().unwrap_or("").replace('_', "");
                        if !name.ends_with("USDT") {
                            continue;
                        }
                        let vol: f64 = t["volume_24h_quote"]
                            .as_str()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0);
                        let funding: f64 = t["funding_rate"]
                            .as_str()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0);
                        let price: f64 = t["last"]
                            .as_str()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0);
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
    for h in remaining {
        h.abort();
    }

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow::anyhow!("WS shard task panicked: {}", e)),
    }
}



/// A single WS shard handling a subset of symbols.
async fn ws_shard(
    idx: usize,
    symbols: Vec<String>,
    price_map: &PriceMap,
    signal: &UpdateSignal,
    sym_map: Arc<std::collections::HashMap<String, (String, f64)>>,
    l2_books: &L2OrderbookMap,
) -> Result<()> {
    let (mut ws, _) =
        connect_async_tls_with_config("wss://fx-ws.gateio.ws/v4/ws/usdt", None, false, None)
            .await
            .context(format!("Gate WS shard {} connect failed", idx))?;

    info!(
        "[gate] Shard {} connected, subscribing to {} symbols",
        idx,
        symbols.len()
    );

    // Subscribe to L1 book_ticker for all symbols in this shard
    for chunk in symbols.chunks(50) {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        ws.send(Message::Text(
            json!({"time":ts,"channel":"futures.book_ticker","event":"subscribe","payload": chunk})
                .to_string()
                .into(),
        ))
        .await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Subscribe to L2 order_book (20 levels) for all symbols in this shard
    for chunk in symbols.chunks(20) {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // payload format: ["CONTRACT", "depth_level", "0"] where "0" = real-time
        let payload: Vec<Value> = chunk.iter()
            .map(|s| json!([s, "20", "0"]))
            .collect();
        for item in payload {
            ws.send(Message::Text(
                json!({"time":ts,"channel":"futures.order_book","event":"subscribe","payload": item})
                    .to_string()
                    .into(),
            ))
            .await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    info!("[gate] Shard {} subscribed L1+L2 OK", idx);

    // Ensure gate map exists for mutably getting later
    price_map.entry("gate".to_string()).or_default();

    let mut ping = tokio::time::interval(Duration::from_secs(20));

    loop {
        tokio::select! {
            msg = tokio::time::timeout(Duration::from_secs(30), ws.next()) => match msg.ok().flatten() {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        let channel = v["channel"].as_str().unwrap_or("");
                        let event = v["event"].as_str().unwrap_or("");
                        if event == "subscribe" || event == "unsubscribe" { continue; }

                        if channel == "futures.book_ticker" {
                            // L1: best bid/ask
                            if let Some(item) = v["result"].as_object() {
                                let raw_sym = item.get("s").and_then(|v| v.as_str()).unwrap_or("");
                                if let Some((sym, mult)) = sym_map.get(raw_sym) {
                                    let bid: f64 = item.get("b").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    let ask: f64 = item.get("a").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                    let bid_qty: f64 = item.get("B").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                    let ask_qty: f64 = item.get("A").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                    if bid <= 0.0 || ask <= 0.0 { continue; }
                                    let mid = (bid + ask) / 2.0;
                                    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                                    if let Some(mut ex_map) = price_map.get_mut("gate") {
                                        if let Some(entry) = ex_map.get_mut(sym) {
                                            entry.bid = bid; entry.ask = ask;
                                            entry.bid_size = bid_qty * mult * mid; entry.ask_size = ask_qty * mult * mid;
                                            if entry.price == 0.0 { entry.price = mid; }
                                            entry.last_updated = ts;
                                        } else {
                                            ex_map.insert(sym.to_string(), crate::state::TickerData {
                                                bid, ask, price: mid, vol: 0.0, funding_rate: 0.0,
                                                bid_size: bid_qty * mult * mid, ask_size: ask_qty * mult * mid,
                                                last_updated: ts
                                            });
                                        }
                                    }
                                    signal.notify_one();
                                }
                            }
                        } else if channel == "futures.order_book" {
                            // L2: order book depth
                            if let Some(result) = v["result"].as_object() {
                                let raw_sym = result.get("s").and_then(|v| v.as_str()).unwrap_or("");
                                if let Some((std_sym, mult)) = sym_map.get(raw_sym) {
                                    let now_ms = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default().as_millis() as i64;
                                    let parse_level = |arr: &Value| -> Vec<(f64, f64)> {
                                        arr.as_array().map(|levels| {
                                            levels.iter().filter_map(|l| {
                                                // Handle {"p": "...", "s": ...} format
                                                if let Some(obj) = l.as_object() {
                                                    let p = obj.get("p").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok())
                                                        .or_else(|| obj.get("p").and_then(|v| v.as_f64()))?;
                                                    let q = obj.get("s").and_then(|v| v.as_str()).and_then(|s| s.parse::<f64>().ok())
                                                        .or_else(|| obj.get("s").and_then(|v| v.as_f64()))
                                                        .or_else(|| obj.get("s").and_then(|v| v.as_i64()).map(|v| v as f64))?;
                                                    return Some((p, q * mult));
                                                }
                                                // Handle ["...", "..."] format
                                                let p = l[0].as_str().and_then(|s| s.parse::<f64>().ok())
                                                    .or_else(|| l[0].as_f64())?;
                                                let q = l[1].as_str().and_then(|s| s.parse::<f64>().ok())
                                                    .or_else(|| l[1].as_f64())?;
                                                Some((p, q * mult))
                                            }).collect()
                                        }).unwrap_or_default()
                                    };
                                    let mut bids = parse_level(&v["result"]["bids"]);
                                    if bids.is_empty() { bids = parse_level(&v["result"]["b"]); }
                                    let mut asks = parse_level(&v["result"]["asks"]);
                                    if asks.is_empty() { asks = parse_level(&v["result"]["a"]); }
                                    if !bids.is_empty() || !asks.is_empty() {
                                        l2_books.insert(
                                            format!("gate:{}", std_sym),
                                            L2Snapshot { bids, asks, updated_ms: now_ms },
                                        );
                                    }
                                }
                            }
                        } else if channel == "futures.pong" || channel == "futures.ping" {
                            // ignore
                        } else if v.get("error").is_some() {
                            warn!("[gate] Shard {} WS Error: {}", idx, text);
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

fn upsert_tickers(
    price_map: &PriceMap,
    signal: &UpdateSignal,
    sym: String,
    price: f64,
    vol: f64,
    funding: f64,
) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if let Some(mut gate_map) = price_map.get_mut("gate") {
        if let Some(entry) = gate_map.get_mut(&sym) {
            entry.price = price;
            entry.vol = vol;
            entry.funding_rate = funding;
            entry.last_updated = ts;
        }
    }
    signal.notify_one();
}
