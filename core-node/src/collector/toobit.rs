//! Toobit USDT-M Perpetual WebSocket collector
//! Public WS: wss://stream.toobit.com/swap-market
//! API mirrors BingX closely - subscribes to ticker data
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{L2OrderbookMap, PriceMap, TickerData, UpdateSignal};

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
            !p.is_empty() && p.iter().all(|x| x.disabled_exchanges.contains(&"toobit".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) { info!("[toobit] Disabled globally. Sleeping..."); }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let dis = settings.read().await;
                if !dis.is_empty() && dis.iter().all(|x| x.disabled_exchanges.contains(&"toobit".to_string())) { break; }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal, &l2_books) => {
                match res {
                    Ok(_) => { warn!("[toobit] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => error!("[toobit] Error: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            _ = disable_watcher => { info!("[toobit] disabled dynamically."); backoff = Duration::from_secs(1); }
        }
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal, l2_books: &L2OrderbookMap) -> Result<()> {
    let http = Client::builder().user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36").build().unwrap();

    // Top USDT-M perp pairs on Toobit (fallback when REST is unreachable)
    const FALLBACK: &[&str] = &[
        "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","DOGEUSDT","ADAUSDT",
        "AVAXUSDT","DOTUSDT","LINKUSDT","LTCUSDT","MATICUSDT","UNIUSDT","ATOMUSDT",
        "ETCUSDT","XLMUSDT","FILUSDT","AAVEUSDT","TRXUSDT","EOSUSDT","ALGOUSDT",
        "ICPUSDT","SANDUSDT","MANAUSDT","AXSUSDT","CRVUSDT","MKRUSDT","SNXUSDT",
        "NEARUSDT","FTMUSDT","HBARUSDT","APEUSDT","OPUSDT","APTUSDT","ARBUSDT",
        "SUIUSDT","PEPEUSDT","WLDUSDT","INJUSDT","TIAUSDT","SEIUSDT","JUPUSDT",
        "ORDIUSDT","RNDRUSDT","FETUSDT","TONUSDT","NOTUSDT","BOMEUSDT","ENAUSDT",
        "WIFUSDT","STXUSDT","RUNEUSDT","LDOUSDT","GMTUSDT","GALAUSDT","FLOWUSDT",
    ];

    // Try REST first (may be blocked from some regions)
    let mut rest_ok = false;
    if let Ok(resp) = http
        .get("https://api.toobit.com/openApi/swap/v2/quote/contracts")
        .timeout(Duration::from_secs(8))
        .send().await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(list) = body["data"].as_array() {
                let mut seeded = 0usize;
                for t in list {
                    let sym = t["symbol"].as_str().unwrap_or("");
                    if !sym.ends_with("USDT") { continue; }
                    price_map.entry("toobit".to_string()).or_default()
                        .entry(sym.to_string()).or_default();
                    seeded += 1;
                }
                if seeded > 0 {
                    info!("[toobit] Seeded {} symbols from REST", seeded);
                    rest_ok = true;
                }
            }
        }
    }

    // Fallback: seed hardcoded list if REST was blocked/empty
    if !rest_ok {
        warn!("[toobit] REST unavailable, using hardcoded fallback ({} symbols)", FALLBACK.len());
        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        for &sym in FALLBACK {
            price_map.entry("toobit".to_string()).or_default()
                .entry(sym.to_string()).or_insert(TickerData {
                    bid: 0.0, ask: 0.0, price: 0.0, vol: 0.0, funding_rate: 0.0,
                    bid_size: 0.0, ask_size: 0.0, last_updated: ts,
                });
        }
    }

    let symbols: Vec<String> = {
        let map = price_map.get("toobit");
        match map { Some(m) => m.keys().cloned().collect(), None => vec![] }
    };
    if symbols.is_empty() { return Err(anyhow::anyhow!("No Toobit symbols loaded")); }

    // Seed 24h volume from REST tickers
    if let Ok(resp) = http
        .get("https://api.toobit.com/quote/v1/ticker/24hr")
        .timeout(Duration::from_secs(15))
        .send().await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(arr) = body.as_array() {
                let mut vol_seeded = 0usize;
                for t in arr {
                    let sym = t["s"].as_str().unwrap_or("");
                    if !sym.ends_with("USDT") { continue; }
                    let qv: f64 = t["qv"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let close: f64 = t["c"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    if qv > 0.0 {
                        if let Some(mut ex_map) = price_map.get_mut("toobit") {
                            if let Some(entry) = ex_map.get_mut(sym) {
                                entry.vol = qv;
                                if close > 0.0 && entry.price == 0.0 { entry.price = close; }
                            }
                        }
                        vol_seeded += 1;
                    }
                }
                info!("[toobit] Seeded 24h volume for {} symbols", vol_seeded);
            }
        }
    }

    let (mut ws, _) = connect_async_tls_with_config(
        "wss://stream.toobit.com/swap-market",
        None, false, None,
    ).await.context("Toobit WS connect failed")?;
    info!("[toobit] Connected, subscribing {} symbols...", symbols.len());

    price_map.entry("toobit".to_string()).or_default();

    // Subscribe in chunks of 30
    for chunk in symbols.chunks(30) {
        let subs: Vec<Value> = chunk.iter().map(|s| {
            json!({"symbol": s, "topic": "depth", "event": "sub"})
        }).collect();
        for sub in subs {
            ws.send(Message::Text(sub.to_string().into())).await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let mut ping = tokio::time::interval(Duration::from_secs(25));

    loop {
        tokio::select! {
            msg = tokio::time::timeout(Duration::from_secs(35), ws.next()) => match msg.ok().flatten() {
                Some(Ok(Message::Text(text))) => {
                    if text.contains("\"ping\"") {
                        let _ = ws.send(Message::Text(text.replace("ping", "pong"))).await;
                        continue;
                    }
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        if v["topic"].as_str() == Some("depth") {
                            let sym = v["symbol"].as_str().unwrap_or("");
                            if !sym.ends_with("USDT") { continue; }
                            
                            let parse_levels = |key: &str| -> Vec<(f64, f64)> {
                                v["data"][key].as_array().map(|arr| {
                                    arr.iter().filter_map(|l| {
                                        let px: f64 = l[0].as_str()?.parse().ok()?;
                                        let sz: f64 = l[1].as_str()?.parse().ok()?;
                                        Some((px, sz))
                                    }).collect()
                                }).unwrap_or_default()
                            };
                            
                            let bids = parse_levels("b");
                            let asks = parse_levels("a");
                            if bids.is_empty() || asks.is_empty() { continue; }
                            
                            let bid = bids[0].0;
                            let ask = asks[0].0;
                            let mid = (bid + ask) / 2.0;
                            let bid_sz = bids[0].1 * bid;
                            let ask_sz = asks[0].1 * ask;
                            let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                            
                            if let Some(mut ex_map) = price_map.get_mut("toobit") {
                                if let Some(entry) = ex_map.get_mut(sym) {
                                    entry.bid = bid; entry.ask = ask;
                                    entry.price = mid;
                                    entry.bid_size = bid_sz; entry.ask_size = ask_sz;
                                    entry.last_updated = ts;
                                } else {
                                    ex_map.insert(sym.to_string(), TickerData {
                                        bid, ask, price: mid,
                                        vol: 0.0, funding_rate: 0.0, bid_size: bid_sz, ask_size: ask_sz,
                                        last_updated: ts,
                                    });
                                }
                            }
                            let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
                            let limited_bids = bids.into_iter().take(5).collect();
                            let limited_asks = asks.into_iter().take(5).collect();
                            l2_books.insert(
                                format!("toobit:{}", sym),
                                crate::state::L2Snapshot {
                                    bids: limited_bids,
                                    asks: limited_asks,
                                    updated_ms: now_ms,
                                }
                            );
                            signal.notify_one();
                        }
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => {
                ws.send(Message::Text("Ping".into())).await?;
            }
        }
    }
    Ok(())
}
