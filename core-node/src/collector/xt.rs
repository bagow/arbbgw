//! XT.COM USDT-M Perpetual WebSocket collector
//! Public WS: wss://fapi.xt.com/ws/market
//! Subscribes to bookTicker channel for all USDT perp pairs
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
    _l2_wanted: std::sync::Arc<dashmap::DashSet<String>>,
    settings: std::sync::Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let is_disabled = {
            let p = settings.read().await;
            !p.is_empty() && p.iter().all(|x| x.disabled_exchanges.contains(&"xt".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) { info!("[xt] Disabled globally. Sleeping..."); }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let dis = settings.read().await;
                if !dis.is_empty() && dis.iter().all(|x| x.disabled_exchanges.contains(&"xt".to_string())) { break; }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal, &l2_books) => {
                match res {
                    Ok(_) => { warn!("[xt] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => warn!("[xt] WS disconnected: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
            _ = disable_watcher => { info!("[xt] disabled dynamically."); backoff = Duration::from_secs(1); }
        }
    }
}

/// XT WS symbol → standard symbol: "btc_usdt" → "BTCUSDT"
fn xt_to_standard(sym: &str) -> String {
    sym.replace('_', "").to_uppercase()
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal, l2_books: &L2OrderbookMap) -> Result<()> {
    let http = Client::builder().user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36").build().unwrap();
    info!("[xt] Loading symbols from REST...");

    // Seed from REST
    let mut xt_symbols: Vec<String> = Vec::new(); // "btc_usdt" format
    if let Ok(resp) = http
        .get("https://fapi.xt.com/future/market/v1/public/symbol/list")
        .timeout(Duration::from_secs(15))
        .send().await
    {
        if let Ok(body) = resp.json::<Value>().await {
            // result is a flat array, not result.symbols
            if let Some(arr) = body["result"].as_array() {
                let mut seeded = 0usize;
                for c in arr {
                    // only USDT-settled perpetuals
                    if c["underlyingType"].as_str() != Some("U_BASED") { continue; }
                    let sym = c["symbol"].as_str().unwrap_or(""); // "btc_usdt"
                    let std_sym = xt_to_standard(sym); // "BTCUSDT"
                    if !std_sym.ends_with("USDT") || sym.is_empty() { continue; }
                    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                    price_map.entry("xt".to_string()).or_default().entry(std_sym).or_insert(TickerData {
                        bid: 0.0, ask: 0.0, price: 0.0, vol: 0.0, funding_rate: 0.0,
                        bid_size: 0.0, ask_size: 0.0, last_updated: ts,
                    });
                    xt_symbols.push(sym.to_string());
                    seeded += 1;
                }
                info!("[xt] Seeded {} symbols from REST", seeded);
            }
        }
    }
    // Seed 24h volume and prices from REST tickers
    if let Ok(resp) = http
        .get("https://fapi.xt.com/future/market/v1/public/q/tickers")
        .timeout(Duration::from_secs(15))
        .send().await
    {
        if let Ok(body) = resp.json::<Value>().await {
            if let Some(arr) = body["result"].as_array() {
                let mut vol_seeded = 0usize;
                for t in arr {
                    let sym = t["s"].as_str().unwrap_or(""); // "btc_usdt"
                    let std_sym = xt_to_standard(sym); // "BTCUSDT"
                    if !std_sym.ends_with("USDT") { continue; }
                    let vol: f64 = t["v"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0); // quote volume
                    let close: f64 = t["c"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    if vol > 0.0 || close > 0.0 {
                        if let Some(mut ex_map) = price_map.get_mut("xt") {
                            if let Some(entry) = ex_map.get_mut(&std_sym) {
                                entry.vol = vol;
                                if close > 0.0 { entry.price = close; }
                            }
                        }
                        vol_seeded += 1;
                    }
                }
                info!("[xt] Seeded 24h volume for {} symbols", vol_seeded);
            }
        }
    }

    if xt_symbols.is_empty() { return Err(anyhow::anyhow!("No XT USDT perp symbols found from REST")); }

    info!("[xt] Connecting WS with {} symbols...", xt_symbols.len());
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://fstream.xt.com/ws/market",
        None, false, None,
    ).await.context("XT WS connect failed")?;
    info!("[xt] Connected, subscribing {} symbols...", xt_symbols.len());

    // Subscribe to ticker (for volume) AND depth_update (for bid/ask) — XT format: topic@symbol
    // ticker only gives OHLC, no bid/ask. depth_update gives incremental bids/asks.
    for chunk in xt_symbols.chunks(20) {
        let mut args: Vec<String> = Vec::new();
        for s in chunk {
            args.push(format!("ticker@{}", s));
            args.push(format!("depth_update@{}", s));
        }
        ws.send(Message::Text(json!({
            "method": "SUBSCRIBE",
            "params": args,
            "id": 1
        }).to_string().into())).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    price_map.entry("xt".to_string()).or_default();

    let mut ping = tokio::time::interval(Duration::from_secs(15));
    let mut msg_count: u64 = 0;
    let mut tick_count: u64 = 0;
    let mut first_msg_logged = false;

    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    msg_count += 1;
                    if msg_count <= 5 {
                        tracing::debug!("[xt] MSG #{} (len={}): {}", msg_count, text.len(), &text[..text.len().min(400)]);
                    }
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        let topic = v["topic"].as_str().unwrap_or("");
                        
                        // Extract symbol from event "topic@symbol" or data.s
                        let xt_sym = v["data"]["s"].as_str()
                            .or_else(|| v["symbol"].as_str())
                            .or_else(|| {
                                v["event"].as_str().and_then(|e| {
                                    e.split_once('@').map(|(_, sym)| sym)
                                })
                            })
                            .unwrap_or("");
                        let std_sym = xt_to_standard(xt_sym);
                        if !std_sym.ends_with("USDT") { continue; }
                        
                        if topic == "ticker" {
                            // Ticker: OHLC + volume — update vol and fallback price
                            let close: f64 = v["data"]["c"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let vol: f64 = v["data"]["v"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                            if let Some(mut ex_map) = price_map.get_mut("xt") {
                                let entry = ex_map.entry(std_sym.clone()).or_insert(TickerData {
                                    bid: 0.0, ask: 0.0, price: 0.0, vol: 0.0, funding_rate: 0.0,
                                    bid_size: 0.0, ask_size: 0.0, last_updated: ts,
                                });
                                if vol > 0.0 { entry.vol = vol; }
                                if close > 0.0 && entry.price == 0.0 { entry.price = close; }
                            }
                        } else if topic == "depth" || topic == "depth_update" {
                            // Depth: bid/ask levels — extract best bid/ask
                            let parse_side = |key: &str| -> Option<(f64, f64)> {
                                v["data"][key].as_array().and_then(|arr| {
                                    arr.first().and_then(|level| {
                                        let px: f64 = level[0].as_str()?.parse().ok()?;
                                        let sz: f64 = level[1].as_str()?.parse().ok()?;
                                        if px > 0.0 { Some((px, sz)) } else { None }
                                    })
                                })
                            };
                            
                            let best_bid = parse_side("b").or_else(|| parse_side("bids"));
                            let best_ask = parse_side("a").or_else(|| parse_side("asks"));
                            
                            if let (Some((bid, bid_sz)), Some((ask, ask_sz))) = (best_bid, best_ask) {
                                if bid <= 0.0 || ask <= 0.0 { continue; }
                                let mid = (bid + ask) / 2.0;
                                let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                                if let Some(mut ex_map) = price_map.get_mut("xt") {
                                    if let Some(entry) = ex_map.get_mut(&std_sym) {
                                        entry.bid = bid; entry.ask = ask; entry.price = mid;
                                        entry.bid_size = bid_sz * mid; entry.ask_size = ask_sz * mid;
                                        entry.last_updated = ts;
                                    } else {
                                        ex_map.insert(std_sym.clone(), TickerData {
                                            bid, ask, price: mid, vol: 0.0, funding_rate: 0.0,
                                            bid_size: bid_sz * mid, ask_size: ask_sz * mid, last_updated: ts,
                                        });
                                    }
                                }
                                let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
                                l2_books.insert(
                                    format!("xt:{}", std_sym),
                                    crate::state::L2Snapshot {
                                        bids: v["data"]["b"].as_array().or_else(|| v["data"]["bids"].as_array())
                                            .map(|arr| arr.iter().take(5).filter_map(|l| {
                                                let px: f64 = l[0].as_str()?.parse().ok()?;
                                                let sz: f64 = l[1].as_str()?.parse().ok()?;
                                                Some((px, sz))
                                            }).collect()).unwrap_or_else(|| vec![(bid, bid_sz)]),
                                        asks: v["data"]["a"].as_array().or_else(|| v["data"]["asks"].as_array())
                                            .map(|arr| arr.iter().take(5).filter_map(|l| {
                                                let px: f64 = l[0].as_str()?.parse().ok()?;
                                                let sz: f64 = l[1].as_str()?.parse().ok()?;
                                                Some((px, sz))
                                            }).collect()).unwrap_or_else(|| vec![(ask, ask_sz)]),
                                        updated_ms: now_ms,
                                    }
                                );
                                signal.notify_one();
                                tick_count += 1;
                            }
                        }
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => {
                ws.send(Message::Text(json!({"method":"ping","id":2}).to_string().into())).await?;
                let _ = ws.send(Message::Text("ping".into())).await; // Fallback text ping
                tracing::debug!("[xt] DIAG: msgs={} ticks={}", msg_count, tick_count);
            }
        }
    }
    Ok(())
}
