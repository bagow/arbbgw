//! Bybit USDT perpetual futures WebSocket collector
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{L2OrderbookMap, L2Snapshot, PriceMap, TickerData, UpdateSignal};

const SYMBOL_CACHE_PATH: &str = "/home/ubuntu/core-node/bybit_symbols_cache.json";

/// Load symbol list from disk cache. Returns symbols if file is fresh enough.
/// always_use=true: use even if stale (for fallback on REST failure).
fn load_disk_cache(always_use: bool) -> Option<Vec<(f64, String)>> {
    use std::fs;
    let meta = fs::metadata(SYMBOL_CACHE_PATH).ok()?;
    let age = meta
        .modified()
        .ok()?
        .elapsed()
        .unwrap_or(std::time::Duration::from_secs(u64::MAX));
    if !always_use && age.as_secs() > 3600 {
        return None; // Older than 1 hour — force fresh fetch
    }
    let data = fs::read_to_string(SYMBOL_CACHE_PATH).ok()?;
    let syms: Vec<(f64, String)> = serde_json::from_str(&data).ok()?;
    if syms.is_empty() {
        return None;
    }
    Some(syms)
}

/// Save symbol list to disk so restarts don't trigger REST rate limits.
fn save_disk_cache(syms: &[(f64, String)]) {
    use std::fs;
    if let Some(parent) = std::path::Path::new(SYMBOL_CACHE_PATH).parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(json) = serde_json::to_string(syms) {
        let _ = fs::write(SYMBOL_CACHE_PATH, json);
    }
}

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
                    .all(|x| x.disabled_exchanges.contains(&"bybit".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) {
                info!("[bybit] Disabled globally in profiles. Sleeping...");
            }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        info!("[bybit] Connecting...");

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"bybit".to_string()))
                };
                if is_disabled {
                    break;
                }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal, &l2_books) => {
                match res {
                    Ok(_)  => { warn!("[bybit] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => error!("[bybit] Error: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            _ = disable_watcher => {
                info!("[bybit] Exchange was disabled dynamically. Dropping connection.");
                backoff = Duration::from_secs(1);
            }
        }
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal, l2_books: &L2OrderbookMap) -> Result<()> {
    let http = Client::new();

    // ── SYMBOL LIST: disk cache → REST fetch → stale disk fallback ──────────
    // Rapid restarts + REST rate limits require a disk-backed symbol cache.
    // Fresh disk cache (< 1h): skip REST entirely.
    // REST fails: use stale disk cache rather than failing entirely.
    let mut symbols: Vec<(f64, String)> = Vec::new();

    // Try fresh disk cache first (< 1 hour old)
    if let Some(cached) = load_disk_cache(false) {
        symbols = cached;
        info!("[bybit] Using disk-cached symbol list ({} symbols)", symbols.len());
    } else {
        // Fetch from REST
        let rest_ok = match http
            .get("https://api.bybit.com/v5/market/tickers?category=linear")
            .timeout(Duration::from_secs(15))
            .send()
            .await
        {
            Ok(resp) => match resp.json::<Value>().await {
                Ok(v) => {
                    if let Some(list) = v["result"]["list"].as_array() {
                        for t in list {
                            let sym = t["symbol"].as_str().unwrap_or("");
                            if !sym.ends_with("USDT") {
                                continue;
                            }
                            let vol: f64 = t["turnover24h"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let bid: f64 = t["bid1Price"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let ask: f64 = t["ask1Price"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let bid_qty: f64 = t["bid1Size"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let ask_qty: f64 = t["ask1Size"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let price: f64 = t["lastPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid + ask) / 2.0);
                            if bid > 0.0 && ask > 0.0 {
                                let ts = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs();
                                price_map.entry("bybit".to_string()).or_default().insert(
                                    sym.to_string(),
                                    TickerData { bid, ask, price, vol, funding_rate: 0.0,
                                        bid_size: bid_qty * price, ask_size: ask_qty * price,
                                        last_updated: ts },
                                );
                                signal.notify_one();
                            }
                            symbols.push((vol, sym.to_string()));
                        }
                    }
                    !symbols.is_empty()
                }
                Err(e) => { warn!("[bybit] REST parse error: {}", e); false }
            },
            Err(e) => { warn!("[bybit] REST fetch error: {}", e); false }
        };

        if rest_ok {
            // Save fresh list to disk for future restarts
            save_disk_cache(&symbols);
            info!("[bybit] Fetched {} USDT symbols from REST, saved to disk cache", symbols.len());
        } else {
            // REST failed — try stale disk cache as last resort
            if let Some(cached) = load_disk_cache(true) {
                symbols = cached;
                warn!("[bybit] REST failed, using stale disk cache ({} symbols)", symbols.len());
            }
        }
    }

    if symbols.is_empty() {
        return Err(anyhow::anyhow!("Failed to fetch symbols: REST failed and no disk cache available"));
    }
    // Sort by 24h volume desc so top-volume symbols get L2 coverage
    symbols.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let symbols: Vec<String> = symbols.into_iter().map(|(_, s)| s).collect();
        // Filter symbols: only subscribe to L2 depth if the symbol exists on at least one OTHER exchange
        let mut needed_symbols = Vec::new();
        for sym in symbols {
            let mut count = 0;
            for ex in price_map.iter() {
                if ex.key() != "bybit" && ex.value().contains_key(&sym) {
                    count += 1;
                    break;
                }
            }
            if count > 0 {
                needed_symbols.push(sym);
            }
        }
        let symbols = needed_symbols;

    info!("[bybit] Seeded {} USDT linear tickers", symbols.len());

    // WS: subscribe to tickers in batches of 10
    let (mut ws, _) =
        connect_async_tls_with_config("wss://stream.bybit.com/v5/public/linear", None, false, None)
            .await
            .context("Bybit WS connect failed")?;
    info!("[bybit] Connected, subscribing...");

    // Subscribe to L1 (orderbook.1) for all symbols
    for chunk in symbols.chunks(10) {
        let args: Vec<String> = chunk.iter().map(|s| format!("orderbook.1.{}", s)).collect();
        ws.send(Message::Text(
            json!({"op":"subscribe","args":args}).to_string().into(),
        ))
        .await?;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // Subscribe to L2 (orderbook.25) for ALL symbols — arb opportunities
    // are often in low-volume pairs, so we can't cap by volume rank.
    for chunk in symbols.chunks(10) {
        let args: Vec<String> = chunk.iter().map(|s| format!("orderbook.25.{}", s)).collect();
        ws.send(Message::Text(
            json!({"op":"subscribe","args":args}).to_string().into(),
        ))
        .await?;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    info!("[bybit] Subscribed L2 depth for ALL {} symbols", symbols.len());

    let mut _ping = tokio::time::interval(Duration::from_secs(20));
    #[derive(Deserialize)]
    struct BybitMsg<'a> {
        #[serde(borrow)]
        topic: Option<&'a str>,
        #[serde(borrow)]
        r#type: Option<&'a str>,
        #[serde(borrow)]
        data: Option<BybitData<'a>>,
    }

    #[derive(Deserialize)]
    struct BybitData<'a> {
        s: Option<&'a str>,
        #[serde(borrow)]
        b: Option<Vec<(&'a str, &'a str)>>,
        #[serde(borrow)]
        a: Option<Vec<(&'a str, &'a str)>>,
    }

    // Ensure bybit map exists for getting mutably later
    price_map.entry("bybit".to_string()).or_default();

    let mut ping = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            msg = tokio::time::timeout(Duration::from_secs(30), ws.next()) => match msg.ok().flatten() {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(v) = serde_json::from_str::<BybitMsg>(&text) {
                        let topic = match v.topic {
                            Some(t) => t,
                            None => continue,
                        };
                        if let Some(d) = v.data {
                            if let Some(sym) = d.s {
                                if !sym.ends_with("USDT") { continue; }

                                // L2 depth (orderbook.25) → update l2_books cache
                                if topic.starts_with("orderbook.25.") {
                                    let type_str = v.r#type.unwrap_or("");
                                    let now_ms = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis() as i64;
                                        
                                    let new_bids: Vec<(f64, f64)> = d.b.as_ref().map(|levels| {
                                        levels.iter().filter_map(|&(p, q)| {
                                            Some((p.parse::<f64>().ok()?, q.parse::<f64>().ok()?))
                                        }).collect()
                                    }).unwrap_or_default();
                                    let new_asks: Vec<(f64, f64)> = d.a.as_ref().map(|levels| {
                                        levels.iter().filter_map(|&(p, q)| {
                                            Some((p.parse::<f64>().ok()?, q.parse::<f64>().ok()?))
                                        }).collect()
                                    }).unwrap_or_default();

                                    let key = format!("bybit:{}", sym);
                                    
                                    if type_str == "snapshot" {
                                        l2_books.insert(key, crate::state::L2Snapshot { bids: new_bids, asks: new_asks, updated_ms: now_ms });
                                    } else if type_str == "delta" {
                                        if let Some(mut book) = l2_books.get_mut(&key) {
                                            // Apply bids
                                            for (p, q) in new_bids {
                                                if q == 0.0 {
                                                    book.bids.retain(|&(bp, _)| (bp - p).abs() > 1e-8);
                                                } else {
                                                    if let Some(idx) = book.bids.iter().position(|&(bp, _)| (bp - p).abs() <= 1e-8) {
                                                        book.bids[idx].1 = q;
                                                    } else {
                                                        book.bids.push((p, q));
                                                    }
                                                }
                                            }
                                            book.bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                                            
                                            // Apply asks
                                            for (p, q) in new_asks {
                                                if q == 0.0 {
                                                    book.asks.retain(|&(ap, _)| (ap - p).abs() > 1e-8);
                                                } else {
                                                    if let Some(idx) = book.asks.iter().position(|&(ap, _)| (ap - p).abs() <= 1e-8) {
                                                        book.asks[idx].1 = q;
                                                    } else {
                                                        book.asks.push((p, q));
                                                    }
                                                }
                                            }
                                            book.asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                                            
                                            book.updated_ms = now_ms;
                                        }
                                    }
                                    continue;
                                }

                                // L1 ticker (orderbook.1) → update PriceMap
                                if !topic.starts_with("orderbook.1.") { continue; }

                                if let Some(mut ex_map) = price_map.get_mut("bybit") {
                                    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

                                    if let Some(prev) = ex_map.get_mut(sym) {
                                        if let Some(bids) = d.b {
                                            if let Some(&(p, q)) = bids.first() {
                                                if let (Ok(p_f), Ok(q_f)) = (p.parse::<f64>(), q.parse::<f64>()) {
                                                    prev.bid = p_f; prev.bid_size = q_f * p_f;
                                                }
                                            }
                                        }
                                        if let Some(asks) = d.a {
                                            if let Some(&(p, q)) = asks.first() {
                                                if let (Ok(p_f), Ok(q_f)) = (p.parse::<f64>(), q.parse::<f64>()) {
                                                    prev.ask = p_f; prev.ask_size = q_f * p_f;
                                                }
                                            }
                                        }

                                        if prev.bid > 0.0 && prev.ask > 0.0 {
                                            prev.price = (prev.bid + prev.ask) / 2.0;
                                            prev.last_updated = ts;
                                            signal.notify_one();
                                        }
                                    } else {
                                        let mut b_p = 0.0; let mut b_q = 0.0;
                                        let mut a_p = 0.0; let mut a_q = 0.0;
                                        if let Some(bids) = d.b {
                                            if let Some(&(p, q)) = bids.first() {
                                                b_p = p.parse().unwrap_or(0.0); b_q = q.parse().unwrap_or(0.0);
                                            }
                                        }
                                        if let Some(asks) = d.a {
                                            if let Some(&(p, q)) = asks.first() {
                                                a_p = p.parse().unwrap_or(0.0); a_q = q.parse().unwrap_or(0.0);
                                            }
                                        }
                                        if b_p > 0.0 && a_p > 0.0 {
                                            let mid = (b_p + a_p) / 2.0;
                                            ex_map.insert(sym.to_string(), TickerData {
                                                bid: b_p, ask: a_p, price: mid, vol: 0.0, funding_rate: 0.0,
                                                bid_size: b_q * b_p, ask_size: a_q * a_p,
                                                last_updated: ts
                                            });
                                            signal.notify_one();
                                        }
                                    }
                                }
                            }
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
