//! Bybit USDT perpetual futures WebSocket collector
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{info, warn};

use crate::state::{L2OrderbookMap, PriceMap, TickerData, UpdateSignal};

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
    l2_wanted: std::sync::Arc<dashmap::DashSet<String>>,
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
            res = handler(&prices, &signal, &l2_books, &l2_wanted) => {
                match res {
                    Ok(_)  => { warn!("[bybit] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => warn!("[bybit] WS disconnected: {}. Reconnecting in {:?}", e, backoff),
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

async fn handler(price_map: &PriceMap, signal: &UpdateSignal, l2_books: &L2OrderbookMap, l2_wanted: &std::sync::Arc<dashmap::DashSet<String>>) -> Result<()> {
    let http = Client::builder().user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36").build().unwrap();

    // ── SYMBOL LIST: disk cache → REST fetch → stale disk fallback ──────────
    // Rapid restarts + REST rate limits require a disk-backed symbol cache.
    // Fresh disk cache (< 1h): skip REST entirely.
    // REST fails: use stale disk cache rather than failing entirely.
    let mut symbols: Vec<(f64, String)> = Vec::new();

    // Try fresh disk cache first (< 1 hour old)
    if let Some(cached) = load_disk_cache(false) {
        symbols = cached;
        info!("[bybit] Using disk-cached symbol list ({} symbols)", symbols.len());
        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        let mut ex_map = price_map.entry("bybit".to_string()).or_default();
        for (vol, sym) in &symbols {
            ex_map.insert(sym.clone(), TickerData {
                bid: 0.0, ask: 0.0, price: 0.0, vol: *vol, funding_rate: 0.0,
                bid_size: 0.0, ask_size: 0.0, last_updated: ts
            });
        }
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
                let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                let mut ex_map = price_map.entry("bybit".to_string()).or_default();
                for (vol, sym) in &symbols {
                    ex_map.insert(sym.clone(), TickerData {
                        bid: 0.0, ask: 0.0, price: 0.0, vol: *vol, funding_rate: 0.0,
                        bid_size: 0.0, ask_size: 0.0, last_updated: ts
                    });
                }
            }
        }
    }

    if symbols.is_empty() {
        return Err(anyhow::anyhow!("Failed to fetch symbols: REST failed and no disk cache available"));
    }
    // Sort by 24h volume desc so top-volume symbols get L2 coverage
    symbols.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let symbols: Vec<String> = symbols.into_iter().map(|(_, s)| s).collect();


    info!("[bybit] Seeded {} USDT linear tickers", symbols.len());

    // WS: connect and split into reader/writer for concurrent access
    let (ws, _) =
        connect_async_tls_with_config("wss://stream.bybit.com/v5/public/linear", None, false, None)
            .await
            .context("Bybit WS connect failed")?;
    let (write_half, read_half) = ws.split();
    let writer = Arc::new(Mutex::new(write_half));
    info!("[bybit] Connected, subscribing...");

    // Subscribe to L1 (orderbook.1) for all symbols
    {
        let mut w = writer.lock().await;
        for chunk in symbols.chunks(10) {
            let args: Vec<String> = chunk.iter().map(|s| format!("orderbook.1.{}", s)).collect();
            w.send(Message::Text(
                json!({"op":"subscribe","args":args}).to_string().into(),
            ))
            .await?;
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }
    info!("[bybit] Subscribed L1 for {} symbols", symbols.len());

    // ── Dynamic L2 subscription manager ─────────────────────────────────────
    // Instead of static top-150, subscribe only to pairs in l2_wanted.
    // Checks every 5s for changes and sends subscribe/unsubscribe.
    let l2_sub_alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
    {
        let writer_sub = writer.clone();
        let l2_wanted_sub = l2_wanted.clone();
        let l2_books_sub = l2_books.clone();
        let alive = l2_sub_alive.clone();
        tokio::spawn(async move {
            let mut current_subs: HashSet<String> = HashSet::new();
            while alive.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if !alive.load(std::sync::atomic::Ordering::Relaxed) { break; }

                let wanted: HashSet<String> = l2_wanted_sub.iter()
                    .filter(|k| k.starts_with("bybit:"))
                    .map(|k| k[6..].to_string())
                    .collect();

                let to_add: Vec<_> = wanted.difference(&current_subs).cloned().collect();
                let to_remove: Vec<_> = current_subs.difference(&wanted).cloned().collect();

                if !to_add.is_empty() {
                    let mut w = writer_sub.lock().await;
                    for chunk in to_add.chunks(10) {
                        let args: Vec<String> = chunk.iter().map(|s| format!("orderbook.50.{}", s)).collect();
                        let _ = w.send(Message::Text(json!({"op":"subscribe","args":args}).to_string().into())).await;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    info!("[bybit_l2] Subscribed {} new L2 pairs (total {})", to_add.len(), wanted.len());
                }
                if !to_remove.is_empty() {
                    let mut w = writer_sub.lock().await;
                    for chunk in to_remove.chunks(10) {
                        let args: Vec<String> = chunk.iter().map(|s| format!("orderbook.50.{}", s)).collect();
                        let _ = w.send(Message::Text(json!({"op":"unsubscribe","args":args}).to_string().into())).await;
                    }
                    // Clean stale L2 entries for unsubscribed pairs
                    for sym in &to_remove {
                        l2_books_sub.remove(&format!("bybit:{}", sym));
                    }
                    info!("[bybit_l2] Unsubscribed {} stale L2 pairs", to_remove.len());
                }

                current_subs = wanted;

                // Heartbeat: refresh timestamps for subscribed L2 books
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                for sym in &current_subs {
                    let key = format!("bybit:{}", sym);
                    if let Some(mut book) = l2_books_sub.get_mut(&key) {
                        book.updated_ms = now_ms;
                    }
                }
            }
        });
    }

    // Reconnect after 30 min to refresh WS connection
    let conn_start = std::time::Instant::now();
    let max_conn_age = Duration::from_secs(1800);

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
    let mut read = read_half;
    loop {
        // Reconnect periodically to refresh WS connection
        if conn_start.elapsed() > max_conn_age {
            info!("[bybit] Refreshing WS connection after 30min...");
            break;
        }
        tokio::select! {
            msg = read.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(v) = serde_json::from_str::<BybitMsg>(&text) {
                        let topic = match v.topic {
                            Some(t) => t,
                            None => continue,
                        };
                        if let Some(d) = v.data {
                            if let Some(sym) = d.s {
                                if !sym.ends_with("USDT") { continue; }

                                // L2 depth (orderbook.50) → update l2_books cache
                                if topic.starts_with("orderbook.50.") {
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
                                        l2_books.insert(key, crate::state::L2Snapshot { bids: new_bids, asks: new_asks, updated_ms: now_ms, received_ms: now_ms });
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
                                            book.received_ms = now_ms;
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
                Some(Err(e)) => { l2_sub_alive.store(false, std::sync::atomic::Ordering::Relaxed); return Err(e.into()); }
                _ => {}
            },
            _ = ping.tick() => {
                let mut w = writer.lock().await;
                w.send(Message::Text(json!({"op":"ping"}).to_string().into())).await?;
            }
        }
    }
    l2_sub_alive.store(false, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}
