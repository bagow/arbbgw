//! Bybit USDT perpetual futures WebSocket collector
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{PriceMap, TickerData, UpdateSignal};

pub async fn run(
    prices: PriceMap,
    signal: UpdateSignal,
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
            res = handler(&prices, &signal) => {
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

async fn handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    // Fetch all linear USDT perpetual tickers (includes volume)
    let resp: Value = http
        .get("https://api.bybit.com/v5/market/tickers?category=linear")
        .timeout(Duration::from_secs(15))
        .send()
        .await?
        .json()
        .await?;

    let mut symbols = Vec::new();
    if let Some(list) = resp["result"]["list"].as_array() {
        for t in list {
            let sym = t["symbol"].as_str().unwrap_or("");
            if !sym.ends_with("USDT") {
                continue;
            }
            let vol: f64 = t["turnover24h"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let bid: f64 = t["bid1Price"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let ask: f64 = t["ask1Price"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let bid_qty: f64 = t["bid1Size"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let ask_qty: f64 = t["ask1Size"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let price: f64 = t["lastPrice"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or((bid + ask) / 2.0);
            if bid > 0.0 && ask > 0.0 {
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                price_map.entry("bybit".to_string()).or_default().insert(
                    sym.to_string(),
                    TickerData {
                        bid,
                        ask,
                        price,
                        vol,
                        funding_rate: 0.0,
                        bid_size: bid_qty * price,
                        ask_size: ask_qty * price,
                        last_updated: ts,
                    },
                );
                signal.notify_one();
            }
            symbols.push(sym.to_string());
        }
    }
    if symbols.is_empty() {
        return Err(anyhow::anyhow!("Failed to fetch symbols or list is empty"));
    }
    info!("[bybit] Seeded {} USDT linear tickers", symbols.len());

    // WS: subscribe to tickers in batches of 10
    let (mut ws, _) =
        connect_async_tls_with_config("wss://stream.bybit.com/v5/public/linear", None, false, None)
            .await
            .context("Bybit WS connect failed")?;
    info!("[bybit] Connected, subscribing...");

    for chunk in symbols.chunks(10) {
        let args: Vec<String> = chunk.iter().map(|s| format!("orderbook.1.{}", s)).collect();
        ws.send(Message::Text(
            json!({"op":"subscribe","args":args}).to_string().into(),
        ))
        .await?;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    let mut _ping = tokio::time::interval(Duration::from_secs(20));
    #[derive(Deserialize)]
    struct BybitMsg<'a> {
        #[serde(borrow)]
        topic: Option<&'a str>,
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
                        if v.topic.map(|t| !t.starts_with("orderbook.1.")).unwrap_or(true) { continue; }
                        if let Some(d) = v.data {
                            if let Some(sym) = d.s {
                                if !sym.ends_with("USDT") { continue; }

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
                                        // new entry creation logic
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
