//! Binance USDT Futures WebSocket collector — !bookTicker stream (all pairs)
use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
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
                    .all(|x| x.disabled_exchanges.contains(&"binance".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) {
                info!("[binance] Disabled globally in profiles. Sleeping...");
            }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        info!("[binance] Connecting...");

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"binance".to_string()))
                };
                if is_disabled {
                    break;
                }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal) => {
                match res {
                    Ok(_)  => { warn!("[binance] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => error!("[binance] Error: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            _ = disable_watcher => {
                info!("[binance] Exchange was disabled dynamically. Dropping connection.");
                backoff = Duration::from_secs(1);
            }
        }
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    // Seed 24h volume from REST miniTicker snapshot
    let http = Client::new();
    if let Ok(resp) = http
        .get("https://fapi.binance.com/fapi/v1/ticker/24hr")
        .timeout(Duration::from_secs(15))
        .send()
        .await
    {
        if let Ok(arr) = resp.json::<Vec<Value>>().await {
            for t in &arr {
                let sym = t["symbol"].as_str().unwrap_or("");
                if !sym.ends_with("USDT") {
                    continue;
                }
                let vol: f64 = t["quoteVolume"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                if vol > 0.0 {
                    let mut entry = price_map.entry("binance".to_string()).or_default();
                    entry
                        .entry(sym.to_string())
                        .or_insert_with(|| TickerData {
                            vol,
                            ..Default::default()
                        })
                        .vol = vol;
                }
            }
            info!("[binance] Seeded volume for {} symbols", arr.len());
        }
    }

    // WS: !bookTicker — instant bid/ask for every USDT future
    let (mut ws, _) = connect_async_tls_with_config(
        "wss://fstream.binance.com/ws/!bookTicker",
        None,
        false,
        None,
    )
    .await
    .context("Binance WS connect failed")?;
    info!("[binance] Connected");

    #[derive(Deserialize)]
    struct BinanceTicker<'a> {
        #[serde(rename = "s")]
        symbol: &'a str,
        #[serde(rename = "b")]
        bid: &'a str,
        #[serde(rename = "a")]
        ask: &'a str,
        #[serde(rename = "B")]
        bid_qty: &'a str,
        #[serde(rename = "A")]
        ask_qty: &'a str,
    }

    // Ensure binance map exists for getting mutably later
    price_map.entry("binance".to_string()).or_default();

    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(30), ws.next()).await {
        if let Ok(Message::Text(text)) = msg {
            if let Ok(t) = serde_json::from_str::<BinanceTicker>(&text) {
                if !t.symbol.ends_with("USDT") {
                    continue;
                }
                let bid: f64 = match t.bid.parse() {
                    Ok(v) if v > 0.0 => v,
                    _ => continue,
                };
                let ask: f64 = match t.ask.parse() {
                    Ok(v) if v > 0.0 => v,
                    _ => continue,
                };
                let bid_qty: f64 = t.bid_qty.parse().unwrap_or(0.0);
                let ask_qty: f64 = t.ask_qty.parse().unwrap_or(0.0);
                let mid = (bid + ask) / 2.0;

                if let Some(mut ex_map) = price_map.get_mut("binance") {
                    let ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    if let Some(entry) = ex_map.get_mut(t.symbol) {
                        entry.bid = bid;
                        entry.ask = ask;
                        entry.price = mid;
                        entry.bid_size = bid_qty * mid;
                        entry.ask_size = ask_qty * mid;
                        entry.last_updated = ts;
                    } else {
                        ex_map.insert(
                            t.symbol.to_string(),
                            TickerData {
                                bid,
                                ask,
                                price: mid,
                                vol: 0.0,
                                funding_rate: 0.0,
                                bid_size: bid_qty * mid,
                                ask_size: ask_qty * mid,
                                last_updated: ts,
                            },
                        );
                    }
                }
                signal.notify_one();
            }
        }
    }
    Ok(())
}
