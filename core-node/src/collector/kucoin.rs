//! KuCoin Futures WebSocket collector
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
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
            !p.is_empty()
                && p.iter()
                    .all(|x| x.disabled_exchanges.contains(&"kucoin".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) {
                info!("[kucoin] Disabled globally in profiles. Sleeping...");
            }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        info!("[kucoin] Connecting...");

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"kucoin".to_string()))
                };
                if is_disabled {
                    break;
                }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal, &l2_books) => {
                match res {
                    Ok(_)  => { warn!("[kucoin] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => error!("[kucoin] Error: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            _ = disable_watcher => {
                info!("[kucoin] Exchange was disabled dynamically. Dropping connection.");
                backoff = Duration::from_secs(1);
            }
        }
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal, l2_books: &L2OrderbookMap) -> Result<()> {
    let http = Client::new();
    let contracts: Value = http
        .get("https://api-futures.kucoin.com/api/v1/contracts/active")
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .json()
        .await?;
    let mut symbols: Vec<String> = Vec::new();
    let mut sym_map = std::collections::HashMap::new();

    for c in contracts["data"].as_array().unwrap_or(&vec![]) {
        if let Some(s) = c["symbol"].as_str() {
            if s.ends_with("USDTM") {
                symbols.push(s.to_string());
                let normalized = s.replace("XBTUSDTM", "BTCUSDT").replace("USDTM", "USDT");
                sym_map.insert(s.to_string(), normalized.clone());

                let vol_usd = c["turnoverOf24h"].as_f64().unwrap_or(0.0);
                if vol_usd > 0.0 {
                    let mut entry = price_map.entry("kucoin".to_string()).or_default();
                    entry
                        .entry(normalized)
                        .or_insert_with(|| TickerData {
                            vol: vol_usd,
                            ..Default::default()
                        })
                        .vol = vol_usd;
                }
            }
        }
    }
    info!(
        "[kucoin] Found {} USDT-M contracts (volumes seeded)",
        symbols.len()
    );



    let resp: Value = http
        .post("https://api-futures.kucoin.com/api/v1/bullet-public")
        .send()
        .await?
        .json()
        .await?;
    let token = resp["data"]["token"]
        .as_str()
        .ok_or_else(|| anyhow!("No KuCoin WS token"))?;
    let endpoint = resp["data"]["instanceServers"][0]["endpoint"]
        .as_str()
        .unwrap_or("wss://ws-api-futures.kucoin.com");
    let (mut ws, _) = connect_async_tls_with_config(
        &format!("{}?token={}&connectId=corenode", endpoint, token),
        None,
        false,
        None,
    )
    .await
    .context("KuCoin WS connect failed")?;
    info!("[kucoin] Connected, subscribing...");
        // Filter symbols: only subscribe to L2 depth if the symbol exists on at least one OTHER exchange
        let mut needed_symbols = Vec::new();
        for sym in &symbols {
            let mut count = 0;
            for ex in price_map.iter() {
                if ex.key() != "kucoin" && ex.value().contains_key(sym.as_str()) {
                    count += 1;
                    break;
                }
            }
            if count > 0 {
                needed_symbols.push(sym.clone());
            }
        }
        let needed_symbols = needed_symbols;

    for chunk in symbols.chunks(100) {
        let topic = format!("/contractMarket/tickerV2:{}", chunk.join(","));
        ws.send(Message::Text(json!({"id":"sub","type":"subscribe","topic":topic,"privateChannel":false,"response":true}).to_string().into())).await?;
    }
    for chunk in needed_symbols.chunks(100) {
        let topic_l2 = format!("/contractMarket/level2Depth50:{}", chunk.join(","));
        ws.send(Message::Text(json!({"id":"subL2","type":"subscribe","topic":topic_l2,"privateChannel":false,"response":true}).to_string().into())).await?;
    }

    #[derive(Deserialize)]
    struct KucoinMsg<'a> {
        topic: Option<String>,
        #[serde(rename = "type")]
        msg_type: &'a str,
        #[serde(borrow)]
        data: Option<KucoinData<'a>>,
    }

    #[derive(Deserialize)]
    struct KucoinData<'a> {
        bids: Option<Vec<Vec<String>>>,
        asks: Option<Vec<Vec<String>>>,
        symbol: Option<&'a str>,
        #[serde(
            deserialize_with = "de_opt_f64_or_str",
            rename = "bestBidPrice",
            default
        )]
        bid: Option<f64>,
        #[serde(
            deserialize_with = "de_opt_f64_or_str",
            rename = "bestAskPrice",
            default
        )]
        ask: Option<f64>,
        #[serde(
            deserialize_with = "de_opt_f64_or_str",
            rename = "bestBidSize",
            default
        )]
        bid_qty: Option<f64>,
        #[serde(
            deserialize_with = "de_opt_f64_or_str",
            rename = "bestAskSize",
            default
        )]
        ask_qty: Option<f64>,
        #[serde(deserialize_with = "de_opt_f64_or_str", default)]
        price: Option<f64>,
    }

    fn de_opt_f64_or_str<'de, D>(deserializer: D) -> std::result::Result<Option<f64>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Any<'a> {
            Float(f64),
            Str(&'a str),
        }
        Ok(match Option::<Any>::deserialize(deserializer)? {
            Some(Any::Float(f)) => Some(f),
            Some(Any::Str(s)) => s.parse().ok(),
            None => None,
        })
    }

    // Ensure kucoin map exists for mutably getting later
    price_map.entry("kucoin".to_string()).or_default();

    let mut ping = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            msg = tokio::time::timeout(Duration::from_secs(30), ws.next()) => match msg.ok().flatten() {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(v) = serde_json::from_str::<KucoinMsg>(&text) {
                        if v.msg_type != "message" { continue; }
                        if let Some(d) = v.data {
                            if let Some(raw_sym) = d.symbol {
                                if let Some(sym) = sym_map.get(raw_sym) {
                                    if v.topic.as_deref().unwrap_or("").starts_with("/contractMarket/level2Depth50:") {
                                        let bids: Vec<(f64, f64)> = d.bids.as_ref().map(|arr| arr.iter().filter_map(|x| Some((x[0].parse().ok()?, x[1].parse().ok()?))).collect()).unwrap_or_default();
                                        let asks: Vec<(f64, f64)> = d.asks.as_ref().map(|arr| arr.iter().filter_map(|x| Some((x[0].parse().ok()?, x[1].parse().ok()?))).collect()).unwrap_or_default();
                                        if !bids.is_empty() || !asks.is_empty() {
                                            let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
                                            l2_books.insert(format!("kucoin:{}", sym), crate::state::L2Snapshot { bids, asks, updated_ms: now_ms });
                                        }
                                        continue;
                                    }
                                    let bid: f64 = match d.bid { Some(v) if v > 0.0 => v, _ => continue };
                                    let ask: f64 = match d.ask { Some(v) if v > 0.0 => v, _ => continue };
                                    let bid_qty = d.bid_qty.unwrap_or(0.0);
                                    let ask_qty = d.ask_qty.unwrap_or(0.0);
                                    let price = d.price.unwrap_or((bid+ask)/2.0);

                                    if let Some(mut ex_map) = price_map.get_mut("kucoin") {
                                        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
                                        if let Some(entry) = ex_map.get_mut(sym) {
                                            entry.bid = bid;
                                            entry.ask = ask;
                                            entry.price = price;
                                            entry.bid_size = bid_qty * price;
                                            entry.ask_size = ask_qty * price;
                                            entry.last_updated = ts;
                                        } else {
                                            ex_map.insert(sym.to_string(), TickerData {
                                                bid, ask, price, vol: 0.0, funding_rate: 0.0,
                                                bid_size: bid_qty * price, ask_size: ask_qty * price,
                                                last_updated: ts
                                            });
                                        }
                                    }
                                    signal.notify_one();
                                }
                            }
                        }
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => { ws.send(Message::Text(json!({"id":"ping","type":"ping"}).to_string().into())).await?; }
        }
    }
    Ok(())
}
