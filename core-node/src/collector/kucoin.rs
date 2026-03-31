//! KuCoin Futures WebSocket collector
use std::{sync::Arc, time::Duration};
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{PriceMap, TickerData, UpdateSignal};

pub async fn run(prices: PriceMap, signal: UpdateSignal) {
    let mut backoff = Duration::from_secs(1);
    loop {
        info!("[kucoin] Connecting...");
        match handler(&prices, &signal).await {
            Ok(_)  => warn!("[kucoin] WS closed, reconnecting..."),
            Err(e) => error!("[kucoin] Error: {}. Reconnecting in {:?}", e, backoff),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    let contracts: Value = http.get("https://api-futures.kucoin.com/api/v1/contracts/active")
        .timeout(Duration::from_secs(10)).send().await?.json().await?;
    let symbols: Vec<String> = contracts["data"].as_array().unwrap_or(&vec![])
        .iter().filter_map(|c| { let s = c["symbol"].as_str()?; if s.ends_with("USDTM") { Some(s.to_string()) } else { None } }).collect();
    info!("[kucoin] Found {} USDT-M contracts", symbols.len());

    // Seed volume from REST (WS tickerV2 doesn't carry volume)
    for sym in &symbols {
        let normalized = sym.replace("XBTUSDTM", "BTCUSDT").replace("USDTM", "USDT");
        if let Ok(resp) = http.get(format!("https://api-futures.kucoin.com/api/v1/ticker?symbol={}", sym))
            .timeout(Duration::from_secs(5)).send().await {
            if let Ok(data) = resp.json::<Value>().await {
                let vol_usd = data["data"]["turnoverOf24h"].as_f64().unwrap_or(0.0);
                if vol_usd > 0.0 {
                    let mut entry = price_map.entry("kucoin".to_string()).or_default();
                    entry.entry(normalized).or_insert_with(|| TickerData { vol: vol_usd, ..Default::default() }).vol = vol_usd;
                }
            }
        }
    }

    let resp: Value = http.post("https://api-futures.kucoin.com/api/v1/bullet-public").send().await?.json().await?;
    let token    = resp["data"]["token"].as_str().ok_or_else(|| anyhow!("No KuCoin WS token"))?;
    let endpoint = resp["data"]["instanceServers"][0]["endpoint"].as_str().unwrap_or("wss://ws-api-futures.kucoin.com");
    let (mut ws, _) = connect_async_tls_with_config(
        &format!("{}?token={}&connectId=corenode", endpoint, token), None, false, None,
    ).await.context("KuCoin WS connect failed")?;
    info!("[kucoin] Connected, subscribing...");

    for chunk in symbols.chunks(100) {
        let topic = format!("/contractMarket/tickerV2:{}", chunk.join(","));
        ws.send(Message::Text(json!({"id":"sub","type":"subscribe","topic":topic,"privateChannel":false,"response":true}).to_string().into())).await?;
    }

    let mut ping = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    if v["type"].as_str() != Some("message") { continue; }
                    let raw = v["topic"].as_str().unwrap_or("").replace("/contractMarket/tickerV2:", "");
                    let sym = raw.replace("XBTUSDTM", "BTCUSDT").replace("USDTM", "USDT");
                    if sym.is_empty() || !sym.ends_with("USDT") { continue; }
                    let d = &v["data"];
                    let bid: f64 = match d["bestBidPrice"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                    let ask: f64 = match d["bestAskPrice"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                    let price = d["price"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
                    let ev = price_map.get("kucoin").and_then(|m| m.get(&sym).map(|d| d.vol)).unwrap_or(0.0);
                    upsert(price_map, signal, sym, TickerData { bid, ask, price, vol: ev, funding_rate: 0.0, last_updated: 0 });
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

fn upsert(price_map: &PriceMap, signal: &UpdateSignal, sym: String, mut data: TickerData) {
    data.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    price_map.entry("kucoin".to_string()).or_default().insert(sym, data);
    signal.notify_one();
}
