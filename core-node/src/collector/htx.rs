//! HTX (Huobi) Linear Swap WebSocket collector
use std::{io::Read, time::Duration};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{PriceMap, TickerData, UpdateSignal};

pub async fn run(prices: PriceMap, signal: UpdateSignal) {
    let mut backoff = Duration::from_secs(1);
    loop {
        info!("[htx] Connecting...");
        match handler(&prices, &signal).await {
            Ok(_)  => { warn!("[htx] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
            Err(e) => error!("[htx] Error: {}. Reconnecting in {:?}", e, backoff),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

async fn handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://api.hbdm.com/linear-swap-api/v1/swap_contract_info")
        .send().await?.json().await?;
    let symbols: Vec<String> = res["data"].as_array().unwrap_or(&vec![])
        .iter().filter_map(|i| {
            let s = i["contract_code"].as_str()?;
            let status = i["contract_status"].as_i64().unwrap_or(0);
            if s.ends_with("-USDT") && status == 1 { Some(s.to_string()) } else { None }
        }).collect();
    info!("[htx] Found {} USDT perps", symbols.len());

    let (mut ws, _) = connect_async_tls_with_config(
        "wss://api.hbdm.com/linear-swap-ws", None, false, None,
    ).await.context("HTX WS connect failed")?;

    for sym in &symbols {
        ws.send(Message::Text(json!({"sub":format!("market.{}.bbo",sym),"id":format!("bbo_{}",sym)}).to_string().into())).await?;
        ws.send(Message::Text(json!({"sub":format!("market.{}.detail",sym),"id":format!("det_{}",sym)}).to_string().into())).await?;
    }

    loop {
        match ws.next().await {
            Some(Ok(Message::Binary(bin))) => {
                let mut dec = flate2::read::GzDecoder::new(bin.as_slice());
                let mut text = String::new();
                if dec.read_to_string(&mut text).is_err() { continue; }
                let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                if let Some(ts) = v["ping"].as_i64() {
                    ws.send(Message::Text(json!({"pong":ts}).to_string().into())).await?;
                    continue;
                }
                let ch = match v["ch"].as_str() { Some(c) => c, None => continue };
                let parts: Vec<&str> = ch.splitn(3, '.').collect();
                if parts.len() < 3 { continue; }
                let sym = parts[1].replace('-', "");
                if !sym.ends_with("USDT") { continue; }
                let tick = &v["tick"];
                if parts[2] == "bbo" {
                    let bid: f64 = match tick["bid"][0].as_f64() { Some(v) if v > 0.0 => v, _ => continue };
                    let ask: f64 = match tick["ask"][0].as_f64() { Some(v) if v > 0.0 => v, _ => continue };
                    let bid_qty: f64 = tick["bid"][1].as_f64().unwrap_or(0.0);
                    let ask_qty: f64 = tick["ask"][1].as_f64().unwrap_or(0.0);
                    let mid = (bid + ask) / 2.0;
                    let ev = price_map.get("htx").and_then(|m| m.get(&sym).map(|d| d.vol)).unwrap_or(0.0);
                    upsert(price_map, signal, sym, TickerData { bid, ask, price: mid, vol: ev, funding_rate: 0.0, bid_size: bid_qty * mid, ask_size: ask_qty * mid, last_updated: 0 });
                } else if parts[2] == "detail" {
                    let close = tick["close"].as_f64().unwrap_or(0.0);
                    if close <= 0.0 { continue; }
                    let vol = tick["vol"].as_f64().unwrap_or(0.0) * close;
                    let existing = price_map.get("htx").and_then(|m| m.get(&sym).cloned());
                    if let Some(d) = existing { if d.bid > 0.0 && d.ask > 0.0 {
                        upsert(price_map, signal, sym, TickerData { bid: d.bid, ask: d.ask, price: d.price, vol, funding_rate: 0.0, bid_size: d.bid_size, ask_size: d.ask_size, last_updated: 0 });
                    }}
                }
            }
            Some(Ok(Message::Close(_))) | None => break,
            Some(Err(e)) => return Err(e.into()),
            _ => {}
        }
    }
    Ok(())
}

fn upsert(price_map: &PriceMap, signal: &UpdateSignal, sym: String, mut data: TickerData) {
    data.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    price_map.entry("htx".to_string()).or_default().insert(sym, data);
    signal.notify_one();
}
