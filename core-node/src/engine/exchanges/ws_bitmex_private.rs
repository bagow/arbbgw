use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::OnceLock;
use std::time::Duration;
use dashmap::DashMap;

use crate::engine::real::now_ms;

// Global cache for BitMEX order execution results: OrderId -> (AvgPrice, Fee, FilledSize, Status)
pub static BITMEX_ORDER_CACHE: OnceLock<Arc<DashMap<String, BitmexOrderState>>> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct BitmexOrderState {
    pub cum_qty: f64,
    pub avg_px: f64,
    pub fee: f64,
    pub status: String,
}

pub fn get_order_cache() -> Arc<DashMap<String, BitmexOrderState>> {
    BITMEX_ORDER_CACHE.get_or_init(|| Arc::new(DashMap::new())).clone()
}

static WS_RUNNING: OnceLock<tokio::sync::Mutex<bool>> = OnceLock::new();

pub async fn ensure_private_ws(api_key: &str, api_secret: &str) {
    let mutex = WS_RUNNING.get_or_init(|| tokio::sync::Mutex::new(false));
    let mut is_running = mutex.lock().await;
    if *is_running {
        return;
    }
    
    let key = api_key.to_string();
    let secret = api_secret.to_string();
    
    tokio::spawn(async move {
        bitmex_private_ws_loop(key, secret).await;
    });
    
    *is_running = true;
}

fn bitmex_ws_sign(secret: &str, expires: i64) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let msg = format!("GET/realtime{}", expires);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(msg.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

async fn bitmex_private_ws_loop(api_key: String, api_secret: String) {
    let cache = get_order_cache();
    
    loop {
        // Auth via URL query is recommended for BitMEX
        let expires = (now_ms() / 1000) + 60; // 60 seconds from now
        let sign = bitmex_ws_sign(&api_secret, expires);
        let url = format!("wss://ws.bitmex.com/realtime?api-expires={}&api-signature={}&api-key={}", expires, sign, api_key);

        let (ws_stream, _) = match connect_async(&url).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("[bitmex_ws_private] connection failed: {}. Retrying in 2s...", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        tracing::info!("[bitmex_ws_private] Connected to BitMEX Private WS!");
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to `execution` channel
        let sub_msg = serde_json::json!({
            "op": "subscribe",
            "args": ["execution"]
        });

        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
            tracing::error!("[bitmex_ws_private] Subscribe send failed: {}", e);
            continue;
        }

        let mut read_failed = false;

        loop {
            tokio::select! {
                Some(msg_res) = read.next() => {
                    match msg_res {
                        Ok(Message::Text(t)) => {
                            if let Ok(val) = serde_json::from_str::<Value>(&t) {
                                if val["table"].as_str() == Some("execution") {
                                    if let Some(data) = val.get("data").and_then(|d| d.as_array()) {
                                        for exec in data {
                                            let order_id = exec["orderID"].as_str().unwrap_or("");
                                            let ord_status = exec["ordStatus"].as_str().unwrap_or("");
                                            let cum_qty = exec["cumQty"].as_f64().unwrap_or(0.0);
                                            let avg_px = exec["avgPx"].as_f64().unwrap_or(0.0);
                                            let commission = exec["commission"].as_f64().unwrap_or(0.0);
                                            
                                            // BitMEX execution messages are differential, but `cumQty` and `avgPx` are usually absolute.
                                            // Fee is calculated as commission * avg_price * filled (as implemented in fetch_order).
                                            let fee = commission.abs() * avg_px * cum_qty;
                                            
                                            if !order_id.is_empty() {
                                                let mut state = cache.entry(order_id.to_string()).or_insert(BitmexOrderState {
                                                    cum_qty: 0.0,
                                                    avg_px: 0.0,
                                                    fee: 0.0,
                                                    status: "New".to_string(),
                                                });
                                                
                                                // If these fields are present in the update
                                                if exec.get("ordStatus").is_some() { state.status = ord_status.to_string(); }
                                                if exec.get("cumQty").is_some() { state.cum_qty = cum_qty; }
                                                if exec.get("avgPx").is_some() { state.avg_px = avg_px; }
                                                if exec.get("commission").is_some() { state.fee = fee; }
                                                
                                                if state.status == "Filled" || state.status == "PartiallyFilled" || state.status == "Canceled" {
                                                    tracing::debug!("[bitmex_ws_private] Order {} update: status={}, cumQty={}, avgPx={}, fee={}", 
                                                        order_id, state.status, state.cum_qty, state.avg_px, state.fee);
                                                }
                                            }
                                        }
                                    }
                                } else if val["info"].as_str() == Some("Welcome to the BitMEX Realtime API.") {
                                    tracing::info!("[bitmex_ws_private] Authenticated and ready");
                                }
                            }
                        }
                        Ok(Message::Ping(_)) => {} // auto-handled
                        Err(e) => {
                            tracing::error!("[bitmex_ws_private] Read error: {}", e);
                            read_failed = true;
                            break;
                        }
                        _ => {}
                    }
                }
                else => {
                    read_failed = true;
                    break;
                }
            }
        }

        if read_failed {
            tracing::warn!("[bitmex_ws_private] Connection dropped. Reconnecting in 2s...");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}
