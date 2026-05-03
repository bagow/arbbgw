use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::OnceLock;
use std::time::Duration;
use dashmap::DashMap;

use crate::engine::real::now_ms;

// Global cache for Bitget order execution results: OrderId -> (PriceAvg, Fee, FilledSize, Status)
pub static BITGET_ORDER_CACHE: OnceLock<Arc<DashMap<String, BitgetOrderState>>> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct BitgetOrderState {
    pub base_volume: f64,
    pub price_avg: f64,
    pub fee: f64,
    pub status: String,
}

pub fn get_order_cache() -> Arc<DashMap<String, BitgetOrderState>> {
    BITGET_ORDER_CACHE.get_or_init(|| Arc::new(DashMap::new())).clone()
}

static WS_RUNNING: OnceLock<tokio::sync::Mutex<bool>> = OnceLock::new();

pub async fn ensure_private_ws(api_key: &str, api_secret: &str, passphrase: &str) {
    let mutex = WS_RUNNING.get_or_init(|| tokio::sync::Mutex::new(false));
    let mut is_running = mutex.lock().await;
    if *is_running {
        return;
    }
    
    let key = api_key.to_string();
    let secret = api_secret.to_string();
    let pass = passphrase.to_string();
    
    tokio::spawn(async move {
        bitget_private_ws_loop(key, secret, pass).await;
    });
    
    *is_running = true;
}

fn bitget_ws_sign(secret: &str, timestamp: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let msg = format!("{}GET/user/verify", timestamp);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(msg.as_bytes());
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

async fn bitget_private_ws_loop(api_key: String, api_secret: String, passphrase: String) {
    let cache = get_order_cache();
    
    loop {
        let url = "wss://ws.bitget.com/v2/ws/private";

        let (ws_stream, _) = match connect_async(url).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("[bitget_ws_private] connection failed: {}. Retrying in 2s...", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        tracing::info!("[bitget_ws_private] Connected to Bitget Private WS!");
        let (mut write, mut read) = ws_stream.split();

        // 1. Authenticate
        let ts = (now_ms()).to_string();
        let sign = bitget_ws_sign(&api_secret, &ts);
        
        let login_msg = serde_json::json!({
            "op": "login",
            "args": [{
                "apiKey": api_key,
                "passphrase": passphrase,
                "timestamp": ts,
                "sign": sign
            }]
        });

        if let Err(e) = write.send(Message::Text(login_msg.to_string())).await {
            tracing::error!("[bitget_ws_private] Login send failed: {}", e);
            continue;
        }

        // Wait for login response
        let mut authed = false;
        let auth_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while let Ok(Some(msg)) = tokio::time::timeout_at(auth_deadline, read.next()).await {
            if let Ok(Message::Text(t)) = msg {
                if let Ok(val) = serde_json::from_str::<Value>(&t) {
                    if val["event"].as_str() == Some("login") && val["code"].as_i64() == Some(0) {
                        tracing::info!("[bitget_ws_private] Authenticated successfully");
                        authed = true;
                        break;
                    } else if val["event"].as_str() == Some("error") {
                        tracing::error!("[bitget_ws_private] Auth error: {}", val);
                        break;
                    }
                }
            }
        }

        if !authed {
            tracing::error!("[bitget_ws_private] Auth failed or timed out. Reconnecting in 3s...");
            tokio::time::sleep(Duration::from_secs(3)).await;
            continue;
        }

        // 2. Subscribe to `orders` channel
        let sub_msg = serde_json::json!({
            "op": "subscribe",
            "args": [{
                "instType": "USDT-FUTURES",
                "channel": "orders",
                "instId": "default"
            }]
        });

        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
            tracing::error!("[bitget_ws_private] Subscribe send failed: {}", e);
            continue;
        }

        // Ping task (Bitget requires 'ping' string every 30s)
        let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
        let ping_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(20)).await;
                if ping_tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        let mut read_failed = false;

        loop {
            tokio::select! {
                Some(msg_res) = read.next() => {
                    match msg_res {
                        Ok(Message::Text(t)) => {
                            if t == "pong" {
                                continue;
                            }
                            if let Ok(val) = serde_json::from_str::<Value>(&t) {
                                if val["action"].as_str() == Some("push") {
                                    if let Some(arg) = val.get("arg") {
                                        if arg["channel"].as_str() == Some("orders") {
                                            if let Some(data) = val.get("data").and_then(|d| d.as_array()) {
                                                for order in data {
                                                    let order_id = order["orderId"].as_str().unwrap_or("");
                                                    let status = order["state"].as_str().unwrap_or("");
                                                    let filled: f64 = order["baseVolume"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                                    let price_avg: f64 = order["priceAvg"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                                    let fee: f64 = order["fee"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0).abs();
                                                    
                                                    if !order_id.is_empty() {
                                                        let mut state = cache.entry(order_id.to_string()).or_insert(BitgetOrderState {
                                                            base_volume: 0.0,
                                                            price_avg: 0.0,
                                                            fee: 0.0,
                                                            status: "new".to_string(),
                                                        });
                                                        
                                                        state.status = status.to_string();
                                                        state.base_volume = filled;
                                                        state.price_avg = price_avg;
                                                        state.fee = fee;
                                                        
                                                        if status == "filled" || status == "partially_filled" || status == "canceled" {
                                                            tracing::debug!("[bitget_ws_private] Order {} update: status={}, filled={}, avg={}, fee={}", 
                                                                order_id, status, filled, price_avg, fee);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(_)) => {} // auto-handled
                        Err(e) => {
                            tracing::error!("[bitget_ws_private] Read error: {}", e);
                            read_failed = true;
                            break;
                        }
                        _ => {}
                    }
                }
                Some(()) = ping_rx.recv() => {
                    if let Err(e) = write.send(Message::Text("ping".to_string())).await {
                        tracing::error!("[bitget_ws_private] Ping failed: {}", e);
                        read_failed = true;
                        break;
                    }
                }
                else => {
                    read_failed = true;
                    break;
                }
            }
        }

        ping_handle.abort();
        if read_failed {
            tracing::warn!("[bitget_ws_private] Connection dropped. Reconnecting in 2s...");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}
