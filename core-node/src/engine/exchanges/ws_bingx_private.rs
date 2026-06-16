use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::OnceLock;
use std::time::Duration;
use dashmap::DashMap;

use crate::engine::real::now_ms;

// Global cache for BingX order execution results: OrderId -> (AvgPrice, Fee, FilledSize, Status)
pub static BINGX_ORDER_CACHE: OnceLock<Arc<DashMap<String, BingxOrderState>>> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct BingxOrderState {
    pub filled: f64,
    pub avg_price: f64,
    pub fee: f64,
    pub status: String,
}

pub fn get_order_cache() -> Arc<DashMap<String, BingxOrderState>> {
    BINGX_ORDER_CACHE.get_or_init(|| Arc::new(DashMap::new())).clone()
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
        bingx_private_ws_loop(key, secret).await;
    });
    
    *is_running = true;
}

fn bingx_ws_sign(secret: &str, params: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(params.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

async fn get_listen_key(api_key: &str, api_secret: &str) -> Result<String, String> {
    let client = reqwest::Client::new();
    let ts = now_ms().to_string();
    // BingX listenKey endpoint
    let _params = "";
    let sign_str = format!("timestamp={}", ts);
    let sig = bingx_ws_sign(api_secret, &sign_str);
    
    let url = format!("https://open-api.bingx.com/openApi/user/auth/userDataStream?timestamp={}&signature={}", ts, sig);
    
    let resp = client.post(&url)
        .header("X-BX-APIKEY", api_key)
        .send()
        .await
        .map_err(|e| format!("BingX HTTP error: {}", e))?;
        
    let v: Value = resp.json().await.map_err(|e| format!("BingX JSON parse: {}", e))?;
    if v["code"].as_i64() != Some(0) {
        return Err(format!("ListenKey error: {}", v));
    }
    
    let listen_key = v["listenKey"].as_str()
        .or_else(|| v["data"]["listenKey"].as_str())
        .unwrap_or("")
        .to_string();
        
    if listen_key.is_empty() {
        return Err("ListenKey is empty".to_string());
    }
    
    Ok(listen_key)
}

async fn bingx_private_ws_loop(api_key: String, api_secret: String) {
    let cache = get_order_cache();
    
    loop {
        tracing::info!("[bingx_ws_private] Fetching listenKey...");
        let listen_key = match get_listen_key(&api_key, &api_secret).await {
            Ok(k) => k,
            Err(e) => {
                tracing::error!("[bingx_ws_private] Failed to get listenKey: {}. Retrying in 5s...", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let url = format!("wss://open-api-swap.bingx.com/swap-market?listenKey={}", listen_key);

        let (ws_stream, _) = match connect_async(&url).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("[bingx_ws_private] connection failed: {}. Retrying in 2s...", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        tracing::info!("[bingx_ws_private] Connected to BingX Private WS!");
        let (mut write, mut read) = ws_stream.split();

        // Ping task (BingX uses standard Ping/Pong)
        let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
        let ping_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                if ping_tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        let mut _read_failed = false;

        loop {
            tokio::select! {
                Some(msg_res) = read.next() => {
                    match msg_res {
                        Ok(Message::Text(t)) => {
                            if t == "Ping" || t.contains("Ping") {
                                let _ = write.send(Message::Text(t.replace("Ping", "Pong"))).await;
                                continue;
                            }
                            
                            if let Ok(val) = serde_json::from_str::<Value>(&t) {
                                // BingX User Data Stream order format (mirrors Binance)
                                if val["e"].as_str() == Some("ORDER_TRADE_UPDATE") || val["e"].as_str() == Some("executionReport") {
                                    if let Some(order) = val.get("o") {
                                        let order_id = order["i"].as_str().map(|s| s.to_string()).or_else(|| order["i"].as_i64().map(|id| id.to_string())).unwrap_or_default();
                                        let status = order["X"].as_str().unwrap_or("");
                                        let filled: f64 = order["z"].as_f64().or_else(|| order["z"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
                                        let avg_price: f64 = order["ap"].as_f64().or_else(|| order["ap"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
                                        let fee: f64 = order["n"].as_f64().or_else(|| order["n"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0).abs();
                                        
                                        if !order_id.is_empty() {
                                            let mut state = cache.entry(order_id.clone()).or_insert(BingxOrderState {
                                                filled: 0.0,
                                                avg_price: 0.0,
                                                fee: 0.0,
                                                status: "NEW".to_string(),
                                            });
                                            
                                            state.status = status.to_string();
                                            state.filled = filled;
                                            state.avg_price = avg_price;
                                            state.fee = fee;
                                            
                                            if status == "FILLED" || status == "PARTIALLY_FILLED" || status == "CANCELED" {
                                                tracing::debug!("[bingx_ws_private] Order {} update: status={}, filled={}, avg={}, fee={}", 
                                                    order_id, status, filled, avg_price, fee);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(_)) => {} // auto-handled
                        Err(e) => {
                            tracing::error!("[bingx_ws_private] Read error: {}", e);
                            _read_failed = true;
                            break;
                        }
                        _ => {}
                    }
                }
                Some(()) = ping_rx.recv() => {
                    // Send Keep-Alive to REST endpoint for listenKey
                    let ts = now_ms().to_string();
                    let sign_str = format!("timestamp={}", ts);
                    let sig = bingx_ws_sign(&api_secret, &sign_str);
                    let url = format!("https://open-api.bingx.com/openApi/user/auth/userDataStream?timestamp={}&signature={}", ts, sig);
                    let _ = reqwest::Client::new().put(&url)
                        .header("X-BX-APIKEY", &api_key)
                        .send().await;
                        
                    // Ping WS
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        tracing::error!("[bingx_ws_private] Ping failed: {}", e);
                        _read_failed = true;
                        break;
                    }
                }
                else => {
                    _read_failed = true;
                    break;
                }
            }
        }

        ping_handle.abort();
        if _read_failed {
            tracing::warn!("[bingx_ws_private] Connection dropped. Reconnecting in 2s...");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}
