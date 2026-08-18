//! Bybit WebSocket Trading API (v5/trade)
//!
//! Persistent WS connection for order execution.
//! Auth: HMAC-SHA256 signature of "GET/realtime{expires}"
//! Orders: {"reqId":"..","header":{..},"op":"order.create","args":[{..}]}
//! Fallback: if WS fails, caller falls back to REST in bybit.rs
//!
//! EXECUTION REPORTS: After auth, subscribes to "execution" topic.
//! Fill data (price, fee, qty) is cached by orderId for instant retrieval
//! by `place_order` and `fetch_order`, eliminating the IOC fill detection gap.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::OnceLock;

type ResponseSender = oneshot::Sender<Value>;

static BYBIT_WS_TX: OnceLock<mpsc::Sender<(Value, ResponseSender)>> = OnceLock::new();

/// Cached execution fill data from Bybit's private execution stream.
/// Key: orderId, Value: (avg_price, total_fee, total_exec_qty)
static BYBIT_EXEC_CACHE: OnceLock<Arc<dashmap::DashMap<String, (f64, f64, f64)>>> = OnceLock::new();

fn get_exec_cache() -> Arc<dashmap::DashMap<String, (f64, f64, f64)>> {
    BYBIT_EXEC_CACHE.get_or_init(|| Arc::new(dashmap::DashMap::new())).clone()
}

/// Try to get cached execution fill data for an orderId.
/// Returns Some((price, fee, exec_qty)) if execution report was received.
pub fn get_cached_fill(order_id: &str) -> Option<(f64, f64, f64)> {
    get_exec_cache().get(order_id).map(|v| *v)
}


pub fn get_or_init_bybit_ws(api_key: String, api_secret: String) -> mpsc::Sender<(Value, ResponseSender)> {
    BYBIT_WS_TX.get_or_init(|| {
        let (tx, rx) = mpsc::channel(1000);
        // Trade stream for order creation
        tokio::spawn(bybit_ws_loop(rx, api_key.clone(), api_secret.clone()));
        // Private stream for execution reports
        tokio::spawn(bybit_private_ws_loop(api_key, api_secret));
        tx
    }).clone()
}

fn bybit_sign_ws(secret: &str, expires: i64) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let val = format!("GET/realtime{}", expires);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(val.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Process an execution report push from Bybit's private stream.
/// Aggregates partial fills into a single (weighted_avg_price, total_fee, total_qty) entry.
fn process_execution_report(data: &Value) {
    let cache = get_exec_cache();

    if let Some(arr) = data.as_array() {
        for exec in arr {
            let order_id = match exec["orderId"].as_str() {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => continue,
            };

            let exec_price: f64 = exec["execPrice"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let exec_qty: f64 = exec["execQty"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let exec_fee: f64 = exec["execFee"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
                .abs();

            if exec_qty <= 0.0 || exec_price <= 0.0 {
                continue;
            }

            // Aggregate partial fills: weighted average price, sum fees, sum qty
            cache
                .entry(order_id.clone())
                .and_modify(|(prev_price, prev_fee, prev_qty)| {
                    let total_qty = *prev_qty + exec_qty;
                    // Volume-weighted average price
                    *prev_price = (*prev_price * *prev_qty + exec_price * exec_qty) / total_qty;
                    *prev_fee += exec_fee;
                    *prev_qty = total_qty;
                })
                .or_insert((exec_price, exec_fee, exec_qty));

            tracing::debug!(
                "[bybit_ws] 📊 Execution report: orderId={} price={} qty={} fee={}",
                order_id, exec_price, exec_qty, exec_fee
            );
        }
    }
}

async fn bybit_private_ws_loop(api_key: String, api_secret: String) {
    loop {
        let url = "wss://stream.bybit.com/v5/private";

        let (ws_stream, _) = match tokio::time::timeout(tokio::time::Duration::from_secs(10), connect_async(url)).await {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => {
                tracing::error!("[bybit_ws_private] connection failed: {}. Retrying in 2s...", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue;
            },
            Err(_) => {
                tracing::error!("[bybit_ws_private] connect_async TIMEOUT after 10s. Retrying in 2s...");
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        tracing::info!("[bybit_ws_private] connected to v5/private WS");
        let (mut write, mut read) = ws_stream.split();

        // ── AUTHENTICATE ──
        let expires = crate::engine::real::now_ms() + 10_000;
        let sig = bybit_sign_ws(&api_secret, expires);
        let auth_msg = serde_json::json!({
            "op": "auth",
            "args": [&api_key, expires, &sig]
        });

        if let Err(e) = write.send(Message::Text(auth_msg.to_string())).await {
            tracing::error!("[bybit_ws_private] auth send failed: {}", e);
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            continue;
        }

        // Wait for auth response
        let mut authed = false;
        let auth_deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        while let Ok(Some(msg)) = tokio::time::timeout_at(auth_deadline, read.next()).await {
            if let Ok(Message::Text(t)) = msg {
                if let Ok(val) = serde_json::from_str::<Value>(&t) {
                    if val["op"].as_str() == Some("auth") {
                        if val["success"].as_bool() == Some(true) || val["retCode"].as_i64() == Some(0) {
                            tracing::info!("[bybit_ws_private] authenticated successfully");
                            authed = true;
                        } else {
                            tracing::error!("[bybit_ws_private] auth failed: {}", val);
                        }
                        break;
                    }
                }
            }
        }

        if !authed {
            tracing::warn!("[bybit_ws_private] Auth timeout or failed. Reconnecting in 3s...");
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            continue;
        }

        // ── SUBSCRIBE TO EXECUTION REPORTS ──
        let sub_msg = serde_json::json!({
            "reqId": uuid::Uuid::new_v4().to_string(),
            "op": "subscribe",
            "args": ["execution"]
        });
        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
            tracing::error!("[bybit_ws_private] execution subscribe failed: {}", e);
        } else {
            tracing::info!("[bybit_ws_private] subscribed to execution reports");
        }

        // ── PING TASK ──
        let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
        let ping_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(18)).await;
                if ping_tx.send(()).await.is_err() { break; }
            }
        });

        // ── READER TASK ──
        loop {
            tokio::select! {
                msg_opt = read.next() => {
                    match msg_opt {
                        Some(Ok(Message::Text(t))) => {
                            if let Ok(val) = serde_json::from_str::<Value>(&t) {
                                if val["topic"].as_str() == Some("execution") {
                                    if let Some(data) = val.get("data") {
                                        process_execution_report(data);
                                    }
                                } else if val["op"].as_str() == Some("subscribe") {
                                    let success = val["success"].as_bool().unwrap_or(false);
                                    tracing::debug!("[bybit_ws_private] subscribe response: success={}", success);
                                }
                            }
                        }
                        Some(Ok(Message::Ping(_))) => {}
                        Some(Err(e)) => {
                            tracing::error!("[bybit_ws_private] read error: {}", e);
                            break;
                        }
                        None => break,
                        _ => {}
                    }
                }
                Some(()) = ping_rx.recv() => {
                    let ping = serde_json::json!({"reqId": uuid::Uuid::new_v4().to_string(), "op": "ping"});
                    if let Err(e) = write.send(Message::Text(ping.to_string())).await {
                        tracing::error!("[bybit_ws_private] ping failed: {}", e);
                        break;
                    }
                }
            }
        }

        ping_handle.abort();
        tracing::warn!("[bybit_ws_private] connection dropped. Restarting...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

async fn bybit_ws_loop(
    mut rx: mpsc::Receiver<(Value, ResponseSender)>,
    api_key: String,
    api_secret: String,
) {
    loop {
        let url = "wss://stream.bybit.com/v5/trade";

        let (ws_stream, _) = match tokio::time::timeout(tokio::time::Duration::from_secs(10), connect_async(url)).await {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => {
                tracing::error!("[bybit_ws] connection failed: {}. Retrying in 2s...", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue;
            },
            Err(_) => {
                tracing::error!("[bybit_ws] connect_async TIMEOUT after 10s. Retrying in 2s...");
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        tracing::info!("[bybit_ws] connected to v5/trade WS");
        crate::engine::utils::record_ws_connected("bybit");
        let (mut write, mut read) = ws_stream.split();

        // ── AUTHENTICATE ──
        let expires = crate::engine::real::now_ms() + 10_000;
        let sig = bybit_sign_ws(&api_secret, expires);
        let auth_msg = serde_json::json!({
            "op": "auth",
            "args": [&api_key, expires, &sig]
        });

        if let Err(e) = write.send(Message::Text(auth_msg.to_string())).await {
            tracing::error!("[bybit_ws] auth send failed: {}", e);
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            continue;
        }

        // Wait for auth response
        let mut authed = false;
        let auth_deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        while let Ok(Some(msg)) = tokio::time::timeout_at(auth_deadline, read.next()).await {
            if let Ok(Message::Text(t)) = msg {
                if let Ok(val) = serde_json::from_str::<Value>(&t) {
                    if val["op"].as_str() == Some("auth") {
                        if val["retCode"].as_i64() == Some(0) {
                            tracing::info!("[bybit_ws] authenticated successfully");
                            authed = true;
                        } else {
                            tracing::error!("[bybit_ws] auth failed: {}", val);
                        }
                        break;
                    }
                }
            }
        }

        if !authed {
            tracing::warn!("[bybit_ws] Auth timeout or failed. Reconnecting in 3s...");
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            continue;
        }

        // ── READER TASK ──
        let pending: Arc<Mutex<HashMap<String, ResponseSender>>> = Arc::new(Mutex::new(HashMap::new()));
        let pending_clone = pending.clone();

        let reader_handle = tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(t)) => {
                        if let Ok(val) = serde_json::from_str::<Value>(&t) {
                            // Bybit uses "reqId" for correlation of request-response
                            if let Some(id) = val["reqId"].as_str() {
                                if let Some(sender) = pending_clone.lock().await.remove(id) {
                                    let _ = sender.send(val);
                                }
                            }
                        }
                    }
                    Ok(Message::Ping(_)) => {}
                    Err(e) => {
                        tracing::error!("[bybit_ws] read error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });

        // ── PING TASK ── (Bybit requires ping every 20s)
        let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
        let ping_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(18)).await;
                if ping_tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        // ── MESSAGE LOOP ──
        loop {
            tokio::select! {
                Some((req_json, sender)) = rx.recv() => {
                    let id = req_json["reqId"].as_str().unwrap_or("unknown").to_string();
                    pending.lock().await.insert(id, sender);

                    let msg = Message::Text(req_json.to_string());
                    if let Err(e) = write.send(msg).await {
                        tracing::error!("[bybit_ws] write error: {}", e);
                        break;
                    }
                }
                Some(()) = ping_rx.recv() => {
                    let ping = serde_json::json!({"op": "ping"});
                    if let Err(e) = write.send(Message::Text(ping.to_string())).await {
                        tracing::error!("[bybit_ws] ping failed: {}", e);
                        break;
                    }
                }
                else => break,
            }
        }

        reader_handle.abort();
        ping_handle.abort();
        tracing::warn!("[bybit_ws] connection dropped. Restarting loop...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

pub async fn send_bybit_ws_request(req: Value) -> Result<Value, String> {
    let tx = BYBIT_WS_TX.get().ok_or("Bybit WS trader not initialized")?;
    let (resp_tx, resp_rx) = oneshot::channel();

    tx.send((req, resp_tx)).await.map_err(|_| "Bybit WS channel closed")?;

    match tokio::time::timeout(tokio::time::Duration::from_secs(5), resp_rx).await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(_)) => Err("Response sender dropped".to_string()),
        Err(_) => Err("Timeout waiting for Bybit WS response".to_string()),
    }
}
