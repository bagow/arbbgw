//! Bybit WebSocket Trading API (v5/trade)
//!
//! Persistent WS connection for order execution.
//! Auth: HMAC-SHA256 signature of "GET/realtime{expires}"
//! Orders: {"reqId":"..","header":{..},"op":"order.create","args":[{..}]}
//! Fallback: if WS fails, caller falls back to REST in bybit.rs

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::OnceLock;

type ResponseSender = oneshot::Sender<Value>;

static BYBIT_WS_TX: OnceLock<mpsc::Sender<(Value, ResponseSender)>> = OnceLock::new();

/// Initialize or get the Bybit WS trading channel.
/// `api_key` and `api_secret` are needed for auth on first connection.
pub fn get_or_init_bybit_ws(api_key: String, api_secret: String) -> mpsc::Sender<(Value, ResponseSender)> {
    BYBIT_WS_TX.get_or_init(|| {
        let (tx, rx) = mpsc::channel(1000);
        tokio::spawn(bybit_ws_loop(rx, api_key, api_secret));
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

async fn bybit_ws_loop(
    mut rx: mpsc::Receiver<(Value, ResponseSender)>,
    api_key: String,
    api_secret: String,
) {
    loop {
        let url = "wss://stream.bybit.com/v5/trade";

        let (ws_stream, _) = match connect_async(url).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("[bybit_ws] connection failed: {}. Retrying in 2s...", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        tracing::info!("[bybit_ws] connected to v5/trade WS");
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
            tracing::error!("[bybit_ws] auth timeout or failed. Reconnecting in 3s...");
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            continue;
        }

        // ── READER TASK ──
        let pending: Arc<Mutex<HashMap<String, ResponseSender>>> = Arc::new(Mutex::new(HashMap::new()));
        let pending_clone = pending.clone();

        let mut reader_handle = tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(t)) => {
                        if let Ok(val) = serde_json::from_str::<Value>(&t) {
                            // Bybit uses "reqId" for correlation
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
