//! Gate.io Futures WebSocket Trading API (v4)
//!
//! Persistent WS connection for order execution.
//! URL: wss://fx-ws.gateio.ws/v4/ws/usdt
//! Auth: HMAC-SHA512 of "channel=<ch>&event=<ev>&time=<ts>"
//! Orders: {"time":..,"channel":"futures.order_place","event":"api","auth":{..},"payload":[{..}]}
//! Response correlation via "request_id" field

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::OnceLock;

type ResponseSender = oneshot::Sender<Value>;

static GATE_WS_TX: OnceLock<mpsc::Sender<(Value, ResponseSender)>> = OnceLock::new();

fn gate_ws_sign(secret: &str, channel: &str, event: &str, ts: i64) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha512;
    type HmacSha512 = Hmac<Sha512>;

    let msg = format!("channel={}&event={}&time={}", channel, event, ts);
    let mut mac = HmacSha512::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(msg.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn get_or_init_gate_ws(api_key: String, api_secret: String) -> mpsc::Sender<(Value, ResponseSender)> {
    GATE_WS_TX.get_or_init(|| {
        let (tx, rx) = mpsc::channel(1000);
        tokio::spawn(gate_ws_loop(rx, api_key, api_secret));
        tx
    }).clone()
}

async fn gate_ws_loop(
    mut rx: mpsc::Receiver<(Value, ResponseSender)>,
    api_key: String,
    api_secret: String,
) {
    loop {
        let url = "wss://fx-ws.gateio.ws/v4/ws/usdt";

        let (ws_stream, _) = match connect_async(url).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("[gate_ws] connection failed: {}. Retrying in 2s...", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        tracing::info!("[gate_ws] connected to futures WS v4");
        crate::engine::utils::record_ws_connected("gate");
        let (mut write, mut read) = ws_stream.split();

        // Gate doesn't have a separate auth step — each request carries auth inline.
        // So we go straight to the message loop.

        let pending: Arc<Mutex<HashMap<String, ResponseSender>>> = Arc::new(Mutex::new(HashMap::new()));
        let pending_clone = pending.clone();

        let reader_handle = tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(t)) => {
                        if let Ok(val) = serde_json::from_str::<Value>(&t) {
                            // Gate uses "request_id" for correlation — can be string OR integer
                            let id_opt = val["request_id"].as_str().map(|s| s.to_string())
                                .or_else(|| val["request_id"].as_i64().map(|n| n.to_string()))
                                .or_else(|| val["request_id"].as_u64().map(|n| n.to_string()));
                            if let Some(id) = id_opt {
                                if let Some(sender) = pending_clone.lock().await.remove(&id) {
                                    let _ = sender.send(val);
                                } else {
                                    tracing::debug!("[gate_ws] unmatched response id={}: {}", id, &t[..t.len().min(200)]);
                                }
                            } else {
                                // Log non-correlated messages (ping/pong responses, errors, etc.)
                                let channel = val["channel"].as_str().unwrap_or("?");
                                if channel != "futures.pong" && channel != "futures.ping" {
                                    tracing::debug!("[gate_ws] non-order msg channel={}: {}", channel, &t[..t.len().min(200)]);
                                }
                            }
                        }
                    }
                    Ok(Message::Ping(p)) => {
                        // tungstenite handles pong automatically
                        let _ = p;
                    }
                    Err(e) => {
                        tracing::error!("[gate_ws] read error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });

        // ── PING TASK ── (Gate futures WS requires ping every 10-15s)
        let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
        let ping_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                if ping_tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        // Store keys for inline auth
        let key = api_key.clone();
        let secret = api_secret.clone();

        loop {
            tokio::select! {
                Some((mut req_json, sender)) = rx.recv() => {
                    let req_id = req_json["request_id"].as_str().unwrap_or("unknown").to_string();
                    pending.lock().await.insert(req_id.clone(), sender);

                    // Inject auth into the request
                    let ts = (crate::engine::real::now_ms() / 1000) as i64;
                    let channel = req_json["channel"].as_str().unwrap_or("futures.order_place").to_string();
                    let event = "api";
                    let sign = gate_ws_sign(&secret, &channel, event, ts);

                    if let Some(obj) = req_json.as_object_mut() {
                        obj.insert("time".to_string(), serde_json::json!(ts));
                        obj.insert("auth".to_string(), serde_json::json!({
                            "method": "api_key",
                            "KEY": key,
                            "SIGN": sign
                        }));
                    }

                    let msg = Message::Text(req_json.to_string());
                    if let Err(e) = write.send(msg).await {
                        tracing::error!("[gate_ws] write error: {}", e);
                        break;
                    }
                }
                Some(()) = ping_rx.recv() => {
                    let ping = serde_json::json!({"channel": "futures.ping"});
                    if let Err(e) = write.send(Message::Text(ping.to_string())).await {
                        tracing::error!("[gate_ws] ping failed: {}", e);
                        break;
                    }
                }
                else => break,
            }
        }

        reader_handle.abort();
        ping_handle.abort();
        tracing::warn!("[gate_ws] connection dropped. Restarting loop...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

pub async fn send_gate_ws_request(req: Value) -> Result<Value, String> {
    let tx = GATE_WS_TX.get().ok_or("Gate WS trader not initialized")?;
    let (resp_tx, resp_rx) = oneshot::channel();

    tx.send((req, resp_tx)).await.map_err(|_| "Gate WS channel closed")?;

    match tokio::time::timeout(tokio::time::Duration::from_secs(5), resp_rx).await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(_)) => Err("Response sender dropped".to_string()),
        Err(_) => Err("Timeout waiting for Gate WS response".to_string()),
    }
}
