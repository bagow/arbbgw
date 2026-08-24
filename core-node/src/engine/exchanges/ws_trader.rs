use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::OnceLock;

type ResponseSender = oneshot::Sender<Value>;

static WS_TRADER_TX: OnceLock<mpsc::Sender<(Value, ResponseSender)>> = OnceLock::new();

pub fn get_or_init_ws() -> mpsc::Sender<(Value, ResponseSender)> {
    WS_TRADER_TX.get_or_init(|| {
        let (tx, rx) = mpsc::channel(1000);
        tokio::spawn(ws_trader_loop(rx));
        tx
    }).clone()
}

async fn ws_trader_loop(mut rx: mpsc::Receiver<(Value, ResponseSender)>) {
    let mut backoff_secs: u64 = 2;
    loop {
        let url = "wss://ws-fapi.binance.com/ws-fapi/v1";
        
        let (ws_stream, _) = match tokio::time::timeout(tokio::time::Duration::from_secs(10), connect_async(url)).await {
            Ok(Ok(res)) => {
                backoff_secs = 2; // Reset on success
                res
            },
            Ok(Err(e)) => {
                let err_str = e.to_string();
                // 418 = IP banned by Binance. Must wait FULL ban duration (300s).
                // Reconnecting early triggers new 418 → resets ban → infinite loop!
                if err_str.contains("418") {
                    let ban_secs: u64 = 300;
                    tracing::error!("[binance_ws] IP BANNED (418). Sleeping full {}s to avoid ban extension...", ban_secs);
                    let ban_until = crate::engine::real::now_ms() + (ban_secs as i64 * 1000);
                    crate::engine::risk_manager::ban_exchange("binance", ban_until);
                    tokio::time::sleep(tokio::time::Duration::from_secs(ban_secs)).await;
                } else {
                    tracing::error!("[binance_ws] connection failed: {}. Retrying in {}s...", e, backoff_secs);
                    tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(60);
                }
                continue;
            },
            Err(_) => {
                tracing::error!("[binance_ws] connect_async TIMEOUT after 10s. Retrying in {}s...", backoff_secs);
                tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
                continue;
            }
        };

        tracing::info!("[binance_ws] connected to Futures WS Trading API");
        crate::engine::utils::record_ws_connected("binance");
        // Reset WS failure counter — connection is alive again, REST fallbacks can resume
        crate::engine::exchanges::binance::reset_ws_failures();
        let (mut write, mut read) = ws_stream.split();
        let pending: Arc<Mutex<HashMap<String, ResponseSender>>> = Arc::new(Mutex::new(HashMap::new()));
        
        let pending_clone = pending.clone();
        
        let reader_handle = tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(t)) => {
                        if let Ok(val) = serde_json::from_str::<Value>(&t) {
                            if let Some(id) = val["id"].as_str() {
                                if let Some(sender) = pending_clone.lock().await.remove(id) {
                                    let _ = sender.send(val);
                                }
                            }
                        }
                    }
                    Ok(Message::Ping(_p)) => {
                        // handled automatically by tungstenite usually, but can be intercepted
                    }
                    Err(e) => {
                        tracing::error!("[binance_ws] read error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });

        // Loop over local messages to send
        while let Some((req_json, sender)) = rx.recv().await {
            let id = req_json["id"].as_str().unwrap().to_string();
            pending.lock().await.insert(id.clone(), sender);
            
            let msg = Message::Text(req_json.to_string());
            // Timeout prevents hanging forever on a silently-dead connection.
            // Without this, a stuck write.send() blocks channel consumption,
            // filling the 1000-capacity mpsc and rejecting ALL new orders.
            match tokio::time::timeout(
                tokio::time::Duration::from_secs(5),
                write.send(msg),
            ).await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => {
                    tracing::error!("[binance_ws] write error: {}", e);
                    // Remove the pending sender so caller gets an error instead of hanging
                    if let Some(s) = pending.lock().await.remove(&id) {
                        let _ = s.send(serde_json::json!({"error": {"msg": format!("WS write failed: {}", e)}}));
                    }
                    break; // Connection lost, restart
                }
                Err(_) => {
                    tracing::error!("[binance_ws] write TIMEOUT (5s) — connection likely dead");
                    if let Some(s) = pending.lock().await.remove(&id) {
                        let _ = s.send(serde_json::json!({"error": {"msg": "WS write timeout"}}));
                    }
                    break; // Force reconnect
                }
            }
        }
        
        reader_handle.abort();
        // Drain any pending requests that won't get a response (connection is dead)
        {
            let mut p = pending.lock().await;
            for (_, sender) in p.drain() {
                let _ = sender.send(serde_json::json!({"error": {"msg": "WS connection reset"}}));
            }
        }
        tracing::warn!("[binance_ws] connection dropped. Restarting loop...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

pub async fn send_ws_request(req: Value) -> Result<Value, String> {
    let tx = get_or_init_ws();
    let (resp_tx, resp_rx) = oneshot::channel();
    
    // try_send: if the WS is disconnected and channel is full, fail immediately
    // instead of blocking the entire trade cycle forever
    tx.try_send((req, resp_tx)).map_err(|e| format!("WS channel full/closed (is Binance WS connected?): {}", e))?;
    
    match tokio::time::timeout(tokio::time::Duration::from_millis(2500), resp_rx).await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(_)) => Err("Response sender dropped".to_string()),
        Err(_) => Err("Timeout waiting for WS response".to_string()),
    }
}
