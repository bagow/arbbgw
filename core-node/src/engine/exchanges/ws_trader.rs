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
        
        let (ws_stream, _) = match connect_async(url).await {
            Ok(res) => {
                backoff_secs = 2; // Reset on success
                res
            },
            Err(e) => {
                let err_str = e.to_string();
                // 418 = IP banned by Binance. Back off aggressively.
                if err_str.contains("418") {
                    tracing::error!("[binance_ws] IP BANNED (418). Backing off {}s...", backoff_secs);
                    tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(300); // max 5 min
                } else {
                    tracing::error!("[binance_ws] connection failed: {}. Retrying in {}s...", e, backoff_secs);
                    tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(60);
                }
                continue;
            }
        };

        tracing::info!("[binance_ws] connected to Futures WS Trading API");
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
            pending.lock().await.insert(id, sender);
            
            let msg = Message::Text(req_json.to_string());
            if let Err(e) = write.send(msg).await {
                tracing::error!("[binance_ws] write error: {}", e);
                break; // Connection lost, restart
            }
        }
        
        reader_handle.abort();
        tracing::warn!("[binance_ws] connection dropped. Restarting loop...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

pub async fn send_ws_request(req: Value) -> Result<Value, String> {
    let tx = get_or_init_ws();
    let (resp_tx, resp_rx) = oneshot::channel();
    
    tx.send((req, resp_tx)).await.map_err(|_| "WS trader loop channel closed")?;
    
    match tokio::time::timeout(tokio::time::Duration::from_secs(5), resp_rx).await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(_)) => Err("Response sender dropped".to_string()),
        Err(_) => Err("Timeout waiting for WS response".to_string()),
    }
}
