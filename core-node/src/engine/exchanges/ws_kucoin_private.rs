use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::OnceLock;
use std::time::Duration;
use dashmap::DashMap;

// Global cache for order execution results: OrderId -> (Price, Fee, Size, Status)
pub static KUCOIN_ORDER_CACHE: OnceLock<Arc<DashMap<String, KucoinOrderState>>> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct KucoinOrderState {
    pub filled_size: f64,
    pub deal_value: f64,
    pub fee: f64,
    pub status: String,
}

pub fn get_order_cache() -> Arc<DashMap<String, KucoinOrderState>> {
    KUCOIN_ORDER_CACHE.get_or_init(|| Arc::new(DashMap::new())).clone()
}

// Ensure the private WS is running for this set of keys
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
        kucoin_private_ws_loop(key, secret, pass).await;
    });
    
    *is_running = true;
}

async fn get_bullet_private(api_key: &str, api_secret: &str, passphrase: &str) -> Result<(String, String), String> {
    let path = "/api/v1/bullet-private";
    let v = super::ws_kucoin::send_kucoin_request(
        api_key, api_secret, passphrase,
        "POST", path, "", 10000,
    ).await?;

    if v["code"].as_str() != Some("200000") {
        return Err(format!("Failed to get bullet-private: {}", v));
    }

    let token = v["data"]["token"].as_str().ok_or("No token in bullet-private")?.to_string();
    let endpoint = v["data"]["instanceServers"][0]["endpoint"].as_str().ok_or("No endpoint")?.to_string();
    
    Ok((token, endpoint))
}

async fn kucoin_private_ws_loop(api_key: String, api_secret: String, passphrase: String) {
    let cache = get_order_cache();
    
    loop {
        tracing::info!("[kucoin_ws_private] Fetching bullet-private token...");
        let (token, endpoint) = match get_bullet_private(&api_key, &api_secret, &passphrase).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("[kucoin_ws_private] bullet-private error: {}. Retrying in 5s...", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let connect_id = uuid::Uuid::new_v4().to_string();
        let ws_url = format!("{}?token={}&connectId={}", endpoint, token, connect_id);

        let (ws_stream, _) = match connect_async(&ws_url).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("[kucoin_ws_private] connection failed: {}. Retrying in 2s...", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        tracing::info!("[kucoin_ws_private] Connected to KuCoin Private Futures WS!");
        let (mut write, mut read) = ws_stream.split();

        // Send subscription
        let sub_msg = serde_json::json!({
            "id": connect_id,
            "type": "subscribe",
            "topic": "/contractMarket/tradeOrders",
            "privateChannel": true,
            "response": true
        });

        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
            tracing::error!("[kucoin_ws_private] Subscribe send failed: {}", e);
            continue;
        }

        // Ping task
        let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
        let ping_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(18)).await; // KuCoin ping is typically 18s or 50s
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
                            if let Ok(val) = serde_json::from_str::<Value>(&t) {
                                if val["type"].as_str() == Some("message") && val["topic"].as_str() == Some("/contractMarket/tradeOrders") {
                                    let subject = val["subject"].as_str().unwrap_or("");
                                    if subject == "orderChange" {
                                        let data = &val["data"];
                                        let status = data["status"].as_str().unwrap_or(""); // "open", "match", "done"
                                        let order_id = data["orderId"].as_str().unwrap_or("");
                                        
                                        if !order_id.is_empty() {
                                            let mut state = cache.entry(order_id.to_string()).or_insert(KucoinOrderState {
                                                filled_size: 0.0,
                                                deal_value: 0.0,
                                                fee: 0.0,
                                                status: "open".to_string(),
                                            });
                                            
                                            state.status = status.to_string();
                                            
                                            // Cumulative updates
                                            if let Some(match_size) = data["matchSize"].as_f64().or_else(|| data["matchSize"].as_str().and_then(|s| s.parse().ok())) {
                                                state.filled_size += match_size;
                                            }
                                            if let Some(match_price) = data["matchPrice"].as_f64().or_else(|| data["matchPrice"].as_str().and_then(|s| s.parse().ok())) {
                                                if let Some(match_size) = data["matchSize"].as_f64().or_else(|| data["matchSize"].as_str().and_then(|s| s.parse().ok())) {
                                                    state.deal_value += match_price * match_size;
                                                    tracing::info!(
                                                        "[kucoin_ws_private] 📊 Fill oid={} status={} matchPrice={:.8} matchSize={:.4} cum_deal_value={:.8} cum_filled_size={:.4} cum_price={:.8}",
                                                        order_id, status, match_price, match_size,
                                                        state.deal_value, state.filled_size,
                                                        if state.filled_size > 0.0 { state.deal_value / state.filled_size } else { 0.0 }
                                                    );
                                                }
                                            }
                                            if let Some(fee) = data["fee"].as_f64().or_else(|| data["fee"].as_str().and_then(|s| s.parse().ok())) {
                                                state.fee += fee; // KuCoin fees can be negative (maker rebate)
                                            }
                                            
                                            // Handle "done" messages which might carry final fill state
                                            if status == "done" || data["type"].as_str() == Some("filled") || data["type"].as_str() == Some("canceled") {
                                                state.status = "done".to_string();
                                                tracing::debug!("[kucoin_ws_private] Order {} is done (filled: {}, fee: {})", order_id, state.filled_size, state.fee);
                                            }
                                        }
                                    }
                                } else if val["type"].as_str() == Some("welcome") {
                                    tracing::info!("[kucoin_ws_private] Received welcome message");
                                }
                            }
                        }
                        Ok(Message::Ping(_)) => {} // tungstenite handles pong
                        Err(e) => {
                            tracing::error!("[kucoin_ws_private] Read error: {}", e);
                            _read_failed = true;
                            break;
                        }
                        _ => {}
                    }
                }
                Some(()) = ping_rx.recv() => {
                    let ping_msg = serde_json::json!({
                        "id": uuid::Uuid::new_v4().to_string(),
                        "type": "ping"
                    });
                    if let Err(e) = write.send(Message::Text(ping_msg.to_string())).await {
                        tracing::error!("[kucoin_ws_private] Ping failed: {}", e);
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
            tracing::warn!("[kucoin_ws_private] Connection dropped. Reconnecting in 2s...");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}
