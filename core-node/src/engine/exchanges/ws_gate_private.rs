//! Gate.io Futures Private WebSocket — Order Fill Notifications
//!
//! Persistent WS connection that subscribes to `futures.orders` channel.
//! Pushes fill data into a global DashMap cache (GATE_ORDER_CACHE).
//! When a fill is detected for a pending maker order, fires `maker_fill_notify`.
//!
//! URL: wss://fx-ws.gateio.ws/v4/ws/usdt
//! Auth: HMAC-SHA512 inline per-subscription (same as ws_gate.rs)

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::OnceLock;
use std::time::Duration;
use dashmap::DashMap;

use crate::engine::real::now_ms;

// ── Global order cache ──────────────────────────────────────────────────────

/// Cached order state from Gate.io private WS order push.
#[derive(Clone, Debug)]
pub struct GateOrderState {
    pub fill_price: f64,
    pub fee: f64,
    pub filled_contracts: i64,
    pub status: String,      // "open", "finished"
    pub finish_as: String,   // "filled", "cancelled", "expired", "ioc", ""
    pub updated_ms: i64,
}

pub static GATE_ORDER_CACHE: OnceLock<Arc<DashMap<String, GateOrderState>>> = OnceLock::new();

pub fn get_order_cache() -> Arc<DashMap<String, GateOrderState>> {
    GATE_ORDER_CACHE.get_or_init(|| Arc::new(DashMap::new())).clone()
}

// ── Maker fill notify reference ─────────────────────────────────────────────
// Set once at startup from EngineContext.maker_fill_notify.
// WS loop uses this to wake up maker_manager when a fill arrives.

static MAKER_NOTIFY: OnceLock<Arc<tokio::sync::Notify>> = OnceLock::new();

pub fn set_maker_notify(notify: Arc<tokio::sync::Notify>) {
    let _ = MAKER_NOTIFY.set(notify);
}

// ── Pending makers reference ────────────────────────────────────────────────
// Set once at startup. WS loop checks if a filled order_id is a pending maker.

use crate::engine::maker_manager::PendingMakerOrder;

static PENDING_MAKERS_REF: OnceLock<Arc<DashMap<String, PendingMakerOrder>>> = OnceLock::new();

pub fn set_pending_makers_ref(pm: Arc<DashMap<String, PendingMakerOrder>>) {
    let _ = PENDING_MAKERS_REF.set(pm);
}

// ── WS Lifecycle ────────────────────────────────────────────────────────────

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
        gate_private_ws_loop(key, secret).await;
    });

    *is_running = true;
}

fn gate_ws_sign(secret: &str, channel: &str, event: &str, ts: i64) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha512;
    type HmacSha512 = Hmac<Sha512>;

    let msg = format!("channel={}&event={}&time={}", channel, event, ts);
    let mut mac = HmacSha512::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(msg.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

async fn gate_private_ws_loop(api_key: String, api_secret: String) {
    let cache = get_order_cache();

    loop {
        let url = "wss://fx-ws.gateio.ws/v4/ws/usdt";

        let (ws_stream, _) = match connect_async(url).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("[gate_ws_private] connection failed: {}. Retrying in 2s...", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        tracing::info!("[gate_ws_private] Connected to Gate.io Private WS for order updates");
        let (mut write, mut read) = ws_stream.split();

        // ── Subscribe to futures.orders with auth ──
        let ts = (now_ms() / 1000) as i64;
        let channel = "futures.orders";
        let event = "subscribe";
        let sign = gate_ws_sign(&api_secret, channel, event, ts);

        let sub_msg = serde_json::json!({
            "time": ts,
            "channel": channel,
            "event": event,
            "payload": ["!all"],
            "auth": {
                "method": "api_key",
                "KEY": api_key,
                "SIGN": sign
            }
        });

        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
            tracing::error!("[gate_ws_private] Subscribe send failed: {}", e);
            continue;
        }

        // ── Ping task (Gate requires ping every 10-15s) ──
        let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
        let ping_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
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
                                let event_type = val["event"].as_str().unwrap_or("");
                                let ch = val["channel"].as_str().unwrap_or("");

                                // Subscribe confirmation
                                if event_type == "subscribe" && ch == "futures.orders" {
                                    let err_val = &val["error"];
                                    if err_val.is_null() || err_val["code"].as_i64() == Some(0) {
                                        tracing::info!("[gate_ws_private] Subscribed to futures.orders successfully");
                                    } else {
                                        tracing::error!("[gate_ws_private] Subscribe error: {}", val);
                                    }
                                    continue;
                                }

                                // Order update push
                                if event_type == "update" && ch == "futures.orders" {
                                    if let Some(data) = val["result"].as_array() {
                                        for order in data {
                                            process_order_update(order, &cache);
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(_)) => {} // auto-handled by tungstenite
                        Err(e) => {
                            tracing::error!("[gate_ws_private] Read error: {}", e);
                            _read_failed = true;
                            break;
                        }
                        _ => {}
                    }
                }
                Some(()) = ping_rx.recv() => {
                    let ping = serde_json::json!({"channel": "futures.ping"});
                    if let Err(e) = write.send(Message::Text(ping.to_string())).await {
                        tracing::error!("[gate_ws_private] Ping failed: {}", e);
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
        tracing::warn!("[gate_ws_private] Connection dropped. Reconnecting in 2s...");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Process a single order update from Gate's futures.orders push.
fn process_order_update(order: &Value, cache: &Arc<DashMap<String, GateOrderState>>) {
    let order_id = order["id"].as_i64().map(|i| i.to_string())
        .or_else(|| order["id"].as_str().map(|s| s.to_string()));

    let Some(oid) = order_id else { return };

    let fill_price: f64 = order["fill_price"]
        .as_str().and_then(|s| s.parse().ok())
        .or_else(|| order["fill_price"].as_f64())
        .unwrap_or(0.0);
    let fee: f64 = order["fee"]
        .as_str().and_then(|s| s.parse::<f64>().ok())
        .or_else(|| order["fee"].as_f64())
        .unwrap_or(0.0);
    let size = order["size"].as_i64().unwrap_or(0).abs();
    let left = order["left"].as_i64().unwrap_or(0).abs();
    let filled_contracts = (size - left).max(0) as i64;
    let status = order["status"].as_str().unwrap_or("").to_string();
    let finish_as = order["finish_as"].as_str().unwrap_or("").to_string();

    let state = GateOrderState {
        fill_price,
        fee: fee.abs(),
        filled_contracts,
        status: status.clone(),
        finish_as: finish_as.clone(),
        updated_ms: now_ms(),
    };

    cache.insert(oid.clone(), state);

    // Log meaningful updates
    if filled_contracts > 0 || status == "finished" {
        tracing::debug!(
            "[gate_ws_private] Order {} update: status={} finish_as={} fill_price={} fee={} filled={}",
            oid, status, finish_as, fill_price, fee, filled_contracts
        );
    }

    // Check if this is a pending maker order and notify
    if filled_contracts > 0 {
        if let Some(pending_ref) = PENDING_MAKERS_REF.get() {
            // Check if ANY pending maker has this order_id
            let is_pending = pending_ref.iter().any(|entry| entry.value().order_id == oid);
            if is_pending {
                tracing::info!(
                    "[gate_ws_private] 🔔 MAKER FILL DETECTED: order_id={} filled_contracts={} price={}",
                    oid, filled_contracts, fill_price
                );
                if let Some(notify) = MAKER_NOTIFY.get() {
                    notify.notify_waiters();
                }
            }
        }
    }
}
