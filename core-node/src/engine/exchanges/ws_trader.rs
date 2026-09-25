use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

type ResponseSender = oneshot::Sender<Value>;

type RequestSender = mpsc::Sender<(Value, ResponseSender)>;

static WS_QUERY_TX: OnceLock<RequestSender> = OnceLock::new();
static WS_ORDER_TX: OnceLock<RequestSender> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WsLane {
    Query,
    Order,
}

impl WsLane {
    fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Order => "order",
        }
    }
}

fn request_lane(request: &Value) -> WsLane {
    if request["method"].as_str() == Some("order.place") {
        WsLane::Order
    } else {
        WsLane::Query
    }
}

fn get_or_init_lane(lane: WsLane) -> RequestSender {
    let slot = match lane {
        WsLane::Query => &WS_QUERY_TX,
        WsLane::Order => &WS_ORDER_TX,
    };
    slot
        .get_or_init(|| {
            let (tx, rx) = mpsc::channel(1000);
            tokio::spawn(ws_trader_loop(rx, lane));
            tx
        })
        .clone()
}

/// Establish the critical order lane before any opportunity reaches execution.
/// Query traffic may reconnect independently without disturbing order.place.
pub fn prewarm_ws_connections() {
    let _ = get_or_init_lane(WsLane::Query);
    let _ = get_or_init_lane(WsLane::Order);
}

async fn ws_trader_loop(mut rx: mpsc::Receiver<(Value, ResponseSender)>, lane: WsLane) {
    let mut backoff_secs: u64 = 2;
    loop {
        let url = "wss://ws-fapi.binance.com/ws-fapi/v1";

        let (ws_stream, _) = match tokio::time::timeout(
            tokio::time::Duration::from_secs(10),
            connect_async(url),
        )
        .await
        {
            Ok(Ok(res)) => {
                backoff_secs = 2; // Reset on success
                res
            }
            Ok(Err(e)) => {
                let err_str = e.to_string();
                // 418 = IP banned by Binance. Must wait FULL ban duration (300s).
                // Reconnecting early triggers new 418 → resets ban → infinite loop!
                if err_str.contains("418") {
                    let ban_secs: u64 = 300;
                    tracing::error!(
                        "[binance_ws] lane={} IP BANNED (418). Sleeping full {}s to avoid ban extension...",
                        lane.label(),
                        ban_secs
                    );
                    let ban_until = crate::engine::real::now_ms() + (ban_secs as i64 * 1000);
                    crate::engine::risk_manager::ban_exchange("binance", ban_until);
                    tokio::time::sleep(tokio::time::Duration::from_secs(ban_secs)).await;
                } else {
                    tracing::error!(
                        "[binance_ws] lane={} connection failed: {}. Retrying in {}s...",
                        lane.label(),
                        e,
                        backoff_secs
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(60);
                }
                continue;
            }
            Err(_) => {
                tracing::error!(
                    "[binance_ws] lane={} connect_async TIMEOUT after 10s. Retrying in {}s...",
                    lane.label(),
                    backoff_secs
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
                continue;
            }
        };

        tracing::info!(
            "[binance_ws] lane={} connected to Futures WS Trading API",
            lane.label()
        );
        crate::engine::utils::record_ws_connected("binance");
        // The counter describes query health. A quiet order connection must not
        // hide failures from account monitoring.
        if lane == WsLane::Query {
            crate::engine::exchanges::binance::reset_ws_failures();
        }
        let mut ws = ws_stream;
        let mut pending: HashMap<String, ResponseSender> = HashMap::new();
        let mut cleanup = tokio::time::interval(tokio::time::Duration::from_secs(1));
        let mut last_received = tokio::time::Instant::now();
        loop {
            tokio::select! {
                incoming=ws.next()=>{
                    last_received=tokio::time::Instant::now();
                    match incoming {
                        Some(Ok(Message::Text(text)))=>if let Ok(v)=serde_json::from_str::<Value>(&text){
                            if let Some(id)=v["id"].as_str(){if let Some(sender)=pending.remove(id){let _=sender.send(v);}}
                        },
                        Some(Ok(Message::Ping(p)))=>{if !matches!(tokio::time::timeout(tokio::time::Duration::from_secs(1),ws.send(Message::Pong(p))).await,Ok(Ok(()))){break;}},
                        Some(Ok(Message::Close(_)))|Some(Err(_))|None=>break,
                        _=>{}
                    }
                },
                next=rx.recv()=>{
                    let(req,sender)=match next{Some(x)=>x,None=>return};
                    if sender.is_closed(){continue;}
                    let id=match req["id"].as_str(){Some(id)=>id.to_string(),None=>continue};
                    pending.insert(id,sender);
                    if !matches!(tokio::time::timeout(tokio::time::Duration::from_secs(1),ws.send(Message::Text(req.to_string()))).await,Ok(Ok(()))){break;}
                },
                _=cleanup.tick()=>{
                    let response_waiter_timed_out = pending.values().any(ResponseSender::is_closed);
                    pending.retain(|_,sender|!sender.is_closed());
                    if response_waiter_timed_out {
                        tracing::warn!(
                            "[binance_ws] lane={} response waiter timed out; reconnecting stale request channel",
                            lane.label()
                        );
                        break;
                    }
                    if last_received.elapsed()>tokio::time::Duration::from_secs(240){break;}
                }
            }
        }
        // A disconnect drops waiters. No queued or possibly accepted order is replayed.
        drop(pending);
        tracing::warn!(
            "[binance_ws] lane={} connection dropped. Restarting loop...",
            lane.label()
        );
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

pub async fn send_ws_request(req: Value) -> Result<Value, String> {
    let lane = request_lane(&req);
    let tx = get_or_init_lane(lane);
    let (resp_tx, resp_rx) = oneshot::channel();

    // try_send: if the WS is disconnected and channel is full, fail immediately
    // instead of blocking the entire trade cycle forever
    tx.try_send((req, resp_tx))
        .map_err(|e| {
            format!(
                "WS {} channel full/closed (is Binance WS connected?): {}",
                lane.label(),
                e
            )
        })?;

    match tokio::time::timeout(tokio::time::Duration::from_millis(2500), resp_rx).await {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(_)) => Err("Response sender dropped".to_string()),
        Err(_) => Err("Timeout waiting for WS response".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_response_waiter_marks_request_channel_stale() {
        let (sender, receiver) = oneshot::channel();
        drop(receiver);
        let mut pending = HashMap::new();
        pending.insert("request-1".to_string(), sender);

        assert!(pending.values().any(ResponseSender::is_closed));
    }

    #[test]
    fn order_place_uses_a_lane_isolated_from_queries() {
        assert_eq!(
            request_lane(&serde_json::json!({"method": "order.place"})),
            WsLane::Order
        );
        assert_eq!(
            request_lane(&serde_json::json!({"method": "depth"})),
            WsLane::Query
        );
        assert_eq!(
            request_lane(&serde_json::json!({"method": "v2/account.balance"})),
            WsLane::Query
        );
    }
}
