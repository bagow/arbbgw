use futures_util::{stream::FuturesUnordered, SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    OnceLock,
};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{client_async_tls, tungstenite::protocol::Message};

type ResponseSender = oneshot::Sender<Value>;

struct QueuedRequest {
    id: String,
    payload: String,
    enqueued: std::time::Instant,
    response: ResponseSender,
}

type RequestSender = mpsc::Sender<QueuedRequest>;

static WS_QUERY_TX: OnceLock<RequestSender> = OnceLock::new();
static WS_ORDER_TX: OnceLock<RequestSender> = OnceLock::new();
static ORDER_READY: AtomicBool = AtomicBool::new(false);
static ROUTE_OFFSET: AtomicUsize = AtomicUsize::new(0);
const MAX_ROUTE_ATTEMPTS: usize = 4;
type BinanceWs = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

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
    slot.get_or_init(|| {
        let (tx, rx) = mpsc::channel(1000);
        tokio::spawn(ws_trader_loop(rx, lane));
        tx
    })
    .clone()
}

async fn connect_and_probe_route(address: SocketAddr) -> Result<(BinanceWs, u128), String> {
    const URL: &str = "wss://ws-fapi.binance.com/ws-fapi/v1";
    let socket = tokio::time::timeout(tokio::time::Duration::from_secs(4),tokio::net::TcpStream::connect(address))
        .await.map_err(|_|format!("TCP timeout via {address}"))?
        .map_err(|e|format!("TCP failure via {address}: {e}"))?;
    socket.set_nodelay(true).map_err(|e|format!("TCP_NODELAY failed via {address}: {e}"))?;
    let (mut ws, _)=tokio::time::timeout(tokio::time::Duration::from_secs(6),client_async_tls(URL,socket))
        .await.map_err(|_|format!("TLS/WebSocket timeout via {address}"))?
        .map_err(|e|format!("TLS/WebSocket failure via {address}: {e}"))?;
    // Measure the application path without consuming an API request or credentials.
    let payload=format!("arb-route-{address}").into_bytes();
    let started=std::time::Instant::now();
    ws.send(Message::Ping(payload.clone().into())).await.map_err(|e|format!("probe send failed via {address}: {e}"))?;
    tokio::time::timeout(tokio::time::Duration::from_secs(2),async {
        loop { match ws.next().await {
            Some(Ok(Message::Pong(received))) if received==payload => return Ok(()),
            Some(Ok(Message::Ping(p))) => ws.send(Message::Pong(p)).await.map_err(|e|format!("probe pong failed via {address}: {e}"))?,
            Some(Ok(Message::Close(frame))) => return Err(format!("closed during probe via {address}: {frame:?}")),
            Some(Ok(_)) => {},
            Some(Err(e)) => return Err(format!("probe receive failed via {address}: {e}")),
            None => return Err(format!("ended during probe via {address}")),
        }}
    }).await.map_err(|_|format!("probe timeout via {address}"))??;
    Ok((ws,started.elapsed().as_micros()))
}

async fn connect_binance_ws() -> Result<BinanceWs, String> {
    const HOST: &str = "ws-fapi.binance.com";

    let addresses = tokio::net::lookup_host((HOST, 443))
        .await
        .map_err(|error| format!("Binance WS DNS lookup failed: {error}"))?;
    let mut addresses:Vec<_>=addresses.collect(); addresses.sort_unstable(); addresses.dedup();
    if addresses.is_empty(){return Err("Binance WS DNS returned no addresses".into());}
    let count=addresses.len(); let attempts_count=count.min(MAX_ROUTE_ATTEMPTS);
    let offset=ROUTE_OFFSET.fetch_add(attempts_count,Ordering::Relaxed)%count;
    let mut attempts = FuturesUnordered::new();
    for index in 0..attempts_count {
        let address=addresses[(offset+index)%count];
        attempts.push(async move {connect_and_probe_route(address).await.map(|result|(result,address))});
    }

    let mut failures = Vec::new();
    while let Some(attempt) = attempts.next().await {
        match attempt {
            Ok(((ws,rtt_us),address)) => {
                tracing::info!("[binance_ws] selected route={} probe_rtt_us={}",address,rtt_us);
                return Ok(ws);
            },
            Err(error) => failures.push(error),
        }
    }
    Err(format!(
        "all Binance WS addresses failed: {}",
        failures.join("; ")
    ))
}

/// Establish the critical order lane before any opportunity reaches execution.
/// Query traffic may reconnect independently without disturbing order.place.
pub fn prewarm_ws_connections() {
    let _ = get_or_init_lane(WsLane::Query);
    let _ = get_or_init_lane(WsLane::Order);
}

pub fn order_lane_ready() -> bool {
    ORDER_READY.load(Ordering::Acquire)
}

async fn ws_trader_loop(mut rx: mpsc::Receiver<QueuedRequest>, lane: WsLane) {
    let mut backoff_secs: u64 = 2;
    loop {
        let ws_stream = match tokio::time::timeout(
            tokio::time::Duration::from_secs(12),
            connect_binance_ws(),
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
                    "[binance_ws] lane={} connection race TIMEOUT after 12s. Retrying in {}s...",
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
        if lane == WsLane::Order {
            ORDER_READY.store(true, Ordering::Release);
        }
        crate::engine::utils::record_ws_connected("binance");
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
                    let QueuedRequest{id,payload,enqueued,response:sender}=match next{Some(x)=>x,None=>return};
                    if sender.is_closed(){continue;}
                    pending.insert(id,sender);
                    if !matches!(tokio::time::timeout(tokio::time::Duration::from_secs(1),ws.send(Message::Text(payload))).await,Ok(Ok(()))){break;}
                    if lane == WsLane::Order {
                        tracing::info!(
                            "[binance_ws] order socket queue_delay_us={}",
                            enqueued.elapsed().as_micros(),
                        );
                    }
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
        if lane == WsLane::Order {
            ORDER_READY.store(false, Ordering::Release);
        }
        tracing::warn!(
            "[binance_ws] lane={} connection dropped. Restarting loop...",
            lane.label()
        );
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

pub async fn send_ws_request(req: Value) -> Result<Value, String> {
    let lane = request_lane(&req);
    let id = req["id"]
        .as_str()
        .ok_or_else(|| "WS request is missing an id".to_string())?
        .to_owned();
    let payload = req.to_string();
    let tx = get_or_init_lane(lane);
    let (resp_tx, resp_rx) = oneshot::channel();
    let started = std::time::Instant::now();

    // try_send: if the WS is disconnected and channel is full, fail immediately
    // instead of blocking the entire trade cycle forever
    tx.try_send(QueuedRequest {
        id,
        payload,
        enqueued: started,
        response: resp_tx,
    }).map_err(|e| {
        format!(
            "WS {} channel full/closed (is Binance WS connected?): {}",
            lane.label(),
            e
        )
    })?;

    match tokio::time::timeout(tokio::time::Duration::from_millis(2500), resp_rx).await {
        Ok(Ok(val)) => {
            if lane == WsLane::Order {
                tracing::info!(
                    "[binance_ws] order response client_rtt_us={}",
                    started.elapsed().as_micros(),
                );
            }
            Ok(val)
        }
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
