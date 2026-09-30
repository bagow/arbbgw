//! Persistent KuCoin Pro WebSocket order transport.
//!
//! This is the only transport used to place KuCoin futures orders.  HTTP is
//! deliberately not used as a fallback: after a socket failure the exchange
//! may already have accepted the order, so replaying it over another transport
//! could create an unintended second position.

use base64::Engine as _;
use futures_util::{stream::FuturesUnordered, SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::{client_async_tls, tungstenite::protocol::Message};
use uuid::Uuid;

type ResponseSender = oneshot::Sender<Value>;
struct QueuedRequest {
    id: String,
    payload: String,
    enqueued: std::time::Instant,
    response: ResponseSender,
}
type RequestSender = mpsc::Sender<QueuedRequest>;

#[derive(Clone)]
struct Credentials {
    api_key: String,
    api_secret: String,
    passphrase: String,
}

struct OrderLane {
    tx: RequestSender,
    ready: watch::Receiver<bool>,
}

static ORDER_LANE: OnceLock<OrderLane> = OnceLock::new();

fn hmac_b64(secret: &str, payload: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key");
    mac.update(payload.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

fn json_frame_bytes(message: Message, phase: &str) -> Result<Vec<u8>, String> {
    match message {
        Message::Text(text) => Ok(text.as_bytes().to_vec()),
        Message::Binary(bytes) => Ok(bytes.to_vec()),
        _ => Err(format!("KuCoin Pro WS sent a non-JSON {phase} frame")),
    }
}

fn authenticated_url(credentials: &Credentials, timestamp: i64) -> String {
    let timestamp = timestamp.to_string();
    let signature = hmac_b64(
        &credentials.api_secret,
        &format!("{}{}", credentials.api_key, timestamp),
    );
    let encrypted_passphrase = hmac_b64(&credentials.api_secret, &credentials.passphrase);
    let mut url = reqwest::Url::parse("wss://wsapi.kucoin.com/v1/private")
        .expect("static KuCoin Pro WebSocket URL is valid");
    url.query_pairs_mut()
        .append_pair("apikey", &credentials.api_key)
        .append_pair("timestamp", &timestamp)
        .append_pair("sign", &signature)
        .append_pair("passphrase", &encrypted_passphrase)
        .append_pair("enable_ns", "true");
    url.to_string()
}

fn init_lane(api_key: &str, api_secret: &str, passphrase: &str) -> &'static OrderLane {
    ORDER_LANE.get_or_init(|| {
        let credentials = Credentials {
            api_key: api_key.to_owned(),
            api_secret: api_secret.to_owned(),
            passphrase: passphrase.to_owned(),
        };
        let (tx, rx) = mpsc::channel(256);
        let (ready_tx, ready_rx) = watch::channel(false);
        tokio::spawn(order_loop(rx, ready_tx, credentials));
        OrderLane {
            tx,
            ready: ready_rx,
        }
    })
}

/// Start authentication before the first opportunity reaches the executor.
pub fn prewarm(api_key: &str, api_secret: &str, passphrase: &str) {
    let _ = init_lane(api_key, api_secret, passphrase);
}

pub fn is_ready() -> bool {
    ORDER_LANE.get().is_some_and(|lane| *lane.ready.borrow())
}

/// Wait until startup authentication has completed.  This prevents a first
/// trade from spending its latency budget waiting for a TLS handshake.
pub async fn wait_ready(
    api_key: &str,
    api_secret: &str,
    passphrase: &str,
    timeout: Duration,
) -> Result<(), String> {
    let mut ready = init_lane(api_key, api_secret, passphrase).ready.clone();
    if *ready.borrow() {
        return Ok(());
    }
    tokio::time::timeout(timeout, async move {
        loop {
            ready
                .changed()
                .await
                .map_err(|_| "KuCoin Pro WS readiness channel closed".to_string())?;
            if *ready.borrow() {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|_| "KuCoin Pro order WS did not become ready before timeout".to_string())?
}

async fn authenticate(
    credentials: &Credentials,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    String,
> {
    let url = authenticated_url(credentials, crate::engine::real::now_ms());
    // Race all resolved addresses and disable Nagle. This is paid only on
    // startup/reconnect; the authenticated socket stays hot for order traffic.
    let addresses = tokio::net::lookup_host(("wsapi.kucoin.com", 443))
        .await
        .map_err(|error| format!("KuCoin Pro WS DNS lookup failed: {error}"))?;
    let mut attempts = FuturesUnordered::new();
    for address in addresses {
        let url = url.clone();
        attempts.push(async move {
            let socket = tokio::time::timeout(
                Duration::from_secs(4),
                tokio::net::TcpStream::connect(address),
            )
            .await
            .map_err(|_| format!("TCP timeout via {address}"))?
            .map_err(|error| format!("TCP failure via {address}: {error}"))?;
            socket
                .set_nodelay(true)
                .map_err(|error| format!("TCP_NODELAY failed via {address}: {error}"))?;
            tokio::time::timeout(Duration::from_secs(6), client_async_tls(&url, socket))
                .await
                .map_err(|_| format!("TLS/WebSocket timeout via {address}"))?
                .map_err(|error| format!("TLS/WebSocket failure via {address}: {error}"))
        });
    }
    let mut failures = Vec::new();
    let mut connected = None;
    while let Some(attempt) = attempts.next().await {
        match attempt {
            Ok(connection) => {
                connected = Some(connection);
                break;
            }
            Err(error) => failures.push(error),
        }
    }
    let (mut ws, _) = connected.ok_or_else(|| {
        format!(
            "all KuCoin Pro WS addresses failed: {}",
            failures.join("; ")
        )
    })?;

    // KuCoin requires the signature of the exact JSON bytes it sends here,
    // not a parsed-and-reserialized equivalent.
    let challenge = match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
        Ok(Some(Ok(message))) => json_frame_bytes(message, "auth challenge")?,
        Ok(Some(Err(error))) => return Err(format!("KuCoin Pro WS auth read failed: {error}")),
        Ok(None) => return Err("KuCoin Pro WS closed before authentication".into()),
        Err(_) => return Err("KuCoin Pro WS auth challenge timeout".into()),
    };
    let challenge_json: Value = serde_json::from_slice(&challenge)
        .map_err(|error| format!("invalid KuCoin Pro WS auth challenge: {error}"))?;
    if challenge_json["sessionId"].as_str().is_none()
        || challenge_json["timestamp"].as_i64().is_none()
    {
        return Err("KuCoin Pro WS auth challenge is missing sessionId/timestamp".into());
    }
    let challenge_text = std::str::from_utf8(&challenge)
        .map_err(|error| format!("KuCoin Pro WS auth challenge is not UTF-8: {error}"))?;
    let auth_signature = hmac_b64(&credentials.api_secret, challenge_text);
    tokio::time::timeout(
        Duration::from_secs(1),
        ws.send(Message::Text(auth_signature)),
    )
    .await
    .map_err(|_| "KuCoin Pro WS auth send timeout".to_string())?
    .map_err(|error| format!("KuCoin Pro WS auth send failed: {error}"))?;

    let welcome = match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
        Ok(Some(Ok(message))) => {
            let bytes = json_frame_bytes(message, "welcome")?;
            serde_json::from_slice::<Value>(&bytes)
                .map_err(|error| format!("invalid KuCoin Pro WS welcome: {error}"))?
        }
        Ok(Some(Err(error))) => return Err(format!("KuCoin Pro WS welcome read failed: {error}")),
        Ok(None) => return Err("KuCoin Pro WS closed during authentication".into()),
        Err(_) => return Err("KuCoin Pro WS welcome timeout".into()),
    };
    if welcome["data"].as_str() != Some("welcome")
        || welcome["sessionId"] != challenge_json["sessionId"]
    {
        return Err(format!("KuCoin Pro WS authentication rejected: {welcome}"));
    }
    Ok(ws)
}

async fn order_loop(
    mut rx: mpsc::Receiver<QueuedRequest>,
    ready: watch::Sender<bool>,
    credentials: Credentials,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let mut ws = match authenticate(&credentials).await {
            Ok(ws) => ws,
            Err(error) => {
                let _ = ready.send(false);
                tracing::error!("[kucoin_order_ws] {error}; reconnecting in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };

        backoff = Duration::from_secs(1);
        let _ = ready.send(true);
        tracing::info!("[kucoin_order_ws] authenticated persistent futures order channel");
        let mut pending: HashMap<String, ResponseSender> = HashMap::new();
        let mut keepalive = tokio::time::interval(Duration::from_secs(15));
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                incoming = ws.next() => {
                    match incoming {
                        Some(Ok(message @ (Message::Text(_) | Message::Binary(_)))) => {
                            let bytes = match json_frame_bytes(message, "response") {
                                Ok(bytes) => bytes,
                                Err(error) => {
                                    tracing::warn!("[kucoin_order_ws] {error}");
                                    continue;
                                }
                            };
                            match serde_json::from_slice::<Value>(&bytes) {
                                Ok(response) => {
                                    if let Some(id) = response["id"].as_str() {
                                        if let Some(sender) = pending.remove(id) {
                                            let _ = sender.send(response);
                                        }
                                    }
                                }
                                Err(error) => tracing::warn!("[kucoin_order_ws] invalid response: {error}"),
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            if !matches!(
                                tokio::time::timeout(Duration::from_secs(1), ws.send(Message::Pong(payload))).await,
                                Ok(Ok(()))
                            ) {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            tracing::warn!("[kucoin_order_ws] server closed connection: {frame:?}");
                            break;
                        }
                        Some(Err(error)) => {
                            tracing::warn!("[kucoin_order_ws] read failed: {error}");
                            break;
                        }
                        None => break,
                        _ => {}
                    }
                }
                queued = rx.recv() => {
                    let QueuedRequest { id, payload, enqueued, response: sender } = match queued {
                        Some(queued) => queued,
                        None => return,
                    };
                    if sender.is_closed() {
                        continue;
                    }
                    pending.insert(id, sender);
                    if !matches!(
                        tokio::time::timeout(Duration::from_millis(500), ws.send(Message::Text(payload))).await,
                        Ok(Ok(()))
                    ) {
                        break;
                    }
                    tracing::info!(
                        "[kucoin_order_ws] socket queue_delay_us={}",
                        enqueued.elapsed().as_micros(),
                    );
                }
                _ = keepalive.tick() => {
                    pending.retain(|_, sender| !sender.is_closed());
                    let ping = json!({
                        "id": format!("p{}", crate::engine::real::now_ms()),
                        "op": "ping",
                        "timestamp": crate::engine::real::now_ms(),
                    });
                    if !matches!(
                        tokio::time::timeout(Duration::from_millis(500), ws.send(Message::Text(ping.to_string()))).await,
                        Ok(Ok(()))
                    ) {
                        break;
                    }
                }
            }
        }

        // Dropping the response senders marks every in-flight order outcome as
        // unknown.  Requests are never replayed after reconnect.
        drop(pending);
        let _ = ready.send(false);
        tracing::warn!("[kucoin_order_ws] order channel disconnected; reconnecting");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub async fn send_order(
    api_key: &str,
    api_secret: &str,
    passphrase: &str,
    args: Value,
    timeout_ms: u64,
) -> Result<Value, String> {
    if !args.is_object() || args["clientOid"].as_str().is_none() {
        return Err("KuCoin WS order args must contain clientOid".into());
    }
    let lane = init_lane(api_key, api_secret, passphrase);
    if !*lane.ready.borrow() {
        return Err("KuCoin Pro order WS is not authenticated".into());
    }
    let request_id = Uuid::new_v4().simple().to_string();
    let request = json!({
        "id": request_id,
        "op": "futures.order",
        "args": args,
    });
    // Serialize in the caller before handing off to the dedicated socket task;
    // the actor's critical section now only updates the response map and writes.
    let payload = request.to_string();
    let (response_tx, response_rx) = oneshot::channel();
    let started = std::time::Instant::now();
    lane.tx
        .try_send(QueuedRequest {
            id: request_id,
            payload,
            enqueued: started,
            response: response_tx,
        })
        .map_err(|error| format!("KuCoin Pro order WS queue unavailable: {error}"))?;
    match tokio::time::timeout(Duration::from_millis(timeout_ms), response_rx).await {
        Ok(Ok(response)) => {
            let exchange_ns = response["outTime"]
                .as_i64()
                .zip(response["inTime"].as_i64())
                .map(|(out_time, in_time)| out_time.saturating_sub(in_time));
            tracing::info!(
                "[kucoin_order_ws] response client_rtt_us={} exchange_processing_ns={:?}",
                started.elapsed().as_micros(),
                exchange_ns,
            );
            Ok(response)
        }
        Ok(Err(_)) => Err("KuCoin Pro order WS disconnected with outcome unknown".into()),
        Err(_) => Err("KuCoin Pro order WS response timeout; outcome unknown".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pro_json_frames_accept_text_and_binary_without_changing_bytes() {
        let payload = br#"{"sessionId":"s","timestamp":123}"#;
        assert_eq!(
            json_frame_bytes(
                Message::Text(String::from_utf8(payload.to_vec()).unwrap().into()),
                "test",
            )
            .unwrap(),
            payload,
        );
        assert_eq!(
            json_frame_bytes(Message::Binary(payload.to_vec().into()), "test").unwrap(),
            payload,
        );
    }

    #[test]
    fn url_credentials_are_encoded_and_signature_uses_key_plus_timestamp() {
        let credentials = Credentials {
            api_key: "key/+".into(),
            api_secret: "secret".into(),
            passphrase: "pass/+".into(),
        };
        let url = authenticated_url(&credentials, 1_234);
        let parsed = reqwest::Url::parse(&url).unwrap();
        let query: HashMap<_, _> = parsed.query_pairs().into_owned().collect();
        assert_eq!(query.get("apikey").map(String::as_str), Some("key/+"));
        assert_eq!(
            query.get("sign").map(String::as_str),
            Some(hmac_b64("secret", "key/+1234").as_str())
        );
        assert_eq!(
            query.get("passphrase").map(String::as_str),
            Some(hmac_b64("secret", "pass/+").as_str())
        );
        assert_eq!(query.get("enable_ns").map(String::as_str), Some("true"));
    }

    #[test]
    fn hmac_matches_rfc_4231_sha256_vector() {
        assert_eq!(
            hmac_b64(&String::from_utf8(vec![0x0b; 20]).unwrap(), "Hi There"),
            "sDRMYdjbOFNcqK/OrwvxK4gdwgDJgz2nJuk3bC4yz/c="
        );
    }
}
