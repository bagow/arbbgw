//! Gate futures API: authenticated session, correlated final responses, no replay.
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::OnceLock, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};
type ResponseSender = oneshot::Sender<Value>;
static GATE_WS_TX: OnceLock<mpsc::Sender<(Value, ResponseSender)>> = OnceLock::new();
fn login(key: &str, secret: &str, ts: i64) -> Value {
    use hmac::{Hmac, Mac};
    use sha2::Sha512;
    let mut mac = Hmac::<Sha512>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts arbitrary key length");
    mac.update(format!("api\nfutures.login\n\n{ts}").as_bytes());
    json!({"time":ts,"channel":"futures.login","event":"api","payload":{
        "req_id":"login","api_key":key,"signature":hex::encode(mac.finalize().into_bytes()),"timestamp":ts.to_string()}})
}
fn request_frame(req: &Value) -> Result<(String, Value), String> {
    let id = req["request_id"]
        .as_str()
        .ok_or("Missing Gate request identity")?
        .to_string();
    let channel = req["channel"].as_str().ok_or("Missing Gate channel")?;
    let raw = &req["payload"];
    let param = if raw.is_object() {
        raw.clone()
    } else if channel == "futures.order_place" {
        raw.as_array()
            .and_then(|a| a.first())
            .filter(|v| v.is_object())
            .cloned()
            .ok_or("Invalid Gate order")?
    } else if channel == "futures.order_cancel" || channel == "futures.order_status" {
        let a = raw.as_array().ok_or("Invalid Gate query")?;
        json!({"order_id":a.last().ok_or("Missing Gate order id")?})
    } else {
        return Err("Unsupported Gate API envelope".into());
    };
    let frame = json!({"time":crate::engine::utils::now_ms()/1000,"channel":channel,"event":"api","payload":{"req_id":id,"req_param":param}});
    Ok((id, frame))
}
fn response_id(v: &Value) -> Option<&str> {
    if v["ack"].as_bool() == Some(true) {
        return None;
    }
    v["request_id"]
        .as_str()
        .or_else(|| v["header"]["request_id"].as_str())
}
pub fn get_or_init_gate_ws(key: String, secret: String) -> mpsc::Sender<(Value, ResponseSender)> {
    GATE_WS_TX
        .get_or_init(|| {
            let (tx, rx) = mpsc::channel(64);
            tokio::spawn(run(rx, key, secret));
            tx
        })
        .clone()
}
async fn run(mut rx: mpsc::Receiver<(Value, ResponseSender)>, key: String, secret: String) {
    loop {
        if rx.is_closed() {
            return;
        }
        let connection = tokio::time::timeout(
            Duration::from_secs(10),
            connect_async("wss://fx-ws.gateio.ws/v4/ws/usdt"),
        )
        .await;
        let mut ws = match connection {
            Ok(Ok((ws, _))) => ws,
            _ => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        // Login must finish before any queued order can leave this process.
        let authenticated = tokio::time::timeout(Duration::from_secs(5), async {
            ws.send(Message::Text(
                login(&key, &secret, crate::engine::utils::now_ms() / 1000).to_string(),
            ))
            .await
            .ok()?;
            while let Some(msg) = ws.next().await {
                match msg.ok()? {
                    Message::Text(text) => {
                        let v: Value = serde_json::from_str(&text).ok()?;
                        if response_id(&v) == Some("login") {
                            return if v["header"]["status"].as_str() == Some("200") {
                                Some(())
                            } else {
                                None
                            };
                        }
                    }
                    Message::Ping(p) => ws.send(Message::Pong(p)).await.ok()?,
                    Message::Close(_) => return None,
                    _ => {}
                }
            }
            None
        })
        .await;
        if !matches!(authenticated, Ok(Some(()))) {
            tracing::warn!("[gate_ws] authentication not confirmed");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        crate::engine::utils::record_ws_connected("gate");
        let mut pending: HashMap<String, ResponseSender> = HashMap::new();
        let mut ping = tokio::time::interval(Duration::from_secs(10));
        let mut last_received = tokio::time::Instant::now();
        loop {
            tokio::select! {
                incoming=ws.next()=>{
                    last_received=tokio::time::Instant::now();
                    match incoming {
                        Some(Ok(Message::Text(text)))=>if let Ok(v)=serde_json::from_str::<Value>(&text){
                            if let Some(id)=response_id(&v){if let Some(sender)=pending.remove(id){let _=sender.send(v);}}
                        },
                        Some(Ok(Message::Ping(p)))=>{if !matches!(tokio::time::timeout(Duration::from_secs(1),ws.send(Message::Pong(p))).await,Ok(Ok(()))){break;}},
                        Some(Ok(Message::Close(_)))|Some(Err(_))|None=>break,
                        _=>{}
                    }
                },
                next=rx.recv()=>{
                    let(req,sender)=match next{Some(x)=>x,None=>return};
                    // A caller that timed out while disconnected must never send late.
                    if sender.is_closed(){continue;}
                    let(id,frame)=match request_frame(&req){Ok(x)=>x,Err(_)=>continue};
                    pending.insert(id,sender);
                    if !matches!(tokio::time::timeout(Duration::from_secs(1),ws.send(Message::Text(frame.to_string()))).await,Ok(Ok(()))){break;}
                },
                _=ping.tick()=>{
                    pending.retain(|_,s|!s.is_closed());
                    if last_received.elapsed()>Duration::from_secs(30){break;}
                    if !matches!(tokio::time::timeout(Duration::from_secs(1),ws.send(Message::Text(json!({"time":crate::engine::utils::now_ms()/1000,"channel":"futures.ping"}).to_string()))).await,Ok(Ok(()))){break;}
                }
            }
        }
        // Dropping pending wakes all callers with UNKNOWN, never resends requests.
        drop(pending);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
pub async fn send_gate_ws_request(req: Value) -> Result<Value, String> {
    let tx = GATE_WS_TX.get().ok_or("Gate WS trader not initialized")?;
    let (sender, receiver) = oneshot::channel();
    tx.try_send((req, sender))
        .map_err(|_| "Gate WS queue unavailable")?;
    match tokio::time::timeout(Duration::from_secs(2), receiver).await {
        Ok(Ok(v)) => Ok(v),
        _ => Err("Gate order outcome UNKNOWN; query its client identity".into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ack_does_not_consume_final_response() {
        assert_eq!(response_id(&json!({"request_id":"abc","ack":true})), None);
        assert_eq!(
            response_id(&json!({"request_id":"abc","ack":false})),
            Some("abc")
        );
    }
    #[test]
    fn order_is_wrapped_in_api_payload() {
        let(_,f)=request_frame(&json!({"request_id":"abc","channel":"futures.order_place","payload":[{"contract":"BTC_USDT","size":1}]})).unwrap();
        assert_eq!(f["payload"]["req_id"], "abc");
        assert_eq!(f["payload"]["req_param"]["size"], 1);
        assert!(f.get("auth").is_none());
    }
}
