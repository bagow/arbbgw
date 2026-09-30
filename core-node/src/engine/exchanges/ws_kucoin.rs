//! KuCoin Futures REST reconciliation/bootstrap transport.
//!
//! Live order submission belongs in `ws_kucoin_order`; this module must never
//! be used as a fallback for placing an order because an ambiguous WebSocket
//! result cannot be safely replayed.  These calls are retained for snapshots,
//! ledger reconciliation and the Classic private-stream bootstrap token.
//!
//! The connector calls `send_kucoin_request()` which handles:
//! - HMAC-SHA256 signing (KC-API-SIGN)
//! - Passphrase signing (KC-API-KEY-VERSION=2)
//! - Request execution via internal HTTP client

use serde_json::Value;
use std::sync::OnceLock;
use std::time::Duration;

static KUCOIN_HTTP: OnceLock<reqwest::Client> = OnceLock::new();

fn get_http() -> &'static reqwest::Client {
    KUCOIN_HTTP.get_or_init(|| {
        reqwest::Client::builder()
            .tcp_nodelay(true)
            .pool_idle_timeout(Some(Duration::from_secs(90)))
            .pool_max_idle_per_host(20)
            .timeout(Duration::from_millis(1500))
            .build()
            .unwrap()
    })
}

fn kucoin_sign_internal(secret: &str, timestamp: &str, method: &str, endpoint: &str, body: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let msg = format!("{}{}{}{}", timestamp, method, endpoint, body);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(msg.as_bytes());
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

fn sign_passphrase(secret: &str, passphrase: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(passphrase.as_bytes());
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

pub async fn send_kucoin_request(
    api_key: &str,
    api_secret: &str,
    api_passphrase: &str,
    method: &str,  // "GET" or "POST"
    path: &str,     // e.g. "/api/v1/orders"
    body: &str,     // "" for GET, JSON string for POST
    timeout_ms: u64,
) -> Result<Value, String> {
    let http = get_http();
    let ts = crate::engine::real::now_ms().to_string();
    let sig = kucoin_sign_internal(api_secret, &ts, method, path, body);
    let pass_sig = sign_passphrase(api_secret, api_passphrase);

    let url = format!("https://api-futures.kucoin.com{}", path);
    let req = match method {
        "POST" => http.post(&url).body(body.to_string()),
        _ => http.get(&url),
    };

    let resp = req
        .header("KC-API-KEY", api_key)
        .header("KC-API-SIGN", &sig)
        .header("KC-API-TIMESTAMP", &ts)
        .header("KC-API-PASSPHRASE", &pass_sig)
        .header("KC-API-KEY-VERSION", "2")
        .header("Content-Type", "application/json")
        .timeout(Duration::from_millis(timeout_ms))
        .send()
        .await
        .map_err(|e| format!("KuCoin request error: {}", e))?;

    resp.json::<Value>().await.map_err(|e| format!("KuCoin parse error: {}", e))
}

/// Public (unauthenticated) request
pub async fn send_kucoin_public(path: &str) -> Result<Value, String> {
    let http = get_http();
    let url = format!("https://api-futures.kucoin.com{}", path);
    let resp = http.get(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| format!("KuCoin public request error: {}", e))?;
    resp.json::<Value>().await.map_err(|e| format!("KuCoin public parse: {}", e))
}
