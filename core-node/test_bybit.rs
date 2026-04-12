use reqwest::Client;
use serde_json::Value;
use std::env;
use hmac::{Hmac, Mac};
use sha2::Sha256;

#[tokio::main]
async fn main() {
    let key = env::var("BYBIT_KEY").unwrap_or_default();
    let secret = env::var("BYBIT_SECRET").unwrap_or_default();
    if key.is_empty() { println!("NO BYBIT KEY"); return; }
    
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis().to_string();
    let recv = "10000";
    let qs1 = "accountType=UNIFIED";
    
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{}{}{}{}", ts, key, recv, qs1).as_bytes());
    let sig1 = hex::encode(mac.finalize().into_bytes());

    let http = Client::new();
    let resp = http.get(format!("https://api.bybit.com/v5/account/wallet-balance?{}", qs1))
        .header("X-BAPI-API-KEY", &key)
        .header("X-BAPI-SIGN", &sig1)
        .header("X-BAPI-TIMESTAMP", &ts)
        .header("X-BAPI-RECV-WINDOW", recv)
        .send().await.unwrap();
    let json: Value = resp.json().await.unwrap();
    println!("UNIFIED: {}", serde_json::to_string_pretty(&json).unwrap());

    // Also CONTRACT
    let qs2 = "accountType=CONTRACT";
    let mut mac2 = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac2.update(format!("{}{}{}{}", ts, key, recv, qs2).as_bytes());
    let sig2 = hex::encode(mac2.finalize().into_bytes());
    let resp2 = http.get(format!("https://api.bybit.com/v5/account/wallet-balance?{}", qs2))
        .header("X-BAPI-API-KEY", &key)
        .header("X-BAPI-SIGN", &sig2)
        .header("X-BAPI-TIMESTAMP", &ts)
        .header("X-BAPI-RECV-WINDOW", recv)
        .send().await.unwrap();
    let json2: Value = resp2.json().await.unwrap();
    println!("CONTRACT: {}", serde_json::to_string_pretty(&json2).unwrap());
}
