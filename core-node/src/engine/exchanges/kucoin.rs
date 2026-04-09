use std::sync::Arc;
use reqwest::Client;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use serde_json::json;
use uuid::Uuid;

use crate::engine::real::{ExchangeKeys, now_ms, kucoin_sign};
use super::traits::ExchangeConnector;
use crate::state::EngineContext;

pub struct KucoinConnector;

#[async_trait]
impl ExchangeConnector for KucoinConnector {
    fn name(&self) -> &'static str {
        "kucoin"
    }

    async fn place_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: f64, dry_run: bool, price: f64, use_ioc: bool) -> Result<(Option<String>, f64), String> {
        let sym = format!("{}M", pair);
        let ts = now_ms().to_string(); 
        let path = "/api/v1/orders";
        let side = if is_buy { "buy" } else { "sell" };
        
        let lev_str = "10"; // Default static, KuCoin requires leverage in order payload

        let mut body_json = if use_ioc && price > 0.0 {
            json!({
                "clientOid": Uuid::new_v4().to_string(),
                "symbol": sym,
                "side": side,
                "type": "limit",
                "timeInForce": "IOC",
                "price": format!("{:.8}", price),
                "size": contracts.round() as i64,
                "leverage": lev_str,
                "marginMode": "CROSS",
                "reduceOnly": false
            })
        } else {
            json!({
                "clientOid": Uuid::new_v4().to_string(),
                "symbol": sym,
                "side": side,
                "type": "market",
                "size": contracts.round() as i64,
                "leverage": lev_str,
                "marginMode": "CROSS",
                "reduceOnly": false
            })
        };

        let body_str = body_json.to_string();

        if dry_run { return Ok((Some(format!("dry_{}_{}", self.name(), now_ms())), price)); }

        let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "POST", path, &body_str);

        let resp = http.post(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig)
            .header("KC-API-TIMESTAMP", &ts)
            .header("KC-API-PASSPHRASE", pass_sig)
            .header("KC-API-KEY-VERSION", "2")
            .header("Content-Type", "application/json")
            .body(body_str)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;

        let status = resp.status();
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        
        if v["code"].as_str() != Some("200000") {
            return Err(format!("KuCoin API Error: {}", v));
        }

        let order_id = v["data"]["orderId"].as_str().map(|s| s.to_string());
        if order_id.is_some() {
            Ok((order_id, price))
        } else {
            Err("KuCoin failed to return orderId".to_string())
        }
    }

    async fn close_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: f64, dry_run: bool, leverage: f64) -> Result<String, String> {
        let sym = format!("{}M", pair);
        let ts = now_ms().to_string(); 
        let path = "/api/v1/orders";
        let side = if is_buy { "buy" } else { "sell" };

        let lev_str = "10";
        let body_json = json!({
            "clientOid": Uuid::new_v4().to_string(),
            "symbol": sym,
            "side": side,
            "type": "market",
            "size": contracts.round() as i64,
            "leverage": lev_str,
            "marginMode": "CROSS",
            "reduceOnly": true
        });

        let body_str = body_json.to_string();

        if dry_run { return Ok(format!("dry_close_{}_{}", self.name(), now_ms())); }

        let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "POST", path, &body_str);

        let resp = http.post(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig)
            .header("KC-API-TIMESTAMP", &ts)
            .header("KC-API-PASSPHRASE", pass_sig)
            .header("KC-API-KEY-VERSION", "2")
            .header("Content-Type", "application/json")
            .body(body_str)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;

        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        
        if v["code"].as_str() != Some("200000") {
            return Err(format!("KuCoin Close API Error: {}", v));
        }

        v["data"]["orderId"].as_str().map(|s| s.to_string())
            .ok_or_else(|| "KuCoin failed to return orderId on close".to_string())
    }

    async fn fetch_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, order_id: &str) -> Result<(f64, f64), String> {
        let ts = now_ms().to_string();
        let path = format!("/api/v1/orders/{}", order_id);
        let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "GET", &path, "");
        
        let resp = http.get(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig)
            .header("KC-API-TIMESTAMP", &ts)
            .header("KC-API-PASSPHRASE", pass_sig)
            .header("KC-API-KEY-VERSION", "2")
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;

        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        
        let size = v["data"]["dealSize"].as_f64().or_else(|| v["data"]["filledSize"].as_f64()).or_else(|| v["data"]["dealSize"].as_str().and_then(|s| s.parse().ok())).or_else(|| v["data"]["filledSize"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
        let val  = v["data"]["dealValue"].as_f64().or_else(|| v["data"]["filledValue"].as_f64()).or_else(|| v["data"]["dealValue"].as_str().and_then(|s| s.parse().ok())).or_else(|| v["data"]["filledValue"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
        let fee  = v["data"]["fee"].as_f64().or_else(|| v["data"]["fee"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
        
        // Note: KuCoin multiplier is usually fetched dynamically, assuming 1.0 here for simple port
        let mult = 1.0; 
        
        let price = if size > 0.0 && mult > 0.0 { val / (size * mult) } else { 0.0 };
        Ok((price, fee))
    }

    async fn fetch_available_balance(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        let ts = now_ms().to_string();
        let path = "/api/v1/account-overview?currency=USDT";
        let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "GET", path, "");
        
        let resp = http.get(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig)
            .header("KC-API-TIMESTAMP", &ts)
            .header("KC-API-PASSPHRASE", pass_sig)
            .header("KC-API-KEY-VERSION", "2")
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let bal = v["data"]["availableBalance"].as_f64()
            .or_else(|| v["data"]["availableBalance"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);
            
        Ok(bal)
    }

    async fn fetch_taker_fee(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        let ts = now_ms().to_string();
        let path = "/api/v1/trade-fees?symbol=XBTUSDTM";
        let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "GET", path, "");
        
        let resp = http.get(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig)
            .header("KC-API-TIMESTAMP", &ts)
            .header("KC-API-PASSPHRASE", pass_sig)
            .header("KC-API-KEY-VERSION", "2")
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let taker = v["data"]["takerFeeRate"].as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| v["data"]["takerFeeRate"].as_f64())
            .unwrap_or(0.0006);
            
        Ok(taker)
    }

    async fn ensure_leverage(&self, http: &Client, keys: &ExchangeKeys, pair: &str, leverage: f64) -> Result<(), String> {
        // KuCoin leverage is set directly in the order payload.
        Ok(())
    }

    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String> {
        let sym = format!("{}M", pair);
        let url = format!("https://api-futures.kucoin.com/api/v1/funding-rate/{}/current", sym);
        if let Ok(resp) = http.get(&url).timeout(Duration::from_secs(5)).send().await {
            if let Ok(json) = resp.json::<Value>().await {
                if let Some(rate) = json["data"]["value"].as_f64() {
                    let next = json["data"]["timePoint"].as_i64().unwrap_or(0) + 8 * 3600 * 1000;
                    return Ok((rate, next));
                }
            }
        }
        Err("KuCoin funding rate fetch failed".to_string())
    }
}
