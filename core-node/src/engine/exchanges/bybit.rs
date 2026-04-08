use std::sync::Arc;
use reqwest::Client;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use serde_json::json;
use uuid::Uuid;

use crate::engine::real::{ExchangeKeys, now_ms, bybit_sign};
use super::traits::ExchangeConnector;

pub struct BybitConnector;

#[async_trait]
impl ExchangeConnector for BybitConnector {
    fn name(&self) -> &'static str {
        "bybit"
    }

    async fn place_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: i64, dry_run: bool, price: f64, use_ioc: bool) -> Result<(Option<String>, f64), String> {
        let ts = now_ms().to_string();
        let recv = "5000";
        let side_str = if is_buy { "Buy" } else { "Sell" };

        let body = if use_ioc && price > 0.0 {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Limit",
                "qty": contracts.to_string(),
                "price": format!("{:.8}", price),
                "timeInForce": "IOC",
                "reduceOnly": false
            })
        } else {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Market",
                "qty": contracts.to_string(),
                "reduceOnly": false
            })
        };
        
        let body_str = body.to_string();
        
        if dry_run { return Ok((Some(format!("dry_{}_{}", self.name(), now_ms())), price)); }
        
        let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &body_str);
        
        let resp = http.post("https://api.bybit.com/v5/order/create")
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .header("Content-Type", "application/json")
            .body(body_str)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;

        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        if v["retCode"].as_i64() != Some(0) { 
            return Err(format!("Bybit API Error: {}", v)); 
        }

        let order_id = v["result"]["orderId"].as_str().map(|s| s.to_string());
        if order_id.is_some() {
            Ok((order_id, price)) // Note: limit price returned, actual fill price requires fetch
        } else {
            Err("Bybit failed to return orderId".to_string())
        }
    }

    async fn close_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: i64, dry_run: bool, leverage: f64) -> Result<String, String> {
        let ts = now_ms().to_string();
        let recv = "5000";
        let side_str = if is_buy { "Buy" } else { "Sell" };

        let body = json!({
            "category": "linear",
            "symbol": pair,
            "side": side_str,
            "orderType": "Market",
            "qty": contracts.to_string(),
            "reduceOnly": true
        });
        
        let body_str = body.to_string();

        if dry_run { return Ok(format!("dry_close_{}_{}", self.name(), now_ms())); }

        let _ = self.ensure_leverage(http, keys, pair, leverage).await;

        let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &body_str);
        
        let resp = http.post("https://api.bybit.com/v5/order/create")
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .header("Content-Type", "application/json")
            .body(body_str)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;

        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        if v["retCode"].as_i64() != Some(0) { 
            return Err(format!("Bybit close API Error: {}", v)); 
        }

        v["result"]["orderId"].as_str().map(|s| s.to_string())
            .ok_or_else(|| "Bybit return no orderId on close".to_string())
    }

    async fn fetch_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, order_id: &str) -> Result<(f64, f64), String> {
        let ts = now_ms().to_string();
        let recv = "5000";
        let qs = format!("category=linear&orderId={}", order_id);
        let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &qs);
        
        let resp = http.get(format!("https://api.bybit.com/v5/order/realtime?{}", qs))
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let order = &v["result"]["list"][0];
        let price = order["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let fee = order["cumExecFee"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        
        Ok((price, fee.abs()))
    }

    async fn fetch_available_balance(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        let ts = now_ms().to_string();
        let recv = "5000";
        let qs = "accountType=UNIFIED&coin=USDT";
        let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs);
        
        let resp = http.get(format!("https://api.bybit.com/v5/account/wallet-balance?{}", qs))
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let bal = v["result"]["list"].as_array()
            .and_then(|arr| arr.first())
            .and_then(|a| a["coin"].as_array())
            .and_then(|coins| coins.iter().find(|c| c["coin"].as_str() == Some("USDT")))
            .and_then(|c| c["walletBalance"].as_str().and_then(|s| s.parse().ok())
                .or_else(|| c["equity"].as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(0.0);
            
        Ok(bal)
    }

    async fn fetch_taker_fee(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        let ts = now_ms().to_string();
        let recv = "5000";
        let qs = "category=linear&symbol=BTCUSDT";
        let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs);
        
        let resp = http.get(format!("https://api.bybit.com/v5/account/fee-rate?{}", qs))
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let taker = v["result"]["list"].as_array()
            .and_then(|arr| arr.first())
            .and_then(|a| a["takerFeeRate"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.00055);
            
        Ok(taker)
    }

    async fn ensure_leverage(&self, http: &Client, keys: &ExchangeKeys, pair: &str, leverage: f64) -> Result<(), String> {
        let ts = now_ms().to_string();
        let recv = "5000";
        
        // 1. Switch ISOLATED / CROSS
        // For logic here we hardcode to CROSS (tradeMode: 0) as typical for Bybit
        let trade_mode = 0; // 0: cross, 1: isolated
        let body1 = json!({
            "category": "linear",
            "symbol": pair,
            "tradeMode": trade_mode,
            "buyLeverage": leverage.to_string(),
            "sellLeverage": leverage.to_string()
        });
        let body_str1 = body1.to_string();
        let sig1 = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &body_str1);
        
        let _ = http.post("https://api.bybit.com/v5/position/switch-isolated")
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig1)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .header("Content-Type", "application/json")
            .body(body_str1)
            .timeout(Duration::from_secs(5))
            .send().await;

        // 2. Set Leverage
        let ts2 = now_ms().to_string();
        let body2 = json!({
            "category": "linear",
            "symbol": pair,
            "buyLeverage": leverage.to_string(),
            "sellLeverage": leverage.to_string()
        });
        let body_str2 = body2.to_string();
        let sig2 = bybit_sign(&keys.bybit_secret, &ts2, &keys.bybit_key, recv, &body_str2);
        
        let _ = http.post("https://api.bybit.com/v5/position/set-leverage")
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig2)
            .header("X-BAPI-TIMESTAMP", &ts2)
            .header("X-BAPI-RECV-WINDOW", recv)
            .header("Content-Type", "application/json")
            .body(body_str2)
            .timeout(Duration::from_secs(5))
            .send().await;
            
        Ok(())
    }

    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String> {
        // Unfortunately standard premium index or tickers doesn't return directly for bybit without special auth or other endpoints. 
        // Returning 0 for Bybit or need to look up real one
        Ok((0.0, 0))
    }
}
