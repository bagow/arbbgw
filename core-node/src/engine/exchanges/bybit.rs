
use reqwest::Client;
use async_trait::async_trait;

use serde_json::Value;
use std::time::Duration;
use serde_json::json;


use crate::engine::real::{ExchangeKeys, now_ms, bybit_sign};
use super::traits::ExchangeConnector;

pub struct BybitConnector;

#[async_trait]
impl ExchangeConnector for BybitConnector {
    fn name(&self) -> &'static str {
        "bybit"
    }

    async fn place_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: f64, dry_run: bool, price: f64, use_ioc: bool, _leverage: f64) -> Result<(Option<String>, f64), String> {
        let ts = now_ms().to_string();
        let recv = "5000";
        let side_str = if is_buy { "Buy" } else { "Sell" };

        let mut qty_str = format!("{:.8}", contracts).trim_end_matches('0').to_string();
        if qty_str.ends_with('.') { qty_str.pop(); }
        if qty_str.is_empty() { qty_str = "0".to_string(); }

        let mut price_str = format!("{:.8}", price).trim_end_matches('0').to_string();
        if price_str.ends_with('.') { price_str.pop(); }
        if price_str.is_empty() { price_str = "0".to_string(); }

        let body = if use_ioc && price > 0.0 {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Limit",
                "qty": qty_str,
                "price": price_str,
                "timeInForce": "IOC",
                "reduceOnly": false
            })
        } else {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Market",
                "qty": qty_str,
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
            Ok((order_id, 0.0)) // Note: limit price returned, actual fill price requires fetch
        } else {
            Err("Bybit failed to return orderId".to_string())
        }
    }

    async fn close_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: f64, dry_run: bool, leverage: f64, price: f64, use_ioc: bool) -> Result<String, String> {
        let ts = now_ms().to_string();
        let recv = "5000";
        let side_str = if is_buy { "Buy" } else { "Sell" };

        let mut qty_str = format!("{:.8}", contracts).trim_end_matches('0').to_string();
        if qty_str.ends_with('.') { qty_str.pop(); }
        if qty_str.is_empty() { qty_str = "0".to_string(); }

        let mut price_str = format!("{:.8}", price).trim_end_matches('0').to_string();
        if price_str.ends_with('.') { price_str.pop(); }
        if price_str.is_empty() { price_str = "0".to_string(); }

        let body = if use_ioc && price > 0.0 {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Limit",
                "qty": qty_str,
                "price": price_str,
                "timeInForce": "IOC",
                "reduceOnly": true
            })
        } else {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Market",
                "qty": qty_str,
                "reduceOnly": true
            })
        };
        
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

    async fn fetch_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, order_id: &str, multiplier: f64) -> Result<(f64, f64, f64), String> {
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

        // Check order status — if Cancelled/Rejected/Deactivated, no fill
        let status = order["orderStatus"].as_str().unwrap_or("");
        if status == "Cancelled" || status == "Rejected" || status == "Deactivated" {
            tracing::warn!("[bybit] fetch_order {} status={} — definitively unfilled", order_id, status);
            return Ok((0.0, 0.0, 0.0));
        }

        // Check cumExecQty — if 0, order didn't fill
        let exec_qty: f64 = order["cumExecQty"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        if exec_qty < 1e-12 {
            return Ok((0.0, 0.0, 0.0));
        }

        let price = order["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let fee = order["cumExecFee"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        
        let exec_contracts = exec_qty / multiplier.max(1e-12);
        Ok((price, fee.abs(), exec_contracts))
    }

    async fn fetch_balances(&self, http: &Client, keys: &ExchangeKeys) -> Result<(f64, f64), String> {
        let ts = now_ms().to_string();
        let recv = "5000";
        
        // 1. Try UNIFIED (Unified Trading Account)
        let qs1 = "accountType=UNIFIED";
        let sig1 = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs1);
        
        let resp1 = http.get(format!("https://api.bybit.com/v5/account/wallet-balance?{}", qs1))
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig1)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        let mut v: Value = resp1.json().await.map_err(|e| e.to_string())?;
        
        // If the account is a Classic/Standard account, retCode will be 10001 or list empty
        let ret_code = v["retCode"].as_i64().unwrap_or(-1);
        if ret_code != 0 || v["result"]["list"].as_array().map(|a| a.is_empty()).unwrap_or(true) {
            // 2. Fallback to CONTRACT (Classic Account)
            let qs2 = "accountType=CONTRACT";
            let sig2 = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs2);
            
            let resp2 = http.get(format!("https://api.bybit.com/v5/account/wallet-balance?{}", qs2))
                .header("X-BAPI-API-KEY", &keys.bybit_key)
                .header("X-BAPI-SIGN", &sig2)
                .header("X-BAPI-TIMESTAMP", &ts)
                .header("X-BAPI-RECV-WINDOW", recv)
                .timeout(Duration::from_secs(10))
                .send().await.map_err(|e| e.to_string())?;
                
            v = resp2.json().await.map_err(|e| e.to_string())?;
        }
        
        let coin_obj = v["result"]["list"].as_array()
            .and_then(|arr| arr.first())
            .and_then(|a| a["coin"].as_array())
            .and_then(|coins| coins.iter().find(|c| c["coin"].as_str() == Some("USDT")));

        let wallet = coin_obj
            .and_then(|c| c["walletBalance"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);
            
        let equity = coin_obj
            .and_then(|c| c["equity"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(wallet);

        Ok((wallet, equity))
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
        // Only set leverage — do NOT force margin mode here.
        // Margin mode should be configured via warmup_leverages (real.rs) which has profile context.
        let ts = now_ms().to_string();
        let recv = "5000";
        let body = json!({
            "category": "linear",
            "symbol": pair,
            "buyLeverage": leverage.to_string(),
            "sellLeverage": leverage.to_string()
        });
        let body_str = body.to_string();
        let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &body_str);
        
        let _ = http.post("https://api.bybit.com/v5/position/set-leverage")
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .header("Content-Type", "application/json")
            .body(body_str)
            .timeout(Duration::from_secs(5))
            .send().await;
        Ok(())
    }

    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String> {
        // Bybit V5 tickers endpoint returns funding rate without authentication
        let url = format!("https://api.bybit.com/v5/market/tickers?category=linear&symbol={}", pair);
        if let Ok(resp) = http.get(&url).timeout(Duration::from_secs(5)).send().await {
            if let Ok(json) = resp.json::<Value>().await {
                if let Some(item) = json["result"]["list"].as_array().and_then(|a| a.first()) {
                    let rate = item["fundingRate"].as_str()
                        .and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                    let next_ts = item["nextFundingTime"].as_str()
                        .and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                    return Ok((rate, next_ts));
                }
            }
        }
        Err("Bybit funding rate fetch failed".to_string())
    }

    async fn fetch_positions(&self, http: &Client, keys: &ExchangeKeys) -> Result<Vec<super::traits::PositionInfo>, String> {
        let ts = now_ms().to_string();
        let recv = "5000";
        let qs = "category=linear&settleCoin=USDT";
        let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs);
        let resp = http.get(format!("https://api.bybit.com/v5/position/list?{}", qs))
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let mut positions = Vec::new();
        if let Some(arr) = v["result"]["list"].as_array() {
            for p in arr {
                let size: f64 = p["size"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if size.abs() < 1e-12 { continue; }
                let side = p["side"].as_str().unwrap_or("");
                let signed_size = if side == "Sell" { -size } else { size };
                positions.push(super::traits::PositionInfo {
                    symbol: p["symbol"].as_str().unwrap_or("").to_string(),
                    size: signed_size,
                    entry_price: p["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                    unrealized_pnl: p["unrealisedPnl"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                });
            }
        }
        Ok(positions)
    }
}
