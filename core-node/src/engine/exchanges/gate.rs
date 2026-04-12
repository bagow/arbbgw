
use reqwest::Client;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use serde_json::json;
use uuid::Uuid;

use crate::engine::real::{ExchangeKeys, gate_sign};
use super::traits::ExchangeConnector;

pub struct GateConnector;

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct GateBalanceResponse {
    available: String,
    total: String,
}

#[derive(Deserialize, Debug)]
struct GateFeeResponse {
    taker_fee: String,
}

#[async_trait]
impl ExchangeConnector for GateConnector {
    fn name(&self) -> &'static str {
        "gate"
    }

    async fn place_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, phys_amount: f64, dry_run: bool, price: f64, use_ioc: bool, _leverage: f64, multiplier: f64) -> Result<(Option<String>, f64), String> {
        let contract = pair.replace("USDT", "_USDT");
        let size = if is_buy { (phys_amount / multiplier.max(1e-12)).round() as i64 } else { -((phys_amount / multiplier.max(1e-12)).round() as i64) };
        let path = "/api/v4/futures/usdt/orders";
        
        let mut raw_price_str = format!("{:.8}", price).trim_end_matches('0').to_string();
        if raw_price_str.ends_with('.') { raw_price_str.pop(); }
        if raw_price_str.is_empty() { raw_price_str = "0".to_string(); }
        let price_str = if use_ioc && price > 0.0 { raw_price_str } else { "0".to_string() };
        let tif = if use_ioc && price > 0.0 { "ioc" } else { "gtc" };
        
        let body = json!({
            "contract": contract,
            "size": size,
            "price": price_str,
            "tif": tif,
            "reduce_only": false,
            "close": false,
            "text": "t-corenode"
        });
        let body_str = body.to_string();

        if dry_run { return Ok((Some(format!("dry_{}", Uuid::new_v4())), 0.0)); }

        let resp = http.post(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(&keys.gate_key, &keys.gate_secret, "POST", path, &body_str))
            .body(body_str)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;

        let status = resp.status();
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        if !status.is_success() { 
            return Err(format!("Gate API Error: {}", v)); 
        }

        let order_id = v["id"].as_i64().map(|i| i.to_string()).or_else(|| v["id"].as_str().map(|s| s.to_string()));
        if order_id.is_some() {
            Ok((order_id, 0.0))
        } else {
            Err("Gate failed to return order ID".to_string())
        }
    }

    async fn close_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, phys_amount: f64, dry_run: bool, leverage: f64, price: f64, use_ioc: bool, multiplier: f64) -> Result<String, String> {
        let contract = pair.replace("USDT", "_USDT");
        let size = if multiplier == 0.0 {
            if is_buy { phys_amount.round() as i64 } else { -(phys_amount.round() as i64) }
        } else {
            if is_buy { (phys_amount / multiplier.max(1e-12)).round() as i64 } else { -((phys_amount / multiplier.max(1e-12)).round() as i64) }
        };
        let path = "/api/v4/futures/usdt/orders";
        
        let mut raw_price_str = format!("{:.8}", price).trim_end_matches('0').to_string();
        if raw_price_str.ends_with('.') { raw_price_str.pop(); }
        if raw_price_str.is_empty() { raw_price_str = "0".to_string(); }
        let price_str = if use_ioc && price > 0.0 { raw_price_str } else { "0".to_string() };
        let tif = if price_str == "0" || (use_ioc && price > 0.0) { "ioc" } else { "gtc" };
        
        let body = json!({
            "contract": contract,
            "size": size,
            "price": price_str,
            "tif": tif,
            "reduce_only": true,
            "close": false,
            "text": "t-corenode-close"
        });
        let body_str = body.to_string();

        if dry_run { return Ok(format!("dry_close_{}", Uuid::new_v4())); }

        let _ = self.ensure_leverage(http, keys, pair, leverage).await;

        let resp = http.post(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(&keys.gate_key, &keys.gate_secret, "POST", path, &body_str))
            .body(body_str)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;

        let status = resp.status();
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        if !status.is_success() { 
            return Err(format!("Gate API Close Error: {}", v)); 
        }

        v["id"].as_i64().map(|i| i.to_string()).or_else(|| v["id"].as_str().map(|s| s.to_string()))
            .ok_or_else(|| "Gate failed to return order ID on close".to_string())
    }

    async fn fetch_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, order_id: &str, multiplier: f64) -> Result<(f64, f64, f64), String> {
        let path = format!("/api/v4/futures/usdt/orders/{}", order_id);
        let resp = http.get(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(&keys.gate_key, &keys.gate_secret, "GET", &path, ""))
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let price = v["fill_price"].as_f64().or_else(|| v["fill_price"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
        let mut fee = v["fee"].as_f64().or_else(|| v["fee"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
        
        let status = v["status"].as_str().unwrap_or("");
        if status == "finished" && price <= 0.0 {
            // Unfilled IOC
            return Ok((0.0, 0.0, 0.0));
        }

        let size = v["size"].as_f64().or_else(|| v["size"].as_i64().map(|i| i as f64)).unwrap_or(0.0).abs();
        let left = v["left"].as_f64().or_else(|| v["left"].as_i64().map(|i| i as f64)).unwrap_or(0.0).abs();
        let exec_contracts = (size - left).max(0.0);

        // Fetch real commission from my_trades endpoint since orders endpoint fee is often 0 or delayed
        if exec_contracts > 0.0 {
            let contract = pair.replace("USDT", "_USDT");
            let trades_path = format!("/api/v4/futures/usdt/my_trades?contract={}&order_id={}", contract, order_id);
            if let Ok(resp2) = http.get(format!("https://api.gateio.ws{}", trades_path))
                .headers(gate_sign(&keys.gate_key, &keys.gate_secret, "GET", &trades_path, ""))
                .timeout(Duration::from_secs(5))
                .send().await {
                if let Ok(v2) = resp2.json::<Value>().await {
                    if let Some(trades) = v2.as_array() {
                        let mut sum_fee = 0.0;
                        for t in trades {
                            let f = t["fee"].as_f64()
                                .or_else(|| t["fee"].as_str().and_then(|s| s.parse().ok()))
                                .unwrap_or(0.0);
                            sum_fee += f;
                        }
                        if sum_fee.abs() > 1e-9 {
                            fee = sum_fee;
                        }
                    }
                }
            }
        }

        Ok((price, fee.abs(), exec_contracts * multiplier.max(1e-12)))
    }

    async fn fetch_balances(&self, http: &Client, keys: &ExchangeKeys) -> Result<(f64, f64), String> {
        let path = "/api/v4/futures/usdt/accounts";
        let resp = http.get(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(&keys.gate_key, &keys.gate_secret, "GET", path, ""))
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        let data: GateBalanceResponse = resp.json().await.map_err(|e| format!("Gate balance parse err: {}", e))?;
        let avail = data.available.parse::<f64>().map_err(|_| "Failed to parse Gate available balance as f64".to_string())?;
        let total = data.total.parse::<f64>().unwrap_or(0.0);
        Ok((total, avail))
    }

    async fn fetch_taker_fee(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        let path = "/api/v4/futures/usdt/fee";
        let resp = http.get(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(&keys.gate_key, &keys.gate_secret, "GET", path, ""))
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;

        let data: GateFeeResponse = resp.json().await.map_err(|e| format!("Gate fee parse err: {}", e))?;
        data.taker_fee.parse::<f64>().map_err(|_| "Failed to parse Gate fee".to_string())
    }

    async fn ensure_leverage(&self, http: &Client, keys: &ExchangeKeys, pair: &str, leverage: f64) -> Result<(), String> {
        let contract = pair.replace("USDT", "_USDT");
        let path = format!("/api/v4/futures/usdt/positions/{}?leverage={}", contract, leverage as i64);
        let _ = http.post(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(&keys.gate_key, &keys.gate_secret, "POST", &path, ""))
            .timeout(Duration::from_secs(10))
            .send().await;
        Ok(())
    }

    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String> {
        let contract = pair.replace("USDT", "_USDT");
        let url = format!("https://api.gateio.ws/api/v4/futures/usdt/contracts/{}", contract);
        if let Ok(resp) = http.get(&url).timeout(Duration::from_secs(5)).send().await {
            if let Ok(json) = resp.json::<Value>().await {
                if let Some(rate_str) = json["funding_rate"].as_str().or_else(|| json["funding_rate_indicative"].as_str()) {
                    if let Ok(rate) = rate_str.parse::<f64>() {
                        let next_ts = json["funding_next_apply"].as_i64().unwrap_or(0) * 1000;
                        return Ok((rate, next_ts));
                    }
                }
            }
        }
        Err("Gate funding rate fetch failed".to_string())
    }

    async fn fetch_positions(&self, http: &Client, keys: &ExchangeKeys) -> Result<Vec<super::traits::PositionInfo>, String> {
        let path = "/api/v4/futures/usdt/positions";
        let resp = http.get(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(&keys.gate_key, &keys.gate_secret, "GET", path, ""))
            .timeout(std::time::Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        if !resp.status().is_success() {
            let err_txt = resp.text().await.unwrap_or_default();
            return Err(format!("Gate position API error: {}", err_txt));
        }
            
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let mut positions = Vec::new();
        if let Some(arr) = v.as_array() {
            for p in arr {
                let size: f64 = p["size"].as_i64().unwrap_or(0) as f64;
                if size.abs() < 1e-12 { continue; }
                // Gate symbol: "BTC_USDT" → "BTCUSDT"
                let contract = p["contract"].as_str().unwrap_or("").replace("_", "");
                positions.push(super::traits::PositionInfo {
                    symbol: contract,
                    size,
                    entry_price: p["entry_price"].as_str().and_then(|s| s.parse().ok())
                        .or_else(|| p["entry_price"].as_f64()).unwrap_or(0.0),
                    unrealized_pnl: p["unrealised_pnl"].as_str().and_then(|s| s.parse().ok())
                        .or_else(|| p["unrealised_pnl"].as_f64()).unwrap_or(0.0),
                });
            }
        }
        Ok(positions)
    }
}
