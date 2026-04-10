
use reqwest::Client;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value; // We will replace these with strict DTOs
use std::time::Duration;

use crate::engine::real::{ExchangeKeys, now_ms};
use super::traits::ExchangeConnector;
use crate::engine::real::binance_sign;

pub struct BinanceConnector;

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct BinanceBalanceAccountResponse {
    availableBalance: String,
    totalWalletBalance: String,
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct BinanceFeeResponse {
    takerCommissionRate: String,
}

#[async_trait]
impl ExchangeConnector for BinanceConnector {
    fn name(&self) -> &'static str {
        "binance"
    }

    async fn place_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: f64, dry_run: bool, price: f64, use_ioc: bool) -> Result<(Option<String>, f64), String> {
        let side = if is_buy { "BUY" } else { "SELL" };
        if dry_run {
            return Ok((Some(format!("dry_{}_{}", self.name(), now_ms())), price));
        }

        let ts = now_ms();
        let time_in_force = if use_ioc { "IOC" } else { "GTC" };
        let mut qty_str = format!("{:.8}", contracts).trim_end_matches('0').to_string();
        if qty_str.ends_with('.') { qty_str.pop(); }
        if qty_str.is_empty() { qty_str = "0".to_string(); }
        
        let mut price_str = format!("{:.8}", price).trim_end_matches('0').to_string();
        if price_str.ends_with('.') { price_str.pop(); }
        if price_str.is_empty() { price_str = "0".to_string(); }
        
        let qs = format!("symbol={}&side={}&type=LIMIT&timeInForce={}&quantity={}&price={}&timestamp={}", 
            pair, side, time_in_force, qty_str, price_str, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let url = format!("https://fapi.binance.com/fapi/v1/order?{}&signature={}", qs, sig);
        let resp = http.post(&url).header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(10)).send().await.map_err(|e| e.to_string())?;

        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        if let Some(msg) = v["msg"].as_str() {
            return Err(format!("Binance API Error: {}", msg));
        }

        let order_id = v["orderId"].as_i64().map(|id| id.to_string());
        if order_id.is_some() {
            Ok((order_id, 0.0))
        } else {
            Err("Binance failed to return orderId".to_string())
        }
    }

    async fn close_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: f64, dry_run: bool, leverage: f64, price: f64, use_ioc: bool) -> Result<String, String> {
        let side = if is_buy { "BUY" } else { "SELL" };
        if dry_run { return Ok(format!("dry_close_{}_{}", self.name(), now_ms())); }

        let ts = now_ms();
        let mut qty_str = format!("{:.8}", contracts).trim_end_matches('0').to_string();
        if qty_str.ends_with('.') { qty_str.pop(); }
        if qty_str.is_empty() { qty_str = "0".to_string(); }
        
        let mut price_str = format!("{:.8}", price).trim_end_matches('0').to_string();
        if price_str.ends_with('.') { price_str.pop(); }
        if price_str.is_empty() { price_str = "0".to_string(); }
        
        let type_str = if use_ioc && price > 0.0 { "LIMIT" } else { "MARKET" };
        let tif_str = if use_ioc && price > 0.0 { "&timeInForce=IOC" } else { "" };
        let price_param = if use_ioc && price > 0.0 { format!("&price={}", price_str) } else { "".to_string() };

        let qs = format!("symbol={}&side={}&type={}&quantity={}&reduceOnly=true{}{}&timestamp={}", pair, side, type_str, qty_str, tif_str, price_param, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let url = format!("https://fapi.binance.com/fapi/v1/order?{}&signature={}", qs, sig);
        
        // Ensure leverage before closing on Binance (sometimes needed if leverage drifted)
        let _ = self.ensure_leverage(http, keys, pair, leverage).await;

        let resp = http.post(&url).header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(10)).send().await.map_err(|e| e.to_string())?;
        
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        if let Some(msg) = v["msg"].as_str() {
            return Err(format!("Binance API Close Error: {}", msg));
        }

        v["orderId"].as_i64().map(|id| id.to_string())
            .ok_or_else(|| "Binance failed to return orderId on close".to_string())
    }

    async fn fetch_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, order_id: &str, multiplier: f64) -> Result<(f64, f64, f64), String> {
        if order_id.starts_with("dry_") { return Ok((0.0, 0.0, 0.0)); }
        let ts = now_ms();
        let qs = format!("symbol={}&orderId={}&timestamp={}", pair, order_id, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let url = format!("https://fapi.binance.com/fapi/v1/order?{}&signature={}", qs, sig);
        let resp = http.get(&url).header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(10)).send().await.map_err(|e| e.to_string())?;
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;

        // Check order status — if EXPIRED/CANCELED/REJECTED, no fill will ever come
        let status = v["status"].as_str().unwrap_or("");
        if status == "EXPIRED" || status == "CANCELED" || status == "REJECTED" {
            tracing::warn!("[binance] fetch_order {} status={} — definitively unfilled", order_id, status);
            return Ok((0.0, 0.0, 0.0));
        }

        // Check executedQty — if 0, order didn't fill
        let exec_qty: f64 = v["executedQty"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        if exec_qty < 1e-12 {
            // Order exists but hasn't filled yet (or was partially cancelled)
            return Ok((0.0, 0.0, 0.0));
        }

        let price = v["avgPrice"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        if price <= 0.0 {
            return Ok((0.0, 0.0, 0.0));
        }

        // Step 2: Fetch real commission from userTrades endpoint
        let ts2 = now_ms();
        let qs2 = format!("symbol={}&orderId={}&timestamp={}", pair, order_id, ts2);
        let sig2 = binance_sign(&keys.binance_secret, &qs2);
        let url2 = format!("https://fapi.binance.com/fapi/v1/userTrades?{}&signature={}", qs2, sig2);
        let fee = match http
            .get(&url2)
            .header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp2) => match resp2.json::<Value>().await {
                Ok(v2) => v2.as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter(|t| {
                                t["orderId"].as_i64().map(|id| id.to_string()).as_deref()
                                    == Some(order_id)
                            })
                            .fold(0.0_f64, |acc, t| {
                                let comm = t["commission"]
                                    .as_str()
                                    .and_then(|s| s.parse::<f64>().ok())
                                    .unwrap_or(0.0);
                                let asset = t["commissionAsset"].as_str().unwrap_or("USDT");
                                acc + match asset {
                                    "USDT" => comm,
                                    _ => if price > 0.0 { comm * price } else { comm },
                                }
                            })
                    })
                    .unwrap_or(0.0),
                Err(_) => 0.0,
            },
            Err(_) => 0.0,
        };
        let exec_contracts = exec_qty / multiplier.max(1e-12);
        Ok((price, fee, exec_contracts))
    }

    async fn fetch_balances(&self, http: &Client, keys: &ExchangeKeys) -> Result<(f64, f64), String> {
        let ts = now_ms();
        let qs = format!("timestamp={}", ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let url = format!("https://fapi.binance.com/fapi/v2/account?{}&signature={}", qs, sig);
        let resp = http.get(&url).header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(10)).send().await.map_err(|e| e.to_string())?;

        let data: BinanceBalanceAccountResponse = resp.json().await.map_err(|e| format!("Binance balance json parse err: {}", e))?;
        let avail = data.availableBalance.parse::<f64>().map_err(|_| "Failed to parse Binance availableBalance as f64".to_string())?;
        let wallet = data.totalWalletBalance.parse::<f64>().unwrap_or(0.0); // totalWalletBalance might not exist in some responses? It should.
        Ok((wallet, avail))
    }

    async fn fetch_taker_fee(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        let ts = now_ms();
        let qs = format!("symbol=BTCUSDT&timestamp={}", ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let url = format!("https://fapi.binance.com/fapi/v1/commissionRate?{}&signature={}", qs, sig);
        let resp = http.get(&url).header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(10)).send().await.map_err(|e| e.to_string())?;

        let data: BinanceFeeResponse = resp.json().await.map_err(|e| format!("Binance fee json parse err: {}", e))?;
        data.takerCommissionRate.parse::<f64>().map_err(|_| "Failed to parse Binance fee as f64".to_string())
    }

    async fn ensure_leverage(&self, http: &Client, keys: &ExchangeKeys, pair: &str, leverage: f64) -> Result<(), String> {
        // Implement cache locally or via shared cache to avoid spamming
        // Since the interface signature doesn't take EngineContext, we'll keep it simple for now
        let ts = now_ms();
        let qs = format!("symbol={}&leverage={}&timestamp={}", pair, leverage as i64, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let url = format!("https://fapi.binance.com/fapi/v1/leverage?{}&signature={}", qs, sig);
        let _ = http.post(&url).header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(10)).send().await;
        
        let ts = now_ms();
        let qs = format!("symbol={}&marginType=ISOLATED&timestamp={}", pair, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let url = format!("https://fapi.binance.com/fapi/v1/marginType?{}&signature={}", qs, sig);
        let _ = http.post(&url).header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(10)).send().await;
            
        Ok(())
    }

    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String> {
        let url = format!("https://fapi.binance.com/fapi/v1/premiumIndex?symbol={}", pair);
        if let Ok(resp) = http.get(&url).timeout(Duration::from_secs(5)).send().await {
            if let Ok(json) = resp.json::<Value>().await {
                if let Some(rate_str) = json["lastFundingRate"].as_str() {
                    if let Ok(rate) = rate_str.parse::<f64>() {
                        let next_ts = json["nextFundingTime"].as_i64().unwrap_or(0);
                        return Ok((rate, next_ts));
                    }
                }
            }
        }
        Err("Binance funding rate fetch failed".to_string())
    }

    async fn fetch_positions(&self, http: &Client, keys: &ExchangeKeys) -> Result<Vec<super::traits::PositionInfo>, String> {
        let ts = now_ms();
        let qs = format!("timestamp={}", ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let url = format!("https://fapi.binance.com/fapi/v2/positionRisk?{}&signature={}", qs, sig);
        let resp = http.get(&url).header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(10)).send().await.map_err(|e| e.to_string())?;
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let mut positions = Vec::new();
        if let Some(arr) = v.as_array() {
            for p in arr {
                let amt: f64 = p["positionAmt"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if amt.abs() < 1e-12 { continue; }
                positions.push(super::traits::PositionInfo {
                    symbol: p["symbol"].as_str().unwrap_or("").to_string(),
                    size: amt,
                    entry_price: p["entryPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                    unrealized_pnl: p["unRealizedProfit"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                });
            }
        }
        Ok(positions)
    }
}
