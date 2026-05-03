use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value; // We will replace these with strict DTOs
use std::time::Duration;

use super::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::{binance_sign, now_ms, ExchangeKeys};

pub struct BinanceConnector;

async fn check_rate_limits(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
) -> Result<(), String> {
    if status.as_u16() == 429 {
        if let Some(retry_after) = headers.get("Retry-After") {
            if let Ok(sec) = retry_after.to_str().unwrap_or("").parse::<u64>() {
                tracing::error!(
                    "[binance] 🔴 RATE LIMIT HIT (429)! Backing off for {} seconds.",
                    sec
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(sec)).await;
            }
        } else {
            tracing::error!("[binance] 🔴 RATE LIMIT HIT (429)! Backing off for 10 seconds.");
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
        return Err("Binance HTTP 429 Too Many Requests (Rate limit hit)".into());
    }
    if status.as_u16() == 418 {
        tracing::error!("[binance] 💀 IP COMPLETELY BANNED (HTTP 418).");
        return Err("Binance HTTP 418 IP Banned".into());
    }

    if let Some(w) = headers.get("X-MBX-USED-WEIGHT-1M") {
        if let Ok(weight) = w.to_str().unwrap_or("").parse::<u32>() {
            if weight > 4000 {
                tracing::warn!("[binance] ⚠️ HIGH RATE LIMIT WEIGHT USED: {}/6000", weight);
            }
        }
    }
    Ok(())
}

async fn execute_binance_order(
    _http: &Client,
    keys: &ExchangeKeys,
    params: &serde_json::Map<String, Value>,
    _api_timeout_ms: u64,
) -> Result<Value, String> {
    use crate::engine::exchanges::ws_trader::send_ws_request;
    
    // 1. Build signature for WS (must include apiKey)
    let mut ws_params = params.clone();
    ws_params.insert("apiKey".to_string(), serde_json::json!(keys.binance_key));
    
    let mut ws_sorted_keys: Vec<&String> = ws_params.keys().collect();
    ws_sorted_keys.sort();
    let ws_qs: String = ws_sorted_keys.iter()
        .map(|k| {
            let v = &ws_params[k.as_str()];
            let vs = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                _ => v.to_string(),
            };
            format!("{}={}", k, vs)
        })
        .collect::<Vec<_>>()
        .join("&");
    
    let ws_sig = binance_sign(&keys.binance_secret, &ws_qs);
    ws_params.insert("signature".to_string(), serde_json::json!(ws_sig));
    
    let req_id = uuid::Uuid::new_v4().to_string();
    let ws_req = serde_json::json!({
        "id": req_id,
        "method": "order.place",
        "params": ws_params
    });
    
    match send_ws_request(ws_req).await {
        Ok(v_ws) => {
            if let Some(status) = v_ws.get("status") {
                if status.as_i64() == Some(200) {
                    if let Some(res) = v_ws.get("result") {
                        return Ok(res.clone());
                    }
                }
            }
            if let Some(err) = v_ws.get("error") {
                tracing::error!("[binance_ws] order error: {}", err);
                return Err(format!("Binance WS API Error: {}", err["msg"].as_str().unwrap_or("?")));
            }
            Ok(v_ws.get("result").cloned().unwrap_or(v_ws))
        },
        Err(e) => {
            tracing::error!("[binance_ws] WS order failed ({}). No REST fallback available.", e);
            Err(format!("Binance WS connection error: {}", e))
        }
    }
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct BinanceBalanceAccountResponse {
    availableBalance: String,
    totalWalletBalance: String,
    totalMarginBalance: Option<String>,
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

    async fn place_order(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        is_buy: bool,
        phys_amount: f64,
        dry_run: bool,
        target_price: f64,
        use_ioc: bool,
        _leverage: f64,
        multiplier: f64,
        slippage_percent: f64,
        tick_size: f64,
        api_timeout_ms: u64,
    ) -> Result<(Option<String>, f64, f64), String> {
        let side = if is_buy { "BUY" } else { "SELL" };
        if dry_run {
            return Ok((
                Some(format!("dry_{}_{}", self.name(), now_ms())),
                target_price,
                0.0,
            ));
        }

        let ts = now_ms();
        let mut qty_str = format!(
            "{:.8}",
            (phys_amount / multiplier.max(1e-12)).round() * multiplier.max(1e-12)
        )
        .trim_end_matches('0')
        .to_string();
        if qty_str.ends_with('.') {
            qty_str.pop();
        }
        if qty_str.is_empty() || qty_str == "0" {
            return Err(format!(
                "Calculated quantity is 0 for amt={} mult={}, check multiplier mismatch",
                phys_amount, multiplier
            ));
        }

        let price = if is_buy {
            target_price * (1.0 + slippage_percent / 100.0)
        } else {
            target_price * (1.0 - slippage_percent / 100.0)
        };
        let rounded_price = if is_buy {
            (price / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8)
        } else {
            (price / tick_size.max(1e-8)).floor() * tick_size.max(1e-8)
        };

        let mut params = serde_json::Map::new();
        params.insert("symbol".to_string(), serde_json::json!(pair));
        params.insert("side".to_string(), serde_json::json!(side));
        params.insert("quantity".to_string(), serde_json::json!(qty_str));
        params.insert("timestamp".to_string(), serde_json::json!(ts));

        // MARKET vs IOC LIMIT
        if use_ioc && target_price > 0.0 {
            let mut price_str = format!("{:.8}", rounded_price)
                .trim_end_matches('0')
                .to_string();
            if price_str.ends_with('.') {
                price_str.pop();
            }
            if price_str.is_empty() {
                price_str = "0".to_string();
            }
            params.insert("type".to_string(), serde_json::json!("LIMIT"));
            params.insert("timeInForce".to_string(), serde_json::json!("IOC"));
            params.insert("price".to_string(), serde_json::json!(price_str));
        } else {
            params.insert("type".to_string(), serde_json::json!("MARKET"));
        }
        
        let v = execute_binance_order(http, keys, &params, api_timeout_ms).await?;

        // Diagnostic: log order request and response
        let resp_oid = v["orderId"]
            .as_i64()
            .map(|i| i.to_string())
            .unwrap_or_else(|| "none".to_string());
        let resp_status_str = v["status"].as_str().unwrap_or("?");
        tracing::info!(
            "[binance] place_order {} side={} qty={} → orderId={} status={}",
            pair,
            side,
            qty_str,
            resp_oid,
            resp_status_str
        );

        let order_id = v["orderId"].as_i64().map(|id| id.to_string());
        let order_status = v["status"].as_str().unwrap_or("?");

        if order_id.is_some() {
            // Extract inline fill data from WS response
            let exec_qty: f64 = v["executedQty"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| v["executedQty"].as_f64())
                .unwrap_or(0.0);
            let avg_price: f64 = v["avgPrice"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| v["avgPrice"].as_f64())
                .unwrap_or(0.0);
            let cum_fee: f64 = v["cumCommission"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| v["cumCommission"].as_f64())
                .unwrap_or(0.0)
                .abs();

            // Log FULL WS response for diagnostics
            tracing::info!(
                "[binance] place_order {} WS result: status={} execQty={} avgPrice={} fee={} | raw_keys={:?}",
                pair, order_status, exec_qty, avg_price, cum_fee,
                v.as_object().map(|m| m.keys().collect::<Vec<_>>()).unwrap_or_default()
            );

            // For IOC/EXPIRED: if order was cancelled without any fill, return None (no orderId)
            // so the engine knows the order didn't execute
            if (order_status == "EXPIRED" || order_status == "CANCELED" || order_status == "REJECTED")
                && exec_qty < 1e-12
            {
                tracing::warn!(
                    "[binance] place_order {} IOC order {} — NOT FILLED (status={})",
                    pair, order_id.as_deref().unwrap_or("?"), order_status
                );
                return Ok((None, 0.0, 0.0));
            }

            Ok((order_id, avg_price, cum_fee))
        } else {
            tracing::error!("[binance] place_order {} — no orderId in WS response: {:?}", pair, v);
            Err("Binance failed to return orderId".to_string())
        }
    }

    async fn close_position(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        is_buy: bool,
        phys_amount: f64,
        dry_run: bool,
        leverage: f64,
        target_price: f64,
        use_ioc: bool,
        slippage_percent: f64,
        multiplier: f64,
        tick_size: f64,
        retries: u32,
        delay_ms: u64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        let side = if is_buy { "BUY" } else { "SELL" };
        if dry_run {
            return Ok(CloseReceipt {
                final_price: target_price,
                total_fee: 0.0,
                closed_volume: phys_amount,
            });
        }

        let mut qty_str = if multiplier == 0.0 {
            format!("{:.8}", phys_amount)
                .trim_end_matches('0')
                .to_string()
        } else {
            format!(
                "{:.8}",
                (phys_amount / multiplier.max(1e-12)).round() * multiplier.max(1e-12)
            )
            .trim_end_matches('0')
            .to_string()
        };
        if qty_str.ends_with('.') {
            qty_str.pop();
        }
        if qty_str.is_empty() || phys_amount < 1e-12 {
            return Err("Amounts too small to close".to_string());
        }

        let price = if is_buy {
            target_price * (1.0 + slippage_percent / 100.0)
        } else {
            target_price * (1.0 - slippage_percent / 100.0)
        };
        let rounded_price = if is_buy {
            (price / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8)
        } else {
            (price / tick_size.max(1e-8)).floor() * tick_size.max(1e-8)
        };

        let mut price_str = format!("{:.8}", rounded_price)
            .trim_end_matches('0')
            .to_string();
        if price_str.ends_with('.') {
            price_str.pop();
        }
        if price_str.is_empty() {
            price_str = "0".to_string();
        }

        let mut params = serde_json::Map::new();
        params.insert("symbol".to_string(), serde_json::json!(pair));
        params.insert("side".to_string(), serde_json::json!(side));
        params.insert("quantity".to_string(), serde_json::json!(qty_str));
        params.insert("reduceOnly".to_string(), serde_json::json!("true"));
        
        let ts = now_ms();
        params.insert("timestamp".to_string(), serde_json::json!(ts));

        let type_str = if use_ioc && target_price > 0.0 { "LIMIT" } else { "MARKET" };
        params.insert("type".to_string(), serde_json::json!(type_str));

        if use_ioc && target_price > 0.0 {
            params.insert("timeInForce".to_string(), serde_json::json!("IOC"));
            params.insert("price".to_string(), serde_json::json!(price_str));
        }

        let _ = self.ensure_leverage(http, keys, pair, leverage).await;

        let v_resp = execute_binance_order(http, keys, &params, api_timeout_ms).await;
        if let Err(e) = &v_resp {
            tracing::error!("[binance] normal close rejected: {}", e);
        }
        let order_id = v_resp.ok().and_then(|v| v["orderId"].as_i64().map(|id| id.to_string()));

        // Polling loop inside connector
        if let Some(ref oid) = order_id {
            for _ in 0..retries {
                if let Ok((p, f, _)) = self.fetch_order(http, keys, pair, oid, multiplier).await {
                    if p > 0.0 {
                        return Ok(CloseReceipt {
                            final_price: p,
                            total_fee: f,
                            closed_volume: phys_amount,
                        });
                    }
                }
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }

        // Fallback market close if IOC or limit failed/unverified
        tracing::error!(
            "[binance] fallback market close triggered for {} (qty {})",
            pair,
            phys_amount
        );
        let mut fb_price_str = String::new();
        if target_price > 0.0 {
            let p = if is_buy {
                target_price * 1.02
            } else {
                target_price * 0.98
            };
            let rp = if is_buy {
                (p / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8)
            } else {
                (p / tick_size.max(1e-8)).floor() * tick_size.max(1e-8)
            };
            let mut s = format!("{:.8}", rp).trim_end_matches('0').to_string();
            if s.ends_with('.') { s.pop(); }
            if !s.is_empty() && s != "0" {
                fb_price_str = s;
            }
        }

        let type_fb = if fb_price_str.is_empty() { "MARKET" } else { "LIMIT" };

        let ts_fb = now_ms();
        let mut fb_params = serde_json::Map::new();
        fb_params.insert("symbol".to_string(), serde_json::json!(pair));
        fb_params.insert("side".to_string(), serde_json::json!(side));
        fb_params.insert("type".to_string(), serde_json::json!(type_fb));
        fb_params.insert("quantity".to_string(), serde_json::json!(qty_str));
        fb_params.insert("reduceOnly".to_string(), serde_json::json!("true"));
        fb_params.insert("timestamp".to_string(), serde_json::json!(ts_fb));
        
        if !fb_price_str.is_empty() {
            fb_params.insert("price".to_string(), serde_json::json!(fb_price_str));
            fb_params.insert("timeInForce".to_string(), serde_json::json!("IOC"));
        }
        
        let v_fb = execute_binance_order(http, keys, &fb_params, api_timeout_ms).await.map_err(|e| format!("Fallback close rejected: {}", e))?;
        let oid_fb = v_fb["orderId"]
            .as_i64()
            .map(|id| id.to_string())
            .ok_or_else(|| "Fallback close rejected: no orderId".to_string())?;

        for _ in 0..5 {
            if let Ok((p, f, _)) = self
                .fetch_order(http, keys, pair, &oid_fb, multiplier)
                .await
            {
                if p > 0.0 {
                    return Ok(CloseReceipt {
                        final_price: p,
                        total_fee: f,
                        closed_volume: phys_amount,
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Deep verification
        let pos = self
            .fetch_positions(http, keys)
            .await
            .map_err(|e| format!("Binance close unverified: failed to fetch positions: {e}"))?;
        let still_has = pos
            .iter()
            .any(|p| crate::engine::real::symbols_match(&p.symbol, pair) && p.size.abs() > 1e-12);
        if !still_has {
            Err("Binance close unverified: position appears closed but fill price/fee is unknown".to_string())
        } else {
            Err("Failed to verify close on Binance despite fallback".to_string())
        }
    }

    async fn trim_position(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        is_buy: bool,
        phys_excess: f64,
        dry_run: bool,
        multiplier: f64,
        tick_size: f64,
        fallback_price: f64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        self.close_position(
            http,
            keys,
            pair,
            is_buy,
            phys_excess,
            dry_run,
            1.0,
            fallback_price,
            true,
            0.0,
            multiplier,
            tick_size,
            15,
            100,
            api_timeout_ms,
        )
        .await
    }

    async fn fetch_order(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        order_id: &str,
        _multiplier: f64,
    ) -> Result<(f64, f64, f64), String> {
        if order_id.starts_with("dry_") {
            return Ok((0.0, 0.0, 0.0));
        }
        let ts = now_ms();
        let qs = format!("symbol={}&orderId={}&timestamp={}", pair, order_id, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let url = format!(
            "https://fapi.binance.com/fapi/v1/order?{}&signature={}",
            qs, sig
        );
        let resp = http
            .get(&url)
            .header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;

        let exec_qty: f64 = v["executedQty"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let status = v["status"].as_str().unwrap_or("");

        // IMPORTANT: Binance marks IOC orders that partially fill as "EXPIRED".
        // We MUST verify executedQty first before blindly assuming the order was un-filled.
        if exec_qty < 1e-12 {
            if status == "EXPIRED" || status == "CANCELED" || status == "REJECTED" {
                tracing::warn!(
                    "[binance] fetch_order {} status={} — definitively unfilled",
                    order_id,
                    status
                );
                return Ok((0.0, 0.0, 0.0));
            }
            // Order exists but hasn't filled yet (or was partially cancelled)
            return Err("Not filled yet".to_string());
        }

        let price = v["avgPrice"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        if price <= 0.0 {
            return Ok((0.0, 0.0, 0.0));
        }

        // Step 2: Fetch real commission from userTrades endpoint
        let ts2 = now_ms();
        let qs2 = format!("symbol={}&orderId={}&timestamp={}", pair, order_id, ts2);
        let sig2 = binance_sign(&keys.binance_secret, &qs2);
        let url2 = format!(
            "https://fapi.binance.com/fapi/v1/userTrades?{}&signature={}",
            qs2, sig2
        );
        let fee = match http
            .get(&url2)
            .header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp2) => match resp2.json::<Value>().await {
                Ok(v2) => v2
                    .as_array()
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
                                    _ => {
                                        if price > 0.0 {
                                            comm * price
                                        } else {
                                            comm
                                        }
                                    }
                                }
                            })
                        // Replace only if the userTrades endpoint yielded a real fee
                        // Note: For Binance USDT-M, executedQty is physical coin volume, DO NOT divide by multiplier!
                    })
                    .unwrap_or(0.0),
                Err(_) => 0.0,
            },
            Err(_) => 0.0,
        };
        Ok((price, fee, exec_qty))
    }

    async fn fetch_balances(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<(f64, f64), String> {
        // Use WS-FAPI to bypass geo-restrictions (REST fapi.binance.com is blocked from Japan)
        use crate::engine::exchanges::ws_trader::send_ws_request;
        
        let ts = now_ms();
        let qs = format!("apiKey={}&timestamp={}", keys.binance_key, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "id": req_id,
            "method": "v2/account.balance",
            "params": {
                "apiKey": keys.binance_key,
                "timestamp": ts,
                "signature": sig
            }
        });
        
        let resp = send_ws_request(ws_req).await?;
        
        // Check for WS API errors
        if let Some(err) = resp.get("error") {
            let code = err["code"].as_i64().unwrap_or(0);
            let msg = err["msg"].as_str().unwrap_or("unknown");
            return Err(format!("Binance WS balance error (code={}): {}", code, msg));
        }
        
        // Parse response: {"id":"...","status":200,"result":[{"asset":"USDT","balance":"16.05",...}]}
        let result = resp["result"].as_array()
            .ok_or_else(|| format!("Binance WS balance: no result array in response: {}", 
                &resp.to_string()[..resp.to_string().len().min(300)]))?;
        
        let mut total_balance = 0.0f64;
        let mut available_balance = 0.0f64;
        
        for item in result {
            let asset = item["asset"].as_str().unwrap_or("");
            if asset == "USDT" {
                total_balance = item["balance"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                available_balance = item["availableBalance"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                break;
            }
        }
        
        Ok((total_balance, available_balance))
    }

    async fn fetch_taker_fee(&self, _http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        use crate::engine::exchanges::ws_trader::send_ws_request;
        let ts = now_ms();
        let qs = format!("apiKey={}&symbol=BTCUSDT&timestamp={}", keys.binance_key, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "id": req_id,
            "method": "v1/account.commissionRate",
            "params": {
                "apiKey": keys.binance_key,
                "symbol": "BTCUSDT",
                "timestamp": ts,
                "signature": sig
            }
        });
        let resp = send_ws_request(ws_req).await?;
        if let Some(err) = resp.get("error") {
            return Err(format!("Binance WS fee error: {}", err));
        }
        resp["result"]["takerCommissionRate"].as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| "Failed to parse Binance fee from WS".to_string())
    }

    async fn ensure_leverage(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        leverage: f64,
    ) -> Result<(), String> {
        // Only set leverage — do NOT force margin type here.
        // Margin type (CROSSED/ISOLATED) should be configured once on the exchange or via
        // the full ensure_leverage in warmup_leverages (real.rs) which has access to profile settings.
        let ts = now_ms();
        let qs = format!(
            "symbol={}&leverage={}&timestamp={}",
            pair, leverage as i64, ts
        );
        let sig = binance_sign(&keys.binance_secret, &qs);
        let _ = http
            .post(format!(
                "https://fapi.binance.com/fapi/v1/leverage?{}&signature={}",
                qs, sig
            ))
            .header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(10))
            .send()
            .await;
        Ok(())
    }

    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String> {
        let url = format!(
            "https://fapi.binance.com/fapi/v1/premiumIndex?symbol={}",
            pair
        );
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

    async fn fetch_positions(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<Vec<PositionInfo>, String> {
        use crate::engine::exchanges::ws_trader::send_ws_request;
        let ts = now_ms();
        let qs = format!("apiKey={}&timestamp={}", keys.binance_key, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "id": req_id,
            "method": "v2/account.position",
            "params": {
                "apiKey": keys.binance_key,
                "timestamp": ts,
                "signature": sig
            }
        });
        let resp = send_ws_request(ws_req).await?;
        if let Some(err) = resp.get("error") {
            return Err(format!("Binance WS position error: {}", err));
        }
        let mut positions = Vec::new();
        if let Some(arr) = resp["result"].as_array() {
            for p in arr {
                let amt: f64 = p["positionAmt"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                if amt.abs() < 1e-12 {
                    continue;
                }
                positions.push(PositionInfo {
                    symbol: p["symbol"].as_str().unwrap_or("").to_string(),
                    size: amt,
                    entry_price: p["entryPrice"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0),
                    unrealized_pnl: p["unRealizedProfit"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0),
                    realized_pnl: 0.0,
                    accumulated_fee: 0.0,
                    accumulated_funding: 0.0,
                });
            }
        }
        Ok(positions)
    }

    async fn fetch_orderbook(
        &self,
        http: &Client,
        pair: &str,
        depth: u32,
    ) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), String> {
        let url = format!(
            "https://fapi.binance.com/fapi/v1/depth?symbol={}&limit={}",
            pair, depth
        );
        let res = http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Binance depth error: {}", e))?;

        let status = res.status();
        let body = res
            .text()
            .await
            .map_err(|e| format!("Body read error: {}", e))?;

        if !status.is_success() {
            return Err(format!("Binance API HTTP {}: {}", status, body));
        }

        let json: Value =
            serde_json::from_str(&body).map_err(|e| format!("Binance Parse error: {}", e))?;

        let parse_levels = |levels: &Value| -> Vec<(f64, f64)> {
            let mut out = Vec::new();
            if let Some(arr) = levels.as_array() {
                for item in arr {
                    if let (Some(px_str), Some(sz_str)) = (
                        item.get(0).and_then(|v| v.as_str()),
                        item.get(1).and_then(|v| v.as_str()),
                    ) {
                        if let (Ok(px), Ok(sz)) = (px_str.parse::<f64>(), sz_str.parse::<f64>()) {
                            out.push((px, sz));
                        }
                    }
                }
            }
            out
        };

        let bids = parse_levels(&json["bids"]);
        let asks = parse_levels(&json["asks"]);

        Ok((bids, asks))
    }
}
