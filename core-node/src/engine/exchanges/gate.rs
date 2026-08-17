use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

use super::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::{gate_sign, ExchangeKeys};

pub struct GateConnector;

/// Try placing order via WS, return Ok((orderId, fill_price, fee)) or Err for REST fallback
async fn try_gate_ws_order(
    _keys: &ExchangeKeys,
    contract: &str,
    size: i64,
    price_str: &str,
    tif: &str,
) -> Result<(String, f64, f64), String> {
    use super::ws_gate::send_gate_ws_request;

    let req_id = uuid::Uuid::new_v4().to_string();

    let ws_req = serde_json::json!({
        "request_id": req_id,
        "channel": "futures.order_place",
        "event": "api",
        "payload": [{
            "contract": contract,
            "size": size,
            "price": price_str,
            "tif": tif,
            "reduce_only": false,
            "close": false,
            "text": "t-corenode-ws"
        }]
    });

    let resp = send_gate_ws_request(ws_req).await?;

    let status = resp["header"]["status"].as_str().unwrap_or("0");
    if status == "200" || status == "201" {
        // Extract order result — could be object or array
        let result = resp["data"]["result"].as_object()
            .cloned()
            .or_else(|| resp["data"]["result"].as_array()
                .and_then(|a| a.first())
                .and_then(|v| v.as_object())
                .cloned());
        
        if let Some(r) = result {
            if let Some(id) = r.get("id") {
                let oid = id.as_i64().map(|i| i.to_string())
                    .or_else(|| id.as_str().map(|s| s.to_string()));
                if let Some(oid) = oid {
                    // Extract fill_price and fee from the response
                    let fill_price: f64 = r.get("fill_price")
                        .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64()))
                        .unwrap_or(0.0);
                    let fee: f64 = r.get("fee")
                        .and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64()))
                        .unwrap_or(0.0);
                    
                    let order_status = r.get("status").and_then(|v| v.as_str()).unwrap_or("");
                    let finish_as = r.get("finish_as").and_then(|v| v.as_str()).unwrap_or("");
                    let left = r.get("left").and_then(|v| v.as_i64()).unwrap_or(0);
                    
                    tracing::info!(
                        "[gate] WS order_place response: oid={} status={} finish_as={} fill_price={} fee={} left={}",
                        oid, order_status, finish_as, fill_price, fee, left
                    );
                    
                    return Ok((oid, fill_price, fee.abs()));
                }
            }
        }
    }

    let errs = resp["data"]["errs"].as_object()
        .map(|e| format!("{:?}", e))
        .unwrap_or_else(|| format!("status={}", status));
    Err(format!("Gate WS: {}", errs))
}


async fn fetch_gate_order_fee(http: &Client, keys: &ExchangeKeys, contract: &str, order_id: &str) -> Option<f64> {
    let path = format!("/api/v4/futures/usdt/my_trades?contract={}&order={}", contract, order_id);
    let headers = gate_sign(&keys.gate_key, &keys.gate_secret, "GET", &path, "");
    let url = format!("https://api.gateio.ws{}", path);
    if let Ok(resp) = http.get(&url).headers(headers).timeout(Duration::from_millis(2000)).send().await {
        if let Ok(trades) = resp.json::<Value>().await {
            if let Some(arr) = trades.as_array() {
                let mut sum_fee = 0.0;
                for t in arr {
                    if let Some(f) = t["fee"].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| t["fee"].as_f64()) {
                        sum_fee += f.abs();
                    }
                }
                if sum_fee > 0.0 {
                    return Some(sum_fee);
                }
            }
        }
    }
    None
}

#[async_trait]
impl ExchangeConnector for GateConnector {
    fn name(&self) -> &'static str {
        "gate"
    }

    async fn place_order(
        &self,
        _http: &Client,
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
        _api_timeout_ms: u64,
    ) -> Result<(Option<String>, f64, f64), String> {
        let contract = pair.replace("USDT", "_USDT");
        let size = if is_buy {
            (phys_amount / multiplier.max(1e-12)).round() as i64
        } else {
            -((phys_amount / multiplier.max(1e-12)).round() as i64)
        };
        if size == 0 {
            return Err(format!(
                "Computed order size is 0 for amt={} mult={}",
                phys_amount, multiplier
            ));
        }
        let _path = "/api/v4/futures/usdt/orders";

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

        let mut raw_price_str = format!("{:.8}", rounded_price)
            .trim_end_matches('0')
            .to_string();
        if raw_price_str.ends_with('.') {
            raw_price_str.pop();
        }
        if raw_price_str.is_empty() {
            raw_price_str = "0".to_string();
        }
        let price_str = if use_ioc && target_price > 0.0 {
            raw_price_str
        } else {
            "0".to_string()
        };
        let tif = if use_ioc && target_price > 0.0 {
            "ioc"
        } else {
            // For price=0 paths use IOC semantics to avoid lingering limit orders.
            "ioc"
        };

        let body = json!({
            "contract": contract,
            "size": size,
            "price": price_str,
            "tif": tif,
            "reduce_only": false,
            "close": false,
            "text": "t-corenode"
        });
        let path = "/api/v4/futures/usdt/orders";
        let body_str = body.to_string();

        if dry_run {
            return Ok((Some(format!("dry_{}", Uuid::new_v4())), target_price, 0.0));
        }

        let _ = super::ws_gate::get_or_init_gate_ws(
            keys.gate_key.clone(),
            keys.gate_secret.clone(),
        );

        match try_gate_ws_order(keys, &contract, size, &price_str, tif).await {
            Ok((oid, fill_price, fee)) => {
                tracing::info!("[gate] ✅ place_order {} orderId={} fill_price={:.6} fee={:.6} (WS)", pair, oid, fill_price, fee);
                Ok((Some(oid), fill_price, fee))
            }
            Err(e) => {
                tracing::warn!("[gate] place_order {} WS failed ({}). Trying REST fallback...", pair, e);
                let headers = gate_sign(&keys.gate_key, &keys.gate_secret, "POST", path, &body_str);
                let url = format!("https://api.gateio.ws{}", path);
                let resp = _http.post(&url)
                    .headers(headers)
                    .header("Content-Type", "application/json")
                    .body(body_str)
                    .timeout(Duration::from_millis(3000))
                    .send()
                    .await
                    .map_err(|e| format!("Gate REST place_order HTTP error: {}", e))?;
                let v = resp.json::<Value>().await
                    .map_err(|e| format!("Gate REST place_order JSON error: {}", e))?;
                if let Some(oid) = v["id"].as_i64().map(|i| i.to_string()).or_else(|| v["id"].as_str().map(|s| s.to_string())) {
                    let fill_price: f64 = v["fill_price"].as_str().and_then(|s| s.parse().ok()).or_else(|| v["fill_price"].as_f64()).unwrap_or(0.0);
                    let raw_fee: f64 = v["fee"].as_str().and_then(|s| s.parse().ok()).or_else(|| v["fee"].as_f64()).unwrap_or(0.0);
                    let fee = if raw_fee.abs() > 0.0 {
                        raw_fee.abs()
                    } else if let Some(real_fee) = fetch_gate_order_fee(_http, keys, &contract, &oid).await {
                        real_fee
                    } else {
                        0.0
                    };
                    tracing::info!("[gate] ✅ place_order {} orderId={} fill_price={:.6} fee={:.6} (REST)", pair, oid, fill_price, fee);
                    Ok((Some(oid), fill_price, fee))
                } else {
                    let err = v["label"].as_str().or_else(|| v["message"].as_str()).unwrap_or("Unknown Gate REST error");
                    Err(format!("Gate REST error: {}", err))
                }
            }
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
        _use_ioc: bool,
        slippage_percent: f64,
        multiplier: f64,
        tick_size: f64,
        _retries: u32,
        _delay_ms: u64,
        _api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        let contract = pair.replace("USDT", "_USDT");
        let size = if multiplier == 0.0 {
            if is_buy { phys_amount.round() as i64 } else { -(phys_amount.round() as i64) }
        } else {
            if is_buy {
                (phys_amount / multiplier.max(1e-12)).round() as i64
            } else {
                -((phys_amount / multiplier.max(1e-12)).round() as i64)
            }
        };
        if size == 0 { return Err("Amount too small to close".to_string()); }

        if dry_run {
            return Ok(CloseReceipt { final_price: target_price, total_fee: 0.0, closed_volume: phys_amount });
        }

        let _ = self.ensure_leverage(http, keys, pair, leverage).await;
        let _ = super::ws_gate::get_or_init_gate_ws(keys.gate_key.clone(), keys.gate_secret.clone());

        let mut final_price = target_price;
        if final_price > 0.0 {
            final_price = if is_buy { target_price * (1.0 + slippage_percent / 100.0) } else { target_price * (1.0 - slippage_percent / 100.0) };
            final_price = if is_buy { (final_price / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8) } else { (final_price / tick_size.max(1e-8)).floor() * tick_size.max(1e-8) };
        }

        let mut raw_price_str = format!("{:.8}", final_price).trim_end_matches('0').to_string();
        if raw_price_str.ends_with('.') { raw_price_str.pop(); }
        if raw_price_str.is_empty() { raw_price_str = "0".to_string(); }
        let price_str = if final_price <= 0.0 { "0".to_string() } else { raw_price_str };

        use super::ws_gate::send_gate_ws_request;
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "request_id": req_id,
            "channel": "futures.order_place",
            "event": "api",
            "payload": [{"contract": contract, "size": size, "price": price_str, "tif": "ioc", "reduce_only": true, "close": false, "text": "t-corenode-close"}]
        });

        if let Ok(resp) = send_gate_ws_request(ws_req).await {
            let status = resp["header"]["status"].as_str().unwrap_or("0");
            if status == "200" || status == "201" {
                let result = resp["data"]["result"].as_object()
                    .or_else(|| resp["data"]["result"].as_array().and_then(|a| a.first()).and_then(|v| v.as_object()));
                if let Some(r) = result {
                    let fill_price: f64 = r.get("fill_price").and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
                    let raw_fee: f64 = r.get("fee").and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
                    let oid_str = r.get("id").and_then(|v| v.as_i64().map(|i| i.to_string()).or_else(|| v.as_str().map(|s| s.to_string())));
                    let fee = if raw_fee.abs() > 0.0 {
                        raw_fee.abs()
                    } else if let Some(oid) = oid_str {
                        fetch_gate_order_fee(http, keys, &contract, &oid).await.unwrap_or(0.0)
                    } else {
                        0.0
                    };
                    if fill_price > 0.0 || r.get("id").is_some() {
                        return Ok(CloseReceipt { final_price: if fill_price > 0.0 { fill_price } else { target_price }, total_fee: fee, closed_volume: phys_amount });
                    }
                }
            }
        }

        // Fallback: aggressive WS close
        tracing::warn!("[gate] fallback close for {} (qty {})", pair, phys_amount);
        let fb_req_id = uuid::Uuid::new_v4().to_string();
        let fb_ws = serde_json::json!({
            "request_id": fb_req_id,
            "channel": "futures.order_place",
            "event": "api",
            "payload": [{"contract": contract, "size": size, "price": "0", "tif": "ioc", "reduce_only": true, "close": false, "text": "t-corenode-fb"}]
        });
        if let Ok(fb_resp) = send_gate_ws_request(fb_ws).await {
            let fb_s = fb_resp["header"]["status"].as_str().unwrap_or("0");
            if fb_s == "200" || fb_s == "201" {
                let result = fb_resp["data"]["result"].as_object()
                    .or_else(|| fb_resp["data"]["result"].as_array().and_then(|a| a.first()).and_then(|v| v.as_object()));
                if let Some(r) = result {
                    let fill_price: f64 = r.get("fill_price").and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
                    let raw_fee: f64 = r.get("fee").and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
                    let oid_str = r.get("id").and_then(|v| v.as_i64().map(|i| i.to_string()).or_else(|| v.as_str().map(|s| s.to_string())));
                    let fee = if raw_fee.abs() > 0.0 {
                        raw_fee.abs()
                    } else if let Some(oid) = oid_str {
                        fetch_gate_order_fee(http, keys, &contract, &oid).await.unwrap_or(0.0)
                    } else {
                        0.0
                    };
                    if fill_price > 0.0 || r.get("id").is_some() {
                        return Ok(CloseReceipt { final_price: if fill_price > 0.0 { fill_price } else { target_price }, total_fee: fee, closed_volume: phys_amount });
                    }
                }
            }
        }

        // 3rd attempt: Signed REST fallback
        let rest_body = json!({
            "contract": contract,
            "size": size,
            "price": "0",
            "tif": "ioc",
            "reduce_only": true,
            "close": false,
            "text": "t-corenode-close-rest"
        }).to_string();
        let path = "/api/v4/futures/usdt/orders";
        let headers = gate_sign(&keys.gate_key, &keys.gate_secret, "POST", path, &rest_body);
        let url = format!("https://api.gateio.ws{}", path);
        if let Ok(resp) = http.post(&url).headers(headers).header("Content-Type", "application/json").body(rest_body).timeout(Duration::from_millis(5000)).send().await {
            if let Ok(v) = resp.json::<Value>().await {
                if v.get("id").is_some() {
                    let fill_price: f64 = v["fill_price"].as_str().and_then(|s| s.parse().ok()).or_else(|| v["fill_price"].as_f64()).unwrap_or(0.0);
                    let raw_fee: f64 = v["fee"].as_str().and_then(|s| s.parse().ok()).or_else(|| v["fee"].as_f64()).unwrap_or(0.0);
                    let oid_str = v.get("id").and_then(|v| v.as_i64().map(|i| i.to_string()).or_else(|| v.as_str().map(|s| s.to_string())));
                    let fee = if raw_fee.abs() > 0.0 {
                        raw_fee.abs()
                    } else if let Some(oid) = oid_str {
                        fetch_gate_order_fee(http, keys, &contract, &oid).await.unwrap_or(0.0)
                    } else {
                        0.0
                    };
                    return Ok(CloseReceipt { final_price: if fill_price > 0.0 { fill_price } else { target_price }, total_fee: fee, closed_volume: phys_amount });
                } else {
                    tracing::error!("[gate] REST close response error for {}: {:?}", pair, v);
                }
            }
        }

        Err(format!("Gate.io close order failed for {}", pair))
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
        _fallback_price: f64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        // Just call close_position utilizing Gate's default logic (where price=0 -> deep limit fallback)
        self.close_position(
            http,
            keys,
            pair,
            is_buy,
            phys_excess,
            dry_run,
            1.0,
            _fallback_price,
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
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        order_id: &str,
        multiplier: f64,
    ) -> Result<(f64, f64, f64), String> {
        use super::ws_gate::send_gate_ws_request;
        let _ = super::ws_gate::get_or_init_gate_ws(keys.gate_key.clone(), keys.gate_secret.clone());
        let contract = pair.replace("USDT", "_USDT");
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "request_id": req_id,
            "channel": "futures.order_status",
            "event": "api",
            "payload": [contract.clone(), order_id]
        });
        let resp = send_gate_ws_request(ws_req).await?;
        let status_hdr = resp["header"]["status"].as_str().unwrap_or("0");
        if status_hdr != "200" {
            return Err(format!("Gate WS order_status error: {}", resp));
        }
        let v = &resp["data"]["result"];
        let fill_price = v["fill_price"].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v["fill_price"].as_f64()).unwrap_or(0.0);
        let fee = v["fee"].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v["fee"].as_f64()).unwrap_or(0.0);
        let status = v["status"].as_str().unwrap_or("");
        let finish_as = v["finish_as"].as_str().unwrap_or("");
        let size = v["size"].as_i64().unwrap_or(0).abs() as f64;
        let left = v["left"].as_i64().unwrap_or(0).abs() as f64;
        let exec_contracts = (size - left).max(0.0);
        
        // Order truly unfilled: finished with 0 executed contracts
        if exec_contracts <= 0.0 {
            if status == "finished" { return Ok((0.0, 0.0, 0.0)); }
            return Err("Not filled yet".to_string());
        }
        
        // Order has fills (exec_contracts > 0). If fill_price is available, use it.
        let mut final_price = fill_price;
        
        // Gate IOC can return fill_price=0 even when filled — use REST /my_trades as fallback
        if final_price <= 0.0 {
            tracing::warn!(
                "[gate] fetch_order {} fill_price=0 but exec_contracts={} (finish_as={}). Trying REST /my_trades...",
                order_id, exec_contracts, finish_as
            );
            let trades_path = format!(
                "/api/v4/futures/usdt/my_trades?order_id={}&contract={}",
                order_id, contract
            );
            let headers = crate::engine::real::gate_sign(
                &keys.gate_key, &keys.gate_secret, "GET", &trades_path, ""
            );
            let url = format!("https://api.gateio.ws{}", trades_path);
            if let Ok(resp) = _http.get(&url)
                .headers(headers)
                .timeout(Duration::from_secs(5))
                .send()
                .await
            {
                if let Ok(trades_json) = resp.json::<Value>().await {
                    if let Some(arr) = trades_json.as_array() {
                        let mut sum_value = 0.0_f64;
                        let mut sum_size = 0.0_f64;
                        let mut sum_fee = 0.0_f64;
                        for trade in arr {
                            let tp = trade["price"].as_str().and_then(|s| s.parse::<f64>().ok())
                                .or_else(|| trade["price"].as_f64()).unwrap_or(0.0);
                            let ts = trade["size"].as_i64().unwrap_or(0).abs() as f64;
                            let tf = trade["fee"].as_str().and_then(|s| s.parse::<f64>().ok())
                                .or_else(|| trade["fee"].as_f64()).unwrap_or(0.0);
                            sum_value += tp * ts;
                            sum_size += ts;
                            sum_fee += tf;
                        }
                        if sum_size > 0.0 {
                            final_price = sum_value / sum_size;
                            tracing::info!(
                                "[gate] 📊 fetch_order {} REST /my_trades: VWAP={:.8} fee={:.6} trades={}",
                                order_id, final_price, sum_fee.abs(), arr.len()
                            );
                            return Ok((final_price, sum_fee.abs(), exec_contracts * multiplier.max(1e-12)));
                        }
                    }
                }
            }
            // If REST also fails, log but still return with exec_contracts
            // (verify_fill will handle the 0 price case)
            tracing::error!(
                "[gate] ⚠️ fetch_order {} has {} filled contracts but no price from any source",
                order_id, exec_contracts
            );
        }
        
        Ok((final_price, fee.abs(), exec_contracts * multiplier.max(1e-12)))
    }

    async fn fetch_balances(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<(f64, f64), String> {
        let path = "/api/v4/futures/usdt/accounts";
        let headers = gate_sign(&keys.gate_key, &keys.gate_secret, "GET", path, "");

        let url = format!("https://api.gateio.ws{}", path);
        let resp = http.get(&url)
            .headers(headers)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("Gate HTTP error: {}", e))?;

        let v: Value = resp.json().await.map_err(|e| format!("Gate JSON error: {}", e))?;
        let total = v["total"].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v["total"].as_f64()).unwrap_or(0.0);
        let avail = v["available"].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v["available"].as_f64()).unwrap_or(0.0);
        Ok((total, avail))
    }

    async fn fetch_taker_fee(&self, _http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        use super::ws_gate::send_gate_ws_request;
        let _ = super::ws_gate::get_or_init_gate_ws(keys.gate_key.clone(), keys.gate_secret.clone());
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "request_id": req_id,
            "channel": "futures.fee_rate",
            "event": "api",
            "payload": []
        });
        match send_gate_ws_request(ws_req).await {
            Ok(resp) => {
                resp["data"]["result"]["taker_fee"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .ok_or_else(|| "Failed to parse Gate fee from WS".to_string())
            }
            Err(_) => Ok(0.0005) // Default taker fee if WS fails
        }
    }

    async fn ensure_leverage(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        leverage: f64,
    ) -> Result<(), String> {
        use super::ws_gate::send_gate_ws_request;
        let _ = super::ws_gate::get_or_init_gate_ws(keys.gate_key.clone(), keys.gate_secret.clone());
        let contract = pair.replace("USDT", "_USDT");
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "request_id": req_id,
            "channel": "futures.set_leverage",
            "event": "api",
            "payload": [contract, leverage as i64]
        });
        let _ = send_gate_ws_request(ws_req).await;
        Ok(())
    }


    async fn fetch_positions(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<Vec<PositionInfo>, String> {
        use super::ws_gate::send_gate_ws_request;
        let _ = super::ws_gate::get_or_init_gate_ws(keys.gate_key.clone(), keys.gate_secret.clone());
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "request_id": req_id,
            "channel": "futures.position_list",
            "event": "api",
            "payload": []
        });

        // Try WS first, fall back to REST on timeout/error
        let result_arr = match send_gate_ws_request(ws_req).await {
            Ok(resp) => {
                resp["data"]["result"].as_array().cloned().unwrap_or_default()
            }
            Err(_e) => {
                // REST fallback — Gate WS is unreliable for position queries
                let path = "/api/v4/futures/usdt/positions";
                let headers = gate_sign(&keys.gate_key, &keys.gate_secret, "GET", path, "");
                let url = format!("https://api.gateio.ws{}", path);
                let resp = _http.get(&url)
                    .headers(headers)
                    .timeout(Duration::from_secs(15))
                    .send()
                    .await
                    .map_err(|e| format!("Gate REST positions HTTP error: {}", e))?;
                let body: Value = resp.json().await
                    .map_err(|e| format!("Gate REST positions JSON error: {}", e))?;
                body.as_array().cloned().unwrap_or_default()
            }
        };

        let mut positions = Vec::new();
        for p in result_arr {
            let size: f64 = p["size"].as_i64().unwrap_or(0) as f64;
            if size.abs() < 1e-12 { continue; }
            let contract = p["contract"].as_str().unwrap_or("").replace("_", "");
            let realized = p["realised_pnl"].as_str().and_then(|s| s.parse().ok()).or_else(|| p["realised_pnl"].as_f64()).unwrap_or(0.0);
            positions.push(PositionInfo {
                symbol: contract,
                size,
                entry_price: p["entry_price"].as_str().and_then(|s| s.parse().ok()).or_else(|| p["entry_price"].as_f64()).unwrap_or(0.0),
                unrealized_pnl: p["unrealised_pnl"].as_str().and_then(|s| s.parse().ok()).or_else(|| p["unrealised_pnl"].as_f64()).unwrap_or(0.0),
                realized_pnl: realized,
                accumulated_fee: 0.0,
                accumulated_funding: 0.0,
            });
        }
        Ok(positions)
    }

    // ── Maker mode methods ──────────────────────────────────────────────────

    async fn place_maker_order(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        is_buy: bool,
        phys_amount: f64,
        limit_price: f64,
        _leverage: f64,
        multiplier: f64,
        tick_size: f64,
        _api_timeout_ms: u64,
    ) -> Result<(String, f64), String> {
        use super::ws_gate::send_gate_ws_request;
        let _ = super::ws_gate::get_or_init_gate_ws(
            keys.gate_key.clone(),
            keys.gate_secret.clone(),
        );

        let contract = pair.replace("USDT", "_USDT");
        let size = if is_buy {
            (phys_amount / multiplier.max(1e-12)).round() as i64
        } else {
            -((phys_amount / multiplier.max(1e-12)).round() as i64)
        };
        if size == 0 {
            return Err(format!("Computed maker order size is 0 for amt={} mult={}", phys_amount, multiplier));
        }

        // Round price to tick_size
        let rounded_price = if is_buy {
            (limit_price / tick_size.max(1e-8)).floor() * tick_size.max(1e-8)
        } else {
            (limit_price / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8)
        };

        let mut price_str = format!("{:.8}", rounded_price)
            .trim_end_matches('0')
            .to_string();
        if price_str.ends_with('.') { price_str.pop(); }
        if price_str.is_empty() { price_str = "0".to_string(); }

        let req_id = uuid::Uuid::new_v4().to_string();

        // TIF = "poc" → Post-Only Cancel: Gate rejects if order would cross spread
        let ws_req = serde_json::json!({
            "request_id": req_id,
            "channel": "futures.order_place",
            "event": "api",
            "payload": [{
                "contract": contract,
                "size": size,
                "price": price_str,
                "tif": "poc",
                "reduce_only": false,
                "close": false,
                "text": "t-corenode-maker"
            }]
        });

        let resp = send_gate_ws_request(ws_req).await?;

        let status = resp["header"]["status"].as_str().unwrap_or("0");
        if status == "200" || status == "201" {
            let result = resp["data"]["result"].as_object()
                .cloned()
                .or_else(|| resp["data"]["result"].as_array()
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_object())
                    .cloned());

            if let Some(r) = result {
                if let Some(id) = r.get("id") {
                    let oid = id.as_i64().map(|i| i.to_string())
                        .or_else(|| id.as_str().map(|s| s.to_string()));
                    if let Some(oid) = oid {
                        let order_status = r.get("status").and_then(|v| v.as_str()).unwrap_or("");
                        let finish_as = r.get("finish_as").and_then(|v| v.as_str()).unwrap_or("");

                        tracing::info!(
                            "[gate] ✅ MAKER order placed: pair={} oid={} price={} status={} finish_as={}",
                            pair, oid, price_str, order_status, finish_as
                        );

                        // Check if immediately cancelled by Gate (poc rejection = would be taker)
                        if order_status == "finished" && finish_as == "cancelled" {
                            return Err("Post-Only rejected: would cross spread (poc cancelled)".to_string());
                        }

                        return Ok((oid, rounded_price));
                    }
                }
            }
        }

        let errs = resp["data"]["errs"].as_object()
            .map(|e| format!("{:?}", e))
            .unwrap_or_else(|| format!("status={} body={}", status, resp));
        Err(format!("Gate WS maker order failed: {}", errs))
    }

    async fn cancel_order(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        order_id: &str,
    ) -> Result<bool, String> {
        use super::ws_gate::send_gate_ws_request;
        let _ = super::ws_gate::get_or_init_gate_ws(
            keys.gate_key.clone(),
            keys.gate_secret.clone(),
        );

        let contract = pair.replace("USDT", "_USDT");
        let req_id = uuid::Uuid::new_v4().to_string();

        let ws_req = serde_json::json!({
            "request_id": req_id,
            "channel": "futures.order_cancel",
            "event": "api",
            "payload": [contract, order_id]
        });

        let resp = send_gate_ws_request(ws_req).await?;
        let status = resp["header"]["status"].as_str().unwrap_or("0");

        if status == "200" {
            let finish_as = resp["data"]["result"]["finish_as"].as_str().unwrap_or("");
            if finish_as == "filled" {
                tracing::info!("[gate] cancel_order {}: already filled", order_id);
                return Ok(false); // Already filled, not cancelled
            }
            tracing::info!("[gate] cancel_order {}: cancelled successfully", order_id);
            return Ok(true);
        }

        // Check if already cancelled/filled (not an error)
        let err_msg = resp["data"]["errs"].as_object()
            .map(|e| format!("{:?}", e))
            .unwrap_or_else(|| format!("status={}", status));

        let err_lower = err_msg.to_lowercase();
        if err_lower.contains("not found") || err_lower.contains("finished") {
            tracing::info!("[gate] cancel_order {}: already gone ({})", order_id, err_msg);
            return Ok(false);
        }

        Err(format!("Gate cancel_order failed: {}", err_msg))
    }

    async fn fetch_maker_fee(&self, _http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        use super::ws_gate::send_gate_ws_request;
        let _ = super::ws_gate::get_or_init_gate_ws(keys.gate_key.clone(), keys.gate_secret.clone());
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "request_id": req_id,
            "channel": "futures.fee_rate",
            "event": "api",
            "payload": []
        });
        match send_gate_ws_request(ws_req).await {
            Ok(resp) => {
                resp["data"]["result"]["maker_fee"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .ok_or_else(|| "Failed to parse Gate maker_fee from WS".to_string())
            }
            Err(_) => Ok(0.00015) // Default maker fee if WS fails
        }
    }

}
