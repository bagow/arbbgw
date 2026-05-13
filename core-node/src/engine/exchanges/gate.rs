use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

use super::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::{gate_sign, ExchangeKeys};

pub struct GateConnector;

/// Try placing order via WS, return Ok(orderId) or Err for REST fallback
async fn try_gate_ws_order(
    _keys: &ExchangeKeys,
    contract: &str,
    size: i64,
    price_str: &str,
    tif: &str,
) -> Result<String, String> {
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
        if let Some(result) = resp["data"]["result"].as_object() {
            if let Some(id) = result.get("id") {
                let oid = id.as_i64().map(|i| i.to_string())
                    .or_else(|| id.as_str().map(|s| s.to_string()));
                if let Some(oid) = oid {
                    return Ok(oid);
                }
            }
        }
        // Check if result is an array (batch response)
        if let Some(arr) = resp["data"]["result"].as_array() {
            if let Some(first) = arr.first() {
                if let Some(id) = first.get("id") {
                    let oid = id.as_i64().map(|i| i.to_string())
                        .or_else(|| id.as_str().map(|s| s.to_string()));
                    if let Some(oid) = oid {
                        return Ok(oid);
                    }
                }
            }
        }
    }

    let errs = resp["data"]["errs"].as_object()
        .map(|e| format!("{:?}", e))
        .unwrap_or_else(|| format!("status={}", status));
    Err(format!("Gate WS: {}", errs))
}

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
        let path = "/api/v4/futures/usdt/orders";

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
        let body_str = body.to_string();

        if dry_run {
            return Ok((Some(format!("dry_{}", Uuid::new_v4())), target_price, 0.0));
        }

        // ── WS ONLY (no REST fallback) ──
        let _ = super::ws_gate::get_or_init_gate_ws(
            keys.gate_key.clone(),
            keys.gate_secret.clone(),
        );

        match try_gate_ws_order(keys, &contract, size, &price_str, tif).await {
            Ok(oid) => {
                tracing::info!("[gate] ✅ place_order {} orderId={} (WS)", pair, oid);
                Ok((Some(oid), 0.0, 0.0))
            }
            Err(e) => {
                tracing::error!("[gate] place_order {} WS failed: {}", pair, e);
                Err(format!("Gate WS order failed: {}", e))
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
        let is_market_close = raw_price_str == "0" || final_price <= 0.0;
        let price_str = if is_market_close { "0".to_string() } else { raw_price_str };
        let final_size = if is_market_close { 0 } else { size };

        use super::ws_gate::send_gate_ws_request;
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "request_id": req_id,
            "channel": "futures.order_place",
            "event": "api",
            "payload": [{"contract": contract, "size": final_size, "price": price_str, "tif": "ioc", "reduce_only": true, "close": is_market_close, "text": "t-corenode-close"}]
        });

        if let Ok(resp) = send_gate_ws_request(ws_req).await {
            let status = resp["header"]["status"].as_str().unwrap_or("0");
            if status == "200" || status == "201" {
                let result = resp["data"]["result"].as_object()
                    .or_else(|| resp["data"]["result"].as_array().and_then(|a| a.first()).and_then(|v| v.as_object()));
                if let Some(r) = result {
                    let fill_price: f64 = r.get("fill_price").and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
                    let fee: f64 = r.get("fee").and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
                    if fill_price > 0.0 {
                        return Ok(CloseReceipt { final_price: fill_price, total_fee: fee.abs(), closed_volume: phys_amount });
                    }
                }
            }
        }

        // Fallback: aggressive WS close
        tracing::error!("[gate] fallback close for {} (qty {})", pair, phys_amount);
        let mut fb_price_str = "0".to_string();
        if target_price > 0.0 {
            let p = if is_buy { target_price * 1.02 } else { target_price * 0.98 };
            let rp = if is_buy { (p / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8) } else { (p / tick_size.max(1e-8)).floor() * tick_size.max(1e-8) };
            fb_price_str = format!("{:.8}", rp).trim_end_matches('0').to_string();
            if fb_price_str.ends_with('.') { fb_price_str.pop(); }
            if fb_price_str.is_empty() { fb_price_str = "0".to_string(); }
        }
        let is_fb_market = fb_price_str == "0";
        let fb_size = if is_fb_market { 0 } else { size };

        let fb_req_id = uuid::Uuid::new_v4().to_string();
        let fb_ws = serde_json::json!({
            "request_id": fb_req_id,
            "channel": "futures.order_place",
            "event": "api",
            "payload": [{"contract": contract, "size": fb_size, "price": fb_price_str, "tif": "ioc", "reduce_only": true, "close": is_fb_market, "text": "t-corenode-fb"}]
        });
        if let Ok(fb_resp) = send_gate_ws_request(fb_ws).await {
            let fb_s = fb_resp["header"]["status"].as_str().unwrap_or("0");
            if fb_s == "200" || fb_s == "201" {
                let result = fb_resp["data"]["result"].as_object()
                    .or_else(|| fb_resp["data"]["result"].as_array().and_then(|a| a.first()).and_then(|v| v.as_object()));
                if let Some(r) = result {
                    let fill_price: f64 = r.get("fill_price").and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
                    let fee: f64 = r.get("fee").and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_f64())).unwrap_or(0.0);
                    if fill_price > 0.0 {
                        return Ok(CloseReceipt { final_price: fill_price, total_fee: fee.abs(), closed_volume: phys_amount });
                    }
                }
            }
        }

        // Deep verification
        let pos = self.fetch_positions(http, keys).await.map_err(|e| format!("Gate close unverified: {e}"))?;
        let still_has = pos.iter().any(|p| crate::engine::real::symbols_match(&p.symbol, &contract.replace("_", "")) && p.size.abs() > 1e-12);
        if !still_has {
            Err("Gate close unverified: position appears closed but fill price/fee is unknown".to_string())
        } else {
            Err("Gate close unverified, position still active".to_string())
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
            "payload": [contract, order_id]
        });
        let resp = send_gate_ws_request(ws_req).await?;
        let status_hdr = resp["header"]["status"].as_str().unwrap_or("0");
        if status_hdr != "200" {
            return Err(format!("Gate WS order_status error: {}", resp));
        }
        let v = &resp["data"]["result"];
        let price = v["fill_price"].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v["fill_price"].as_f64()).unwrap_or(0.0);
        let fee = v["fee"].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v["fee"].as_f64()).unwrap_or(0.0);
        let status = v["status"].as_str().unwrap_or("");
        if status == "finished" && price <= 0.0 { return Ok((0.0, 0.0, 0.0)); }
        let size = v["size"].as_i64().unwrap_or(0).abs() as f64;
        let left = v["left"].as_i64().unwrap_or(0).abs() as f64;
        let exec_contracts = (size - left).max(0.0);
        if exec_contracts <= 0.0 {
            if status == "finished" { return Ok((0.0, 0.0, 0.0)); }
            return Err("Not filled yet".to_string());
        }
        Ok((price, fee.abs(), exec_contracts * multiplier.max(1e-12)))
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

    async fn get_funding_rate(&self, _http: &Client, _pair: &str) -> Result<(f64, i64), String> {
        // Funding rate is non-critical background data — available from collector WS streams
        Ok((0.0, 0))
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
        let resp = send_gate_ws_request(ws_req).await?;
        let mut positions = Vec::new();
        if let Some(arr) = resp["data"]["result"].as_array() {
            for p in arr {
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
        }
        Ok(positions)
    }

    async fn fetch_orderbook(
        &self,
        _http: &Client,
        pair: &str,
        depth: u32,
    ) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), String> {
        use super::ws_gate::send_gate_ws_request;
        let contract = pair.replace("USDT", "_USDT");
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "request_id": req_id,
            "channel": "futures.order_book_query",
            "event": "api",
            "payload": [contract, depth]
        });
        let resp = send_gate_ws_request(ws_req).await?;
        let json = &resp["data"]["result"];

        let parse_levels = |levels: &Value| -> Vec<(f64, f64)> {
            let mut out = Vec::new();
            if let Some(arr) = levels.as_array() {
                for item in arr {
                    if let Some(px_str) = item["p"].as_str() {
                        let sz = item["s"].as_i64().unwrap_or(0) as f64;
                        if let Ok(px) = px_str.parse::<f64>() {
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
