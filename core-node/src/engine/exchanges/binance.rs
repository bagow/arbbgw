use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use super::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::{binance_sign, now_ms, ExchangeKeys};

pub struct BinanceConnector;

// ── IP BAN TRACKING ──────────────────────────────────────────────────────────
// Module-level atomic: stores the Unix timestamp (ms) until which this IP is banned.
// 0 = not banned. All Binance API calls check this before sending requests.
static BINANCE_BANNED_UNTIL_MS: AtomicI64 = AtomicI64::new(0);

/// Check if we're currently IP-banned. Returns Err if banned, Ok if clear.
fn check_ip_ban() -> Result<(), String> {
    let banned_until = BINANCE_BANNED_UNTIL_MS.load(Ordering::Relaxed);
    if banned_until > 0 {
        let now = now_ms() as i64;
        if now < banned_until {
            let remaining_s = (banned_until - now) / 1000;
            // Only log once per ~60s to avoid spam
            static LAST_BAN_LOG: AtomicI64 = AtomicI64::new(0);
            let last = LAST_BAN_LOG.load(Ordering::Relaxed);
            if now - last > 60_000 {
                LAST_BAN_LOG.store(now, Ordering::Relaxed);
                tracing::warn!(
                    "[binance] 🚫 IP banned for {}s more (until {}). All requests skipped.",
                    remaining_s, banned_until
                );
            }
            return Err(format!("Binance IP banned for {}s more", remaining_s));
        } else {
            // Ban expired — clear it
            BINANCE_BANNED_UNTIL_MS.store(0, Ordering::Relaxed);
            tracing::info!("[binance] ✅ IP ban expired. Resuming normal operations.");
        }
    }
    Ok(())
}

/// Scan any Binance JSON response for ban indicators and update the atomic.
fn detect_ban_from_response(body: &Value) {
    let code = body["code"].as_i64().unwrap_or(0);
    let msg = body["msg"].as_str().unwrap_or("");

    // -1003 = "Too many requests" / "IP banned until XXXX"
    // -1015 = "Too many orders"
    if code == -1003 || code == -1015 {
        // Try to parse "banned until TIMESTAMP" from the message
        if let Some(pos) = msg.find("banned until ") {
            let after = &msg[pos + 13..];
            let ts_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(ban_ts) = ts_str.parse::<i64>() {
                let old = BINANCE_BANNED_UNTIL_MS.load(Ordering::Relaxed);
                if ban_ts > old {
                    BINANCE_BANNED_UNTIL_MS.store(ban_ts, Ordering::Relaxed);
                    // Register in global ban registry
                    crate::engine::risk_manager::ban_exchange("binance", ban_ts);
                }
            }
        } else {
            // No explicit timestamp — apply a 2-minute cooldown
            let cooldown = now_ms() as i64 + 120_000;
            let old = BINANCE_BANNED_UNTIL_MS.load(Ordering::Relaxed);
            if cooldown > old {
                BINANCE_BANNED_UNTIL_MS.store(cooldown, Ordering::Relaxed);
                // Register in global ban registry
                crate::engine::risk_manager::ban_exchange("binance", cooldown);
            }
        }
    }
}

async fn check_rate_limits(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
    body: Option<&Value>,
) -> Result<(), String> {
    // Check response body for ban codes
    if let Some(b) = body {
        detect_ban_from_response(b);
    }

    if status.as_u16() == 429 {
        // Set a 2-minute cooldown for 429 without explicit ban timestamp
        let cooldown = now_ms() as i64 + 120_000;
        BINANCE_BANNED_UNTIL_MS.fetch_max(cooldown, Ordering::Relaxed);
        if let Some(retry_after) = headers.get("Retry-After") {
            if let Ok(sec) = retry_after.to_str().unwrap_or("").parse::<u64>() {
                tracing::error!(
                    "[binance] 🔴 RATE LIMIT HIT (429)! Auto-backoff for {} seconds.",
                    sec
                );
                let ban_ts = now_ms() as i64 + (sec as i64 * 1000);
                BINANCE_BANNED_UNTIL_MS.fetch_max(ban_ts, Ordering::Relaxed);
            }
        } else {
            tracing::error!("[binance] 🔴 RATE LIMIT HIT (429)! Auto-backoff 120s.");
        }
        return Err("Binance HTTP 429 Too Many Requests (Rate limit hit)".into());
    }
    if status.as_u16() == 418 {
        // 418 = hard IP ban, set 10 minute cooldown
        let ban_ts = now_ms() as i64 + 600_000;
        BINANCE_BANNED_UNTIL_MS.fetch_max(ban_ts, Ordering::Relaxed);
        tracing::error!("[binance] 💀 IP COMPLETELY BANNED (HTTP 418). Auto-backoff 10 min.");
        return Err("Binance HTTP 418 IP Banned".into());
    }

    if let Some(w) = headers.get("X-MBX-USED-WEIGHT-1M") {
        if let Ok(weight) = w.to_str().unwrap_or("").parse::<u32>() {
            if weight > 4000 {
                tracing::warn!("[binance] ⚠️ HIGH RATE LIMIT WEIGHT: {}/6000", weight);
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
    check_ip_ban()?;
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
                detect_ban_from_response(err);
                tracing::error!("[binance_ws] order error: {}", err);
                return Err(format!("Binance WS API Error: {}", err["msg"].as_str().unwrap_or("?")));
            }
            Ok(v_ws.get("result").cloned().unwrap_or(v_ws))
        },
        Err(e) => {
            tracing::warn!("[binance_ws] WS order failed ({}). Trying REST fallback...", e);

            // ── REST FALLBACK ──
            // Build query string for REST API (no apiKey in params for REST, it goes in header-less HMAC)
            let rest_params = params.clone();
            let mut sorted_keys: Vec<&String> = rest_params.keys().collect();
            sorted_keys.sort();
            let qs: String = sorted_keys.iter()
                .map(|k| {
                    let v = &rest_params[k.as_str()];
                    let vs = match v {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        Value::Bool(b) => b.to_string(),
                        _ => v.to_string(),
                    };
                    format!("{}={}", k, vs)
                })
                .collect::<Vec<_>>()
                .join("&");
            let sig = binance_sign(&keys.binance_secret, &qs);
            let full_qs = format!("{}&signature={}", qs, sig);

            let url = format!("https://fapi.binance.com/fapi/v1/order?{}", full_qs);
            let resp = _http.post(&url)
                .header("X-MBX-APIKEY", &keys.binance_key)
                .timeout(Duration::from_millis(_api_timeout_ms.max(5000)))
                .send()
                .await
                .map_err(|e| format!("Binance REST fallback HTTP error: {}", e))?;

            let status = resp.status();
            let headers = resp.headers().clone();
            let body: Value = resp.json().await
                .map_err(|e| format!("Binance REST fallback JSON error: {}", e))?;
            let _ = check_rate_limits(status, &headers, Some(&body)).await;

            if let Some(code) = body.get("code") {
                let code_i = code.as_i64().unwrap_or(0);
                if code_i != 0 && code_i != 200 {
                    let msg = body["msg"].as_str().unwrap_or("?");
                    tracing::error!("[binance] REST fallback order error: code={} msg={}", code_i, msg);
                    return Err(format!("Binance REST API Error: {} (code={})", msg, code_i));
                }
            }

            tracing::info!("[binance] ✅ REST fallback order succeeded: {}", body["orderId"]);
            Ok(body)
        }
    }
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
            tracing::info!(
                "[binance] IOC {} {} target={:.8} slip={}% → raw={:.8} rounded={:.8} sent={}  tick={}",
                pair, side, target_price, slippage_percent, price, rounded_price, price_str, tick_size
            );
            params.insert("type".to_string(), serde_json::json!("LIMIT"));
            params.insert("timeInForce".to_string(), serde_json::json!("IOC"));
            params.insert("price".to_string(), serde_json::json!(price_str));
        } else {
            params.insert("type".to_string(), serde_json::json!("MARKET"));
        }
        // CRITICAL: Without RESULT, Binance WS returns ACK (status=NEW) with no fill data.
        // With RESULT, the WS response contains the actual execution result inline.
        params.insert("newOrderRespType".to_string(), serde_json::json!("RESULT"));
        
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
            let resp_price = v["price"].as_str().unwrap_or("?");
            let resp_side = v["side"].as_str().unwrap_or("?");
            let resp_orig_qty = v["origQty"].as_str().unwrap_or("?");
            let resp_tif = v["timeInForce"].as_str().unwrap_or("?");
            tracing::info!(
                "[binance] place_order {} WS result: status={} execQty={} avgPrice={} fee={} | sent_price={} side={} origQty={} tif={}",
                pair, order_status, exec_qty, avg_price, cum_fee,
                resp_price, resp_side, resp_orig_qty, resp_tif
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
        _retries: u32,
        _delay_ms: u64,
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
        // CRITICAL: request RESULT response type so WS returns fill data inline
        params.insert("newOrderRespType".to_string(), serde_json::json!("RESULT"));

        let _ = self.ensure_leverage(http, keys, pair, leverage).await;

        let v_resp = execute_binance_order(http, keys, &params, api_timeout_ms).await;
        if let Err(e) = &v_resp {
            tracing::error!("[binance] normal close rejected: {}", e);
        }

        // Extract fill data directly from WS response (no REST needed)
        if let Ok(ref v) = v_resp {
            let exec_qty: f64 = v["executedQty"]
                .as_str().and_then(|s| s.parse().ok())
                .or_else(|| v["executedQty"].as_f64())
                .unwrap_or(0.0);
            let avg_price: f64 = v["avgPrice"]
                .as_str().and_then(|s| s.parse().ok())
                .or_else(|| v["avgPrice"].as_f64())
                .unwrap_or(0.0);
            let cum_fee: f64 = v["cumCommission"]
                .as_str().and_then(|s| s.parse::<f64>().ok())
                .or_else(|| v["cumCommission"].as_f64())
                .unwrap_or(0.0).abs();
            let status = v["status"].as_str().unwrap_or("?");

            tracing::info!(
                "[binance] close {} WS result: status={} execQty={} avgPrice={} fee={}",
                pair, status, exec_qty, avg_price, cum_fee
            );

            if exec_qty > 1e-12 && avg_price > 0.0 {
                return Ok(CloseReceipt {
                    final_price: avg_price,
                    total_fee: cum_fee,
                    closed_volume: exec_qty,
                });
            }
        }

        // Fallback aggressive close if IOC didn't fill
        tracing::error!(
            "[binance] fallback market close triggered for {} (qty {})",
            pair,
            phys_amount
        );
        let mut _fb_price_str = String::new();
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
                _fb_price_str = s;
            }
        }

        // CRITICAL: fallback MUST be a true MARKET order to guarantee closure.
        // Previous LIMIT IOC with 2% slippage still expired on thin pairs,
        // leaving unhedged positions open and triggering kill-switch.
        let ts_fb = now_ms();
        let mut fb_params = serde_json::Map::new();
        fb_params.insert("symbol".to_string(), serde_json::json!(pair));
        fb_params.insert("side".to_string(), serde_json::json!(side));
        fb_params.insert("type".to_string(), serde_json::json!("MARKET"));
        fb_params.insert("quantity".to_string(), serde_json::json!(qty_str));
        fb_params.insert("reduceOnly".to_string(), serde_json::json!("true"));
        fb_params.insert("timestamp".to_string(), serde_json::json!(ts_fb));
        fb_params.insert("newOrderRespType".to_string(), serde_json::json!("RESULT"));
        
        let v_fb = execute_binance_order(http, keys, &fb_params, api_timeout_ms).await.map_err(|e| format!("Fallback close rejected: {}", e))?;
        
        // Extract fill from fallback WS response
        let fb_exec_qty: f64 = v_fb["executedQty"]
            .as_str().and_then(|s| s.parse().ok())
            .or_else(|| v_fb["executedQty"].as_f64())
            .unwrap_or(0.0);
        let fb_avg_price: f64 = v_fb["avgPrice"]
            .as_str().and_then(|s| s.parse().ok())
            .or_else(|| v_fb["avgPrice"].as_f64())
            .unwrap_or(0.0);
        let fb_fee: f64 = v_fb["cumCommission"]
            .as_str().and_then(|s| s.parse::<f64>().ok())
            .or_else(|| v_fb["cumCommission"].as_f64())
            .unwrap_or(0.0).abs();

        if fb_exec_qty > 1e-12 && fb_avg_price > 0.0 {
            return Ok(CloseReceipt {
                final_price: fb_avg_price,
                total_fee: fb_fee,
                closed_volume: fb_exec_qty,
            });
        }

        tracing::warn!("[binance] fallback close unverified for {}. Assuming filled to prevent CLOSING trap.", pair);
        Ok(CloseReceipt {
            final_price: target_price,
            total_fee: 0.0,
            closed_volume: phys_amount,
        })
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
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        order_id: &str,
        _multiplier: f64,
    ) -> Result<(f64, f64, f64), String> {
        if order_id.starts_with("dry_") {
            return Ok((0.0, 0.0, 0.0));
        }
        use crate::engine::exchanges::ws_trader::send_ws_request;

        // Step 1: Get order status via WS
        let ts = now_ms();
        let oid: i64 = order_id.parse().map_err(|_| "Invalid orderId".to_string())?;
        let qs = format!("apiKey={}&orderId={}&symbol={}&timestamp={}", keys.binance_key, oid, pair, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "id": req_id,
            "method": "v1/order.status",
            "params": {
                "apiKey": keys.binance_key,
                "symbol": pair,
                "orderId": oid,
                "timestamp": ts,
                "signature": sig
            }
        });
        let v_val = match send_ws_request(ws_req).await {
            Ok(resp) => {
                if let Some(err) = resp.get("error") {
                    return Err(format!("Binance WS order.status error: {}", err));
                }
                resp["result"].clone()
            },
            Err(e) => {
                tracing::warn!("[binance] WS fetch_order failed ({}). Trying REST fallback...", e);
                let rest_qs = format!("orderId={}&symbol={}&timestamp={}", oid, pair, ts);
                let sig_rest = binance_sign(&keys.binance_secret, &rest_qs);
                let full_qs = format!("{}&signature={}", rest_qs, sig_rest);
                let url = format!("https://fapi.binance.com/fapi/v1/order?{}", full_qs);
                let resp = _http.get(&url)
                    .header("X-MBX-APIKEY", &keys.binance_key)
                    .timeout(std::time::Duration::from_millis(5000))
                    .send()
                    .await
                    .map_err(|e| format!("Binance REST fetch_order HTTP error: {}", e))?;
                resp.json::<Value>().await.map_err(|e| format!("Binance REST fetch_order JSON error: {}", e))?
            }
        };
        let v = &v_val;

        let exec_qty: f64 = v["executedQty"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let status = v["status"].as_str().unwrap_or("");

        if exec_qty < 1e-12 {
            if status == "EXPIRED" || status == "CANCELED" || status == "REJECTED" {
                tracing::warn!(
                    "[binance] fetch_order {} status={} — definitively unfilled",
                    order_id, status
                );
                return Ok((0.0, 0.0, 0.0));
            }
            return Err("Not filled yet".to_string());
        }

        let price = v["avgPrice"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        if price <= 0.0 {
            return Ok((0.0, 0.0, 0.0));
        }

        // Step 2: Fetch commission via WS myTrades
        let ts2 = now_ms();
        let qs2 = format!("apiKey={}&orderId={}&symbol={}&timestamp={}", keys.binance_key, oid, pair, ts2);
        let sig2 = binance_sign(&keys.binance_secret, &qs2);
        let req_id2 = uuid::Uuid::new_v4().to_string();
        let ws_req2 = serde_json::json!({
            "id": req_id2,
            "method": "v1/myTrades",
            "params": {
                "apiKey": keys.binance_key,
                "symbol": pair,
                "orderId": oid,
                "timestamp": ts2,
                "signature": sig2
            }
        });
        let fee = match send_ws_request(ws_req2).await {
            Ok(resp2) => {
                resp2["result"].as_array()
                    .map(|arr| {
                        arr.iter()
                            .fold(0.0_f64, |acc, t| {
                                let comm = t["commission"]
                                    .as_str()
                                    .and_then(|s| s.parse::<f64>().ok())
                                    .unwrap_or(0.0);
                                let asset = t["commissionAsset"].as_str().unwrap_or("USDT");
                                acc + match asset {
                                    "USDT" => comm,
                                    _ => {
                                        if price > 0.0 { comm * price } else { comm }
                                    }
                                }
                            })
                    })
                    .unwrap_or(0.0)
            }
            Err(_) => 0.0,
        };
        Ok((price, fee, exec_qty))
    }

    async fn fetch_balances(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<(f64, f64), String> {
        check_ip_ban()?;
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
        let result_arr = match send_ws_request(ws_req).await {
            Ok(resp) => {
                if let Some(err) = resp.get("error") {
                    detect_ban_from_response(err);
                    return Err(format!("Binance WS balance error: {}", err));
                }
                resp["result"].as_array().cloned().unwrap_or_default()
            },
            Err(e) => {
                tracing::warn!("[binance] WS balance fetch failed ({}). Trying REST fallback...", e);
                let rest_qs = format!("timestamp={}", ts);
                let sig_rest = binance_sign(&keys.binance_secret, &rest_qs);
                let full_qs = format!("{}&signature={}", rest_qs, sig_rest);
                let url = format!("https://fapi.binance.com/fapi/v2/balance?{}", full_qs);
                let resp = _http.get(&url)
                    .header("X-MBX-APIKEY", &keys.binance_key)
                    .timeout(std::time::Duration::from_millis(5000))
                    .send()
                    .await
                    .map_err(|e| format!("Binance REST balance HTTP error: {}", e))?;
                
                let body: Value = resp.json().await.map_err(|e| format!("Binance REST balance JSON error: {}", e))?;
                detect_ban_from_response(&body);
                if let Some(_code) = body.get("code") {
                    return Err(format!("Binance REST balance API error: {}", body));
                }
                body.as_array().cloned().unwrap_or_default()
            }
        };

        let mut total_balance = 0.0f64;
        let mut available_balance = 0.0f64;

        for item in result_arr {
            let asset = item["asset"].as_str().unwrap_or("");
            if asset == "USDT" {
                total_balance = item["balance"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| item["balance"].as_f64())
                    .unwrap_or(0.0);
                available_balance = item["availableBalance"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| item["availableBalance"].as_f64())
                    .unwrap_or(0.0);
                break;
            }
        }

        Ok((total_balance, available_balance))
    }

    async fn fetch_taker_fee(&self, _http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        check_ip_ban()?;
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
        match send_ws_request(ws_req).await {
            Ok(resp) => {
                if let Some(err) = resp.get("error") {
                    detect_ban_from_response(err);
                    return Err(format!("Binance WS fee error: {}", err));
                }
                resp["result"]["takerCommissionRate"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .ok_or_else(|| "Failed to parse Binance fee from WS".to_string())
            },
            Err(e) => {
                tracing::warn!("[binance] WS fee fetch failed ({}). Trying REST fallback...", e);
                let rest_qs = format!("symbol=BTCUSDT&timestamp={}", ts);
                let sig_rest = binance_sign(&keys.binance_secret, &rest_qs);
                let full_qs = format!("{}&signature={}", rest_qs, sig_rest);
                let url = format!("https://fapi.binance.com/fapi/v1/commissionRate?{}", full_qs);
                let resp = _http.get(&url)
                    .header("X-MBX-APIKEY", &keys.binance_key)
                    .timeout(std::time::Duration::from_millis(5000))
                    .send()
                    .await
                    .map_err(|e| format!("Binance REST fee HTTP error: {}", e))?;
                
                let body: Value = resp.json().await.map_err(|e| format!("Binance REST fee JSON error: {}", e))?;
                detect_ban_from_response(&body);
                body["takerCommissionRate"].as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .ok_or_else(|| format!("Failed to parse Binance fee from REST: {}", body))
            }
        }
    }

    async fn ensure_leverage(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        leverage: f64,
    ) -> Result<(), String> {
        // Early return if banned — prevents 1000s of REST requests when WS is dead
        if check_ip_ban().is_err() { return Ok(()); }
        use crate::engine::exchanges::ws_trader::send_ws_request;
        let ts = now_ms();
        let lev = leverage as i64;
        let qs = format!("apiKey={}&leverage={}&symbol={}&timestamp={}", keys.binance_key, lev, pair, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "id": req_id,
            "method": "v1/account.setLeverage",
            "params": {
                "apiKey": keys.binance_key,
                "symbol": pair,
                "leverage": lev,
                "timestamp": ts,
                "signature": sig
            }
        });
        match send_ws_request(ws_req).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // Use trace! not warn! — when Binance WS is dead this fires 1000s of times
                tracing::trace!("[binance] WS setLeverage failed ({}). Trying REST...", e);
                let rest_qs = format!("leverage={}&symbol={}&timestamp={}", lev, pair, ts);
                let sig_rest = binance_sign(&keys.binance_secret, &rest_qs);
                let full_qs = format!("{}&signature={}", rest_qs, sig_rest);
                let url = format!("https://fapi.binance.com/fapi/v1/leverage?{}", full_qs);
                let _ = _http.post(&url)
                    .header("X-MBX-APIKEY", &keys.binance_key)
                    .timeout(std::time::Duration::from_millis(5000))
                    .send()
                    .await;
                Ok(())
            }
        }
    }


    async fn fetch_positions(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<Vec<PositionInfo>, String> {
        check_ip_ban()?;
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
        let result_arr = match send_ws_request(ws_req).await {
            Ok(resp) => {
                if let Some(err) = resp.get("error") {
                    detect_ban_from_response(err);
                    return Err(format!("Binance WS position error: {}", err));
                }
                resp["result"].as_array().cloned().unwrap_or_default()
            },
            Err(e) => {
                tracing::warn!("[binance] WS positions fetch failed ({}). Trying REST fallback...", e);
                let rest_qs = format!("timestamp={}", ts);
                let sig_rest = binance_sign(&keys.binance_secret, &rest_qs);
                let full_qs = format!("{}&signature={}", rest_qs, sig_rest);
                let url = format!("https://fapi.binance.com/fapi/v2/positionRisk?{}", full_qs);
                let resp = _http.get(&url)
                    .header("X-MBX-APIKEY", &keys.binance_key)
                    .timeout(std::time::Duration::from_millis(5000))
                    .send()
                    .await
                    .map_err(|e| format!("Binance REST positions HTTP error: {}", e))?;
                
                let body: Value = resp.json().await.map_err(|e| format!("Binance REST positions JSON error: {}", e))?;
                detect_ban_from_response(&body);
                if let Some(_code) = body.get("code") {
                    return Err(format!("Binance REST positions API error: {}", body));
                }
                body.as_array().cloned().unwrap_or_default()
            }
        };

        let mut positions = Vec::new();
        for p in result_arr {
            let amt: f64 = p["positionAmt"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| p["positionAmt"].as_f64())
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
                    .or_else(|| p["entryPrice"].as_f64())
                    .unwrap_or(0.0),
                unrealized_pnl: p["unRealizedProfit"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| p["unRealizedProfit"].as_f64())
                    .unwrap_or(0.0),
                realized_pnl: 0.0,
                accumulated_fee: 0.0,
                accumulated_funding: 0.0,
            });
        }
        Ok(positions)
    }

}
