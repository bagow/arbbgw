//! XT.COM USDT-M Perpetual connector
//! API v4: REST with HMAC-SHA256 auth
//! Auth: sign = HMAC-SHA256(secret, "xt-validate-appkey=KEY&xt-validate-timestamp=TS&xt-validate-recvwindow=5000&xt-validate-algorithms=HmacSHA256&body=BODY")

use crate::engine::exchanges::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::ExchangeKeys;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;
use std::time::Duration;
use tracing::{info, warn};

pub struct XtConnector;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn xt_sign(secret: &str, api_key: &str, ts: i64, body: &str) -> String {
    let recv_window = "5000";
    let msg = format!(
        "xt-validate-appkey={}&xt-validate-timestamp={}&xt-validate-recvwindow={}&xt-validate-algorithms=HmacSHA256{}",
        api_key, ts, recv_window,
        if body.is_empty() { String::new() } else { format!("&body={}", body) }
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(msg.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// XT.COM uses underscore-separated lowercase: "btc_usdt"
fn to_xt_symbol(pair: &str) -> String {
    if pair.ends_with("USDT") && !pair.contains('_') {
        let base = &pair[..pair.len() - 4];
        format!("{}_usdt", base.to_lowercase())
    } else {
        pair.to_lowercase()
    }
}

async fn xt_request(
    http: &Client,
    keys: &ExchangeKeys,
    method: &str,
    path: &str,
    body: &str,
    timeout_ms: u64,
) -> Result<Value, String> {
    let ts = now_ms();
    let sig = xt_sign(&keys.xt_secret, &keys.xt_key, ts, body);
    let url = format!("https://fapi.xt.com{}", path);

    let builder = match method {
        "GET" => http.get(&url),
        "POST" => http.post(&url).header("Content-Type", "application/json").body(body.to_string()),
        "DELETE" => http.delete(&url),
        _ => return Err("Unsupported method".to_string()),
    };

    let resp = builder
        .header("xt-validate-appkey", &keys.xt_key)
        .header("xt-validate-timestamp", &ts.to_string())
        .header("xt-validate-recvwindow", "5000")
        .header("xt-validate-algorithms", "HmacSHA256")
        .header("xt-validate-signature", &sig)
        .timeout(Duration::from_millis(timeout_ms))
        .send()
        .await
        .map_err(|e| format!("XT HTTP error: {}", e))?;

    let v: Value = resp.json().await.map_err(|e| format!("XT JSON parse error: {}", e))?;
    let result_code = v["returnCode"].as_i64().unwrap_or(-1);
    if result_code != 0 {
        let msg = v["msgInfo"].as_str().unwrap_or("unknown");
        return Err(format!("XT API error {}: {}", result_code, msg));
    }
    Ok(v)
}

fn fmt(v: f64) -> String {
    format!("{:.8}", v).trim_end_matches('0').trim_end_matches('.').to_string()
}

#[async_trait]
impl ExchangeConnector for XtConnector {
    fn name(&self) -> &'static str { "xt" }

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
        _multiplier: f64,
        slippage_percent: f64,
        _tick_size: f64,
        api_timeout_ms: u64,
    ) -> Result<(Option<String>, f64, f64), String> {
        if dry_run {
            return Ok((Some(format!("dry_xt_{}", now_ms())), 0.0, 0.0));
        }
        let sym = to_xt_symbol(pair);
        let side = if is_buy { "BUY" } else { "SELL" };
        let slip = slippage_percent / 100.0;
        let raw_price = if is_buy { target_price * (1.0 + slip) } else { target_price * (1.0 - slip) };

        let body = if use_ioc && raw_price > 0.0 {
            serde_json::json!({
                "symbol": sym, "side": side, "entrustType": "LIMIT",
                "origQty": fmt(phys_amount), "price": fmt(raw_price),
                "timeInForce": "IOC", "positionSide": if is_buy { "LONG" } else { "SHORT" }
            }).to_string()
        } else {
            serde_json::json!({
                "symbol": sym, "side": side, "entrustType": "MARKET",
                "origQty": fmt(phys_amount),
                "positionSide": if is_buy { "LONG" } else { "SHORT" }
            }).to_string()
        };

        info!("[xt] place_order {} side={} qty={}", pair, side, fmt(phys_amount));
        let v = xt_request(http, keys, "POST", "/future/trade/v1/entrust/create-plan", &body, api_timeout_ms).await?;
        let oid = v["result"].as_str().map(|s| s.to_string());
        if let Some(ref id) = oid {
            info!("[xt] place_order {} → orderId={}", pair, id);
            Ok((oid, 0.0, 0.0))
        } else {
            Err("XT: no orderId in response".to_string())
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
        _leverage: f64,
        target_price: f64,
        use_ioc: bool,
        slippage_percent: f64,
        _multiplier: f64,
        _tick_size: f64,
        retries: u32,
        delay_ms: u64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        if dry_run {
            return Ok(CloseReceipt { final_price: target_price, total_fee: 0.0, closed_volume: phys_amount });
        }
        let sym = to_xt_symbol(pair);
        let close_side = if is_buy { "SELL" } else { "BUY" };
        let pos_side = if is_buy { "LONG" } else { "SHORT" };

        for attempt in 0..retries {
            let slip = slippage_percent / 100.0;
            let raw_price = if is_buy { target_price * (1.0 - slip) } else { target_price * (1.0 + slip) };
            let body = if use_ioc && raw_price > 0.0 {
                serde_json::json!({
                    "symbol": sym, "side": close_side, "entrustType": "LIMIT",
                    "origQty": fmt(phys_amount), "price": fmt(raw_price),
                    "timeInForce": "IOC", "positionSide": pos_side
                }).to_string()
            } else {
                serde_json::json!({
                    "symbol": sym, "side": close_side, "entrustType": "MARKET",
                    "origQty": fmt(phys_amount), "positionSide": pos_side
                }).to_string()
            };
            match xt_request(http, keys, "POST", "/future/trade/v1/entrust/create-plan", &body, api_timeout_ms).await {
                Ok(v) => {
                    let oid = v["result"].as_str().unwrap_or("?");
                    info!("[xt] close {} attempt {} → orderId={}", pair, attempt, oid);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if let Ok((px, fee, filled)) = self.fetch_order(http, keys, pair, oid, 1.0).await {
                        if filled > 0.0 {
                            return Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled });
                        }
                    }
                }
                Err(e) => { warn!("[xt] close {} attempt {} failed: {}", pair, attempt, e); }
            }
            if attempt + 1 < retries { tokio::time::sleep(Duration::from_millis(delay_ms)).await; }
        }
        // Fallback
        let body = serde_json::json!({
            "symbol": sym, "side": close_side, "entrustType": "MARKET",
            "origQty": fmt(phys_amount), "positionSide": pos_side
        }).to_string();
        let v = xt_request(http, keys, "POST", "/future/trade/v1/entrust/create-plan", &body, api_timeout_ms)
            .await.map_err(|e| format!("XT fallback close failed: {}", e))?;
        let oid = v["result"].as_str().unwrap_or("?");
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (px, fee, filled) = self.fetch_order(http, keys, pair, oid, 1.0).await.unwrap_or((target_price, 0.0, phys_amount));
        Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled })
    }

    async fn trim_position(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, phys_excess: f64, dry_run: bool, multiplier: f64, tick_size: f64, fallback_price: f64, api_timeout_ms: u64) -> Result<CloseReceipt, String> {
        self.close_position(http, keys, pair, is_buy, phys_excess, dry_run, 1.0, fallback_price, true, 0.0, multiplier, tick_size, 5, 100, api_timeout_ms).await
    }

    async fn fetch_order(&self, http: &Client, keys: &ExchangeKeys, _pair: &str, order_id: &str, _multiplier: f64) -> Result<(f64, f64, f64), String> {
        if order_id.starts_with("dry_") { return Ok((0.0, 0.0, 0.0)); }
        let path = format!("/future/trade/v1/entrust/detail?entrustId={}", order_id);
        let v = xt_request(http, keys, "GET", &path, "", 10_000).await?;
        let data = &v["result"];
        let filled: f64 = data["dealQty"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let avg_price: f64 = data["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let fee: f64 = data["fee"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0).abs();
        Ok((avg_price, fee, filled))
    }

    async fn fetch_balances(&self, http: &Client, keys: &ExchangeKeys) -> Result<(f64, f64), String> {
        let v = xt_request(http, keys, "GET", "/future/user/v1/balance/list", "", 10_000).await?;
        let equity: f64 = v["result"].as_array()
            .and_then(|arr| arr.iter().find(|b| b["coin"].as_str() == Some("usdt")))
            .and_then(|b| b["accountBalance"].as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let available: f64 = v["result"].as_array()
            .and_then(|arr| arr.iter().find(|b| b["coin"].as_str() == Some("usdt")))
            .and_then(|b| b["availableBalance"].as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        Ok((equity, available))
    }

    async fn fetch_taker_fee(&self, _http: &Client, _keys: &ExchangeKeys) -> Result<f64, String> {
        Ok(0.0006) // XT.COM taker fee: 0.06%
    }

    async fn ensure_leverage(&self, http: &Client, keys: &ExchangeKeys, pair: &str, leverage: f64) -> Result<(), String> {
        let sym = to_xt_symbol(pair);
        let body = serde_json::json!({ "symbol": sym, "leverage": leverage as u32 }).to_string();
        match xt_request(http, keys, "POST", "/future/user/v1/position/adjust-leverage", &body, 10_000).await {
            Ok(_) => Ok(()),
            Err(e) => if e.contains("same") || e.contains("already") { Ok(()) } else { Err(e) },
        }
    }

    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String> {
        let sym = to_xt_symbol(pair);
        let url = format!("https://fapi.xt.com/future/market/v1/public/q/index-funding-rate?symbol={}", sym);
        let resp = http.get(&url).timeout(Duration::from_secs(10)).send().await.map_err(|e| e.to_string())?;
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let rate: f64 = v["result"]["lastFundingRate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        Ok((rate, 0))
    }

    async fn fetch_positions(&self, http: &Client, keys: &ExchangeKeys) -> Result<Vec<PositionInfo>, String> {
        let v = xt_request(http, keys, "GET", "/future/user/v1/position/list", "", 10_000).await?;
        let mut positions = Vec::new();
        if let Some(arr) = v["result"].as_array() {
            for p in arr {
                let sym_raw = p["symbol"].as_str().unwrap_or("").replace('_', "").to_uppercase();
                let size: f64 = p["positionAmt"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if size.abs() < 1e-12 { continue; }
                let side = p["positionSide"].as_str().unwrap_or("");
                let signed = if side == "SHORT" { -size.abs() } else { size.abs() };
                let entry: f64 = p["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let upnl: f64 = p["unrealizedPnl"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                positions.push(PositionInfo {
                    symbol: sym_raw, size: signed, entry_price: entry,
                    unrealized_pnl: upnl, realized_pnl: 0.0,
                    accumulated_fee: 0.0, accumulated_funding: 0.0,
                });
            }
        }
        Ok(positions)
    }

    async fn fetch_orderbook(&self, http: &Client, pair: &str, depth: u32) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), String> {
        let sym = to_xt_symbol(pair);
        let url = format!("https://fapi.xt.com/future/market/v1/public/q/depth?symbol={}&level={}", sym, depth.min(50));
        let resp = http.get(&url).timeout(Duration::from_secs(5)).send().await.map_err(|e| e.to_string())?;
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let parse = |key: &str| -> Vec<(f64, f64)> {
            v["result"][key].as_array().map(|arr| {
                arr.iter().filter_map(|l| {
                    let px: f64 = l[0].as_str()?.parse().ok()?;
                    let sz: f64 = l[1].as_str()?.parse().ok()?;
                    Some((px, sz))
                }).collect()
            }).unwrap_or_default()
        };
        Ok((parse("b"), parse("a")))
    }
}
