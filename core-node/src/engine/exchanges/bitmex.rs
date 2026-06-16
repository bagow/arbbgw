//! BitMEX USDT/XBT Perpetual connector
//! Auth: HMAC-SHA256, hex-encoded
//! API key in "api-key" header, signature in "api-signature"

use crate::engine::exchanges::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::ExchangeKeys;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;
use std::time::Duration;
use tracing::{info, warn};

pub struct BitmexConnector;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn bitmex_sign(secret: &str, verb: &str, path: &str, expires: i64, body: &str) -> String {
    let data = format!("{}{}{}{}", verb, path, expires, body);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(data.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// BitMEX uses "XBTUSD" style internally; for USDT perps the symbol is "BTCUSDT"
fn to_bitmex_symbol(pair: &str) -> String {
    if pair == "BTCUSDT" {
        "XBTUSDT".to_string()
    } else {
        pair.to_string()
    }
}

async fn bitmex_request(
    http: &Client,
    keys: &ExchangeKeys,
    method: &str,
    path: &str,
    body: &str,
    timeout_ms: u64,
) -> Result<Value, String> {
    let expires = (now_ms() / 1000) + 60;
    let sig = bitmex_sign(&keys.bitmex_secret, method, path, expires, body);
    let url = format!("https://www.bitmex.com{}", path);

    let builder = match method {
        "GET" => http.get(&url),
        "POST" => http.post(&url).header("Content-Type", "application/json").body(body.to_string()),
        "DELETE" => http.delete(&url),
        _ => return Err("Unsupported method".to_string()),
    };

    let resp = builder
        .header("api-key", &keys.bitmex_key)
        .header("api-signature", &sig)
        .header("api-expires", &expires.to_string())
        .timeout(Duration::from_millis(timeout_ms))
        .send()
        .await
        .map_err(|e| format!("BitMEX HTTP error: {}", e))?;

    let status = resp.status().as_u16();
    let v: Value = resp.json().await.map_err(|e| format!("BitMEX JSON parse error: {}", e))?;

    if status >= 400 {
        let msg = v["error"]["message"].as_str().unwrap_or("unknown");
        return Err(format!("BitMEX API error {}: {}", status, msg));
    }
    Ok(v)
}

fn fmt(v: f64) -> String {
    format!("{:.8}", v).trim_end_matches('0').trim_end_matches('.').to_string()
}

#[async_trait]
impl ExchangeConnector for BitmexConnector {
    fn name(&self) -> &'static str { "bitmex" }

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
            return Ok((Some(format!("dry_bitmex_{}", now_ms())), 0.0, 0.0));
        }

        super::ws_bitmex_private::ensure_private_ws(&keys.bitmex_key, &keys.bitmex_secret).await;

        let sym = to_bitmex_symbol(pair);
        let side = if is_buy { "Buy" } else { "Sell" };
        let slip = slippage_percent / 100.0;
        let raw_price = if is_buy { target_price * (1.0 + slip) } else { target_price * (1.0 - slip) };

        let body = if use_ioc {
            serde_json::json!({
                "symbol": sym,
                "side": side,
                "orderQty": phys_amount,
                "price": raw_price,
                "ordType": "Limit",
                "execInst": "ParticipateDoNotInitiate"
            }).to_string()
        } else {
            serde_json::json!({
                "symbol": sym,
                "side": side,
                "orderQty": phys_amount,
                "ordType": "Market"
            }).to_string()
        };

        info!("[bitmex] place_order {} side={} qty={}", pair, side, fmt(phys_amount));
        let v = bitmex_request(http, keys, "POST", "/api/v1/order", &body, api_timeout_ms).await?;
        let oid = v["orderID"].as_str().map(|s| s.to_string());
        if let Some(ref id) = oid {
            info!("[bitmex] place_order {} → orderId={}", pair, id);
            Ok((oid, 0.0, 0.0))
        } else {
            Err("BitMEX: no orderID in response".to_string())
        }
    }

    async fn close_position(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        _is_buy: bool,
        _phys_amount: f64,
        dry_run: bool,
        _leverage: f64,
        target_price: f64,
        _use_ioc: bool,
        _slippage_percent: f64,
        _multiplier: f64,
        _tick_size: f64,
        retries: u32,
        delay_ms: u64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        if dry_run {
            return Ok(CloseReceipt { final_price: target_price, total_fee: 0.0, closed_volume: 0.0 });
        }

        super::ws_bitmex_private::ensure_private_ws(&keys.bitmex_key, &keys.bitmex_secret).await;

        let sym = to_bitmex_symbol(pair);
        // BitMEX "Close" instruction closes the entire position
        let body = serde_json::json!({
            "symbol": sym,
            "execInst": "Close"
        }).to_string();

        for attempt in 0..retries {
            match bitmex_request(http, keys, "POST", "/api/v1/order", &body, api_timeout_ms).await {
                Ok(v) => {
                    let oid = v["orderID"].as_str().unwrap_or("?");
                    info!("[bitmex] close {} attempt {} → orderId={}", pair, attempt, oid);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if let Ok((px, fee, filled)) = self.fetch_order(http, keys, pair, oid, 1.0).await {
                        if filled > 0.0 {
                            return Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled });
                        }
                    }
                }
                Err(e) => { warn!("[bitmex] close {} attempt {} failed: {}", pair, attempt, e); }
            }
            if attempt + 1 < retries {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
        Ok(CloseReceipt { final_price: target_price, total_fee: 0.0, closed_volume: 0.0 })
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
        self.close_position(http, keys, pair, is_buy, phys_excess, dry_run, 1.0, fallback_price, false, 0.0, multiplier, tick_size, 5, 100, api_timeout_ms).await
    }

    async fn fetch_order(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        order_id: &str,
        _multiplier: f64,
    ) -> Result<(f64, f64, f64), String> {
        if order_id.starts_with("dry_") { return Ok((0.0, 0.0, 0.0)); }
        
        // 1. Fast path: Wait for WS execution report (up to 1000ms)
        let cache = super::ws_bitmex_private::get_order_cache();
        for _ in 0..50 {
            if let Some(state) = cache.get(order_id) {
                if state.status == "Filled" || state.status == "PartiallyFilled" || state.status == "Canceled" || state.cum_qty > 0.0 {
                    tracing::debug!("[bitmex] fetch_order {} resolved via WS in <1s!", order_id);
                    let fee = state.fee;
                    let filled = state.cum_qty;
                    let avg_price = state.avg_px;
                    if filled < 1e-12 && state.status == "Canceled" {
                        return Ok((0.0, 0.0, 0.0));
                    }
                    return Ok((avg_price, fee, filled));
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // 2. Slow path: Fallback to REST API
        let sym = to_bitmex_symbol(pair);
        let path = format!("/api/v1/order?filter={{\"orderID\":\"{}\",\"symbol\":\"{}\"}}", order_id, sym);
        let v = bitmex_request(http, keys, "GET", &path, "", 10_000).await?;
        if let Some(arr) = v.as_array() {
            if let Some(o) = arr.first() {
                let filled: f64 = o["cumQty"].as_f64().unwrap_or(0.0);
                let avg_price: f64 = o["avgPx"].as_f64().unwrap_or(0.0);
                let commission: f64 = o["commission"].as_f64().unwrap_or(0.0).abs();
                let fee = commission * avg_price * filled;
                return Ok((avg_price, fee, filled));
            }
        }
        Ok((0.0, 0.0, 0.0))
    }

    async fn fetch_balances(&self, http: &Client, keys: &ExchangeKeys) -> Result<(f64, f64), String> {
        let parse_f64 = |v: &serde_json::Value| -> Option<f64> {
            v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        };

        let v = bitmex_request(http, keys, "GET", "/api/v1/user/margin?currency=USDt", "", 10_000).await?;
        let wallet_balance: f64 = parse_f64(&v["walletBalance"]).unwrap_or(0.0) / 1_000_000.0; // satoshis → USD
        let available_margin: f64 = parse_f64(&v["availableMargin"]).unwrap_or(0.0) / 1_000_000.0;
        Ok((wallet_balance, available_margin))
    }

    async fn fetch_taker_fee(&self, _http: &Client, _keys: &ExchangeKeys) -> Result<f64, String> {
        Ok(0.00075) // BitMEX taker fee: 0.075%
    }

    async fn ensure_leverage(&self, http: &Client, keys: &ExchangeKeys, pair: &str, leverage: f64) -> Result<(), String> {
        let sym = to_bitmex_symbol(pair);
        let body = serde_json::json!({ "symbol": sym, "leverage": leverage }).to_string();
        match bitmex_request(http, keys, "POST", "/api/v1/position/leverage", &body, 10_000).await {
            Ok(_) => Ok(()),
            Err(e) => if e.contains("same") || e.contains("already") { Ok(()) } else { Err(e) },
        }
    }


    async fn fetch_positions(&self, http: &Client, keys: &ExchangeKeys) -> Result<Vec<PositionInfo>, String> {
        let v = bitmex_request(http, keys, "GET", "/api/v1/position?filter={\"isOpen\":true}", "", 10_000).await?;
        let mut positions = Vec::new();
        if let Some(arr) = v.as_array() {
            for p in arr {
                let sym_raw = p["symbol"].as_str().unwrap_or("").to_string();
                // Normalize XBTUSDT → BTCUSDT
                let sym = if sym_raw == "XBTUSDT" { "BTCUSDT".to_string() } else { sym_raw };
                let size: f64 = p["currentQty"].as_f64().unwrap_or(0.0);
                if size.abs() < 1e-12 { continue; }
                let entry: f64 = p["avgEntryPrice"].as_f64().unwrap_or(0.0);
                let upnl: f64 = p["unrealisedPnl"].as_f64().unwrap_or(0.0) / 1_000_000.0;
                positions.push(PositionInfo {
                    symbol: sym,
                    size,
                    entry_price: entry,
                    unrealized_pnl: upnl,
                    realized_pnl: 0.0,
                    accumulated_fee: 0.0,
                    accumulated_funding: 0.0,
                });
            }
        }
        Ok(positions)
    }

}
