//! AscendEX (BitMax) USDT Perpetual connector
//! API v2: REST with HMAC-SHA256 auth
//! Auth: timestamp + accountGroup + path, base64-encoded HMAC-SHA256

use crate::engine::exchanges::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::ExchangeKeys;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;
use std::time::Duration;
use tracing::{info, warn};

pub struct AscendexConnector;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn ascendex_sign(secret: &str, ts: i64, path: &str) -> String {
    let msg = format!("{}+{}", ts, path);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(msg.as_bytes());
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, mac.finalize().into_bytes())
}

/// AscendEX uses "BTC/USDT" style with slash
fn to_ascendex_symbol(pair: &str) -> String {
    if pair.ends_with("USDT") && !pair.contains('/') {
        let base = &pair[..pair.len() - 4];
        format!("{}/USDT", base)
    } else {
        pair.to_string()
    }
}

const ACCOUNT_GROUP: &str = "0"; // AscendEX account group (user's is 0)
const BASE_URL: &str = "https://ascendex.com";

async fn ascendex_request(
    http: &Client,
    keys: &ExchangeKeys,
    method: &str,
    path: &str,
    body: &str,
    timeout_ms: u64,
) -> Result<Value, String> {
    let ts = now_ms();
    let sign_path = format!("v2/{}", path);
    let sig = ascendex_sign(&keys.ascendex_secret, ts, &sign_path);
    let url = format!("{}/{}/api/pro/v2/{}", BASE_URL, ACCOUNT_GROUP, path);

    let builder = match method {
        "GET" => http.get(&url),
        "POST" => http.post(&url).header("Content-Type", "application/json").body(body.to_string()),
        "DELETE" => http.delete(&url),
        _ => return Err("Unsupported method".to_string()),
    };

    let resp = builder
        .header("x-auth-key", &keys.ascendex_key)
        .header("x-auth-signature", &sig)
        .header("x-auth-timestamp", &ts.to_string())
        .timeout(Duration::from_millis(timeout_ms))
        .send()
        .await
        .map_err(|e| format!("AscendEX HTTP error: {}", e))?;

    let v: Value = resp.json().await.map_err(|e| format!("AscendEX JSON parse error: {}", e))?;
    let code = v["code"].as_i64().unwrap_or(-1);
    if code != 0 {
        let msg = v["message"].as_str().unwrap_or("unknown");
        return Err(format!("AscendEX API error {}: {}", code, msg));
    }
    Ok(v)
}

fn fmt(v: f64) -> String {
    format!("{:.8}", v).trim_end_matches('0').trim_end_matches('.').to_string()
}

#[async_trait]
impl ExchangeConnector for AscendexConnector {
    fn name(&self) -> &'static str { "ascendex" }

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
            return Ok((Some(format!("dry_ascendex_{}", now_ms())), 0.0, 0.0));
        }
        let sym = to_ascendex_symbol(pair);
        let side = if is_buy { "buy" } else { "sell" };
        let slip = slippage_percent / 100.0;
        let raw_price = if is_buy { target_price * (1.0 + slip) } else { target_price * (1.0 - slip) };

        let (order_type, price_val) = if use_ioc && raw_price > 0.0 {
            ("limit", serde_json::json!(fmt(raw_price)))
        } else {
            ("market", serde_json::json!(fmt(target_price)))
        };

        let body = serde_json::json!({
            "symbol": sym,
            "orderQty": fmt(phys_amount),
            "side": side,
            "orderType": order_type,
            "respInst": "ACK",
            "timeInForce": if use_ioc { "IOC" } else { "GTC" },
            "price": price_val
        }).to_string();

        info!("[ascendex] place_order {} side={} qty={}", pair, side, fmt(phys_amount));
        let v = ascendex_request(http, keys, "POST", "futures/order", &body, api_timeout_ms).await?;
        let oid = v["data"]["orderId"].as_str().map(|s| s.to_string());
        if let Some(ref id) = oid {
            info!("[ascendex] place_order {} → orderId={}", pair, id);
            Ok((oid, 0.0, 0.0))
        } else {
            Err("AscendEX: no orderId in response".to_string())
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
        let sym = to_ascendex_symbol(pair);
        // Closing: is_buy=true means BUY to close short; is_buy=false means SELL to close long
        let side = if is_buy { "buy" } else { "sell" };

        for attempt in 0..retries {
            let slip = slippage_percent / 100.0;
            let raw_price = if is_buy { target_price * (1.0 + slip) } else { target_price * (1.0 - slip) };
            let body = serde_json::json!({
                "symbol": sym,
                "orderQty": fmt(phys_amount),
                "side": side,
                "orderType": if use_ioc { "limit" } else { "market" },
                "timeInForce": if use_ioc { "IOC" } else { "GTC" },
                "price": fmt(raw_price),
                "reduceOnly": true
            }).to_string();

            match ascendex_request(http, keys, "POST", "futures/order", &body, api_timeout_ms).await {
                Ok(v) => {
                    let oid = v["data"]["orderId"].as_str().unwrap_or("?");
                    info!("[ascendex] close {} attempt {} → orderId={}", pair, attempt, oid);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if let Ok((px, fee, filled)) = self.fetch_order(http, keys, pair, oid, 1.0).await {
                        if filled > 0.0 {
                            return Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled });
                        }
                    }
                }
                Err(e) => { warn!("[ascendex] close {} attempt {} failed: {}", pair, attempt, e); }
            }
            if attempt + 1 < retries {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
        // Fallback market close
        let body = serde_json::json!({
            "symbol": sym, "orderQty": fmt(phys_amount),
            "side": side, "orderType": "market", "reduceOnly": true
        }).to_string();
        let v = ascendex_request(http, keys, "POST", "futures/order", &body, api_timeout_ms)
            .await.map_err(|e| format!("AscendEX fallback close failed: {}", e))?;
        let oid = v["data"]["orderId"].as_str().unwrap_or("?");
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (px, fee, filled) = self.fetch_order(http, keys, pair, oid, 1.0).await.unwrap_or((target_price, 0.0, phys_amount));
        Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled })
    }

    async fn trim_position(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, phys_excess: f64, dry_run: bool, multiplier: f64, tick_size: f64, fallback_price: f64, api_timeout_ms: u64) -> Result<CloseReceipt, String> {
        self.close_position(http, keys, pair, is_buy, phys_excess, dry_run, 1.0, fallback_price, true, 0.0, multiplier, tick_size, 5, 100, api_timeout_ms).await
    }

    async fn fetch_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, order_id: &str, _multiplier: f64) -> Result<(f64, f64, f64), String> {
        if order_id.starts_with("dry_") { return Ok((0.0, 0.0, 0.0)); }
        let sym = to_ascendex_symbol(pair);
        let path = format!("futures/order/status?orderId={}&symbol={}", order_id, sym);
        let v = ascendex_request(http, keys, "GET", &path, "", 10_000).await?;
        let data = &v["data"];
        let filled: f64 = data["cumFilledQty"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let avg_price: f64 = data["avgPx"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let fee: f64 = data["cumFee"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0).abs();
        Ok((avg_price, fee, filled))
    }

    async fn fetch_balances(&self, http: &Client, keys: &ExchangeKeys) -> Result<(f64, f64), String> {
        let parse_f64 = |v: &serde_json::Value| -> Option<f64> {
            v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        };

        let v = ascendex_request(http, keys, "GET", "futures/position", "", 10_000).await?;
        let mut equity = 0.0;
        let mut available = 0.0;
        if let Some(colls) = v["data"]["collaterals"].as_array() {
            if let Some(b) = colls.iter().find(|c| c["asset"].as_str() == Some("USDT")) {
                equity = parse_f64(&b["balance"]).unwrap_or(0.0);
                available = equity; // AscendEX collateral balance is total
            }
        }
        Ok((equity, available))
    }

    async fn fetch_taker_fee(&self, _http: &Client, _keys: &ExchangeKeys) -> Result<f64, String> {
        Ok(0.0006) // AscendEX taker fee: 0.06%
    }

    async fn ensure_leverage(&self, http: &Client, keys: &ExchangeKeys, pair: &str, leverage: f64) -> Result<(), String> {
        let sym = to_ascendex_symbol(pair);
        let body = serde_json::json!({ "symbol": sym, "leverage": leverage as u32 }).to_string();
        match ascendex_request(http, keys, "POST", "futures/leverage", &body, 10_000).await {
            Ok(_) => Ok(()),
            Err(e) => if e.contains("same") || e.contains("already") { Ok(()) } else { Err(e) },
        }
    }


    async fn fetch_positions(&self, http: &Client, keys: &ExchangeKeys) -> Result<Vec<PositionInfo>, String> {
        let v = ascendex_request(http, keys, "GET", "futures/position", "", 10_000).await?;
        let mut positions = Vec::new();
        if let Some(arr) = v["data"].as_array() {
            for p in arr {
                let sym_raw = p["symbol"].as_str().unwrap_or("").replace('/', "");
                let size: f64 = p["size"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if size.abs() < 1e-12 { continue; }
                let side = p["side"].as_str().unwrap_or("");
                let signed = if side == "Short" { -size } else { size };
                let entry: f64 = p["avgEntryPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
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

}
