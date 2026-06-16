//! Toobit USDT-M Perpetual connector
//! Auth: HMAC-SHA256, similar to Binance-style
//! API endpoint: https://api.toobit.com

use crate::engine::exchanges::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::ExchangeKeys;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;
use std::time::Duration;
use tracing::{info, warn};

pub struct ToobitConnector;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn toobit_sign(secret: &str, params: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(params.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

async fn toobit_request(
    http: &Client,
    keys: &ExchangeKeys,
    method: &str,
    path: &str,
    params: &str,
    body: &str,
    timeout_ms: u64,
) -> Result<Value, String> {
    let ts = now_ms().to_string();
    let sign_str = if params.is_empty() {
        format!("timestamp={}", ts)
    } else {
        format!("{}&timestamp={}", params, ts)
    };
    let sig = toobit_sign(&keys.toobit_secret, &sign_str);

    let url = if method == "GET" {
        if params.is_empty() {
            format!("https://api.toobit.com{}?timestamp={}&signature={}", path, ts, sig)
        } else {
            format!("https://api.toobit.com{}?{}&timestamp={}&signature={}", path, params, ts, sig)
        }
    } else {
        format!("https://api.toobit.com{}?timestamp={}&signature={}", path, ts, sig)
    };

    let builder = match method {
        "GET" => http.get(&url),
        "POST" => http.post(&url).header("Content-Type", "application/json").body(body.to_string()),
        "DELETE" => http.delete(&url),
        _ => return Err("Unsupported method".to_string()),
    };

    let resp = builder
        .header("X-BB-APIKEY", &keys.toobit_key)
        .timeout(Duration::from_millis(timeout_ms))
        .send()
        .await
        .map_err(|e| format!("Toobit HTTP error: {}", e))?;

    let v: Value = resp.json().await.map_err(|e| format!("Toobit JSON parse error: {}", e))?;
    if v.is_array() {
        return Ok(v);
    }
    let code = v["code"].as_i64().unwrap_or(-1);
    if code != 0 {
        let msg = v["msg"].as_str().unwrap_or("unknown");
        return Err(format!("Toobit API error {}: {}", code, msg));
    }
    Ok(v)
}

fn fmt(v: f64) -> String {
    format!("{:.8}", v).trim_end_matches('0').trim_end_matches('.').to_string()
}

fn round_to_tick(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 { return price; }
    (price / tick).round() * tick
}

#[async_trait]
impl ExchangeConnector for ToobitConnector {
    fn name(&self) -> &'static str { "toobit" }

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
        tick_size: f64,
        api_timeout_ms: u64,
    ) -> Result<(Option<String>, f64, f64), String> {
        if dry_run {
            return Ok((Some(format!("dry_toobit_{}", now_ms())), 0.0, 0.0));
        }
        let side = if is_buy { "BUY" } else { "SELL" };
        let slip = slippage_percent / 100.0;
        let raw_price = if is_buy { target_price * (1.0 + slip) } else { target_price * (1.0 - slip) };
        let limit_price = round_to_tick(raw_price, tick_size);

        let params = if use_ioc && limit_price > 0.0 {
            format!(
                "symbol={}&side={}&type=LIMIT&quantity={}&price={}&timeInForce=IOC&positionSide={}",
                pair, side, fmt(phys_amount), fmt(limit_price),
                if is_buy { "LONG" } else { "SHORT" }
            )
        } else {
            format!(
                "symbol={}&side={}&type=MARKET&quantity={}&positionSide={}",
                pair, side, fmt(phys_amount),
                if is_buy { "LONG" } else { "SHORT" }
            )
        };

        info!("[toobit] place_order {} side={} qty={}", pair, side, fmt(phys_amount));
        let v = toobit_request(http, keys, "POST", "/openApi/swap/v2/trade/order", &params, "", api_timeout_ms).await?;
        let oid = v["data"]["order"]["orderId"].as_i64().map(|id| id.to_string());
        if let Some(ref id) = oid {
            info!("[toobit] place_order {} → orderId={}", pair, id);
            Ok((oid, 0.0, 0.0))
        } else {
            Err("Toobit: no orderId in response".to_string())
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
        tick_size: f64,
        retries: u32,
        delay_ms: u64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        if dry_run {
            return Ok(CloseReceipt { final_price: target_price, total_fee: 0.0, closed_volume: phys_amount });
        }
        let close_side = if is_buy { "SELL" } else { "BUY" };
        let pos_side = if is_buy { "LONG" } else { "SHORT" };

        for attempt in 0..retries {
            let slip = slippage_percent / 100.0;
            let raw_price = if is_buy { target_price * (1.0 - slip) } else { target_price * (1.0 + slip) };
            let limit_price = round_to_tick(raw_price, tick_size);
            let params = if use_ioc && limit_price > 0.0 {
                format!("symbol={}&side={}&type=LIMIT&quantity={}&price={}&timeInForce=IOC&positionSide={}", pair, close_side, fmt(phys_amount), fmt(limit_price), pos_side)
            } else {
                format!("symbol={}&side={}&type=MARKET&quantity={}&positionSide={}", pair, close_side, fmt(phys_amount), pos_side)
            };
            match toobit_request(http, keys, "POST", "/openApi/swap/v2/trade/order", &params, "", api_timeout_ms).await {
                Ok(v) => {
                    let oid = v["data"]["order"]["orderId"].as_i64().map(|id| id.to_string()).unwrap_or_default();
                    info!("[toobit] close {} attempt {} → orderId={}", pair, attempt, oid);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if let Ok((px, fee, filled)) = self.fetch_order(http, keys, pair, &oid, 1.0).await {
                        if filled > 0.0 {
                            return Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled });
                        }
                    }
                }
                Err(e) => { warn!("[toobit] close {} attempt {} failed: {}", pair, attempt, e); }
            }
            if attempt + 1 < retries { tokio::time::sleep(Duration::from_millis(delay_ms)).await; }
        }
        // Fallback market close
        let params = format!("symbol={}&side={}&type=MARKET&quantity={}&positionSide={}", pair, close_side, fmt(phys_amount), pos_side);
        let v = toobit_request(http, keys, "POST", "/openApi/swap/v2/trade/order", &params, "", api_timeout_ms)
            .await.map_err(|e| format!("Toobit fallback close failed: {}", e))?;
        let oid = v["data"]["order"]["orderId"].as_i64().map(|id| id.to_string()).unwrap_or_default();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (px, fee, filled) = self.fetch_order(http, keys, pair, &oid, 1.0).await.unwrap_or((target_price, 0.0, phys_amount));
        Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled })
    }

    async fn trim_position(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, phys_excess: f64, dry_run: bool, multiplier: f64, tick_size: f64, fallback_price: f64, api_timeout_ms: u64) -> Result<CloseReceipt, String> {
        self.close_position(http, keys, pair, is_buy, phys_excess, dry_run, 1.0, fallback_price, true, 0.0, multiplier, tick_size, 5, 100, api_timeout_ms).await
    }

    async fn fetch_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, order_id: &str, _multiplier: f64) -> Result<(f64, f64, f64), String> {
        if order_id.starts_with("dry_") { return Ok((0.0, 0.0, 0.0)); }
        let params = format!("symbol={}&orderId={}", pair, order_id);
        let v = toobit_request(http, keys, "GET", "/openApi/swap/v2/trade/order", &params, "", 10_000).await?;
        let data = &v["data"]["order"];
        let filled: f64 = data["executedQty"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let avg_price: f64 = data["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let fee: f64 = data["fee"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0).abs();
        Ok((avg_price, fee, filled))
    }

    async fn fetch_balances(&self, http: &Client, keys: &ExchangeKeys) -> Result<(f64, f64), String> {
        let parse_f64 = |v: &serde_json::Value| -> Option<f64> {
            v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        };

        let v = toobit_request(http, keys, "GET", "/api/v1/futures/balance", "", "", 10_000).await?;
        let arr = v.as_array();
        let (equity, available) = if let Some(a) = arr {
            let b = a.iter().find(|x| x["asset"].as_str() == Some("USDT"));
            if let Some(bal) = b {
                let eq = parse_f64(&bal["balance"]).unwrap_or(0.0);
                let av = parse_f64(&bal["availableBalance"]).unwrap_or(eq);
                (eq, av)
            } else { (0.0, 0.0) }
        } else {
            (0.0, 0.0)
        };
        Ok((equity, available))
    }

    async fn fetch_taker_fee(&self, _http: &Client, _keys: &ExchangeKeys) -> Result<f64, String> {
        Ok(0.0006) // Toobit taker fee: 0.06%
    }

    async fn ensure_leverage(&self, http: &Client, keys: &ExchangeKeys, pair: &str, leverage: f64) -> Result<(), String> {
        let params = format!("symbol={}&side=LONG&leverage={}", pair, leverage as u32);
        let _ = toobit_request(http, keys, "POST", "/openApi/swap/v2/trade/leverage", &params, "", 10_000).await;
        let params2 = format!("symbol={}&side=SHORT&leverage={}", pair, leverage as u32);
        match toobit_request(http, keys, "POST", "/openApi/swap/v2/trade/leverage", &params2, "", 10_000).await {
            Ok(_) => Ok(()),
            Err(e) => if e.contains("same") || e.contains("already") { Ok(()) } else { Err(e) },
        }
    }


    async fn fetch_positions(&self, http: &Client, keys: &ExchangeKeys) -> Result<Vec<PositionInfo>, String> {
        let v = toobit_request(http, keys, "GET", "/openApi/swap/v2/user/positions", "symbol=", "", 10_000).await?;
        let mut positions = Vec::new();
        if let Some(arr) = v["data"].as_array() {
            for p in arr {
                let sym_raw = p["symbol"].as_str().unwrap_or("").to_string();
                let size: f64 = p["positionAmt"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if size.abs() < 1e-12 { continue; }
                let entry: f64 = p["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let upnl: f64 = p["unrealizedProfit"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                positions.push(PositionInfo {
                    symbol: sym_raw, size, entry_price: entry,
                    unrealized_pnl: upnl, realized_pnl: 0.0,
                    accumulated_fee: 0.0, accumulated_funding: 0.0,
                });
            }
        }
        Ok(positions)
    }

}
