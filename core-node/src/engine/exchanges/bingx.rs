//! BingX USDT-M Perpetual Swap exchange connector
//! API v3: REST for orders, WS for data
//! Auth: HMAC-SHA256 of sorted query params

use crate::engine::exchanges::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::ExchangeKeys;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;
use std::time::Duration;
use tracing::{info, warn};

pub struct BingxConnector;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn bingx_sign(secret: &str, params: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(params.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// BingX: symbol format is "BTC-USDT" (with dash separator)
fn to_bingx_symbol(pair: &str) -> String {
    if pair.ends_with("USDT") && !pair.contains('-') {
        let base = &pair[..pair.len() - 4];
        format!("{}-USDT", base)
    } else {
        pair.to_string()
    }
}

async fn bingx_request(
    http: &Client,
    keys: &ExchangeKeys,
    method: &str,
    path: &str,
    params: &str,
    body: &str,
    timeout_ms: u64,
) -> Result<Value, String> {
    let ts = now_ms().to_string();
    let sign_str = if body.is_empty() {
        format!("{}&timestamp={}", params, ts)
    } else {
        format!("{}&timestamp={}", params, ts)
    };
    let sig = bingx_sign(&keys.bingx_secret, &sign_str);

    let url = if method == "GET" {
        format!(
            "https://open-api.bingx.com{}?{}&timestamp={}&signature={}",
            path, params, ts, sig
        )
    } else {
        format!(
            "https://open-api.bingx.com{}?timestamp={}&signature={}",
            path, ts, sig
        )
    };

    let builder = match method {
        "GET" => http.get(&url),
        "POST" => http
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body.to_string()),
        "DELETE" => http.delete(&url),
        _ => return Err("Unsupported method".to_string()),
    };

    let resp = builder
        .header("X-BX-APIKEY", &keys.bingx_key)
        .timeout(Duration::from_millis(timeout_ms))
        .send()
        .await
        .map_err(|e| format!("BingX HTTP error: {}", e))?;

    let v: Value = resp
        .json()
        .await
        .map_err(|e| format!("BingX JSON parse error: {}", e))?;

    let code = v["code"].as_i64().unwrap_or(-1);
    if code != 0 {
        let msg = v["msg"].as_str().unwrap_or("unknown");
        return Err(format!("BingX API error {}: {}", code, msg));
    }
    Ok(v)
}

fn round_to_tick(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 { return price; }
    (price / tick).round() * tick
}

fn fmt(v: f64) -> String {
    if v >= 1.0 {
        format!("{:.6}", v).trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        format!("{:.10}", v).trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

#[async_trait]
impl ExchangeConnector for BingxConnector {
    fn name(&self) -> &'static str { "bingx" }

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
            return Ok((Some(format!("dry_bingx_{}", now_ms())), 0.0, 0.0));
        }
        let sym = to_bingx_symbol(pair);
        let side = if is_buy { "BUY" } else { "SELL" };
        // BingX: positionSide=LONG for buy open, SHORT for sell open
        let pos_side = if is_buy { "LONG" } else { "SHORT" };

        let slip = slippage_percent / 100.0;
        let raw_price = if is_buy { target_price * (1.0 + slip) } else { target_price * (1.0 - slip) };
        let limit_price = round_to_tick(raw_price, tick_size);

        let (order_type, price_part) = if use_ioc && limit_price > 0.0 {
            ("LIMIT", format!("&price={}", fmt(limit_price)))
        } else {
            ("MARKET", String::new())
        };

        let params = format!(
            "symbol={}&side={}&positionSide={}&type={}&quantity={}{}",
            sym, side, pos_side, order_type, fmt(phys_amount), price_part
        );

        info!("[bingx] place_order {} side={} qty={} price={}", pair, side, fmt(phys_amount), fmt(limit_price));

        let v = bingx_request(http, keys, "POST", "/openApi/swap/v2/trade/order", &params, "", api_timeout_ms).await?;
        let oid = v["data"]["order"]["orderId"].as_i64().map(|id| id.to_string());
        if let Some(ref id) = oid {
            info!("[bingx] place_order {} → orderId={}", pair, id);
            Ok((oid, 0.0, 0.0))
        } else {
            Err("BingX: no orderId in response".to_string())
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

        let sym = to_bingx_symbol(pair);
        // Closing: reverse side, reverse positionSide
        let side = if is_buy { "SELL" } else { "BUY" };
        let pos_side = if is_buy { "LONG" } else { "SHORT" };

        for attempt in 0..retries {
            let slip = slippage_percent / 100.0;
            let raw_price = if is_buy { target_price * (1.0 - slip) } else { target_price * (1.0 + slip) };
            let limit_price = round_to_tick(raw_price, tick_size);
            let (order_type, price_part) = if use_ioc && limit_price > 0.0 {
                ("LIMIT", format!("&price={}", fmt(limit_price)))
            } else {
                ("MARKET", String::new())
            };
            let params = format!(
                "symbol={}&side={}&positionSide={}&type={}&quantity={}{}",
                sym, side, pos_side, order_type, fmt(phys_amount), price_part
            );
            match bingx_request(http, keys, "POST", "/openApi/swap/v2/trade/order", &params, "", api_timeout_ms).await {
                Ok(v) => {
                    let oid = v["data"]["order"]["orderId"].as_i64().map(|id| id.to_string()).unwrap_or_default();
                    info!("[bingx] close {} attempt {} → orderId={}", pair, attempt, oid);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if let Ok((px, fee, filled)) = self.fetch_order(http, keys, pair, &oid, 1.0).await {
                        if filled > 0.0 {
                            return Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled });
                        }
                    }
                }
                Err(e) => { warn!("[bingx] close {} attempt {} failed: {}", pair, attempt, e); }
            }
            if attempt + 1 < retries {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }

        // Final fallback: market order
        let params = format!(
            "symbol={}&side={}&positionSide={}&type=MARKET&quantity={}",
            sym, side, pos_side, fmt(phys_amount)
        );
        let v = bingx_request(http, keys, "POST", "/openApi/swap/v2/trade/order", &params, "", api_timeout_ms)
            .await
            .map_err(|e| format!("BingX fallback close failed: {}", e))?;
        let oid = v["data"]["order"]["orderId"].as_i64().map(|id| id.to_string()).unwrap_or_default();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (px, fee, filled) = self.fetch_order(http, keys, pair, &oid, 1.0).await.unwrap_or((target_price, 0.0, phys_amount));
        Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled })
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
        self.close_position(http, keys, pair, is_buy, phys_excess, dry_run, 1.0, fallback_price, true, 0.0, multiplier, tick_size, 5, 100, api_timeout_ms).await
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
        let sym = to_bingx_symbol(pair);
        let params = format!("symbol={}&orderId={}", sym, order_id);
        let v = bingx_request(http, keys, "GET", "/openApi/swap/v2/trade/order", &params, "", 10_000).await?;
        let data = &v["data"]["order"];
        let filled: f64 = data["executedQty"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let avg_price: f64 = data["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let fee: f64 = data["fee"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0).abs();
        Ok((avg_price, fee, filled))
    }

    async fn fetch_balances(&self, http: &Client, keys: &ExchangeKeys) -> Result<(f64, f64), String> {
        let v = bingx_request(http, keys, "GET", "/openApi/swap/v2/user/balance", "", "", 10_000).await?;
        let equity: f64 = v["data"]["balance"]["equity"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let available: f64 = v["data"]["balance"]["availableMargin"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        Ok((equity, available))
    }

    async fn fetch_taker_fee(&self, _http: &Client, _keys: &ExchangeKeys) -> Result<f64, String> {
        Ok(0.0005) // BingX standard taker fee: 0.05%
    }

    async fn ensure_leverage(&self, http: &Client, keys: &ExchangeKeys, pair: &str, leverage: f64) -> Result<(), String> {
        let sym = to_bingx_symbol(pair);
        let params = format!("symbol={}&side=LONG&leverage={}", sym, leverage as u32);
        let _ = bingx_request(http, keys, "POST", "/openApi/swap/v2/trade/leverage", &params, "", 10_000).await;
        let params2 = format!("symbol={}&side=SHORT&leverage={}", sym, leverage as u32);
        match bingx_request(http, keys, "POST", "/openApi/swap/v2/trade/leverage", &params2, "", 10_000).await {
            Ok(_) => Ok(()),
            Err(e) => if e.contains("same") || e.contains("already") { Ok(()) } else { Err(e) },
        }
    }

    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String> {
        let sym = to_bingx_symbol(pair);
        let url = format!("https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex?symbol={}", sym);
        let resp = http.get(&url).timeout(Duration::from_secs(10)).send().await.map_err(|e| e.to_string())?;
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let rate: f64 = v["data"]["lastFundingRate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        Ok((rate, 0))
    }

    async fn fetch_positions(&self, http: &Client, keys: &ExchangeKeys) -> Result<Vec<PositionInfo>, String> {
        let v = bingx_request(http, keys, "GET", "/openApi/swap/v2/user/positions", "symbol=", "", 10_000).await?;
        let mut positions = Vec::new();
        if let Some(arr) = v["data"].as_array() {
            for p in arr {
                let sym_raw = p["symbol"].as_str().unwrap_or("").replace('-', "");
                let size: f64 = p["positionAmt"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if size.abs() < 1e-12 { continue; }
                let entry: f64 = p["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let upnl: f64 = p["unrealizedProfit"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                positions.push(PositionInfo {
                    symbol: sym_raw,
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

    async fn fetch_orderbook(&self, http: &Client, pair: &str, depth: u32) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), String> {
        let sym = to_bingx_symbol(pair);
        let url = format!("https://open-api.bingx.com/openApi/swap/v2/quote/depth?symbol={}&limit={}", sym, depth);
        let resp = http.get(&url).timeout(Duration::from_secs(5)).send().await.map_err(|e| e.to_string())?;
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let parse = |key: &str| -> Vec<(f64, f64)> {
            v["data"][key].as_array().map(|arr| {
                arr.iter().filter_map(|l| {
                    let px: f64 = l[0].as_str()?.parse().ok()?;
                    let sz: f64 = l[1].as_str()?.parse().ok()?;
                    Some((px, sz))
                }).collect()
            }).unwrap_or_default()
        };
        Ok((parse("bids"), parse("asks")))
    }
}
