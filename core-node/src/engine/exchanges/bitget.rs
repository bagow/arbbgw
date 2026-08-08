//! Bitget USDT-FUTURES exchange connector
//! API v2: REST for orders, WS for data
//! Auth: HMAC-SHA256 with passphrase
use crate::engine::exchanges::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::ExchangeKeys;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;
use std::time::Duration;
use tracing::{info, warn};

pub struct BitgetConnector;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Bitget signature: HMAC-SHA256(secret, timestamp + method + requestPath + body)
/// Result is Base64-encoded
fn bitget_sign(secret: &str, timestamp: &str, method: &str, path: &str, body: &str) -> String {
    let prehash = format!("{}{}{}{}", timestamp, method, path, body);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(prehash.as_bytes());
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, mac.finalize().into_bytes())
}

/// Send authenticated request to Bitget
async fn bitget_request(
    http: &Client,
    keys: &ExchangeKeys,
    method: &str,
    path: &str,
    body: &str,
    timeout_ms: u64,
) -> Result<Value, String> {
    let ts = now_ms().to_string();
    let sig = bitget_sign(&keys.bitget_secret, &ts, method, path, body);

    let url = format!("https://api.bitget.com{}", path);
    let builder = match method {
        "GET" => http.get(&url),
        "POST" => http
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body.to_string()),
        _ => return Err("Unsupported method".to_string()),
    };

    let resp = builder
        .header("ACCESS-KEY", &keys.bitget_key)
        .header("ACCESS-SIGN", &sig)
        .header("ACCESS-TIMESTAMP", &ts)
        .header("ACCESS-PASSPHRASE", &keys.bitget_passphrase)
        .header("locale", "en-US")
        .timeout(Duration::from_millis(timeout_ms))
        .send()
        .await
        .map_err(|e| format!("Bitget HTTP error: {}", e))?;

    let v: Value = resp
        .json()
        .await
        .map_err(|e| format!("Bitget JSON parse error: {}", e))?;

    let code = v["code"].as_str().unwrap_or("");
    if code != "00000" {
        let msg = v["msg"].as_str().unwrap_or("unknown");
        return Err(format!("Bitget API error {}: {}", code, msg));
    }
    Ok(v)
}

fn round_to_tick(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price;
    }
    (price / tick).round() * tick
}

fn format_price(price: f64) -> String {
    if price >= 1.0 {
        format!("{:.6}", price)
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    } else {
        format!("{:.10}", price)
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

#[async_trait]
impl ExchangeConnector for BitgetConnector {
    fn name(&self) -> &'static str {
        "bitget"
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
        if dry_run {
            return Ok((Some(format!("dry_bitget_{}", now_ms())), 0.0, 0.0));
        }

        let side = if is_buy { "buy" } else { "sell" };
        // Bitget: tradeSide for opening = "open"
        let trade_side = "open";

        let slip = slippage_percent / 100.0;
        let raw_price = if is_buy {
            target_price * (1.0 + slip)
        } else {
            target_price * (1.0 - slip)
        };
        let limit_price = round_to_tick(raw_price, tick_size);

        // Bitget uses base coin quantity for USDT-FUTURES
        let qty = if multiplier > 0.0 && multiplier != 1.0 {
            (phys_amount / multiplier).round() * multiplier
        } else {
            phys_amount
        };
        let qty_str = format_price(qty);
        let price_str = format_price(limit_price);

        let order_type = if use_ioc { "limit" } else { "market" };
        let force = if use_ioc { "ioc" } else { "gtc" };

        let body = if use_ioc {
            serde_json::json!({
                "symbol": pair,
                "productType": "USDT-FUTURES",
                "marginMode": "crossed",
                "marginCoin": "USDT",
                "side": side,
                "tradeSide": trade_side,
                "orderType": order_type,
                "force": force,
                "price": price_str,
                "size": qty_str
            })
            .to_string()
        } else {
            serde_json::json!({
                "symbol": pair,
                "productType": "USDT-FUTURES",
                "marginMode": "crossed",
                "marginCoin": "USDT",
                "side": side,
                "tradeSide": trade_side,
                "orderType": "market",
                "size": qty_str
            })
            .to_string()
        };

        info!(
            "[bitget] place_order {} side={} qty={} price={} ioc={}",
            pair, side, qty_str, price_str, use_ioc
        );

        let v = bitget_request(
            http,
            keys,
            "POST",
            "/api/v2/mix/order/place-order",
            &body,
            api_timeout_ms,
        )
        .await?;

        let order_id = v["data"]["orderId"]
            .as_str()
            .map(|s| s.to_string());

        if let Some(ref oid) = order_id {
            info!("[bitget] place_order {} → orderId={}", pair, oid);
            Ok((order_id, 0.0, 0.0))
        } else {
            Err("Bitget: no orderId in response".to_string())
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
        multiplier: f64,
        tick_size: f64,
        retries: u32,
        delay_ms: u64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        if dry_run {
            return Ok(CloseReceipt {
                final_price: target_price,
                total_fee: 0.0,
                closed_volume: phys_amount,
            });
        }

        super::ws_bitget_private::ensure_private_ws(&keys.bitget_key, &keys.bitget_secret, &keys.bitget_passphrase).await;

        let side = if is_buy { "buy" } else { "sell" };
        let trade_side = "close";
        // Hedge Mode requires holdSide to know which position to close
        let hold_side = if is_buy { "short" } else { "long" };

        for attempt in 0..retries {
            let slip = slippage_percent / 100.0;
            let raw_price = if is_buy {
                target_price * (1.0 + slip)
            } else {
                target_price * (1.0 - slip)
            };
            let limit_price = round_to_tick(raw_price, tick_size);
            let qty = if multiplier > 0.0 && multiplier != 1.0 {
                (phys_amount / multiplier).round() * multiplier
            } else {
                phys_amount
            };
            let qty_str = format_price(qty);
            let price_str = format_price(limit_price);

            // Exit always uses IOC limit when target_price is available.
            // use_ioc controls entry order type, not exit — exit must never be a pure market order.
            let body = if target_price > 0.0 {
                serde_json::json!({
                    "symbol": pair,
                    "productType": "USDT-FUTURES",
                    "marginMode": "crossed",
                    "marginCoin": "USDT",
                    "side": side,
                    "tradeSide": trade_side,
                    "holdSide": hold_side,
                    "orderType": "limit",
                    "force": "ioc",
                    "price": price_str,
                    "size": qty_str
                })
                .to_string()
            } else {
                serde_json::json!({
                    "symbol": pair,
                    "productType": "USDT-FUTURES",
                    "marginMode": "crossed",
                    "marginCoin": "USDT",
                    "side": side,
                    "tradeSide": trade_side,
                    "holdSide": hold_side,
                    "orderType": "market",
                    "size": qty_str
                })
                .to_string()
            };

            match bitget_request(http, keys, "POST", "/api/v2/mix/order/place-order", &body, api_timeout_ms).await {
                Ok(v) => {
                    let oid = v["data"]["orderId"].as_str().unwrap_or("?");
                    info!("[bitget] close {} attempt {} → orderId={}", pair, attempt, oid);

                    // Fetch fill info
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    if let Ok((px, fee, filled)) = self.fetch_order(http, keys, pair, oid, multiplier).await {
                        if filled > 0.0 {
                            return Ok(CloseReceipt {
                                final_price: px,
                                total_fee: fee,
                                closed_volume: filled,
                            });
                        }
                    }
                }
                Err(e) => {
                    warn!("[bitget] close {} attempt {} failed: {}", pair, attempt, e);
                    // 22002 = No position to close with given params (marginMode/holdSide mismatch)
                    // Use Bitget Flash Close API which auto-detects all params
                    if e.contains("22002") || e.to_lowercase().contains("no position") {
                        // 22002 = holdSide/marginMode mismatch (hedge params on one-way account).
                        // Retry as one-way mode close: use tradeSide="close" + reduceOnly
                        // WITHOUT holdSide. Bitget API v2 requires tradeSide for one-way mode.
                        info!("[bitget] close {} — retrying as one-way mode (tradeSide=close, no holdSide)", pair);
                        let oneway_body = if target_price > 0.0 {
                            serde_json::json!({
                                "symbol": pair,
                                "productType": "USDT-FUTURES",
                                "marginMode": "crossed",
                                "marginCoin": "USDT",
                                "side": side,
                                "tradeSide": "close",
                                "orderType": "limit",
                                "force": "ioc",
                                "price": price_str,
                                "size": qty_str,
                                "reduceOnly": "YES"
                            })
                            .to_string()
                        } else {
                            serde_json::json!({
                                "symbol": pair,
                                "productType": "USDT-FUTURES",
                                "marginMode": "crossed",
                                "marginCoin": "USDT",
                                "side": side,
                                "tradeSide": "close",
                                "orderType": "market",
                                "size": qty_str,
                                "reduceOnly": "YES"
                            })
                            .to_string()
                        };
                        match bitget_request(http, keys, "POST", "/api/v2/mix/order/place-order", &oneway_body, api_timeout_ms).await {
                            Ok(v) => {
                                let oid = v["data"]["orderId"].as_str().unwrap_or("?");
                                info!("[bitget] one-way close {} → orderId={}", pair, oid);
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                if let Ok((px, fee, filled)) = self.fetch_order(http, keys, pair, oid, multiplier).await {
                                    if filled > 0.0 {
                                        return Ok(CloseReceipt {
                                            final_price: px,
                                            total_fee: fee,
                                            closed_volume: filled,
                                        });
                                    }
                                }
                                // Order placed but no fill confirmed yet — return with target price
                                return Ok(CloseReceipt {
                                    final_price: target_price,
                                    total_fee: 0.0,
                                    closed_volume: phys_amount,
                                });
                            }
                            Err(e2) => {
                                warn!("[bitget] one-way close {} also failed: {}", pair, e2);
                                // Try flash close-positions API as ultimate fallback
                                if e2.contains("22002") || e2.to_lowercase().contains("no position") {
                                    let flash_body = serde_json::json!({
                                        "symbol": pair,
                                        "productType": "USDT-FUTURES",
                                        "holdSide": hold_side
                                    }).to_string();
                                    info!("[bitget] flash close-positions {} holdSide={}", pair, hold_side);
                                    if let Ok(fv) = bitget_request(http, keys, "POST", "/api/v2/mix/order/close-positions", &flash_body, api_timeout_ms).await {
                                        if let Some(arr) = fv["data"]["successList"].as_array() {
                                            if let Some(first) = arr.first() {
                                                let oid = first["orderId"].as_str().unwrap_or("?");
                                                info!("[bitget] flash close {} → orderId={}", pair, oid);
                                                tokio::time::sleep(Duration::from_millis(500)).await;
                                                let (px, fee, filled) = self.fetch_order(http, keys, pair, oid, multiplier).await.unwrap_or((target_price, 0.0, phys_amount));
                                                return Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        return Err(e);
                    }
                }
            }

            if attempt + 1 < retries {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }

        // Final fallback: market order (try hedge mode first, then one-way)
        let qty = if multiplier > 0.0 && multiplier != 1.0 {
            (phys_amount / multiplier).round() * multiplier
        } else {
            phys_amount
        };
        let qty_formatted = format_price(qty);

        // Attempt 1: hedge mode
        let hedge_body = serde_json::json!({
            "symbol": pair,
            "productType": "USDT-FUTURES",
            "marginMode": "crossed",
            "marginCoin": "USDT",
            "side": side,
            "tradeSide": "close",
            "holdSide": hold_side,
            "orderType": "market",
            "size": qty_formatted
        })
        .to_string();

        match bitget_request(http, keys, "POST", "/api/v2/mix/order/place-order", &hedge_body, api_timeout_ms).await {
            Ok(v) => {
                let oid = v["data"]["orderId"].as_str().unwrap_or("?");
                tokio::time::sleep(Duration::from_millis(500)).await;
                let (px, fee, filled) = self.fetch_order(http, keys, pair, oid, multiplier).await.unwrap_or((target_price, 0.0, phys_amount));
                return Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled });
            }
            Err(e) if e.contains("22002") || e.to_lowercase().contains("no position") => {
                warn!("[bitget] fallback hedge close {} failed ({}), retrying as one-way market", pair, e);
            }
            Err(e) => {
                return Err(format!("Bitget fallback close failed: {}", e));
            }
        }

        // Attempt 2: one-way mode (no holdSide, tradeSide=close + reduceOnly)
        let oneway_body = serde_json::json!({
            "symbol": pair,
            "productType": "USDT-FUTURES",
            "marginMode": "crossed",
            "marginCoin": "USDT",
            "side": side,
            "tradeSide": "close",
            "orderType": "market",
            "size": qty_formatted,
            "reduceOnly": "YES"
        })
        .to_string();

        match bitget_request(http, keys, "POST", "/api/v2/mix/order/place-order", &oneway_body, api_timeout_ms).await {
            Ok(v) => {
                let oid = v["data"]["orderId"].as_str().unwrap_or("?");
                tokio::time::sleep(Duration::from_millis(500)).await;
                let (px, fee, filled) = self.fetch_order(http, keys, pair, oid, multiplier).await.unwrap_or((target_price, 0.0, phys_amount));
                return Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled });
            }
            Err(e) if e.contains("22002") || e.to_lowercase().contains("no position") => {
                warn!("[bitget] one-way fallback close {} also 22002 — using flash close-positions API", pair);
            }
            Err(e) => {
                return Err(format!("Bitget fallback one-way close failed: {}", e));
            }
        }

        // Ultimate fallback: Bitget close-positions API (flash close)
        // This auto-detects posMode/holdSide/marginMode and always works
        let flash_body = serde_json::json!({
            "symbol": pair,
            "productType": "USDT-FUTURES",
            "holdSide": hold_side
        })
        .to_string();

        info!("[bitget] flash close-positions {} holdSide={}", pair, hold_side);
        match bitget_request(http, keys, "POST", "/api/v2/mix/order/close-positions", &flash_body, api_timeout_ms).await {
            Ok(v) => {
                // close-positions returns successList with orderId
                if let Some(arr) = v["data"]["successList"].as_array() {
                    if let Some(first) = arr.first() {
                        let oid = first["orderId"].as_str().unwrap_or("?");
                        info!("[bitget] flash close {} → orderId={}", pair, oid);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        let (px, fee, filled) = self.fetch_order(http, keys, pair, oid, multiplier).await.unwrap_or((target_price, 0.0, phys_amount));
                        return Ok(CloseReceipt { final_price: px, total_fee: fee, closed_volume: filled });
                    }
                }
                // If successList is empty but no error, still return OK with target price
                warn!("[bitget] flash close {} succeeded but no orderId in response", pair);
                Ok(CloseReceipt { final_price: target_price, total_fee: 0.0, closed_volume: phys_amount })
            }
            Err(e) => {
                Err(format!("Bitget flash close-positions also failed: {}", e))
            }
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

        // 1. Fast path: Wait for WS execution report (up to 1000ms)
        let cache = super::ws_bitget_private::get_order_cache();
        for _ in 0..50 {
            if let Some(state) = cache.get(order_id) {
                if state.status == "filled" || state.status == "canceled" || state.base_volume > 0.0 {
                    tracing::debug!("[bitget] fetch_order {} resolved via WS in <1s!", order_id);
                    let fee = state.fee;
                    let filled = state.base_volume;
                    let avg_price = state.price_avg;
                    if filled < 1e-12 && (state.status == "canceled" || state.status == "expired") {
                        return Ok((0.0, 0.0, 0.0));
                    }
                    return Ok((avg_price, fee, filled));
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // 2. Slow path: Fallback to REST API
        let path = format!(
            "/api/v2/mix/order/detail?symbol={}&productType=USDT-FUTURES&orderId={}",
            pair, order_id
        );
        let v = bitget_request(http, keys, "GET", &path, "", 10_000).await?;

        let data = &v["data"];
        let filled: f64 = data["baseVolume"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let avg_price: f64 = data["priceAvg"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let fee: f64 = data["fee"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0)
            .abs();

        let status = data["state"].as_str().unwrap_or("");
        if filled < 1e-12 && (status == "cancelled" || status == "expired") {
            return Ok((0.0, 0.0, 0.0));
        }

        Ok((avg_price, fee, filled))
    }

    async fn fetch_balances(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<(f64, f64), String> {
        let v = bitget_request(
            http,
            keys,
            "GET",
            "/api/v2/mix/account/account?symbol=BTCUSDT&productType=USDT-FUTURES&marginCoin=USDT",
            "",
            10_000,
        )
        .await?;

        let data = &v["data"];
        let equity: f64 = data["accountEquity"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let available: f64 = data["crossedMaxAvailable"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        Ok((equity, available))
    }

    async fn fetch_taker_fee(&self, _http: &Client, _keys: &ExchangeKeys) -> Result<f64, String> {
        // Bitget standard taker fee for USDT-FUTURES
        Ok(0.0006)
    }

    async fn ensure_leverage(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        leverage: f64,
    ) -> Result<(), String> {
        let body = serde_json::json!({
            "symbol": pair,
            "productType": "USDT-FUTURES",
            "marginMode": "crossed",
            "marginCoin": "USDT",
            "leverage": format!("{}", leverage as u32)
        })
        .to_string();

        match bitget_request(
            http,
            keys,
            "POST",
            "/api/v2/mix/account/set-leverage",
            &body,
            10_000,
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.contains("same") || e.contains("already") {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }


    async fn fetch_positions(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<Vec<PositionInfo>, String> {
        let v = bitget_request(
            http,
            keys,
            "GET",
            "/api/v2/mix/position/all-position?productType=USDT-FUTURES&marginCoin=USDT",
            "",
            10_000,
        )
        .await?;

        let mut positions = Vec::new();
        if let Some(arr) = v["data"].as_array() {
            for p in arr {
                let symbol = p["symbol"].as_str().unwrap_or("").to_string();
                let size: f64 = p["total"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let hold_side = p["holdSide"].as_str().unwrap_or("");
                let signed_size = if hold_side == "short" { -size } else { size };
                if signed_size.abs() < 1e-12 {
                    continue;
                }
                let entry: f64 = p["openPriceAvg"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let upnl: f64 = p["unrealizedPL"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);

                positions.push(PositionInfo {
                    symbol,
                    size: signed_size,
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
