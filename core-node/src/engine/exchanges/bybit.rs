use async_trait::async_trait;
use reqwest::Client;

use serde_json::json;
use serde_json::Value;
use std::time::Duration;

use super::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::{bybit_sign, now_ms, ExchangeKeys};

pub struct BybitConnector;

/// Try placing order via WS, return Ok(orderId) or Err to trigger REST fallback
async fn try_bybit_ws_order(
    _keys: &ExchangeKeys,
    order_args: &Value,
) -> Result<String, String> {
    use super::ws_bybit::send_bybit_ws_request;

    let req_id = uuid::Uuid::new_v4().to_string();
    let ts = now_ms().to_string();

    let ws_req = json!({
        "reqId": req_id,
        "header": {
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": "8000"
        },
        "op": "order.create",
        "args": [order_args]
    });

    let resp = send_bybit_ws_request(ws_req).await?;

    if resp["retCode"].as_i64() == Some(0) {
        if let Some(oid) = resp["data"]["orderId"].as_str() {
            if !oid.is_empty() {
                return Ok(oid.to_string());
            }
        }
    }

    let msg = resp["retMsg"].as_str().unwrap_or("unknown");
    Err(format!("Bybit WS retCode={}: {}", resp["retCode"], msg))
}


#[async_trait]
impl ExchangeConnector for BybitConnector {
    fn name(&self) -> &'static str {
        "bybit"
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
        let side_str = if is_buy { "Buy" } else { "Sell" };

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

        let mut price_str = format!("{:.8}", rounded_price)
            .trim_end_matches('0')
            .to_string();
        if price_str.ends_with('.') {
            price_str.pop();
        }
        if price_str.is_empty() {
            price_str = "0".to_string();
        }

        let body = if use_ioc && target_price > 0.0 {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Limit",
                "qty": qty_str,
                "price": price_str,
                "timeInForce": "IOC",
                "reduceOnly": false
            })
        } else {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Market",
                "qty": qty_str,
                "reduceOnly": false
            })
        };

        if dry_run {
            return Ok((
                Some(format!("dry_{}_{}", self.name(), now_ms())),
                target_price,
                0.0,
            ));
        }

        // ── ONLY WS ──
        // Initialize WS on first call (lazy)
        let _ = super::ws_bybit::get_or_init_bybit_ws(
            keys.bybit_key.clone(),
            keys.bybit_secret.clone(),
        );

        match try_bybit_ws_order(keys, &body).await {
            Ok(oid) => {
                tracing::info!(
                    "[bybit_ws] ✅ place_order {} orderId={} (WS)",
                    pair, oid
                );

                // Wait up to 500ms for execution report to arrive via the
                // private 'execution' subscription. IOC orders on Bybit fill
                // within ~50-200ms, but the execution push may arrive with
                // a small delay. Polling in 50ms increments keeps latency low
                // while giving the stream enough time.
                let mut inline_price = 0.0;
                let mut inline_fee = 0.0;
                    let mut _inline_qty = 0.0;
                for _wait in 0..10 {
                    if let Some((p, f, q)) = super::ws_bybit::get_cached_fill(&oid) {
                        inline_price = p;
                        inline_fee = f;
                        _inline_qty = q;
                        tracing::info!(
                            "[bybit_ws] ⚡ Inline fill {} price={:.6} fee={:.6} qty={:.6}",
                            pair, p, f, q
                        );
                        break;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                }

                return Ok((Some(oid), inline_price, inline_fee));
            }
            Err(e) => {
                tracing::error!("[bybit_ws] WS order failed ({}). No REST fallback available.", e);
                return Err(format!("Bybit WS connection error: {}", e));
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
        use_ioc: bool,
        slippage_percent: f64,
        multiplier: f64,
        tick_size: f64,
        retries: u32,
        delay_ms: u64,
        _api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        let side_str = if is_buy { "Buy" } else { "Sell" };

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
        if qty_str.is_empty() {
            qty_str = "0".to_string();
        }
        if qty_str == "0" {
            return Err("Amount too small to close".to_string());
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

        let body = if use_ioc && target_price > 0.0 {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Limit",
                "qty": qty_str,
                "price": price_str,
                "timeInForce": "IOC",
                "reduceOnly": true
            })
        } else {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Market",
                "qty": qty_str,
                "reduceOnly": true
            })
        };

        if dry_run {
            return Ok(CloseReceipt {
                final_price: target_price,
                total_fee: 0.0,
                closed_volume: phys_amount,
            });
        }

        let _ = self.ensure_leverage(http, keys, pair, leverage).await;

        // ── ONLY WS for close ──
        let _ = super::ws_bybit::get_or_init_bybit_ws(
            keys.bybit_key.clone(),
            keys.bybit_secret.clone(),
        );

        let ws_order_id = match try_bybit_ws_order(keys, &body).await {
            Ok(oid) => {
                tracing::info!("[bybit_ws] ✅ close_position {} orderId={} (WS)", pair, oid);
                Some(oid)
            }
            Err(e) => {
                tracing::error!("[bybit_ws] WS close failed ({}). No REST fallback available.", e);
                None
            }
        };
        if let Some(ref oid) = ws_order_id {
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

        tracing::error!(
            "[bybit] fallback market close triggered for {} (qty {})",
            pair,
            phys_amount
        );
        let mut fb_price_str = "0".to_string();
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
            fb_price_str = format!("{:.8}", rp).trim_end_matches('0').to_string();
            if fb_price_str.ends_with('.') { fb_price_str.pop(); }
            if fb_price_str.is_empty() { fb_price_str = "0".to_string(); }
        }

        let fb_body = if fb_price_str == "0" {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Market",
                "qty": qty_str,
                "reduceOnly": true
            })
        } else {
            json!({
                "category": "linear",
                "symbol": pair,
                "side": side_str,
                "orderType": "Limit",
                "qty": qty_str,
                "price": fb_price_str,
                "timeInForce": "IOC",
                "reduceOnly": true
            })
        };
        let oid_fb = match try_bybit_ws_order(keys, &fb_body).await {
            Ok(oid) => {
                tracing::info!("[bybit_ws] ✅ fallback close {} orderId={} (WS)", pair, oid);
                oid
            }
            Err(e) => {
                return Err(format!("Bybit fallback close WS failed: {}", e));
            }
        };

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

        tracing::warn!("[bybit] fallback close unverified for {} (oid {}). Assuming filled to prevent CLOSING trap.", pair, oid_fb);
        Ok(CloseReceipt {
            final_price: target_price,
            total_fee: 0.0, // Assuming 0 fee if unverified to allow DB finalize
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
        _fallback_price: f64,
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
        http: &Client,
        keys: &ExchangeKeys,
        _pair: &str,
        order_id: &str,
        _multiplier: f64,
    ) -> Result<(f64, f64, f64), String> {
        // ── FAST PATH: Check execution report cache first ──
        if let Some((p, f, q)) = super::ws_bybit::get_cached_fill(order_id) {
            if p > 0.0 && q > 1e-12 {
                tracing::debug!("[bybit] fetch_order {} — found in exec cache: price={:.6} qty={:.6}", order_id, p, q);
                return Ok((p, f, q));
            }
        }

        let ts = now_ms();
        let qs = format!("category=linear&orderId={}", order_id);
        let ts_str = ts.to_string();
        let sig = bybit_sign(&keys.bybit_secret, &ts_str, &keys.bybit_key, "5000", &qs);

        let url = format!("https://api.bybit.com/v5/order/realtime?{}", qs);
        let req = http
            .get(&url)
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-TIMESTAMP", ts.to_string())
            .header("X-BAPI-SIGN", sig)
            .header("X-BAPI-RECV-WINDOW", "5000");

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => return Err(format!("Bybit fetch_order network error: {}", e)),
        };

        let body: Value = match resp.json().await {
            Ok(b) => b,
            Err(e) => return Err(format!("Bybit fetch_order JSON error: {}", e)),
        };

        if body["retCode"].as_i64() != Some(0) {
            return Err(format!("Bybit API error: {}", body["retMsg"]));
        }

        let mut order = body["result"]["list"].as_array().and_then(|a| a.first()).cloned();

        // Fallback to history for IOC
        if order.is_none() {
            let ts2 = now_ms();
            let ts2_str = ts2.to_string();
            let sig2 = bybit_sign(&keys.bybit_secret, &ts2_str, &keys.bybit_key, "5000", &qs);
            let url2 = format!("https://api.bybit.com/v5/order/history?{}", qs);
            let req2 = http
                .get(&url2)
                .header("X-BAPI-API-KEY", &keys.bybit_key)
                .header("X-BAPI-TIMESTAMP", ts2.to_string())
                .header("X-BAPI-SIGN", sig2)
                .header("X-BAPI-RECV-WINDOW", "5000");

            if let Ok(resp2) = req2.send().await {
                if let Ok(body2) = resp2.json::<Value>().await {
                    if body2["retCode"].as_i64() == Some(0) {
                        order = body2["result"]["list"].as_array().and_then(|a| a.first()).cloned();
                    }
                }
            }
        }

        let order = match order {
            Some(o) => o,
            None => {
                // Last resort: re-check execution cache
                if let Some((p, f, q)) = super::ws_bybit::get_cached_fill(order_id) {
                    if p > 0.0 && q > 1e-12 {
                        tracing::info!("[bybit] fetch_order {} — late exec cache hit: price={:.6} qty={:.6}", order_id, p, q);
                        return Ok((p, f, q));
                    }
                }
                return Err("Order not found in API yet".to_string());
            }
        };

        let status = order["orderStatus"].as_str().unwrap_or("");
        if status == "Cancelled" || status == "Rejected" || status == "Deactivated" {
            let exec_qty: f64 = order["cumExecQty"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            if exec_qty < 1e-12 {
                tracing::warn!("[bybit] fetch_order {} status={} — definitively unfilled", order_id, status);
                return Ok((0.0, 0.0, 0.0));
            }
        }

        let exec_qty: f64 = order["cumExecQty"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        if exec_qty < 1e-12 {
            return Err("Not filled yet".to_string());
        }

        let price = order["avgPrice"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let fee = order["cumExecFee"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        Ok((price, fee.abs(), exec_qty))
    }

    async fn fetch_balances(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<(f64, f64), String> {
        let ts = now_ms().to_string();
        let recv = "10000";
        let qs = "accountType=UNIFIED&coin=USDT";
        let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs);

        let url = format!("https://api.bybit.com/v5/account/wallet-balance?{}", qs);
        let resp = http.get(&url)
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .header("X-BAPI-SIGN", &sig)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("Bybit HTTP error: {}", e))?;

        let v: Value = resp.json().await.map_err(|e| format!("Bybit JSON error: {}", e))?;
        if v["retCode"].as_i64() != Some(0) {
            return Err(format!("Bybit API error: {}", v["retMsg"].as_str().unwrap_or("?")));
        }

        let coin_obj = v["result"]["list"].as_array()
            .and_then(|arr| arr.first())
            .and_then(|first| {
                first["coin"].as_array().and_then(|coins| {
                    coins.iter().find(|c| c["coin"].as_str() == Some("USDT")).cloned()
                })
            });
        if let Some(coin_ref) = coin_obj {
            let wallet: f64 = coin_ref["walletBalance"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let equity: f64 = coin_ref["equity"].as_str().and_then(|s| s.parse().ok()).unwrap_or(wallet);
            let available: f64 = coin_ref["availableToWithdraw"].as_str().and_then(|s| s.parse().ok())
                .or_else(|| coin_ref["availableBalance"].as_str().and_then(|s| s.parse().ok())).unwrap_or(wallet);
            return Ok((equity, available));
        }
        Ok((0.0, 0.0))
    }

    async fn fetch_taker_fee(&self, _http: &Client, _keys: &ExchangeKeys) -> Result<f64, String> {
        // Bybit default taker fee; rarely changes, cached at startup
        Ok(0.00055)
    }

    async fn ensure_leverage(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        leverage: f64,
    ) -> Result<(), String> {
        use super::ws_bybit::send_bybit_ws_request;
        let _ = super::ws_bybit::get_or_init_bybit_ws(keys.bybit_key.clone(), keys.bybit_secret.clone());
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "reqId": req_id,
            "header": {"X-BAPI-TIMESTAMP": now_ms().to_string(), "X-BAPI-RECV-WINDOW": "5000"},
            "op": "position.set-leverage",
            "args": [{"category": "linear", "symbol": pair, "buyLeverage": leverage.to_string(), "sellLeverage": leverage.to_string()}]
        });
        let _ = send_bybit_ws_request(ws_req).await;
        Ok(())
    }

    async fn get_funding_rate(&self, _http: &Client, _pair: &str) -> Result<(f64, i64), String> {
        // Funding rate is non-critical background data — available from collector WS streams
        Ok((0.0, 0))
    }

    async fn fetch_positions(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<Vec<PositionInfo>, String> {
        let ts = now_ms().to_string();
        let recv = "10000";
        let qs = "category=linear&settleCoin=USDT";
        let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs);

        let url = format!("https://api.bybit.com/v5/position/list?{}", qs);
        let resp = http.get(&url)
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .header("X-BAPI-SIGN", &sig)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("Bybit HTTP error: {}", e))?;

        let v: Value = resp.json().await.map_err(|e| format!("Bybit JSON error: {}", e))?;
        if v["retCode"].as_i64() != Some(0) {
            return Err(format!("Bybit API error: {}", v["retMsg"].as_str().unwrap_or("?")));
        }

        let mut positions = Vec::new();
        if let Some(arr) = v["result"]["list"].as_array() {
            for p in arr {
                let size: f64 = p["size"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                if size.abs() < 1e-12 { continue; }
                let side = p["side"].as_str().unwrap_or("");
                let signed_size = if side == "Sell" { -size } else { size };
                positions.push(PositionInfo {
                    symbol: p["symbol"].as_str().unwrap_or("").to_string(),
                    size: signed_size,
                    entry_price: p["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                    unrealized_pnl: p["unrealisedPnl"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                    realized_pnl: p["cumRealisedPnl"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
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
        use super::ws_bybit::send_bybit_ws_request;
        let bybit_limit = if depth <= 1 { 1 } else { 50 };
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "reqId": req_id,
            "op": "order.book",
            "args": [{"category": "linear", "symbol": pair, "limit": bybit_limit}]
        });
        let resp = send_bybit_ws_request(ws_req).await?;
        let parse_levels = |levels: &Value| -> Vec<(f64, f64)> {
            let mut out = Vec::new();
            if let Some(arr) = levels.as_array() {
                for item in arr {
                    if let (Some(px_str), Some(sz_str)) = (item.get(0).and_then(|v| v.as_str()), item.get(1).and_then(|v| v.as_str())) {
                        if let (Ok(px), Ok(sz)) = (px_str.parse::<f64>(), sz_str.parse::<f64>()) { out.push((px, sz)); }
                    }
                }
            }
            out
        };
        let bids = parse_levels(&resp["data"]["b"]);
        let asks = parse_levels(&resp["data"]["a"]);
        Ok((bids, asks))
    }
}
