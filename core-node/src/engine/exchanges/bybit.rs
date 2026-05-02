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

/// REST fallback for placing order (existing logic)
async fn rest_bybit_order(
    http: &Client,
    keys: &ExchangeKeys,
    body: &Value,
    api_timeout_ms: u64,
) -> Result<Value, String> {
    let ts = now_ms().to_string();
    let recv = "5000";
    let body_str = body.to_string();
    let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &body_str);

    let resp = http
        .post("https://api.bybit.com/v5/order/create")
        .header("X-BAPI-API-KEY", &keys.bybit_key)
        .header("X-BAPI-SIGN", &sig)
        .header("X-BAPI-TIMESTAMP", &ts)
        .header("X-BAPI-RECV-WINDOW", recv)
        .header("Content-Type", "application/json")
        .body(body_str)
        .timeout(Duration::from_millis(api_timeout_ms))
        .send()
        .await
        .map_err(|e| e.to_string())?;

    resp.json().await.map_err(|e| e.to_string())
}

#[async_trait]
impl ExchangeConnector for BybitConnector {
    fn name(&self) -> &'static str {
        "bybit"
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

        // ── TRY WS FIRST ──
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
                return Ok((Some(oid), 0.0, 0.0));
            }
            Err(e) => {
                tracing::warn!("[bybit_ws] WS order failed ({}). Falling back to REST.", e);
            }
        }

        // ── REST FALLBACK ──
        let v = rest_bybit_order(http, keys, &body, api_timeout_ms).await?;

        let resp_oid = v["result"]["orderId"].as_str().unwrap_or("none");
        tracing::info!(
            "[bybit] place_order {} → orderId={} retCode={} (REST fallback)",
            pair,
            resp_oid,
            v["retCode"]
        );

        if v["retCode"].as_i64() != Some(0) {
            return Err(format!("Bybit API Error: {}", v));
        }

        let order_id = v["result"]["orderId"].as_str().map(|s| s.to_string());
        if order_id.is_some() {
            Ok((order_id, 0.0, 0.0))
        } else {
            Err("Bybit failed to return orderId".to_string())
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
        api_timeout_ms: u64,
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

        // ── TRY WS FIRST for close ──
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
                tracing::warn!("[bybit_ws] WS close failed ({}). Falling back to REST.", e);
                // REST fallback
                let v = rest_bybit_order(http, keys, &body, api_timeout_ms).await?;
                if v["retCode"].as_i64() != Some(0) {
                    tracing::error!("[bybit] close rejected: {}", v);
                }
                v["result"]["orderId"].as_str().map(|s| s.to_string())
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
        let fb_str = fb_body.to_string();
        let ts_fb = now_ms().to_string();
        let recv_fb = "5000";
        let sig_fb = bybit_sign(&keys.bybit_secret, &ts_fb, &keys.bybit_key, recv_fb, &fb_str);

        let resp_fb = http
            .post("https://api.bybit.com/v5/order/create")
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig_fb)
            .header("X-BAPI-TIMESTAMP", &ts_fb)
            .header("X-BAPI-RECV-WINDOW", recv_fb)
            .header("Content-Type", "application/json")
            .body(fb_str)
            .timeout(Duration::from_millis(api_timeout_ms))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let v_fb: Value = resp_fb.json().await.map_err(|e| e.to_string())?;
        let oid_fb = v_fb["result"]["orderId"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| "Bybit fallack rejected".to_string())?;

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

        let pos = self
            .fetch_positions(http, keys)
            .await
            .map_err(|e| format!("Bybit close unverified: failed to fetch positions: {e}"))?;
        let still_has = pos
            .iter()
            .any(|p| crate::engine::real::symbols_match(&p.symbol, pair) && p.size.abs() > 1e-12);
        if !still_has {
            Err("Bybit close unverified: position appears closed but fill price/fee is unknown".to_string())
        } else {
            Err("Bybit close unverified, position still active".to_string())
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
        let ts = now_ms().to_string();
        let recv = "5000";
        let qs = format!("category=linear&orderId={}", order_id);

        // 1. Check REALTIME first (often holds recent active/filled orders)
        let sig_rt = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &qs);
        let resp_rt = http
            .get(format!("https://api.bybit.com/v5/order/realtime?{}", qs))
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig_rt)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let v_rt: Value = resp_rt.json().await.map_err(|e| e.to_string())?;
        let mut order = v_rt["result"]["list"]
            .as_array()
            .and_then(|a| a.first())
            .cloned();

        // 2. Fallback to HISTORY if not in realtime (e.g. dropped from active orders list)
        if order.is_none() {
            let ts_h = now_ms().to_string();
            let sig_h = bybit_sign(&keys.bybit_secret, &ts_h, &keys.bybit_key, recv, &qs);
            let resp_h = http
                .get(format!("https://api.bybit.com/v5/order/history?{}", qs))
                .header("X-BAPI-API-KEY", &keys.bybit_key)
                .header("X-BAPI-SIGN", &sig_h)
                .header("X-BAPI-TIMESTAMP", &ts_h)
                .header("X-BAPI-RECV-WINDOW", recv)
                .timeout(Duration::from_secs(5))
                .send()
                .await
                .map_err(|e| e.to_string())?;

            let v_h: Value = resp_h.json().await.map_err(|e| e.to_string())?;
            order = v_h["result"]["list"]
                .as_array()
                .and_then(|a| a.first())
                .cloned();
        }

        let order = match order {
            Some(o) => o,
            None => return Ok((0.0, 0.0, 0.0)),
        };

        // Check order status — if Cancelled/Rejected/Deactivated, but might have partial fill!
        let status = order["orderStatus"].as_str().unwrap_or("");
        if status == "Cancelled" || status == "Rejected" || status == "Deactivated" {
            let exec_qty: f64 = order["cumExecQty"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            if exec_qty < 1e-12 {
                tracing::warn!(
                    "[bybit] fetch_order {} status={} — definitively unfilled",
                    order_id,
                    status
                );
                return Ok((0.0, 0.0, 0.0));
            }
        }

        // Check cumExecQty
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
        let recv = "5000";

        // 1. Try UNIFIED (Unified Trading Account)
        let qs1 = "accountType=UNIFIED";
        let sig1 = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs1);

        let resp1 = http
            .get(format!(
                "https://api.bybit.com/v5/account/wallet-balance?{}",
                qs1
            ))
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig1)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let mut v: Value = resp1.json().await.map_err(|e| e.to_string())?;

        // If the account is a Classic/Standard account, retCode will be 10001 or list empty
        let ret_code = v["retCode"].as_i64().unwrap_or(-1);
        if ret_code != 0
            || v["result"]["list"]
                .as_array()
                .map(|a| a.is_empty())
                .unwrap_or(true)
        {
            // 2. Fallback to CONTRACT (Classic Account)
            let qs2 = "accountType=CONTRACT";
            let sig2 = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs2);

            let resp2 = http
                .get(format!(
                    "https://api.bybit.com/v5/account/wallet-balance?{}",
                    qs2
                ))
                .header("X-BAPI-API-KEY", &keys.bybit_key)
                .header("X-BAPI-SIGN", &sig2)
                .header("X-BAPI-TIMESTAMP", &ts)
                .header("X-BAPI-RECV-WINDOW", recv)
                .timeout(Duration::from_secs(10))
                .send()
                .await
                .map_err(|e| e.to_string())?;

            v = resp2.json().await.map_err(|e| e.to_string())?;
        }

        let coin_obj = v["result"]["list"]
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|first| {
                if first["coin"].is_array() {
                    first["coin"].as_array().and_then(|coins| {
                        coins.iter().find(|c| c["coin"].as_str() == Some("USDT")).cloned()
                    })
                } else {
                    v["result"]["list"].as_array().and_then(|arr| {
                        arr.iter().find(|c| c["coin"].as_str() == Some("USDT")).cloned()
                    })
                }
            });

        if let Some(coin_ref) = coin_obj {
            let wallet: f64 = coin_ref["walletBalance"]
                .as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let equity: f64 = coin_ref["equity"]
                .as_str().and_then(|s| s.parse().ok()).unwrap_or(wallet);
            let available: f64 = coin_ref["availableToWithdraw"]
                .as_str().and_then(|s| s.parse().ok())
                .or_else(|| coin_ref["availableBalance"].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(wallet);
            return Ok((equity, available));
        }
        
        let ret = v["retCode"].as_i64().unwrap_or(-999);
        if ret == 0 {
            // Success but no USDT means exactly 0 balance
            Ok((0.0, 0.0))
        } else {
            let msg = v["retMsg"].as_str().or_else(|| v["error"].as_str()).unwrap_or("unknown");
            Err(format!("Bybit API error (retCode={ret}, msg={msg})"))
        }
    }

    async fn fetch_taker_fee(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        let ts = now_ms().to_string();
        let recv = "5000";
        let qs = "category=linear&symbol=BTCUSDT";
        let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs);

        let resp = http
            .get(format!("https://api.bybit.com/v5/account/fee-rate?{}", qs))
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let taker = v["result"]["list"]
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|a| a["takerFeeRate"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.00055);

        Ok(taker)
    }

    async fn ensure_leverage(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        leverage: f64,
    ) -> Result<(), String> {
        // Only set leverage — do NOT force margin mode here.
        // Margin mode should be configured via warmup_leverages (real.rs) which has profile context.
        let ts = now_ms().to_string();
        let recv = "5000";
        let body = json!({
            "category": "linear",
            "symbol": pair,
            "buyLeverage": leverage.to_string(),
            "sellLeverage": leverage.to_string()
        });
        let body_str = body.to_string();
        let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &body_str);

        let _ = http
            .post("https://api.bybit.com/v5/position/set-leverage")
            .header("X-BAPI-API-KEY", &keys.bybit_key)
            .header("X-BAPI-SIGN", &sig)
            .header("X-BAPI-TIMESTAMP", &ts)
            .header("X-BAPI-RECV-WINDOW", recv)
            .header("Content-Type", "application/json")
            .body(body_str)
            .timeout(Duration::from_secs(5))
            .send()
            .await;
        Ok(())
    }

    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String> {
        // Bybit V5 tickers endpoint returns funding rate without authentication
        let url = format!(
            "https://api.bybit.com/v5/market/tickers?category=linear&symbol={}",
            pair
        );
        if let Ok(resp) = http.get(&url).timeout(Duration::from_secs(5)).send().await {
            if let Ok(json) = resp.json::<Value>().await {
                if let Some(item) = json["result"]["list"].as_array().and_then(|a| a.first()) {
                    let rate = item["fundingRate"]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let next_ts = item["nextFundingTime"]
                        .as_str()
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);
                    return Ok((rate, next_ts));
                }
            }
        }
        Err("Bybit funding rate fetch failed".to_string())
    }

    async fn fetch_positions(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<Vec<PositionInfo>, String> {
        let mut positions = Vec::new();
        let mut cursor = String::new();

        loop {
            let ts = now_ms().to_string();
            let recv = "5000";
            let mut qs = "category=linear&settleCoin=USDT&limit=50".to_string();
            if !cursor.is_empty() {
                qs.push_str(&format!("&cursor={}", cursor));
            }
            let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &qs);
            let resp = http
                .get(format!("https://api.bybit.com/v5/position/list?{}", qs))
                .header("X-BAPI-API-KEY", &keys.bybit_key)
                .header("X-BAPI-SIGN", &sig)
                .header("X-BAPI-TIMESTAMP", &ts)
                .header("X-BAPI-RECV-WINDOW", recv)
                .timeout(Duration::from_secs(10))
                .send()
                .await
                .map_err(|e| e.to_string())?;

            if !resp.status().is_success() {
                let err_txt = resp.text().await.unwrap_or_default();
                return Err(format!("Bybit position API error: {}", err_txt));
            }

            let v: Value = resp.json().await.map_err(|e| e.to_string())?;

            if v["retCode"].as_i64().unwrap_or(-1) != 0 {
                return Err(format!("Bybit API returned error: {}", v));
            }

            if let Some(arr) = v["result"]["list"].as_array() {
                for p in arr {
                    let size: f64 = p["size"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    if size.abs() < 1e-12 {
                        continue;
                    }
                    let side = p["side"].as_str().unwrap_or("");
                    let signed_size = if side == "Sell" { -size } else { size };
                    let realized = p["cumRealisedPnl"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    positions.push(PositionInfo {
                        symbol: p["symbol"].as_str().unwrap_or("").to_string(),
                        size: signed_size,
                        entry_price: p["avgPrice"]
                            .as_str()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0),
                        unrealized_pnl: p["unrealisedPnl"]
                            .as_str()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0),
                        realized_pnl: realized,
                        accumulated_fee: 0.0,
                        accumulated_funding: 0.0,
                    });
                }
            }

            if let Some(c) = v["result"]["nextPageCursor"].as_str() {
                if c.is_empty() {
                    break;
                }
                cursor = c.to_string();
            } else {
                break;
            }
        }
        Ok(positions)
    }

    async fn fetch_orderbook(
        &self,
        http: &Client,
        pair: &str,
        depth: u32,
    ) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), String> {
        // Bybit limits: 1, 50, 200, 500, 1000. Closest to depth is usually 50.
        let bybit_limit = if depth <= 1 { 1 } else { 50 };
        let url = format!(
            "https://api.bybit.com/v5/market/orderbook?category=linear&symbol={}&limit={}",
            pair, bybit_limit
        );
        let res = http
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Bybit depth error: {}", e))?;

        let status = res.status();
        let body = res
            .text()
            .await
            .map_err(|e| format!("Body read error: {}", e))?;

        if !status.is_success() {
            return Err(format!("Bybit API HTTP {}: {}", status, body));
        }

        let json: Value =
            serde_json::from_str(&body).map_err(|e| format!("Bybit Parse error: {}", e))?;

        if json["retCode"].as_i64().unwrap_or(-1) != 0 {
            return Err(format!("Bybit API returned error: {}", json));
        }

        let parse_levels = |levels: &Value| -> Vec<(f64, f64)> {
            let mut out = Vec::new();
            if let Some(arr) = levels.as_array() {
                for item in arr {
                    if let (Some(px_str), Some(sz_str)) = (
                        item.get(0).and_then(|v| v.as_str()),
                        item.get(1).and_then(|v| v.as_str()),
                    ) {
                        if let (Ok(px), Ok(sz)) = (px_str.parse::<f64>(), sz_str.parse::<f64>()) {
                            out.push((px, sz));
                        }
                    }
                }
            }
            out
        };

        // Bybit returns nested "b" and "a" arrays inside "result"
        let bids = parse_levels(&json["result"]["b"]);
        let asks = parse_levels(&json["result"]["a"]);

        Ok((bids, asks))
    }
}
