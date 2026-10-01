use async_trait::async_trait;
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

use super::traits::{CloseReceipt, ExchangeConnector, PositionInfo, TradeAuditResult};
use crate::engine::real::{now_ms, ExchangeKeys};

pub struct KucoinConnector;

fn public_depth_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
        .filter(|number| number.is_finite())
}

fn parse_public_depth_side(
    value: &Value,
    multiplier: f64,
) -> Result<Vec<(f64, f64)>, String> {
    if !multiplier.is_finite() || multiplier <= 0.0 {
        return Err("Invalid KuCoin contract multiplier".into());
    }
    let levels = value
        .as_array()
        .ok_or("KuCoin public depth side is not an array")?;
    if levels.is_empty() || levels.len() > 20 {
        return Err("KuCoin public depth side has invalid length".into());
    }
    levels
        .iter()
        .map(|level| {
            let level = level
                .as_array()
                .filter(|level| level.len() >= 2)
                .ok_or("Invalid KuCoin public depth level")?;
            let price = public_depth_number(&level[0])
                .filter(|price| *price > 0.0)
                .ok_or("Invalid KuCoin public depth price")?;
            let contracts = public_depth_number(&level[1])
                .filter(|quantity| *quantity > 0.0)
                .ok_or("Invalid KuCoin public depth quantity")?;
            let quantity = contracts * multiplier;
            if !quantity.is_finite() || quantity <= 0.0 {
                return Err("Invalid KuCoin public depth physical quantity".into());
            }
            Ok((price, quantity))
        })
        .collect()
}

fn parse_public_depth_snapshot(
    body: &Value,
    pair: &str,
    multiplier: f64,
    received_ms: i64,
) -> Result<crate::state::L2Snapshot, String> {
    if body["code"].as_str() != Some("200000") {
        return Err(format!("KuCoin public depth rejected: {}", body["code"]));
    }
    let data = body
        .get("data")
        .filter(|value| value.is_object())
        .ok_or("KuCoin public depth response has no data")?;
    let symbol = managed_kucoin_symbol(pair);
    if data["symbol"].as_str() != Some(symbol.as_str()) {
        return Err("KuCoin public depth symbol mismatch".into());
    }
    let timestamp_ns = data["ts"]
        .as_i64()
        .or_else(|| data["ts"].as_str().and_then(|raw| raw.parse().ok()))
        .ok_or("KuCoin public depth response has no timestamp")?;
    let updated_ms = timestamp_ns / 1_000_000;
    if !crate::engine::utils::local_timestamp_is_fresh(received_ms, updated_ms, 750) {
        return Err(format!(
            "Stale KuCoin public depth snapshot: {}ms",
            received_ms.saturating_sub(updated_ms),
        ));
    }
    let bids = parse_public_depth_side(&data["bids"], multiplier)?;
    let asks = parse_public_depth_side(&data["asks"], multiplier)?;
    if bids[0].0 >= asks[0].0
        || bids.windows(2).any(|levels| levels[1].0 > levels[0].0)
        || asks.windows(2).any(|levels| levels[1].0 < levels[0].0)
    {
        return Err("KuCoin public depth snapshot is crossed or unsorted".into());
    }
    Ok(crate::state::L2Snapshot {
        bids,
        asks,
        updated_ms,
    })
}

/// Fetch a bounded public futures book while the 10 ms Pro stream is warming.
/// The exchange timestamp, symbol and contract multiplier remain mandatory;
/// receipt time is never substituted for stale market data.
pub async fn fetch_public_depth_snapshot(
    http: &Client,
    pair: &str,
    multiplier: f64,
) -> Result<crate::state::L2Snapshot, String> {
    let response = http
        .get("https://api-futures.kucoin.com/api/v1/level2/depth20")
        .query(&[("symbol", managed_kucoin_symbol(pair))])
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .map_err(|error| format!("KuCoin public depth request failed: {error}"))?;
    let status = response.status();
    let body = response
        .json::<Value>()
        .await
        .map_err(|error| format!("Invalid KuCoin public depth response: {error}"))?;
    if !status.is_success() {
        return Err(format!(
            "KuCoin public depth returned HTTP {}",
            status.as_u16(),
        ));
    }
    parse_public_depth_snapshot(&body, pair, multiplier, now_ms())
}

#[async_trait]
impl ExchangeConnector for KucoinConnector {
    async fn symbol_has_no_open_orders(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
    ) -> Result<bool, String> {
        let data = kucoin_managed_call(
            keys,
            "GET",
            &format!(
                "/api/v1/orders?status=active&symbol={}&pageSize=1",
                managed_kucoin_symbol(pair)
            ),
            "",
        )
        .await?;
        Ok(data["items"]
            .as_array()
            .ok_or("Invalid KuCoin open orders")?
            .is_empty()
            && data["totalNum"].as_i64() == Some(0))
    }

    async fn funding_events(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::engine::orders::FundingEvent>, String> {
        use crate::engine::orders::{identity, number, FundingEvent};
        let symbol = managed_kucoin_symbol(pair);
        let mut windows = vec![(from, to)];
        let mut out = Vec::new();
        let mut requests = 0;
        while let Some((start, end)) = windows.pop() {
            requests += 1;
            if requests > 100 {
                return Err("KuCoin funding pagination incomplete".into());
            }
            let path = format!(
                "/api/v1/funding-history?symbol={symbol}&startAt={start}&endAt={end}&maxCount=100"
            );
            let v = super::ws_kucoin::send_kucoin_request(
                &keys.kucoin_key,
                &keys.kucoin_secret,
                &keys.kucoin_passphrase,
                "GET",
                &path,
                "",
                3000,
            )
            .await?;
            if v["code"].as_str() != Some("200000") {
                return Err("KuCoin funding request failed".into());
            }
            let more = v["data"]["hasMore"]
                .as_bool()
                .ok_or("Missing KuCoin funding pagination")?;
            if more {
                if end <= start {
                    return Err("Ambiguous KuCoin funding page boundary".into());
                }
                let mid = start + (end - start) / 2;
                windows.push((start, mid));
                windows.push((mid + 1, end));
                continue;
            }
            let rows = v["data"]["dataList"]
                .as_array()
                .ok_or("Invalid KuCoin funding ledger")?;
            for r in rows {
                if r["symbol"].as_str() != Some(symbol.as_str())
                    || r["settleCurrency"].as_str() != Some("USDT")
                {
                    return Err("KuCoin funding attribution mismatch".into());
                }
                let time_ms = number(r, "timePoint")? as i64;
                if time_ms < start || time_ms > end {
                    return Err("KuCoin funding outside requested range".into());
                }
                out.push(FundingEvent {
                    id: identity(r, "id").ok_or("Missing KuCoin funding identity")?,
                    time_ms,
                    cost: -number(r, "funding")?,
                });
            }
        }
        Ok(out)
    }
    fn supports_managed_orders(&self) -> bool {
        true
    }
    async fn submit_managed(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        r: &crate::engine::orders::OrderRequest,
    ) -> Result<crate::engine::orders::OrderObservation, String> {
        r.validate()?;
        super::ws_kucoin_private::ensure_private_ws(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
        )
        .await;
        let n = r.quantity / r.multiplier;
        if (n - n.round()).abs() > 1e-8 {
            return Err("Off-grid KuCoin quantity".into());
        }
        let mut body = json!({"clientOid":r.client_id,"symbol":managed_kucoin_symbol(&r.pair),"side":if r.is_buy{"buy"}else{"sell"},"type":if r.limit_price>0.0{"limit"}else{"market"},"size":n.round() as i64,"leverage":r.leverage.to_string(),"marginMode":"CROSS","reduceOnly":r.reduce_only});
        if r.limit_price > 0.0 {
            body["price"] = json!(format!(
                "{:.8}",
                crate::engine::orders::bounded_price(r.limit_price, r.tick_size, r.is_buy)
            ));
            body["timeInForce"] = json!("IOC");
        }
        let v = super::ws_kucoin_order::send_order(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            body,
            2000,
        )
        .await?;
        if v["code"].as_str() != Some("200000") {
            if matches!(
                v["code"].as_str(),
                Some(
                    "400001"
                        | "400002"
                        | "400003"
                        | "400004"
                        | "400005"
                        | "400006"
                        | "400007"
                        | "100001"
                        | "300003"
                        | "300004"
                        | "300005"
                        | "300009"
                        | "300011"
                        | "300012"
                )
            ) {
                return Ok(crate::engine::orders::OrderObservation::rejected());
            }
            return Err(format!(
                "KuCoin submission outcome unknown code={}",
                v["code"]
            ));
        }
        let order_id = crate::engine::orders::identity(&v["data"], "orderId")
            .ok_or("KuCoin accepted managed order without order id")?;
        let cache = super::ws_kucoin_private::get_order_cache();
        for _ in 0..15 {
            if let Some(state) = cache.get(&order_id).map(|state| state.clone()) {
                if let Some(observation) = managed_kucoin_ws_observation(&order_id, &state, r)? {
                    cache.remove(&order_id);
                    return Ok(observation);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(crate::engine::orders::OrderObservation::pending(Some(
            order_id,
        )))
    }
    async fn query_managed(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        r: &crate::engine::orders::OrderRequest,
        oid: Option<&str>,
    ) -> Result<crate::engine::orders::OrderObservation, String> {
        let path = match oid {
            Some(id) => format!("/api/v1/orders/{}", id),
            None => format!("/api/v1/orders/byClientOid?clientOid={}", r.client_id),
        };
        let v = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            "GET",
            &path,
            "",
            3000,
        )
        .await?;
        if v["code"].as_str() != Some("200000") {
            return Err("KuCoin order lookup unresolved".into());
        }
        // Do not block the hedge-critical path on a second fill-history request.
        // A missing fee remains None (and therefore reserved conservatively) until
        // the background exchange-ledger reconciliation verifies final accounting.
        parse_managed_kucoin(&v["data"], r)
    }
    async fn reconcile_managed(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        r: &crate::engine::orders::OrderRequest,
        oid: Option<&str>,
    ) -> Result<crate::engine::orders::OrderObservation, String> {
        let mut observation = self.query_managed(http, keys, r, oid).await?;
        if observation.filled_quantity > 0.0 && observation.fee.is_none() {
            let order_id = observation
                .order_id
                .as_deref()
                .ok_or("KuCoin filled order has no exchange id")?;
            let path = match oid {
                Some(id) => format!("/api/v1/orders/{id}"),
                None => format!("/api/v1/orders/byClientOid?clientOid={}", r.client_id),
            };
            let order = super::ws_kucoin::send_kucoin_request(
                &keys.kucoin_key,
                &keys.kucoin_secret,
                &keys.kucoin_passphrase,
                "GET",
                &path,
                "",
                3000,
            )
            .await?;
            if order["code"].as_str() != Some("200000") {
                return Err("KuCoin order lookup unresolved during fee reconciliation".into());
            }
            let created_ms = crate::engine::orders::number(&order["data"], "createdAt")? as i64;
            let path = kucoin_fill_history_path(order_id, created_ms, now_ms())?;
            let fills = super::ws_kucoin::send_kucoin_request(
                &keys.kucoin_key,
                &keys.kucoin_secret,
                &keys.kucoin_passphrase,
                "GET",
                &path,
                "",
                3000,
            )
            .await?;
            observation.fee = parse_kucoin_fill_fee(
                &fills,
                order_id,
                observation.filled_quantity,
                r.multiplier,
            )?;
        }
        Ok(observation)
    }
    fn name(&self) -> &'static str {
        "kucoin"
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
        leverage: f64,
        multiplier: f64,
        slippage_percent: f64,
        tick_size: f64,
        api_timeout_ms: u64,
    ) -> Result<(Option<String>, f64, f64), String> {
        let sym = format!("{}M", pair);
        let _ts = now_ms().to_string();
        let side = if is_buy { "buy" } else { "sell" };

        let lev_str = (leverage.max(1.0) as i64).to_string();

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

        let req_size = (phys_amount / multiplier.max(1e-12)).round() as i64;
        if req_size <= 0 {
            return Err(format!(
                "Computed order size is 0 for amt={} mult={}",
                phys_amount, multiplier
            ));
        }

        let body_json = if use_ioc && target_price > 0.0 {
            json!({
                "clientOid": Uuid::new_v4().to_string(),
                "symbol": sym,
                "side": side,
                "type": "limit",
                "timeInForce": "IOC",
                "price": price_str,
                "size": req_size,
                "leverage": lev_str,
                "marginMode": "CROSS",
                "reduceOnly": false
            })
        } else {
            json!({
                "clientOid": Uuid::new_v4().to_string(),
                "symbol": sym,
                "side": side,
                "type": "market",
                "size": req_size,
                "leverage": lev_str,
                "marginMode": "CROSS",
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

        super::ws_kucoin_private::ensure_private_ws(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
        )
        .await;

        let v = super::ws_kucoin_order::send_order(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            body_json.clone(),
            api_timeout_ms,
        )
        .await?;

        // Diagnostic: log order request and response
        let resp_oid = v["data"]["orderId"].as_str().unwrap_or("none");
        tracing::info!(
            "[kucoin] place_order {} req={} → orderId={} code={}",
            pair,
            body_json,
            resp_oid,
            v["code"]
        );

        let mut v = v;
        if v["code"].as_str() != Some("200000") {
            let api_code = v["code"].as_str().unwrap_or("");
            let api_msg = v["msg"].as_str().unwrap_or("");
            // Handle error 300011: "Order price cannot be higher than 92.01."
            if api_code == "300011" {
                if let Some(clamped_str) = api_msg
                    .split("than ")
                    .nth(1)
                    .and_then(|s| s.trim_end_matches('.').trim().split_whitespace().next())
                {
                    let mut retry_body = body_json.clone();
                    retry_body["price"] = serde_json::json!(clamped_str);
                    tracing::warn!(
                        "[kucoin] ⚠️ Clamping order price to {} due to 300011 filter for {}",
                        clamped_str,
                        pair
                    );
                    if let Ok(v_retry) = super::ws_kucoin_order::send_order(
                        &keys.kucoin_key,
                        &keys.kucoin_secret,
                        &keys.kucoin_passphrase,
                        retry_body,
                        api_timeout_ms,
                    )
                    .await
                    {
                        if v_retry["code"].as_str() == Some("200000") {
                            v = v_retry;
                        } else {
                            return Err(format!("KuCoin API Error: {}", v_retry));
                        }
                    } else {
                        return Err(format!("KuCoin API Error: {}", v));
                    }
                } else {
                    return Err(format!("KuCoin API Error: {}", v));
                }
            } else {
                return Err(format!("KuCoin API Error: {}", v));
            }
        }

        let order_id = v["data"]["orderId"].as_str().map(|s| s.to_string());
        if let Some(ref oid_str) = order_id {
            // Wait up to 500ms for WS execution report (like Bybit).
            // Without this, KuCoin returns (orderId, 0.0, 0.0) immediately,
            // triggering IOC fast-path (2 retries) → ASYMMETRIC_CLOSE.
            let cache = super::ws_kucoin_private::get_order_cache();
            let mut inline_price = 0.0;
            let mut inline_fee = 0.0;
            for _wait in 0..5 {
                // 5 × 20ms = 100ms max (verify_fill handles the rest)
                if let Some(state) = cache.get(oid_str) {
                    // BUG-FIX: Wait for status="done" before taking final VWAP.
                    // Previously broke on first partial fill (filled_size > 0),
                    // which gave incomplete VWAP when order had multiple fills.
                    if state.status == "done" {
                        // WS matchPrice = per-unit price, matchSize = contracts.
                        // deal_value = Σ(matchPrice × matchSize), filled_size = Σ(matchSize).
                        // VWAP = deal_value / filled_size = weighted avg per-unit price.
                        inline_price = if state.filled_size > 0.0 {
                            state.deal_value / state.filled_size
                        } else {
                            0.0
                        };
                        inline_fee = state.fee.abs();
                        tracing::info!(
                            "[kucoin] ⚡ Inline fill {} price={:.6} fee={:.6} size={:.1} raw_deal_value={:.8} raw_filled_size={:.4} mult={}",
                            pair, inline_price, inline_fee, state.filled_size,
                            state.deal_value, state.filled_size, multiplier
                        );
                        // Sanity check: if inline_price is way off from target_price, something is wrong
                        if target_price > 0.0 && inline_price > 0.0 {
                            let ratio = inline_price / target_price;
                            if ratio > 2.0 || ratio < 0.5 {
                                tracing::error!(
                                    "[kucoin] 🚨 PRICE SANITY FAIL {}: inline={:.8} target={:.8} ratio={:.2} deal_val={:.8} filled_sz={:.4}. Using target_price!",
                                    pair, inline_price, target_price, ratio, state.deal_value, state.filled_size
                                );
                                inline_price = target_price;
                            }
                        }
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            // If WS didn't report "done" in 500ms but we have partial fills, use them.
            // This is a fallback — verify_fill will confirm via REST anyway.
            if inline_price <= 0.0 {
                if let Some(state) = cache.get(oid_str) {
                    if state.filled_size > 0.0 {
                        inline_price = state.deal_value / state.filled_size;
                        inline_fee = state.fee.abs();
                        tracing::warn!(
                            "[kucoin] ⏳ Partial WS fill {} (no done): price={:.6} fee={:.6} size={:.1}",
                            pair, inline_price, inline_fee, state.filled_size
                        );
                    }
                }
            }
            Ok((order_id, inline_price, inline_fee))
        } else {
            Err("KuCoin failed to return orderId".to_string())
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
        retries: u32,
        delay_ms: u64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        let sym = format!("{}M", pair);
        let side = if is_buy { "buy" } else { "sell" };

        let lev_str = (leverage.max(1.0) as i64).to_string();

        let mut final_price = target_price;
        if final_price <= 0.0 {
            // Fetch mark price via transport layer
            let ticker_path = format!("/api/v1/level1?symbol={}", sym);
            if let Ok(v) = super::ws_kucoin::send_kucoin_public(&ticker_path).await {
                if let Some(mark) = v["data"]["price"]
                    .as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| v["data"]["price"].as_f64())
                {
                    if mark > 0.0 {
                        final_price = if is_buy { mark * 1.05 } else { mark * 0.95 };
                    }
                }
            }
        }

        let price = if final_price > 0.0 {
            if is_buy {
                final_price * (1.0 + slippage_percent / 100.0)
            } else {
                final_price * (1.0 - slippage_percent / 100.0)
            }
        } else {
            0.0
        };

        let mut price_str = "".to_string();
        if price > 0.0 {
            let rounded_price = if is_buy {
                (price / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8)
            } else {
                (price / tick_size.max(1e-8)).floor() * tick_size.max(1e-8)
            };
            let mut s = format!("{:.8}", rounded_price)
                .trim_end_matches('0')
                .to_string();
            if s.ends_with('.') {
                s.pop();
            }
            if !s.is_empty() && s != "0" {
                price_str = s;
            }
        }

        let req_size = if multiplier == 0.0 {
            phys_amount.round() as i64
        } else {
            (phys_amount / multiplier.max(1e-12)).round() as i64
        };
        if req_size == 0 {
            return Err("Amount too small to close".to_string());
        }

        let is_market = price_str.is_empty();

        let body_json = if is_market {
            json!({
                "clientOid": Uuid::new_v4().to_string(),
                "symbol": sym,
                "side": side,
                "type": "market",
                "size": req_size,
                "leverage": lev_str,
                "marginMode": "CROSS",
                "reduceOnly": true
            })
        } else {
            json!({
                "clientOid": Uuid::new_v4().to_string(),
                "symbol": sym,
                "side": side,
                "type": "limit",
                "timeInForce": "IOC",
                "price": price_str,
                "size": req_size,
                "leverage": lev_str,
                "marginMode": "CROSS",
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

        super::ws_kucoin_private::ensure_private_ws(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
        )
        .await;

        let v_resp = super::ws_kucoin_order::send_order(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            body_json,
            api_timeout_ms,
        )
        .await?;

        if v_resp["code"].as_str() != Some("200000") {
            let api_code = v_resp["code"].as_str().unwrap_or("");
            let api_msg = v_resp["msg"].as_str().unwrap_or("");
            tracing::error!("[kucoin] close rejected: {}", v_resp);
            // If exchange says no position exists, return error immediately (don't retry)
            if api_code == "300009" || api_msg.to_lowercase().contains("no open position") {
                return Err(format!("Kucoin API error {}: {}", api_code, api_msg));
            }
        }

        let order_id = v_resp["data"]["orderId"].as_str().map(|s| s.to_string());
        if let Some(ref oid) = order_id {
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
            "[kucoin] fallback close triggered for {} (qty {})",
            pair,
            phys_amount
        );
        let mut fb_price_str = "".to_string();
        if final_price > 0.0 {
            // Use the same slippage_percent as the first attempt — do NOT widen to 2%.
            let p = if is_buy {
                final_price * (1.0 + slippage_percent / 100.0)
            } else {
                final_price * (1.0 - slippage_percent / 100.0)
            };
            let rp = if is_buy {
                (p / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8)
            } else {
                (p / tick_size.max(1e-8)).floor() * tick_size.max(1e-8)
            };
            fb_price_str = format!("{:.8}", rp).trim_end_matches('0').to_string();
            if fb_price_str.ends_with('.') {
                fb_price_str.pop();
            }
            if fb_price_str == "0" {
                fb_price_str = "".to_string();
            }
        }

        let is_fb_market = fb_price_str.is_empty();

        let fb_body = if is_fb_market {
            json!({
                "clientOid": Uuid::new_v4().to_string(),
                "symbol": sym,
                "side": side,
                "type": "market",
                "size": req_size,
                "leverage": lev_str,
                "marginMode": "CROSS",
                "reduceOnly": true
            })
        } else {
            json!({
                "clientOid": Uuid::new_v4().to_string(),
                "symbol": sym,
                "side": side,
                "type": "limit",
                "timeInForce": "IOC",
                "price": fb_price_str,
                "size": req_size,
                "leverage": lev_str,
                "marginMode": "CROSS",
                "reduceOnly": true
            })
        };
        let v_fb = super::ws_kucoin_order::send_order(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            fb_body,
            api_timeout_ms,
        )
        .await?;
        let oid_fb = v_fb["data"]["orderId"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| {
                let api_code = v_fb["code"].as_str().unwrap_or("?");
                let api_msg = v_fb["msg"].as_str().unwrap_or("unknown");
                tracing::error!("[kucoin] Fallback rejected, response: {}", v_fb);
                format!("Kucoin Fallback rejected {}: {}", api_code, api_msg)
            })?;

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

        tracing::warn!("[kucoin] fallback close unverified for {} (oid {}). Assuming filled to prevent CLOSING trap.", pair, oid_fb);
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
        _fallback_price: f64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        // Drop excess using market order natively via close_position signature
        self.close_position(
            http,
            keys,
            pair,
            is_buy,
            phys_excess,
            dry_run,
            1.0,
            0.0, // target_price = 0.0 (triggers aggressive L1 limit fallback)
            true,
            0.0, // slippage_percent (not used when target_price is 0.0)
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
        _pair: &str,
        order_id: &str,
        multiplier: f64,
    ) -> Result<(f64, f64, f64), String> {
        let mult = if multiplier > 0.0 { multiplier } else { 1.0 };

        // ═══════════════════════════════════════════════════════════════════
        // STRATEGY: Always get price from /api/v1/fills (real fill prices).
        // KuCoin order API returns dealAvgPrice=null for IOC orders,
        // and dealValue/dealSize gives per-CONTRACT value (wrong units).
        // Fills API returns actual matchPrice (per-unit) for each fill.
        // ═══════════════════════════════════════════════════════════════════

        // 1. First check order status via order API (to know if filled)
        let path = format!("/api/v1/orders/{}", order_id);
        let mut deal_size = 0.0_f64;

        for attempt in 0..6 {
            // Quick WS check first
            let cache = super::ws_kucoin_private::get_order_cache();
            if let Some(state) = cache.get(order_id) {
                if state.status == "done" {
                    deal_size = state.filled_size;
                    break;
                }
                if state.filled_size > 0.0 {
                    deal_size = state.filled_size;
                }
            }

            if attempt < 3 {
                tokio::time::sleep(Duration::from_millis(150)).await;
                continue;
            }

            // REST fallback after 450ms
            match super::ws_kucoin::send_kucoin_request(
                &keys.kucoin_key,
                &keys.kucoin_secret,
                &keys.kucoin_passphrase,
                "GET",
                &path,
                "",
                10000,
            )
            .await
            {
                Ok(v) => {
                    if v["code"].as_str() != Some("200000") {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                    let size = v["data"]["dealSize"]
                        .as_f64()
                        .or_else(|| v["data"]["filledSize"].as_f64())
                        .or_else(|| v["data"]["dealSize"].as_str().and_then(|s| s.parse().ok()))
                        .or_else(|| {
                            v["data"]["filledSize"]
                                .as_str()
                                .and_then(|s| s.parse().ok())
                        })
                        .unwrap_or(0.0);
                    let is_done = v["data"]["status"].as_str() == Some("done");
                    if size > 0.0 {
                        deal_size = size;
                    }
                    if is_done && size <= 0.0 {
                        return Ok((0.0, 0.0, 0.0));
                    }
                    if is_done {
                        break;
                    }
                }
                Err(_) => {}
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        if deal_size <= 0.0 {
            return Err("Not filled yet".to_string());
        }

        // 2. Get REAL price and fee from fills API — this is the source of truth
        let path_fills = format!("/api/v1/fills?orderId={}", order_id);
        let mut fills_price = 0.0_f64;
        let mut fills_fee = 0.0_f64;
        let mut fills_found = false;

        for fill_attempt in 0..3 {
            match super::ws_kucoin::send_kucoin_request(
                &keys.kucoin_key,
                &keys.kucoin_secret,
                &keys.kucoin_passphrase,
                "GET",
                &path_fills,
                "",
                5000,
            )
            .await
            {
                Ok(v2) => {
                    if let Some(items) = v2["data"]["data"]
                        .as_array()
                        .or_else(|| v2["data"]["items"].as_array())
                    {
                        if !items.is_empty() {
                            let mut sum_fee = 0.0_f64;
                            let mut sum_value = 0.0_f64;
                            let mut sum_size = 0.0_f64;
                            for item in items {
                                let fp = item["price"]
                                    .as_f64()
                                    .or_else(|| item["price"].as_str().and_then(|s| s.parse().ok()))
                                    .unwrap_or(0.0);
                                let fs = item["size"]
                                    .as_f64()
                                    .or_else(|| item["size"].as_str().and_then(|s| s.parse().ok()))
                                    .unwrap_or(0.0);
                                let ff = item["fee"]
                                    .as_f64()
                                    .or_else(|| item["fee"].as_str().and_then(|s| s.parse().ok()))
                                    .unwrap_or(0.0);
                                sum_value += fp * fs;
                                sum_size += fs;
                                sum_fee += ff;
                            }
                            if sum_size > 0.0 {
                                // VWAP from fills = Σ(matchPrice × matchSize) / Σ(matchSize)
                                // matchPrice is per-unit price (e.g. 0.10474 per ESPORTS token)
                                fills_price = sum_value / sum_size;
                                fills_fee = sum_fee.abs();
                                fills_found = true;
                                tracing::info!(
                                    "[kucoin] 📊 fetch_order {} FILLS: price={:.8} fee={:.6} fills={} sum_val={:.8} sum_sz={:.4}",
                                    order_id, fills_price, fills_fee, items.len(), sum_value, sum_size
                                );
                            }
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "[kucoin] fills API attempt {} failed for {}: {}",
                        fill_attempt,
                        order_id,
                        e
                    );
                }
            }
            if fill_attempt < 2 {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }

        // 3. Fallback: WS inline price (matchPrice from WS is also per-unit)
        if !fills_found {
            let cache = super::ws_kucoin_private::get_order_cache();
            if let Some(state) = cache.get(order_id) {
                if state.filled_size > 0.0 && state.deal_value > 0.0 {
                    fills_price = state.deal_value / state.filled_size;
                    fills_fee = state.fee.abs();
                    tracing::warn!(
                        "[kucoin] 📊 fetch_order {} WS fallback: price={:.8} fee={:.6}",
                        order_id,
                        fills_price,
                        fills_fee
                    );
                    fills_found = true;
                }
            };
        }

        if !fills_found || fills_price <= 0.0 {
            tracing::error!(
                "[kucoin] ❌ fetch_order {} NO PRICE from fills or WS. deal_size={}",
                order_id,
                deal_size
            );
            return Err(format!("No fill price available for order {}", order_id));
        }

        let phys_volume = deal_size * mult;

        tracing::info!(
            "[kucoin] 📊 fetch_order {} FINAL: price={:.8} fee={:.6} phys={:.4} ({}×{}) fills_api={}",
            order_id, fills_price, fills_fee, phys_volume, deal_size, mult, fills_found
        );

        Ok((fills_price, fills_fee, phys_volume))
    }

    async fn fetch_balances(
        &self,
        __http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<(f64, f64), String> {
        let path = "/api/v1/account-overview?currency=USDT";
        let v = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            "GET",
            path,
            "",
            10000,
        )
        .await?;
        let avail = v["data"]["availableBalance"]
            .as_f64()
            .or_else(|| {
                v["data"]["availableBalance"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(0.0);
        let equity = v["data"]["accountEquity"]
            .as_f64()
            .or_else(|| {
                v["data"]["accountEquity"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(0.0);
        Ok((equity, avail))
    }

    async fn fetch_taker_fee(&self, _http: &Client, keys: &ExchangeKeys, pair: &str) -> Result<f64, String> {
        let symbol = managed_kucoin_symbol(pair);
        let path = format!("/api/v1/trade-fees?symbol={symbol}");
        let v = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            "GET",
            &path,
            "",
            10000,
        )
        .await?;
        let taker = v["data"]["takerFeeRate"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| v["data"]["takerFeeRate"].as_f64())
            .ok_or_else(|| "Failed to parse KuCoin symbol fee".to_string())?;
        if !(0.0..=0.01).contains(&taker) {
            return Err("KuCoin returned an invalid taker fee".into());
        }
        Ok(taker)
    }

    async fn ensure_leverage(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        leverage: f64,
    ) -> Result<(), String> {
        let symbol = managed_kucoin_symbol(pair);
        let margin_path = format!("/api/v2/position/getMarginMode?symbol={symbol}");
        let leverage_path = format!("/api/v2/getCrossUserLeverage?symbol={symbol}");
        // These are independent, read-only confirmations. Starting them together
        // avoids consuming a short-lived edge with three sequential network trips.
        let (position_mode, mode, current) = tokio::join!(
            kucoin_managed_call(keys, "GET", "/api/v2/position/getPositionMode", ""),
            kucoin_managed_call(keys, "GET", &margin_path, ""),
            kucoin_managed_call(keys, "GET", &leverage_path, ""),
        );
        let position_mode = position_mode?;
        let mode = mode?;
        let current = current?;
        // Only the independent reads are concurrent; any required account
        // mutation below remains ordered and is authoritatively re-verified.
        if position_mode["positionMode"].as_i64() != Some(0) {
            return Err("Managed execution requires KuCoin one-way mode".into());
        }
        if mode["symbol"].as_str() != Some(symbol.as_str()) {
            return Err("KuCoin margin symbol mismatch".into());
        }
        if !matches!(mode["marginMode"].as_str(), Some("CROSS" | "ISOLATED")) {
            return Err("Unknown KuCoin margin mode".into());
        }
        if mode["marginMode"].as_str() != Some("CROSS") {
            let changed = kucoin_managed_call(
                keys,
                "POST",
                "/api/v2/position/changeMarginMode",
                &json!({"symbol":symbol,"marginMode":"CROSS"}).to_string(),
            )
            .await?;
            if changed["symbol"].as_str() != Some(symbol.as_str())
                || changed["marginMode"].as_str() != Some("CROSS")
            {
                return Err("KuCoin cross margin not confirmed".into());
            }
        }
        if !kucoin_leverage_matches(&current, &symbol, leverage) {
            let changed = kucoin_managed_call(
                keys,
                "POST",
                "/api/v2/changeCrossUserLeverage",
                &json!({"symbol":symbol,"leverage":leverage.to_string()}).to_string(),
            )
            .await?;
            if changed.as_bool() != Some(true) {
                return Err("KuCoin leverage change not acknowledged".into());
            }
            let confirmed = kucoin_managed_call(keys, "GET", &leverage_path, "").await?;
            if !kucoin_leverage_matches(&confirmed, &symbol, leverage) {
                return Err("KuCoin cross leverage not confirmed".into());
            }
        }
        Ok(())
    }

    async fn fetch_positions(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<Vec<PositionInfo>, String> {
        let path = "/api/v1/positions";
        let v = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            "GET",
            path,
            "",
            10000,
        )
        .await?;
        let code = v["code"].as_str().unwrap_or("");
        if code != "200000" {
            return Err(format!("Kucoin API returned error: {}", v));
        }
        let mut positions = Vec::new();
        {
            let arr = v["data"].as_array().ok_or("Malformed KuCoin positions")?;
            for p in arr {
                let qty = crate::engine::orders::number(p, "currentQty")?;
                if qty.abs() < 1e-12 {
                    continue;
                }
                let raw_sym = p["symbol"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or("Missing KuCoin position symbol")?;
                let mut std_sym = raw_sym.replace("USDTM", "USDT").replace("USDM", "USDT");
                if std_sym == "XBTUSDT" {
                    std_sym = "BTCUSDT".to_string();
                }
                let realized = p["realisedPnl"]
                    .as_f64()
                    .or_else(|| p["realisedPnl"].as_str().and_then(|s| s.parse().ok()))
                    .unwrap_or(0.0);
                positions.push(PositionInfo {
                    symbol: std_sym,
                    size: qty,
                    entry_price: p["avgEntryPrice"]
                        .as_f64()
                        .or_else(|| p["avgEntryPrice"].as_str().and_then(|s| s.parse().ok()))
                        .unwrap_or(0.0),
                    unrealized_pnl: p["unrealisedPnl"]
                        .as_f64()
                        .or_else(|| p["unrealisedPnl"].as_str().and_then(|s| s.parse().ok()))
                        .unwrap_or(0.0),
                    realized_pnl: realized,
                    accumulated_fee: 0.0,
                    accumulated_funding: 0.0,
                });
            }
        }
        Ok(positions)
    }

    async fn audit_trade(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<TradeAuditResult, String> {
        let norm = pair.replace('_', "").replace('-', "").to_uppercase();
        let kucoin_sym = if norm == "BTCUSDT" {
            "XBTUSDTM".to_string()
        } else if norm.ends_with("USDT") {
            format!("{}M", norm)
        } else {
            norm
        };
        let path = format!(
            "/api/v1/fills?symbol={}&startAt={}&endAt={}",
            kucoin_sym, start_ts, end_ts
        );
        let v = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            "GET",
            &path,
            "",
            10000,
        )
        .await?;

        let code = v["code"].as_str().unwrap_or("");
        if code != "200000" {
            return Err(format!("KuCoin fills audit error: {}", v));
        }

        let mut total_fee = 0.0_f64;
        let mut first_price = None;
        let mut last_price = None;
        let mut sum_qty = 0.0_f64;

        let empty_vec = Vec::new();
        let items = v["data"]["items"]
            .as_array()
            .or_else(|| v["data"].as_array())
            .unwrap_or(&empty_vec);

        for fill in items {
            let fee: f64 = fill["fee"]
                .as_f64()
                .or_else(|| fill["fee"].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(0.0);
            let price: f64 = fill["matchPrice"]
                .as_f64()
                .or_else(|| fill["matchPrice"].as_str().and_then(|s| s.parse().ok()))
                .or_else(|| fill["price"].as_f64())
                .unwrap_or(0.0);
            let size: f64 = fill["size"]
                .as_f64()
                .or_else(|| fill["size"].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(0.0);

            total_fee += fee.abs();
            sum_qty += size;
            if first_price.is_none() && price > 0.0 {
                first_price = Some(price);
            }
            if price > 0.0 {
                last_price = Some(price);
            }
        }

        Ok(TradeAuditResult {
            total_fee,
            entry_price: first_price,
            exit_price: last_price,
            realized_pnl: None,
            funding_cost: None,
            filled_qty: sum_qty,
        })
    }
}

fn managed_kucoin_symbol(pair: &str) -> String {
    if pair == "BTCUSDT" {
        "XBTUSDTM".into()
    } else {
        format!("{}M", pair)
    }
}

#[cfg(test)]
mod public_depth_tests {
    use super::*;

    fn response(timestamp_ns: i64) -> Value {
        json!({
            "code": "200000",
            "data": {
                "symbol": "NILUSDTM",
                "sequence": 42,
                "ts": timestamp_ns,
                "bids": [["0.1273", "2"], [0.1272, 3]],
                "asks": [["0.1274", "4"], [0.1275, 5]]
            }
        })
    }

    #[test]
    fn public_depth_parser_requires_fresh_identity_and_converts_contracts() {
        let book = parse_public_depth_snapshot(
            &response(9_800_000_000),
            "NILUSDT",
            10.0,
            10_000,
        )
        .unwrap();
        assert_eq!(book.updated_ms, 9_800);
        assert_eq!(book.bids[0], (0.1273, 20.0));
        assert_eq!(book.asks[1], (0.1275, 50.0));

        let mut wrong = response(9_800_000_000);
        wrong["data"]["symbol"] = json!("OTHERUSDTM");
        assert!(parse_public_depth_snapshot(&wrong, "NILUSDT", 10.0, 10_000).is_err());
        assert!(parse_public_depth_snapshot(
            &response(9_000_000_000),
            "NILUSDT",
            10.0,
            10_000,
        )
        .is_err());
        assert!(parse_public_depth_snapshot(
            &response(9_800_000_000),
            "NILUSDT",
            0.0,
            10_000,
        )
        .is_err());
    }

    #[test]
    fn public_depth_parser_rejects_crossed_unsorted_or_malformed_books() {
        let mut crossed = response(9_800_000_000);
        crossed["data"]["asks"][0][0] = json!(0.1272);
        assert!(parse_public_depth_snapshot(&crossed, "NILUSDT", 1.0, 10_000).is_err());

        let mut unsorted = response(9_800_000_000);
        unsorted["data"]["bids"][1][0] = json!(0.1274);
        assert!(parse_public_depth_snapshot(&unsorted, "NILUSDT", 1.0, 10_000).is_err());

        let mut malformed = response(9_800_000_000);
        malformed["data"]["asks"][0][1] = json!("nan");
        assert!(parse_public_depth_snapshot(&malformed, "NILUSDT", 1.0, 10_000).is_err());
    }

    #[tokio::test]
    #[ignore = "live KuCoin public depth check"]
    async fn live_public_depth_is_fresh_and_valid() {
        let client = Client::new();
        let mut last_error = None;
        let mut fresh_book = None;
        // KuCoin can occasionally serve a depth20 snapshot just outside the
        // production freshness bound. Production retries on its bounded lane,
        // so the ignored network test should exercise that same behavior
        // instead of failing on one correctly rejected stale response.
        for _ in 0..5 {
            match fetch_public_depth_snapshot(&client, "BTCUSDT", 0.001).await {
                Ok(book) => {
                    fresh_book = Some(book);
                    break;
                }
                Err(error) => last_error = Some(error),
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        let book = fresh_book.unwrap_or_else(|| {
            panic!(
                "KuCoin did not return a fresh BTC depth snapshot after retries: {}",
                last_error.unwrap_or_else(|| "no response".to_string()),
            )
        });
        assert!(!book.bids.is_empty());
        assert!(!book.asks.is_empty());
        assert!(book.bids[0].0 < book.asks[0].0);
        assert!(crate::engine::utils::local_timestamp_is_fresh(
            now_ms(),
            book.updated_ms,
            750,
        ));
    }
}

fn managed_kucoin_ws_observation(
    order_id: &str,
    state: &super::ws_kucoin_private::KucoinOrderState,
    request: &crate::engine::orders::OrderRequest,
) -> Result<Option<crate::engine::orders::OrderObservation>, String> {
    use crate::engine::orders::{OrderObservation, OrderState};
    if state.status != "done" {
        return Ok(None);
    }
    if !state.filled_size.is_finite()
        || !state.deal_value.is_finite()
        || !state.fee.is_finite()
        || state.filled_size < 0.0
        || state.deal_value < 0.0
    {
        return Err("Invalid KuCoin private-WS execution report".into());
    }
    let quantity = state.filled_size * request.multiplier;
    if quantity > request.quantity + request.quantity * 1e-8 {
        return Err("KuCoin private-WS fill exceeds requested quantity".into());
    }
    let average_price = if quantity > 0.0 {
        if state.deal_value <= 0.0 {
            return Err("KuCoin private-WS fill has no value".into());
        }
        Some(state.deal_value / state.filled_size)
    } else {
        None
    };
    let observation = OrderObservation {
        order_id: Some(order_id.to_string()),
        state: if quantity + request.quantity * 1e-8 >= request.quantity {
            OrderState::Filled
        } else {
            OrderState::Cancelled
        },
        filled_quantity: quantity,
        average_price,
        // A zero in the WS cache can mean the fee event has not arrived. Keep
        // it unknown so the PnL path reserves the configured taker fee.
        fee: if quantity == 0.0 {
            Some(0.0)
        } else if state.fee != 0.0 {
            Some(state.fee.abs())
        } else {
            None
        },
    };
    observation.validate(request)?;
    Ok(Some(observation))
}

fn parse_managed_kucoin(
    v: &Value,
    r: &crate::engine::orders::OrderRequest,
) -> Result<crate::engine::orders::OrderObservation, String> {
    use crate::engine::orders::{identity, number, OrderObservation, OrderState};
    if v["symbol"].as_str() != Some(managed_kucoin_symbol(&r.pair).as_str())
        || v["clientOid"].as_str() != Some(r.client_id.as_str())
    {
        return Err("KuCoin identity mismatch".into());
    }
    let qty = number(v, "dealSize")? * r.multiplier;
    let state = match v["isActive"].as_bool() {
        Some(false) => {
            if qty + 1e-10 >= r.quantity {
                OrderState::Filled
            } else {
                OrderState::Cancelled
            }
        }
        Some(true) => {
            if qty > 0.0 {
                OrderState::PartiallyFilled
            } else {
                OrderState::Submitted
            }
        }
        None => match v["status"].as_str() {
            Some("done") => {
                if qty + 1e-10 >= r.quantity {
                    OrderState::Filled
                } else {
                    OrderState::Cancelled
                }
            }
            Some("open") => OrderState::Submitted,
            _ => return Err("Missing KuCoin order state".into()),
        },
    };
    let fee = if qty == 0.0 && state.terminal() {
        Some(0.0)
    } else if v["feeCurrency"].as_str() == Some("USDT") {
        Some(number(v, "fee")?)
    } else {
        None
    };
    Ok(OrderObservation {
        order_id: identity(v, "id"),
        state,
        filled_quantity: qty,
        average_price: if qty > 0.0 {
            Some(number(v, "dealValue")? / qty)
        } else {
            None
        },
        fee,
    })
}

fn parse_kucoin_fill_fee(
    response: &Value,
    expected_order_id: &str,
    expected_quantity: f64,
    multiplier: f64,
) -> Result<Option<f64>, String> {
    use crate::engine::orders::{identity, number};
    if response["code"].as_str() != Some("200000") {
        return Err("KuCoin fills lookup rejected".into());
    }
    if !expected_quantity.is_finite()
        || expected_quantity <= 0.0
        || !multiplier.is_finite()
        || multiplier <= 0.0
    {
        return Err("Invalid KuCoin fee reconciliation quantity".into());
    }
    let data = &response["data"];
    let items = data["items"]
        .as_array()
        .or_else(|| data["data"].as_array())
        .ok_or("KuCoin fills response has no items")?;
    if items.is_empty() {
        return Err("KuCoin fills response is empty".into());
    }
    if data["totalNum"]
        .as_u64()
        .is_some_and(|total| total > items.len() as u64)
    {
        return Err("KuCoin fills response is paginated".into());
    }
    let mut seen = std::collections::HashSet::new();
    let mut filled_quantity = 0.0;
    let mut fee = 0.0;
    for fill in items {
        let order_id = identity(fill, "orderId").ok_or("KuCoin fill has no order id")?;
        if order_id != expected_order_id {
            return Err("KuCoin fill order identity mismatch".into());
        }
        let trade_id = identity(fill, "tradeId").ok_or("KuCoin fill has no trade id")?;
        if !seen.insert(trade_id) {
            continue;
        }
        if fill["feeCurrency"].as_str() != Some("USDT") {
            return Err("KuCoin fill fee is not denominated in USDT".into());
        }
        let size = number(fill, "size")?;
        if size <= 0.0 {
            return Err("KuCoin fill has non-positive size".into());
        }
        filled_quantity += size * multiplier;
        fee += number(fill, "fee")?.abs();
    }
    if (filled_quantity - expected_quantity).abs() > expected_quantity * 1e-8 + 1e-10 {
        return Err(format!(
            "KuCoin fills quantity mismatch: expected={expected_quantity} observed={filled_quantity}"
        ));
    }
    Ok(Some(fee))
}

fn kucoin_fill_history_path(
    order_id: &str,
    order_created_ms: i64,
    current_ms: i64,
) -> Result<String, String> {
    if order_id.is_empty() || order_created_ms <= 0 || current_ms < order_created_ms {
        return Err("Invalid KuCoin fill history window".into());
    }
    // KuCoin retains futures fills for three months, but each history query may
    // cover no more than seven days. Anchor the bounded window to the order so
    // reconciliation still works after the endpoint's default recent window.
    const MAX_WINDOW_MS: i64 = 7 * 24 * 60 * 60 * 1000;
    let start_ms = order_created_ms.saturating_sub(60_000);
    let end_ms = current_ms.min(start_ms.saturating_add(MAX_WINDOW_MS - 1));
    Ok(format!(
        "/api/v1/fills?orderId={order_id}&startAt={start_ms}&endAt={end_ms}"
    ))
}

#[cfg(test)]
mod managed_ws_tests {
    use super::*;
    use crate::engine::exchanges::ws_kucoin_private::KucoinOrderState;
    use crate::engine::orders::{OrderRequest, OrderState};

    fn request() -> OrderRequest {
        OrderRequest {
            client_id: "managed-test".into(),
            pair: "NILUSDT".into(),
            is_buy: true,
            quantity: 47.0,
            limit_price: 0.13,
            reduce_only: false,
            multiplier: 1.0,
            tick_size: 0.00001,
            leverage: 2.0,
        }
    }

    #[test]
    fn private_ws_done_report_can_complete_managed_fill_without_rest_lookup() {
        let state = KucoinOrderState {
            filled_size: 47.0,
            deal_value: 5.98592,
            fee: 0.003591552,
            status: "done".into(),
        };
        let observation = managed_kucoin_ws_observation("order-1", &state, &request())
            .unwrap()
            .unwrap();
        assert_eq!(observation.state, OrderState::Filled);
        assert_eq!(observation.filled_quantity, 47.0);
        assert!((observation.average_price.unwrap() - 0.12736).abs() < 1e-12);
        assert_eq!(observation.fee, Some(0.003591552));
    }

    #[test]
    fn private_ws_report_waits_for_done_and_rejects_overfill() {
        let mut state = KucoinOrderState {
            filled_size: 47.0,
            deal_value: 5.98592,
            fee: 0.0,
            status: "match".into(),
        };
        assert!(managed_kucoin_ws_observation("order-1", &state, &request())
            .unwrap()
            .is_none());
        state.status = "done".into();
        state.filled_size = 48.0;
        assert!(managed_kucoin_ws_observation("order-1", &state, &request()).is_err());
    }
}

async fn kucoin_managed_call(
    keys: &ExchangeKeys,
    method: &str,
    path: &str,
    body: &str,
) -> Result<Value, String> {
    let v = super::ws_kucoin::send_kucoin_request(
        &keys.kucoin_key,
        &keys.kucoin_secret,
        &keys.kucoin_passphrase,
        method,
        path,
        body,
        3000,
    )
    .await?;
    if v["code"].as_str() != Some("200000") || v.get("data").is_none() {
        return Err("KuCoin preflight query rejected".into());
    }
    Ok(v["data"].clone())
}
fn kucoin_leverage_matches(data: &Value, symbol: &str, leverage: f64) -> bool {
    data["symbol"].as_str() == Some(symbol)
        && crate::engine::orders::number(data, "leverage")
            .map(|n| (n - leverage).abs() < 1e-8)
            .unwrap_or(false)
}
#[cfg(test)]
mod cross_leverage_tests {
    use super::*;
    #[test]
    fn leverage_requires_matching_symbol_and_authoritative_value() {
        assert!(kucoin_leverage_matches(
            &json!({"symbol":"XBTUSDTM","leverage":"10"}),
            "XBTUSDTM",
            10.0
        ));
        assert!(!kucoin_leverage_matches(
            &json!({"symbol":"XBTUSDTM","leverage":"3"}),
            "XBTUSDTM",
            10.0
        ));
        assert!(!kucoin_leverage_matches(
            &json!({"symbol":"ETHUSDTM","leverage":"10"}),
            "XBTUSDTM",
            10.0
        ));
        assert!(!kucoin_leverage_matches(
            &json!({"symbol":"XBTUSDTM"}),
            "XBTUSDTM",
            10.0
        ));
    }

    #[test]
    fn fill_ledger_confirms_complete_usdt_fee() {
        let response = json!({
            "code": "200000",
            "data": {
                "totalNum": 2,
                "items": [
                    {"tradeId":"t1","orderId":"o1","size":"12","fee":"0.0012","feeCurrency":"USDT"},
                    {"tradeId":"t2","orderId":"o1","size":"12","fee":"0.0013","feeCurrency":"USDT"}
                ]
            }
        });
        let fee = parse_kucoin_fill_fee(&response, "o1", 240.0, 10.0)
            .unwrap()
            .unwrap();
        assert!((fee - 0.0025).abs() < 1e-12);
    }

    #[test]
    fn fill_ledger_rejects_wrong_identity_currency_or_partial_coverage() {
        let valid_fill = json!({
            "code": "200000",
            "data": {"items": [
                {"tradeId":"t1","orderId":"o1","size":"24","fee":"0.0025","feeCurrency":"USDT"}
            ]}
        });
        assert!(parse_kucoin_fill_fee(&valid_fill, "o1", 250.0, 10.0).is_err());
        let wrong_order = json!({
            "code": "200000",
            "data": {"items": [
                {"tradeId":"t1","orderId":"other","size":"24","fee":"0.0025","feeCurrency":"USDT"}
            ]}
        });
        assert!(parse_kucoin_fill_fee(&wrong_order, "o1", 240.0, 10.0).is_err());
        let wrong_currency = json!({
            "code": "200000",
            "data": {"items": [
                {"tradeId":"t1","orderId":"o1","size":"24","fee":"0.0025","feeCurrency":"KCS"}
            ]}
        });
        assert!(parse_kucoin_fill_fee(&wrong_currency, "o1", 240.0, 10.0).is_err());
    }

    #[test]
    fn historical_fill_query_is_anchored_and_bounded_to_seven_days() {
        let created = 1_000_000_000;
        let path = kucoin_fill_history_path("o1", created, created + 30 * 24 * 60 * 60 * 1000)
            .unwrap();
        assert!(path.contains("orderId=o1"));
        assert!(path.contains("startAt=999940000"));
        assert!(path.contains("endAt=1604739999"));
        assert!(kucoin_fill_history_path("", created, created).is_err());
    }
}
