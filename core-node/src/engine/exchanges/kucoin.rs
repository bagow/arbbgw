use async_trait::async_trait;
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

use super::traits::{CloseReceipt, ExchangeConnector, PositionInfo, TradeAuditResult};
use crate::engine::real::{now_ms, ExchangeKeys};

pub struct KucoinConnector;

#[async_trait]
impl ExchangeConnector for KucoinConnector {
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
        let v = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            "POST",
            "/api/v1/orders",
            &body.to_string(),
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
        Ok(crate::engine::orders::OrderObservation::pending(
            crate::engine::orders::identity(&v["data"], "orderId"),
        ))
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
        parse_managed_kucoin(&v["data"], r)
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
        let path = "/api/v1/orders";
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

        let body_str = body_json.to_string();

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

        let v = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            "POST",
            path,
            &body_str,
            api_timeout_ms,
        )
        .await?;

        // Diagnostic: log order request and response
        let resp_oid = v["data"]["orderId"].as_str().unwrap_or("none");
        tracing::info!(
            "[kucoin] place_order {} req={} → orderId={} code={}",
            pair,
            body_str,
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
                    let retry_body_str = retry_body.to_string();
                    tracing::warn!(
                        "[kucoin] ⚠️ Clamping order price to {} due to 300011 filter for {}",
                        clamped_str,
                        pair
                    );
                    if let Ok(v_retry) = super::ws_kucoin::send_kucoin_request(
                        &keys.kucoin_key,
                        &keys.kucoin_secret,
                        &keys.kucoin_passphrase,
                        "POST",
                        path,
                        &retry_body_str,
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
        let path = "/api/v1/orders";
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

        let body_str = body_json.to_string();

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

        let v_resp = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            "POST",
            path,
            &body_str,
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
        let fb_str = fb_body.to_string();

        let v_fb = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key,
            &keys.kucoin_secret,
            &keys.kucoin_passphrase,
            "POST",
            path,
            &fb_str,
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

    async fn fetch_taker_fee(&self, _http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        let path = "/api/v1/trade-fees?symbol=XBTUSDTM";
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
        let taker = v["data"]["takerFeeRate"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| v["data"]["takerFeeRate"].as_f64())
            .unwrap_or(0.0006);
        Ok(taker)
    }

    async fn ensure_leverage(
        &self,
        __http: &Client,
        _keys: &ExchangeKeys,
        _pair: &str,
        _leverage: f64,
    ) -> Result<(), String> {
        // KuCoin leverage is set directly in the order payload.
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
