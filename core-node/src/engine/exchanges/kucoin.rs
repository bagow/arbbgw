use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

use super::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::{now_ms, ExchangeKeys};

pub struct KucoinConnector;

#[async_trait]
impl ExchangeConnector for KucoinConnector {
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

        super::ws_kucoin_private::ensure_private_ws(&keys.kucoin_key, &keys.kucoin_secret, &keys.kucoin_passphrase).await;

        let v = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key, &keys.kucoin_secret, &keys.kucoin_passphrase,
            "POST", path, &body_str, api_timeout_ms,
        ).await?;

        // Diagnostic: log order request and response
        let resp_oid = v["data"]["orderId"].as_str().unwrap_or("none");
        tracing::info!(
            "[kucoin] place_order {} req={} → orderId={} code={}",
            pair,
            body_str,
            resp_oid,
            v["code"]
        );

        if v["code"].as_str() != Some("200000") {
            return Err(format!("KuCoin API Error: {}", v));
        }

        let order_id = v["data"]["orderId"].as_str().map(|s| s.to_string());
        if let Some(ref oid_str) = order_id {
            // Wait up to 500ms for WS execution report (like Bybit).
            // Without this, KuCoin returns (orderId, 0.0, 0.0) immediately,
            // triggering IOC fast-path (2 retries) → ASYMMETRIC_CLOSE.
            let cache = super::ws_kucoin_private::get_order_cache();
            let mut inline_price = 0.0;
            let mut inline_fee = 0.0;
            for _wait in 0..25 {  // 25 × 20ms = 500ms
                if let Some(state) = cache.get(oid_str) {
                    if state.status == "done" || state.filled_size > 0.0 {
                        let mult = if multiplier > 0.0 { multiplier } else { 1.0 };
                        inline_price = if state.filled_size > 0.0 && mult > 0.0 {
                            state.deal_value / (state.filled_size * mult)
                        } else { 0.0 };
                        inline_fee = state.fee.abs();
                        tracing::info!(
                            "[kucoin] ⚡ Inline fill {} price={:.6} fee={:.6} size={:.1}",
                            pair, inline_price, inline_fee, state.filled_size
                        );
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
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
                if let Some(mark) = v["data"]["price"].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v["data"]["price"].as_f64()) {
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

        super::ws_kucoin_private::ensure_private_ws(&keys.kucoin_key, &keys.kucoin_secret, &keys.kucoin_passphrase).await;

        let v_resp = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key, &keys.kucoin_secret, &keys.kucoin_passphrase,
            "POST", path, &body_str, api_timeout_ms,
        ).await?;

        if v_resp["code"].as_str() != Some("200000") {
            tracing::error!("[kucoin] close rejected: {}", v_resp);
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
            let p = if is_buy { final_price * 1.02 } else { final_price * 0.98 };
            let rp = if is_buy {
                (p / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8)
            } else {
                (p / tick_size.max(1e-8)).floor() * tick_size.max(1e-8)
            };
            fb_price_str = format!("{:.8}", rp).trim_end_matches('0').to_string();
            if fb_price_str.ends_with('.') { fb_price_str.pop(); }
            if fb_price_str == "0" { fb_price_str = "".to_string(); }
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
            &keys.kucoin_key, &keys.kucoin_secret, &keys.kucoin_passphrase,
            "POST", path, &fb_str, api_timeout_ms,
        ).await?;
        let oid_fb = v_fb["data"]["orderId"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| {
                tracing::error!("[kucoin] Fallback rejected, response: {}", v_fb);
                "Kucoin Fallback rejected".to_string()
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
            0.0,              // target_price = 0.0 (triggers aggressive L1 limit fallback)
            true,
            0.0,              // slippage_percent (not used when target_price is 0.0)
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
        let path = format!("/api/v1/orders/{}", order_id);
        
        let mut final_size = 0.0;
        let mut final_val = 0.0;
        let mut final_fee = 0.0;
        let mut resolved_via_ws = false;

        // 1. Fast path: Wait for WS execution report (up to 1000ms)
        let cache = super::ws_kucoin_private::get_order_cache();
        for _ in 0..50 { // 50 * 20ms = 1000ms
            if let Some(state) = cache.get(order_id) {
                if state.status == "done" || state.filled_size > 0.0 {
                    final_size = state.filled_size;
                    final_val = state.deal_value;
                    final_fee = state.fee;
                    resolved_via_ws = true;
                    tracing::debug!("[kucoin] fetch_order {} resolved via WS in <1s!", order_id);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let mut retries = if resolved_via_ws { 0 } else { 5 };

        while !resolved_via_ws {
            let v = super::ws_kucoin::send_kucoin_request(
                &keys.kucoin_key, &keys.kucoin_secret, &keys.kucoin_passphrase,
                "GET", &path, "", 10000,
            ).await?;

            if v["code"].as_str() != Some("200000") {
                if retries > 0 {
                    retries -= 1;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                return Err(format!("Kucoin fetch_order error: {}", v));
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
            
            let val = v["data"]["dealValue"]
                .as_f64()
                .or_else(|| v["data"]["filledValue"].as_f64())
                .or_else(|| v["data"]["dealValue"].as_str().and_then(|s| s.parse().ok()))
                .or_else(|| {
                    v["data"]["filledValue"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                })
                .unwrap_or(0.0);
            
            let fee = v["data"]["fee"]
                .as_f64()
                .or_else(|| v["data"]["fee"].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(0.0);

            let status = v["data"]["status"].as_str().unwrap_or("");
            
            if size <= 0.0 {
                if retries > 0 {
                    retries -= 1;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                if status == "done" {
                    return Ok((0.0, 0.0, 0.0));
                }
                return Err("Not filled yet".to_string());
            }

            final_size = size;
            final_val = val;
            final_fee = fee;
            break;
        }

        // Note: KuCoin multiplier is dynamically passed to correctly compute price
        let mult = if multiplier > 0.0 { multiplier } else { 1.0 };

        let price = if final_size > 0.0 && mult > 0.0 {
            final_val / (final_size * mult)
        } else {
            0.0
        };

        let fee = final_fee;
        if price > 0.0 && !resolved_via_ws && fee <= 1e-9 {
            let path_fills = format!("/api/v1/fills?orderId={}", order_id);
            if let Ok(v2) = super::ws_kucoin::send_kucoin_request(
                &keys.kucoin_key, &keys.kucoin_secret, &keys.kucoin_passphrase,
                "GET", &path_fills, "", 5000,
            ).await {
                if let Some(items) = v2["data"]["data"].as_array().or_else(|| v2["data"]["items"].as_array()) {
                    let mut sum_fee = 0.0;
                    for item in items {
                        sum_fee += item["fee"].as_f64().or_else(|| item["fee"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
                    }
                    if sum_fee > 1e-9 { final_fee = sum_fee; }
                }
            }
        }

        Ok((price, final_fee, final_size * mult))
    }

    async fn fetch_balances(&self, __http: &Client, keys: &ExchangeKeys) -> Result<(f64, f64), String> {
        let path = "/api/v1/account-overview?currency=USDT";
        let v = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key, &keys.kucoin_secret, &keys.kucoin_passphrase,
            "GET", path, "", 10000,
        ).await?;
        let avail = v["data"]["availableBalance"].as_f64().or_else(|| v["data"]["availableBalance"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
        let equity = v["data"]["accountEquity"].as_f64().or_else(|| v["data"]["accountEquity"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
        Ok((equity, avail))
    }

    async fn fetch_taker_fee(&self, _http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        let path = "/api/v1/trade-fees?symbol=XBTUSDTM";
        let v = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key, &keys.kucoin_secret, &keys.kucoin_passphrase,
            "GET", path, "", 10000,
        ).await?;
        let taker = v["data"]["takerFeeRate"].as_str().and_then(|s| s.parse().ok()).or_else(|| v["data"]["takerFeeRate"].as_f64()).unwrap_or(0.0006);
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

    async fn get_funding_rate(&self, _http: &Client, pair: &str) -> Result<(f64, i64), String> {
        let sym = format!("{}M", pair);
        let path = format!("/api/v1/funding-rate/{}/current", sym);
        match super::ws_kucoin::send_kucoin_public(&path).await {
            Ok(json) => {
                let rate = json["data"]["value"].as_f64().unwrap_or(0.0);
                let next = json["data"]["timePoint"].as_i64().unwrap_or(0) + 8 * 3600 * 1000;
                Ok((rate, next))
            }
            Err(_) => Ok((0.0, 0))
        }
    }

    async fn fetch_positions(&self, _http: &Client, keys: &ExchangeKeys) -> Result<Vec<PositionInfo>, String> {
        let path = "/api/v1/positions";
        let v = super::ws_kucoin::send_kucoin_request(
            &keys.kucoin_key, &keys.kucoin_secret, &keys.kucoin_passphrase,
            "GET", path, "", 10000,
        ).await?;
        let code = v["code"].as_str().unwrap_or("");
        if code != "200000" { return Err(format!("Kucoin API returned error: {}", v)); }
        let mut positions = Vec::new();
        if let Some(arr) = v["data"].as_array() {
            for p in arr {
                let qty: f64 = p["currentQty"].as_f64().or_else(|| p["currentQty"].as_i64().map(|i| i as f64)).unwrap_or(0.0);
                if qty.abs() < 1e-12 { continue; }
                let raw_sym = p["symbol"].as_str().unwrap_or("");
                let mut std_sym = raw_sym.replace("USDTM", "USDT").replace("USDM", "USDT");
                if std_sym == "XBTUSDT" { std_sym = "BTCUSDT".to_string(); }
                let realized = p["realisedPnl"].as_f64().or_else(|| p["realisedPnl"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
                positions.push(PositionInfo {
                    symbol: std_sym, size: qty,
                    entry_price: p["avgEntryPrice"].as_f64().or_else(|| p["avgEntryPrice"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0),
                    unrealized_pnl: p["unrealisedPnl"].as_f64().or_else(|| p["unrealisedPnl"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0),
                    realized_pnl: realized, accumulated_fee: 0.0, accumulated_funding: 0.0,
                });
            }
        }
        Ok(positions)
    }

    async fn fetch_orderbook(&self, _http: &Client, pair: &str, _depth: u32) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), String> {
        let sym = format!("{}M", pair);
        let path = format!("/api/v1/level2/depth20?symbol={}", sym);
        let json = super::ws_kucoin::send_kucoin_public(&path).await?;
        if json["code"].as_str() != Some("200000") {
            return Err(format!("Kucoin API returned error: {}", json));
        }
        let parse_levels = |levels: &Value| -> Vec<(f64, f64)> {
            let mut out = Vec::new();
            if let Some(arr) = levels.as_array() {
                for item in arr {
                    if let (Some(px_val), Some(sz_val)) = (item.get(0), item.get(1)) {
                        let px = px_val.as_f64().or_else(|| px_val.as_str().and_then(|s| s.parse().ok()));
                        let sz = sz_val.as_f64().or_else(|| sz_val.as_str().and_then(|s| s.parse().ok())).or_else(|| sz_val.as_i64().map(|i| i as f64));
                        if let (Some(p), Some(s)) = (px, sz) { out.push((p, s)); }
                    }
                }
            }
            out
        };
        let bids = parse_levels(&json["data"]["bids"]);
        let asks = parse_levels(&json["data"]["asks"]);
        Ok((bids, asks))
    }
}
