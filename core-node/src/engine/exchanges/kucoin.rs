use reqwest::Client;
use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;
use serde_json::json;
use uuid::Uuid;

use crate::engine::real::{ExchangeKeys, now_ms, kucoin_sign};
use super::traits::{ExchangeConnector, CloseReceipt, PositionInfo};

pub struct KucoinConnector;

#[async_trait]
impl ExchangeConnector for KucoinConnector {
    fn name(&self) -> &'static str {
        "kucoin"
    }

    async fn place_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, phys_amount: f64, dry_run: bool, target_price: f64, use_ioc: bool, leverage: f64, multiplier: f64, slippage_percent: f64, tick_size: f64) -> Result<(Option<String>, f64, f64), String> {
        let sym = format!("{}M", pair);
        let ts = now_ms().to_string(); 
        let path = "/api/v1/orders";
        let side = if is_buy { "buy" } else { "sell" };
        
        let lev_str = (leverage.max(1.0) as i64).to_string();

        let price = if is_buy { target_price * (1.0 + slippage_percent / 100.0) } else { target_price * (1.0 - slippage_percent / 100.0) };
        let rounded_price = (price / tick_size.max(1e-8)).round() * tick_size.max(1e-8);

        let mut price_str = format!("{:.8}", rounded_price).trim_end_matches('0').to_string();
        if price_str.ends_with('.') { price_str.pop(); }
        if price_str.is_empty() { price_str = "0".to_string(); }

        let body_json = if use_ioc && target_price > 0.0 {
            json!({
                "clientOid": Uuid::new_v4().to_string(),
                "symbol": sym,
                "side": side,
                "type": "limit",
                "timeInForce": "IOC",
                "price": price_str,
                "size": (phys_amount / multiplier.max(1e-12)).round() as i64,
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
                "size": (phys_amount / multiplier.max(1e-12)).round() as i64,
                "leverage": lev_str,
                "marginMode": "CROSS",
                "reduceOnly": false
            })
        };

        let body_str = body_json.to_string();

        if dry_run { return Ok((Some(format!("dry_{}_{}", self.name(), now_ms())), target_price, 0.0)); }

        let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "POST", path, &body_str);

        let resp = http.post(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig)
            .header("KC-API-TIMESTAMP", &ts)
            .header("KC-API-PASSPHRASE", pass_sig)
            .header("KC-API-KEY-VERSION", "2")
            .header("Content-Type", "application/json")
            .body(body_str)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;

        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        
        if v["code"].as_str() != Some("200000") {
            return Err(format!("KuCoin API Error: {}", v));
        }

        let order_id = v["data"]["orderId"].as_str().map(|s| s.to_string());
        if order_id.is_some() {
            Ok((order_id, 0.0, 0.0))
        } else {
            Err("KuCoin failed to return orderId".to_string())
        }
    }

    async fn close_position(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, phys_amount: f64, dry_run: bool, leverage: f64, target_price: f64, use_ioc: bool, slippage_percent: f64, multiplier: f64, tick_size: f64, retries: u32, delay_ms: u64) -> Result<CloseReceipt, String> {
        let sym = format!("{}M", pair);
        let path = "/api/v1/orders";
        let side = if is_buy { "buy" } else { "sell" };

        let lev_str = (leverage.max(1.0) as i64).to_string();
        
        let price = if is_buy { target_price * (1.0 + slippage_percent / 100.0) } else { target_price * (1.0 - slippage_percent / 100.0) };
        let rounded_price = (price / tick_size.max(1e-8)).round() * tick_size.max(1e-8);
        
        let mut price_str = format!("{:.8}", rounded_price).trim_end_matches('0').to_string();
        if price_str.ends_with('.') { price_str.pop(); }
        if price_str.is_empty() { price_str = "0".to_string(); }

        let req_size = if multiplier == 0.0 { phys_amount.round() as i64 } else { (phys_amount / multiplier.max(1e-12)).round() as i64 };
        if req_size == 0 { return Err("Amount too small to close".to_string()); }

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
                "reduceOnly": true
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
                "reduceOnly": true
            })
        };

        let body_str = body_json.to_string();

        if dry_run { return Ok(CloseReceipt { final_price: target_price, total_fee: 0.0, closed_volume: phys_amount }); }

        let ts = now_ms().to_string(); 
        let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "POST", path, &body_str);

        let resp = http.post(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig)
            .header("KC-API-TIMESTAMP", &ts)
            .header("KC-API-PASSPHRASE", pass_sig)
            .header("KC-API-KEY-VERSION", "2")
            .header("Content-Type", "application/json")
            .body(body_str)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;

        let v_resp: Value = resp.json().await.map_err(|e| e.to_string())?;
        
        if v_resp["code"].as_str() != Some("200000") {
            tracing::error!("[kucoin] close rejected: {}", v_resp);
        }

        let order_id = v_resp["data"]["orderId"].as_str().map(|s| s.to_string());
        if let Some(ref oid) = order_id {
            for _ in 0..retries {
                if let Ok((p, f, _)) = self.fetch_order(http, keys, pair, oid, multiplier).await {
                    if p > 0.0 {
                        return Ok(CloseReceipt { final_price: p, total_fee: f, closed_volume: phys_amount });
                    }
                }
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
        
        tracing::error!("[kucoin] fallback market close triggered for {} (qty {})", pair, phys_amount);
        let fb_body = json!({
            "clientOid": Uuid::new_v4().to_string(),
            "symbol": sym,
            "side": side,
            "type": "market",
            "size": req_size,
            "leverage": lev_str,
            "marginMode": "CROSS",
            "reduceOnly": true
        });
        let fb_str = fb_body.to_string();
        let ts_fb = now_ms().to_string();
        let (sig_fb, pass_sig_fb) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts_fb, "POST", path, &fb_str);
        
        let resp_fb = http.post(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig_fb)
            .header("KC-API-TIMESTAMP", &ts_fb)
            .header("KC-API-PASSPHRASE", pass_sig_fb)
            .header("KC-API-KEY-VERSION", "2")
            .header("Content-Type", "application/json")
            .body(fb_str)
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        let v_fb: Value = resp_fb.json().await.map_err(|e| e.to_string())?;
        let oid_fb = v_fb["data"]["orderId"].as_str().map(|s| s.to_string()).ok_or_else(|| "Kucoin Fallback rejected".to_string())?;
        
        for _ in 0..5 {
            if let Ok((p, f, _)) = self.fetch_order(http, keys, pair, &oid_fb, multiplier).await {
                if p > 0.0 {
                    return Ok(CloseReceipt { final_price: p, total_fee: f, closed_volume: phys_amount });
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let pos = self.fetch_positions(http, keys).await.unwrap_or_else(|_| vec![]);
        let mut std_sym = sym.replace("USDTM", "USDT");
        if std_sym == "XBTUSDT" { std_sym = "BTCUSDT".to_string(); }
        let still_has = pos.iter().any(|p| p.symbol == std_sym && p.size.abs() > 1e-12);
        if !still_has {
            Ok(CloseReceipt { final_price: target_price, total_fee: 0.0, closed_volume: phys_amount })
        } else {
            Err("Kucoin close unverified, position still active".to_string())
        }
    }

    async fn trim_position(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, phys_excess: f64, dry_run: bool, multiplier: f64, tick_size: f64, fallback_price: f64) -> Result<CloseReceipt, String> {
        // Drop excess using market order natively via close_position signature
        self.close_position(http, keys, pair, is_buy, phys_excess, dry_run, 1.0, 0.0, false, 0.0, multiplier, tick_size, 5, 20).await
    }

    async fn fetch_order(&self, http: &Client, keys: &ExchangeKeys, _pair: &str, order_id: &str, multiplier: f64) -> Result<(f64, f64, f64), String> {
        let ts = now_ms().to_string();
        let path = format!("/api/v1/orders/{}", order_id);
        let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "GET", &path, "");
        
        let resp = http.get(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig)
            .header("KC-API-TIMESTAMP", &ts)
            .header("KC-API-PASSPHRASE", pass_sig)
            .header("KC-API-KEY-VERSION", "2")
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;

        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        
        let size = v["data"]["dealSize"].as_f64().or_else(|| v["data"]["filledSize"].as_f64()).or_else(|| v["data"]["dealSize"].as_str().and_then(|s| s.parse().ok())).or_else(|| v["data"]["filledSize"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
        let val  = v["data"]["dealValue"].as_f64().or_else(|| v["data"]["filledValue"].as_f64()).or_else(|| v["data"]["dealValue"].as_str().and_then(|s| s.parse().ok())).or_else(|| v["data"]["filledValue"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
        let fee  = v["data"]["fee"].as_f64().or_else(|| v["data"]["fee"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
        
        let status = v["data"]["status"].as_str().unwrap_or("");
        if status == "done" && size <= 0.0 {
            return Ok((0.0, 0.0, 0.0));
        }
        
        // Note: KuCoin multiplier is dynamically passed to correctly compute price
        let mult = if multiplier > 0.0 { multiplier } else { 1.0 }; 
        
        let price = if size > 0.0 && mult > 0.0 { val / (size * mult) } else { 0.0 };
        
        let mut final_fee = fee;
        if price > 0.0 {
            let path_fills = format!("/api/v1/fills?orderId={}", order_id);
            let ts2 = now_ms().to_string();
            let (sig2, pass_sig2) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts2, "GET", &path_fills, "");
            if let Ok(resp2) = http.get(format!("https://api-futures.kucoin.com{}", path_fills))
                .header("KC-API-KEY", &keys.kucoin_key)
                .header("KC-API-SIGN", sig2)
                .header("KC-API-TIMESTAMP", &ts2)
                .header("KC-API-PASSPHRASE", pass_sig2)
                .header("KC-API-KEY-VERSION", "2")
                .timeout(Duration::from_secs(5))
                .send().await {
                
                if let Ok(v2) = resp2.json::<Value>().await {
                    if let Some(items) = v2["data"]["data"].as_array().or_else(|| v2["data"]["items"].as_array()) {
                        let mut sum_fee = 0.0;
                        for item in items {
                            sum_fee += item["fee"].as_f64()
                                .or_else(|| item["fee"].as_str().and_then(|s| s.parse().ok()))
                                .unwrap_or(0.0);
                        }
                        // Replace only if the fills endpoint yielded a real fee
                        if sum_fee > 1e-9 {
                            final_fee = sum_fee;
                        }
                    }
                }
            }
        }
        
        Ok((price, final_fee, size * mult))
    }

    async fn fetch_balances(&self, http: &Client, keys: &ExchangeKeys) -> Result<(f64, f64), String> {
        let ts = now_ms().to_string();
        let path = "/api/v1/account-overview?currency=USDT";
        let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "GET", path, "");
        
        let resp = http.get(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig)
            .header("KC-API-TIMESTAMP", &ts)
            .header("KC-API-PASSPHRASE", pass_sig)
            .header("KC-API-KEY-VERSION", "2")
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let avail = v["data"]["availableBalance"].as_f64()
            .or_else(|| v["data"]["availableBalance"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);
        let equity = v["data"]["accountEquity"].as_f64()
            .or_else(|| v["data"]["accountEquity"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);
            
        Ok((equity, avail))
    }

    async fn fetch_taker_fee(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        let ts = now_ms().to_string();
        let path = "/api/v1/trade-fees?symbol=XBTUSDTM";
        let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "GET", path, "");
        
        let resp = http.get(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig)
            .header("KC-API-TIMESTAMP", &ts)
            .header("KC-API-PASSPHRASE", pass_sig)
            .header("KC-API-KEY-VERSION", "2")
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let taker = v["data"]["takerFeeRate"].as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| v["data"]["takerFeeRate"].as_f64())
            .unwrap_or(0.0006);
            
        Ok(taker)
    }

    async fn ensure_leverage(&self, _http: &Client, _keys: &ExchangeKeys, _pair: &str, _leverage: f64) -> Result<(), String> {
        // KuCoin leverage is set directly in the order payload.
        Ok(())
    }

    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String> {
        let sym = format!("{}M", pair);
        let url = format!("https://api-futures.kucoin.com/api/v1/funding-rate/{}/current", sym);
        if let Ok(resp) = http.get(&url).timeout(Duration::from_secs(5)).send().await {
            if let Ok(json) = resp.json::<Value>().await {
                if let Some(rate) = json["data"]["value"].as_f64() {
                    let next = json["data"]["timePoint"].as_i64().unwrap_or(0) + 8 * 3600 * 1000;
                    return Ok((rate, next));
                }
            }
        }
        Err("KuCoin funding rate fetch failed".to_string())
    }

    async fn fetch_positions(&self, http: &Client, keys: &ExchangeKeys) -> Result<Vec<PositionInfo>, String> {
        let ts = now_ms().to_string();
        let path = "/api/v1/positions";
        let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "GET", path, "");
        let resp = http.get(format!("https://api-futures.kucoin.com{}", path))
            .header("KC-API-KEY", &keys.kucoin_key)
            .header("KC-API-SIGN", sig)
            .header("KC-API-TIMESTAMP", &ts)
            .header("KC-API-PASSPHRASE", pass_sig)
            .header("KC-API-KEY-VERSION", "2")
            .timeout(Duration::from_secs(10))
            .send().await.map_err(|e| e.to_string())?;
            
        if !resp.status().is_success() {
            let err_txt = resp.text().await.unwrap_or_default();
            return Err(format!("Kucoin position API error: {}", err_txt));
        }
            
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        
        let code = v["code"].as_str().unwrap_or("");
        if code != "200000" {
            return Err(format!("Kucoin API returned error: {}", v));
        }
        
        let mut positions = Vec::new();
        if let Some(arr) = v["data"].as_array() {
            for p in arr {
                let qty: f64 = p["currentQty"].as_f64()
                    .or_else(|| p["currentQty"].as_i64().map(|i| i as f64))
                    .unwrap_or(0.0);
                if qty.abs() < 1e-12 { continue; }
                // KuCoin symbol: "XBTUSDTM" → "BTCUSDT", "ETHUSDTM" → "ETHUSDT"
                let raw_sym = p["symbol"].as_str().unwrap_or("");
                let mut std_sym = raw_sym.replace("USDTM", "USDT").replace("USDM", "USDT");
                if std_sym == "XBTUSDT" { std_sym = "BTCUSDT".to_string(); }
                let realized = p["realisedPnl"].as_f64().or_else(|| p["realisedPnl"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
                positions.push(PositionInfo {
                    symbol: std_sym,
                    size: qty,
                    entry_price: p["avgEntryPrice"].as_f64()
                        .or_else(|| p["avgEntryPrice"].as_str().and_then(|s| s.parse().ok()))
                        .unwrap_or(0.0),
                    unrealized_pnl: p["unrealisedPnl"].as_f64()
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
}
