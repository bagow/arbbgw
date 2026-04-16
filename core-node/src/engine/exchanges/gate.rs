use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

use super::traits::{CloseReceipt, ExchangeConnector, PositionInfo};
use crate::engine::real::{gate_sign, ExchangeKeys};

pub struct GateConnector;

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct GateBalanceResponse {
    available: String,
    total: String,
}

#[derive(Deserialize, Debug)]
struct GateFeeResponse {
    taker_fee: String,
}

#[async_trait]
impl ExchangeConnector for GateConnector {
    fn name(&self) -> &'static str {
        "gate"
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
        let contract = pair.replace("USDT", "_USDT");
        let size = if is_buy {
            (phys_amount / multiplier.max(1e-12)).round() as i64
        } else {
            -((phys_amount / multiplier.max(1e-12)).round() as i64)
        };
        if size == 0 {
            return Err(format!(
                "Computed order size is 0 for amt={} mult={}",
                phys_amount, multiplier
            ));
        }
        let path = "/api/v4/futures/usdt/orders";

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

        let mut raw_price_str = format!("{:.8}", rounded_price)
            .trim_end_matches('0')
            .to_string();
        if raw_price_str.ends_with('.') {
            raw_price_str.pop();
        }
        if raw_price_str.is_empty() {
            raw_price_str = "0".to_string();
        }
        let price_str = if use_ioc && target_price > 0.0 {
            raw_price_str
        } else {
            "0".to_string()
        };
        let tif = if use_ioc && target_price > 0.0 {
            "ioc"
        } else {
            "gtc"
        };

        let body = json!({
            "contract": contract,
            "size": size,
            "price": price_str,
            "tif": tif,
            "reduce_only": false,
            "close": false,
            "text": "t-corenode"
        });
        let body_str = body.to_string();

        if dry_run {
            return Ok((Some(format!("dry_{}", Uuid::new_v4())), target_price, 0.0));
        }

        let resp = http
            .post(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(
                &keys.gate_key,
                &keys.gate_secret,
                "POST",
                path,
                &body_str,
            ))
            .body(body_str.clone())
            .timeout(Duration::from_millis(api_timeout_ms))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let status = resp.status();
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;

        // Diagnostic: log full order request and response for debugging IOC fill issues
        let resp_status = v["status"].as_str().unwrap_or("?");
        let resp_size = v["size"].as_i64().unwrap_or(0);
        let resp_left = v["left"].as_i64().unwrap_or(0);
        let resp_fill = v["fill_price"].as_str().unwrap_or("0");
        let resp_oid = v["id"]
            .as_i64()
            .map(|i| i.to_string())
            .unwrap_or_else(|| "none".to_string());
        tracing::info!("[gate] place_order {} req={} → orderId={} http={} status={} size={} left={} fill_price={}", 
            pair, body_str, resp_oid, status, resp_status, resp_size, resp_left, resp_fill);

        if !status.is_success() {
            return Err(format!("Gate API Error: {}", v));
        }

        let order_id = v["id"]
            .as_i64()
            .map(|i| i.to_string())
            .or_else(|| v["id"].as_str().map(|s| s.to_string()));
        if order_id.is_some() {
            Ok((order_id, 0.0, 0.0))
        } else {
            Err("Gate failed to return order ID".to_string())
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
        let contract = pair.replace("USDT", "_USDT");
        let size = if multiplier == 0.0 {
            if is_buy {
                phys_amount.round() as i64
            } else {
                -(phys_amount.round() as i64)
            }
        } else {
            if is_buy {
                (phys_amount / multiplier.max(1e-12)).round() as i64
            } else {
                -((phys_amount / multiplier.max(1e-12)).round() as i64)
            }
        };
        if size == 0 {
            return Err("Amount too small to close".to_string());
        }
        let path = "/api/v4/futures/usdt/orders";

        if dry_run {
            return Ok(CloseReceipt {
                final_price: target_price,
                total_fee: 0.0,
                closed_volume: phys_amount,
            });
        }

        let mut final_price = target_price;
        if final_price > 0.0 {
            final_price = if is_buy {
                target_price * (1.0 + slippage_percent / 100.0)
            } else {
                target_price * (1.0 - slippage_percent / 100.0)
            };
            final_price = if is_buy {
                (final_price / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8)
            } else {
                (final_price / tick_size.max(1e-8)).floor() * tick_size.max(1e-8)
            };
        } else {
            let ticker_url = format!(
                "https://api.gateio.ws/api/v4/futures/usdt/tickers?contract={}",
                contract
            );
            if let Ok(resp) = http.get(&ticker_url).send().await {
                if let Ok(v) = resp.json::<Vec<serde_json::Value>>().await {
                    if let Some(t) = v.first() {
                        let mark: f64 = t["mark_price"]
                            .as_str()
                            .unwrap_or("0")
                            .parse()
                            .unwrap_or(0.0);
                        if mark > 0.0 {
                            final_price = if is_buy { mark * 1.05 } else { mark * 0.95 };
                            final_price = if is_buy {
                                (final_price / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8)
                            } else {
                                (final_price / tick_size.max(1e-8)).floor() * tick_size.max(1e-8)
                            };
                        }
                    }
                }
            }
        }

        let mut raw_price_str = format!("{:.8}", final_price)
            .trim_end_matches('0')
            .to_string();
        if raw_price_str.ends_with('.') {
            raw_price_str.pop();
        }
        if raw_price_str.is_empty() {
            raw_price_str = "0".to_string();
        }

        let use_limit_ioc = final_price > 0.0;
        let price_str = if use_limit_ioc {
            raw_price_str
        } else {
            "0".to_string()
        };
        let tif = if price_str == "0" || use_limit_ioc {
            "ioc"
        } else {
            "gtc"
        };

        let is_market_close = price_str == "0";
        let final_size = if is_market_close { 0 } else { size };

        let body = json!({
            "contract": contract,
            "size": final_size,
            "price": price_str,
            "tif": tif,
            "reduce_only": true,
            "close": is_market_close,
            "text": "t-corenode-close"
        });
        let body_str = body.to_string();
        let _ = self.ensure_leverage(http, keys, pair, leverage).await;

        let resp = http
            .post(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(
                &keys.gate_key,
                &keys.gate_secret,
                "POST",
                path,
                &body_str,
            ))
            .body(body_str)
            .timeout(Duration::from_millis(api_timeout_ms))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let v_resp: Value = resp.json().await.map_err(|e| e.to_string())?;
        if let Some(msg) = v_resp["label"].as_str() {
            tracing::error!("[gate] close rejected: {}", msg);
        }

        let order_id = v_resp["id"]
            .as_i64()
            .map(|i| i.to_string())
            .or_else(|| v_resp["id"].as_str().map(|s| s.to_string()));

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
            "[gate] fallback market close triggered for {} (qty {})",
            pair,
            phys_amount
        );
        let fb_body = json!({
            "contract": contract,
            "size": size,
            "price": "0",
            "tif": "ioc",
            "reduce_only": true,
            "close": true,
            "text": "t-corenode-fb"
        });
        let fb_str = fb_body.to_string();

        if let Ok(resp_fb) = http
            .post(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(
                &keys.gate_key,
                &keys.gate_secret,
                "POST",
                path,
                &fb_str,
            ))
            .body(fb_str)
            .timeout(Duration::from_millis(api_timeout_ms))
            .send()
            .await
        {
            if let Ok(v_fb) = resp_fb.json::<Value>().await {
                let oid_fb = v_fb["id"]
                    .as_i64()
                    .map(|i| i.to_string())
                    .or_else(|| v_fb["id"].as_str().map(|s| s.to_string()));
                if let Some(fb_id) = oid_fb {
                    for _ in 0..5 {
                        if let Ok((p, f, _)) =
                            self.fetch_order(http, keys, pair, &fb_id, multiplier).await
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
                }
            }
        }

        // Deep verification fallback
        let pos = self
            .fetch_positions(http, keys)
            .await
            .unwrap_or_else(|_| vec![]);
        let still_has = pos
            .iter()
            .any(|p| p.symbol == contract.replace("_", "") && p.size.abs() > 1e-12);
        if !still_has {
            Ok(CloseReceipt {
                final_price: target_price,
                total_fee: 0.0,
                closed_volume: phys_amount,
            })
        } else {
            Err("Gate close unverified, position still active".to_string())
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
        // Just call close_position utilizing Gate's default logic (where price=0 -> deep limit fallback)
        self.close_position(
            http,
            keys,
            pair,
            is_buy,
            phys_excess,
            dry_run,
            1.0,
            0.0,
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
        multiplier: f64,
    ) -> Result<(f64, f64, f64), String> {
        let path = format!("/api/v4/futures/usdt/orders/{}", order_id);
        let resp = http
            .get(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(
                &keys.gate_key,
                &keys.gate_secret,
                "GET",
                &path,
                "",
            ))
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let price = v["fill_price"]
            .as_f64()
            .or_else(|| v["fill_price"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);
        let mut fee = v["fee"]
            .as_f64()
            .or_else(|| v["fee"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);

        let status = v["status"].as_str().unwrap_or("");
        if status == "finished" && price <= 0.0 {
            // Unfilled IOC
            return Ok((0.0, 0.0, 0.0));
        }

        let size = v["size"]
            .as_f64()
            .or_else(|| v["size"].as_i64().map(|i| i as f64))
            .unwrap_or(0.0)
            .abs();
        let left = v["left"]
            .as_f64()
            .or_else(|| v["left"].as_i64().map(|i| i as f64))
            .unwrap_or(0.0)
            .abs();
        let exec_contracts = (size - left).max(0.0);

        if exec_contracts <= 0.0 {
            if status == "finished" {
                return Ok((0.0, 0.0, 0.0));
            }
            return Err("Not filled yet".to_string());
        }

        // Fetch real commission from my_trades endpoint since orders endpoint fee is often 0 or delayed
        if exec_contracts > 0.0 {
            let contract = pair.replace("USDT", "_USDT");
            let trades_path = format!(
                "/api/v4/futures/usdt/my_trades?contract={}&order_id={}",
                contract, order_id
            );
            if let Ok(resp2) = http
                .get(format!("https://api.gateio.ws{}", trades_path))
                .headers(gate_sign(
                    &keys.gate_key,
                    &keys.gate_secret,
                    "GET",
                    &trades_path,
                    "",
                ))
                .timeout(Duration::from_secs(5))
                .send()
                .await
            {
                if let Ok(v2) = resp2.json::<Value>().await {
                    if let Some(trades) = v2.as_array() {
                        let mut sum_fee = 0.0;
                        for t in trades {
                            let f = t["fee"]
                                .as_f64()
                                .or_else(|| t["fee"].as_str().and_then(|s| s.parse().ok()))
                                .unwrap_or(0.0);
                            sum_fee += f;
                        }
                        if sum_fee.abs() > 1e-9 {
                            fee = sum_fee;
                        }
                    }
                }
            }
        }

        Ok((price, fee.abs(), exec_contracts * multiplier.max(1e-12)))
    }

    async fn fetch_balances(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<(f64, f64), String> {
        let path = "/api/v4/futures/usdt/accounts";
        let resp = http
            .get(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(
                &keys.gate_key,
                &keys.gate_secret,
                "GET",
                path,
                "",
            ))
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let data: GateBalanceResponse = resp
            .json()
            .await
            .map_err(|e| format!("Gate balance parse err: {}", e))?;
        let avail = data
            .available
            .parse::<f64>()
            .map_err(|_| "Failed to parse Gate available balance as f64".to_string())?;
        let total = data.total.parse::<f64>().unwrap_or(0.0);
        Ok((total, avail))
    }

    async fn fetch_taker_fee(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        let path = "/api/v4/futures/usdt/fee";
        let resp = http
            .get(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(
                &keys.gate_key,
                &keys.gate_secret,
                "GET",
                path,
                "",
            ))
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let data: GateFeeResponse = resp
            .json()
            .await
            .map_err(|e| format!("Gate fee parse err: {}", e))?;
        data.taker_fee
            .parse::<f64>()
            .map_err(|_| "Failed to parse Gate fee".to_string())
    }

    async fn ensure_leverage(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        leverage: f64,
    ) -> Result<(), String> {
        let contract = pair.replace("USDT", "_USDT");
        let path = format!(
            "/api/v4/futures/usdt/positions/{}?leverage={}",
            contract, leverage as i64
        );
        let _ = http
            .post(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(
                &keys.gate_key,
                &keys.gate_secret,
                "POST",
                &path,
                "",
            ))
            .timeout(Duration::from_secs(10))
            .send()
            .await;
        Ok(())
    }

    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String> {
        let contract = pair.replace("USDT", "_USDT");
        let url = format!(
            "https://api.gateio.ws/api/v4/futures/usdt/contracts/{}",
            contract
        );
        if let Ok(resp) = http.get(&url).timeout(Duration::from_secs(5)).send().await {
            if let Ok(json) = resp.json::<Value>().await {
                if let Some(rate_str) = json["funding_rate"]
                    .as_str()
                    .or_else(|| json["funding_rate_indicative"].as_str())
                {
                    if let Ok(rate) = rate_str.parse::<f64>() {
                        let next_ts = json["funding_next_apply"].as_i64().unwrap_or(0) * 1000;
                        return Ok((rate, next_ts));
                    }
                }
            }
        }
        Err("Gate funding rate fetch failed".to_string())
    }

    async fn fetch_positions(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<Vec<PositionInfo>, String> {
        let path = "/api/v4/futures/usdt/positions";
        let resp = http
            .get(format!("https://api.gateio.ws{}", path))
            .headers(gate_sign(
                &keys.gate_key,
                &keys.gate_secret,
                "GET",
                path,
                "",
            ))
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            let err_txt = resp.text().await.unwrap_or_default();
            return Err(format!("Gate position API error: {}", err_txt));
        }

        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        let mut positions = Vec::new();
        if let Some(arr) = v.as_array() {
            for p in arr {
                let size: f64 = p["size"].as_i64().unwrap_or(0) as f64;
                if size.abs() < 1e-12 {
                    continue;
                }
                // Gate symbol: "BTC_USDT" → "BTCUSDT"
                let contract = p["contract"].as_str().unwrap_or("").replace("_", "");
                let realized = p["realised_pnl"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| p["realised_pnl"].as_f64())
                    .unwrap_or(0.0);
                positions.push(PositionInfo {
                    symbol: contract,
                    size,
                    entry_price: p["entry_price"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .or_else(|| p["entry_price"].as_f64())
                        .unwrap_or(0.0),
                    unrealized_pnl: p["unrealised_pnl"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .or_else(|| p["unrealised_pnl"].as_f64())
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
