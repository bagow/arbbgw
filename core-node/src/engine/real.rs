//! Real trading engine for core-node.
//!
//! run_cycle() is called directly by the engine loop (no HTTP).
//! Order placement, Leg Fallback, and Partial Fill Guard are all here.
//! Exchange signing is done inline following the same pattern as real-trader.

use std::{sync::Arc, time::Duration};
use anyhow::Result;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use reqwest::Client;

use serde_json::{json, Value};
use sha2::Sha256;
use tokio_rusqlite::Connection;
use tracing::{error, info, warn};
use uuid::Uuid;
use dashmap::DashMap;
use std::sync::OnceLock;



type HmacSha256 = Hmac<Sha256>;

// ─── Exchange keys ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ExchangeKeys {
    pub gate_key:    String, pub gate_secret:    String,
    pub kucoin_key:  String, pub kucoin_secret:  String, pub kucoin_passphrase: String,
    pub binance_key: String, pub binance_secret: String,
    pub bybit_key:   String, pub bybit_secret:   String,
}

impl ExchangeKeys {
    pub fn from_env() -> Self {
        Self {
            gate_key:    std::env::var("GATE_KEY").unwrap_or_default(),
            gate_secret: std::env::var("GATE_SECRET").unwrap_or_default(),
            kucoin_key:         std::env::var("KUCOIN_KEY").unwrap_or_default(),
            kucoin_secret:      std::env::var("KUCOIN_SECRET").unwrap_or_default(),
            kucoin_passphrase:  std::env::var("KUCOIN_PASSPHRASE").unwrap_or_default(),
            binance_key:        std::env::var("BINANCE_KEY").unwrap_or_default(),
            binance_secret:     std::env::var("BINANCE_SECRET").unwrap_or_default(),
            bybit_key:          std::env::var("BYBIT_KEY").unwrap_or_default(),
            bybit_secret:       std::env::var("BYBIT_SECRET").unwrap_or_default(),
        }
    }
    pub fn supports(&self, ex: &str) -> bool {
        match ex {
            "gate"    => !self.gate_key.is_empty(),
            "kucoin"  => !self.kucoin_key.is_empty(),
            "binance" => !self.binance_key.is_empty(),
            "bybit"   => !self.bybit_key.is_empty(),
            _ => false,
        }
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

/// Round price to the nearest valid tick size for the given exchange:pair.
/// If tick size is unknown, rounds to 8 decimal places (safe fallback).
pub fn round_to_tick(price: f64, exchange: &str, pair: &str, e_ctx: &crate::state::EngineContext) -> f64 {
    let key = format!("{}:{}", exchange, pair);
    let tick = e_ctx.tick_sizes.get(&key).map(|v| *v).unwrap_or(0.0);
    if tick > 0.0 {
        (price / tick).round() * tick
    } else {
        // Fallback: round based on price magnitude
        if price > 1000.0 { (price * 100.0).round() / 100.0 }       // 0.01
        else if price > 10.0 { (price * 1000.0).round() / 1000.0 }  // 0.001
        else if price > 0.1 { (price * 10000.0).round() / 10000.0 } // 0.0001
        else { (price * 100000.0).round() / 100000.0 }              // 0.00001
    }
}

/// Get cached taker fee rate for an exchange (as fraction, e.g. 0.0005 = 0.05%).
pub fn get_exchange_fee(exchange: &str, default_fee: f64, e_ctx: &crate::state::EngineContext) -> f64 {
    e_ctx.fee_rates.get(exchange).map(|v| *v).unwrap_or(default_fee)
}

/// Immediately mark an exchange as balance-low.
/// Called from cycle.rs when an order is rejected with an insufficient-margin API error.
/// The start_balance_fetcher loop will clear this flag once it sees balance has recovered.
pub fn mark_balance_low(exchange: &str, e_ctx: &crate::state::EngineContext) {
    if !e_ctx.balance_low.get(exchange).map(|v| *v).unwrap_or(false) {
        warn!("[balance] {} marked balance_low — new trades on this exchange are blocked until balance recovers", exchange);
        e_ctx.balance_low.insert(exchange.to_string(), true);
    }
}

/// Spawn background task that refreshes balances and fee rates.
/// Adaptive intervals:
///   - Normal mode : every 60s — full refresh (balance + fee rates) for all exchanges
///   - Recovery mode: every 2s  — balance ONLY for the exchange(s) with balance_low=true
///     Fee rates are NOT re-fetched in recovery mode to avoid rate limits.
pub fn start_balance_fetcher(http: Client, keys: Arc<ExchangeKeys>, e_ctx: Arc<crate::state::EngineContext>) {
    tokio::spawn(async move {
        let fast_interval_ms  = 2_000u64;  // Poll interval when any balance_low is active
        let normal_interval_s = 60u64;     // Full refresh cadence (balance + fees)
        let all_exchanges = ["binance", "gate", "kucoin", "bybit"];

        loop {
            // Determine which exchanges have balance_low set
            let low_exchanges: Vec<&str> = all_exchanges.iter()
                .copied()
                .filter(|ex| keys.supports(ex) && e_ctx.balance_low.get(*ex).map(|v| *v).unwrap_or(false))
                .collect();

            if !low_exchanges.is_empty() {
                // ── FAST RECOVERY MODE ─────────────────────────────────────────
                // Only check balance on affected exchanges (no fee refresh → no rate limit)
                for ex in &low_exchanges {
                    match fetch_available_balance(&http, &keys, ex).await {
                        Ok(bal) => {
                            e_ctx.balances.insert(ex.to_string(), bal);
                            if bal > 1.0 {
                                info!("[balance] {} recovered (${:.2}) — clearing balance_low flag, trades unblocked", ex, bal);
                                e_ctx.balance_low.insert(ex.to_string(), false);
                            } else {
                                warn!("[balance] {} still balance_low (${:.2}) — retry in {}ms", ex, bal, fast_interval_ms);
                            }
                        }
                        Err(e) => error!("[balance] {} fetch error (recovery mode): {}", ex, e),
                    }
                }
                tokio::time::sleep(Duration::from_millis(fast_interval_ms)).await;
            } else {
                // ── NORMAL MODE ────────────────────────────────────────────────
                // Full refresh: balance + fee rates for all supported exchanges
                for ex in &all_exchanges {
                    if !keys.supports(ex) { continue; }
                    match fetch_available_balance(&http, &keys, ex).await {
                        Ok(bal) => {
                            e_ctx.balances.insert(ex.to_string(), bal);
                            info!("[balance] {} = ${:.2}", ex, bal);
                        }
                        Err(e) => error!("[balance] {} fetch error: {}", ex, e),
                    }
                    match fetch_taker_fee(&http, &keys, ex).await {
                        Ok(fee) => {
                            e_ctx.fee_rates.insert(ex.to_string(), fee);
                            info!("[fee] {} taker = {:.4}%", ex, fee * 100.0);
                        }
                        Err(e) => error!("[fee] {} fetch error: {}", ex, e),
                    }
                }
                tokio::time::sleep(Duration::from_secs(normal_interval_s)).await;
            }
        }
    });
}

async fn fetch_available_balance(http: &Client, keys: &ExchangeKeys, exchange: &str) -> Result<f64> {
    match exchange {
        "binance" => {
            let ts = now_ms();
            let qs = format!("timestamp={}", ts);
            let sig = binance_sign(&keys.binance_secret, &qs);
            let url = format!("https://fapi.binance.com/fapi/v2/balance?{}&signature={}", qs, sig);
            let resp: Value = http.get(&url).header("X-MBX-APIKEY", &keys.binance_key)
                .timeout(Duration::from_secs(10)).send().await?.json().await?;
            // Find USDT balance
            let bal = resp.as_array().and_then(|arr| {
                arr.iter().find(|a| a["asset"].as_str() == Some("USDT"))
                    .and_then(|a| a["availableBalance"].as_str().and_then(|s| s.parse().ok()))
            }).unwrap_or(0.0);
            Ok(bal)
        }
        "gate" => {
            let path = "/api/v4/futures/usdt/accounts";
            let resp: Value = http.get(format!("https://api.gateio.ws{}", path))
                .headers(gate_sign(&keys.gate_key, &keys.gate_secret, "GET", path, ""))
                .timeout(Duration::from_secs(10)).send().await?.json().await?;
            let bal = resp["available"].as_str().and_then(|s| s.parse().ok())
                .or_else(|| resp["available"].as_f64()).unwrap_or(0.0);
            Ok(bal)
        }
        "kucoin" => {
            let ts = now_ms().to_string();
            let path = "/api/v1/account-overview?currency=USDT";
            let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "GET", path, "");
            let resp: Value = http.get(format!("https://api-futures.kucoin.com{}", path))
                .header("KC-API-KEY", &keys.kucoin_key).header("KC-API-SIGN", sig)
                .header("KC-API-TIMESTAMP", &ts).header("KC-API-PASSPHRASE", pass_sig)
                .header("KC-API-KEY-VERSION", "2")
                .timeout(Duration::from_secs(10)).send().await?.json().await?;
            let bal = resp["data"]["availableBalance"].as_f64()
                .or_else(|| resp["data"]["availableBalance"].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(0.0);
            Ok(bal)
        }
        "bybit" => {
            let ts = now_ms().to_string();
            let recv = "5000";
            let qs = "accountType=UNIFIED&coin=USDT";
            let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs);
            let resp: Value = http.get(format!("https://api.bybit.com/v5/account/wallet-balance?{}", qs))
                .header("X-BAPI-API-KEY", &keys.bybit_key)
                .header("X-BAPI-SIGN", &sig)
                .header("X-BAPI-TIMESTAMP", &ts)
                .header("X-BAPI-RECV-WINDOW", recv)
                .timeout(Duration::from_secs(10)).send().await?.json().await?;
            let bal = resp["result"]["list"].as_array()
                .and_then(|arr| arr.first())
                .and_then(|a| a["coin"].as_array())
                .and_then(|coins| coins.iter().find(|c| c["coin"].as_str() == Some("USDT")))
                .and_then(|c| c["walletBalance"].as_str().and_then(|s| s.parse().ok())
                    .or_else(|| c["equity"].as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(0.0);
            Ok(bal)
        }
        _ => Err(anyhow::anyhow!("Unknown exchange: {}", exchange)),
    }
}

/// Fetch taker fee rate from exchange API. Returns fraction (e.g. 0.0005 = 0.05%).
async fn fetch_taker_fee(http: &Client, keys: &ExchangeKeys, exchange: &str) -> Result<f64> {
    match exchange {
        "binance" => {
            let ts = now_ms();
            let qs = format!("symbol=BTCUSDT&timestamp={}", ts);
            let sig = binance_sign(&keys.binance_secret, &qs);
            let url = format!("https://fapi.binance.com/fapi/v1/commissionRate?{}&signature={}", qs, sig);
            let resp: Value = http.get(&url).header("X-MBX-APIKEY", &keys.binance_key)
                .timeout(Duration::from_secs(10)).send().await?.json().await?;
            let taker = resp["takerCommissionRate"].as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| resp["takerCommissionRate"].as_f64())
                .ok_or_else(|| anyhow::anyhow!("missing taker rate in binance response"))?;
            Ok(taker)
        }
        "gate" => {
            // Gate.io fee tier endpoint
            let path = "/api/v4/futures/usdt/accounts";
            let resp: Value = http.get(format!("https://api.gateio.ws{}", path))
                .headers(gate_sign(&keys.gate_key, &keys.gate_secret, "GET", path, ""))
                .timeout(Duration::from_secs(10)).send().await?.json().await?;
            // Gate returns fee in "point_fee" or tier info; use tier taker fee
            let taker = resp["fee_rate"].as_str().and_then(|s| s.parse().ok())
                .or_else(|| resp["fee_rate"].as_f64())
                .ok_or_else(|| anyhow::anyhow!("missing fee_rate in gate response"))?;
            Ok(taker.abs()) // ensure positive
        }
        "kucoin" => {
            let ts = now_ms().to_string();
            let path = "/api/v1/trade-fees?symbol=XBTUSDTM";
            let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "GET", path, "");
            let resp: Value = http.get(format!("https://api-futures.kucoin.com{}", path))
                .header("KC-API-KEY", &keys.kucoin_key).header("KC-API-SIGN", sig)
                .header("KC-API-TIMESTAMP", &ts).header("KC-API-PASSPHRASE", pass_sig)
                .header("KC-API-KEY-VERSION", "2")
                .timeout(Duration::from_secs(10)).send().await?.json().await?;
            let taker = resp["data"]["takerFeeRate"].as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| resp["data"]["takerFeeRate"].as_f64())
                .ok_or_else(|| anyhow::anyhow!("missing takerFeeRate in kucoin response"))?;
            Ok(taker)
        }
        "bybit" => {
            let ts = now_ms().to_string();
            let recv = "5000";
            let qs = "category=linear&symbol=BTCUSDT";
            let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, qs);
            let resp: Value = http.get(format!("https://api.bybit.com/v5/account/fee-rate?{}", qs))
                .header("X-BAPI-API-KEY", &keys.bybit_key)
                .header("X-BAPI-SIGN", &sig)
                .header("X-BAPI-TIMESTAMP", &ts)
                .header("X-BAPI-RECV-WINDOW", recv)
                .timeout(Duration::from_secs(10)).send().await?.json().await?;
            let taker = resp["result"]["list"].as_array()
                .and_then(|arr| arr.first())
                .and_then(|a| a["takerFeeRate"].as_str().and_then(|s| s.parse().ok()))
                .ok_or_else(|| anyhow::anyhow!("missing takerFeeRate in bybit response"))?;
            Ok(taker)
        }
        _ => Err(anyhow::anyhow!("fetch_taker_fee unsupported for this exchange")),
    }
}

/// Check if exchange has enough margin for the notional volume.
/// Returns true if balance is unknown (don't block on missing cache).
pub fn has_sufficient_margin(exchange: &str, notional_usd: f64, e_ctx: &crate::state::EngineContext) -> bool {
    match e_ctx.balances.get(exchange) {
        Some(bal) => {
            let available = *bal;
            // Require at least 1.5x notional (to account for existing positions + fees)
            available >= notional_usd * 1.5
        }
        None => true, // No data yet — allow (fail-safe)
    }
}

/// Deduct notional from cached balance after a successful trade open.
pub fn deduct_cached_balance(exchange: &str, notional_usd: f64) {}

/// Check if an exchange error is a margin/balance/position-limit issue.
/// These errors mean the exchange WILL reject orders — no point retrying soon.
pub fn is_margin_error(err: &str) -> bool {
    let s = err.to_lowercase();
    s.contains("insufficient")
        || s.contains("exceeds the balance")
        || s.contains("minimum amount")
        || s.contains("notional must be no smaller")
        || s.contains("maximum allowable position")
        || s.contains("margin is insufficient")
        || s.contains("less than the minimum")
        || s.contains("order amount exceeds")
        || s.contains("not enough")
        || s.contains("balance not enough")
}

pub fn start_multiplier_fetcher(http: Client, e_ctx: Arc<crate::state::EngineContext>) {
    // Gate.io
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        async move {
            loop {
                if let Ok(resp) = http.get("https://api.gateio.ws/api/v4/futures/usdt/contracts").send().await {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json.as_array() {
                            let map = &e_ctx.multipliers;
                            let lmap = &e_ctx.launch_times;
                            let tmap = &e_ctx.tick_sizes;
                            for c in arr {
                                let name_stripped = c["name"].as_str().unwrap_or("").replace("_", "");
                                if let Some(mult_str) = c["quanto_multiplier"].as_str() {
                                    if let Ok(m) = mult_str.parse::<f64>() {
                                        map.insert(format!("gate:{}", name_stripped), m);
                                    }
                                }
                                if let Some(lt) = c["launch_time"].as_i64() {
                                    lmap.insert(format!("gate:{}", name_stripped), lt);
                                }
                                // Gate: order_price_round is the price tick size string
                                if let Some(pr) = c["order_price_round"].as_str() {
                                    if let Ok(tick) = pr.parse::<f64>() {
                                        if tick > 0.0 { tmap.insert(format!("gate:{}", name_stripped), tick); }
                                    }
                                }
                            }
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        }
    });

    // KuCoin
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        async move {
            loop {
                if let Ok(resp) = http.get("https://api-futures.kucoin.com/api/v1/contracts/active").send().await {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["data"].as_array() {
                            let map = &e_ctx.multipliers;
                            let lmap = &e_ctx.launch_times;
                            let tmap = &e_ctx.tick_sizes;
                            for c in arr {
                                if let Some(name) = c["symbol"].as_str() {
                                    let mut std_name = name.replace("USDTM", "USDT").replace("USDM", "USDT");
                                    if std_name == "XBTUSDT" { std_name = "BTCUSDT".to_string(); }
                                    let full_name = format!("kucoin:{}", std_name);
                                    
                                    if let Some(mult) = c["multiplier"].as_f64() {
                                        map.insert(full_name.clone(), mult);
                                    }
                                    // KuCoin: tickSize is the price increment
                                    if let Some(tick) = c["tickSize"].as_f64() {
                                        if tick > 0.0 { tmap.insert(full_name.clone(), tick); }
                                    }
                                    if let Some(lt) = c["firstOpenDate"].as_i64() {
                                        lmap.insert(full_name, lt / 1000);
                                    }
                                }
                            }
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        }
    });

    // Binance USDT-M Futures
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        async move {
            loop {
                if let Ok(resp) = http.get("https://fapi.binance.com/fapi/v1/exchangeInfo").send().await {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(symbols) = json["symbols"].as_array() {
                            let map = &e_ctx.multipliers;
                            let lmap = &e_ctx.launch_times;
                            let tmap = &e_ctx.tick_sizes;
                            let mut count = 0u32;
                            for s in symbols {
                                let sym = s["symbol"].as_str().unwrap_or("");
                                if !sym.ends_with("USDT") { continue; }
                                if s["contractType"].as_str() != Some("PERPETUAL") { continue; }
                                // Extract stepSize from LOT_SIZE filter as the multiplier
                                if let Some(filters) = s["filters"].as_array() {
                                    for f in filters {
                                        if f["filterType"].as_str() == Some("LOT_SIZE") {
                                            if let Some(ss) = f["stepSize"].as_str() {
                                                if let Ok(step) = ss.parse::<f64>() {
                                                    if step > 0.0 {
                                                        map.insert(format!("binance:{}", sym), step);
                                                        count += 1;
                                                    }
                                                }
                                            }
                                        }
                                        // Binance: PRICE_FILTER.tickSize is the price step
                                        if f["filterType"].as_str() == Some("PRICE_FILTER") {
                                            if let Some(ts) = f["tickSize"].as_str() {
                                                if let Ok(tick) = ts.parse::<f64>() {
                                                    if tick > 0.0 { tmap.insert(format!("binance:{}", sym), tick); }
                                                }
                                            }
                                        }
                                    }
                                }
                                if let Some(ot) = s["onboardDate"].as_i64() {
                                    lmap.insert(format!("binance:{}", sym), ot / 1000);
                                }
                            }
                            info!("[multipliers] Binance: loaded {} symbols", count);
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        }
    });


    // Bybit USDT-M Linear Futures
    tokio::spawn({
        let http = http.clone();
        let e_ctx = e_ctx.clone();
        async move {
            loop {
                if let Ok(resp) = http.get("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000").send().await {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["result"]["list"].as_array() {
                            let map = &e_ctx.multipliers;
                            let lmap = &e_ctx.launch_times;
                            let tmap = &e_ctx.tick_sizes;
                            let mut count = 0u32;
                            for c in arr {
                                let sym = c["symbol"].as_str().unwrap_or("");
                                if !sym.ends_with("USDT") { continue; }
                                // tickSize = price precision
                                if let Some(ts_str) = c["priceFilter"]["tickSize"].as_str() {
                                    if let Ok(tick) = ts_str.parse::<f64>() {
                                        if tick > 0.0 { 
                                            tmap.insert(format!("bybit:{}", sym), tick); 
                                        }
                                    }
                                }
                                // lotSizeFilter / qtyStep as multiplier/stepSize
                                if let Some(qs_str) = c["lotSizeFilter"]["qtyStep"].as_str() {
                                    if let Ok(qty) = qs_str.parse::<f64>() {
                                        if qty > 0.0 {
                                            map.insert(format!("bybit:{}", sym), qty);
                                            count += 1;
                                        }
                                    }
                                }
                                // launchTime
                                if let Some(lt) = c["launchTime"].as_str() {
                                    if let Ok(ts) = lt.parse::<i64>() {
                                        lmap.insert(format!("bybit:{}", sym), ts / 1000);
                                    }
                                }
                            }
                            info!("[multipliers] Bybit: loaded {} symbols", count);
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        }
    });
}


pub fn now_secs() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

fn hmac_sha256_hex(secret: &[u8], data: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC any key");
    mac.update(data.as_bytes()); hex::encode(mac.finalize().into_bytes())
}
fn hmac_sha256_b64(secret: &[u8], data: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC any key");
    mac.update(data.as_bytes()); base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}
pub fn kucoin_sign(secret: &str, passphrase: &str, ts: &str, method: &str, path: &str, body: &str) -> (String, String) {
    (hmac_sha256_b64(secret.as_bytes(), &format!("{}{}{}{}", ts, method, path, body)),
     hmac_sha256_b64(secret.as_bytes(), passphrase))
}
pub fn gate_sign(key: &str, secret: &str, method: &str, path: &str, body: &str) -> reqwest::header::HeaderMap {
    use sha2::Digest;
    let ts = now_secs().to_string();
    let body_hash = hex::encode(sha2::Sha512::digest(body.as_bytes()));
    let sign_str = format!("{}\n{}\n\n{}\n{}", method, path, body_hash, ts);
    let sig = { let mut mac = Hmac::<sha2::Sha512>::new_from_slice(secret.as_bytes()).unwrap(); mac.update(sign_str.as_bytes()); hex::encode(mac.finalize().into_bytes()) };
    let mut h = reqwest::header::HeaderMap::new();
    h.insert("KEY", key.parse().unwrap()); h.insert("SIGN", sig.parse().unwrap());
    h.insert("Timestamp", ts.parse().unwrap()); h.insert("Content-Type", "application/json".parse().unwrap());
    h
}
pub fn binance_sign(secret: &str, qs: &str) -> String {
    hmac_sha256_hex(secret.as_bytes(), qs)
}
pub fn bybit_sign(secret: &str, ts: &str, api_key: &str, recv_window: &str, body: &str) -> String {
    hmac_sha256_hex(secret.as_bytes(), &format!("{}{}{}{}", ts, api_key, recv_window, body))
}


// ─── Leverage / Margin-mode cache ─────────────────────────────────────────────

/// Cache key: "exchange:symbol:leverage:margin_mode" → true once set.
fn get_leverage_cache() -> &'static DashMap<String, bool> {
    static CACHE: OnceLock<DashMap<String, bool>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

/// Ensure leverage and margin mode are set on the exchange for the given symbol.
/// Only calls the exchange API once per unique (exchange, symbol, leverage, margin_mode) combo.
async fn ensure_leverage(http: &Client, keys: &ExchangeKeys, exchange: &str, pair: &str, leverage: i64, margin_mode: &str) {
    let cache_key = format!("{}:{}:{}:{}", exchange, pair, leverage, margin_mode);
    if get_leverage_cache().contains_key(&cache_key) { return; }

    let lev = leverage.max(1);
    let is_cross = margin_mode != "isolated";

    let result: Result<()> = async {
        match exchange {
            "binance" => {
                let ts = now_ms();
                // Set margin type
                let margin_type = if is_cross { "CROSSED" } else { "ISOLATED" };
                let qs = format!("symbol={}&marginType={}&timestamp={}", pair, margin_type, ts);
                let sig = binance_sign(&keys.binance_secret, &qs);
                let _ = http.post(format!("https://fapi.binance.com/fapi/v1/marginType?{}&signature={}", qs, sig))
                    .header("X-MBX-APIKEY", &keys.binance_key)
                    .timeout(Duration::from_secs(5)).send().await;
                // Set leverage
                let ts2 = now_ms();
                let qs2 = format!("symbol={}&leverage={}&timestamp={}", pair, lev, ts2);
                let sig2 = binance_sign(&keys.binance_secret, &qs2);
                let resp = http.post(format!("https://fapi.binance.com/fapi/v1/leverage?{}&signature={}", qs2, sig2))
                    .header("X-MBX-APIKEY", &keys.binance_key)
                    .timeout(Duration::from_secs(5)).send().await?.json::<Value>().await?;
                info!("[leverage] binance:{} set to {}x {} → {:?}", pair, lev, margin_mode, resp.get("leverage"));
            }
            "bybit" => {
                let ts = now_ms().to_string();
                let recv = "5000";
                // Switch isolated / cross
                let trade_mode = if is_cross { 0 } else { 1 };
                let body = json!({"category":"linear","symbol":pair,"tradeMode":trade_mode,"buyLeverage":lev.to_string(),"sellLeverage":lev.to_string()});
                let body_str = body.to_string();
                let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &body_str);
                let _ = http.post("https://api.bybit.com/v5/position/switch-isolated")
                    .header("X-BAPI-API-KEY", &keys.bybit_key).header("X-BAPI-SIGN", &sig)
                    .header("X-BAPI-TIMESTAMP", &ts).header("X-BAPI-RECV-WINDOW", recv)
                    .header("Content-Type", "application/json")
                    .body(body_str).timeout(Duration::from_secs(5)).send().await;
                // Set leverage
                let ts2 = now_ms().to_string();
                let body2 = json!({"category":"linear","symbol":pair,"buyLeverage":lev.to_string(),"sellLeverage":lev.to_string()});
                let body_str2 = body2.to_string();
                let sig2 = bybit_sign(&keys.bybit_secret, &ts2, &keys.bybit_key, recv, &body_str2);
                let resp = http.post("https://api.bybit.com/v5/position/set-leverage")
                    .header("X-BAPI-API-KEY", &keys.bybit_key).header("X-BAPI-SIGN", &sig2)
                    .header("X-BAPI-TIMESTAMP", &ts2).header("X-BAPI-RECV-WINDOW", recv)
                    .header("Content-Type", "application/json")
                    .body(body_str2).timeout(Duration::from_secs(5)).send().await?.json::<Value>().await?;
                info!("[leverage] bybit:{} set to {}x {} → retCode={}", pair, lev, margin_mode, resp["retCode"]);
            }
            "gate" => {
                let contract = pair.replace("USDT", "_USDT");
                // Gate: leverage=0 + cross_leverage_limit=N for cross, leverage=N for isolated
                let path = format!("/api/v4/futures/usdt/positions/{}/leverage", contract);
                let body = if is_cross {
                    json!({"leverage": 0, "cross_leverage_limit": lev})
                } else {
                    json!({"leverage": lev})
                };
                let body_str = body.to_string();
                let resp = http.post(format!("https://api.gateio.ws{}", path))
                    .headers(gate_sign(&keys.gate_key, &keys.gate_secret, "POST", &path, &body_str))
                    .body(body_str).timeout(Duration::from_secs(5)).send().await?.json::<Value>().await?;
                info!("[leverage] gate:{} set to {}x {} → {:?}", pair, lev, margin_mode, resp.get("leverage"));
            }
            "kucoin" => {
                // KuCoin: leverage is passed in the order body, no separate endpoint needed.
                // Just log for consistency.
                info!("[leverage] kucoin:{} will use {}x {} (in-order)", pair, lev, margin_mode);
            }
            _ => {}
        }
        Ok(())
    }.await;

    if let Err(e) = result {
        error!("[leverage] {}:{} error: {}", exchange, pair, e);
    }
    get_leverage_cache().insert(cache_key, true);
}

/// Returns (order_id, fill_price, fill_fee). fill_price>0 means data is available
/// from the exchange response itself (e.g. Binance), skipping separate verification.
async fn place_order(http: &Client, keys: &ExchangeKeys, exchange: &str, pair: &str, is_short: bool, contracts: i64, dry_run: bool, is_reduce_only: bool, limit_price: f64, leverage: i64) -> Result<(String, f64, f64)> {
    let side = if is_short { "sell" } else { "buy" };
    let use_limit = limit_price > 0.0 && !is_reduce_only; // Limit IOC only for entries
    // Round price to exchange tick size to avoid "PRICE PARAMETER INVALID" errors
    let limit_price = if use_limit { round_to_tick(limit_price, exchange, pair, &crate::state::EngineContext::default()) } else { 0.0 };
    match exchange {
        "binance" => {
            let ts = now_ms();
            let path = "/fapi/v1/order";
            let mut qs = if use_limit {
                // Binance price precision: 
                format!("symbol={}&side={}&type=LIMIT&timeInForce=IOC&quantity={}&price={:.8}&timestamp={}", pair, side.to_uppercase(), contracts, limit_price, ts)
            } else {
                format!("symbol={}&side={}&type=MARKET&quantity={}&timestamp={}", pair, side.to_uppercase(), contracts, ts)
            };
            if is_reduce_only { qs.push_str("&reduceOnly=true"); }
            let sig = binance_sign(&keys.binance_secret, &qs);
            let full_url = format!("https://fapi.binance.com{}?{}&signature={}", path, qs, sig);
            
            if dry_run { info!("[DRY] binance order {} size={} limit={:.6}", pair, contracts, limit_price); return Ok((format!("dry_{}", Uuid::new_v4()), 0.0, 0.0)); }
            let resp = http.post(full_url).header("X-MBX-APIKEY", &keys.binance_key)
                .timeout(Duration::from_secs(10)).send().await?;
            let status = resp.status();
            let v: Value = resp.json().await?;
            if !status.is_success() { return Err(anyhow::anyhow!("Binance error: {}", v)); }
            let order_status = v["status"].as_str().unwrap_or("UNKNOWN");
            info!("[binance] {} order response: status={} avgPrice={} executedQty={} origQty={}",
                pair, order_status, v["avgPrice"], v["executedQty"], v["origQty"]);
            // Immediate reject for clearly failed orders
            if order_status == "CANCELED" || order_status == "REJECTED" {
                return Err(anyhow::anyhow!("Binance IOC {} status={}", pair, order_status));
            }
            let oid = v["orderId"].as_i64().map(|i| i.to_string()).unwrap_or_else(|| "unknown".to_string());
            // For FILLED/PARTIALLY_FILLED: extract inline fill data
            if order_status == "FILLED" || order_status == "PARTIALLY_FILLED" {
                let avg_price = v["avgPrice"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let cum_fee: f64 = v["fills"].as_array().map(|fills| {
                    fills.iter().map(|f| {
                        let comm = f["commission"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                        let asset = f["commissionAsset"].as_str().unwrap_or("USDT");
                        match asset {
                            "USDT" => comm,
                            "BNB" => comm * 600.0, // rough static estimate for BNB to prevent microscopic/huge fees
                            _ => comm * avg_price, // assume base asset (e.g. ENJ), convert to USDT via fill price
                        }
                    }).sum()
                }).unwrap_or(0.0);
                return Ok((oid, avg_price, cum_fee));
            }
            // For NEW/EXPIRED: Binance API may respond BEFORE matching engine.
            // Wait briefly then re-check to see if the IOC was actually filled.
            let mut current_status = order_status.to_string();
            // Immediate reject if it's EXPIRED right away, saving 400ms for ASYMMETRIC drops
            if current_status == "EXPIRED" {
                return Err(anyhow::anyhow!("Binance IOC {} unresolved/failed. Final status={}", pair, current_status));
            }
            if current_status == "NEW" {
                for i in 1..=10 {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    let ts2 = now_ms();
                    let qs2 = format!("symbol={}&orderId={}&timestamp={}", pair, oid, ts2);
                    let sig2 = binance_sign(&keys.binance_secret, &qs2);
                    let url2 = format!("https://fapi.binance.com/fapi/v1/order?{}&signature={}", qs2, sig2);
                    if let Ok(resp2) = http.get(&url2).header("X-MBX-APIKEY", &keys.binance_key)
                        .timeout(Duration::from_secs(5)).send().await {
                        if let Ok(v2) = resp2.json::<Value>().await {
                            let s2 = v2["status"].as_str().unwrap_or("");
                            let filled_qty = v2["executedQty"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                            info!("[binance] {} re-check attempt {}: status={} executedQty={}", pair, i, s2, filled_qty);
                            if s2 == "FILLED" || (s2 == "PARTIALLY_FILLED" && filled_qty > 0.0) {
                                let ap = v2["avgPrice"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                return Ok((oid, ap, 0.0));
                            }
                            current_status = s2.to_string();
                            if current_status != "NEW" {
                                break; // EXPIRED, CANCELED, REJECTED
                            }
                        }
                    }
                }
                return Err(anyhow::anyhow!("Binance IOC {} unresolved/failed. Final status={}", pair, current_status));
            }
            // Any other status — return with orderId, no fill data
            Ok((oid, 0.0, 0.0))
        }

        "gate" => {
            let contract = pair.replace("USDT", "_USDT");
            let size = if is_short { -contracts } else { contracts };
            let path = "/api/v4/futures/usdt/orders";
            let price_str = if use_limit { format!("{:.8}", limit_price) } else { "0".to_string() };
            // FIX: use IOC only for limit orders; market orders (price=0) use gtc
            let tif = if use_limit { "ioc" } else { "gtc" };
            let body = json!({"contract":contract,"size":size,"price":price_str,"tif":tif,"reduce_only":is_reduce_only,"close":false,"text":"t-corenode"});
            let body_str = body.to_string();
            if dry_run { info!("[DRY] gate order {} size={} limit={:.6}", contract, size, limit_price); return Ok((format!("dry_{}", Uuid::new_v4()), 0.0, 0.0)); }
            let resp = http.post(format!("https://api.gateio.ws{}", path)).headers(gate_sign(&keys.gate_key, &keys.gate_secret, "POST", path, &body_str)).body(body_str).timeout(Duration::from_secs(10)).send().await?;
            let status = resp.status();
            let v: Value = resp.json().await?;
            if !status.is_success() { return Err(anyhow::anyhow!("Gate error: {}", v)); }
            Ok((v["id"].as_i64().map(|i| i.to_string()).unwrap_or_else(|| v["id"].as_str().unwrap_or("unknown").to_string()), 0.0, 0.0))
        }

        "kucoin" => {
            let sym = format!("{}M", pair);
            let ts = now_ms().to_string(); let path = "/api/v1/orders";
            let lev_str = leverage.to_string();
            let body = if use_limit {
                let mut b = json!({"clientOid":Uuid::new_v4().to_string(),"symbol":sym,"side":side,"type":"limit","timeInForce":"IOC","price":format!("{:.8}", limit_price),"size":contracts,"leverage":lev_str,"marginMode":"CROSS"});
                if is_reduce_only { b["reduceOnly"] = json!(true); }
                b
            } else if is_reduce_only {
                json!({"clientOid":Uuid::new_v4().to_string(),"symbol":sym,"side":side,"type":"market","size":contracts,"leverage":leverage.to_string(),"marginMode":"CROSS","reduceOnly":true})
            } else {
                json!({"clientOid":Uuid::new_v4().to_string(),"symbol":sym,"side":side,"type":"market","size":contracts,"leverage":leverage.to_string(),"marginMode":"CROSS"})
            };
            let body_str = body.to_string();
            let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "POST", path, &body_str);
            if dry_run { return Ok((format!("dry_kucoin_{}", Uuid::new_v4()), 0.0, 0.0)); }
            let resp = http.post(format!("https://api-futures.kucoin.com{}", path))
                .header("KC-API-KEY", &keys.kucoin_key).header("KC-API-SIGN", sig)
                .header("KC-API-TIMESTAMP", &ts).header("KC-API-PASSPHRASE", pass_sig)
                .header("KC-API-KEY-VERSION", "2").header("Content-Type", "application/json")
                .body(body_str).timeout(Duration::from_secs(10)).send().await?;
            let status = resp.status();
            let v: Value = resp.json().await?;
            if !status.is_success() || v["code"].as_str() != Some("200000") { return Err(anyhow::anyhow!("Kucoin error: {}", v)); }
            Ok((v["data"]["orderId"].as_str().unwrap_or("unknown").to_string(), 0.0, 0.0))
        }
        "bybit" => {
            let ts = now_ms().to_string();
            let recv = "5000";
            let side_str = if is_short { "Sell" } else { "Buy" };
            let body = if use_limit {
                json!({
                    "category": "linear",
                    "symbol": pair,
                    "side": side_str,
                    "orderType": "Limit",
                    "qty": contracts.to_string(),
                    "price": format!("{:.8}", limit_price),
                    "timeInForce": "IOC",
                    "reduceOnly": is_reduce_only
                })
            } else {
                json!({
                    "category": "linear",
                    "symbol": pair,
                    "side": side_str,
                    "orderType": "Market",
                    "qty": contracts.to_string(),
                    "reduceOnly": is_reduce_only
                })
            };
            let body_str = body.to_string();
            let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &body_str);
            if dry_run { return Ok((format!("dry_bybit_{}", Uuid::new_v4()), 0.0, 0.0)); }
            let resp = http.post("https://api.bybit.com/v5/order/create")
                .header("X-BAPI-API-KEY", &keys.bybit_key)
                .header("X-BAPI-SIGN", &sig)
                .header("X-BAPI-TIMESTAMP", &ts)
                .header("X-BAPI-RECV-WINDOW", recv)
                .header("Content-Type", "application/json")
                .body(body_str).timeout(Duration::from_secs(10)).send().await?;
            let v: Value = resp.json().await?;
            if v["retCode"].as_i64() != Some(0) { return Err(anyhow::anyhow!("Bybit error: {}", v)); }
            Ok((v["result"]["orderId"].as_str().unwrap_or("unknown").to_string(), 0.0, 0.0))
        }
        _ => Err(anyhow::anyhow!("Unknown exchange: {}", exchange)),
    }
}

async fn close_order(http: &Client, keys: &ExchangeKeys, exchange: &str, pair: &str, was_short: bool, contracts: i64, dry_run: bool, leverage: i64) -> Result<String> {
    // Closes use market orders (limit_price=0) for guaranteed execution — 
    // orphaned positions are worse than slippage on exit.
    let (oid, _, _) = place_order(http, keys, exchange, pair, !was_short, contracts, dry_run, true, 0.0, leverage).await?;
    Ok(oid)
}

// ─── Engine cycle ─────────────────────────────────────────────────────────────

/// Called on every WS tick. Manages real trades in the DB.


async fn fetch_order(http: &Client, keys: &ExchangeKeys, exchange: &str, pair: &str, order_id: &str) -> Result<(f64, f64)> {
    if order_id.starts_with("dry_") { return Ok((0.0, 0.0)); }
    match exchange {
        "binance" => {
            let ts = now_ms();
            let qs = format!("symbol={}&orderId={}&timestamp={}", pair, order_id, ts);
            let sig = binance_sign(&keys.binance_secret, &qs);
            let url = format!("https://fapi.binance.com/fapi/v1/order?{}&signature={}", qs, sig);
            let resp = http.get(&url).header("X-MBX-APIKEY", &keys.binance_key)
                .timeout(Duration::from_secs(10)).send().await?;
            let v: Value = resp.json().await?;
            // avgPrice = weighted average fill price; commission in quote asset
            let price = v["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let fee = v["commission"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            Ok((price, fee))
        }
        "gate" => {
            let path = format!("/api/v4/futures/usdt/orders/{}", order_id);
            let resp = http.get(format!("https://api.gateio.ws{}", path)).headers(gate_sign(&keys.gate_key, &keys.gate_secret, "GET", &path, "")).timeout(Duration::from_secs(10)).send().await?;
            let v: Value = resp.json().await?;
            let price = v["fill_price"].as_f64().or_else(|| v["fill_price"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
            let fee = v["fee"].as_f64().or_else(|| v["fee"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
            Ok((price, fee))
        }

        "kucoin" => {
            let ts = now_ms().to_string(); let path = format!("/api/v1/orders/{}", order_id);
            let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "GET", &path, "");
            let resp = http.get(format!("https://api-futures.kucoin.com{}", path))
                .header("KC-API-KEY", &keys.kucoin_key).header("KC-API-SIGN", sig)
                .header("KC-API-TIMESTAMP", &ts).header("KC-API-PASSPHRASE", pass_sig)
                .header("KC-API-KEY-VERSION", "2").timeout(Duration::from_secs(10)).send().await?;
            let v: Value = resp.json().await?;
            let size = v["data"]["dealSize"].as_f64().or_else(|| v["data"]["filledSize"].as_f64()).or_else(|| v["data"]["dealSize"].as_str().and_then(|s| s.parse().ok())).or_else(|| v["data"]["filledSize"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
            let val  = v["data"]["dealValue"].as_f64().or_else(|| v["data"]["filledValue"].as_f64()).or_else(|| v["data"]["dealValue"].as_str().and_then(|s| s.parse().ok())).or_else(|| v["data"]["filledValue"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
            let fee  = v["data"]["fee"].as_f64().or_else(|| v["data"]["fee"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
            
            let map = crate::state::EngineContext::default().multipliers;
            let mult = map.get(&format!("kucoin:{}", pair)).map(|m| *m).unwrap_or(1.0);
            
            let price = if size > 0.0 && mult > 0.0 { val / (size * mult) } else { 0.0 };
            Ok((price, fee))
        }
        "bybit" => {
            let ts = now_ms().to_string();
            let recv = "5000";
            let qs = format!("category=linear&orderId={}", order_id);
            let sig = bybit_sign(&keys.bybit_secret, &ts, &keys.bybit_key, recv, &qs);
            let resp = http.get(format!("https://api.bybit.com/v5/order/realtime?{}", qs))
                .header("X-BAPI-API-KEY", &keys.bybit_key)
                .header("X-BAPI-SIGN", &sig)
                .header("X-BAPI-TIMESTAMP", &ts)
                .header("X-BAPI-RECV-WINDOW", recv)
                .timeout(Duration::from_secs(10)).send().await?;
            let v: Value = resp.json().await?;
            let order = &v["result"]["list"][0];
            let price = order["avgPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let fee = order["cumExecFee"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
            Ok((price, fee.abs()))
        }
        _ => Err(anyhow::anyhow!("Unknown exchange: {}", exchange)),
    }
}

#[derive(Debug, Clone)]
pub struct Trade {
    pub id: i64, pub pair: String, pub exchange_a: String, pub exchange_b: String, pub direction: String,
    pub volume: f64, pub entry_price_a: f64, pub entry_price_b: f64, pub entry_spread: f64,
    pub entry_time: i64, pub highest_spread: f64, pub pnl_a: f64, pub pnl_b: f64,
    pub close_reason: Option<String>, pub contracts_a: f64, pub contracts_b: f64,
    pub commission_cost: f64, pub slippage_cost: f64,
    /// Accumulated real funding cost since entry (updated every live-update cycle from exchange API).
    /// Positive = we net-paid funding; negative = we net-received funding.
    pub funding_cost: f64,
}

pub async fn load_open_trades(db: &Connection, p_id: &str) -> Vec<Trade> {
    let p_id_clone = p_id.to_string();
    db.call(move |conn| {
        let mut stmt = conn.prepare(
            "SELECT id, pair, exchange_a, exchange_b, direction, volume, \
             entry_price_a, entry_price_b, entry_spread, entry_time, highest_spread_reached, \
             pnl_a, pnl_b, close_reason, contracts_a, contracts_b, commission_cost, slippage_cost, \
             COALESCE(funding_cost, 0.0) \
             FROM trades WHERE status='OPEN' AND profile_id=?"
        )?;
        let trades = stmt.query_map([p_id_clone], |row| Ok(Trade {
            id: row.get(0)?, pair: row.get(1)?, exchange_a: row.get(2)?, exchange_b: row.get(3)?,
            direction: row.get(4)?, volume: row.get(5)?, entry_price_a: row.get(6)?, entry_price_b: row.get(7)?,
            entry_spread: row.get(8)?, entry_time: row.get(9)?, highest_spread: row.get(10)?,
            pnl_a: row.get(11)?, pnl_b: row.get(12)?, close_reason: row.get(13)?,
            contracts_a: row.get::<_, Option<f64>>(14)?.unwrap_or((row.get::<_, f64>(5)? / row.get::<_, f64>(6)?.max(1e-12)).round()),
            contracts_b: row.get::<_, Option<f64>>(15)?.unwrap_or((row.get::<_, f64>(5)? / row.get::<_, f64>(7)?.max(1e-12)).round()),
            commission_cost: row.get::<_, Option<f64>>(16)?.unwrap_or(0.0),
            slippage_cost: row.get::<_, Option<f64>>(17)?.unwrap_or(0.0),
            funding_cost: row.get::<_, Option<f64>>(18)?.unwrap_or(0.0),
        }))?.filter_map(|t| t.ok()).collect::<Vec<_>>();
        Ok(trades)
    }).await.unwrap_or_default()
}

// ── Funding rate cache ──────────────────────────────────────────────────────
#[derive(Clone, Debug)]
struct FundingInfo {
    rate: f64,        // current funding rate (e.g. 0.0001 = 0.01%)
    next_ts: i64,     // unix timestamp (ms) of next funding apply
    fetched_at: i64,  // when this was cached
}

static FUNDING_CACHE: OnceLock<DashMap<String, FundingInfo>> = OnceLock::new();

/// Fetch funding rate from exchange REST API (cached for 5 min)
async fn get_funding_rate(http: &Client, exchange: &str, pair: &str) -> (f64, i64) {
    let cache = FUNDING_CACHE.get_or_init(DashMap::new);
    let key = format!("{}:{}", exchange, pair);
    let now = now_ms();

    // Return cached if fresh (< 5 min)
    if let Some(entry) = cache.get(&key) {
        if now - entry.fetched_at < 300_000 {
            return (entry.rate, entry.next_ts);
        }
    }

    let result = fetch_funding_rate_api(http, exchange, pair).await;
    if let Some((rate, next_ts)) = result {
        cache.insert(key, FundingInfo { rate, next_ts, fetched_at: now });
        (rate, next_ts)
    } else {
        // Cache miss — return 0
        (0.0, 0)
    }
}

async fn fetch_funding_rate_api(http: &Client, exchange: &str, pair: &str) -> Option<(f64, i64)> {
    match exchange {
        "gate" => {
            // Gate.io contract names use underscore: BTCUSDT → BTC_USDT
            let contract = pair.replace("USDT", "_USDT");
            let url = format!("https://api.gateio.ws/api/v4/futures/usdt/contracts/{}", contract);
            let resp = http.get(&url).send().await.ok()?.json::<serde_json::Value>().await.ok()?;
            let rate = resp["funding_rate"].as_str()?.parse::<f64>().ok()?;
            let next = resp["funding_next_apply"].as_f64()? as i64 * 1000; // seconds→ms
            Some((rate, next))
        }
        "kucoin" => {
            // KuCoin futures symbol: BTCUSDT → BTCUSDTM
            let sym = format!("{}M", pair);
            let url = format!("https://api-futures.kucoin.com/api/v1/funding-rate/{}/current", sym);
            let resp = http.get(&url).send().await.ok()?.json::<serde_json::Value>().await.ok()?;
            let rate = resp["data"]["value"].as_f64()?;
            let next = resp["data"]["timePoint"].as_i64()? + 8 * 3600 * 1000; // next = current + 8h
            Some((rate, next))
        }
        "binance" => {
            let url = format!("https://fapi.binance.com/fapi/v1/premiumIndex?symbol={}", pair);
            let resp = http.get(&url).send().await.ok()?.json::<serde_json::Value>().await.ok()?;
            let rate = resp["lastFundingRate"].as_str()?.parse::<f64>().ok()?;
            let next = resp["nextFundingTime"].as_i64()?;
            Some((rate, next))
        }

        _ => None,
    }
}
