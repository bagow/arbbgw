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
use rusqlite::params;
use serde_json::{json, Value};
use sha2::Sha256;
use tokio_rusqlite::Connection;
use tracing::{error, info};
use uuid::Uuid;
use dashmap::DashMap;
use std::sync::OnceLock;

use shared::{TradingSettings, SpreadRow};

type HmacSha256 = Hmac<Sha256>;

// ─── Exchange keys ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ExchangeKeys {
    pub gate_key:    String, pub gate_secret:    String,
    pub htx_key:     String, pub htx_secret:     String,
    pub kucoin_key:  String, pub kucoin_secret:  String, pub kucoin_passphrase: String,
    pub binance_key: String, pub binance_secret: String,
    pub bitget_key:  String, pub bitget_secret:  String, pub bitget_passphrase: String,
}

impl ExchangeKeys {
    pub fn from_env() -> Self {
        Self {
            gate_key:    std::env::var("GATE_KEY").unwrap_or_default(),
            gate_secret: std::env::var("GATE_SECRET").unwrap_or_default(),
            htx_key:     std::env::var("HTX_KEY").unwrap_or_default(),
            htx_secret:  std::env::var("HTX_SECRET").unwrap_or_default(),
            kucoin_key:         std::env::var("KUCOIN_KEY").unwrap_or_default(),
            kucoin_secret:      std::env::var("KUCOIN_SECRET").unwrap_or_default(),
            kucoin_passphrase:  std::env::var("KUCOIN_PASSPHRASE").unwrap_or_default(),
            binance_key:        std::env::var("BINANCE_KEY").unwrap_or_default(),
            binance_secret:     std::env::var("BINANCE_SECRET").unwrap_or_default(),
            bitget_key:         std::env::var("BITGET_KEY").unwrap_or_default(),
            bitget_secret:      std::env::var("BITGET_SECRET").unwrap_or_default(),
            bitget_passphrase:  std::env::var("BITGET_PASSPHRASE").unwrap_or_default(),
        }
    }
    pub fn supports(&self, ex: &str) -> bool {
        match ex {
            "gate"    => !self.gate_key.is_empty(),
            "htx"     => !self.htx_key.is_empty(),
            "kucoin"  => !self.kucoin_key.is_empty(),
            "binance" => !self.binance_key.is_empty(),
            "bitget"  => !self.bitget_key.is_empty(),
            _ => false,
        }
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

static MULTIPLIERS: OnceLock<DashMap<String, f64>> = OnceLock::new();
static LAUNCH_TIMES: OnceLock<DashMap<String, i64>> = OnceLock::new();
static TICK_SIZES: OnceLock<DashMap<String, f64>> = OnceLock::new();

pub fn get_multipliers() -> &'static DashMap<String, f64> {
    MULTIPLIERS.get_or_init(DashMap::new)
}

pub fn get_launch_times() -> &'static DashMap<String, i64> {
    LAUNCH_TIMES.get_or_init(DashMap::new)
}

pub fn get_tick_sizes() -> &'static DashMap<String, f64> {
    TICK_SIZES.get_or_init(DashMap::new)
}

/// Round price to the nearest valid tick size for the given exchange:pair.
/// If tick size is unknown, rounds to 8 decimal places (safe fallback).
fn round_to_tick(price: f64, exchange: &str, pair: &str) -> f64 {
    let key = format!("{}:{}", exchange, pair);
    let tick = get_tick_sizes().get(&key).map(|v| *v).unwrap_or(0.0);
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

// ─── Balance cache ────────────────────────────────────────────────────────────

static BALANCE_CACHE: OnceLock<DashMap<String, f64>> = OnceLock::new();
static FEE_RATE_CACHE: OnceLock<DashMap<String, f64>> = OnceLock::new();

fn get_balance_cache() -> &'static DashMap<String, f64> {
    BALANCE_CACHE.get_or_init(DashMap::new)
}

fn get_fee_cache() -> &'static DashMap<String, f64> {
    FEE_RATE_CACHE.get_or_init(DashMap::new)
}

/// Get cached taker fee rate for an exchange (as fraction, e.g. 0.0005 = 0.05%).
/// Falls back to 0.05% if not yet fetched.
pub fn get_exchange_fee(exchange: &str) -> f64 {
    get_fee_cache().get(exchange).map(|v| *v).unwrap_or(0.0005) // default 0.05%
}

/// Spawn background task that refreshes balances and fee rates every 30s.
pub fn start_balance_fetcher(http: Client, keys: Arc<ExchangeKeys>) {
    tokio::spawn(async move {
        loop {
            let bal_cache = get_balance_cache();
            let fee_cache = get_fee_cache();
            let exchanges = ["binance", "gate", "htx", "kucoin", "bitget"];
            for ex in &exchanges {
                if !keys.supports(ex) { continue; }
                match fetch_available_balance(&http, &keys, ex).await {
                    Ok(bal) => {
                        bal_cache.insert(ex.to_string(), bal);
                        info!("[balance] {} = ${:.2}", ex, bal);
                    }
                    Err(e) => error!("[balance] {} fetch error: {}", ex, e),
                }
                match fetch_taker_fee(&http, &keys, ex).await {
                    Ok(fee) => {
                        fee_cache.insert(ex.to_string(), fee);
                        info!("[fee] {} taker = {:.4}%", ex, fee * 100.0);
                    }
                    Err(e) => error!("[fee] {} fetch error: {}", ex, e),
                }
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
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
        "htx" => {
            let host = "api.hbdm.com";
            let path = "/linear-swap-api/v1/swap_account_info";
            let body = json!({"margin_account": "USDT"});
            let resp: Value = http.post(format!("https://{}{}", host, htx_sign(&keys.htx_key, &keys.htx_secret, "POST", host, path)))
                .json(&body).timeout(Duration::from_secs(10)).send().await?.json().await?;
            let bal = resp["data"].as_array()
                .and_then(|arr| arr.first())
                .and_then(|a| a["withdraw_available"].as_f64())
                .unwrap_or(0.0);
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
        "bitget" => {
            let ts = now_ms().to_string();
            let path = "/api/v2/mix/account/account?symbol=BTCUSDT&productType=USDT-FUTURES&marginCoin=USDT";
            let sig = bitget_sign(&keys.bitget_secret, &ts, "GET", path, "");
            let resp: Value = http.get(format!("https://api.bitget.com{}", path))
                .header("ACCESS-KEY", &keys.bitget_key).header("ACCESS-SIGN", sig)
                .header("ACCESS-TIMESTAMP", &ts).header("ACCESS-PASSPHRASE", &keys.bitget_passphrase)
                .timeout(Duration::from_secs(10)).send().await?.json().await?;
            let bal = resp["data"]["available"].as_str().and_then(|s| s.parse().ok())
                .or_else(|| resp["data"]["available"].as_f64()).unwrap_or(0.0);
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
                .unwrap_or(0.0004);
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
                .unwrap_or(0.0005);
            Ok(taker.abs()) // ensure positive
        }
        "htx" => {
            // HTX uses a fee field in the account response
            Ok(0.0004) // HTX typical futures taker — no simple endpoint
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
                .unwrap_or(0.0006);
            Ok(taker)
        }
        "bitget" => {
            let ts = now_ms().to_string();
            let path = "/api/v2/mix/market/query-position-lever?symbol=BTCUSDT&productType=USDT-FUTURES";
            let sig = bitget_sign(&keys.bitget_secret, &ts, "GET", path, "");
            let resp: Value = http.get(format!("https://api.bitget.com{}", path))
                .header("ACCESS-KEY", &keys.bitget_key).header("ACCESS-SIGN", sig)
                .header("ACCESS-TIMESTAMP", &ts).header("ACCESS-PASSPHRASE", &keys.bitget_passphrase)
                .timeout(Duration::from_secs(10)).send().await?.json().await?;
            let taker = resp["data"]["takerFeeRate"].as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| resp["data"]["takerFeeRate"].as_f64())
                .unwrap_or(0.0006);
            Ok(taker)
        }
        _ => Ok(0.0005),
    }
}

/// Check if exchange has enough margin for the notional volume.
/// Returns true if balance is unknown (don't block on missing cache).
fn has_sufficient_margin(exchange: &str, notional_usd: f64) -> bool {
    let cache = get_balance_cache();
    match cache.get(exchange) {
        Some(bal) => {
            let available = *bal;
            // Require at least 1.5x notional (to account for existing positions + fees)
            available >= notional_usd * 1.5
        }
        None => true, // No data yet — allow (fail-safe)
    }
}

/// Deduct notional from cached balance after a successful trade open.
fn deduct_cached_balance(exchange: &str, notional_usd: f64) {
    let cache = get_balance_cache();
    if let Some(mut bal) = cache.get_mut(exchange) {
        *bal = (*bal - notional_usd).max(0.0);
    }
}

/// Check if an exchange error is a margin/balance/position-limit issue.
/// These errors mean the exchange WILL reject orders — no point retrying soon.
fn is_margin_error(err: &str) -> bool {
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

pub fn start_multiplier_fetcher(http: Client) {
    let _ = get_multipliers();
    let _ = get_launch_times();
    let _ = get_tick_sizes();

    // Gate.io
    tokio::spawn({
        let http = http.clone();
        async move {
            loop {
                if let Ok(resp) = http.get("https://api.gateio.ws/api/v4/futures/usdt/contracts").send().await {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json.as_array() {
                            let map = get_multipliers();
                            let lmap = get_launch_times();
                            let tmap = get_tick_sizes();
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

    // HTX
    tokio::spawn({
        let http = http.clone();
        async move {
            loop {
                if let Ok(resp) = http.get("https://api.hbdm.com/linear-swap-api/v1/swap_contract_info").send().await {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["data"].as_array() {
                            let map = get_multipliers();
                            let lmap = get_launch_times();
                            let tmap = get_tick_sizes();
                            for c in arr {
                                let name_stripped = c["contract_code"].as_str().unwrap_or("").replace("-", "");
                                if let Some(mult) = c["contract_size"].as_f64() {
                                    map.insert(format!("htx:{}", name_stripped), mult);
                                }
                                // HTX: price_tick is the minimum price increment
                                if let Some(tick) = c["price_tick"].as_f64() {
                                    if tick > 0.0 { tmap.insert(format!("htx:{}", name_stripped), tick); }
                                }
                                if let Some(date_str) = c["create_date"].as_str() {
                                    if let Ok(dt) = chrono::NaiveDate::parse_from_str(date_str, "%Y%m%d") {
                                        if let Some(dti) = dt.and_hms_opt(0, 0, 0) {
                                            lmap.insert(format!("htx:{}", name_stripped), dti.timestamp());
                                        }
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
        async move {
            loop {
                if let Ok(resp) = http.get("https://api-futures.kucoin.com/api/v1/contracts/active").send().await {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["data"].as_array() {
                            let map = get_multipliers();
                            let lmap = get_launch_times();
                            let tmap = get_tick_sizes();
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
        async move {
            loop {
                if let Ok(resp) = http.get("https://fapi.binance.com/fapi/v1/exchangeInfo").send().await {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(symbols) = json["symbols"].as_array() {
                            let map = get_multipliers();
                            let lmap = get_launch_times();
                            let tmap = get_tick_sizes();
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

    // Bitget USDT-M Futures
    tokio::spawn({
        let http = http.clone();
        async move {
            loop {
                if let Ok(resp) = http.get("https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES").send().await {
                    if let Ok(json) = resp.json::<Value>().await {
                        if let Some(arr) = json["data"].as_array() {
                            let map = get_multipliers();
                            let lmap = get_launch_times();
                            let tmap = get_tick_sizes();
                            let mut count = 0u32;
                            for c in arr {
                                let sym = c["symbol"].as_str().unwrap_or("");
                                // Bitget uses e.g. "BTCUSDT" symbol names
                                if let Some(sm) = c["sizeMultiplier"].as_str() {
                                    if let Ok(m) = sm.parse::<f64>() {
                                        if m > 0.0 {
                                            map.insert(format!("bitget:{}", sym), m);
                                            count += 1;
                                        }
                                    }
                                }
                                // Bitget: pricePlace = number of decimal places
                                if let Some(pp) = c["pricePlace"].as_str() {
                                    if let Ok(places) = pp.parse::<i32>() {
                                        let tick = 10f64.powi(-places);
                                        tmap.insert(format!("bitget:{}", sym), tick);
                                    }
                                }
                                if let Some(lt) = c["listTime"].as_str() {
                                    if let Ok(ts) = lt.parse::<i64>() {
                                        lmap.insert(format!("bitget:{}", sym), ts / 1000);
                                    }
                                }
                            }
                            info!("[multipliers] Bitget: loaded {} symbols", count);
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        }
    });
}


fn now_secs() -> i64 {
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
fn htx_ts() -> String { chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string() }
fn url_enc(s: &str) -> String {
    s.replace('+', "%2B").replace('/', "%2F").replace('=', "%3D").replace(':', "%3A")
}
fn htx_sign(key: &str, secret: &str, method: &str, host: &str, path: &str) -> String {
    let ts = htx_ts();
    let params = format!("AccessKeyId={}&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp={}", key, url_enc(&ts));
    let sig = hmac_sha256_b64(secret.as_bytes(), &format!("{}\n{}\n{}\n{}", method, host, path, params));
    format!("{}?{}&Signature={}", path, params, url_enc(&sig))
}
fn kucoin_sign(secret: &str, passphrase: &str, ts: &str, method: &str, path: &str, body: &str) -> (String, String) {
    (hmac_sha256_b64(secret.as_bytes(), &format!("{}{}{}{}", ts, method, path, body)),
     hmac_sha256_b64(secret.as_bytes(), passphrase))
}
fn gate_sign(key: &str, secret: &str, method: &str, path: &str, body: &str) -> reqwest::header::HeaderMap {
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
fn binance_sign(secret: &str, qs: &str) -> String {
    hmac_sha256_hex(secret.as_bytes(), qs)
}
fn bitget_sign(secret: &str, ts: &str, method: &str, path: &str, body: &str) -> String {
    hmac_sha256_b64(secret.as_bytes(), &format!("{}{}{}{}", ts, method, path, body))
}


/// Place an order on an exchange.
/// `limit_price` > 0 → Limit IOC (price-protected, no slippage beyond this).
/// `limit_price` == 0 → Market order (used for closes where execution is critical).
async fn place_order(http: &Client, keys: &ExchangeKeys, exchange: &str, pair: &str, is_short: bool, contracts: i64, dry_run: bool, is_reduce_only: bool, limit_price: f64) -> Result<String> {
    let side = if is_short { "sell" } else { "buy" };
    let use_limit = limit_price > 0.0 && !is_reduce_only; // Limit IOC only for entries
    // Round price to exchange tick size to avoid "PRICE PARAMETER INVALID" errors
    let limit_price = if use_limit { round_to_tick(limit_price, exchange, pair) } else { 0.0 };
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
            
            if dry_run { info!("[DRY] binance order {} size={} limit={:.6}", pair, contracts, limit_price); return Ok(format!("dry_{}", Uuid::new_v4())); }
            let resp = http.post(full_url).header("X-MBX-APIKEY", &keys.binance_key)
                .timeout(Duration::from_secs(10)).send().await?;
            let status = resp.status();
            let v: Value = resp.json().await?;
            if !status.is_success() { return Err(anyhow::anyhow!("Binance error: {}", v)); }
            Ok(v["orderId"].as_i64().map(|i| i.to_string()).unwrap_or_else(|| "unknown".to_string()))
        }
        "bitget" => {
            let ts = now_ms().to_string();
            let path = "/api/v2/mix/order/place-order";
            let trade_side = if is_reduce_only { "close" } else { "open" };
            let body = if use_limit {
                json!({
                    "symbol": pair,
                    "productType": "USDT-FUTURES",
                    "marginMode": "crossed",
                    "marginCoin": "USDT",
                    "size": contracts.to_string(),
                    "side": side,
                    "tradeSide": trade_side,
                    "orderType": "limit",
                    "force": "ioc",
                    "price": format!("{:.8}", limit_price)
                })
            } else {
                json!({
                    "symbol": pair,
                    "productType": "USDT-FUTURES",
                    "marginMode": "crossed",
                    "marginCoin": "USDT",
                    "size": contracts.to_string(),
                    "side": side,
                    "tradeSide": trade_side,
                    "orderType": "market"
                })
            };
            let body_str = body.to_string();
            let sig = bitget_sign(&keys.bitget_secret, &ts, "POST", path, &body_str);
            if dry_run { info!("[DRY] bitget order {} size={} limit={:.6}", pair, contracts, limit_price); return Ok(format!("dry_{}", Uuid::new_v4())); }
            let resp = http.post(format!("https://api.bitget.com{}", path))
                .header("ACCESS-KEY", &keys.bitget_key).header("ACCESS-SIGN", sig)
                .header("ACCESS-TIMESTAMP", &ts).header("ACCESS-PASSPHRASE", &keys.bitget_passphrase)
                .header("Content-Type", "application/json")
                .body(body_str).timeout(Duration::from_secs(10)).send().await?;
            let status = resp.status();
            let v: Value = resp.json().await?;
            if !status.is_success() || v["code"].as_str() != Some("00000") { return Err(anyhow::anyhow!("Bitget error: {}", v)); }
            Ok(v["data"]["orderId"].as_str().unwrap_or_else(|| "unknown").to_string())
        }
        "gate" => {
            let contract = pair.replace("USDT", "_USDT");
            let size = if is_short { -contracts } else { contracts };
            let path = "/api/v4/futures/usdt/orders";
            let price_str = if use_limit { format!("{:.8}", limit_price) } else { "0".to_string() };
            let body = json!({"contract":contract,"size":size,"price":price_str,"tif":"ioc","reduce_only":is_reduce_only,"close":false,"text":"t-corenode"});
            let body_str = body.to_string();
            if dry_run { info!("[DRY] gate order {} size={} limit={:.6}", contract, size, limit_price); return Ok(format!("dry_{}", Uuid::new_v4())); }
            let resp = http.post(format!("https://api.gateio.ws{}", path)).headers(gate_sign(&keys.gate_key, &keys.gate_secret, "POST", path, &body_str)).body(body_str).timeout(Duration::from_secs(10)).send().await?;
            let status = resp.status();
            let v: Value = resp.json().await?;
            if !status.is_success() { return Err(anyhow::anyhow!("Gate error: {}", v)); }
            Ok(v["id"].as_i64().map(|i| i.to_string()).unwrap_or_else(|| v["id"].as_str().unwrap_or("unknown").to_string()))
        }
        "htx" => {
            let cc = format!("{}-{}", &pair[..pair.len()-4], &pair[pair.len()-4..]);
            let host = "api.hbdm.com"; let path = "/linear-swap-api/v1/swap_order";
            let offset = if is_reduce_only { "close" } else { "open" };
            // HTX: use "opponent_ioc" for controlled market-like execution (fills at best available, cancels remainder)
            // For limit: use price + ioc
            let body = if use_limit {
                json!({"contract_code":cc,"client_order_id":now_ms(),"volume":contracts,"direction":side,"offset":offset,"lever_rate":1,"order_price_type":"ioc","price":format!("{:.8}", limit_price)})
            } else {
                json!({"contract_code":cc,"client_order_id":now_ms(),"volume":contracts,"direction":side,"offset":offset,"lever_rate":1,"order_price_type":"opponent_ioc"})
            };
            if dry_run { info!("[DRY] htx order {} {} limit={:.6}", cc, side, limit_price); return Ok(format!("dry_{}", Uuid::new_v4())); }
            let resp = http.post(format!("https://{}{}", host, htx_sign(&keys.htx_key, &keys.htx_secret, "POST", host, path))).json(&body).timeout(Duration::from_secs(10)).send().await?;
            let status = resp.status();
            let v: Value = resp.json().await?;
            if !status.is_success() || v["status"].as_str() == Some("error") { return Err(anyhow::anyhow!("HTX error: {}", v)); }
            Ok(v["data"]["order_id"].as_i64().map(|i| i.to_string()).unwrap_or_else(|| "unknown".to_string()))
        }
        "kucoin" => {
            let sym = format!("{}M", pair);
            let ts = now_ms().to_string(); let path = "/api/v1/orders";
            let body = if use_limit {
                let mut b = json!({"clientOid":Uuid::new_v4().to_string(),"symbol":sym,"side":side,"type":"limit","timeInForce":"IOC","price":format!("{:.8}", limit_price),"size":contracts,"leverage":"1","marginMode":"CROSS"});
                if is_reduce_only { b["reduceOnly"] = json!(true); }
                b
            } else if is_reduce_only {
                json!({"clientOid":Uuid::new_v4().to_string(),"symbol":sym,"side":side,"type":"market","size":contracts,"leverage":"1","marginMode":"CROSS","reduceOnly":true})
            } else {
                json!({"clientOid":Uuid::new_v4().to_string(),"symbol":sym,"side":side,"type":"market","size":contracts,"leverage":"1","marginMode":"CROSS"})
            };
            let body_str = body.to_string();
            let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "POST", path, &body_str);
            if dry_run { info!("[DRY] kucoin order {} {} limit={:.6}", sym, side, limit_price); return Ok(format!("dry_{}", Uuid::new_v4())); }
            let resp = http.post(format!("https://api-futures.kucoin.com{}", path))
                .header("KC-API-KEY", &keys.kucoin_key).header("KC-API-SIGN", sig)
                .header("KC-API-TIMESTAMP", &ts).header("KC-API-PASSPHRASE", pass_sig)
                .header("KC-API-KEY-VERSION", "2").header("Content-Type", "application/json")
                .body(body_str).timeout(Duration::from_secs(10)).send().await?;
            let status = resp.status();
            let v: Value = resp.json().await?;
            if !status.is_success() || v["code"].as_str() != Some("200000") { return Err(anyhow::anyhow!("Kucoin error: {}", v)); }
            Ok(v["data"]["orderId"].as_str().unwrap_or("unknown").to_string())
        }
        _ => Err(anyhow::anyhow!("Unknown exchange: {}", exchange)),
    }
}

async fn close_order(http: &Client, keys: &ExchangeKeys, exchange: &str, pair: &str, was_short: bool, contracts: i64, dry_run: bool) -> Result<String> {
    // Closes use market orders (limit_price=0) for guaranteed execution — 
    // orphaned positions are worse than slippage on exit.
    place_order(http, keys, exchange, pair, !was_short, contracts, dry_run, true, 0.0).await
}

// ─── Engine cycle ─────────────────────────────────────────────────────────────

/// Called on every WS tick. Manages real trades in the DB.
pub async fn run_cycle(spreads: Vec<SpreadRow>, prices: crate::state::PriceMap, settings: TradingSettings, http: Client, db: Connection, keys: Arc<ExchangeKeys>) {
    let dry_run = false;

    let trades = load_open_trades(&db, &settings.id).await;
    let num_active = trades.len() as i64;
    let funding = settings.funding_rate_8h / 100.0;

    // NOTE: We process closes ALWAYS (even when is_active=false)
    // so that MANUAL_CLOSE from the UI works even when the bot is stopped.
    // Only new trade entries are gated by is_active.

    // ── Close qualifying trades ────────────────────────────────────────────────
    if !trades.is_empty() {
        info!("[real] Processing {} open trades for profile {}", trades.len(), settings.id);
    }
    let mut live_updates: Vec<(i64, f64, f64, f64, f64, f64, f64, i64)> = Vec::new();
    for trade in &trades {
        let is_a_higher = trade.direction.starts_with(&trade.exchange_a);
        let elapsed_ms = now_ms() - trade.entry_time;

        let manual_close = trade.close_reason.as_deref() == Some("MANUAL_CLOSE");
        if manual_close {
            info!("[real] MANUAL_CLOSE detected for trade {} {} on {}/{}", trade.id, trade.pair, trade.exchange_a, trade.exchange_b);
        }

        // CRITICAL: skip trades where entry prices aren't confirmed yet (fast-poll pending)
        // Bypass this check if the user is forcing a manual close.
        if (trade.entry_price_a <= 0.0 || trade.entry_price_b <= 0.0) && !manual_close {
            info!("[real] Skipping close for {} {} — entry prices not confirmed yet", trade.id, trade.pair);
            continue;
        }

        let data_a = prices.get(&trade.exchange_a).and_then(|m| m.get(&trade.pair).cloned());
        let data_b = prices.get(&trade.exchange_b).and_then(|m| m.get(&trade.pair).cloned());

        if (data_a.is_none() || data_b.is_none()) && !manual_close { continue; }
        
        // Fallback prices in case of missing WebSocket data during a manual close
        let mut exit_price_a = trade.entry_price_a.max(1e-12);
        let mut exit_price_b = trade.entry_price_b.max(1e-12);
        let mut c_price_a = exit_price_a;
        let mut c_price_b = exit_price_b;
        let mut current_spread = 0.0;
        let mut exit_spread = 0.0;

        if let (Some(da), Some(db)) = (&data_a, &data_b) {
            if da.bid <= 0.0 || da.ask <= 0.0 || db.bid <= 0.0 || db.ask <= 0.0 {
                if !manual_close { continue; }
            } else {
                c_price_a = da.price;
                c_price_b = db.price;
                exit_price_a = if is_a_higher { da.ask } else { da.bid };
                exit_price_b = if is_a_higher { db.bid } else { db.ask };
                
                current_spread = if is_a_higher {
                    (da.price - db.price) / db.price.max(1e-12) * 100.0
                } else {
                    (db.price - da.price) / da.price.max(1e-12) * 100.0
                };
                exit_spread = if is_a_higher {
                    (da.ask - db.bid) / db.bid.max(1e-12) * 100.0
                } else {
                    (db.ask - da.bid) / da.bid.max(1e-12) * 100.0
                };
            }
        }

        let contracts_a = trade.contracts_a as f64;
        let contracts_b = trade.contracts_b as f64;

        // PnL for DISPLAY (using mark/mid prices — matches exchange PnL)
        let live_pnl_a = if is_a_higher { (trade.entry_price_a - c_price_a) * (trade.volume / trade.entry_price_a.max(1e-12)) }
                    else { (c_price_a - trade.entry_price_a) * (trade.volume / trade.entry_price_a.max(1e-12)) };
        let live_pnl_b = if is_a_higher { (c_price_b - trade.entry_price_b) * (trade.volume / trade.entry_price_b.max(1e-12)) }
                    else { (trade.entry_price_b - c_price_b) * (trade.volume / trade.entry_price_b.max(1e-12)) };
        let live_gross_pnl = live_pnl_a + live_pnl_b;
        let fund_cost = funding * trade.volume;
        let live_net_pnl = live_gross_pnl - trade.commission_cost - fund_cost;

        // PnL for CLOSE TRIGGERS (using bid/ask — pessimistic, for safe execution)
        let exec_pnl_a = if is_a_higher { (trade.entry_price_a - exit_price_a) * (trade.volume / trade.entry_price_a.max(1e-12)) }
                    else { (exit_price_a - trade.entry_price_a) * (trade.volume / trade.entry_price_a.max(1e-12)) };
        let exec_pnl_b = if is_a_higher { (exit_price_b - trade.entry_price_b) * (trade.volume / trade.entry_price_b.max(1e-12)) }
                    else { (trade.entry_price_b - exit_price_b) * (trade.volume / trade.entry_price_b.max(1e-12)) };
        let exec_gross_pnl = exec_pnl_a + exec_pnl_b;
        let exec_net_pnl = exec_gross_pnl - trade.commission_cost - fund_cost;
        let exec_net_pct = exec_net_pnl / trade.volume * 100.0;

        let top_spread = trade.highest_spread.max(current_spread);
        let stop_limit = top_spread - settings.trailing_stop_percent;

        let triggered_tp   = exec_net_pct >= settings.take_profit_percent;
        let triggered_stop = top_spread > 0.0 && current_spread < stop_limit;
        let zero_spread    = exit_spread <= 0.0 && exec_net_pnl >= 0.0;

        if manual_close || triggered_tp || triggered_stop || zero_spread {
            let reason = if manual_close { "MANUAL" } else if triggered_tp { "TP" } else if zero_spread { "ZERO_SPREAD" } else { "STOP" };
            info!("[real] Closing {} {} reason={} pct={:.3}% exit_sp={:.4} norm={:.4}", trade.id, trade.pair, reason, exec_net_pct, exit_spread,
                spreads.iter().find(|s| s.pair == trade.pair && s.direction == trade.direction).map(|s| s.structural_spread).unwrap_or(0.0));

            let exa = trade.exchange_a.clone(); let exb = trade.exchange_b.clone(); let pair = trade.pair.clone();
            let c_a = trade.contracts_a; let c_b = trade.contracts_b;
            let id = trade.id; let vol = trade.volume;
            let entry_a = trade.entry_price_a; let entry_b = trade.entry_price_b;
            let h = http.clone(); let k = Arc::clone(&keys); let db_c = db.clone();
            let r = reason.to_string();
            let settings_comm = settings.commission_rate;

            tokio::spawn(async move {
                // Place close orders in parallel
                let (ra, rb) = tokio::join!(
                    close_order(&h, &k, &exa, &pair, is_a_higher, c_a, dry_run),
                    close_order(&h, &k, &exb, &pair, !is_a_higher, c_b, dry_run)
                );
                let oid_close_a = ra.ok();
                let oid_close_b = rb.ok();

                // Mark CLOSED immediately with current prices and estimated PnL
                let _ = db_c.call(move |conn| {
                    conn.execute(
                        "UPDATE trades SET status='CLOSED',exit_price_a=?,exit_price_b=?,exit_spread=?,exit_time=?,pnl=?,gross_pnl=?,pnl_a=?,pnl_b=?,curr_gross_pnl=?,close_reason=?,funding_cost=? WHERE id=?",
                        params![exit_price_a, exit_price_b, exit_spread, now_ms(), exec_net_pnl, exec_gross_pnl, exec_pnl_a, exec_pnl_b, exec_gross_pnl, r, fund_cost, id]
                    )?;
                    Ok(())
                }).await;

                // Fast-poll real exit prices and recalculate PnL
                let mut real_exit_a = exit_price_a; let mut real_exit_b = exit_price_b;
                let mut exit_fee = 0.0; let mut got_a = false; let mut got_b = false;

                for _ in 0..20 {
                    if !got_a {
                        if let Some(ref oid) = oid_close_a {
                            if let Ok((p, f)) = fetch_order(&h, &k, &exa, &pair, oid).await {
                                if p > 0.0 {
                                    // Sanity: exit can't differ from entry by more than 20%
                                    if (p - entry_a).abs() / entry_a < 0.20 { real_exit_a = p; }
                                    else { tracing::warn!("Exit price A sanity fail: entry={} exit={}", entry_a, p); }
                                    exit_fee += f.abs(); got_a = true;
                                }
                            }
                        } else { got_a = true; }
                    }
                    if !got_b {
                        if let Some(ref oid) = oid_close_b {
                            if let Ok((p, f)) = fetch_order(&h, &k, &exb, &pair, oid).await {
                                if p > 0.0 {
                                    if (p - entry_b).abs() / entry_b < 0.20 { real_exit_b = p; }
                                    else { tracing::warn!("Exit price B sanity fail: entry={} exit={}", entry_b, p); }
                                    exit_fee += f.abs(); got_b = true;
                                }
                            }
                        } else { got_b = true; }
                    }
                    if got_a && got_b { break; }
                    tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
                }

                // Recalculate PnL with real exit prices
                let real_pna = if is_a_higher { (entry_a - real_exit_a) * (vol / entry_a) } else { (real_exit_a - entry_a) * (vol / entry_a) };
                let real_pnb = if is_a_higher { (real_exit_b - entry_b) * (vol / entry_b) } else { (entry_b - real_exit_b) * (vol / entry_b) };
                let real_gp = real_pna + real_pnb;
                
                // Exit slippage cost — clamped to >= 0 (price improvement is not cost)
                let slip_cost_exit_a = if real_exit_a > 0.0 {
                    if is_a_higher { ((real_exit_a - exit_price_a) * (vol / entry_a)).max(0.0) }
                    else           { ((exit_price_a - real_exit_a) * (vol / entry_a)).max(0.0) }
                } else { 0.0 };
                let slip_cost_exit_b = if real_exit_b > 0.0 {
                    if !is_a_higher { ((real_exit_b - exit_price_b) * (vol / entry_b)).max(0.0) }
                    else            { ((exit_price_b - real_exit_b) * (vol / entry_b)).max(0.0) }
                } else { 0.0 };
                let exit_slip = slip_cost_exit_a + slip_cost_exit_b;

                // Fallback: if exchange didn't report fees, estimate from commission_rate (2 exit legs)
                if exit_fee < 1e-9 {
                    exit_fee = vol * 2.0 * (settings_comm / 100.0);
                }

                info!("[real] PnL raw id={} pna={:.4} pnb={:.4} gross={:.4} exit_fee={:.4} exit_slip={:.4}", id, real_pna, real_pnb, real_gp, exit_fee, exit_slip);

                let _ = db_c.call(move |conn| {
                    conn.execute(
                        "UPDATE trades SET exit_price_a=?,exit_price_b=?,pnl_a=?,pnl_b=?,gross_pnl=?,commission_cost=commission_cost+?,slippage_cost=slippage_cost+?,curr_gross_pnl=?, pnl=(?-funding_cost-(commission_cost+?)) WHERE id=?",
                        params![real_exit_a, real_exit_b, real_pna, real_pnb, real_gp, exit_fee, exit_slip, real_gp, real_gp, exit_fee, id]
                    )?;
                    Ok(())
                }).await;
            });
        } else {
            // Fetch real funding rates from exchange APIs (cached 5 min)
            let (fr_a, next_a) = get_funding_rate(&http, &trade.exchange_a, &trade.pair).await;
            let (fr_b, next_b) = get_funding_rate(&http, &trade.exchange_b, &trade.pair).await;
            let fund_next = next_a.min(next_b); // nearest funding event
            live_updates.push((trade.id, c_price_a, c_price_b, current_spread, live_net_pnl, fr_a, fr_b, fund_next));
            if top_spread > trade.highest_spread {
                let id = trade.id;
                let _ = db.call(move |conn| {
                    conn.execute("UPDATE trades SET highest_spread_reached=? WHERE id=?", params![top_spread, id])?;
                    Ok(())
                }).await;
            }
        }
    }

    static LAST_LIVE_FLUSH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let current_ms = now_ms() as u64;
    let elapsed = current_ms.saturating_sub(LAST_LIVE_FLUSH.load(std::sync::atomic::Ordering::Relaxed));
    if elapsed > 1000 && !live_updates.is_empty() {
        LAST_LIVE_FLUSH.store(current_ms, std::sync::atomic::Ordering::Relaxed);
        let db_clone = db.clone();
        tokio::spawn(async move {
            let _ = db_clone.call(move |conn| {
                let tx = conn.transaction()?;
                {
                    let mut stmt = tx.prepare_cached("UPDATE trades SET last_price_a=?, last_price_b=?, last_spread=?, curr_gross_pnl=?, funding_rate_a=?, funding_rate_b=?, funding_next_ts=? WHERE id=?")?;
                    for (id, pa, pb, cs, pnl, fra, frb, fnt) in live_updates {
                        stmt.execute(params![pa, pb, cs, pnl, fra, frb, fnt, id])?;
                    }
                }
                tx.commit()?;
                Ok::<(), tokio_rusqlite::Error>(())
            }).await;
        });
    }

    // ── Open new real trades ──────────────────────────────────────────────────
    // KEY PRINCIPLE: We do NOT write to DB until BOTH legs are confirmed filled.
    // This eliminates phantom trades, $0 PnL records, and immediate open/close chaos.
    if !settings.is_active { return; } // only block NEW entries, not closes above
    if num_active >= settings.max_active_trades { return; }
    let open_pairs: std::collections::HashSet<String> = trades.iter().map(|t| t.pair.clone()).collect();

    // ── Cooldown maps (persist across cycles to prevent spam) ─────────────────
    static EXCHANGE_COOLDOWN: OnceLock<DashMap<String, i64>> = OnceLock::new();
    static PAIR_COOLDOWN: OnceLock<DashMap<String, i64>> = OnceLock::new();
    let ex_cd = EXCHANGE_COOLDOWN.get_or_init(DashMap::new);
    let pair_cd = PAIR_COOLDOWN.get_or_init(DashMap::new);
    let now = now_ms();

    // Diagnostic logging (throttled)
    static DIAG_TICK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dtick = DIAG_TICK.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if dtick % 100 == 0 {
        let fees: Vec<_> = get_fee_cache().iter().map(|e| format!("{}={:.3}%", e.key(), e.value() * 100.0)).collect();
        let in_range = spreads.iter().filter(|r| r.spread >= settings.min_entry_spread && r.spread <= settings.max_entry_spread).count();
        let supported = spreads.iter().filter(|r| keys.supports(&r.exchange_a) && keys.supports(&r.exchange_b)).count();
        let cooled_ex: Vec<_> = ex_cd.iter().filter(|e| now - *e.value() < 1_800_000).map(|e| e.key().clone()).collect();
        info!("[diag] spreads={} in_range={} supported={} active={}/{} cooled={:?} fees={:?}",
            spreads.len(), in_range, supported, num_active, settings.max_active_trades, cooled_ex, fees);
    }

    // Only open ONE trade per engine cycle to prevent burst-opening
    let mut opened_this_cycle = false;

    for row in spreads.iter().take(200) {
        if opened_this_cycle { break; }
        if row.spread < settings.min_entry_spread || row.spread > settings.max_entry_spread { continue; }

        // Absolute floor: spread must exceed round-trip costs from REAL exchange fee rates
        // fee_a, fee_b are fetched from exchange APIs and cached every 60s
        let fee_a = get_exchange_fee(&row.exchange_a);
        let fee_b = get_exchange_fee(&row.exchange_b);
        let slippage_frac = settings.slippage_percent / 100.0;
        // 4 legs total: open_A(fee_a) + open_B(fee_b) + close_A(fee_a) + close_B(fee_b) + 4*slippage
        let break_even_pct = (2.0 * (fee_a + fee_b) + 4.0 * slippage_frac) * 100.0;
        if row.spread < break_even_pct * 1.5 {
            continue; // spread too small to cover real costs (1.5x safety margin)
        }

        // Smart structural filter: deviation above baseline must ALSO cover costs
        let deviation = row.spread - row.structural_spread;
        if deviation < break_even_pct * 1.5 { continue; }

        if open_pairs.contains(&row.pair) { continue; }

        // Exchange support check
        if !keys.supports(&row.exchange_a) || !keys.supports(&row.exchange_b) { continue; }
        if settings.disabled_exchanges.contains(&row.exchange_a) || settings.disabled_exchanges.contains(&row.exchange_b) { continue; }

        // Exchange cooldown check (30 min after margin/balance error)
        let cd_a = ex_cd.get(&row.exchange_a).map(|v| *v).unwrap_or(0);
        let cd_b = ex_cd.get(&row.exchange_b).map(|v| *v).unwrap_or(0);
        if now - cd_a < 1_800_000 || now - cd_b < 1_800_000 { continue; }

        // Pair cooldown check (5 min after any failure)
        let cd_pair = pair_cd.get(&row.pair).map(|v| *v).unwrap_or(0);
        if now - cd_pair < 300_000 { continue; }

        let is_a_higher = row.direction.starts_with(&row.exchange_a);
        let entry_a = if is_a_higher { row.bid_a } else { row.ask_a };
        let entry_b = if is_a_higher { row.ask_b } else { row.bid_b };

        // Price sanity: both legs must price the same asset — reject if >10% apart
        let price_ratio = if entry_a > entry_b { entry_a / entry_b } else { entry_b / entry_a };
        if price_ratio > 1.10 {
            info!("[SKIP] {} price sanity: A={:.6} B={:.6} ratio={:.3}", row.pair, entry_a, entry_b, price_ratio);
            continue;
        }

        // ── LIQUIDITY DEPTH FILTER ────────────────────────────────────────────
        // Check that the best bid/ask level has enough volume for our trade.
        // bid_size/ask_size are in USD, populated by WS collectors.
        let data_a = prices.get(&row.exchange_a).and_then(|m| m.get(&row.pair).cloned());
        let data_b = prices.get(&row.exchange_b).and_then(|m| m.get(&row.pair).cloned());
        let min_depth_usd = settings.trade_volume * 2.0;
        if let (Some(ref da), Some(ref db)) = (&data_a, &data_b) {
            // For the selling side (is_a_higher=true → sell A, buy B), check bid_size on A and ask_size on B
            let depth_sell = if is_a_higher { da.bid_size } else { db.bid_size };
            let depth_buy  = if is_a_higher { db.ask_size } else { da.ask_size };
            if depth_sell > 0.0 && depth_sell < min_depth_usd {
                continue; // not enough depth on the sell side
            }
            if depth_buy > 0.0 && depth_buy < min_depth_usd {
                continue; // not enough depth on the buy side
            }

            // ── INTRA-SPREAD FILTER ───────────────────────────────────────────
            // If bid-ask spread on either exchange is too wide, the pair is illiquid NOW
            let intra_a = (da.ask - da.bid) / da.bid.max(1e-12) * 100.0;
            let intra_b = (db.ask - db.bid) / db.bid.max(1e-12) * 100.0;
            if intra_a > 0.5 || intra_b > 0.5 {
                continue; // > 0.5% bid-ask spread = too illiquid
            }
        }

        let map = get_multipliers();
        let mult_a = match map.get(&format!("{}:{}", row.exchange_a, row.pair)) {
            Some(v) => *v,
            None => { continue; } // silent skip — multiplier not loaded yet
        };
        let mult_b = match map.get(&format!("{}:{}", row.exchange_b, row.pair)) {
            Some(v) => *v,
            None => { continue; }
        };

        let contracts_a = ((settings.trade_volume / entry_a.max(1e-12)) / mult_a).round() as i64;
        if contracts_a < 1 { continue; }

        let actual_vol_a = contracts_a as f64 * mult_a * entry_a;
        let contracts_b = ((actual_vol_a / entry_b.max(1e-12)) / mult_b).round() as i64;
        if contracts_b < 1 { continue; }

        let actual_vol_b = contracts_b as f64 * mult_b * entry_b;
        let vol_diff_pct = (actual_vol_a - actual_vol_b).abs() / actual_vol_a * 100.0;
        if vol_diff_pct > 5.0 { continue; }

        let max_v = settings.trade_volume * 1.15;
        let min_v = settings.trade_volume * 0.85;
        if actual_vol_a > max_v || actual_vol_a < min_v { continue; }

        let lmap = get_launch_times();
        let now_s = now_secs();
        let thirty_days = 30 * 24 * 3600;
        let age_a = lmap.get(&format!("{}:{}", row.exchange_a, row.pair)).map(|r| *r).unwrap_or(0);
        let age_b = lmap.get(&format!("{}:{}", row.exchange_b, row.pair)).map(|r| *r).unwrap_or(0);
        if (age_a > 0 && now_s - age_a < thirty_days) || (age_b > 0 && now_s - age_b < thirty_days) {
            continue;
        }

        // ── PRE-FLIGHT BALANCE CHECK ──────────────────────────────────────────
        if !has_sufficient_margin(&row.exchange_a, actual_vol_a) {
            info!("[SKIP] {} — {} insufficient margin (need ${:.0}, cached bal too low)",
                row.pair, row.exchange_a, actual_vol_a);
            continue;
        }
        if !has_sufficient_margin(&row.exchange_b, actual_vol_b) {
            info!("[SKIP] {} — {} insufficient margin (need ${:.0}, cached bal too low)",
                row.pair, row.exchange_b, actual_vol_b);
            continue;
        }

        // ── COMPUTE PROTECTIVE LIMIT PRICES ──────────────────────────────────
        // Limit IOC: max price we will pay (buy) / min price we will accept (sell)
        // max_slip_frac controls how far from best bid/ask we allow execution.
        let max_slip_frac = (settings.slippage_percent / 100.0).max(0.0005); // at least 0.05%
        let limit_price_a = if is_a_higher {
            // Selling on A (short) → min price = bid * (1 - slip)
            entry_a * (1.0 - max_slip_frac)
        } else {
            // Buying on A (long) → max price = ask * (1 + slip)
            entry_a * (1.0 + max_slip_frac)
        };
        let limit_price_b = if is_a_higher {
            // Buying on B (long) → max price = ask * (1 + slip)
            entry_b * (1.0 + max_slip_frac)
        } else {
            // Selling on B (short) → min price = bid * (1 - slip)
            entry_b * (1.0 - max_slip_frac)
        };

        info!("[real] Placing orders: {} A={} c={} vol=${:.3} lim={:.6} | B={} c={} vol=${:.3} lim={:.6} spread={:.4}% (struct={:.4}% dev={:.4}% be={:.4}%)",
            row.pair, row.exchange_a, contracts_a, actual_vol_a, limit_price_a, row.exchange_b, contracts_b, actual_vol_b, limit_price_b,
            row.spread, row.structural_spread, deviation, break_even_pct);

        // ── SEQUENTIAL LEG PLACEMENT ─────────────────────────────────────────
        // Place LEG_A first. If it fails → no exposure, no loss.
        // Only if LEG_A succeeds → place LEG_B.
        // This eliminates the asymmetric-fill problem where one leg succeeds
        // and the other fails, forcing an expensive emergency close.

        let exa = row.exchange_a.clone();
        let exb = row.exchange_b.clone();
        let pair = row.pair.clone();

        // ── LEG_A (Limit IOC — price protected) ──────────────────────────────
        let oid_a = match place_order(&http, &keys, &exa, &pair, is_a_higher, contracts_a, dry_run, false, limit_price_a).await {
            Ok(id) => id,
            Err(e) => {
                let err_str = e.to_string();
                error!("[real] LEG_A failed {}/{}: {}", exa, pair, err_str);
                if is_margin_error(&err_str) {
                    ex_cd.insert(exa.clone(), now);
                    deduct_cached_balance(&exa, f64::MAX); // mark as zero
                    error!("[real] Exchange {} cooled down for 30 min (margin error)", exa);
                }
                pair_cd.insert(pair.clone(), now);
                // LEG_A failed BEFORE any position opened → record and move on (no exposure!)
                let reason = format!("LEG_A_FAILED: {}", &err_str[..err_str.len().min(120)]);
                let (db_c, ea_s, eb_s, d, sp, pid) = (db.clone(), exa.clone(), exb.clone(), row.direction.clone(), row.spread, settings.id.clone());
                let pc = pair.clone();
                tokio::spawn(async move {
                    let _ = db_c.call(move |conn| {
                        conn.execute(
                            "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,exit_time,highest_spread_reached,status,pnl_a,pnl_b,profile_id,commission_cost,slippage_cost,funding_cost,close_reason,pnl,gross_pnl) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            params![pc, ea_s, eb_s, d, 0.0f64, 0.0f64, 0.0f64, sp, now_ms(), now_ms(), sp, "CLOSED", 0.0f64, 0.0f64, pid, 0.0f64, 0.0f64, 0.0f64, reason, 0.0f64, 0.0f64]
                        )?;
                        Ok(())
                    }).await;
                });
                continue;
            }
        };

        // LEG_A succeeded! Deduct from cached balance.
        deduct_cached_balance(&exa, actual_vol_a);

        // ── LEG_B (only executed after LEG_A confirmed) ──────────────────────
        // ── LEG_B (Limit IOC — price protected) ──────────────────────────────
        let oid_b = match place_order(&http, &keys, &exb, &pair, !is_a_higher, contracts_b, dry_run, false, limit_price_b).await {
            Ok(id) => id,
            Err(e) => {
                let err_str = e.to_string();
                error!("[real] LEG_B failed {}/{}: {} — emergency close A", exb, pair, err_str);
                if is_margin_error(&err_str) {
                    ex_cd.insert(exb.clone(), now);
                    deduct_cached_balance(&exb, f64::MAX); // mark as zero
                    error!("[real] Exchange {} cooled down for 30 min (margin error)", exb);
                }
                pair_cd.insert(pair.clone(), now);
                let reason = format!("LEG_B_FAILED: {}", &err_str[..err_str.len().min(120)]);
                let (hc, kc, db_c) = (http.clone(), Arc::clone(&keys), db.clone());
                let (ec, pc) = (exa.clone(), pair.clone());
                let (ea_s, eb_s, d, sp, pid) = (exa.clone(), exb.clone(), row.direction.clone(), row.spread, settings.id.clone());
                let oid_a_c = oid_a.clone();
                let comm_rate = settings.commission_rate;
                tokio::spawn(async move {
                    // 1. Fetch real entry fill price + fee
                    let mut entry_fill = entry_a;
                    let mut total_fee = 0.0f64;
                    for _ in 0..10 {
                        if let Ok((p, f)) = fetch_order(&hc, &kc, &ec, &pc, &oid_a_c).await {
                            if p > 0.0 { entry_fill = p; total_fee += f.abs(); break; }
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }

                    // 2. Emergency close LEG_A
                    let mut close_oid = None;
                    for attempt in 1u32..=5 {
                        match close_order(&hc, &kc, &ec, &pc, is_a_higher, contracts_a, false).await {
                            Ok(id) => { error!("🛡️ Emergency close A ok: {}", id); close_oid = Some(id); break; }
                            Err(err) => { let w = 200 * 2u64.pow(attempt-1); tokio::time::sleep(Duration::from_millis(w)).await; error!("Emergency close A {}/5: {}", attempt, err); }
                        }
                    }

                    // 3. Fetch real close fill price + fee
                    let mut close_fill = entry_fill;
                    if let Some(ref coid) = close_oid {
                        for _ in 0..10 {
                            if let Ok((p, f)) = fetch_order(&hc, &kc, &ec, &pc, coid).await {
                                if p > 0.0 { close_fill = p; total_fee += f.abs(); break; }
                            }
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                    }

                    // 4. Calculate real PnL from open→close price movement
                    let qty = actual_vol_a / entry_fill.max(1e-12);
                    let gross_pnl = if is_a_higher {
                        (entry_fill - close_fill) * qty  // short: profit if price dropped
                    } else {
                        (close_fill - entry_fill) * qty  // long: profit if price rose
                    };
                    // Fallback commission if exchange didn't report fees
                    if total_fee < 1e-9 { total_fee = actual_vol_a * 2.0 * (comm_rate / 100.0); }
                    let net_pnl = gross_pnl - total_fee;
                    let entry_time = now_ms();

                    error!("[real] LEG_B_FAILED {} real PnL: entry={:.6} close={:.6} gross={:.4} fee={:.4} net={:.4}",
                        pc, entry_fill, close_fill, gross_pnl, total_fee, net_pnl);

                    // 5. Write to DB with real numbers
                    let _ = db_c.call(move |conn| {
                        conn.execute(
                            "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,exit_time,highest_spread_reached,status,pnl_a,pnl_b,profile_id,commission_cost,slippage_cost,funding_cost,close_reason,pnl,gross_pnl,exit_price_a) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            params![pc, ea_s, eb_s, d, actual_vol_a, entry_fill, 0.0f64, sp, entry_time, entry_time, sp, "CLOSED", gross_pnl, 0.0f64, pid, total_fee, 0.0f64, 0.0f64, reason, net_pnl, gross_pnl, close_fill]
                        )?;
                        Ok(())
                    }).await;
                });
                continue;
            }
        };

        // LEG_B also succeeded! Deduct from cached balance.
        deduct_cached_balance(&exb, actual_vol_b);

        // Mark this cycle as having opened a trade (only 1 per cycle)
        opened_this_cycle = true;

        // ── Fast-poll both fills simultaneously (max 750ms) ───────────────────
        // We do NOT write to DB until BOTH fills are confirmed with real prices.
        let mut fill_a = 0.0f64; let mut fee_a = 0.0f64;
        let mut fill_b = 0.0f64; let mut fee_b = 0.0f64;
        let mut got_a = dry_run; let mut got_b = dry_run; // skip poll for dry runs

        for _ in 0..15 {
            if !got_a {
                if let Ok((p, f)) = fetch_order(&http, &keys, &exa, &pair, &oid_a).await {
                    if p > 0.0 { fill_a = p; fee_a = f.abs(); got_a = true; }
                }
            }
            if !got_b {
                if let Ok((p, f)) = fetch_order(&http, &keys, &exb, &pair, &oid_b).await {
                    if p > 0.0 { fill_b = p; fee_b = f.abs(); got_b = true; }
                }
            }
            if got_a && got_b { break; }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // Use scanner entry prices as fallback for dry runs
        if dry_run { fill_a = entry_a; fill_b = entry_b; }

        if !got_a && !got_b {
            error!("[real] Fill confirmation timeout: {} no fills at all — NOT writing to DB", pair);
            continue; // ← DO NOT INSERT TO DB
        }

        // ── At least one leg confirmed! Write to DB with real fill prices ────────────
        let status = if got_a && got_b { "OPEN" } else { "FAILED_ASYMMETRIC" };
        let vol = settings.trade_volume;
        let dir = row.direction.clone();
        let sp  = row.spread;
        let profile_id = settings.id.clone();
        
        // Use actual exchange-reported fees if available, fall back to settings-based estimate
        let api_comm = fee_a + fee_b;
        let entry_comm = if api_comm > 1e-9 {
            api_comm
        } else {
            // API didn't report fees — estimate from commission_rate setting (2 legs)
            vol * 2.0 * (settings.commission_rate / 100.0)
        };

        // Slippage cost: difference between expected and actual fill price.
        // Clamped to >= 0 — price improvement is not a "cost".
        let slip_cost_a = if fill_a > 0.0 {
            if is_a_higher { ((entry_a - fill_a) * (vol / entry_a)).max(0.0) }
            else           { ((fill_a - entry_a) * (vol / entry_a)).max(0.0) }
        } else { 0.0 };
        let slip_cost_b = if fill_b > 0.0 {
            if !is_a_higher { ((entry_b - fill_b) * (vol / entry_b)).max(0.0) }
            else            { ((fill_b - entry_b) * (vol / entry_b)).max(0.0) }
        } else { 0.0 };
        let entry_slip = slip_cost_a + slip_cost_b;

        let entry_time = now_ms();
        let oid_a_db = oid_a.clone(); let oid_b_db = oid_b.clone();

        info!("[real] Inserting trade: {} status={} A={:.6}(c={}) B={:.6}(c={}) fees={:.4} slip={:.4}", pair, status, fill_a, contracts_a, fill_b, contracts_b, entry_comm, entry_slip);

        let res = db.call(move |conn| {
            conn.execute(
                "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,highest_spread_reached,status,pnl_a,pnl_b,profile_id,commission_cost,slippage_cost,funding_cost,order_id_a,order_id_b,contracts_a,contracts_b) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![pair, exa, exb, dir, vol, fill_a, fill_b, sp, entry_time, sp, status, 0.0f64, 0.0f64, profile_id, entry_comm, entry_slip, 0.0f64, oid_a_db, oid_b_db, contracts_a, contracts_b]
            )?;
            Ok::<i64, tokio_rusqlite::Error>(conn.last_insert_rowid())
        }).await;

        match res {
            Ok(trade_id) => {
                if status == "FAILED_ASYMMETRIC" {
                    let (hc, kc, db_c) = (http.clone(), Arc::clone(&keys), db.clone());
                    let (exa_c, exb_c, pair_c) = (row.exchange_a.clone(), row.exchange_b.clone(), row.pair.clone());
                    tokio::spawn(async move {
                        error!("🚨 EMERGENCY CLOSE {} {} (Asymmetric fill) — trade_id={}", exa_c, pair_c, trade_id);
                        let (mut raid, mut rbid) = (None, None);
                        if got_a && fill_a > 0.0 { raid = close_order(&hc, &kc, &exa_c, &pair_c, is_a_higher, contracts_a, false).await.ok(); }
                        if got_b && fill_b > 0.0 { rbid = close_order(&hc, &kc, &exb_c, &pair_c, !is_a_higher, contracts_b, false).await.ok(); }
                        
                        let mut fa = 0.0; let mut fb = 0.0; let mut ec = 0.0;
                        for _ in 0..15 {
                            if fa == 0.0 && raid.is_some() {
                                if let Ok((p, f)) = fetch_order(&hc, &kc, &exa_c, &pair_c, raid.as_ref().unwrap()).await {
                                    if p > 0.0 { fa = p; ec += f.abs(); }
                                }
                            }
                            if fb == 0.0 && rbid.is_some() {
                                if let Ok((p, f)) = fetch_order(&hc, &kc, &exb_c, &pair_c, rbid.as_ref().unwrap()).await {
                                    if p > 0.0 { fb = p; ec += f.abs(); }
                                }
                            }
                            if (raid.is_none() || fa > 0.0) && (rbid.is_none() || fb > 0.0) { break; }
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                        
                        let r_pna = if fa > 0.0 && fill_a > 0.0 { if is_a_higher { (fill_a - fa)*(vol/fill_a) } else { (fa - fill_a)*(vol/fill_a) } } else { 0.0 };
                        let r_pnb = if fb > 0.0 && fill_b > 0.0 { if is_a_higher { (fb - fill_b)*(vol/fill_b) } else { (fill_b - fb)*(vol/fill_b) } } else { 0.0 };
                        let np = r_pna + r_pnb - (ec + entry_comm); // rough PnL estimate
                        
                        let _ = db_c.call(move |conn| {
                            conn.execute("UPDATE trades SET status='CLOSED', close_reason='Execution Failed', exit_price_a=?, exit_price_b=?, commission_cost=commission_cost+?, pnl_a=?, pnl_b=?, gross_pnl=?, pnl=?, curr_gross_pnl=? WHERE id=?",
                                params![fa, fb, ec, r_pna, r_pnb, r_pna+r_pnb, np, r_pna+r_pnb, trade_id])?;
                            Ok(())
                        }).await;
                    });
                }
            }
            Err(e) => {
                error!("[real] DB INSERT failed: {:?}", e);
            }
        }
    }
}

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
        "bitget" => {
            let ts = now_ms().to_string();
            let path = format!("/api/v2/mix/order/detail?symbol={}&orderId={}&productType=USDT-FUTURES", pair, order_id);
            let sig = bitget_sign(&keys.bitget_secret, &ts, "GET", &path, "");
            let resp = http.get(format!("https://api.bitget.com{}", path))
                .header("ACCESS-KEY", &keys.bitget_key).header("ACCESS-SIGN", sig)
                .header("ACCESS-TIMESTAMP", &ts).header("ACCESS-PASSPHRASE", &keys.bitget_passphrase)
                .timeout(Duration::from_secs(10)).send().await?;
            let v: Value = resp.json().await?;
            let price = v["data"]["priceAvg"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let fee = v["data"]["fee"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
            Ok((price, fee.abs()))
        }
        "gate" => {
            let path = format!("/api/v4/futures/usdt/orders/{}", order_id);
            let resp = http.get(format!("https://api.gateio.ws{}", path)).headers(gate_sign(&keys.gate_key, &keys.gate_secret, "GET", &path, "")).timeout(Duration::from_secs(10)).send().await?;
            let v: Value = resp.json().await?;
            let price = v["fill_price"].as_f64().or_else(|| v["fill_price"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
            let fee = v["fee"].as_f64().or_else(|| v["fee"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
            Ok((price, fee))
        }
        "htx" => {
            let cc = format!("{}-{}", &pair[..pair.len()-4], &pair[pair.len()-4..]);
            let host = "api.hbdm.com"; let path = "/linear-swap-api/v1/swap_order_info";
            let body = json!({"contract_code":cc,"order_id":order_id});
            let resp = http.post(format!("https://{}{}", host, htx_sign(&keys.htx_key, &keys.htx_secret, "POST", host, path))).json(&body).timeout(Duration::from_secs(10)).send().await?;
            let v: Value = resp.json().await?;
            let price = v["data"][0]["trade_avg_price"].as_f64().or_else(|| v["data"][0]["trade_avg_price"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
            let fee = v["data"][0]["fee"].as_f64().or_else(|| v["data"][0]["fee"].as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
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
            
            let map = get_multipliers();
            let mult = map.get(&format!("kucoin:{}", pair)).map(|m| *m).unwrap_or(1.0);
            
            let price = if size > 0.0 && mult > 0.0 { val / (size * mult) } else { 0.0 };
            Ok((price, fee))
        }
        _ => Err(anyhow::anyhow!("Unknown exchange: {}", exchange)),
    }
}

#[derive(Debug, Clone)]
struct Trade {
    id: i64, pair: String, exchange_a: String, exchange_b: String, direction: String,
    volume: f64, entry_price_a: f64, entry_price_b: f64, entry_spread: f64,
    entry_time: i64, highest_spread: f64, pnl_a: f64, pnl_b: f64,
    close_reason: Option<String>, contracts_a: i64, contracts_b: i64,
    commission_cost: f64, slippage_cost: f64,
}

async fn load_open_trades(db: &Connection, p_id: &str) -> Vec<Trade> {
    let p_id_clone = p_id.to_string();
    db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT id, pair, exchange_a, exchange_b, direction, volume, entry_price_a, entry_price_b, entry_spread, entry_time, highest_spread_reached, pnl_a, pnl_b, close_reason, contracts_a, contracts_b, commission_cost, slippage_cost FROM trades WHERE status='OPEN' AND profile_id=?")?;
        let trades = stmt.query_map([p_id_clone], |row| Ok(Trade {
            id: row.get(0)?, pair: row.get(1)?, exchange_a: row.get(2)?, exchange_b: row.get(3)?,
            direction: row.get(4)?, volume: row.get(5)?, entry_price_a: row.get(6)?, entry_price_b: row.get(7)?,
            entry_spread: row.get(8)?, entry_time: row.get(9)?, highest_spread: row.get(10)?,
            pnl_a: row.get(11)?, pnl_b: row.get(12)?, close_reason: row.get(13)?,
            contracts_a: row.get::<_, Option<i64>>(14)?.unwrap_or((row.get::<_, f64>(5)? / row.get::<_, f64>(6)?.max(1e-12)).round() as i64),
            contracts_b: row.get::<_, Option<i64>>(15)?.unwrap_or((row.get::<_, f64>(5)? / row.get::<_, f64>(7)?.max(1e-12)).round() as i64),
            commission_cost: row.get::<_, Option<f64>>(16)?.unwrap_or(0.0),
            slippage_cost: row.get::<_, Option<f64>>(17)?.unwrap_or(0.0),
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
        "bitget" => {
            let url = format!("https://api.bitget.com/api/v2/mix/market/current-fund-rate?symbol={}&productType=USDT-FUTURES", pair);
            let resp = http.get(&url).send().await.ok()?.json::<serde_json::Value>().await.ok()?;
            let rate = resp["data"].as_array()?.first()?["fundingRate"].as_str()?.parse::<f64>().ok()?;
            let next_str = resp["data"].as_array()?.first()?["nextUpdate"].as_str()?;
            let next = next_str.parse::<i64>().ok()?;
            Some((rate, next))
        }
        _ => None,
    }
}
