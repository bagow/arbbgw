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

use crate::settings::TradingSettings;
use shared::SpreadRow;

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

pub fn get_multipliers() -> &'static DashMap<String, f64> {
    MULTIPLIERS.get_or_init(DashMap::new)
}

pub fn get_launch_times() -> &'static DashMap<String, i64> {
    LAUNCH_TIMES.get_or_init(DashMap::new)
}

pub fn start_multiplier_fetcher(http: Client) {
    let _ = get_multipliers();
    let _ = get_launch_times();

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
                            for c in arr {
                                let name_stripped = c["contract_code"].as_str().unwrap_or("").replace("-", "");
                                if let Some(mult) = c["contract_size"].as_f64() {
                                    map.insert(format!("htx:{}", name_stripped), mult);
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
                            for c in arr {
                                if let Some(name) = c["symbol"].as_str() {
                                    let mut std_name = name.replace("USDTM", "USDT").replace("USDM", "USDT");
                                    if std_name == "XBTUSDT" { std_name = "BTCUSDT".to_string(); }
                                    let full_name = format!("kucoin:{}", std_name);
                                    
                                    if let Some(mult) = c["multiplier"].as_f64() {
                                        map.insert(full_name.clone(), mult);
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

// ─── Order placement ──────────────────────────────────────────────────────────

async fn place_order(http: &Client, keys: &ExchangeKeys, exchange: &str, pair: &str, is_short: bool, contracts: i64, dry_run: bool, is_reduce_only: bool) -> Result<String> {
    let side = if is_short { "sell" } else { "buy" };
    match exchange {
        "binance" => {
            let ts = now_ms();
            let path = "/fapi/v1/order";
            let mut qs = format!("symbol={}&side={}&type=MARKET&quantity={}&timestamp={}", pair, side.to_uppercase(), contracts, ts);
            if is_reduce_only { qs.push_str("&reduceOnly=true"); }
            let sig = binance_sign(&keys.binance_secret, &qs);
            let full_url = format!("https://fapi.binance.com{}?{}&signature={}", path, qs, sig);
            
            if dry_run { info!("[DRY] binance order {} size={}", pair, contracts); return Ok(format!("dry_{}", Uuid::new_v4())); }
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
            let body = json!({
                "symbol": pair,
                "productType": "USDT-FUMAX",
                "marginMode": "crossed",
                "marginCoin": "USDT",
                "size": contracts.to_string(), // Bitget often requires string values
                "side": side,
                "tradeSide": trade_side,
                "orderType": "market"
            });
            let body_str = body.to_string();
            let sig = bitget_sign(&keys.bitget_secret, &ts, "POST", path, &body_str);
            if dry_run { info!("[DRY] bitget order {} size={}", pair, contracts); return Ok(format!("dry_{}", Uuid::new_v4())); }
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
            let body = json!({"contract":contract,"size":size,"price":"0","tif":"ioc","reduce_only":is_reduce_only,"close":false,"text":"t-corenode"});
            let body_str = body.to_string();
            if dry_run { info!("[DRY] gate order {} size={}", contract, size); return Ok(format!("dry_{}", Uuid::new_v4())); }
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
            let body = json!({"contract_code":cc,"client_order_id":now_ms(),"volume":contracts,"direction":side,"offset":offset,"lever_rate":1,"order_price_type":"optimal_20"});
            if dry_run { info!("[DRY] htx order {} {}", cc, side); return Ok(format!("dry_{}", Uuid::new_v4())); }
            let resp = http.post(format!("https://{}{}", host, htx_sign(&keys.htx_key, &keys.htx_secret, "POST", host, path))).json(&body).timeout(Duration::from_secs(10)).send().await?;
            let status = resp.status();
            let v: Value = resp.json().await?;
            if !status.is_success() || v["status"].as_str() == Some("error") { return Err(anyhow::anyhow!("HTX error: {}", v)); }
            Ok(v["data"]["order_id"].as_i64().map(|i| i.to_string()).unwrap_or_else(|| "unknown".to_string()))
        }
        "kucoin" => {
            let sym = format!("{}M", pair);
            let ts = now_ms().to_string(); let path = "/api/v1/orders";
            let body = if is_reduce_only {
                json!({"clientOid":Uuid::new_v4().to_string(),"symbol":sym,"side":side,"type":"market","size":contracts,"leverage":"1","marginMode":"CROSS","reduceOnly":true})
            } else {
                json!({"clientOid":Uuid::new_v4().to_string(),"symbol":sym,"side":side,"type":"market","size":contracts,"leverage":"1","marginMode":"CROSS"})
            };
            let body_str = body.to_string();
            let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "POST", path, &body_str);
            if dry_run { info!("[DRY] kucoin order {} {}", sym, side); return Ok(format!("dry_{}", Uuid::new_v4())); }
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
    place_order(http, keys, exchange, pair, !was_short, contracts, dry_run, true).await
}

// ─── Engine cycle ─────────────────────────────────────────────────────────────

/// Called on every WS tick. Manages real trades in the DB.
pub async fn run_cycle(spreads: Vec<SpreadRow>, prices: crate::state::PriceMap, settings: TradingSettings, http: Client, db: Connection) {
    if !settings.is_active { return; }

    let dry_run = false;
    let keys = Arc::new(ExchangeKeys::from_env());

    let trades = load_open_trades(&db, &settings.id).await;
    let num_active = trades.len() as i64;
    let funding = settings.funding_rate_8h / 100.0;

    // ── Close qualifying trades ────────────────────────────────────────────────
    let mut live_updates = Vec::new();
    for trade in &trades {
        let is_a_higher = trade.direction.starts_with(&trade.exchange_a);
        let elapsed_ms = now_ms() - trade.entry_time;

        let manual_close = trade.close_reason.as_deref() == Some("MANUAL_CLOSE");

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
        let live_net_pnl = live_gross_pnl - trade.commission_cost - trade.slippage_cost - fund_cost;

        // PnL for CLOSE TRIGGERS (using bid/ask — pessimistic, for safe execution)
        let exec_pnl_a = if is_a_higher { (trade.entry_price_a - exit_price_a) * (trade.volume / trade.entry_price_a.max(1e-12)) }
                    else { (exit_price_a - trade.entry_price_a) * (trade.volume / trade.entry_price_a.max(1e-12)) };
        let exec_pnl_b = if is_a_higher { (exit_price_b - trade.entry_price_b) * (trade.volume / trade.entry_price_b.max(1e-12)) }
                    else { (trade.entry_price_b - exit_price_b) * (trade.volume / trade.entry_price_b.max(1e-12)) };
        let exec_gross_pnl = exec_pnl_a + exec_pnl_b;
        let exec_net_pnl = exec_gross_pnl - trade.commission_cost - trade.slippage_cost - fund_cost;
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
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                }

                // Recalculate PnL with real exit prices
                let real_pna = if is_a_higher { (entry_a - real_exit_a) * (vol / entry_a) } else { (real_exit_a - entry_a) * (vol / entry_a) };
                let real_pnb = if is_a_higher { (real_exit_b - entry_b) * (vol / entry_b) } else { (entry_b - real_exit_b) * (vol / entry_b) };
                let real_gp = real_pna + real_pnb;
                
                let mut slip_cost_exit_a = 0.0;
                if real_exit_a > 0.0 {
                    if is_a_higher { slip_cost_exit_a = (real_exit_a - exit_price_a) * (vol / entry_a); }
                    else           { slip_cost_exit_a = (exit_price_a - real_exit_a) * (vol / entry_a); }
                }
                let mut slip_cost_exit_b = 0.0;
                if real_exit_b > 0.0 {
                    if !is_a_higher { slip_cost_exit_b = (real_exit_b - exit_price_b) * (vol / entry_b); }
                    else            { slip_cost_exit_b = (exit_price_b - real_exit_b) * (vol / entry_b); }
                }
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
            live_updates.push((trade.id, c_price_a, c_price_b, current_spread, live_net_pnl));
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
                    let mut stmt = tx.prepare_cached("UPDATE trades SET last_price_a=?, last_price_b=?, last_spread=?, curr_gross_pnl=? WHERE id=?")?;
                    for (id, pa, pb, cs, pnl) in live_updates {
                        stmt.execute(params![pa, pb, cs, pnl, id])?;
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
        let in_range = spreads.iter().filter(|r| r.spread >= settings.min_entry_spread && r.spread <= settings.max_entry_spread).count();
        let supported = spreads.iter().filter(|r| keys.supports(&r.exchange_a) && keys.supports(&r.exchange_b)).count();
        let cooled_ex: Vec<_> = ex_cd.iter().filter(|e| now - *e.value() < 600_000).map(|e| e.key().clone()).collect();
        info!("[diag] spreads={} in_range={} supported={} active={}/{} cooled_exchanges={:?}",
            spreads.len(), in_range, supported, num_active, settings.max_active_trades, cooled_ex);
    }

    // Only open ONE trade per engine cycle to prevent burst-opening
    let mut opened_this_cycle = false;

    for row in spreads.iter().take(200) {
        if opened_this_cycle { break; }
        if row.spread < settings.min_entry_spread || row.spread > settings.max_entry_spread { continue; }

        // Smart structural filter: deviation above baseline must cover round-trip costs
        let break_even_pct = settings.break_even_fraction() * 100.0 * 1.5; // 150% safety margin
        let deviation = row.spread - row.structural_spread;
        if deviation < break_even_pct { continue; }

        if open_pairs.contains(&row.pair) { continue; }

        // Exchange support check
        if !keys.supports(&row.exchange_a) || !keys.supports(&row.exchange_b) { continue; }
        if settings.disabled_exchanges.contains(&row.exchange_a) || settings.disabled_exchanges.contains(&row.exchange_b) { continue; }

        // Exchange cooldown check (10 min after margin/balance error)
        let cd_a = ex_cd.get(&row.exchange_a).map(|v| *v).unwrap_or(0);
        let cd_b = ex_cd.get(&row.exchange_b).map(|v| *v).unwrap_or(0);
        if now - cd_a < 600_000 || now - cd_b < 600_000 { continue; }

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

        info!("[real] Placing orders: {} A={} c={} vol=${:.3} | B={} c={} vol=${:.3} spread={:.4}% (struct={:.4}% dev={:.4}% be={:.4}%)",
            row.pair, row.exchange_a, contracts_a, actual_vol_a, row.exchange_b, contracts_b, actual_vol_b,
            row.spread, row.structural_spread, deviation, break_even_pct);

        // Place both legs in parallel
        let exa = row.exchange_a.clone(); let exb = row.exchange_b.clone(); let pair = row.pair.clone();
        let (oa_result, ob_result) = tokio::join!(
            place_order(&http, &keys, &exa, &pair, is_a_higher, contracts_a, dry_run, false),
            place_order(&http, &keys, &exb, &pair, !is_a_higher, contracts_b, dry_run, false)
        );

        let oid_a = match oa_result {
            Ok(id) => id,
            Err(e) => {
                let err_str = e.to_string();
                error!("[real] LEG_A failed {}/{}: {}", exa, pair, err_str);
                if err_str.contains("insufficient") || err_str.contains("exceeds the balance") || err_str.contains("minimum amount") {
                    ex_cd.insert(exa.clone(), now);
                    error!("[real] Exchange {} cooled down for 10 min", exa);
                }
                pair_cd.insert(pair.clone(), now);
                // Emergency close B if it succeeded
                if let Ok(_) = &ob_result {
                    let (hc, kc, ec, pc) = (http.clone(), Arc::clone(&keys), exb.clone(), pair.clone());
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        let _ = close_order(&hc, &kc, &ec, &pc, !is_a_higher, contracts_b, false).await;
                    });
                }
                // Record failed attempt in DB for history visibility
                let reason = format!("LEG_A_FAILED: {}", &err_str[..err_str.len().min(120)]);
                let db_c = db.clone();
                let (p, ea, eb, d, sp, pid) = (pair.clone(), exa.clone(), exb.clone(), row.direction.clone(), row.spread, settings.id.clone());
                let entry_time = now_ms();
                tokio::spawn(async move {
                    let _ = db_c.call(move |conn| {
                        conn.execute(
                            "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,highest_spread_reached,status,pnl_a,pnl_b,profile_id,commission_cost,slippage_cost,funding_cost,close_reason,pnl,gross_pnl) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            params![p, ea, eb, d, 0.0f64, 0.0f64, 0.0f64, sp, entry_time, sp, "CLOSED", 0.0f64, 0.0f64, pid, 0.0f64, 0.0f64, 0.0f64, reason, 0.0f64, 0.0f64]
                        )?;
                        Ok(())
                    }).await;
                });
                continue;
            }
        };
        let oid_b = match ob_result {
            Ok(id) => id,
            Err(e) => {
                let err_str = e.to_string();
                error!("[real] LEG_B failed {}/{}: {} — emergency close A", exb, pair, err_str);
                if err_str.contains("insufficient") || err_str.contains("exceeds the balance") || err_str.contains("minimum amount") {
                    ex_cd.insert(exb.clone(), now);
                    error!("[real] Exchange {} cooled down for 10 min", exb);
                }
                pair_cd.insert(pair.clone(), now);
                let (hc, kc, ec, pc) = (http.clone(), Arc::clone(&keys), exa.clone(), pair.clone());
                tokio::spawn(async move {
                    for attempt in 1u32..=5 {
                        match close_order(&hc, &kc, &ec, &pc, is_a_higher, contracts_a, false).await {
                            Ok(id) => { error!("🛡️ Emergency close A ok: {}", id); break; }
                            Err(err) => { let w = 200 * 2u64.pow(attempt-1); tokio::time::sleep(Duration::from_millis(w)).await; error!("Emergency close A {}/5: {}", attempt, err); }
                        }
                    }
                });
                // Record failed attempt in DB for history visibility
                let reason = format!("LEG_B_FAILED: {}", &err_str[..err_str.len().min(120)]);
                let db_c = db.clone();
                let (p, ea, eb, d, sp, pid) = (pair.clone(), exa.clone(), exb.clone(), row.direction.clone(), row.spread, settings.id.clone());
                let entry_time = now_ms();
                tokio::spawn(async move {
                    let _ = db_c.call(move |conn| {
                        conn.execute(
                            "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,highest_spread_reached,status,pnl_a,pnl_b,profile_id,commission_cost,slippage_cost,funding_cost,close_reason,pnl,gross_pnl) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            params![p, ea, eb, d, 0.0f64, 0.0f64, 0.0f64, sp, entry_time, sp, "CLOSED", 0.0f64, 0.0f64, pid, 0.0f64, 0.0f64, 0.0f64, reason, 0.0f64, 0.0f64]
                        )?;
                        Ok(())
                    }).await;
                });
                continue;
            }
        };

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
            tokio::time::sleep(Duration::from_millis(50)).await;
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

        let mut slip_cost_a = 0.0;
        if fill_a > 0.0 {
            if is_a_higher { slip_cost_a = (entry_a - fill_a) * (vol / entry_a); }
            else           { slip_cost_a = (fill_a - entry_a) * (vol / entry_a); }
        }
        let mut slip_cost_b = 0.0;
        if fill_b > 0.0 {
            if !is_a_higher { slip_cost_b = (entry_b - fill_b) * (vol / entry_b); }
            else            { slip_cost_b = (fill_b - entry_b) * (vol / entry_b); }
        }
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
                            tokio::time::sleep(Duration::from_millis(50)).await;
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


