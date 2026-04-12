//! Real trading engine for core-node.
//!
//! run_cycle() is called directly by the engine loop (no HTTP).
//! Order placement, Leg Fallback, and Partial Fill Guard are all here.
//! Exchange signing is done inline following the same pattern as real-trader.

use std::{sync::Arc, time::Duration};
use base64::Engine as _;
use hmac::{Hmac, Mac};
use reqwest::Client;

use serde_json::Value;
use sha2::Sha256;
use tokio_rusqlite::Connection;
use tracing::{error, info};



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
        tracing::warn!("[balance] {} marked balance_low — new trades on this exchange are blocked until balance recovers", exchange);
        e_ctx.balance_low.insert(exchange.to_string(), true);
    }
}
/// Background task: detect positions closed externally on exchanges.
/// Every 10 seconds, checks all open trades against actual exchange positions.
/// If a position is gone from the exchange but still OPEN in DB, handle it.
pub fn start_position_sync(
    http: Client,
    keys: Arc<ExchangeKeys>,
    db: Connection,
    e_ctx: Arc<crate::state::EngineContext>,
) {
    tokio::spawn(async move {
        // Wait for initial startup to settle
        tokio::time::sleep(Duration::from_secs(30)).await;

        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            // 1. Load ALL open trades (across all profiles)
            let open_trades: Vec<Trade> = db.call(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT id, pair, exchange_a, exchange_b, direction, volume, \
                     entry_price_a, entry_price_b, entry_spread, entry_time, highest_spread_reached, \
                     pnl_a, pnl_b, close_reason, contracts_a, contracts_b, commission_cost, slippage_cost, \
                     COALESCE(funding_cost, 0.0) \
                     FROM trades WHERE status='OPEN'"
                )?;
                let trades = stmt.query_map([], |row| Ok(Trade {
                    id: row.get(0)?, pair: row.get(1)?, exchange_a: row.get(2)?, exchange_b: row.get(3)?,
                    direction: row.get(4)?, volume: row.get(5)?, entry_price_a: row.get(6)?, entry_price_b: row.get(7)?,
                    entry_spread: row.get(8)?, entry_time: row.get(9)?, highest_spread: row.get(10)?,
                    pnl_a: row.get(11)?, pnl_b: row.get(12)?, close_reason: row.get(13)?,
                    contracts_a: row.get::<_, Option<f64>>(14)?.unwrap_or(0.0),
                    contracts_b: row.get::<_, Option<f64>>(15)?.unwrap_or(0.0),
                    commission_cost: row.get::<_, Option<f64>>(16)?.unwrap_or(0.0),
                    slippage_cost: row.get::<_, Option<f64>>(17)?.unwrap_or(0.0),
                    funding_cost: row.get::<_, Option<f64>>(18)?.unwrap_or(0.0),
                }))?.filter_map(|t| t.ok()).collect::<Vec<_>>();
                Ok(trades)
            }).await.unwrap_or_default();

            if open_trades.is_empty() { continue; }

            // 2. Collect which exchanges have open trades
            let mut exchanges_in_use = std::collections::HashSet::new();
            for t in &open_trades {
                exchanges_in_use.insert(t.exchange_a.clone());
                exchanges_in_use.insert(t.exchange_b.clone());
            }

            // 3. Fetch positions from each exchange (1 API call per exchange)
            let mut all_positions: std::collections::HashMap<String, Vec<super::exchanges::traits::PositionInfo>> = std::collections::HashMap::new();
            for ex in &exchanges_in_use {
                if !keys.supports(ex) { continue; }
                if let Some(conn) = super::exchanges::get_connector(ex) {
                    match conn.fetch_positions(&http, &keys).await {
                        Ok(positions) => { all_positions.insert(ex.clone(), positions); }
                        Err(e) => { tracing::warn!("[pos_sync] Failed to fetch positions from {}: {}", ex, e); }
                    }
                }
            }

            // 4. Check each open trade against exchange positions
            for trade in &open_trades {
                let has_pos_a = all_positions.get(&trade.exchange_a)
                    .map(|ps| ps.iter().any(|p| p.symbol == trade.pair && p.size.abs() > 1e-12))
                    .unwrap_or(true); // conservative: if can't check, assume exists
                let has_pos_b = all_positions.get(&trade.exchange_b)
                    .map(|ps| ps.iter().any(|p| p.symbol == trade.pair && p.size.abs() > 1e-12))
                    .unwrap_or(true); // conservative

                if has_pos_a && has_pos_b {
                    continue; // Both positions exist, all normal
                }

                if !has_pos_a && !has_pos_b {
                    // BOTH positions gone externally
                    error!("[pos_sync] Trade id={} {} — BOTH positions gone! Marking EXTERNAL_CLOSE", trade.id, trade.pair);
                    let id = trade.id;
                    let fund_cost = trade.funding_cost;
                    let _ = db.call(move |conn| {
                        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
                        conn.execute(
                            "UPDATE trades SET status='CLOSED', exit_time=?, close_reason='EXTERNAL_CLOSE', \
                             pnl=0.0, gross_pnl=0.0, curr_gross_pnl=0.0, pnl_a=0.0, pnl_b=0.0, funding_cost=? WHERE id=?",
                            rusqlite::params![now, fund_cost, id]
                        )?;
                        Ok(())
                    }).await;
                    continue;
                }

                // ONE leg is gone — emergency close the remaining leg
                let (missing_ex, remaining_ex) = if !has_pos_a {
                    (&trade.exchange_a, &trade.exchange_b)
                } else {
                    (&trade.exchange_b, &trade.exchange_a)
                };
                error!("[pos_sync] Trade id={} {} — {} gone, {} still open! Emergency closing",
                    trade.id, trade.pair, missing_ex, remaining_ex);

                let is_a_higher = trade.direction == "A>B";
                let remaining_is_a = remaining_ex == &trade.exchange_a;
                let close_is_buy = if remaining_is_a { is_a_higher } else { !is_a_higher };
                let contracts = if remaining_is_a { trade.contracts_a } else { trade.contracts_b };
                let mult = e_ctx.multipliers.get(&format!("{}:{}", remaining_ex, trade.pair)).map(|v| *v).unwrap_or(1.0);
                let q_qty = if remaining_ex == "binance" || remaining_ex == "bybit" { contracts * mult } else { contracts };

                if let Some(conn) = super::exchanges::get_connector(remaining_ex) {
                    match conn.close_order(&http, &keys, &trade.pair, close_is_buy, q_qty, false, 10.0, 0.0, false).await {
                        Ok(oid) => {
                            let mut close_fill = 0.0_f64;
                            let mut close_fee = 0.0_f64;
                            for _ in 0..5 {
                                if let Ok::<(f64, f64, f64), String>((p, f, _ev)) = conn.fetch_order(&http, &keys, &trade.pair, &oid, mult).await {
                                    if p > 0.0 { close_fill = p; close_fee = if f > 1e-9 { f.abs() } else { 0.0 }; break; }
                                }
                                tokio::time::sleep(Duration::from_millis(25)).await;
                            }
                            error!("[pos_sync] Emergency close done id={} {} on {} fill={:.6} fee={:.4}",
                                trade.id, trade.pair, remaining_ex, close_fill, close_fee);
                            let id = trade.id;
                            let fund_cost = trade.funding_cost;
                            let total_fee = trade.commission_cost + close_fee;
                            let _ = db.call(move |conn| {
                                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
                                conn.execute(
                                    "UPDATE trades SET status='CLOSED', exit_time=?, close_reason='EXTERNAL_PARTIAL_CLOSE', \
                                     pnl=0.0, gross_pnl=0.0, curr_gross_pnl=0.0, pnl_a=0.0, pnl_b=0.0, \
                                     commission_cost=?, funding_cost=? WHERE id=?",
                                    rusqlite::params![now, total_fee, fund_cost, id]
                                )?;
                                Ok(())
                            }).await;
                        }
                        Err(e) => {
                            error!("[pos_sync] Emergency close FAILED id={} {} on {}: {}",
                                trade.id, trade.pair, remaining_ex, e);
                        }
                    }
                }
            }
        }
    });
}

pub fn has_sufficient_margin(exchange: &str, notional_usd: f64, leverage: f64, e_ctx: &crate::state::EngineContext) -> bool {
    match e_ctx.margin_balances.get(exchange) {
        Some(margin) => {
            let available = *margin;
            // Physical margin needed for this trade
            let required = notional_usd / leverage.max(1.0);
            // Require 1.5x of the required margin as a safety buffer
            available >= required * 1.5
        }
        None => false, // STRICT MODE: Do not enter trades blindly if we don't know our balance
    }
}

/// Deduct notional from cached margin balance after a successful trade open.
/// Prevents the engine from over-committing margin between balance refresh cycles (25s).
pub fn deduct_cached_balance(exchange: &str, notional_usd: f64, leverage: f64, e_ctx: &crate::state::EngineContext) {
    let required_margin = notional_usd / leverage.max(1.0);
    if let Some(mut bal) = e_ctx.margin_balances.get_mut(exchange) {
        *bal -= required_margin;
    }
}

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
                let loaded = e_ctx.multipliers.iter().filter(|e| e.key().starts_with("gate:")).count();
                if loaded > 0 {
                    info!("[multipliers] Gate: loaded {} symbols", loaded);
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                } else {
                    tracing::warn!("[multipliers] Gate: FAILED to load, retrying in 30s");
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                }
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
                let loaded = e_ctx.multipliers.iter().filter(|e| e.key().starts_with("kucoin:")).count();
                if loaded > 0 {
                    info!("[multipliers] KuCoin: loaded {} symbols", loaded);
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                } else {
                    tracing::warn!("[multipliers] KuCoin: FAILED to load, retrying in 30s");
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                }
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
                                let ct = s["contractType"].as_str().unwrap_or("");
                                if ct != "PERPETUAL" && ct != "TRADIFI_PERPETUAL" { continue; }
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
                let loaded = e_ctx.multipliers.iter().filter(|e| e.key().starts_with("binance:")).count();
                if loaded > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                } else {
                    tracing::warn!("[multipliers] Binance: FAILED to load, retrying in 30s");
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                }
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
                let loaded = e_ctx.multipliers.iter().filter(|e| e.key().starts_with("bybit:")).count();
                if loaded > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                } else {
                    tracing::warn!("[multipliers] Bybit: FAILED to load, retrying in 30s");
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                }
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

/// Asynchronously preemptively initialize leverage for spreads that are nearing their entry criteria.
/// If a spread is >= 80% to its target, ensure leverage so there is 0ms delay during trade time.
pub fn warmup_leverages(
    spreads: &[shared::SpreadRow],
    profiles: &[shared::TradingSettings],
    http: Client,
    keys: Arc<ExchangeKeys>
) {
    let mut pairs_to_warm = Vec::new();

    // Find pairs that are close to breaking entry threshold
    for profile in profiles {
        if !profile.is_active { continue; }
        for row in spreads.iter().take(50) {
            // A heuristic: if spread is at least 80% of config min_entry_spread we warm it up
            if row.spread >= profile.min_entry_spread * 0.8 {
                pairs_to_warm.push((row.exchange_a.clone(), row.exchange_b.clone(), row.pair.clone(), profile.leverage as f64, profile.margin_mode.clone()));
            }
        }
    }

    if pairs_to_warm.is_empty() { return; }

    tokio::spawn(async move {
        for (exa, exb, pair, lev, _margin) in pairs_to_warm {
            if let Some(conn_a) = crate::engine::exchanges::get_connector(&exa) {
                let _ = conn_a.ensure_leverage(&http, &keys, &pair, lev).await; // Ignore result
            }
            if let Some(conn_b) = crate::engine::exchanges::get_connector(&exb) {
                let _ = conn_b.ensure_leverage(&http, &keys, &pair, lev).await; // Ignore result
            }
        }
    });
}


