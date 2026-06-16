//! Real trading engine for core-node.
//!
//! run_cycle() is called directly by the engine loop (no HTTP).
//! Order placement, Leg Fallback, and Partial Fill Guard are all here.
//! Exchange signing is done inline following the same pattern as real-trader.

use base64::Engine as _;
use hmac::{Hmac, Mac};
use reqwest::Client;
use std::{sync::Arc, time::Duration};

use serde_json::Value;
use sha2::Sha256;
use tokio_rusqlite::Connection;
use tracing::{error, info};

type HmacSha256 = Hmac<Sha256>;

// ─── Exchange keys ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ExchangeKeys {
    pub gate_key: String,
    pub gate_secret: String,
    pub kucoin_key: String,
    pub kucoin_secret: String,
    pub kucoin_passphrase: String,
    pub binance_key: String,
    pub binance_secret: String,
    pub bybit_key: String,
    pub bybit_secret: String,
    pub bitget_key: String,
    pub bitget_secret: String,
    pub bitget_passphrase: String,
    // New exchanges
    pub bingx_key: String,
    pub bingx_secret: String,
    pub bitmex_key: String,
    pub bitmex_secret: String,
    pub ascendex_key: String,
    pub ascendex_secret: String,
    pub xt_key: String,
    pub xt_secret: String,
    pub toobit_key: String,
    pub toobit_secret: String,
}

impl ExchangeKeys {
    pub fn from_env() -> Self {
        Self {
            gate_key: std::env::var("GATE_KEY").unwrap_or_default(),
            gate_secret: std::env::var("GATE_SECRET").unwrap_or_default(),
            kucoin_key: std::env::var("KUCOIN_KEY").unwrap_or_default(),
            kucoin_secret: std::env::var("KUCOIN_SECRET").unwrap_or_default(),
            kucoin_passphrase: std::env::var("KUCOIN_PASSPHRASE").unwrap_or_default(),
            binance_key: std::env::var("BINANCE_KEY").unwrap_or_default(),
            binance_secret: std::env::var("BINANCE_SECRET").unwrap_or_default(),
            bybit_key: std::env::var("BYBIT_KEY").unwrap_or_default(),
            bybit_secret: std::env::var("BYBIT_SECRET").unwrap_or_default(),
            bitget_key: std::env::var("BITGET_KEY").unwrap_or_default(),
            bitget_secret: std::env::var("BITGET_SECRET").unwrap_or_default(),
            bitget_passphrase: std::env::var("BITGET_PASSPHRASE").unwrap_or_default(),
            bingx_key: std::env::var("BINGX_KEY").unwrap_or_default(),
            bingx_secret: std::env::var("BINGX_SECRET").unwrap_or_default(),
            bitmex_key: std::env::var("BITMEX_KEY").unwrap_or_default(),
            bitmex_secret: std::env::var("BITMEX_SECRET").unwrap_or_default(),
            ascendex_key: std::env::var("ASCENDEX_KEY").unwrap_or_default(),
            ascendex_secret: std::env::var("ASCENDEX_SECRET").unwrap_or_default(),
            xt_key: std::env::var("XT_KEY").unwrap_or_default(),
            xt_secret: std::env::var("XT_SECRET").unwrap_or_default(),
            toobit_key: std::env::var("TOOBIT_KEY").unwrap_or_default(),
            toobit_secret: std::env::var("TOOBIT_SECRET").unwrap_or_default(),
        }
    }
    pub fn supports(&self, ex: &str) -> bool {
        match ex {
            "gate" => !self.gate_key.is_empty(),
            "kucoin" => !self.kucoin_key.is_empty(),
            "binance" => !self.binance_key.is_empty(),
            "bybit" => !self.bybit_key.is_empty(),
            "bitget" => !self.bitget_key.is_empty(),
            "bingx" => !self.bingx_key.is_empty(),
            "bitmex" => !self.bitmex_key.is_empty(),
            "ascendex" => !self.ascendex_key.is_empty(),
            "xt" => !self.xt_key.is_empty(),
            "toobit" => !self.toobit_key.is_empty(),
            _ => false,
        }
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}



pub fn normalize_symbol(sym: &str) -> String {
    let mut s = sym.to_uppercase();
    s = s.replace(['-', '_', '/'], "");
    if s == "XBTUSDTM" {
        return "BTCUSDT".to_string();
    }
    if s == "XBTUSDT" {
        return "BTCUSDT".to_string();
    }
    if s.ends_with("USDTM") {
        s = s.trim_end_matches('M').to_string();
    }
    s
}

pub fn symbols_match(a: &str, b: &str) -> bool {
    normalize_symbol(a) == normalize_symbol(b)
}

/// Round price to the nearest valid tick size for the given exchange:pair.
/// If tick size is unknown, rounds to 8 decimal places (safe fallback).
pub fn round_to_tick(
    price: f64,
    exchange: &str,
    pair: &str,
    e_ctx: &crate::state::EngineContext,
) -> f64 {
    let key = format!("{}:{}", exchange, pair);
    let tick = e_ctx.tick_sizes.get(&key).map(|v| *v).unwrap_or(0.0);
    if tick > 0.0 {
        (price / tick).round() * tick
    } else {
        // Fallback: round based on price magnitude
        if price > 1000.0 {
            (price * 100.0).round() / 100.0
        }
        // 0.01
        else if price > 10.0 {
            (price * 1000.0).round() / 1000.0
        }
        // 0.001
        else if price > 0.1 {
            (price * 10000.0).round() / 10000.0
        }
        // 0.0001
        else {
            (price * 100000.0).round() / 100000.0
        } // 0.00001
    }
}

/// Get cached taker fee rate for an exchange (as fraction, e.g. 0.0005 = 0.05%).
pub fn get_exchange_fee(
    exchange: &str,
    default_fee: f64,
    e_ctx: &crate::state::EngineContext,
) -> f64 {
    e_ctx
        .fee_rates
        .get(exchange)
        .map(|v| *v)
        .unwrap_or(default_fee)
}

// Restored from market_data
pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn hmac_sha256_hex(secret: &[u8], data: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC any key");
    mac.update(data.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
fn hmac_sha256_b64(secret: &[u8], data: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC any key");
    mac.update(data.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}
pub fn kucoin_sign(
    secret: &str,
    passphrase: &str,
    ts: &str,
    method: &str,
    path: &str,
    body: &str,
) -> (String, String) {
    (
        hmac_sha256_b64(
            secret.as_bytes(),
            &format!("{}{}{}{}", ts, method, path, body),
        ),
        hmac_sha256_b64(secret.as_bytes(), passphrase),
    )
}
pub fn gate_sign(
    key: &str,
    secret: &str,
    method: &str,
    path: &str,
    body: &str,
) -> reqwest::header::HeaderMap {
    use sha2::Digest;
    let ts = now_secs().to_string();
    let body_hash = hex::encode(sha2::Sha512::digest(body.as_bytes()));
    let sign_str = format!("{}\n{}\n\n{}\n{}", method, path, body_hash, ts);
    let sig = {
        let mut mac = Hmac::<sha2::Sha512>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(sign_str.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    };
    let mut h = reqwest::header::HeaderMap::new();
    h.insert("KEY", key.parse().unwrap());
    h.insert("SIGN", sig.parse().unwrap());
    h.insert("Timestamp", ts.parse().unwrap());
    h.insert("Content-Type", "application/json".parse().unwrap());
    h
}
pub fn binance_sign(secret: &str, qs: &str) -> String {
    hmac_sha256_hex(secret.as_bytes(), qs)
}
pub fn bybit_sign(secret: &str, ts: &str, api_key: &str, recv_window: &str, body: &str) -> String {
    hmac_sha256_hex(
        secret.as_bytes(),
        &format!("{}{}{}{}", ts, api_key, recv_window, body),
    )
}

#[derive(Debug, Clone)]
pub struct Trade {
    pub id: i64,
    pub pair: String,
    pub status: String,
    pub exchange_a: String,
    pub exchange_b: String,
    pub direction: String,
    pub volume: f64,
    pub entry_price_a: f64,
    pub entry_price_b: f64,
    pub entry_spread: f64,
    pub entry_time: i64,
    pub highest_spread: f64,
    pub pnl_a: f64,
    pub pnl_b: f64,
    pub close_reason: Option<String>,
    pub contracts_a: f64,
    pub contracts_b: f64,
    pub commission_cost: f64,
    pub slippage_cost: f64,
    /// Accumulated real funding cost since entry (updated every live-update cycle from exchange API).
    /// Positive = we net-paid funding; negative = we net-received funding.
    pub funding_cost: f64,
}

pub async fn load_open_trades(db: &Connection, p_id: &str) -> Vec<Trade> {
    let p_id_clone = p_id.to_string();
    db.call(move |conn| {
        let mut stmt = conn.prepare(
            "SELECT id, pair, status, exchange_a, exchange_b, direction, volume, \
             entry_price_a, entry_price_b, entry_spread, entry_time, highest_spread_reached, \
             pnl_a, pnl_b, close_reason, contracts_a, contracts_b, commission_cost, slippage_cost, \
             COALESCE(funding_cost, 0.0) \
             FROM trades WHERE status IN ('OPEN','CLOSING') AND profile_id=?",
        )?;
        let trades = stmt
            .query_map([p_id_clone], |row| {
                Ok(Trade {
                    id: row.get(0)?,
                    pair: row.get(1)?,
                    status: row.get(2)?,
                    exchange_a: row.get(3)?,
                    exchange_b: row.get(4)?,
                    direction: row.get(5)?,
                    volume: row.get(6)?,
                    entry_price_a: row.get(7)?,
                    entry_price_b: row.get(8)?,
                    entry_spread: row.get(9)?,
                    entry_time: row.get(10)?,
                    highest_spread: row.get(11)?,
                    pnl_a: row.get(12)?,
                    pnl_b: row.get(13)?,
                    close_reason: row.get(14)?,
                    contracts_a: row.get::<_, Option<f64>>(15)?.unwrap_or(
                        (row.get::<_, f64>(6)? / row.get::<_, f64>(7)?.max(1e-12)).round(),
                    ),
                    contracts_b: row.get::<_, Option<f64>>(16)?.unwrap_or(
                        (row.get::<_, f64>(6)? / row.get::<_, f64>(8)?.max(1e-12)).round(),
                    ),
                    commission_cost: row.get::<_, Option<f64>>(17)?.unwrap_or(0.0),
                    slippage_cost: row.get::<_, Option<f64>>(18)?.unwrap_or(0.0),
                    funding_cost: row.get::<_, Option<f64>>(19)?.unwrap_or(0.0),
                })
            })?
            .filter_map(|t| t.ok())
            .collect::<Vec<_>>();
        Ok(trades)
    })
    .await
    .unwrap_or_default()
}

/// Asynchronously preemptively initialize leverage for spreads that are nearing their entry criteria.
/// If a spread is >= 80% to its target, ensure leverage so there is 0ms delay during trade time.
pub fn warmup_leverages(
    spreads: &[shared::SpreadRow],
    profiles: &[shared::TradingSettings],
    http: Client,
    keys: Arc<ExchangeKeys>,
) {
    let mut pairs_to_warm = Vec::new();

    // Find pairs that are close to breaking entry threshold
    for profile in profiles {
        if !profile.is_active {
            continue;
        }
        for row in spreads.iter().take(50) {
            // A heuristic: if spread is at least 80% of config min_entry_spread we warm it up
            if row.spread >= profile.min_entry_spread * 0.8 {
                pairs_to_warm.push((
                    row.exchange_a.clone(),
                    row.exchange_b.clone(),
                    row.pair.clone(),
                    profile.leverage as f64,
                    profile.margin_mode.clone(),
                ));
            }
        }
    }

    if pairs_to_warm.is_empty() {
        return;
    }

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

