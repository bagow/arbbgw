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
