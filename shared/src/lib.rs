//! shared — common types used across all services
//!
//! This crate is the single source of truth for data structures that are
//! shared between rust-collector, real-trader, paper-trader and vps-api.
//!
//! When building core-node, all services import from here instead of
//! duplicating struct definitions.

use serde::{Deserialize, Serialize};

// ─── Market data ──────────────────────────────────────────────────────────────

/// Best bid/ask, mid-price, 24h volume (USD) and current funding rate for a
/// single perpetual contract on one exchange.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TickerData {
    pub bid: f64,
    pub ask: f64,
    pub price: f64,
    pub vol: f64,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: f64,
    /// Unix seconds of last WebSocket update (set by collector, not serialised).
    #[serde(skip)]
    pub last_updated: u64,
}

/// One spread opportunity: a single pair available on two exchanges where the
/// price diverges enough to be interesting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadRow {
    pub pair: String,
    #[serde(rename = "exchangeA")]  pub exchange_a: String,
    #[serde(rename = "exchangeB")]  pub exchange_b: String,
    #[serde(rename = "priceA")]     pub price_a: f64,
    #[serde(rename = "priceB")]     pub price_b: f64,
    #[serde(rename = "bidA")]       pub bid_a: f64,
    #[serde(rename = "askA")]       pub ask_a: f64,
    #[serde(rename = "bidB")]       pub bid_b: f64,
    #[serde(rename = "askB")]       pub ask_b: f64,
    #[serde(rename = "volA", default)] pub vol_a: f64,
    #[serde(rename = "volB", default)] pub vol_b: f64,
    #[serde(rename = "fundingA", default)] pub funding_a: f64,
    #[serde(rename = "fundingB", default)] pub funding_b: f64,
    /// Structural mean spread over the last 24h (Time-decay EMA).
    pub structural_spread: f64,
    /// Spread in percent (positive = buy B / sell A is profitable before costs).
    pub spread: f64,
    /// E.g. "gate>kucoin" — the exchange with the higher bid comes first.
    pub direction: String,
}

/// Payload sent from rust-collector to traders via HTTP POST /internal/spreads.
/// In core-node, this is replaced by a direct in-memory reference to the
/// PriceMap (Arc<DashMap>), but the HTTP form is preserved for compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadPayload {
    pub spreads: Vec<SpreadRow>,
    /// Full ticker snapshot: exchange → symbol → TickerData.
    pub maps: std::collections::HashMap<String, std::collections::HashMap<String, TickerData>>,
}

// ─── Trading profile / settings ───────────────────────────────────────────────

/// Active settings for one trading profile, mirroring the `trading_profiles`
/// SQLite table row.  Both paper-trader and real-trader use this struct.
///
/// Column order in DB:
///   0=id  1=name  2=mode  3=is_active  4=min_entry_spread  5=max_entry_spread
///   6=trade_volume  7=take_profit_percent  8=trailing_stop_percent
///   9=max_active_trades  10=commission_rate  11=slippage_percent
///   12=funding_rate_8h  13=min_volume_millions  14=collector_interval_secs
///   15=pending_commands  …  19=disabled_exchanges
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingSettings {
    pub id: String,
    pub is_active: bool,
    pub min_entry_spread: f64,
    pub max_entry_spread: f64,
    pub trade_volume: f64,
    pub take_profit_percent: f64,
    pub trailing_stop_percent: f64,
    pub max_active_trades: i64,
    pub commission_rate: f64,
    pub slippage_percent: f64,
    pub funding_rate_8h: f64,
    pub min_volume_millions: f64,
    pub collector_interval_secs: i64,
    pub pending_commands: Option<String>,
    pub disabled_exchanges: Vec<String>,
}

impl Default for TradingSettings {
    fn default() -> Self {
        Self {
            id: "default".to_string(),
            is_active: false,
            min_entry_spread: 0.5,
            max_entry_spread: 10.0,
            trade_volume: 100.0,
            take_profit_percent: 0.2,
            trailing_stop_percent: 0.3,
            max_active_trades: 3,
            commission_rate: 0.05,
            slippage_percent: 0.05,
            funding_rate_8h: 0.01,
            min_volume_millions: 10.0,
            collector_interval_secs: 5,
            pending_commands: None,
            disabled_exchanges: vec![],
        }
    }
}

impl TradingSettings {
    /// Minimum spread (as fraction, not %) needed to cover all round-trip costs.
    /// Uses realistic exchange taker fee (0.05%) — NOT user-configured commission_rate,
    /// since real engine reads actual fees from exchange API.
    /// Slippage is still from settings since it varies by market conditions.
    ///
    /// Formula: 4 × (taker_fee + slippage) — both occur on all 4 legs.
    pub fn break_even_fraction(&self) -> f64 {
        const TAKER_FEE: f64 = 0.05 / 100.0; // 0.05% — typical CEX futures taker rate
        4.0 * (TAKER_FEE + self.slippage_percent / 100.0)
    }
}

// ─── Order result ─────────────────────────────────────────────────────────────

/// Result of placing one order leg on an exchange.
#[derive(Debug, Clone)]
pub struct OrderResult {
    pub order_id: String,
    /// Actually filled quantity in USD (may differ from requested if partial fill).
    pub filled_usd: f64,
    /// "FILLED" | "PARTIAL" | "CANCELLED"
    pub status: String,
}
