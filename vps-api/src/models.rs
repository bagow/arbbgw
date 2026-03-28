use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerData {
    pub bid: f64,
    pub ask: f64,
    pub price: f64,
    pub vol: f64,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SpreadRow {
    pub pair: String,
    #[serde(rename = "exchangeA")]
    pub exchange_a: String,
    #[serde(rename = "exchangeB")]
    pub exchange_b: String,
    #[serde(rename = "priceA")]
    pub price_a: f64,
    #[serde(rename = "priceB")]
    pub price_b: f64,
    #[serde(rename = "bidA")]
    pub bid_a: f64,
    #[serde(rename = "askA")]
    pub ask_a: f64,
    #[serde(rename = "bidB")]
    pub bid_b: f64,
    #[serde(rename = "askB")]
    pub ask_b: f64,
    #[serde(rename = "volA", default)]
    pub vol_a: f64,
    #[serde(rename = "volB", default)]
    pub vol_b: f64,
    #[serde(rename = "fundingA", default)]
    pub funding_a: f64,
    #[serde(rename = "fundingB", default)]
    pub funding_b: f64,
    pub spread: f64,
    pub direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadPayload {
    pub spreads: Vec<SpreadRow>,
    pub maps: std::collections::HashMap<String, std::collections::HashMap<String, TickerData>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingProfile {
    pub id: String,
    pub name: String,
    pub mode: String,
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
    pub owner: Option<String>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: i64,
    pub pair: String,
    pub exchange_a: String,
    pub exchange_b: String,
    pub direction: String,
    pub volume: f64,
    pub entry_price_a: f64,
    pub entry_price_b: f64,
    pub entry_spread: f64,
    pub entry_time: i64,
    pub highest_spread_reached: f64,
    pub status: String,
    pub exit_price_a: Option<f64>,
    pub exit_price_b: Option<f64>,
    pub exit_spread: Option<f64>,
    pub exit_time: Option<i64>,
    pub pnl: Option<f64>,
    pub commission_cost: Option<f64>,
    pub funding_cost: Option<f64>,
    pub last_price_a: Option<f64>,
    pub last_price_b: Option<f64>,
    pub last_spread: Option<f64>,
    pub curr_gross_pnl: Option<f64>,
    pub pnl_a: Option<f64>,
    pub pnl_b: Option<f64>,
    pub close_reason: Option<String>,
    pub profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_price_a: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_price_b: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_spread: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_pnl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gross_pnl: Option<f64>,
}
