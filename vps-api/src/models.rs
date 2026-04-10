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
    pub maps: std::collections::HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
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
    pub max_active_trades: i32,
    pub commission_rate: f64,
    pub slippage_percent: f64,
    pub funding_rate_8h: f64,
    pub min_volume_millions: f64,
    pub collector_interval_secs: i64,
    pub owner: Option<String>,
    pub pending_commands: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub disabled_exchanges: Option<String>,
    pub leverage: i64,
    pub margin_mode: String,
}

impl Default for TradingProfile {
    fn default() -> Self {
        Self {
            id: "".to_string(),
            name: "New Profile".to_string(),
            mode: "real".to_string(),
            is_active: false,
            min_entry_spread: 1.0,
            max_entry_spread: 10.0,
            trade_volume: 1000.0,
            take_profit_percent: 0.1,
            trailing_stop_percent: 0.5,
            max_active_trades: 5,
            commission_rate: 0.45,
            slippage_percent: 0.1,
            funding_rate_8h: 0.01,
            min_volume_millions: 10.0,
            collector_interval_secs: 5,
            owner: None,
            pending_commands: None,
            created_at: 0,
            updated_at: 0,
            disabled_exchanges: Some("[]".to_string()),
            leverage: 10,
            margin_mode: "cross".to_string(),
        }
    }
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
    pub slippage_cost: Option<f64>,
    pub last_price_a: Option<f64>,
    pub last_price_b: Option<f64>,
    pub last_spread: Option<f64>,
    pub curr_gross_pnl: Option<f64>,
    pub pnl_a: Option<f64>,
    pub pnl_b: Option<f64>,
    pub close_reason: Option<String>,
    pub profile_id: Option<String>,
    pub contracts_a: Option<i64>,
    pub contracts_b: Option<i64>,
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
    pub funding_rate_a: Option<f64>,
    pub funding_rate_b: Option<f64>,
    pub funding_next_ts: Option<i64>,
}
