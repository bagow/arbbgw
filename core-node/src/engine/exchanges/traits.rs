
use reqwest::Client;
use async_trait::async_trait;
use crate::engine::real::ExchangeKeys;

/// Info about a single open position on an exchange.
/// Returned by fetch_positions() — used to verify that trades actually exist on the exchange.
#[derive(Debug, Clone)]
pub struct PositionInfo {
    /// Standardised symbol, e.g. "BTCUSDT"
    pub symbol: String,
    /// Signed size: >0 = long, <0 = short, 0 = no position
    pub size: f64,
    /// Average entry price (from exchange)
    pub entry_price: f64,
    /// Unrealised PnL reported by the exchange
    pub unrealized_pnl: f64,
}

#[async_trait]
pub trait ExchangeConnector: Send + Sync {
    /// Returns the exchange name
    fn name(&self) -> &'static str;

    /// Connects to the exchange and opens a new order.
    /// Returns (order_id, limit_price)
    async fn place_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: f64, dry_run: bool, price: f64, use_ioc: bool) -> Result<(Option<String>, f64), String>;

    /// Connects to the exchange and closes an existing position.
    async fn close_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: f64, dry_run: bool, leverage: f64, price: f64, use_ioc: bool) -> Result<String, String>;

    /// Fetches the order details. Returns (executed_price, absolute_fee)
    async fn fetch_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, order_id: &str, multiplier: f64) -> Result<(f64, f64), String>;

    /// Fetches available balance in USD(T).
    async fn fetch_available_balance(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String>;

    /// Fetches taker fee rate. (e.g. 0.0005)
    async fn fetch_taker_fee(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String>;

    /// Ensures leverage and margin mode are correctly set.
    async fn ensure_leverage(&self, http: &Client, keys: &ExchangeKeys, pair: &str, leverage: f64) -> Result<(), String>;

    /// Fetches funding rate and next timestamp. Returns (rate, next_timestamp_ms)
    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String>;

    /// Fetch all open positions with non-zero size.
    /// One API call returns ALL positions — efficient for checking multiple trades.
    async fn fetch_positions(&self, http: &Client, keys: &ExchangeKeys) -> Result<Vec<PositionInfo>, String>;
}
