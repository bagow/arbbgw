use crate::engine::real::ExchangeKeys;
use async_trait::async_trait;
use reqwest::Client;

use serde::{Deserialize, Serialize};

/// Receipt returned after confirming a successful position closure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloseReceipt {
    pub final_price: f64,
    pub total_fee: f64,
    pub closed_volume: f64,
}

/// Info about a single open position on an exchange.
/// Returned by fetch_positions() — used to verify that trades actually exist on the exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionInfo {
    /// Standardised symbol, e.g. "BTCUSDT"
    pub symbol: String,
    /// Signed size: >0 = long, <0 = short, 0 = no position
    pub size: f64,
    /// Average entry price (from exchange)
    pub entry_price: f64,
    /// Unrealised PnL reported by the exchange
    pub unrealized_pnl: f64,
    /// Realised PnL reported by the exchange (if available)
    pub realized_pnl: f64,
    /// Accumulated fee for this position so far
    pub accumulated_fee: f64,
    /// Accumulated funding for this position so far
    pub accumulated_funding: f64,
}

#[async_trait]
pub trait ExchangeConnector: Send + Sync {
    /// Returns the exchange name
    fn name(&self) -> &'static str;

    /// Connects to the exchange and opens a new order.
    /// The connector itself computes the final limit price using `target_price` and `slippage_percent`.
    /// Returns (order_id, executed_price, executed_fee)
    async fn place_order(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        is_buy: bool,
        phys_amount: f64,
        dry_run: bool,
        target_price: f64,
        use_ioc: bool,
        leverage: f64,
        multiplier: f64,
        slippage_percent: f64,
        tick_size: f64,
        api_timeout_ms: u64,
    ) -> Result<(Option<String>, f64, f64), String>;

    /// Connects to the exchange and closes an existing position smartly.
    /// The connector itself handles the delay, verification, and fallback market logic.
    async fn close_position(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        is_buy: bool,
        phys_amount: f64,
        dry_run: bool,
        leverage: f64,
        target_price: f64,
        use_ioc: bool,
        slippage_percent: f64,
        multiplier: f64,
        tick_size: f64,
        retries: u32,
        delay_ms: u64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String>;

    /// Adjusts position to drop excess volume (trimming). Returns the loss and fee incurred.
    async fn trim_position(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        is_buy: bool,
        phys_excess: f64,
        dry_run: bool,
        multiplier: f64,
        tick_size: f64,
        fallback_price: f64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String>;

    /// Fetches the order details. Returns (executed_price, absolute_fee, executed_physical_qty)
    async fn fetch_order(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        order_id: &str,
        multiplier: f64,
    ) -> Result<(f64, f64, f64), String>;

    /// Fetches balances in USD(T). Returns (wallet_balance, available_margin).
    async fn fetch_balances(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<(f64, f64), String>;

    /// Fetches taker fee rate. (e.g. 0.0005)
    async fn fetch_taker_fee(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String>;

    /// Ensures leverage and margin mode are correctly set.
    async fn ensure_leverage(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        leverage: f64,
    ) -> Result<(), String>;

    /// Fetches funding rate and next timestamp. Returns (rate, next_timestamp_ms)
    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String>;

    /// Fetch all open positions with non-zero size, including accumulated metrics if available.
    async fn fetch_positions(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<Vec<PositionInfo>, String>;

    /// Fetches the orderbook (depth) for a given pair.
    /// Returns (bids, asks) where each element is (price, quantity).
    async fn fetch_orderbook(
        &self,
        http: &Client,
        pair: &str,
        depth: u32,
    ) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), String>;
}
