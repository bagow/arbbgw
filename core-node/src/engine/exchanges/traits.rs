use std::sync::Arc;
use reqwest::Client;
use async_trait::async_trait;
use crate::engine::real::ExchangeKeys;

#[async_trait]
pub trait ExchangeConnector: Send + Sync {
    /// Returns the exchange name
    fn name(&self) -> &'static str;

    /// Connects to the exchange and opens a new order.
    /// Returns (order_id, limit_price)
    async fn place_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: i64, dry_run: bool, price: f64, use_ioc: bool) -> Result<(Option<String>, f64), String>;

    /// Connects to the exchange and closes an existing position.
    async fn close_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, is_buy: bool, contracts: i64, dry_run: bool, leverage: f64) -> Result<String, String>;

    /// Fetches the order details. Returns (executed_price, absolute_fee)
    async fn fetch_order(&self, http: &Client, keys: &ExchangeKeys, pair: &str, order_id: &str) -> Result<(f64, f64), String>;

    /// Fetches available balance in USD(T).
    async fn fetch_available_balance(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String>;

    /// Fetches taker fee rate. (e.g. 0.0005)
    async fn fetch_taker_fee(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String>;

    /// Ensures leverage and margin mode are correctly set.
    async fn ensure_leverage(&self, http: &Client, keys: &ExchangeKeys, pair: &str, leverage: f64) -> Result<(), String>;

    /// Fetches funding rate and next timestamp. Returns (rate, next_timestamp_ms)
    async fn get_funding_rate(&self, http: &Client, pair: &str) -> Result<(f64, i64), String>;
}
