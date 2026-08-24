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

/// Result of querying exchange trade/income history for auditing a completed or recovered trade.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TradeAuditResult {
    /// Total real commission paid across all fills in the time window (in USD/USDT)
    pub total_fee: f64,
    /// Average entry fill price (if detected)
    pub entry_price: Option<f64>,
    /// Average exit fill price (if detected)
    pub exit_price: Option<f64>,
    /// Realized PnL reported directly by the exchange (if available)
    pub realized_pnl: Option<f64>,
    /// Realized funding payments (if available)
    pub funding_cost: Option<f64>,
    /// Total filled volume in base asset
    pub filled_qty: f64,
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


    /// Fetch all open positions with non-zero size, including accumulated metrics if available.
    async fn fetch_positions(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<Vec<PositionInfo>, String>;

    // ── Maker mode methods (default: not supported) ─────────────────────────

    /// Place a Post-Only (Maker) limit order inside the spread.
    /// The exchange MUST reject this order if it would execute as taker.
    /// Returns (order_id, placed_price) on success.
    /// Default: not supported — returns Err.
    async fn place_maker_order(
        &self,
        _http: &Client,
        _keys: &ExchangeKeys,
        _pair: &str,
        _is_buy: bool,
        _phys_amount: f64,
        _limit_price: f64,
        _leverage: f64,
        _multiplier: f64,
        _tick_size: f64,
        _api_timeout_ms: u64,
    ) -> Result<(String, f64), String> {
        Err(format!("{} does not support maker orders", self.name()))
    }

    /// Cancel an outstanding order by ID.
    /// Returns Ok(true) if cancelled, Ok(false) if already filled/gone, Err on failure.
    async fn cancel_order(
        &self,
        _http: &Client,
        _keys: &ExchangeKeys,
        _pair: &str,
        _order_id: &str,
    ) -> Result<bool, String> {
        Err(format!("{} does not support cancel_order", self.name()))
    }

    /// Fetch maker fee rate (e.g. 0.0002 = 0.02%).
    /// Default: falls back to taker fee.
    async fn fetch_maker_fee(&self, http: &Client, keys: &ExchangeKeys) -> Result<f64, String> {
        self.fetch_taker_fee(http, keys).await
    }

    /// Asynchronously fetch verified exchange history (fills, fees, realized PnL, funding) for a symbol.
    /// Runs in background after trade completion / reconciliation — does not block fast-path execution.
    async fn audit_trade(
        &self,
        _http: &Client,
        _keys: &ExchangeKeys,
        _pair: &str,
        _start_ts: i64,
        _end_ts: i64,
    ) -> Result<TradeAuditResult, String> {
        Ok(TradeAuditResult::default())
    }
}
