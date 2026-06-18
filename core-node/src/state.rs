//! core-node shared state
//!
//! `PriceMap`  — живая память: exchange → symbol → TickerData, обновляется WebSocket-хендлерами
//! `UpdateSignal` — `Notify` который WS-хендлер вызывает после каждого обновления
//!
//! Движок читает PriceMap без блокировок (DashMap = concurrent RwLock per shard)
//! и ждёт `signal.notified()` вместо polling — нулевой overhead в idle.

use dashmap::{DashMap, DashSet};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Notify;

// Re-export canonical types from shared crate —
// single source of truth, no local duplicates.
pub use shared::TickerData;

/// exchange_id → symbol → TickerData (e.g. "gate" → "BTCUSDT" → {...})
pub type PriceMap = Arc<DashMap<String, HashMap<String, TickerData>>>;

/// Live L2 orderbook depth — updated by WS collectors every ~100ms.
/// Key: "exchange:PAIR" (e.g. "binance:BTCUSDT")
/// Value: (bids, asks) where each is Vec<(price, size_in_base_asset)> sorted by best first.
pub type L2OrderbookMap = Arc<DashMap<String, L2Snapshot>>;

#[derive(Clone, Debug, Default)]
pub struct L2Snapshot {
    /// Sorted descending by price (best bid first)
    pub bids: Vec<(f64, f64)>,
    /// Sorted ascending by price (best ask first)
    pub asks: Vec<(f64, f64)>,
    /// Unix millis of last update
    pub updated_ms: i64,
}

/// Fired after every WS tick update. Engine wakes up and recomputes spreads.
pub type UpdateSignal = Arc<Notify>;

#[derive(Clone, Debug)]
pub struct FundingInfo {
    pub rate: f64,       // current funding rate (e.g. 0.0001 = 0.01%)
    pub next_ts: i64,    // unix timestamp (ms) of next funding apply
    #[allow(dead_code)]
    pub fetched_at: i64, // when this was cached
}

#[derive(Clone)]
pub struct EngineContext {
    pub multipliers: Arc<DashMap<String, f64>>,
    pub launch_times: Arc<DashMap<String, i64>>,
    pub tick_sizes: Arc<DashMap<String, f64>>,
    pub balances: Arc<DashMap<String, f64>>,
    pub fee_rates: Arc<DashMap<String, f64>>,
    #[allow(dead_code)]
    pub leverage_set: Arc<DashMap<String, bool>>,
    pub exchange_cooldown: Arc<DashMap<String, i64>>,
    pub pair_cooldown: Arc<DashMap<String, i64>>,
    pub funding_rates: Arc<DashMap<String, FundingInfo>>,
    /// Live flag: true = exchange recently rejected an order due to insufficient balance/margin.
    /// Set immediately on API rejection. Cleared by start_balance_fetcher when balance recovers.
    /// Prevents entering trades on an exchange that has no margin, avoiding asymmetric fills.
    pub balance_low: Arc<DashMap<String, bool>>,
    pub margin_balances: Arc<DashMap<String, f64>>,
    pub positions: Arc<DashMap<String, Vec<crate::engine::exchanges::traits::PositionInfo>>>,
    /// Guard against parallel close attempts on the same trade_id.
    /// Set to true when a close is in-flight; cleared when the spawn finishes.
    pub closing_in_flight: Arc<DashMap<i64, bool>>,
    /// Risk Kill-Switch: Number of consecutive delta trims. Reset on successful symmetric execution.
    pub consecutive_trims: Arc<std::sync::atomic::AtomicU32>,
    /// Risk Kill-Switch: Number of consecutive entry failures. Reset on successful entry.
    pub consecutive_failures: Arc<std::sync::atomic::AtomicU32>,
    /// Risk Kill-Switch: If true, engine stops opening new trades until manual reset.
    pub is_killed: Arc<std::sync::atomic::AtomicBool>,
    /// Tracks consecutive high-latency occurrences for an exchange.
    pub high_latency_strikes: Arc<DashMap<String, u32>>,
    /// Market Regime Tracker: pair -> (price, timestamp_ms). Used to calculate spread velocity.
    pub price_history: Arc<DashMap<String, (f64, i64)>>,
    /// Live L2 orderbook depth cache — updated by WS collectors in real-time.
    pub l2_books: L2OrderbookMap,
    /// Demand-driven L2 subscription set — keys like "bybit:HUSDT".
    /// Populated by scanner (spread rows) + cycle (open trades).
    /// Collectors subscribe only to pairs present here.
    pub l2_wanted: Arc<DashSet<String>>,
    /// Per-trade MIRAGE cooldown: trade_id → last_mirage_log_ms.
    /// Prevents logging MIRAGE spam (was 10+ per second, now max 1 per 10s).
    pub mirage_cooldown: Arc<DashMap<i64, i64>>,
    /// Position-sync confirmation counter: trade_id → (consecutive_miss_count, last_miss_timestamp_ms).
    /// Requires 6 consecutive confirmations (within 30s window) before emergency close to avoid false positives.
    pub position_missing_counter: Arc<DashMap<i64, (u32, i64)>>,
    /// Asymmetric failure counter per pair: pair → (fail_count, first_fail_timestamp_ms).
    /// Bans pair for 5 min after 3+ failures within 1 hour.
    pub asymmetric_fail_counter: Arc<DashMap<String, (u32, i64)>>,
    /// Profiles DB connection — used for kill-switch status persistence in server_cache.
    pub profiles_db: Option<tokio_rusqlite::Connection>,
    /// WS order channel unhealthy-until timestamp per exchange.
    /// Set to `now + TTL_ms` on WS timeout/failure. Auto-recovers when timestamp expires.
    /// Prevents firing trades on exchanges with recently broken WS order channels.
    pub ws_order_unhealthy_until: Arc<DashMap<String, i64>>,
}

impl Default for EngineContext {
    fn default() -> Self {
        Self {
            multipliers: Arc::new(DashMap::new()),
            launch_times: Arc::new(DashMap::new()),
            tick_sizes: Arc::new(DashMap::new()),
            balances: Arc::new(DashMap::new()),
            fee_rates: Arc::new(DashMap::new()),
            leverage_set: Arc::new(DashMap::new()),
            exchange_cooldown: Arc::new(DashMap::new()),
            pair_cooldown: Arc::new(DashMap::new()),
            funding_rates: Arc::new(DashMap::new()),
            balance_low: Arc::new(DashMap::new()),
            margin_balances: Arc::new(DashMap::new()),
            positions: Arc::new(DashMap::new()),
            closing_in_flight: Arc::new(DashMap::new()),
            consecutive_trims: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            consecutive_failures: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            is_killed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            high_latency_strikes: Arc::new(DashMap::new()),
            price_history: Arc::new(DashMap::new()),
            l2_books: Arc::new(DashMap::new()),
            l2_wanted: Arc::new(DashSet::new()),
            mirage_cooldown: Arc::new(DashMap::new()),
            position_missing_counter: Arc::new(DashMap::new()),
            asymmetric_fail_counter: Arc::new(DashMap::new()),
            profiles_db: None,
            ws_order_unhealthy_until: Arc::new(DashMap::new()),
        }
    }
}
