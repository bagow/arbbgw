//! core-node shared state
//!
//! `PriceMap`  — живая память: exchange → symbol → TickerData, обновляется WebSocket-хендлерами
//! `UpdateSignal` — `Notify` который WS-хендлер вызывает после каждого обновления
//!
//! Движок читает PriceMap без блокировок (DashMap = concurrent RwLock per shard)
//! и ждёт `signal.notified()` вместо polling — нулевой overhead в idle.

use std::{collections::HashMap, sync::Arc};
use dashmap::DashMap;
use tokio::sync::Notify;

// Re-export canonical types from shared crate —
// single source of truth, no local duplicates.
pub use shared::TickerData;

/// exchange_id → symbol → TickerData (e.g. "gate" → "BTCUSDT" → {...})
pub type PriceMap = Arc<DashMap<String, HashMap<String, TickerData>>>;

/// Fired after every WS tick update. Engine wakes up and recomputes spreads.
pub type UpdateSignal = Arc<Notify>;

#[derive(Clone, Debug)]
pub struct FundingInfo {
    pub rate: f64,        // current funding rate (e.g. 0.0001 = 0.01%)
    pub next_ts: i64,     // unix timestamp (ms) of next funding apply
    pub fetched_at: i64,  // when this was cached
}

#[derive(Clone)]
pub struct EngineContext {
    pub multipliers: Arc<DashMap<String, f64>>,
    pub launch_times: Arc<DashMap<String, i64>>,
    pub tick_sizes: Arc<DashMap<String, f64>>,
    pub balances: Arc<DashMap<String, f64>>,
    pub fee_rates: Arc<DashMap<String, f64>>,
    pub leverage_set: Arc<DashMap<String, bool>>,
    pub exchange_cooldown: Arc<DashMap<String, i64>>,
    pub pair_cooldown: Arc<DashMap<String, i64>>,
    pub funding_rates: Arc<DashMap<String, FundingInfo>>,
    /// Live flag: true = exchange recently rejected an order due to insufficient balance/margin.
    /// Set immediately on API rejection. Cleared by start_balance_fetcher when balance recovers.
    /// Prevents entering trades on an exchange that has no margin, avoiding asymmetric fills.
    pub balance_low: Arc<DashMap<String, bool>>,
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
        }
    }
}
