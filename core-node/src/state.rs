//! core-node shared state
//!
//! `PriceMap`  — живая память: exchange → symbol → TickerData, обновляется WebSocket-хендлерами
//! `UpdateSignal` — `Notify` который WS-хендлер вызывает после каждого обновления
//!
//! Движок читает PriceMap без блокировок (DashMap = concurrent RwLock per shard)
//! и ждёт `signal.notified()` вместо polling — нулевой overhead в idle.

use std::{collections::HashMap, sync::Arc};
use dashmap::DashMap;
use tokio::sync::{Notify, RwLock};

use crate::settings::TradingSettings;

/// exchange_id → symbol → TickerData (e.g. "gate" → "BTCUSDT" → {...})
pub type PriceMap = Arc<DashMap<String, HashMap<String, TickerData>>>;

/// Fired after every WS tick update. Engine wakes up and recomputes spreads.
pub type UpdateSignal = Arc<Notify>;

// Re-export from shared (will migrate fully later)
pub use shared::{SpreadRow, OrderResult};

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct TickerData {
    pub bid: f64,
    pub ask: f64,
    pub price: f64,
    pub vol: f64,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: f64,
    #[serde(skip)]
    pub last_updated: u64,
}

/// Shared application state passed to every task.
#[derive(Clone)]
pub struct AppState {
    /// Live prices — written by collector tasks, read by engine
    pub prices: PriceMap,
    /// Signal: fires when any price changes
    pub signal: UpdateSignal,
    /// Settings refreshed from DB every 5s
    pub real_settings:  Arc<RwLock<TradingSettings>>,
    pub paper_settings: Arc<RwLock<TradingSettings>>,
    /// HTTP client shared across all REST calls
    pub http: reqwest::Client,
}

impl AppState {
    pub fn new(http: reqwest::Client) -> Self {
        Self {
            prices:         Arc::new(DashMap::new()),
            signal:         Arc::new(Notify::new()),
            real_settings:  Arc::new(RwLock::new(TradingSettings::default())),
            paper_settings: Arc::new(RwLock::new(TradingSettings::default())),
            http,
        }
    }
}
