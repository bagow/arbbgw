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
pub use shared::SpreadRow;

/// exchange_id → symbol → TickerData (e.g. "gate" → "BTCUSDT" → {...})
pub type PriceMap = Arc<DashMap<String, HashMap<String, TickerData>>>;

/// Fired after every WS tick update. Engine wakes up and recomputes spreads.
pub type UpdateSignal = Arc<Notify>;
