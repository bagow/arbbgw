use crate::state::EngineContext;
use crate::engine::utils::now_ms;
use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;

static EXCHANGE_BANS: OnceLock<DashMap<String, i64>> = OnceLock::new();
pub static LAST_MARGIN_LOG: AtomicI64 = AtomicI64::new(0);

fn ban_map() -> &'static DashMap<String, i64> {
    EXCHANGE_BANS.get_or_init(|| DashMap::new())
}

/// Mark an exchange as banned until `until_ms` (Unix timestamp in ms).
pub fn ban_exchange(exchange: &str, until_ms: i64) {
    let map = ban_map();
    let current = map.get(exchange).map(|v| *v).unwrap_or(0);
    if until_ms > current {
        map.insert(exchange.to_lowercase(), until_ms);
        let dur_s = (until_ms - now_ms()) / 1000;
        tracing::error!(
            "[ban_registry] 🔴 {} ЗАБАНЕН на {}с (до {}). Торговля приостановлена.",
            exchange, dur_s.max(0), until_ms
        );
    }
}

/// Check if an exchange is currently banned. Returns true if banned.
pub fn is_exchange_banned(exchange: &str) -> bool {
    let map = ban_map();
    if let Some(entry) = map.get(&exchange.to_lowercase()) {
        let until = *entry;
        if until > 0 && now_ms() < until {
            return true;
        }
        // Ban expired — remove it
        drop(entry);
        map.remove(&exchange.to_lowercase());
        tracing::info!("[ban_registry] ✅ {} бан истёк. Торговля возобновлена.", exchange);
    }
    false
}

pub fn has_sufficient_margin(
    exchange: &str,
    notional_usd: f64,
    leverage: f64,
    e_ctx: &EngineContext,
) -> bool {
    match e_ctx.margin_balances.get(exchange) {
        Some(margin) => {
            let available = *margin;
            let required = notional_usd / leverage.max(1.0);
            let ok = available >= required * 1.5;
            if !ok {
                let now = now_ms();
                if now - LAST_MARGIN_LOG.load(Ordering::Relaxed) > 15_000 {
                    LAST_MARGIN_LOG.store(now, Ordering::Relaxed);
                    tracing::warn!(
                        "[margin] {} available=${:.2} required=${:.2} (notional=${:.2} lev={}x) — INSUFFICIENT",
                        exchange, available, required * 1.5, notional_usd, leverage
                    );
                }
            }
            ok
        }
        None => {
            static LAST_NONE_LOG: AtomicI64 = AtomicI64::new(0);
            let now = now_ms();
            if now - LAST_NONE_LOG.load(Ordering::Relaxed) > 15_000 {
                LAST_NONE_LOG.store(now, Ordering::Relaxed);
                tracing::warn!("[margin] {} NOT in margin_balances — allowing trade (exchange will reject if no margin)", exchange);
            }
            true
        }
    }
}

/// Immediately mark an exchange as balance-low.
pub fn mark_balance_low(exchange: &str, e_ctx: &EngineContext) {
    if !e_ctx.balance_low.get(exchange).map(|v| *v).unwrap_or(false) {
        tracing::warn!("[balance] {} marked balance_low — new trades on this exchange are blocked until balance recovers", exchange);
        e_ctx.balance_low.insert(exchange.to_string(), true);
    }
}

/// Deduct notional from cached margin balance after a successful trade open.
pub fn deduct_cached_balance(
    exchange: &str,
    notional_usd: f64,
    leverage: f64,
    e_ctx: &EngineContext,
) {
    let required_margin = notional_usd / leverage.max(1.0);
    if let Some(mut bal) = e_ctx.margin_balances.get_mut(exchange) {
        *bal -= required_margin;
    }
}

#[allow(dead_code)]
pub fn is_margin_error(err: &str) -> bool {
    let s = err.to_lowercase();
    s.contains("insufficient")
        || s.contains("exceeds the balance")
        || s.contains("minimum amount")
        || s.contains("notional must be no smaller")
        || s.contains("maximum allowable position")
        || s.contains("margin is insufficient")
        || s.contains("less than the minimum")
        || s.contains("order amount exceeds")
        || s.contains("not enough")
        || s.contains("balance not enough")
}
