use crate::state::EngineContext;
use crate::engine::utils::now_ms;
use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;

static EXCHANGE_BANS: OnceLock<DashMap<String, i64>> = OnceLock::new();
pub static LAST_MARGIN_LOG: AtomicI64 = AtomicI64::new(0);
pub const PRIVATE_SNAPSHOT_MAX_AGE_MS: i64 = 60_000;

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
    // Binance has its own IP ban tracking (in binance.rs) — check it too
    if exchange.eq_ignore_ascii_case("binance") {
        if crate::engine::exchanges::binance::is_binance_banned() {
            return true;
        }
    }
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
    multiplier: f64,
) -> bool {
    let mult = if multiplier > 0.0 { multiplier } else { 1.2 };
    match e_ctx.margin_balances.get(exchange) {
        Some(margin) => {
            let available = *margin;
            let required = notional_usd / leverage.max(1.0);
            let ok = available >= required * mult;
            if !ok {
                let now = now_ms();
                if now - LAST_MARGIN_LOG.load(Ordering::Relaxed) > 15_000 {
                    LAST_MARGIN_LOG.store(now, Ordering::Relaxed);
                    tracing::warn!(
                        "[margin] {} available=${:.2} required=${:.2} (notional=${:.2} lev={}x mult={:.2}x) — INSUFFICIENT",
                        exchange, available, required * mult, notional_usd, leverage, mult
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
                tracing::warn!("[margin] {} NOT in margin_balances — blocking entry until an authoritative balance arrives", exchange);
            }
            false
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

/// A centralized check that verifies the health of both exchanges before attempting a trade.
/// It checks global bans and low balances. Does NOT check specific margins (done JIT in managers).
/// Returns true if both exchanges are healthy and ready for trading.
pub fn verify_trade_health(exa: &str, exb: &str, e_ctx: &EngineContext) -> bool {
    if is_exchange_banned(exa) || is_exchange_banned(exb) {
        return false;
    }

    let a_low = e_ctx.balance_low.get(exa).map(|v| *v).unwrap_or(false);
    let b_low = e_ctx.balance_low.get(exb).map(|v| *v).unwrap_or(false);
    if a_low || b_low {
        return false;
    }

    // Cached balances and positions are part of entry authorization, not only
    // health-page telemetry. A later private REST preflight remains mandatory,
    // but a stale account cache must prevent a signal from reaching it at all.
    let now = now_ms();
    for exchange in [exa, exb] {
        let balance_fresh = e_ctx
            .balances_updated_ms
            .get(exchange)
            .map(|stamp| {
                crate::engine::utils::local_timestamp_is_fresh(
                    now,
                    *stamp,
                    PRIVATE_SNAPSHOT_MAX_AGE_MS,
                )
            })
            .unwrap_or(false);
        let positions_fresh = e_ctx
            .positions_updated_ms
            .get(exchange)
            .map(|stamp| {
                crate::engine::utils::local_timestamp_is_fresh(
                    now,
                    *stamp,
                    PRIVATE_SNAPSHOT_MAX_AGE_MS,
                )
            })
            .unwrap_or(false);
        if !balance_fresh || !positions_fresh {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trade_health_requires_fresh_private_snapshots_for_both_venues() {
        let ctx = EngineContext::default();
        let now = now_ms();
        assert!(!verify_trade_health("venue-a", "venue-b", &ctx));

        for exchange in ["venue-a", "venue-b"] {
            ctx.balances_updated_ms.insert(exchange.into(), now);
            ctx.positions_updated_ms.insert(exchange.into(), now);
        }
        assert!(verify_trade_health("venue-a", "venue-b", &ctx));

        ctx.positions_updated_ms.insert(
            "venue-b".into(),
            now - PRIVATE_SNAPSHOT_MAX_AGE_MS - 1,
        );
        assert!(!verify_trade_health("venue-a", "venue-b", &ctx));
    }
}
