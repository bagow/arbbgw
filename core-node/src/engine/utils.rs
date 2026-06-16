//! Engine utilities — single source of truth for common helpers.
//!
//! Previously duplicated across real.rs, risk_manager.rs, scanner.rs, hft_scanner.rs, hft_cycle.rs.

/// Current time in milliseconds since UNIX epoch.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Current time in seconds since UNIX epoch.
pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Check if a cooldown is still active.
/// Supports two contract modes:
/// 1) `marker` = last event timestamp → active if `now - marker < cooldown_ms`
/// 2) `marker` = blocked-until timestamp → active if `marker > now`
pub fn cooldown_active(marker: i64, now: i64, cooldown_ms: i64) -> bool {
    if marker <= 0 {
        return false;
    }
    if marker > now {
        return true;
    }
    now - marker < cooldown_ms
}

/// Normalize exchange-specific symbol format to canonical form (e.g. "BTCUSDT").
pub fn normalize_symbol(sym: &str) -> String {
    let mut s = sym.to_uppercase();
    s = s.replace(['-', '_', '/'], "");
    if s == "XBTUSDTM" {
        return "BTCUSDT".to_string();
    }
    if s == "XBTUSDT" {
        return "BTCUSDT".to_string();
    }
    if s.ends_with("USDTM") {
        s = s.trim_end_matches('M').to_string();
    }
    s
}

/// Check if two symbols refer to the same instrument.
pub fn symbols_match(a: &str, b: &str) -> bool {
    normalize_symbol(a) == normalize_symbol(b)
}

/// Round price to the nearest valid tick size for the given exchange:pair.
/// If tick size is unknown, rounds based on price magnitude (safe fallback).
pub fn round_to_tick(
    price: f64,
    exchange: &str,
    pair: &str,
    e_ctx: &crate::state::EngineContext,
) -> f64 {
    let key = format!("{}:{}", exchange, pair);
    let tick = e_ctx.tick_sizes.get(&key).map(|v| *v).unwrap_or(0.0);
    if tick > 0.0 {
        (price / tick).round() * tick
    } else {
        if price > 1000.0 {
            (price * 100.0).round() / 100.0
        } else if price > 10.0 {
            (price * 1000.0).round() / 1000.0
        } else if price > 0.1 {
            (price * 10000.0).round() / 10000.0
        } else {
            (price * 100000.0).round() / 100000.0
        }
    }
}

/// Get cached taker fee rate for an exchange (as fraction, e.g. 0.0005 = 0.05%).
pub fn get_exchange_fee(
    exchange: &str,
    default_fee: f64,
    e_ctx: &crate::state::EngineContext,
) -> f64 {
    e_ctx
        .fee_rates
        .get(exchange)
        .map(|v| *v)
        .unwrap_or(default_fee)
}

/// Check if a position error indicates the position is already gone (liquidation/external close).
pub fn is_position_missing_or_liquidated_error(err: &str) -> bool {
    let s = err.to_lowercase();
    s.contains("position not found")
        || s.contains("no position")
        || s.contains("position size is zero")
        || s.contains("reduce-only")
        || s.contains("liquidat")
}
