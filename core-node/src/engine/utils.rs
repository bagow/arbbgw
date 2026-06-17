//! Engine utilities — single source of truth for common helpers.
//!
//! Previously duplicated across real.rs, risk_manager.rs, and scanner.rs.

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
        || s.contains("no open position")
        || s.contains("position size is zero")
        || s.contains("reduce-only")
        || s.contains("liquidat")
        || s.contains("fallback rejected")
        || s.contains("22002")  // Bitget error code for "No position to close"
        || s.contains("300009") // Kucoin error code for "No open positions to close"
}

/// Static liquidity ranking for exchanges.
/// Lower = more liquid. Used to determine entry order (less liquid first).
pub fn liquidity_rank(exchange: &str) -> u8 {
    match exchange {
        "binance" => 1,
        "bybit" => 2,
        "gate" => 3,
        "bitget" => 4,
        "bingx" => 5,
        "kucoin" => 6,
        "bitmex" => 7,
        "ascendex" => 8,
        "toobit" => 9,
        "xt" => 10,
        _ => 50,
    }
}

/// Build the canonical \"exchange:PAIR\" key used across all DashMaps.
/// Eliminates 15+ `format!("{}:{}", exchange, pair)` duplications.
#[inline]
pub fn exchange_pair_key(exchange: &str, pair: &str) -> String {
    format!("{}:{}", exchange, pair)
}
