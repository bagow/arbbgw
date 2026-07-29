//! Engine utilities — single source of truth for common helpers.
//!
//! Previously duplicated across real.rs, risk_manager.rs, and scanner.rs.

use dashmap::DashMap;
use std::sync::OnceLock;

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

// ── WS CONNECTED-AT TRACKING ────────────────────────────────────────────────
// Global static (like ban_registry in risk_manager.rs) — avoids threading
// EngineContext through all WS trader loops.

static WS_CONNECTED_AT: OnceLock<DashMap<String, i64>> = OnceLock::new();

fn ws_connected_map() -> &'static DashMap<String, i64> {
    WS_CONNECTED_AT.get_or_init(|| DashMap::new())
}

/// Record when a WS trading connection was established.
/// Called from ws_trader.rs, ws_gate.rs, ws_bybit.rs on reconnect.
pub fn record_ws_connected(exchange: &str) {
    ws_connected_map().insert(exchange.to_string(), now_ms());
}

/// Check if a WS connection was established recently (within `freshness_ms`).
/// Returns true if connection is TOO FRESH (should wait).
pub fn is_ws_too_fresh(exchange: &str, freshness_ms: i64) -> bool {
    if let Some(connected_at) = ws_connected_map().get(exchange) {
        return now_ms() - *connected_at < freshness_ms;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_symbol_strips_separators_and_case() {
        assert_eq!(normalize_symbol("btc-usdt"), "BTCUSDT");
        assert_eq!(normalize_symbol("BTC_USDT"), "BTCUSDT");
        assert_eq!(normalize_symbol("BTC/USDT"), "BTCUSDT");
        assert_eq!(normalize_symbol("BTCUSDT"), "BTCUSDT");
    }

    #[test]
    fn normalize_symbol_maps_exchange_aliases() {
        assert_eq!(normalize_symbol("XBTUSDTM"), "BTCUSDT");
        assert_eq!(normalize_symbol("XBTUSDT"), "BTCUSDT");
        assert_eq!(normalize_symbol("ETHUSDTM"), "ETHUSDT");
    }

    #[test]
    fn symbols_match_across_exchange_formats() {
        assert!(symbols_match("BTC_USDT", "btc-usdt"));
        assert!(symbols_match("XBTUSDTM", "BTCUSDT"));
        assert!(!symbols_match("BTCUSDT", "ETHUSDT"));
    }

    #[test]
    fn cooldown_inactive_when_marker_unset() {
        assert!(!cooldown_active(0, 1_000_000, 5_000));
        assert!(!cooldown_active(-1, 1_000_000, 5_000));
    }

    #[test]
    fn cooldown_active_for_blocked_until_marker() {
        let now = 1_000_000;
        assert!(cooldown_active(now + 1, now, 0));
    }

    #[test]
    fn cooldown_expires_exactly_at_window_edge() {
        let now = 1_000_000;
        assert!(cooldown_active(now - 4_999, now, 5_000));
        assert!(!cooldown_active(now - 5_000, now, 5_000));
        assert!(!cooldown_active(now - 10_000, now, 5_000));
    }

    #[test]
    fn liquidity_rank_orders_thin_exchanges_last() {
        assert!(liquidity_rank("binance") < liquidity_rank("kucoin"));
        assert!(liquidity_rank("kucoin") < liquidity_rank("xt"));
        assert_eq!(liquidity_rank("unknown-exchange"), 50);
    }

    #[test]
    fn detects_missing_position_errors_case_insensitively() {
        assert!(is_position_missing_or_liquidated_error("Position not found"));
        assert!(is_position_missing_or_liquidated_error("ORDER LIQUIDATED"));
        assert!(is_position_missing_or_liquidated_error("bitget error 22002"));
        assert!(is_position_missing_or_liquidated_error("code 300009 kucoin"));
        // Не должно глотать ошибки, требующие реальной реакции.
        assert!(!is_position_missing_or_liquidated_error("insufficient balance"));
        assert!(!is_position_missing_or_liquidated_error("rate limit exceeded"));
        assert!(!is_position_missing_or_liquidated_error("timeout"));
    }

    #[test]
    fn exchange_pair_key_is_stable() {
        assert_eq!(exchange_pair_key("gate", "BTCUSDT"), "gate:BTCUSDT");
    }

    #[test]
    fn ws_freshness_tracks_reconnects() {
        record_ws_connected("test-freshness-ex");
        assert!(is_ws_too_fresh("test-freshness-ex", 60_000));
        assert!(!is_ws_too_fresh("test-freshness-ex", 0));
        assert!(!is_ws_too_fresh("never-connected-ex", 60_000));
    }

    #[test]
    fn now_ms_and_now_secs_agree() {
        let ms = now_ms();
        let secs = now_secs();
        assert!((ms / 1000 - secs).abs() <= 1);
    }
}
