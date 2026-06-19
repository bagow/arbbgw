use reqwest::Client;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info, warn};
use dashmap::DashMap;
use std::sync::OnceLock;

use crate::engine::exchanges::get_connector;

use crate::engine::real::ExchangeKeys;

/// Consecutive fetch error counter per exchange.
/// After MAX_CONSECUTIVE_ERRORS, the exchange is auto-banned.
static FETCH_ERROR_COUNTS: OnceLock<DashMap<String, u32>> = OnceLock::new();
fn error_counts() -> &'static DashMap<String, u32> {
    FETCH_ERROR_COUNTS.get_or_init(|| DashMap::new())
}
const MAX_CONSECUTIVE_ERRORS: u32 = 3;
const AUTO_BAN_DURATION_MS: i64 = 120_000; // 2 minutes

/// Record a successful fetch — resets error counter and clears ban.
fn record_fetch_success(ex: &str) {
    let counts = error_counts();
    if counts.get(ex).map(|v| *v > 0).unwrap_or(false) {
        counts.insert(ex.to_string(), 0);
    }
}

/// Record a fetch error — increments counter and auto-bans after threshold.
fn record_fetch_error(ex: &str, e_ctx: &crate::state::EngineContext) {
    let counts = error_counts();
    let mut entry = counts.entry(ex.to_string()).or_insert(0);
    *entry += 1;
    let count = *entry;
    drop(entry);

    if count >= MAX_CONSECUTIVE_ERRORS {
        let now = crate::engine::utils::now_ms();
        crate::engine::risk_manager::ban_exchange(ex, now + AUTO_BAN_DURATION_MS);
        crate::engine::risk_manager::mark_balance_low(ex, e_ctx);
        error!(
            "[fetcher] 🔴 AUTO-BAN {}: {} consecutive fetch errors. Banned for {}s, balance_low=true.",
            ex, count, AUTO_BAN_DURATION_MS / 1000
        );
        // Reset counter so ban message doesn't repeat every cycle
        counts.insert(ex.to_string(), 0);
    }
}

/// Spawn background task that refreshes balances and fee rates.
/// Adaptive intervals:
///   - Normal mode : every 25s — full refresh (balance + fee rates) for all exchanges
///   - Recovery mode: every 5s  — balance ONLY for the exchange(s) with balance_low=true
///     Fee rates are NOT re-fetched in recovery mode to avoid rate limits.
pub fn start_balance_fetcher(
    http: Client,
    keys: Arc<ExchangeKeys>,
    e_ctx: Arc<crate::state::EngineContext>,
    settings: Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    tokio::spawn(async move {
        let fast_interval_ms = 5_000u64;
        let normal_interval_s = 25u64;
        let all_exchanges = [
            "binance", "gate", "kucoin", "bybit", "bitget",
            "bingx", "bitmex", "ascendex", "xt", "toobit",
        ];

        loop {
            // Check which exchanges are explicitly enabled
            let active_exchanges: Vec<&str> = {
                let p = settings.read().await;
                all_exchanges
                    .into_iter()
                    .filter(|ex| {
                        p.is_empty()
                            || !p
                                .iter()
                                .all(|x| x.disabled_exchanges.contains(&ex.to_string()))
                    })
                    .collect()
            };

            // Determine which exchanges have balance_low set
            let low_exchanges: Vec<&str> = active_exchanges
                .iter()
                .copied()
                .filter(|ex| {
                    keys.supports(ex) && e_ctx.balance_low.get(*ex).map(|v| *v).unwrap_or(false)
                })
                .collect();

            if !low_exchanges.is_empty() {
                // Recovery mode: only fetch balances for low exchanges
                for ex in &low_exchanges {
                    let conn = match get_connector(ex) {
                        Some(c) => c,
                        None => continue,
                    };
                    match conn.fetch_balances(&http, &keys).await {
                        Ok((bal, _)) => {
                            e_ctx.balances.insert(ex.to_string(), bal);
                            record_fetch_success(ex);
                            if bal > 1.0 {
                                info!("[balance] {} recovered (${:.2}) — clearing balance_low flag, trades unblocked", ex, bal);
                                e_ctx.balance_low.insert(ex.to_string(), false);
                            } else {
                                warn!(
                                    "[balance] {} still balance_low (${:.2}) — retry in {}ms",
                                    ex, bal, fast_interval_ms
                                );
                            }
                        }
                        Err(e) => {
                            error!("[balance] {} fetch error (recovery mode): {}", ex, e);
                            record_fetch_error(ex, &e_ctx);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(fast_interval_ms)).await;
            } else {
                // Normal mode: full refresh for all active exchanges
                // Clear positions for disabled exchanges to prevent stale ghost positions
                for ex in &all_exchanges {
                    if !active_exchanges.contains(ex) {
                        e_ctx.positions.remove(*ex);
                    }
                }

                for ex in &active_exchanges {
                    if !keys.supports(ex) {
                        continue;
                    }
                    // Skip exchanges that are currently banned (no point fetching if we can't trade)
                    if crate::engine::risk_manager::is_exchange_banned(ex) {
                        continue;
                    }
                    let conn = match get_connector(ex) {
                        Some(c) => c,
                        None => continue,
                    };

                    let mut had_error = false;

                    match conn.fetch_balances(&http, &keys).await {
                        Ok((w, a)) => {
                            e_ctx.balances.insert(ex.to_string(), w);
                            e_ctx.margin_balances.insert(ex.to_string(), a);
                        }
                        Err(e) => {
                            had_error = true;
                            let msg = e.to_string();
                            if msg.contains("banned") || msg.contains("ban") {
                                tracing::debug!("[{}] balance fetch skipped (banned)", ex);
                            } else {
                                error!("[{}] balance fetch err: {}", ex, msg);
                            }
                        }
                    }
                    match conn.fetch_positions(&http, &keys).await {
                        Ok(pos) => {
                            let prev_count = e_ctx.positions.get(*ex)
                                .map(|v| v.value().len())
                                .unwrap_or(0);
                            let new_count = pos.len();
                            // Guard: don't replace non-empty with empty — exchange API
                            // sometimes returns empty even when positions exist (margin locked).
                            // Only allow clearing if available_balance ≈ wallet_balance
                            // (meaning no margin is actually locked).
                            if new_count == 0 && prev_count > 0 {
                                let wallet = e_ctx.balances.get(*ex).map(|v| *v).unwrap_or(0.0);
                                let avail = e_ctx.margin_balances.get(*ex).map(|v| *v).unwrap_or(0.0);
                                let margin_locked = wallet - avail;
                                if margin_locked > 0.5 {
                                    warn!("[{}] fetch_positions returned 0 but {} positions cached \
                                           and ${:.2} margin locked — keeping cache", ex, prev_count, margin_locked);
                                    continue;  // skip the insert, keep old positions
                                }
                            }
                            if new_count != prev_count {
                                info!("[{}] positions: {} → {}", ex, prev_count, new_count);
                            }
                            e_ctx.positions.insert(ex.to_string(), pos);
                        }
                        Err(e) => {
                            had_error = true;
                            warn!("[{}] fetch_positions err: {} — keeping stale cache", ex, e);
                        }
                    }

                    // Track consecutive errors for auto-ban
                    if had_error {
                        record_fetch_error(ex, &e_ctx);
                    } else {
                        record_fetch_success(ex);
                    }

                    if let Ok(fee) = conn.fetch_taker_fee(&http, &keys).await {
                        e_ctx.fee_rates.insert(ex.to_string(), fee);
                    }
                }
                // Diagnostic: log current fee_rates and balances
                {
                    let mut fee_diag = String::from("[fetcher] fee_rates:");
                    for item in e_ctx.fee_rates.iter() {
                        fee_diag.push_str(&format!(" {}={:.6}", item.key(), item.value()));
                    }
                    let mut bal_diag = String::from(" | balances:");
                    for item in e_ctx.balances.iter() {
                        bal_diag.push_str(&format!(" {}=${:.2}", item.key(), item.value()));
                    }
                    info!("{}{}", fee_diag, bal_diag);
                }
                tokio::time::sleep(Duration::from_secs(normal_interval_s)).await;
            }
        }
    });
}
