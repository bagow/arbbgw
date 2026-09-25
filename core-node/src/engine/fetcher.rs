use reqwest::Client;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info, warn};
use dashmap::DashMap;
use futures_util::{stream::FuturesUnordered, StreamExt};
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

async fn refresh_exchange(
    ex: &'static str,
    http: Client,
    keys: Arc<ExchangeKeys>,
    e_ctx: Arc<crate::state::EngineContext>,
) {
    let Some(conn) = get_connector(ex) else {
        return;
    };
    // Balance and position endpoints are independent. Running them together
    // prevents one slow endpoint from consuming the freshness budget of the
    // other, while each connector keeps its own bounded request timeout.
    let (balances, positions) = tokio::join!(
        conn.fetch_balances(&http, &keys),
        conn.fetch_positions(&http, &keys),
    );
    let mut had_error = false;

    match balances {
        Ok((wallet, available)) => {
            e_ctx.balances.insert(ex.to_string(), wallet);
            e_ctx.margin_balances.insert(ex.to_string(), available);
            e_ctx
                .balances_updated_ms
                .insert(ex.to_string(), crate::engine::utils::now_ms());
            e_ctx.balance_low.insert(ex.to_string(), wallet <= 1.0);
        }
        Err(error) => {
            had_error = true;
            let message = error.to_string();
            if message.contains("banned") || message.contains("ban") {
                tracing::debug!("[{}] balance fetch skipped (banned)", ex);
            } else {
                error!("[{}] balance fetch err: {}", ex, message);
            }
        }
    }

    match positions {
        Ok(positions) => {
            let previous_count = e_ctx
                .positions
                .get(ex)
                .map(|value| value.value().len())
                .unwrap_or(0);
            if positions.len() != previous_count {
                info!("[{}] positions: {} → {}", ex, previous_count, positions.len());
            }
            e_ctx.positions.insert(ex.to_string(), positions);
            e_ctx
                .positions_updated_ms
                .insert(ex.to_string(), crate::engine::utils::now_ms());
        }
        Err(error) => {
            had_error = true;
            warn!(
                "[{}] fetch_positions err: {} — keeping previous cache",
                ex, error
            );
        }
    }

    if had_error {
        record_fetch_error(ex, &e_ctx);
    } else {
        record_fetch_success(ex);
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
        let normal_interval_s = 25u64;
        let all_exchanges = [
            "binance", "gate", "kucoin", "bybit", "bitget",
            "bingx", "bitmex", "ascendex", "xt", "toobit",
        ];
        let mut refresh_timer = tokio::time::interval(Duration::from_secs(normal_interval_s));
        refresh_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            refresh_timer.tick().await;
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

            // Clear positions for disabled exchanges to prevent stale ghost positions
            for ex in &all_exchanges {
                if !active_exchanges.contains(ex) {
                    e_ctx.positions.remove(*ex);
                }
            }

            let mut refreshes = FuturesUnordered::new();
            for &ex in &active_exchanges {
                if !keys.supports(ex) {
                    continue;
                }
                refreshes.push(refresh_exchange(
                    ex,
                    http.clone(),
                    keys.clone(),
                    e_ctx.clone(),
                ));
            }
            while refreshes.next().await.is_some() {
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
        }
    });
}
