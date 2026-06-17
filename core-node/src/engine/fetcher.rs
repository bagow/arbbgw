use reqwest::Client;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info, warn};

use crate::engine::exchanges::get_connector;

use crate::engine::real::ExchangeKeys;

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
                        Err(e) => error!("[balance] {} fetch error (recovery mode): {}", ex, e),
                    }
                }
                tokio::time::sleep(Duration::from_millis(fast_interval_ms)).await;
            } else {
                // Normal mode: full refresh for all active exchanges
                // Clear positions for disabled exchanges to prevent stale ghost positions
                for ex in &all_exchanges {
                    if !active_exchanges.contains(ex) {
                        let had = e_ctx.positions.remove(*ex);
                        if had.is_some() {
                            warn!("[fetcher] cleared positions for disabled exchange '{}' (active: {:?})", ex, active_exchanges);
                        }
                    }
                }

                for ex in &active_exchanges {
                    if !keys.supports(ex) {
                        continue;
                    }
                    let conn = match get_connector(ex) {
                        Some(c) => c,
                        None => continue,
                    };

                    match conn.fetch_balances(&http, &keys).await {
                        Ok((w, a)) => {
                            e_ctx.balances.insert(ex.to_string(), w);
                            e_ctx.margin_balances.insert(ex.to_string(), a);
                        }
                        Err(e) => {
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
                            warn!("[{}] fetch_positions err: {} — keeping stale cache", ex, e);
                        }
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
