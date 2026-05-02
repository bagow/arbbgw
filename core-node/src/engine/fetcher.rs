use reqwest::Client;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info, warn};

use crate::engine::real::ExchangeKeys;

/// Spawn background task that refreshes balances and fee rates.
/// Adaptive intervals:
///   - Normal mode : every 25s — full refresh (balance + fee rates) for all exchanges
///   - Recovery mode: every 2s  — balance ONLY for the exchange(s) with balance_low=true
///     Fee rates are NOT re-fetched in recovery mode to avoid rate limits.
pub fn start_balance_fetcher(
    http: Client,
    keys: Arc<ExchangeKeys>,
    e_ctx: Arc<crate::state::EngineContext>,
    settings: Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    tokio::spawn(async move {
        let fast_interval_ms = 2_000u64;
        let normal_interval_s = 25u64;
        let all_exchanges = [
            "binance", "gate", "kucoin", "bybit", "bitget",
            "bingx", "bitmex", "ascendex", "xt", "toobit",
        ];

        let binance  = crate::engine::exchanges::binance::BinanceConnector;
        let gate     = crate::engine::exchanges::gate::GateConnector;
        let kucoin   = crate::engine::exchanges::kucoin::KucoinConnector;
        let bybit    = crate::engine::exchanges::bybit::BybitConnector;
        let bitget   = crate::engine::exchanges::bitget::BitgetConnector;
        let bingx    = crate::engine::exchanges::bingx::BingxConnector;
        let bitmex   = crate::engine::exchanges::bitmex::BitmexConnector;
        let ascendex = crate::engine::exchanges::ascendex::AscendexConnector;
        let xt       = crate::engine::exchanges::xt::XtConnector;
        let toobit   = crate::engine::exchanges::toobit::ToobitConnector;
        use crate::engine::exchanges::traits::ExchangeConnector;

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
                for ex in &low_exchanges {
                    let fetch_res = match *ex {
                        "binance"  => binance.fetch_balances(&http, &keys).await.map(|(w, _a)| w).map_err(|e| e.to_string()),
                        "gate"     => gate.fetch_balances(&http, &keys).await.map(|(w, _a)| w).map_err(|e| e.to_string()),
                        "kucoin"   => kucoin.fetch_balances(&http, &keys).await.map(|(w, _a)| w).map_err(|e| e.to_string()),
                        "bybit"    => bybit.fetch_balances(&http, &keys).await.map(|(w, _a)| w).map_err(|e| e.to_string()),
                        "bitget"   => bitget.fetch_balances(&http, &keys).await.map(|(w, _a)| w).map_err(|e| e.to_string()),
                        "bingx"    => bingx.fetch_balances(&http, &keys).await.map(|(w, _a)| w).map_err(|e| e.to_string()),
                        "bitmex"   => bitmex.fetch_balances(&http, &keys).await.map(|(w, _a)| w).map_err(|e| e.to_string()),
                        "ascendex" => ascendex.fetch_balances(&http, &keys).await.map(|(w, _a)| w).map_err(|e| e.to_string()),
                        "xt"       => xt.fetch_balances(&http, &keys).await.map(|(w, _a)| w).map_err(|e| e.to_string()),
                        "toobit"   => toobit.fetch_balances(&http, &keys).await.map(|(w, _a)| w).map_err(|e| e.to_string()),
                        _ => Err("Unknown exchange".to_string()),
                    };

                    match fetch_res {
                        Ok(bal) => {
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
                for ex in &active_exchanges {
                    if !keys.supports(ex) {
                        continue;
                    }
                    match *ex {
                        "binance" => {
                            if let Ok((w, a)) = binance.fetch_balances(&http, &keys).await {
                                e_ctx.balances.insert(ex.to_string(), w);
                                e_ctx.margin_balances.insert(ex.to_string(), a);
                            }
                            if let Ok(pos) = binance.fetch_positions(&http, &keys).await {
                                e_ctx.positions.insert(ex.to_string(), pos);
                            }
                            if let Ok(fee) = binance.fetch_taker_fee(&http, &keys).await {
                                e_ctx.fee_rates.insert(ex.to_string(), fee);
                            }
                        }
                        "gate" => {
                            if let Ok((w, a)) = gate.fetch_balances(&http, &keys).await {
                                e_ctx.balances.insert(ex.to_string(), w);
                                e_ctx.margin_balances.insert(ex.to_string(), a);
                            }
                            if let Ok(pos) = gate.fetch_positions(&http, &keys).await {
                                e_ctx.positions.insert(ex.to_string(), pos);
                            }
                            if let Ok(fee) = gate.fetch_taker_fee(&http, &keys).await {
                                e_ctx.fee_rates.insert(ex.to_string(), fee);
                            }
                        }
                        "kucoin" => {
                            if let Ok((w, a)) = kucoin.fetch_balances(&http, &keys).await {
                                e_ctx.balances.insert(ex.to_string(), w);
                                e_ctx.margin_balances.insert(ex.to_string(), a);
                            }
                            if let Ok(pos) = kucoin.fetch_positions(&http, &keys).await {
                                e_ctx.positions.insert(ex.to_string(), pos);
                            }
                            if let Ok(fee) = kucoin.fetch_taker_fee(&http, &keys).await {
                                e_ctx.fee_rates.insert(ex.to_string(), fee);
                            }
                        }
                        "bitget" => {
                            if let Ok((w, a)) = bitget.fetch_balances(&http, &keys).await {
                                e_ctx.balances.insert(ex.to_string(), w);
                                e_ctx.margin_balances.insert(ex.to_string(), a);
                            }
                            if let Ok(pos) = bitget.fetch_positions(&http, &keys).await {
                                e_ctx.positions.insert(ex.to_string(), pos);
                            }
                            if let Ok(fee) = bitget.fetch_taker_fee(&http, &keys).await {
                                e_ctx.fee_rates.insert(ex.to_string(), fee);
                            }
                        }
                        "bybit" => {
                            if let Ok((w, a)) = bybit.fetch_balances(&http, &keys).await {
                                e_ctx.balances.insert(ex.to_string(), w);
                                e_ctx.margin_balances.insert(ex.to_string(), a);
                            }
                            if let Ok(pos) = bybit.fetch_positions(&http, &keys).await {
                                e_ctx.positions.insert(ex.to_string(), pos);
                            }
                            if let Ok(fee) = bybit.fetch_taker_fee(&http, &keys).await {
                                e_ctx.fee_rates.insert(ex.to_string(), fee);
                            }
                        }
                        "bingx" => {
                            if let Ok((w, a)) = bingx.fetch_balances(&http, &keys).await {
                                e_ctx.balances.insert(ex.to_string(), w);
                                e_ctx.margin_balances.insert(ex.to_string(), a);
                            }
                            if let Ok(pos) = bingx.fetch_positions(&http, &keys).await {
                                e_ctx.positions.insert(ex.to_string(), pos);
                            }
                            if let Ok(fee) = bingx.fetch_taker_fee(&http, &keys).await {
                                e_ctx.fee_rates.insert(ex.to_string(), fee);
                            }
                        }
                        "bitmex" => {
                            if let Ok((w, a)) = bitmex.fetch_balances(&http, &keys).await {
                                e_ctx.balances.insert(ex.to_string(), w);
                                e_ctx.margin_balances.insert(ex.to_string(), a);
                            }
                            if let Ok(pos) = bitmex.fetch_positions(&http, &keys).await {
                                e_ctx.positions.insert(ex.to_string(), pos);
                            }
                            if let Ok(fee) = bitmex.fetch_taker_fee(&http, &keys).await {
                                e_ctx.fee_rates.insert(ex.to_string(), fee);
                            }
                        }
                        "ascendex" => {
                            match ascendex.fetch_balances(&http, &keys).await {
                                Ok((w, a)) => {
                                    e_ctx.balances.insert(ex.to_string(), w);
                                    e_ctx.margin_balances.insert(ex.to_string(), a);
                                }
                                Err(e) => tracing::error!("[ascendex] balance err: {}", e),
                            }
                            if let Ok(pos) = ascendex.fetch_positions(&http, &keys).await {
                                e_ctx.positions.insert(ex.to_string(), pos);
                            }
                            if let Ok(fee) = ascendex.fetch_taker_fee(&http, &keys).await {
                                e_ctx.fee_rates.insert(ex.to_string(), fee);
                            }
                        }
                        "xt" => {
                            match xt.fetch_balances(&http, &keys).await {
                                Ok((w, a)) => {
                                    e_ctx.balances.insert(ex.to_string(), w);
                                    e_ctx.margin_balances.insert(ex.to_string(), a);
                                }
                                Err(e) => tracing::error!("[xt] balance err: {}", e),
                            }
                            if let Ok(pos) = xt.fetch_positions(&http, &keys).await {
                                e_ctx.positions.insert(ex.to_string(), pos);
                            }
                            if let Ok(fee) = xt.fetch_taker_fee(&http, &keys).await {
                                e_ctx.fee_rates.insert(ex.to_string(), fee);
                            }
                        }
                        "toobit" => {
                            match toobit.fetch_balances(&http, &keys).await {
                                Ok((w, a)) => {
                                    e_ctx.balances.insert(ex.to_string(), w);
                                    e_ctx.margin_balances.insert(ex.to_string(), a);
                                }
                                Err(e) => tracing::error!("[toobit] balance err: {}", e),
                            }
                            if let Ok(pos) = toobit.fetch_positions(&http, &keys).await {
                                e_ctx.positions.insert(ex.to_string(), pos);
                            }
                            if let Ok(fee) = toobit.fetch_taker_fee(&http, &keys).await {
                                e_ctx.fee_rates.insert(ex.to_string(), fee);
                            }
                        }
                        _ => {}
                    }
                }
                tokio::time::sleep(Duration::from_secs(normal_interval_s)).await;
            }
        }
    });
}
