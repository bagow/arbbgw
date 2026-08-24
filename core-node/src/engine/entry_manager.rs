use std::sync::atomic::Ordering;
use crate::engine::risk_manager::LAST_MARGIN_LOG;
use crate::engine::db::TradeRepository;
use reqwest::Client;
use std::sync::Arc;
use tokio_rusqlite::Connection;
use tracing::{error, info};
use shared::{TradeSignal, TechnicalSettings, TradingSettings};
use crate::state::EngineContext;
use crate::engine::real::*;
use crate::engine::risk_manager::*;
use crate::engine::cycle::cooldown_active;



pub async fn process_entries(
    signals: Vec<TradeSignal>,
    prices: &crate::state::PriceMap,
    settings: &TradingSettings,
    http: &Client,
    db: &Connection,
    keys: &Arc<ExchangeKeys>,
    ts: &TechnicalSettings,
    e_ctx: &Arc<EngineContext>,
    open_pairs: &std::collections::HashSet<String>,
    num_active: i64,
) {
    let current_ms = now_ms() as u64;
    // ── Open new real trades ──────────────────────────────────────────────────
    // KEY PRINCIPLE: We do NOT write to DB until BOTH legs are confirmed filled.
    // This eliminates phantom trades, $0 PnL records, and immediate open/close chaos.
    if !settings.is_active {
        return;
    } // only block NEW entries, not closes above
    if num_active >= settings.max_active_trades {
        return;
    }

    if e_ctx.is_killed.load(std::sync::atomic::Ordering::Relaxed) {
        static LAST_KILL_LOG: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let curr = now_ms() as u64;
        let last = LAST_KILL_LOG.load(std::sync::atomic::Ordering::Relaxed);
        if curr.saturating_sub(last) > 60000 {
            error!("[real] 🚨 RISK KILL-SWITCH IS ACTIVE. Engine will not open new trades until restarted.");
            LAST_KILL_LOG.store(curr, std::sync::atomic::Ordering::Relaxed);
        }
        return;
    }

    // Only open ONE trade per engine cycle to prevent burst-opening
    let mut opened_this_cycle = false;

    // Filter signals meant for current profile
    let profile_signals: Vec<_> = signals
        .into_iter()
        .filter(|s| s.profile_id == settings.id)
        .collect();

    for sig in profile_signals {
        if opened_this_cycle {
            break;
        }
        if open_pairs.contains(&sig.pair) {
            continue;
        }

        let exa = sig.exchange_a.clone();
        let exb = sig.exchange_b.clone();
        let pair = sig.pair.clone();

        let current_price = sig.entry_a;
        if current_price > 0.0 {
            let entry = e_ctx.price_history.get(&pair).map(|v| *v);
            match entry {
                Some((old_price, old_ts)) => {
                    let elapsed_ms = current_ms.saturating_sub(old_ts as u64);
                    if elapsed_ms > 2000 {
                        let velocity = (current_price - old_price).abs() / old_price;
                        if velocity > 0.003 && elapsed_ms < 10000 {
                            // >0.3% move inside 10s
                            e_ctx
                                .pair_cooldown
                                .insert(pair.clone(), current_ms as i64 + 15000);
                            error!("[real] 🌪️ MARKET REGIME ALERT: {} velocity {:.2}% in {}ms. Pausing pair for 15s to avoid volatile slippage.", pair, velocity*100.0, elapsed_ms);
                            e_ctx
                                .price_history
                                .insert(pair.clone(), (current_price, current_ms as i64));
                            continue;
                        }
                        e_ctx
                            .price_history
                            .insert(pair.clone(), (current_price, current_ms as i64));
                    }
                }
                None => {
                    e_ctx
                        .price_history
                        .insert(pair.clone(), (current_price, current_ms as i64));
                }
            }
        }

        let actual_vol_a = sig.actual_vol_a_usd;
        let actual_vol_b = sig.actual_vol_b_usd;

        let contracts_a = sig.contracts_a;
        let contracts_b = sig.contracts_b;
        let qty_a = sig.target_qty_a;
        let qty_b = sig.target_qty_b;

        let limit_price_a = sig.limit_price_a;
        let limit_price_b = sig.limit_price_b;

        let break_even_pct = sig.break_even_pct;
        let is_a_higher = sig.is_a_higher;

        let now = now_ms();

        // ── EXCHANGE BAN & HEALTH GUARD ──
        if !crate::engine::risk_manager::verify_trade_health(&exa, &exb, e_ctx) {
            continue;
        }

        // ── WS ORDER HEALTH CHECK (auto-recovery via TTL) ──
        // Skip trades involving exchanges with recently broken WS order channels.
        // unhealthy_until is a timestamp; once expired, the exchange auto-recovers.
        {
            let now_ws = now_ms() as i64;
            let unhealthy_a = e_ctx.ws_order_unhealthy_until.get(&exa).map(|v| *v > now_ws).unwrap_or(false);
            let unhealthy_b = e_ctx.ws_order_unhealthy_until.get(&exb).map(|v| *v > now_ws).unwrap_or(false);
            if unhealthy_a || unhealthy_b {
                let unhealthy = if unhealthy_a { &exa } else { &exb };
                let remaining = e_ctx.ws_order_unhealthy_until.get(unhealthy).map(|v| (*v - now_ws) / 1000).unwrap_or(0);
                tracing::info!("[real] ⏭️ SKIP {}: {} WS order channel unhealthy (auto-recovery in {}s)", pair, unhealthy, remaining);
                continue;
            }
        }

        // ── WS FRESHNESS GUARD ──
        // Skip trades if WS trading connection was just established (< 5s ago).
        // Protects against unstable post-reconnect fills (e.g. Binance IOC EXPIRED).
        {
            let fresh_a = crate::engine::utils::is_ws_too_fresh(&exa, 5_000);
            let fresh_b = crate::engine::utils::is_ws_too_fresh(&exb, 5_000);
            if fresh_a || fresh_b {
                let fresh_ex = if fresh_a { &exa } else { &exb };
                tracing::info!("[real] ⏭️ SKIP {}: {} WS just reconnected (<5s), waiting for stability", pair, fresh_ex);
                continue;
            }
        }

        // ── PRICE DATA STALENESS GUARD ──
        // Skip entry if WS price data hasn't been updated in >10s — data may be frozen
        // due to WS disconnect. Prevents trading on stale/phantom orderbooks.
        {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let stale_threshold_secs = 10u64;
            let age_a = prices.get(&exa)
                .and_then(|m| m.get(&pair).map(|d| d.last_updated))
                .map(|lu| if lu > 0 { now_secs.saturating_sub(lu) } else { 0 });
            let age_b = prices.get(&exb)
                .and_then(|m| m.get(&pair).map(|d| d.last_updated))
                .map(|lu| if lu > 0 { now_secs.saturating_sub(lu) } else { 0 });

            let stale_a = age_a.map(|a| a > stale_threshold_secs).unwrap_or(false);
            let stale_b = age_b.map(|a| a > stale_threshold_secs).unwrap_or(false);
            if stale_a || stale_b {
                let stale_ex = if stale_a { &exa } else { &exb };
                let age = if stale_a { age_a.unwrap_or(0) } else { age_b.unwrap_or(0) };
                tracing::warn!("[real] ⏭️ SKIP {}: {} price data stale ({}s old) — WS may be disconnected", pair, stale_ex, age);
                e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + 5_000);
                continue;
            }
        }

        // ── BALANCE LOW GUARD (zero-latency) ─────────────────────────────────
        if !has_sufficient_margin(&exa, actual_vol_a, settings.leverage as f64, &e_ctx, ts.margin_check_multiplier) {
            let now_m = now_ms() as i64;
            if now_m - LAST_MARGIN_LOG.load(Ordering::Relaxed) > 10000 {
                let avail = e_ctx.margin_balances.get(&exa).map(|v| *v).unwrap_or(-1.0);
                let mult = if ts.margin_check_multiplier > 0.0 { ts.margin_check_multiplier } else { 1.2 };
                let req = actual_vol_a / (settings.leverage as f64).max(1.0) * mult;
                info!("[real] SKIP {}: insufficient margin {} (avail=${:.2}, required=${:.2}, notional=${:.2}, lev={}x)", pair, exa, avail, req, actual_vol_a, settings.leverage);
                LAST_MARGIN_LOG.store(now_m, Ordering::Relaxed);
            }
            continue;
        }
        if !has_sufficient_margin(&exb, actual_vol_b, settings.leverage as f64, &e_ctx, ts.margin_check_multiplier) {
            let now_m = now_ms() as i64;
            if now_m - LAST_MARGIN_LOG.load(Ordering::Relaxed) > 10000 {
                let avail = e_ctx.margin_balances.get(&exb).map(|v| *v).unwrap_or(-1.0);
                let mult = if ts.margin_check_multiplier > 0.0 { ts.margin_check_multiplier } else { 1.2 };
                let req = actual_vol_b / (settings.leverage as f64).max(1.0) * mult;
                info!("[real] SKIP {}: insufficient margin {} (avail=${:.2}, required=${:.2}, notional=${:.2}, lev={}x)", pair, exb, avail, req, actual_vol_b, settings.leverage);
                LAST_MARGIN_LOG.store(now_m, Ordering::Relaxed);
            }
            continue;
        }

        // Leverage is pre-warmed asynchronously via `real.rs:warmup_leverages`.
        // DO NOT call ensure_leverage here, it introduces 2+ seconds of latency!

        // ── PAIR COOLDOWN RE-CHECK (defense-in-depth) ──
        // Scanner checks cooldown too, but signals are batched — a second signal for
        // the same pair could sneak through the same scan cycle. Block it here.
        {
            let cd_val = e_ctx.pair_cooldown.get(&pair).map(|v| *v).unwrap_or(0);
            if cooldown_active(cd_val, now, ts.pair_cooldown_ms) {
                continue;
            }
        }

        let (_conn_ensure_a, _conn_ensure_b) = (
            crate::engine::exchanges::get_connector(&exa).unwrap(),
            crate::engine::exchanges::get_connector(&exb).unwrap(),
        );
        let _t0 = std::time::Instant::now();
        let use_ioc = ts.entry_order_type == "ioc";
        // Sequential IOC slippage: allows the primary leader leg (Binance) to fill reliably
        // within the available profit margin (spread - BE - min_profit).
        // Phase 2 automatically recalculates remaining spread from Leg 1's actual fill price.
        let ioc_slippage_pct = (sig.spread - break_even_pct - 0.10)
            .max(0.25)
            .min(1.0);
        // NOTE: FIRE log moved to after L2/depth checks to avoid false-positive log entries

        // ── PRE-FORMAT CACHE KEYS ──
        let key_a = format!("{}:{}", exa, pair);
        let key_b = format!("{}:{}", exb, pair);

        // ── BOTH LEGS SIMULTANEOUSLY ─────────────────────────────────────────
        let mult_a = e_ctx.multipliers.get(&key_a).map(|v| *v).unwrap_or(1.0);
        let mult_b = e_ctx.multipliers.get(&key_b).map(|v| *v).unwrap_or(1.0);

        let tick_a = e_ctx.tick_sizes.get(&key_a).map(|v| *v).unwrap_or(0.000001);
        let tick_b = e_ctx.tick_sizes.get(&key_b).map(|v| *v).unwrap_or(0.000001);

        // ── L1 PRE-FILTER (skip REST fetch if WS L1 already shows insufficient depth) ──
        {
            let l1_a = prices.get(&exa).and_then(|m| m.get(&pair).cloned());
            let l1_b = prices.get(&exb).and_then(|m| m.get(&pair).cloned());
            if let (Some(da), Some(db)) = (&l1_a, &l1_b) {
                let l1_depth_a_usd = if !is_a_higher { da.ask_size } else { da.bid_size };
                let l1_depth_b_usd = if is_a_higher { db.ask_size } else { db.bid_size };
                let required_usd = settings.trade_volume;
                if l1_depth_a_usd < required_usd * 0.5 || l1_depth_b_usd < required_usd * 0.5 {
                    tracing::warn!(
                        "[real] ⏭️ SKIP {} L1 depth too shallow: A={:.1}$ B={:.1}$ < {:.0}$",
                        pair, l1_depth_a_usd, l1_depth_b_usd, required_usd * 0.5
                    );
                    // Cooldown 5s to avoid re-checking the same thin pair every tick
                    e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + 5_000);
                    continue;
                }
            }
        }

        // ── L2 ORDERBOOK VERIFICATION (WS cache only, zero REST) ─────────────
        let cached_a = e_ctx.l2_books.get(&key_a).map(|v| v.clone());
        let cached_b = e_ctx.l2_books.get(&key_b).map(|v| v.clone());
        let now_cache = now_ms();

        // Helper: check if L2 is fresh, or build synthetic from L1 if L2 was never received.
        // Exchanges without L2 WS (gate, bitget, kucoin, bingx, xt) never populate l2_books,
        // so cached is None. We allow entry using L1 bid/ask (already validated in scanner).
        // If L2 EXISTS but is STALE (>5s) — still block (data was live but stopped updating).
        let make_l2 = |cached: &Option<crate::state::L2Snapshot>, ex_name: &str, price_data: &crate::state::TickerData| -> Option<(Vec<(f64, f64)>, Vec<(f64, f64)>)> {
            match cached {
                Some(snap) if (now_cache - snap.updated_ms) < 2_000 => {
                    let l2_best_bid = snap.bids.first().map(|x| x.0).unwrap_or(0.0);
                    let l2_best_ask = snap.asks.first().map(|x| x.0).unwrap_or(0.0);
                    let diff_bid = if price_data.bid > 0.0 && l2_best_bid > 0.0 {
                        ((l2_best_bid - price_data.bid) / price_data.bid).abs()
                    } else { 0.0 };
                    let diff_ask = if price_data.ask > 0.0 && l2_best_ask > 0.0 {
                        ((l2_best_ask - price_data.ask) / price_data.ask).abs()
                    } else { 0.0 };

                    if diff_bid > 0.002 || diff_ask > 0.002 {
                        // L2 orderbook is lagging behind the live L1 ticker stream — use fresh L1
                        tracing::debug!("[real] L2 diverged from L1 for {} {} (bid_diff={:.4}%, ask_diff={:.4}%), using fresh L1", pair, ex_name, diff_bid*100.0, diff_ask*100.0);
                        if price_data.bid > 0.0 && price_data.ask > 0.0 {
                            let bid_usd = price_data.bid_size;
                            let ask_usd = price_data.ask_size;
                            let bid_coins = if price_data.bid > 0.0 { bid_usd / price_data.bid } else { 0.0 };
                            let ask_coins = if price_data.ask > 0.0 { ask_usd / price_data.ask } else { 0.0 };
                            Some((vec![(price_data.bid, bid_coins)], vec![(price_data.ask, ask_coins)]))
                        } else {
                            None
                        }
                    } else {
                        Some((snap.bids.clone(), snap.asks.clone()))
                    }
                }
                Some(snap) => {
                    // L2 exists but stale (>2s) — fallback to fresh L1 ticker
                    tracing::debug!("[real] L2 stale for {} {} (age={}ms), using fresh L1", pair, ex_name, now_cache - snap.updated_ms);
                    if price_data.bid > 0.0 && price_data.ask > 0.0 {
                        let bid_usd = price_data.bid_size;
                        let ask_usd = price_data.ask_size;
                        let bid_coins = if price_data.bid > 0.0 { bid_usd / price_data.bid } else { 0.0 };
                        let ask_coins = if price_data.ask > 0.0 { ask_usd / price_data.ask } else { 0.0 };
                        Some((vec![(price_data.bid, bid_coins)], vec![(price_data.ask, ask_coins)]))
                    } else {
                        None
                    }
                }
                None => {
                    // No L2 at all — build synthetic from L1 (bid/ask already passed scanner filters)
                    if price_data.bid > 0.0 && price_data.ask > 0.0 {
                        let bid_usd = price_data.bid_size; // already in USD from collectors
                        let ask_usd = price_data.ask_size;
                        let bid_coins = if price_data.bid > 0.0 { bid_usd / price_data.bid } else { 0.0 };
                        let ask_coins = if price_data.ask > 0.0 { ask_usd / price_data.ask } else { 0.0 };
                        Some((vec![(price_data.bid, bid_coins)], vec![(price_data.ask, ask_coins)]))
                    } else {
                        None
                    }
                }
            }
        };

        let data_a_ref = prices.get(&exa).and_then(|m| m.get(&pair).cloned());
        let data_b_ref = prices.get(&exb).and_then(|m| m.get(&pair).cloned());

        let ob_a = data_a_ref.as_ref().and_then(|d| make_l2(&cached_a, &exa, d));
        let ob_b = data_b_ref.as_ref().and_then(|d| make_l2(&cached_b, &exb, d));

        let (ob_a_res, ob_b_res) = match (ob_a, ob_b) {
            (Some(a), Some(b)) => (Ok(a), Ok(b)),
            _ => {
                let a_age = cached_a.as_ref().map(|a| now_cache - a.updated_ms).unwrap_or(-1);
                let b_age = cached_b.as_ref().map(|b| now_cache - b.updated_ms).unwrap_or(-1);
                tracing::info!("[real] ⏭️ SKIP {} — no fresh L2 WS data for {} (age: {}ms) / {} (age: {}ms)", pair, exa, a_age, exb, b_age);
                e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + 3_000);
                continue;
            }
        };

        let analyze_depth = |ob_res: Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), String>,
                             is_buy: bool,
                             limit_price: f64,
                             _slippage_pct: f64|
         -> f64 {
            if let Ok((bids, asks)) = ob_res {
                let levels = if is_buy { asks } else { bids };
                let mut cum_vol = 0.0;
                // Use a wide tolerance (2%) for depth existence check.
                // This is NOT a fill-price check — VWAP verification handles that.
                // slippage_percent (0.05%) was causing false 0.0 depth when L2
                // prices diverged even slightly from L1 scan-time prices.
                let depth_tolerance_pct = 2.0;
                let actual_limit = if limit_price > 0.0 {
                    if is_buy {
                        limit_price * (1.0 + depth_tolerance_pct / 100.0)
                    } else {
                        limit_price * (1.0 - depth_tolerance_pct / 100.0)
                    }
                } else {
                    0.0
                };
                
                for (px, sz) in levels {
                    if actual_limit > 0.0 {
                        if is_buy && px > actual_limit {
                            continue;
                        }
                        if !is_buy && px < actual_limit {
                            continue;
                        }
                    }
                    cum_vol += sz; // Collectors natively store size in coins
                }
                cum_vol
            } else {
                -1.0
            }
        };

        let depth_a = analyze_depth(ob_a_res, !is_a_higher, limit_price_a, settings.slippage_percent);
        let depth_b = analyze_depth(ob_b_res, is_a_higher, limit_price_b, settings.slippage_percent);

        if depth_a < qty_a || depth_b < qty_b {
            tracing::warn!("[real] 🛑 ABORT {}: Insufficient L2 depth! req A={} (has {:.2}), req B={} (has {:.2})", pair, qty_a, depth_a, qty_b, depth_b);
            // Cooldown 5s after abort — prevents hammering the same illiquid pair
            e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + 5_000);
            continue;
        }

        // ── VWAP ENTRY VERIFICATION ───────────────────────────────────────────
        // Check that the VWAP spread (actual fill prices) is still above break-even.
        // This prevents opening structurally loss-making positions.
        {
            // NOTE: L2 books already store sizes in coins (collectors apply
            // multiplier at parse time for Gate/KuCoin). Do NOT re-apply mult here.
            if let (Some(ref ca), Some(ref cb)) = (&cached_a, &cached_b) {
                // Entry: A = short (sell) if is_a_higher, B = long (buy) if is_a_higher
                let ob_a_res = Ok((ca.bids.clone(), ca.asks.clone()));
                let ob_b_res = Ok((cb.bids.clone(), cb.asks.clone()));
                
                let vwap_a = crate::engine::execution::verify_l2_vwap(ob_a_res, !is_a_higher, qty_a, 1.0);
                let vwap_b = crate::engine::execution::verify_l2_vwap(ob_b_res, is_a_higher, qty_b, 1.0);

                if let (Some(pa), Some(pb)) = (vwap_a, vwap_b) {
                    let vwap_spread = if is_a_higher {
                        (pa - pb) / pb.max(1e-12) * 100.0
                    } else {
                        (pb - pa) / pa.max(1e-12) * 100.0
                    };

                    // VWAP threshold = max(break_even, min_entry_spread)
                    // User's min_entry_spread setting always takes priority over calculated BE
                    let vwap_threshold = break_even_pct.max(settings.min_entry_spread as f64);

                    if vwap_spread < vwap_threshold {
                        tracing::warn!(
                            "[real] 🛑 ABORT {}: VWAP spread {:.4}% < порог {:.4}% (L1={:.4}%, BE={:.4}%, min_spread={:.2}%)",
                            pair, vwap_spread, vwap_threshold, sig.spread, break_even_pct, settings.min_entry_spread
                        );
                        e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + 5_000);
                        continue;
                    }
                    tracing::info!("[real] ✅ VWAP OK {}: {:.4}% > порог {:.4}%", pair, vwap_spread, vwap_threshold);
                }
            }
        }

        // ── ALL CHECKS PASSED — log FIRE and execute ─────────────────────────

        // ── MAKER MODE: submit Post-Only and return ─────────────────────────
        if ts.entry_order_type == "maker" {
            // Skip if pending maker already exists for this pair
            if e_ctx.pending_makers.contains_key(&pair) {
                continue;
            }

            info!(
                "[real] FIRE {} [MAKER] A={} c={} | B={} c={} spread={:.4}% be={:.4}%",
                pair, exa, contracts_a, exb, contracts_b, sig.spread, break_even_pct
            );

            let snapshot = serde_json::json!({
                "bid_a": sig.entry_a, "bid_b": sig.entry_b,
                "ask_a": sig.entry_a, "ask_b": sig.entry_b,
                "spread": sig.spread, "latency": 0, "mode": "maker"
            }).to_string();
            tokio::spawn({
                let snap_repo = TradeRepository::new(db.clone());
                let sp_pair = pair.clone(); let sp_exa = exa.clone(); let sp_exb = exb.clone();
                let ts_ms = current_ms as i64;
                async move { snap_repo.insert_snapshot(sp_pair, sp_exa, sp_exb, snapshot, ts_ms).await; }
            });

            crate::engine::maker_manager::submit_maker_entry(
                &sig, prices, settings, http, keys, ts, e_ctx,
            ).await;
            opened_this_cycle = true;
            continue;
        }

        // ── LIVE SPREAD PRE-FLIGHT RE-CHECK (Instant in-memory validation) ──
        let mut final_limit_a = limit_price_a;
        let mut final_limit_b = limit_price_b;
        let mut final_slip = ioc_slippage_pct;
        let mut final_entry_a = sig.entry_a;
        let mut final_entry_b = sig.entry_b;
        let mut final_spread = sig.spread;

        if use_ioc {
            if let (Some(da), Some(db)) = (
                prices.get(&exa).and_then(|m| m.get(&pair).cloned()),
                prices.get(&exb).and_then(|m| m.get(&pair).cloned())
            ) {
                let live_a = if is_a_higher { da.bid } else { da.ask };
                let live_b = if is_a_higher { db.ask } else { db.bid };
                if live_a > 0.0 && live_b > 0.0 {
                    let live_spread = if is_a_higher {
                        (live_a - live_b) / live_b.max(1e-12) * 100.0
                    } else {
                        (live_b - live_a) / live_a.max(1e-12) * 100.0
                    };
                    // If spread already dropped below break-even + profit buffer, abort cleanly
                    if live_spread < break_even_pct + 0.10 {
                        tracing::info!(
                            "[real] ⏭️ PRE-FLIGHT ABORT {}: spread evaporated (live={:.4}% < min_req={:.4}%, was {:.4}%)",
                            pair, live_spread, break_even_pct + 0.10, sig.spread
                        );
                        e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + 3_000);
                        continue;
                    }
                    final_limit_a = live_a;
                    final_limit_b = live_b;
                    final_entry_a = live_a;
                    final_entry_b = live_b;
                    final_spread = live_spread;
                    final_slip = (live_spread - break_even_pct - 0.10).max(0.20).min(1.0);
                }
            }
        }

        // ── IOC / MARKET MODE: existing parallel entry ──────────────────────
        let order_type_label_fire = if use_ioc { "IOC" } else { "MKT" };
        info!(
            "[real] FIRE {} [{}] A={} c={} lim={:.6} | B={} c={} lim={:.6} spread={:.4}% be={:.4}% slip={:.3}% dir={} is_a_hi={}",
            pair, order_type_label_fire, exa, contracts_a, final_limit_a,
            exb, contracts_b, final_limit_b, final_spread, break_even_pct, final_slip,
            sig.direction, is_a_higher
        );
        let snapshot = serde_json::json!({
            "bid_a": final_entry_a, "bid_b": final_entry_b,
            "ask_a": final_entry_a, "ask_b": final_entry_b,
            "spread": final_spread, "latency": 0
        }).to_string();
        tokio::spawn({
            let snap_repo = TradeRepository::new(db.clone());
            let sp_pair = pair.clone(); let sp_exa = exa.clone(); let sp_exb = exb.clone();
            let ts_ms = current_ms as i64;
            async move { snap_repo.insert_snapshot(sp_pair, sp_exa, sp_exb, snapshot, ts_ms).await; }
        });

        let ctx = crate::engine::execution::SequentialEntryContext {
            pair: pair.clone(),
            exa: exa.clone(),
            exb: exb.clone(),
            is_a_higher,
            qty_a,
            qty_b,
            limit_price_a: final_limit_a,
            limit_price_b: final_limit_b,
            mult_a,
            mult_b,
            tick_a,
            tick_b,
            ioc_slippage_pct: final_slip,
            leverage: settings.leverage as f64,
            dry_run: false,
            sig_spread: final_spread,
            sig_structural_spread: sig.structural_spread,
            sig_direction: sig.direction.clone(),
            sig_entry_a: final_entry_a,
            sig_entry_b: final_entry_b,
            profile_id: settings.id.clone(),
            break_even_pct,
        };

        if crate::engine::execution::execute_sequential_entry(
            ctx,
            http,
            keys,
            db,
            e_ctx,
            ts,
            true, // sort_by_liquidity: fast/strict exchange first
        ).await {
            opened_this_cycle = true;
        }
    }
}
