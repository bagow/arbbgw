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

/// Static liquidity ranking for exchanges.
/// Lower = more liquid. Used to determine entry order (less liquid first).
fn liquidity_rank(exchange: &str) -> u8 {
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
) {
    let dry_run = false;
    let repo = TradeRepository::new(db.clone());
    let current_ms = now_ms() as u64;
    let num_active = repo.load_open_trades(&settings.id).await.len() as i64;
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

        let entry_a = sig.entry_a;
        let entry_b = sig.entry_b;

        let break_even_pct = sig.break_even_pct;
        let is_a_higher = sig.is_a_higher;

        let now = now_ms();

        // ── EXCHANGE BAN GUARD ──
        if is_exchange_banned(&exa)
            || is_exchange_banned(&exb)
        {
            continue;
        }

        // ── BALANCE LOW GUARD (zero-latency) ─────────────────────────────────
        if !has_sufficient_margin(&exa, actual_vol_a, settings.leverage as f64, &e_ctx) {
            let now_m = now_ms() as i64;
            if now_m - LAST_MARGIN_LOG.load(Ordering::Relaxed) > 10000 {
                let avail = e_ctx.margin_balances.get(&exa).map(|v| *v).unwrap_or(-1.0);
                let req = actual_vol_a / (settings.leverage as f64).max(1.0) * 1.5;
                info!("[real] SKIP {}: insufficient margin {} (avail=${:.2}, required=${:.2}, notional=${:.2}, lev={}x)", pair, exa, avail, req, actual_vol_a, settings.leverage);
                LAST_MARGIN_LOG.store(now_m, Ordering::Relaxed);
            }
            continue;
        }
        if !has_sufficient_margin(&exb, actual_vol_b, settings.leverage as f64, &e_ctx) {
            let now_m = now_ms() as i64;
            if now_m - LAST_MARGIN_LOG.load(Ordering::Relaxed) > 10000 {
                let avail = e_ctx.margin_balances.get(&exb).map(|v| *v).unwrap_or(-1.0);
                let req = actual_vol_b / (settings.leverage as f64).max(1.0) * 1.5;
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

        let (conn_ensure_a, conn_ensure_b) = (
            crate::engine::exchanges::get_connector(&exa).unwrap(),
            crate::engine::exchanges::get_connector(&exb).unwrap(),
        );
        let t0 = std::time::Instant::now();
        let use_ioc = ts.entry_order_type == "ioc";
        let order_type_label = if use_ioc { "IOC" } else { "MKT" };
        // Dynamic IOC slippage: (signal_spread - break_even) / 2
        // This guarantees profitability: even if BOTH legs get worst-case fill,
        // total slippage = 2 * per_leg ≤ (spread - BE) → net PnL ≥ 0
        // Clamped: min 0.05% (tick rounding), max 0.5% (safety cap)
        let ioc_slippage_pct = ((sig.spread - break_even_pct) / 2.0)
            .max(0.05)
            .min(0.5);
        // NOTE: FIRE log moved to after L2/depth checks to avoid false-positive log entries

        // ── BOTH LEGS SIMULTANEOUSLY ─────────────────────────────────────────
        let mult_a = e_ctx
            .multipliers
            .get(&format!("{}:{}", exa, pair))
            .map(|v| *v)
            .unwrap_or(1.0);
        let mult_b = e_ctx
            .multipliers
            .get(&format!("{}:{}", exb, pair))
            .map(|v| *v)
            .unwrap_or(1.0);

        let tick_a = e_ctx
            .tick_sizes
            .get(&format!("{}:{}", exa, pair))
            .map(|v| *v)
            .unwrap_or(0.000001);
        let tick_b = e_ctx
            .tick_sizes
            .get(&format!("{}:{}", exb, pair))
            .map(|v| *v)
            .unwrap_or(0.000001);

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
        let l2_key_a = format!("{}:{}", exa, pair);
        let l2_key_b = format!("{}:{}", exb, pair);
        let cached_a = e_ctx.l2_books.get(&l2_key_a).map(|v| v.clone());
        let cached_b = e_ctx.l2_books.get(&l2_key_b).map(|v| v.clone());
        let now_cache = now_ms();

        // L2 cache must exist AND be fresh (< 5s). No REST fallback.
        let (ob_a_res, ob_b_res) = match (&cached_a, &cached_b) {
            (Some(a), Some(b)) if (now_cache - a.updated_ms) < 5_000 && (now_cache - b.updated_ms) < 5_000 => {
                (Ok((a.bids.clone(), a.asks.clone())), Ok((b.bids.clone(), b.asks.clone())))
            },
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
            let compute_vwap = |book: &crate::state::L2Snapshot, is_buy: bool, required_coins: f64| -> Option<f64> {
                let levels = if is_buy { &book.asks } else { &book.bids };
                let mut cum_vol = 0.0;
                let mut cum_cost = 0.0;
                for &(px, sz) in levels {
                    let remaining = required_coins - cum_vol;
                    if remaining <= 0.0 { break; }
                    let take = if sz > remaining { remaining } else { sz };
                    cum_vol += take;
                    cum_cost += take * px;
                }
                if cum_vol >= required_coins * 0.99 {
                    Some(cum_cost / cum_vol)
                } else {
                    None
                }
            };

            if let (Some(ref ca), Some(ref cb)) = (&cached_a, &cached_b) {
                // Entry: A = short (sell) if is_a_higher, B = long (buy) if is_a_higher
                let vwap_a = compute_vwap(ca, !is_a_higher, qty_a);
                let vwap_b = compute_vwap(cb, is_a_higher, qty_b);

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
        info!(
            "[real] FIRE {} [{}] A={} c={} lim={:.6} | B={} c={} lim={:.6} spread={:.4}% be={:.4}% slip={:.3}%",
            pair, order_type_label, exa, contracts_a, limit_price_a,
            exb, contracts_b, limit_price_b, sig.spread, break_even_pct, ioc_slippage_pct
        );
        let snapshot = serde_json::json!({
            "bid_a": sig.entry_a, "bid_b": sig.entry_b,
            "ask_a": sig.entry_a, "ask_b": sig.entry_b,
            "spread": sig.spread, "latency": 0
        }).to_string();
        tokio::spawn({
            let snap_repo = TradeRepository::new(db.clone());
            let sp_pair = pair.clone(); let sp_exa = exa.clone(); let sp_exb = exb.clone();
            let ts_ms = current_ms as i64;
            async move { snap_repo.insert_snapshot(sp_pair, sp_exa, sp_exb, snapshot, ts_ms).await; }
        });

        // ── SEQUENTIAL ORDER EXECUTION (less liquid leg first) ─────────────────
        // Strategy: open the LESS LIQUID exchange first. If it doesn't fill,
        // skip the trade entirely — zero exposure, zero loss.
        // Only if the less liquid leg fills, we open the more liquid leg.
        // This eliminates asymmetric fills that cause 100% of current losses.
        opened_this_cycle = true;

        let rank_a = liquidity_rank(&exa);
        let rank_b = liquidity_rank(&exb);
        let a_is_less_liquid = rank_a >= rank_b;

        let (first_label, second_label) = if a_is_less_liquid {
            (exa.as_str(), exb.as_str())
        } else {
            (exb.as_str(), exa.as_str())
        };

        info!(
            "[real] 🔄 Последовательное исполнение: сначала {} (менее ликв.), затем {} для {}...",
            first_label, second_label, pair
        );

        let t0_exec = std::time::Instant::now();

        // ── PHASE 1: Less liquid leg ─────────────────────────────────────────
        let (first_conn, first_is_buy, first_qty, first_limit, first_mult, first_tick) = if a_is_less_liquid {
            (&conn_ensure_a, !is_a_higher, qty_a, limit_price_a, mult_a, tick_a)
        } else {
            (&conn_ensure_b, is_a_higher, qty_b, limit_price_b, mult_b, tick_b)
        };

        let res_first = first_conn.place_order(
            &http, &keys, &pair, first_is_buy, first_qty, dry_run,
            first_limit, first_limit > 0.0,
            settings.leverage as f64, first_mult, ioc_slippage_pct, first_tick, ts.api_timeout_ms
        ).await;

        let (oid_first, fill_first, fee_first) = match res_first {
            Ok(t) => (t.0, t.1, t.2),
            Err(e) => {
                error!("[real] LEG_FIRST reject {}/{}: {}", first_label, pair, e);
                let esl = e.to_string().to_lowercase();
                if esl.contains("insufficient") || esl.contains("balance") || esl.contains("margin") || esl.contains("exceed") {
                    mark_balance_low(first_label, &e_ctx);
                }
                (None, 0.0, 0.0)
            }
        };

        // Verify first leg fill
        let first_phys_init = if fill_first > 0.0 { Some(first_qty) } else { None };
        let first_max_retries = if use_ioc && fill_first <= 0.0 { 5_i64 } else { ts.fill_verify_retries };

        let ver_first = crate::engine::execution::verify_fill(
            first_conn, &http, &keys, &pair, &oid_first,
            fill_first, fee_first, first_phys_init, first_qty, dry_run,
            crate::engine::execution::VerifyConfig {
                fast_retries: first_max_retries,
                fast_delay_ms: ts.fill_verify_delay_ms as u64,
                slow_retries: 8,
                slow_delay_ms: 500,
                position_fallback: true,
                mult: first_mult,
            },
            "real",
        ).await;

        let first_phys = ver_first.phys_volume.unwrap_or(0.0);

        // If less liquid leg didn't fill → skip entirely, no exposure
        if first_phys <= 1e-12 {
            info!(
                "[real] ⏭️ {} IOC не заполнен для {} — нет экспозиции, пропускаем ({}ms)",
                first_label, pair, t0_exec.elapsed().as_millis()
            );
            e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + 10_000);
            continue;
        }

        info!(
            "[real] ✅ Первая нога {} заполнена: price={:.6} qty={:.6}. Открываем {}...",
            first_label, ver_first.fill_price, first_phys, second_label
        );

        // ── PHASE 2: More liquid leg (should fill easily) ────────────────────
        let (second_conn, second_is_buy, second_qty, second_limit, second_mult, second_tick) = if a_is_less_liquid {
            (&conn_ensure_b, is_a_higher, qty_b, limit_price_b, mult_b, tick_b)
        } else {
            (&conn_ensure_a, !is_a_higher, qty_a, limit_price_a, mult_a, tick_a)
        };

        let res_second = second_conn.place_order(
            &http, &keys, &pair, second_is_buy, second_qty, dry_run,
            second_limit, second_limit > 0.0,
            settings.leverage as f64, second_mult, ioc_slippage_pct, second_tick, ts.api_timeout_ms
        ).await;

        let (oid_second, fill_second, fee_second) = match res_second {
            Ok(t) => (t.0, t.1, t.2),
            Err(e) => {
                error!("[real] LEG_SECOND reject {}/{}: {}", second_label, pair, e);
                let esl = e.to_string().to_lowercase();
                if esl.contains("insufficient") || esl.contains("balance") || esl.contains("margin") || esl.contains("exceed") {
                    mark_balance_low(second_label, &e_ctx);
                }
                (None, 0.0, 0.0)
            }
        };

        // Verify second leg fill
        let second_phys_init = if fill_second > 0.0 { Some(second_qty) } else { None };
        let second_max_retries = if use_ioc && fill_second <= 0.0 { 5_i64 } else { ts.fill_verify_retries };

        let ver_second = crate::engine::execution::verify_fill(
            second_conn, &http, &keys, &pair, &oid_second,
            fill_second, fee_second, second_phys_init, second_qty, dry_run,
            crate::engine::execution::VerifyConfig {
                fast_retries: second_max_retries,
                fast_delay_ms: ts.fill_verify_delay_ms as u64,
                slow_retries: 8,
                slow_delay_ms: 500,
                position_fallback: true,
                mult: second_mult,
            },
            "real",
        ).await;

        let total_exec_ms = t0_exec.elapsed().as_millis();

        // Latency checks
        if total_exec_ms > 800 {
            let mut strikes_a = e_ctx.high_latency_strikes.entry(exa.clone()).or_insert(0);
            *strikes_a += 1;
            if *strikes_a >= 3 {
                e_ctx.exchange_cooldown.insert(exa.clone(), now_ms() + 60_000);
                error!("[real] 🐢 LATENCY ALERT: {} RTT >800ms. Cooldown 1m.", exa);
                *strikes_a = 0;
            }

            let mut strikes_b = e_ctx.high_latency_strikes.entry(exb.clone()).or_insert(0);
            *strikes_b += 1;
            if *strikes_b >= 3 {
                e_ctx.exchange_cooldown.insert(exb.clone(), now_ms() + 60_000);
                error!("[real] 🐢 LATENCY ALERT: {} RTT >800ms. Cooldown 1m.", exb);
                *strikes_b = 0;
            }
        } else {
            e_ctx.high_latency_strikes.insert(exa.clone(), 0);
            e_ctx.high_latency_strikes.insert(exb.clone(), 0);
        }

        // Map first/second results back to A/B
        let (oid_a, oid_b);
        let (mut fill_a, fee_a_val, actual_vol_phys_a);
        let (mut fill_b, fee_b_val, actual_vol_phys_b);
        if a_is_less_liquid {
            oid_a = oid_first;
            fill_a = ver_first.fill_price;
            fee_a_val = ver_first.fee;
            actual_vol_phys_a = ver_first.phys_volume;
            oid_b = oid_second;
            fill_b = ver_second.fill_price;
            fee_b_val = ver_second.fee;
            actual_vol_phys_b = ver_second.phys_volume;
        } else {
            oid_a = oid_second;
            fill_a = ver_second.fill_price;
            fee_a_val = ver_second.fee;
            actual_vol_phys_a = ver_second.phys_volume;
            oid_b = oid_first;
            fill_b = ver_first.fill_price;
            fee_b_val = ver_first.fee;
            actual_vol_phys_b = ver_first.phys_volume;
        }


        // 4. Handle asymmetry (Trimming)
        let phys_a = actual_vol_phys_a.unwrap_or(0.0);
        let phys_b = actual_vol_phys_b.unwrap_or(0.0);

        if !dry_run {
            let diff = (phys_a - phys_b).abs();
            // Use 1% margin for rounding errors
            if diff > phys_a.max(phys_b) * 0.01 && diff > 1e-9 {
                if phys_a > phys_b {
                    error!("[real] ⚠️ ASYMMETRIC FILL {}: A ({}) filled {}, B ({}) filled {}. Trimming A by {}.", pair, exa, phys_a, exb, phys_b, diff);
                    let hc = http.clone(); let kc = Arc::clone(&keys); let p = pair.clone(); let ex = exa.clone();
                    let fall = sig.entry_a; let at = ts.api_timeout_ms;
                    tokio::spawn(async move {
                        let _ = crate::engine::exchanges::get_connector(&ex).unwrap()
                            .trim_position(&hc, &kc, &p, !is_a_higher, diff, dry_run, mult_a, tick_a, fall, at).await;
                    });
                } else {
                    error!("[real] ⚠️ ASYMMETRIC FILL {}: B ({}) filled {}, A ({}) filled {}. Trimming B by {}.", pair, exb, phys_b, exa, phys_a, diff);
                    let hc = http.clone(); let kc = Arc::clone(&keys); let p = pair.clone(); let ex = exb.clone();
                    let fall = sig.entry_b; let at = ts.api_timeout_ms;
                    tokio::spawn(async move {
                        let _ = crate::engine::exchanges::get_connector(&ex).unwrap()
                            .trim_position(&hc, &kc, &p, is_a_higher, diff, dry_run, mult_b, tick_b, fall, at).await;
                    });
                }
                e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + ts.asymmetric_cooldown_ms as i64);
            }
        }


        let total_ms = t0.elapsed().as_millis();
        if dry_run {
            fill_a = entry_a;
            fill_b = entry_b;
        }

        let phys_a = actual_vol_phys_a.unwrap_or(0.0);
        let phys_b = actual_vol_phys_b.unwrap_or(0.0);

        let a_filled = oid_a.is_some() && phys_a > 0.0;
        let b_filled = oid_b.is_some() && phys_b > 0.0;

        info!("[real] RESULT {} A={} B={} phys_a={:.6} phys_b={:.6} fill_a={:.6} fill_b={:.6} latency={}ms",
            pair, a_filled, b_filled, phys_a, phys_b, fill_a, fill_b, total_ms);

        if !a_filled && !b_filled {
            info!(
                "[real] Both IOC unfilled {} — no exposure ({}ms)",
                pair, total_ms
            );
            // Both unfilled = safe outcome (no position, no risk).
            // With tight 0.05% IOC this is common — NOT a failure.
            // Cooldown this pair briefly to avoid re-trying the same thin spread.
            e_ctx.pair_cooldown.insert(pair.clone(), now_ms() + 10_000);
            continue;
        }

        let mv = crate::engine::execution::match_volumes(phys_a, phys_b, mult_a, mult_b);
        let matched_phys = mv.matched_phys;
        let delta_a = mv.delta_a;
        let delta_b = mv.delta_b;

        // ── DELTA TRIMMING (Phase 3) ──
        let mut trimming_loss = 0.0;
        let mut trimming_fee = 0.0;
        let (hc, kc, _db_c) = (http.clone(), Arc::clone(&keys), db.clone());
        let fp = pair.clone();

        if delta_a > 1e-12 {
            let phys_to_dump = delta_a; // Pass physical volume directly
            if phys_to_dump >= mult_a.min(1e-8) {
                let is_buy = is_a_higher; // If we entered A by Selling (!is_a_higher), we exit A by Buying (is_a_higher)
                error!(
                    "[real] ⚠️ TRIMMING {} LEG A — Selling excess {:.6} phys",
                    pair, phys_to_dump
                );

                let tick_for_close = e_ctx
                    .tick_sizes
                    .get(&format!("{}:{}", exa, pair))
                    .map(|v| *v)
                    .unwrap_or(0.000001);
                match crate::engine::exchanges::get_connector(&exa)
                    .unwrap()
                    .trim_position(
                        &hc,
                        &kc,
                        &fp,
                        is_buy,
                        phys_to_dump,
                        false,
                        mult_a,
                        tick_for_close,
                        fill_a,
                        ts.api_timeout_ms,
                    )
                    .await
                {
                    Ok(rc) => {
                        trimming_fee += rc.total_fee;
                        // If exchange didn't report fill price (0), assume closed at entry → loss ≈ 0.
                        // Without this guard, final_price=0 makes the entire position look like a 100% loss.
                        let close_price = if rc.final_price > 1e-12 { rc.final_price } else { fill_a };
                        let gross_loss = if is_buy {
                            (fill_a - close_price) * delta_a
                        } else {
                            (close_price - fill_a) * delta_a
                        };
                        trimming_loss += gross_loss.min(0.0).abs();
                    }
                    Err(e) => {
                        error!("[real] Failed to trim DELTA A: {}", e);
                    }
                }
            }
        }

        if delta_b > 1e-12 {
            let phys_to_dump = delta_b; // Use physical coin volume
            if phys_to_dump >= mult_b.min(1e-8) {
                let is_buy = !is_a_higher; // Opposite of entry B
                error!(
                    "[real] ⚠️ TRIMMING {} LEG B — Selling excess {:.6} phys",
                    pair, phys_to_dump
                );

                let tick_for_close = e_ctx
                    .tick_sizes
                    .get(&format!("{}:{}", exb, pair))
                    .map(|v| *v)
                    .unwrap_or(0.000001);
                match crate::engine::exchanges::get_connector(&exb)
                    .unwrap()
                    .trim_position(
                        &hc,
                        &kc,
                        &fp,
                        is_buy,
                        phys_to_dump,
                        false,
                        mult_b,
                        tick_for_close,
                        fill_b,
                        ts.api_timeout_ms,
                    )
                    .await
                {
                    Ok(rc) => {
                        trimming_fee += rc.total_fee;
                        // If exchange didn't report fill price (0), assume closed at entry → loss ≈ 0.
                        let close_price = if rc.final_price > 1e-12 { rc.final_price } else { fill_b };
                        let gross_loss = if is_buy {
                            (fill_b - close_price) * delta_b
                        } else {
                            (close_price - fill_b) * delta_b
                        };
                        trimming_loss += gross_loss.min(0.0).abs();
                    }
                    Err(e) => {
                        error!("[real] Failed to trim DELTA B: {}", e);
                    }
                }
            }
        }

        // ── TRADE BOOKING (Phase 4) ──
        if matched_phys < 1e-12 {
            let trims = e_ctx
                .consecutive_trims
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            if trims >= 3 {
                e_ctx
                    .is_killed
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                error!("[real] 💀 KILL-SWITCH TRIGGERED: 3 consecutive FULL TRIMS (total asymmetry). Suspending new entries.");
            }
            // Nothing survived the trim (e.g. one leg didn't fill at all, and we dumped the other)
            let total_fee = fee_a_val + fee_b_val + trimming_fee;
            error!("[real] ❌ FULL ASYMMETRY DUMPED {}. Trade canceled. Trimming Loss: {:.4}, Trimming Fee: {:.4}", pair, trimming_loss, trimming_fee);
            e_ctx
                .pair_cooldown
                .insert(pair.clone(), now + ts.asymmetric_cooldown_ms);

            // Record failed asymmetrical trade into database...
            let (ea_s, eb_s, d, sp, pid) = (
                exa.clone(),
                exb.clone(),
                sig.direction.clone(),
                sig.spread,
                settings.id.clone(),
            );
            let (ent_a, ent_b) = if a_filled {
                (fill_a, 0.0)
            } else {
                (0.0, fill_b)
            };
            let fv_usd = if a_filled {
                delta_a * fill_a
            } else {
                delta_b * fill_b
            };
            let gross_pnl = -trimming_loss;
            let net_pnl = gross_pnl - total_fee;

            let pnl_a_val = if a_filled { gross_pnl } else { 0.0 };
            let pnl_b_val = if !a_filled { gross_pnl } else { 0.0 };

            let repo_close = TradeRepository::new(db.clone());
            // slip is 0.0 for cancelled trades, loss comes from gross_pnl / pnl_x
            repo_close
                .append_asymmetric_close(
                    pair.clone(),
                    ea_s,
                    eb_s,
                    d,
                    fv_usd,
                    ent_a,
                    ent_b,
                    sp,
                    now_ms(),
                    pnl_a_val,
                    pnl_b_val,
                    pid,
                    total_fee,
                    0.0,
                    net_pnl,
                    gross_pnl,
                    0.0,
                    0.0,
                )
                .await;
            continue;
        }

        // We have a VALID matching volume! Calculate final trading volume
        e_ctx
            .consecutive_failures
            .store(0, std::sync::atomic::Ordering::Relaxed);
        e_ctx
            .consecutive_trims
            .store(0, std::sync::atomic::Ordering::Relaxed);
        // BUG-S4 FIX: Use actual fill price, not signal price, for volume record
        let final_vol_usd = matched_phys * fill_a;
        deduct_cached_balance(
            &exa,
            final_vol_usd,
            settings.leverage as f64,
            &e_ctx,
        );
        deduct_cached_balance(
            &exb,
            final_vol_usd,
            settings.leverage as f64,
            &e_ctx,
        );

        // 🛡️ Apply cooldown to the pair so we don't spam duplicate entries
        // while the orderbook absorbs our impact
        e_ctx.pair_cooldown.insert(pair.clone(), now + ts.pair_cooldown_ms);

        // opened_this_cycle is already set to true earlier
        let dir = sig.direction.clone();
        let actual_entry_spread = crate::engine::execution::compute_entry_spread(
            fill_a, fill_b, is_a_higher, sig.spread,
        );

        // BUG-6 FIX: Include trimming_loss in commission so net PnL (gross - comm - fund)
        // correctly accounts for the cost of dumping excess asymmetric fill volume.
        let entry_comm = fee_a_val + fee_b_val + trimming_fee + trimming_loss;
        let slip_a = if fill_a > 0.0 {
            if is_a_higher {
                ((entry_a - fill_a) * matched_phys).max(0.0)
            } else {
                ((fill_a - entry_a) * matched_phys).max(0.0)
            }
        } else {
            0.0
        };
        let slip_b = if fill_b > 0.0 {
            if !is_a_higher {
                ((entry_b - fill_b) * matched_phys).max(0.0)
            } else {
                ((fill_b - entry_b) * matched_phys).max(0.0)
            }
        } else {
            0.0
        };

        // Trimming loss is now in commission_cost (BUG-6 FIX). Slippage tracks pure price deviation only.
        let entry_slip = slip_a + slip_b;

        let contracts_a_db = matched_phys / mult_a;
        let contracts_b_db = matched_phys / mult_b;

        // 🛡️ PRICE SANITY CHECK: Protect against connector bugs that return wrong price units.
        // Compare fill price against scanner L1 price (entry_a/entry_b). If off by >50%, use L1 fallback.
        if fill_a > 0.0 && entry_a > 0.0 {
            let ratio_a = fill_a / entry_a;
            if ratio_a > 2.0 || ratio_a < 0.5 {
                error!("[real] 🚨 PRICE SANITY FAIL {}/{}: fill_a={:.8} entry_a={:.8} ratio={:.2}. Using L1 price as fallback!",
                    pair, exa, fill_a, entry_a, ratio_a);
                fill_a = entry_a;
            }
        }
        if fill_b > 0.0 && entry_b > 0.0 {
            let ratio_b = fill_b / entry_b;
            if ratio_b > 2.0 || ratio_b < 0.5 {
                error!("[real] 🚨 PRICE SANITY FAIL {}/{}: fill_b={:.8} entry_b={:.8} ratio={:.2}. Using L1 price as fallback!",
                    pair, exb, fill_b, entry_b, ratio_b);
                fill_b = entry_b;
            }
        }

        info!("[real] [EXCHANGE_VERIFIED] ✅ OPEN {} phys={:.6} (Trimming: -${:.4}) A_price={:.6} B_price={:.6} fees={:.4} slip={:.4} actual_sp={:.4}%",
            pair, matched_phys, trimming_loss, fill_a, fill_b, entry_comm, entry_slip, actual_entry_spread);

        repo.insert_new_entry(
            pair.clone(),
            exa.clone(),
            exb.clone(),
            dir.clone(),
            final_vol_usd,
            fill_a,
            fill_b,
            actual_entry_spread,
            now_ms(),
            settings.id.clone(),
            entry_comm,
            entry_slip,
            oid_a.unwrap_or_default(),
            oid_b.unwrap_or_default(),
            contracts_a_db,
            contracts_b_db,
        )
        .await;
    }
}
