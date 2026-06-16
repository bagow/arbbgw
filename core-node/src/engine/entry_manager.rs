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

        // ── PARALLEL ORDER EXECUTION ─────────────────────────────────────────────
        // With tight IOC slippage (0.05%), parallel is safe:
        // - Both legs see the same price → maximizes fill chance
        // - Sequential was failing: Leg A fills → 1-2s delay → spread gone → Leg B never fills
        //   → 100% asymmetric positions → all losses
        // - With parallel + tight IOC: worst case one leg fills → trim at 0.05% cost (tiny)
        opened_this_cycle = true;

        info!(
            "[real] ⚡ Выполнение ПАРАЛЛЕЛЬНО: Leg A ({}) + Leg B ({}) для {}...",
            exa, exb, pair
        );

        let t0_exec = std::time::Instant::now();

        // Execute both legs simultaneously via tokio::join!
        let (res_a, res_b) = tokio::join!(
            conn_ensure_a.place_order(
                &http, &keys, &pair, !is_a_higher, qty_a, dry_run,
                limit_price_a, limit_price_a > 0.0,
                settings.leverage as f64, mult_a, ioc_slippage_pct, tick_a, ts.api_timeout_ms
            ),
            conn_ensure_b.place_order(
                &http, &keys, &pair, is_a_higher, qty_b, dry_run,
                limit_price_b, limit_price_b > 0.0,
                settings.leverage as f64, mult_b, ioc_slippage_pct, tick_b, ts.api_timeout_ms
            )
        );

        let total_ms = t0_exec.elapsed().as_millis();

        // Latency checks
        if total_ms > 800 {
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

        // 2. Parse inline results
        let (oid_a, mut fill_a, mut fee_a_val) = match res_a {
            Ok(t) => (t.0, t.1, t.2),
            Err(e) => {
                error!("[real] LEG_A reject {}/{}: {}", exa, pair, e);
                let esl = e.to_string().to_lowercase();
                if esl.contains("insufficient") || esl.contains("balance") || esl.contains("margin") || esl.contains("exceed") {
                    mark_balance_low(&exa, &e_ctx);
                }
                (None, 0.0, 0.0)
            }
        };

        let (oid_b, mut fill_b, mut fee_b_val) = match res_b {
            Ok(t) => (t.0, t.1, t.2),
            Err(e) => {
                error!("[real] LEG_B reject {}/{}: {}", exb, pair, e);
                let esl = e.to_string().to_lowercase();
                if esl.contains("insufficient") || esl.contains("balance") || esl.contains("margin") || esl.contains("exceed") {
                    mark_balance_low(&exb, &e_ctx);
                }
                (None, 0.0, 0.0)
            }
        };

        let mut actual_vol_phys_a: Option<f64> = None;
        let mut actual_vol_phys_b: Option<f64> = None;

        if fill_a > 0.0 { actual_vol_phys_a = Some(qty_a); }
        if fill_b > 0.0 { actual_vol_phys_b = Some(qty_b); }

        // IOC verify: Even if inline fill returned 0 (e.g., WS lag), give enough
        // retries for execution report to arrive. Don't skip slow path — a filled
        // IOC order that we miss leads to orphaned positions and asymmetric losses.
        let ioc_no_fill_a = use_ioc && fill_a <= 0.0;
        let ioc_no_fill_b = use_ioc && fill_b <= 0.0;
        let max_retries_a = if ioc_no_fill_a { 5_i64 } else { ts.fill_verify_retries };
        let max_retries_b = if ioc_no_fill_b { 5_i64 } else { ts.fill_verify_retries };
        let skip_slow_a = false; // Never skip slow verify — orphaned positions are worse than latency
        let skip_slow_b = false;

        // 3. Parallel verification loops (if needed)
        let verify_a_fut = async {
            let mut phys = actual_vol_phys_a;
            let mut got = dry_run || fill_a > 0.0;
            if got && phys.is_some() { return (got, phys, fill_a, fee_a_val); }
    
            for attempt in 0..max_retries_a {
                if got && phys.is_some() { break; }
                if attempt > 0 { tokio::time::sleep(std::time::Duration::from_millis(ts.fill_verify_delay_ms as u64)).await; }
        
                if let Some(ref oid) = oid_a {
                    if let Ok((p, f, ev)) = conn_ensure_a.fetch_order(&http, &keys, &pair, oid, mult_a).await {
                        if p > 0.0 { fill_a = p; }
                        if f > 1e-9 { fee_a_val = f; }
                        if ev > 1e-12 { phys = Some(ev); got = true; }
                    }
                }
            }
    
            if !got && oid_a.is_some() && !dry_run && !skip_slow_a {
                tracing::warn!("[real] ⏳ {}: Leg A orderId={} accepted but fill not yet confirmed. Extended wait...", pair, oid_a.as_deref().unwrap_or("?"));
                for slow_attempt in 0..8 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    if let Some(ref oid) = oid_a {
                        if let Ok((p, f, ev)) = conn_ensure_a.fetch_order(&http, &keys, &pair, oid, mult_a).await {
                            if ev > 1e-12 {
                                phys = Some(ev); got = true;
                                if p > 0.0 { fill_a = p; }
                                if f > 1e-9 { fee_a_val = f; }
                                tracing::info!("[real] ✅ {}: Leg A fill confirmed on slow attempt {} — qty={}, price={}", pair, slow_attempt + 1, ev, p);
                                break;
                            }
                            if p == 0.0 && f == 0.0 && ev == 0.0 { break; }
                        }
                    }
                }
        
                if phys.unwrap_or(0.0) <= 1e-12 {
                    tracing::warn!("[real] 🔍 {}: Leg A fill unverified. Checking position...", pair);
                    if let Ok(positions) = conn_ensure_a.fetch_positions(&http, &keys).await {
                        if let Some(pos) = positions.iter().find(|p| symbols_match(&p.symbol, &pair) && p.size.abs() > 1e-12) {
                            phys = Some(pos.size.abs()); got = true;
                            if pos.entry_price > 0.0 { fill_a = pos.entry_price; }
                        }
                    }
                }
            }
            (got, phys, fill_a, fee_a_val)
        };

        let verify_b_fut = async {
            let mut phys = actual_vol_phys_b;
            let mut got = dry_run || fill_b > 0.0;
            if got && phys.is_some() { return (got, phys, fill_b, fee_b_val); }
    
            for attempt in 0..max_retries_b {
                if got && phys.is_some() { break; }
                if attempt > 0 { tokio::time::sleep(std::time::Duration::from_millis(ts.fill_verify_delay_ms as u64)).await; }
        
                if let Some(ref oid) = oid_b {
                    if let Ok((p, f, ev)) = conn_ensure_b.fetch_order(&http, &keys, &pair, oid, mult_b).await {
                        if p > 0.0 { fill_b = p; }
                        if f > 1e-9 { fee_b_val = f; }
                        if ev > 1e-12 { phys = Some(ev); got = true; }
                    }
                }
            }
    
            if !got && oid_b.is_some() && !dry_run && !skip_slow_b {
                tracing::warn!("[real] ⏳ {}: Leg B orderId={} accepted but fill not yet confirmed. Extended wait...", pair, oid_b.as_deref().unwrap_or("?"));
                for slow_attempt in 0..8 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    if let Some(ref oid) = oid_b {
                        if let Ok((p, f, ev)) = conn_ensure_b.fetch_order(&http, &keys, &pair, oid, mult_b).await {
                            if ev > 1e-12 {
                                phys = Some(ev); got = true;
                                if p > 0.0 { fill_b = p; }
                                if f > 1e-9 { fee_b_val = f; }
                                tracing::info!("[real] ✅ {}: Leg B fill confirmed on slow attempt {} — qty={}, price={}", pair, slow_attempt + 1, ev, p);
                                break;
                            }
                            if p == 0.0 && f == 0.0 && ev == 0.0 { break; }
                        }
                    }
                }
        
                if phys.unwrap_or(0.0) <= 1e-12 {
                    tracing::warn!("[real] 🔍 {}: Leg B fill unverified. Checking position...", pair);
                    if let Ok(positions) = conn_ensure_b.fetch_positions(&http, &keys).await {
                        if let Some(pos) = positions.iter().find(|p| symbols_match(&p.symbol, &pair) && p.size.abs() > 1e-12) {
                            phys = Some(pos.size.abs()); got = true;
                            if pos.entry_price > 0.0 { fill_b = pos.entry_price; }
                        }
                    }
                }
            }
            (got, phys, fill_b, fee_b_val)
        };

        let (ver_a, ver_b) = tokio::join!(verify_a_fut, verify_b_fut);
        actual_vol_phys_a = ver_a.1;
        fill_a = ver_a.2;
        fee_a_val = ver_a.3;
        actual_vol_phys_b = ver_b.1;
        fill_b = ver_b.2;
        fee_b_val = ver_b.3;

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

        let min_phys = phys_a.min(phys_b);
        let step_a = mult_a.max(1e-12);
        let step_b = mult_b.max(1e-12);
        let valid_a = (min_phys / step_a).floor() * step_a;
        let valid_b = (min_phys / step_b).floor() * step_b;
        let matched_phys = valid_a.min(valid_b);

        let delta_a = phys_a - matched_phys;
        let delta_b = phys_b - matched_phys;

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
        let actual_entry_spread = if fill_a > 0.0 && fill_b > 0.0 {
            if is_a_higher {
                (fill_a - fill_b) / fill_b.max(1e-12) * 100.0
            } else {
                (fill_b - fill_a) / fill_a.max(1e-12) * 100.0
            }
        } else {
            sig.spread
        };

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
