//! Order ownership, bounded entries and independent position supervision.
use super::{
    exchanges::get_connector,
    journal::{aggregate, Journal, JournalOrder, ManagedTrade, Plan},
    orders::{OrderObservation, OrderRequest, OrderState},
    real::ExchangeKeys,
    utils::now_ms,
};
use crate::state::{EngineContext, PriceMap};
use reqwest::Client;
use shared::{TechnicalSettings, TradeSignal, TradingSettings};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

static ENTRY_BUSY: AtomicBool = AtomicBool::new(false);
struct BusyGuard;
impl Drop for BusyGuard {
    fn drop(&mut self) {
        ENTRY_BUSY.store(false, Ordering::Release);
    }
}
struct TradeGuard {
    ctx: Arc<EngineContext>,
    id: i64,
}
impl Drop for TradeGuard {
    fn drop(&mut self) {
        self.ctx.closing_in_flight.remove(&self.id);
    }
}
fn claim(ctx: &Arc<EngineContext>, id: i64) -> Option<TradeGuard> {
    match ctx.closing_in_flight.entry(id) {
        dashmap::mapref::entry::Entry::Vacant(v) => {
            v.insert(true);
            Some(TradeGuard {
                ctx: ctx.clone(),
                id,
            })
        }
        _ => None,
    }
}

pub async fn kill(ctx: &EngineContext, j: &Journal, reason: String) {
    ctx.is_killed.store(true, Ordering::Release);
    tracing::error!("[managed] NEW ENTRIES HALTED: {}", reason);
    let _ = j.alert("execution_integrity".into(), reason).await;
    if let Some(db) = &ctx.profiles_db {
        if let Err(e)=db.call(|c|{c.execute("INSERT INTO server_cache(key,value,expires_at) VALUES('kill_switch_active','1',0) ON CONFLICT(key) DO UPDATE SET value='1'",[])?;Ok(())}).await {tracing::error!("Cannot persist kill switch: {}",e);}
    }
}

/// An exact fresh book (or a fully sufficient measured best level), in base quantities.
pub fn executable(
    prices: &PriceMap,
    ctx: &EngineContext,
    ex: &str,
    pair: &str,
    buy: bool,
    qty: f64,
) -> Option<f64> {
    let now = now_ms();
    let d = prices.get(ex).and_then(|m| m.get(pair).cloned())?;
    if d.last_updated == 0
        || now / 1000 < d.last_updated as i64
        || now / 1000 - d.last_updated as i64 > 1
    {
        return None;
    }
    if let Some(book) = ctx.l2_books.get(&format!("{}:{}", ex, pair)) {
        if now >= book.updated_ms && now - book.updated_ms <= 750 {
            return super::execution::verify_l2_vwap(
                Ok((book.bids.clone(), book.asks.clone())),
                buy,
                qty,
                1.0,
            );
        }
    }
    // L1 is sufficient only if its measured available quantity covers this entire order.
    let (px, usd) = if buy {
        (d.ask, d.ask_size)
    } else {
        (d.bid, d.bid_size)
    };
    if px.is_finite() && usd.is_finite() && px > 0.0 && usd >= px * qty * 2.5 {
        Some(px)
    } else {
        None
    }
}

fn request(t: &ManagedTrade, leg: &str, qty: f64, price: f64, reduce: bool) -> JournalOrder {
    let s = &t.plan.signal;
    let a = leg == "A";
    let entry_buy = if a { !s.is_a_higher } else { s.is_a_higher };
    JournalOrder {
        trade_id: t.id,
        leg: leg.into(),
        exchange: if a {
            s.exchange_a.clone()
        } else {
            s.exchange_b.clone()
        },
        request: OrderRequest {
            client_id: format!("arb{}", &uuid::Uuid::new_v4().simple().to_string()[..24]),
            pair: s.pair.clone(),
            is_buy: if reduce { !entry_buy } else { entry_buy },
            quantity: qty,
            limit_price: price,
            reduce_only: reduce,
            multiplier: if a { s.mult_a } else { s.mult_b },
            tick_size: if a { s.tick_a } else { s.tick_b },
            leverage: t.plan.settings.leverage as f64,
        },
        observation: OrderObservation {
            order_id: None,
            state: OrderState::Unknown,
            filled_quantity: 0.0,
            average_price: None,
            fee: None,
        },
    }
}

async fn submit(
    j: &Journal,
    o: &JournalOrder,
    http: &Client,
    keys: &ExchangeKeys,
) -> anyhow::Result<OrderObservation> {
    j.prepare(o).await?; // durable identity BEFORE any external side effect
    let conn = get_connector(&o.exchange).ok_or_else(|| anyhow::anyhow!("Unknown connector"))?;
    let mut known = o.observation.clone();
    match conn.submit_managed(http, keys, &o.request).await {
        Ok(v) => {
            j.observe(o, v.clone()).await?;
            known = v;
        }
        Err(e) => {
            tracing::warn!(
                "[managed] submit outcome unknown client_id={} exchange={} detail={}",
                o.request.client_id,
                o.exchange,
                e
            );
        }
    }
    // No resubmission, even after transport failure. Resolve this SAME identity.
    for _ in 0..5 {
        if known.state.terminal() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if let Ok(v) = conn
            .query_managed(http, keys, &o.request, known.order_id.as_deref())
            .await
        {
            j.observe(o, v.clone()).await?;
            known = v;
        }
    }
    Ok(known)
}

pub fn propose(
    signals: Vec<TradeSignal>,
    settings: Vec<TradingSettings>,
    prices: PriceMap,
    http: Client,
    keys: Arc<ExchangeKeys>,
    j: Journal,
    ctx: Arc<EngineContext>,
    ts: TechnicalSettings,
) {
    if ctx.is_killed.load(Ordering::Acquire)
        || ENTRY_BUSY
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
    {
        return;
    }
    tokio::spawn(async move {
        let _busy = BusyGuard;
        if let Err(e) = enter(signals, settings, &prices, &http, &keys, &j, &ctx, &ts).await {
            kill(&ctx, &j, e.to_string()).await;
        }
    });
}
async fn enter(
    signals: Vec<TradeSignal>,
    settings: Vec<TradingSettings>,
    prices: &PriceMap,
    http: &Client,
    keys: &ExchangeKeys,
    j: &Journal,
    ctx: &Arc<EngineContext>,
    ts: &TechnicalSettings,
) -> anyhow::Result<()> {
    // Existing unowned positions must be reconciled, not silently absorbed by a new trade.
    for s in signals {
        let p = match settings
            .iter()
            .find(|p| p.id == s.profile_id && p.is_active)
        {
            Some(p) => p.clone(),
            None => continue,
        };
        if ts.entry_order_type != "ioc" || p.strategy_type != "ema" {
            continue;
        }
        let ca = get_connector(&s.exchange_a).unwrap();
        let cb = get_connector(&s.exchange_b).unwrap();
        if !ca.supports_managed_orders() || !cb.supports_managed_orders() {
            continue;
        }
        if !super::risk_manager::verify_trade_health(&s.exchange_a, &s.exchange_b, ctx) {
            continue;
        }
        if !super::risk_manager::has_sufficient_margin(
            &s.exchange_a,
            s.actual_vol_a_usd,
            p.leverage as f64,
            ctx,
            ts.margin_check_multiplier,
        ) || !super::risk_manager::has_sufficient_margin(
            &s.exchange_b,
            s.actual_vol_b_usd,
            p.leverage as f64,
            ctx,
            ts.margin_check_multiplier,
        ) {
            continue;
        }
        let cooldown = ctx.pair_cooldown.get(&s.pair).map(|v| *v).unwrap_or(0);
        if super::utils::cooldown_active(cooldown, now_ms(), ts.pair_cooldown_ms) {
            continue;
        }
        // Authoritative preflight, before the first leg. Errors fail closed.
        let (posa, posb) = tokio::join!(
            ca.fetch_positions(http, keys),
            cb.fetch_positions(http, keys)
        );
        let (posa, posb) = match (posa, posb) {
            (Ok(a), Ok(b)) => (a, b),
            _ => continue,
        };
        if posa
            .iter()
            .chain(posb.iter())
            .any(|x| super::utils::symbols_match(&x.symbol, &s.pair) && x.size.abs() > 1e-12)
        {
            continue;
        }
        // Configure both venues before taking exposure; failures cannot be ignored.
        let (la, lb) = tokio::join!(
            ca.ensure_leverage(http, keys, &s.pair, p.leverage as f64),
            cb.ensure_leverage(http, keys, &s.pair, p.leverage as f64)
        );
        if la.is_err() || lb.is_err() {
            continue;
        }
        let (pa, pb) = match (
            executable(
                prices,
                ctx,
                &s.exchange_a,
                &s.pair,
                !s.is_a_higher,
                s.target_qty_a,
            ),
            executable(
                prices,
                ctx,
                &s.exchange_b,
                &s.pair,
                s.is_a_higher,
                s.target_qty_b,
            ),
        ) {
            (Some(a), Some(b)) => (a, b),
            _ => continue,
        };
        let gross = if s.is_a_higher {
            (pa - pb) / pb * 100.0
        } else {
            (pb - pa) / pa * 100.0
        };
        let required = s.structural_spread + s.break_even_pct + p.min_entry_spread.max(0.05);
        let budget = (gross - required).max(0.0);
        if budget <= 0.0 || gross - s.structural_spread > p.max_entry_spread {
            continue;
        }
        let half = (budget / 2.0).min(p.slippage_percent);
        if half < 0.0 {
            continue;
        }
        let id = match j
            .reserve(Plan {
                signal: s.clone(),
                settings: p.clone(),
            })
            .await?
        {
            Some(id) => id,
            None => continue,
        };
        let _guard =
            claim(ctx, id).ok_or_else(|| anyhow::anyhow!("Trade already being supervised"))?;
        let t = ManagedTrade {
            id,
            state: "ENTRY".into(),
            reason: None,
            created_ms: now_ms(),
            plan: Plan {
                signal: s.clone(),
                settings: p,
            },
        };
        // Liquidity ranking only selects sequence; both legs have hard price bounds.
        let first_a = super::utils::liquidity_rank(&s.exchange_a)
            <= super::utils::liquidity_rank(&s.exchange_b);
        let (first, second, price, buy, qty) = if first_a {
            ("A", "B", pa, !s.is_a_higher, s.target_qty_a)
        } else {
            ("B", "A", pb, s.is_a_higher, s.target_qty_b)
        };
        let limit = price * (1.0 + if buy { half / 100.0 } else { -half / 100.0 });
        let o = request(&t, first, qty, limit, false);
        let v = submit(j, &o, http, keys).await?;
        if !v.state.terminal() {
            j.state(id, "RECONCILING", "ENTRY_UNKNOWN").await?;
            return Ok(());
        }
        if v.filled_quantity <= 1e-12 {
            j.state(id, "CLOSING", "ENTRY_UNFILLED").await?;
            return Ok(());
        }
        let second_step = if first_a { s.mult_b } else { s.mult_a };
        let q = v.filled_quantity;
        let on_grid = (q / second_step - (q / second_step).round()).abs() < 1e-8;
        let px = executable(
            prices,
            ctx,
            if first_a {
                &s.exchange_b
            } else {
                &s.exchange_a
            },
            &s.pair,
            !buy,
            q,
        );
        if !on_grid || px.is_none() {
            j.state(id, "CLOSING", "HEDGE_UNAVAILABLE").await?;
            return Ok(());
        }
        let actual = v.average_price.unwrap();
        // Solve spread inequality using the actual first fill. This preserves the total budget.
        let bound = if buy {
            actual * (1.0 + required / 100.0)
        } else {
            actual / (1.0 + required / 100.0)
        };
        let px = px.unwrap();
        if (!buy && px > bound) || (buy && px < bound) {
            j.state(id, "CLOSING", "HEDGE_BUDGET_EXHAUSTED").await?;
            return Ok(());
        }
        let second_order = request(&t, second, q, bound, false);
        let second_fill = submit(j, &second_order, http, keys).await?;
        let orders = j.orders(id).await?;
        if !second_fill.state.terminal() {
            j.state(id, "RECONCILING", "HEDGE_UNKNOWN").await?;
            return Ok(());
        }
        if (second_fill.filled_quantity - q).abs() > q * 1e-8 {
            j.state(id, "CLOSING", "PARTIAL_HEDGE").await?;
        } else {
            j.project(&t, &orders, false).await?;
            ctx.consecutive_failures.store(0, Ordering::Release);
        }
        ctx.pair_cooldown
            .insert(s.pair.clone(), now_ms() + ts.pair_cooldown_ms);
        return Ok(());
    }
    Ok(())
}

pub fn supervise(
    j: Journal,
    http: Client,
    keys: Arc<ExchangeKeys>,
    prices: PriceMap,
    ctx: Arc<EngineContext>,
) {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(std::time::Duration::from_secs(1));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            match j.active().await {
                Ok(trades) => {
                    for t in trades {
                        if let Some(guard) = claim(&ctx, t.id) {
                            let (j, h, k, p, c) = (
                                j.clone(),
                                http.clone(),
                                keys.clone(),
                                prices.clone(),
                                ctx.clone(),
                            );
                            tokio::spawn(async move {
                                let _guard = guard;
                                if let Err(e) = manage(t, &j, &h, &k, &p, &c).await {
                                    kill(&c, &j, e.to_string()).await;
                                }
                            });
                        }
                    }
                }
                Err(e) => kill(&ctx, &j, format!("Cannot load exposure: {}", e)).await,
            }
        }
    });
}
async fn manage(
    t: ManagedTrade,
    j: &Journal,
    http: &Client,
    keys: &ExchangeKeys,
    prices: &PriceMap,
    ctx: &Arc<EngineContext>,
) -> anyhow::Result<()> {
    let mut orders = j.orders(t.id).await?;
    for o in &orders {
        if !o.observation.state.terminal() || o.observation.fee.is_none() {
            let conn =
                get_connector(&o.exchange).ok_or_else(|| anyhow::anyhow!("Missing connector"))?;
            if let Ok(obs) = conn
                .query_managed(http, keys, &o.request, o.observation.order_id.as_deref())
                .await
            {
                j.observe(o, obs).await?;
            }
        }
    }
    orders = j.orders(t.id).await?;
    if orders.iter().any(|o| !o.observation.state.terminal()) {
        j.state(t.id, "RECONCILING", "ORDER_OUTCOME_UNKNOWN")
            .await?;
        // Existing owned exposure remains tracked. Do not retry or invent a zero fill.
        return Ok(());
    }
    let s = &t.plan.signal;
    let a = aggregate(&orders, "A");
    let b = aggregate(&orders, "B");
    let closing = t.state != "OPEN" || t.reason.as_deref() == Some("MANUAL_CLOSE");
    let elapsed = now_ms() - t.created_ms;
    let timed =
        t.plan.settings.max_holding_time_ms > 0 && elapsed > t.plan.settings.max_holding_time_ms;
    let (pxa, pxb) = (
        executable(
            prices,
            ctx,
            &s.exchange_a,
            &s.pair,
            s.is_a_higher,
            a.remaining(),
        ),
        executable(
            prices,
            ctx,
            &s.exchange_b,
            &s.pair,
            !s.is_a_higher,
            b.remaining(),
        ),
    );
    let mut profit = false;
    let mut stop = false;
    if let (Some(pa), Some(pb)) = (pxa, pxb) {
        let pnl = super::pnl::calculate_gross_pnl(
            s.is_a_higher,
            a.entry_price(),
            b.entry_price(),
            pa,
            pb,
            a.remaining(),
            b.remaining(),
        )
        .gross_pnl;
        let reserve =
            a.remaining() * pa * super::utils::get_exchange_fee(&s.exchange_a, 0.001, ctx)
                + b.remaining() * pb * super::utils::get_exchange_fee(&s.exchange_b, 0.001, ctx);
        let percent = (pnl - a.fee - b.fee - reserve) / a.entry_value.max(1e-12) * 100.0;
        profit = percent >= t.plan.settings.take_profit_percent.max(0.05);
        stop = t.plan.settings.trailing_stop_percent > 0.0
            && percent <= -t.plan.settings.trailing_stop_percent;
    }
    if !closing && !timed && !profit && !stop {
        return Ok(());
    }
    let reason = if closing {
        t.reason
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("INTERRUPTED_ENTRY")
    } else if timed {
        "TIME_STOP"
    } else if stop {
        "STOP"
    } else {
        "TP"
    };
    j.state(t.id, "CLOSING", reason).await?;
    // Exit starts only once. Partial closes remain journaled and only residuals are submitted.
    let emergency = closing || timed || stop;
    for (leg, qty, px) in [("A", a.remaining(), pxa), ("B", b.remaining(), pxb)] {
        if qty <= 1e-9 {
            continue;
        }
        let limit = if emergency { 0.0 } else { px.unwrap_or(0.0) };
        if !emergency && limit <= 0.0 {
            continue;
        }
        let o = request(&t, leg, qty, limit, true);
        let _ = submit(j, &o, http, keys).await?;
    }
    let updated = j.orders(t.id).await?;
    j.project(&t, &updated, true).await?;
    if !profit && a.opened + b.opened > 0.0 {
        let n = ctx.consecutive_failures.fetch_add(1, Ordering::AcqRel) + 1;
        if n >= 5 {
            kill(
                ctx,
                j,
                "Five failed/risk exits; inspect ledger before resuming".into(),
            )
            .await;
        }
    }
    Ok(())
}
