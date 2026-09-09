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
static LAST_PREFLIGHT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
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

/// A fresh, uncrossed book with full depth coverage, in base quantities.
pub fn executable(
    prices: &PriceMap,
    ctx: &EngineContext,
    ex: &str,
    pair: &str,
    buy: bool,
    qty: f64,
) -> Option<f64> {
    executable_quote(prices, ctx, ex, pair, buy, qty).map(|q| q.vwap)
}

fn executable_quote(
    prices: &PriceMap,
    ctx: &EngineContext,
    ex: &str,
    pair: &str,
    buy: bool,
    qty: f64,
) -> Option<super::book::DepthQuote> {
    if !qty.is_finite() || qty <= 0.0 {
        return None;
    }
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
            let bid = book.bids.first()?.0;
            let ask = book.asks.first()?.0;
            if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || bid >= ask {
                return None;
            }
            return super::book::quote(&book.bids, &book.asks, buy, qty);
        }
    }
    None
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

#[async_trait::async_trait]
trait OrderTransport: Send + Sync {
    async fn submit(&self, r: &OrderRequest) -> Result<OrderObservation, String>;
    async fn query(&self, r: &OrderRequest, id: Option<&str>) -> Result<OrderObservation, String>;
}
struct ConnectorTransport<'a> {
    connector: &'a dyn super::exchanges::traits::ExchangeConnector,
    http: &'a Client,
    keys: &'a ExchangeKeys,
}
#[async_trait::async_trait]
impl OrderTransport for ConnectorTransport<'_> {
    async fn submit(&self, r: &OrderRequest) -> Result<OrderObservation, String> {
        self.connector.submit_managed(self.http, self.keys, r).await
    }
    async fn query(&self, r: &OrderRequest, id: Option<&str>) -> Result<OrderObservation, String> {
        self.connector
            .query_managed(self.http, self.keys, r, id)
            .await
    }
}
async fn submit(
    j: &Journal,
    o: &JournalOrder,
    http: &Client,
    keys: &ExchangeKeys,
) -> anyhow::Result<OrderObservation> {
    let conn = get_connector(&o.exchange).ok_or_else(|| anyhow::anyhow!("Unknown connector"))?;
    submit_using(
        j,
        o,
        &ConnectorTransport {
            connector: conn.as_ref(),
            http,
            keys,
        },
    )
    .await
}
async fn submit_using(
    j: &Journal,
    o: &JournalOrder,
    transport: &dyn OrderTransport,
) -> anyhow::Result<OrderObservation> {
    o.request.validate().map_err(anyhow::Error::msg)?;
    j.prepare(o).await?;
    let mut known = o.observation.clone();
    match transport.submit(&o.request).await {
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
        if let Ok(v) = transport.query(&o.request, known.order_id.as_deref()).await {
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
    let now = now_ms();
    if now - LAST_PREFLIGHT.load(Ordering::Acquire) < 2_000 {
        return Ok(());
    }
    LAST_PREFLIGHT.store(now, Ordering::Release);
    for s in signals.into_iter().take(3) {
        if ctx.is_killed.load(Ordering::Acquire) {
            break;
        }
        let p = match settings
            .iter()
            .find(|p| p.id == s.profile_id && p.is_active)
        {
            Some(p) => p.clone(),
            None => continue,
        };
        p.validate().map_err(anyhow::Error::msg)?;
        if ts.entry_order_type != "ioc" || p.strategy_type != "ema" {
            continue;
        }
        let (ca, cb) = match (get_connector(&s.exchange_a), get_connector(&s.exchange_b)) {
            (Some(a), Some(b)) => (a, b),
            _ => continue,
        };
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
        let (idle_a, idle_b) = tokio::join!(
            ca.symbol_has_no_open_orders(http, keys, &s.pair),
            cb.symbol_has_no_open_orders(http, keys, &s.pair)
        );
        if !matches!(idle_a, Ok(true)) || !matches!(idle_b, Ok(true)) {
            continue;
        }
        let (ba, bb) = tokio::join!(ca.fetch_balances(http, keys), cb.fetch_balances(http, keys));
        let (available_a, available_b) = match (ba, bb) {
            (Ok((_, a)), Ok((_, b))) if a.is_finite() && b.is_finite() => (a, b),
            _ => continue,
        };
        let required_a = s.actual_vol_a_usd / (p.leverage as f64)
            * ts.margin_check_multiplier.max(1.0)
            + s.actual_vol_a_usd * 0.002;
        let required_b = s.actual_vol_b_usd / (p.leverage as f64)
            * ts.margin_check_multiplier.max(1.0)
            + s.actual_vol_b_usd * 0.002;
        if available_a < required_a || available_b < required_b {
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
        let (fa, fb) = tokio::join!(
            super::funding_guard::next(http, &s.exchange_a, &s.pair),
            super::funding_guard::next(http, &s.exchange_b, &s.pair)
        );
        let funding_deadline_ms = match (fa, fb) {
            (Ok(a), Ok(b)) => super::funding_guard::deadline(a, b, now_ms()),
            _ => None,
        };
        if funding_deadline_ms.is_none() {
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
        let specs_ok = [
            (&s.exchange_a, s.target_qty_a, pa, s.mult_a, s.tick_a),
            (&s.exchange_b, s.target_qty_b, pb, s.mult_b, s.tick_b),
        ]
        .iter()
        .all(|(ex, qty, price, step, tick)| {
            ctx.specifications
                .get(&format!("{ex}:{}", s.pair))
                .map(|spec| spec.permits(now_ms(), *qty, *price, *step, *tick))
                .unwrap_or(false)
        });
        if !specs_ok {
            continue;
        }
        let gross = if s.is_a_higher {
            (pa - pb) / pb * 100.0
        } else {
            (pb - pa) / pa * 100.0
        };
        let required = s.structural_spread
            + s.break_even_pct
            + p.min_entry_spread.max(p.take_profit_percent).max(0.05);
        let budget = (gross - required).max(0.0);
        if budget <= 0.0 || gross - s.structural_spread > p.max_entry_spread {
            continue;
        }
        let half = (budget / 2.0).min(p.slippage_percent);
        if half < 0.0 {
            continue;
        }
        // Re-read entry authorization after REST preflight; a disabled profile must not
        // open using the settings snapshot captured several seconds earlier.
        let authorized = if let Some(db) = &ctx.profiles_db {
            let profile_id = s.profile_id.clone();
            db.call(move |c| {
                Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM trading_profiles WHERE id=? AND is_active=1) AND NOT EXISTS(SELECT 1 FROM server_cache WHERE key='kill_switch_active' AND value='1')", [profile_id], |r|r.get::<_,bool>(0))?)
            }).await.unwrap_or(false)
        } else {
            false
        };
        if !authorized || ctx.is_killed.load(Ordering::Acquire) {
            continue;
        }
        let id = match j
            .reserve(Plan {
                funding_deadline_ms,
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
                funding_deadline_ms,
                signal: s.clone(),
                settings: p,
            },
        };
        // Liquidity ranking only selects sequence; both legs have hard price bounds.
        let first_a = super::utils::liquidity_rank(&s.exchange_a)
            >= super::utils::liquidity_rank(&s.exchange_b);
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
        let (ex, step, tick) = if first_a {
            (&s.exchange_b, s.mult_b, s.tick_b)
        } else {
            (&s.exchange_a, s.mult_a, s.tick_a)
        };
        let bound = super::orders::bounded_price(bound, tick, !buy);
        if !ctx
            .specifications
            .get(&format!("{ex}:{}", s.pair))
            .map(|spec| spec.permits(now_ms(), q, bound, step, tick))
            .unwrap_or(false)
        {
            j.state(id, "CLOSING", "HEDGE_FILTER_CHANGED").await?;
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
            // A hedged entry proves execution, not profit. Preserve the risk-exit
            // counter until the operator explicitly resets the kill switch.
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
            ctx.supervisor_tick_ms.store(now_ms(), Ordering::Release);
            match j.active().await {
                Ok(trades) => {
                    *ctx.owned_l2_wanted.write().await = trades
                        .iter()
                        .flat_map(|t| {
                            [
                                format!("{}:{}", t.plan.signal.exchange_a, t.plan.signal.pair),
                                format!("{}:{}", t.plan.signal.exchange_b, t.plan.signal.pair),
                            ]
                        })
                        .collect();
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
        if !o.observation.state.terminal() {
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
        if now_ms() - t.created_ms > 10_000 && !ctx.is_killed.load(Ordering::Acquire) {
            kill(
                ctx,
                j,
                format!("Trade {} has an unresolved order; entries paused", t.id),
            )
            .await;
        }
        // Existing owned exposure remains tracked. Do not retry or invent a zero fill.
        return Ok(());
    }
    let s = &t.plan.signal;
    let a = aggregate(&orders, "A");
    let b = aggregate(&orders, "B");
    let manual = j.close_requested(t.id).await?;
    let closing = t.state != "OPEN" || manual;
    let elapsed = now_ms() - t.created_ms;
    let funding_due = t
        .plan
        .funding_deadline_ms
        .map(|d| now_ms() >= d)
        .unwrap_or(true);
    let timed =
        t.plan.settings.max_holding_time_ms > 0 && elapsed > t.plan.settings.max_holding_time_ms;
    let (quote_a, quote_b) = (
        executable_quote(
            prices,
            ctx,
            &s.exchange_a,
            &s.pair,
            s.is_a_higher,
            a.remaining(),
        ),
        executable_quote(
            prices,
            ctx,
            &s.exchange_b,
            &s.pair,
            !s.is_a_higher,
            b.remaining(),
        ),
    );
    let (pxa, pxb) = (quote_a.map(|q| q.vwap), quote_b.map(|q| q.vwap));
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
        let entry_fees: f64 = orders
            .iter()
            .map(|o| {
                o.observation
                    .fee_with_reserve(super::utils::get_exchange_fee(&o.exchange, 0.001, ctx))
            })
            .sum();
        let percent = (pnl - entry_fees - reserve) / a.entry_value.max(1e-12) * 100.0;
        let peak = j.mark_to_market(t.id, pa, pb, pnl, percent).await?;
        // TP must remain profitable at both hard IOC limits, not merely at
        // their average prices. Otherwise a multi-level book creates partial
        // closes followed by an avoidable emergency market exit.
        if let (Some(qa), Some(qb)) = (quote_a, quote_b) {
            let worst_pnl = super::pnl::calculate_gross_pnl(
                s.is_a_higher,
                a.entry_price(),
                b.entry_price(),
                qa.worst_price,
                qb.worst_price,
                a.remaining(),
                b.remaining(),
            )
            .gross_pnl;
            let worst_reserve = a.remaining()
                * qa.worst_price
                * super::utils::get_exchange_fee(&s.exchange_a, 0.001, ctx)
                + b.remaining()
                    * qb.worst_price
                    * super::utils::get_exchange_fee(&s.exchange_b, 0.001, ctx);
            let worst_percent =
                (worst_pnl - entry_fees - worst_reserve) / a.entry_value.max(1e-12) * 100.0;
            profit = worst_percent >= t.plan.settings.take_profit_percent.max(0.05);
        }
        stop = t.plan.settings.trailing_stop_percent > 0.0
            && percent <= peak - t.plan.settings.trailing_stop_percent;
    }
    if !closing && !timed && !funding_due && !profit && !stop {
        return Ok(());
    }
    let reason = if manual {
        "MANUAL_CLOSE"
    } else if closing {
        t.reason
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("INTERRUPTED_ENTRY")
    } else if funding_due {
        "FUNDING_BOUNDARY"
    } else if timed {
        "TIME_STOP"
    } else if stop {
        "STOP"
    } else {
        "TP"
    };
    j.state(t.id, "CLOSING", reason).await?;
    // Exit starts only once. Partial closes remain journaled and only residuals are submitted.
    let emergency = closing || timed || funding_due || stop;
    for (leg, qty, px) in [
        ("A", a.remaining(), quote_a.map(|q| q.worst_price)),
        ("B", b.remaining(), quote_b.map(|q| q.worst_price)),
    ] {
        let a_leg = leg == "A";
        let observed = if now_ms() - t.created_ms >= 10_000 {
            super::account_monitor::observed_remaining(
                ctx,
                if a_leg { &s.exchange_a } else { &s.exchange_b },
                &s.pair,
                if a_leg { s.mult_a } else { s.mult_b },
                if a_leg { !s.is_a_higher } else { s.is_a_higher },
                t.created_ms + 10_000,
            )
        } else {
            None
        };
        let qty = observed.map(|actual| actual.min(qty)).unwrap_or(qty);
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
    let flat = aggregate(&updated, "A").remaining() <= 1e-9
        && aggregate(&updated, "B").remaining() <= 1e-9
        && updated.iter().all(|o| o.observation.state.terminal());
    if flat {
        ctx.pair_cooldown.insert(s.pair.clone(), now_ms() + 60_000);
    }
    if flat && !matches!(reason, "TP" | "MANUAL_CLOSE") && a.opened + b.opened > 0.0 {
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

#[cfg(test)]
mod transport_tests {
    use super::*;
    use std::sync::Mutex;
    struct LostAck {
        queries: Mutex<Vec<String>>,
        submits: std::sync::atomic::AtomicU32,
        resolved: bool,
    }
    #[async_trait::async_trait]
    impl OrderTransport for LostAck {
        async fn submit(&self, _: &OrderRequest) -> Result<OrderObservation, String> {
            self.submits.fetch_add(1, Ordering::Relaxed);
            Err("Connection lost after exchange accepted order".into())
        }
        async fn query(
            &self,
            r: &OrderRequest,
            _: Option<&str>,
        ) -> Result<OrderObservation, String> {
            self.queries.lock().unwrap().push(r.client_id.clone());
            if self.resolved {
                Ok(OrderObservation {
                    order_id: Some("exchange-owned-id".into()),
                    state: OrderState::Cancelled,
                    filled_quantity: 3.0,
                    average_price: Some(10.0),
                    fee: Some(0.01),
                })
            } else {
                Err("Exchange unavailable".into())
            }
        }
    }
    async fn journal() -> Journal {
        let db = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
        crate::engine::startup::init_trades_db(&db).await.unwrap();
        let j = Journal { db };
        j.init().await.unwrap();
        j
    }
    fn order() -> JournalOrder {
        JournalOrder {
            trade_id: 42,
            leg: "A".into(),
            exchange: "binance".into(),
            request: OrderRequest {
                client_id: "persist-before-send".into(),
                pair: "TESTUSDT".into(),
                is_buy: true,
                quantity: 10.0,
                limit_price: 10.0,
                reduce_only: false,
                multiplier: 1.0,
                tick_size: 0.01,
                leverage: 1.0,
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
    #[tokio::test]
    async fn lost_ack_recovers_same_order_without_second_submission() {
        let j = journal().await;
        let o = order();
        let fake = LostAck {
            queries: Mutex::new(vec![]),
            submits: std::sync::atomic::AtomicU32::new(0),
            resolved: true,
        };
        let observed = submit_using(&j, &o, &fake).await.unwrap();
        assert_eq!(observed.filled_quantity, 3.0);
        assert_eq!(observed.state, OrderState::Cancelled);
        assert_eq!(fake.submits.load(Ordering::Relaxed), 1);
        assert_eq!(
            fake.queries.lock().unwrap().as_slice(),
            [o.request.client_id.clone()]
        );
        // Duplicate invocation after a restart fails at the durable identity BEFORE transport.
        assert!(submit_using(&j, &o, &fake).await.is_err());
        assert_eq!(fake.submits.load(Ordering::Relaxed), 1);
        assert_eq!(
            j.orders(42).await.unwrap()[0].observation.filled_quantity,
            3.0
        );
    }
    #[tokio::test]
    async fn timeout_is_persisted_as_unknown_never_zero_fill_success() {
        let j = journal().await;
        let fake = LostAck {
            queries: Mutex::new(vec![]),
            submits: std::sync::atomic::AtomicU32::new(0),
            resolved: false,
        };
        let result = submit_using(&j, &order(), &fake).await.unwrap();
        assert_eq!(result.state, OrderState::Unknown);
        assert!(!result.state.terminal());
        assert_eq!(
            j.orders(42).await.unwrap()[0].observation.state,
            OrderState::Unknown
        );
        assert_eq!(fake.submits.load(Ordering::Relaxed), 1);
    }
}
