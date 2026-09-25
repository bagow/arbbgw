//! Order ownership, bounded entries and independent position supervision.
use super::{
    exchanges::{get_connector, traits::ExchangeConnector},
    journal::{aggregate, Journal, JournalOrder, ManagedTrade, Plan},
    orders::{OrderObservation, OrderRequest, OrderState},
    real::ExchangeKeys,
    utils::now_ms,
};
use crate::state::{EngineContext, PriceMap};
use reqwest::Client;
use shared::{TechnicalSettings, TradeSignal, TradingSettings};
use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

static ENTRY_BUSY: AtomicBool = AtomicBool::new(false);
static LAST_PREFLIGHT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
const FEE_TTL_MS: i64 = 3_600_000;
const NORMAL_EXIT_PAIR_COOLDOWN_MS: i64 = 60_000;
const RISK_EXIT_PAIR_COOLDOWN_MS: i64 = 30 * 60_000;
const ENTRY_DIRECTION_CONFIRMATION_MS: u64 = 500;
const ENTRY_WIDENING_EPSILON_PCT: f64 = 0.000001;

async fn timed_preflight<F: Future>(future: F) -> (F::Output, i64) {
    let started_ms = now_ms();
    let output = future.await;
    (output, now_ms().saturating_sub(started_ms))
}

fn pair_cooldown_after_close(reason: &str) -> i64 {
    if matches!(reason, "TP" | "MANUAL_CLOSE") {
        NORMAL_EXIT_PAIR_COOLDOWN_MS
    } else {
        RISK_EXIT_PAIR_COOLDOWN_MS
    }
}

fn trailing_stop_reached(percent: f64, peak: f64, activation: f64, distance: f64) -> bool {
    distance > 0.0 && peak >= activation && percent <= (peak - distance).max(0.0)
}

fn entry_spread_is_widening(initial_pct: f64, confirmed_pct: f64) -> bool {
    confirmed_pct > initial_pct + ENTRY_WIDENING_EPSILON_PCT
}

fn entry_ioc_limit(quote: super::book::DepthQuote, buy: bool, movement_budget_pct: f64) -> f64 {
    quote.worst_price
        * (1.0
            + if buy {
                movement_budget_pct / 100.0
            } else {
                -movement_budget_pct / 100.0
            })
}

fn hedge_quote_fits_bound(
    quote: super::book::DepthQuote,
    first_leg_buy: bool,
    bound: f64,
) -> bool {
    bound.is_finite()
        && bound > 0.0
        && if first_leg_buy {
            quote.vwap >= bound && quote.worst_price >= bound
        } else {
            quote.vwap <= bound && quote.worst_price <= bound
        }
}

fn hedge_unavailable_reason(
    on_grid: bool,
    has_fresh_quote: bool,
    binance_refresh_failed: bool,
) -> Option<&'static str> {
    if !on_grid {
        Some("HEDGE_UNAVAILABLE")
    } else if !has_fresh_quote {
        Some(if binance_refresh_failed {
            "HEDGE_MARKET_DATA_UNAVAILABLE"
        } else {
            "HEDGE_UNAVAILABLE"
        })
    } else {
        None
    }
}
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

fn record_entry_decision(ctx: &EngineContext, decision: &'static str) {
    *ctx.entry_decisions.entry(decision.to_string()).or_insert(0) += 1;
}

pub async fn kill(ctx: &EngineContext, j: &Journal, reason: String) {
    ctx.is_killed.store(true, Ordering::Release);
    tracing::error!("[managed] NEW ENTRIES HALTED: {}", reason);
    let _ = j.alert("execution_integrity".into(), reason).await;
    if let Some(db) = &ctx.profiles_db {
        if let Err(e)=db.call(|c|{c.execute("INSERT INTO server_cache(key,value,expires_at) VALUES('kill_switch_active','1',0) ON CONFLICT(key) DO UPDATE SET value='1'",[])?;Ok(())}).await {tracing::error!("Cannot persist kill switch: {}",e);}
    }
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
        || !super::utils::local_timestamp_is_fresh(
            now,
            (d.last_updated as i64).saturating_mul(1_000),
            1_999,
        )
    {
        return None;
    }
    if let Some(book) = ctx.l2_books.get(&format!("{}:{}", ex, pair)) {
        if super::utils::local_timestamp_is_fresh(now, book.updated_ms, 750) {
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

async fn refresh_binance_execution_book(
    pair: &str,
    prices: &PriceMap,
    http: &Client,
    ctx: &EngineContext,
) -> Result<(), String> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let rest_sender = sender.clone();
    let rest_http = http.clone();
    let rest_pair = pair.to_string();
    tokio::spawn(async move {
        let result =
            super::exchanges::binance::fetch_public_depth_snapshot(&rest_http, &rest_pair).await;
        let _ = rest_sender.send(("REST", result)).await;
    });
    let ws_pair = pair.to_string();
    tokio::spawn(async move {
        let result = super::exchanges::binance::fetch_public_depth_snapshot_ws(&ws_pair).await;
        let _ = sender.send(("WS API", result)).await;
    });

    let mut errors = Vec::with_capacity(2);
    let (source, book) = loop {
        let Some((source, result)) = receiver.recv().await else {
            return Err(format!(
                "Binance REST and WS API depth unavailable: {}",
                errors.join("; ")
            ));
        };
        match result {
            Ok(book) => break (source, book),
            Err(error) => errors.push(format!("{source}: {error}")),
        }
    };
    tracing::debug!("[managed] Binance hedge depth refreshed through {source}");
    let bid = book.bids.first().ok_or("Binance depth has no bid")?;
    let ask = book.asks.first().ok_or("Binance depth has no ask")?;
    if let Some(mut venue) = prices.get_mut("binance") {
        if let Some(ticker) = venue.get_mut(pair) {
            ticker.bid = bid.0;
            ticker.ask = ask.0;
            ticker.price = (bid.0 + ask.0) / 2.0;
            ticker.bid_size = bid.0 * bid.1;
            ticker.ask_size = ask.0 * ask.1;
            ticker.last_updated = (book.updated_ms / 1000) as u64;
        }
    }
    ctx.l2_books.insert(format!("binance:{pair}"), book);
    Ok(())
}

async fn actual_taker_fee(
    connector: &dyn ExchangeConnector,
    exchange: &str,
    pair: &str,
    http: &Client,
    keys: &ExchangeKeys,
    ctx: &EngineContext,
) -> Result<f64, String> {
    let cache_key = super::utils::exchange_pair_key(exchange, pair);
    let now = now_ms();
    let fresh = ctx.fee_updated_ms.get(&cache_key)
        .map(|stamp| now >= *stamp && now - *stamp <= FEE_TTL_MS).unwrap_or(false);
    if fresh {
        if let Some(value) = ctx.fee_rates.get(&cache_key).map(|v| *v) {
            if value.is_finite() && (0.0..=0.01).contains(&value) { return Ok(value); }
        }
    }
    let value = connector.fetch_taker_fee(http, keys, pair).await?;
    if !value.is_finite() || !(0.0..=0.01).contains(&value) {
        return Err(format!("Invalid {exchange}:{pair} taker fee"));
    }
    ctx.fee_rates.insert(cache_key.clone(), value);
    ctx.fee_updated_ms.insert(cache_key, now_ms());
    Ok(value)
}

async fn retry_once<F, Fut>(
    initial: Result<(), String>,
    deadline: std::time::Duration,
    retry: F,
) -> Result<(), String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let first_error = match initial {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };
    match tokio::time::timeout(deadline, retry()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(retry_error)) => Err(format!(
            "initial: {}; retry: {}",
            first_error, retry_error
        )),
        Err(_) => Err(format!("initial: {}; retry timed out", first_error)),
    }
}

async fn confirm_leverage_with_retry(
    initial: Result<(), String>,
    connector: &dyn ExchangeConnector,
    exchange: &str,
    pair: &str,
    leverage: f64,
    http: &Client,
    keys: &ExchangeKeys,
) -> Result<(), String> {
    let Some(first_error) = initial.as_ref().err() else {
        return Ok(());
    };
    tracing::warn!(
        "[managed] Initial leverage setup failed for {} on {}: {}; retrying once before exposure",
        pair,
        exchange,
        first_error,
    );
    let result = retry_once(initial, std::time::Duration::from_secs(5), || {
        connector.ensure_leverage(http, keys, pair, leverage)
    })
    .await;
    if result.is_ok() {
        tracing::info!(
            "[managed] Leverage setup retry recovered for {} on {}",
            pair,
            exchange,
        );
    }
    result
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
    j.execution_event(o.trade_id, &o.request.client_id, "SEND_STARTED", None).await?;
    let mut known = o.observation.clone();
    match transport.submit(&o.request).await {
        Ok(v) => {
            j.observe(o, v.clone()).await?;
            j.execution_event(o.trade_id, &o.request.client_id, "SUBMIT_RETURNED", Some(format!("state={:?};filled={}", v.state, v.filled_quantity))).await?;
            known = v;
        }
        Err(e) => {
            j.execution_event(o.trade_id, &o.request.client_id, "SUBMIT_UNKNOWN", Some(e.clone())).await?;
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
            j.execution_event(o.trade_id, &o.request.client_id, "QUERY_RETURNED", Some(format!("state={:?};filled={}", v.state, v.filled_quantity))).await?;
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
    // Empty engine ticks must not consume LAST_PREFLIGHT. Previously they advanced
    // the two-second throttle continuously, delaying the first real short-lived signal.
    if signals.is_empty()
        || ctx.is_killed.load(Ordering::Acquire)
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
        record_entry_decision(ctx, "preflight_throttled");
        return Ok(());
    }
    LAST_PREFLIGHT.store(now, Ordering::Release);
    for mut s in signals.into_iter().take(3) {
        record_entry_decision(ctx, "signal_received");
        if ctx.is_killed.load(Ordering::Acquire) {
            record_entry_decision(ctx, "rejected_kill_switch");
            break;
        }
        let p = match settings
            .iter()
            .find(|p| p.id == s.profile_id && p.is_active)
        {
            Some(p) => p.clone(),
            None => {
                record_entry_decision(ctx, "rejected_profile_unavailable");
                continue;
            }
        };
        p.validate().map_err(anyhow::Error::msg)?;
        if p.daily_loss_limit_usdt > 0.0 {
            let daily = j.daily_verified_pnl(&p.id).await?;
            if daily <= -p.daily_loss_limit_usdt {
                kill(ctx,j,format!("Daily VERIFIED loss limit reached for {}: {:.4} <= -{:.4} USDT",p.id,daily,p.daily_loss_limit_usdt)).await;
                return Ok(());
            }
        }
        if ts.entry_order_type != "ioc" || p.strategy_type != "ema" {
            record_entry_decision(ctx, "rejected_execution_mode");
            continue;
        }
        let (ca, cb) = match (get_connector(&s.exchange_a), get_connector(&s.exchange_b)) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                record_entry_decision(ctx, "rejected_connector_unavailable");
                continue;
            }
        };
        if !ca.supports_managed_orders() || !cb.supports_managed_orders() {
            record_entry_decision(ctx, "rejected_connector_capability");
            continue;
        }
        if !super::risk_manager::verify_trade_health(&s.exchange_a, &s.exchange_b, ctx) {
            record_entry_decision(ctx, "rejected_exchange_health");
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
            record_entry_decision(ctx, "rejected_cached_margin");
            continue;
        }
        let cooldown = ctx.pair_cooldown.get(&s.pair).map(|v| *v).unwrap_or(0);
        if super::utils::cooldown_active(cooldown, now_ms(), ts.pair_cooldown_ms) {
            record_entry_decision(ctx, "rejected_pair_cooldown");
            continue;
        }
        let recent_pair_losses = j.recent_verified_pair_losses(&s.pair).await?;
        if recent_pair_losses >= super::journal::PAIR_LOSS_QUARANTINE_THRESHOLD {
            tracing::info!(
                "[managed] Pair loss quarantine rejected {}: {} VERIFIED losses in the last 7d",
                s.pair,
                recent_pair_losses,
            );
            record_entry_decision(ctx, "rejected_pair_loss_quarantine");
            continue;
        }
        // These checks are independent and all happen before the first leg. Run
        // them together so serial exchange requests do not consume a short-lived
        // edge. Every result is still required and errors fail closed.
        let preflight_started_ms = now_ms();
        let (
            (posa, posa_ms),
            (posb, posb_ms),
            (idle_a, idle_a_ms),
            (idle_b, idle_b_ms),
            (fee_a, fee_a_ms),
            (fee_b, fee_b_ms),
            (ba, ba_ms),
            (bb, bb_ms),
            (la, la_ms),
            (lb, lb_ms),
            (fa, fa_ms),
            (fb, fb_ms),
        ) = tokio::join!(
            timed_preflight(ca.fetch_positions(http, keys)),
            timed_preflight(cb.fetch_positions(http, keys)),
            timed_preflight(ca.symbol_has_no_open_orders(http, keys, &s.pair)),
            timed_preflight(cb.symbol_has_no_open_orders(http, keys, &s.pair)),
            timed_preflight(actual_taker_fee(
                ca.as_ref(),
                &s.exchange_a,
                &s.pair,
                http,
                keys,
                ctx,
            )),
            timed_preflight(actual_taker_fee(
                cb.as_ref(),
                &s.exchange_b,
                &s.pair,
                http,
                keys,
                ctx,
            )),
            timed_preflight(ca.fetch_balances(http, keys)),
            timed_preflight(cb.fetch_balances(http, keys)),
            timed_preflight(ca.ensure_leverage(http, keys, &s.pair, p.leverage as f64)),
            timed_preflight(cb.ensure_leverage(http, keys, &s.pair, p.leverage as f64)),
            timed_preflight(super::funding_guard::next(http, &s.exchange_a, &s.pair,)),
            timed_preflight(super::funding_guard::next(http, &s.exchange_b, &s.pair,)),
        );
        tracing::info!(
            "[managed] Preflight timings for {}: positions {}={}ms {}={}ms; open_orders {}={}ms {}={}ms; fees {}={}ms {}={}ms; balances {}={}ms {}={}ms; leverage {}={}ms {}={}ms; funding {}={}ms {}={}ms",
            s.pair,
            s.exchange_a,
            posa_ms,
            s.exchange_b,
            posb_ms,
            s.exchange_a,
            idle_a_ms,
            s.exchange_b,
            idle_b_ms,
            s.exchange_a,
            fee_a_ms,
            s.exchange_b,
            fee_b_ms,
            s.exchange_a,
            ba_ms,
            s.exchange_b,
            bb_ms,
            s.exchange_a,
            la_ms,
            s.exchange_b,
            lb_ms,
            s.exchange_a,
            fa_ms,
            s.exchange_b,
            fb_ms,
        );
        let (posa, posb) = match (posa, posb) {
            (Ok(a), Ok(b)) => (a, b),
            _ => {
                record_entry_decision(ctx, "rejected_positions_fetch");
                continue;
            }
        };
        if posa
            .iter()
            .chain(posb.iter())
            .any(|x| super::utils::symbols_match(&x.symbol, &s.pair) && x.size.abs() > 1e-12)
        {
            record_entry_decision(ctx, "rejected_existing_position");
            continue;
        }
        if !matches!(idle_a, Ok(true)) || !matches!(idle_b, Ok(true)) {
            record_entry_decision(ctx, "rejected_open_orders_check");
            continue;
        }
        let (fee_a, fee_b) = match (fee_a, fee_b) {
            (Ok(a), Ok(b)) => (a, b),
            (a, b) => {
                tracing::warn!("[managed] fee preflight failed for {} {}-{}: {:?} / {:?}",s.pair,s.exchange_a,s.exchange_b,a.err(),b.err());
                record_entry_decision(ctx, "rejected_fee_fetch");
                continue;
            }
        };
        s.break_even_pct = super::scanner_common::ioc_break_even_pct(fee_a, fee_b);
        let (available_a, available_b) = match (ba, bb) {
            (Ok((_, a)), Ok((_, b))) if a.is_finite() && b.is_finite() => (a, b),
            _ => {
                record_entry_decision(ctx, "rejected_balance_fetch");
                continue;
            }
        };
        let required_a = s.actual_vol_a_usd / (p.leverage as f64)
            * ts.margin_check_multiplier.max(1.0)
            + s.actual_vol_a_usd * 0.002;
        let required_b = s.actual_vol_b_usd / (p.leverage as f64)
            * ts.margin_check_multiplier.max(1.0)
            + s.actual_vol_b_usd * 0.002;
        if available_a < required_a || available_b < required_b {
            record_entry_decision(ctx, "rejected_live_margin");
            continue;
        }
        // A transient setup request must not consume a short-lived edge. Retry only
        // the failed venue once, while there is still no exposure, then re-price the
        // complete trade from fresh L2 below. Persistent failures remain fail-closed.
        let leverage_retry_started_ms = now_ms();
        let (la, lb) = tokio::join!(
            confirm_leverage_with_retry(
                la,
                ca.as_ref(),
                &s.exchange_a,
                &s.pair,
                p.leverage as f64,
                http,
                keys,
            ),
            confirm_leverage_with_retry(
                lb,
                cb.as_ref(),
                &s.exchange_b,
                &s.pair,
                p.leverage as f64,
                http,
                keys,
            ),
        );
        let leverage_retry_ms = now_ms().saturating_sub(leverage_retry_started_ms);
        // Both venues must be authoritatively configured before taking exposure.
        if la.is_err() || lb.is_err() {
            tracing::warn!(
                "[managed] leverage setup failed for {} on {} / {} on {}: {:?} / {:?}",
                s.pair,
                s.exchange_a,
                s.pair,
                s.exchange_b,
                la.as_ref().err(),
                lb.as_ref().err(),
            );
            record_entry_decision(ctx, "rejected_leverage_setup");
            continue;
        }
        let funding_deadline_ms = match (fa, fb) {
            (Ok(a), Ok(b)) => super::funding_guard::deadline(a, b, now_ms()),
            _ => None,
        };
        if funding_deadline_ms.is_none() {
            record_entry_decision(ctx, "rejected_funding_fetch");
            continue;
        }
        tracing::info!(
            "[managed] Authoritative preflight completed for {} in {}ms (leverage retry phase {}ms)",
            s.pair,
            now_ms().saturating_sub(preflight_started_ms),
            leverage_retry_ms,
        );
        // The Binance public WS occasionally queues seconds-old frames on the VPS.
        // Refresh its top-5 snapshot after all slower private/funding checks, so the
        // final IOC quote is based on authoritative data inside the 750 ms budget.
        if s.exchange_a == "binance" || s.exchange_b == "binance" {
            if let Err(error) = refresh_binance_execution_book(&s.pair, prices, http, ctx).await {
                tracing::warn!("[managed] Binance L2 refresh failed for {}: {}", s.pair, error);
                record_entry_decision(ctx, "rejected_binance_l2_refresh");
                continue;
            }
        }
        let (quote_a, quote_b) = match (
            executable_quote(
                prices,
                ctx,
                &s.exchange_a,
                &s.pair,
                !s.is_a_higher,
                s.target_qty_a,
            ),
            executable_quote(
                prices,
                ctx,
                &s.exchange_b,
                &s.pair,
                s.is_a_higher,
                s.target_qty_b,
            ),
        ) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                record_entry_decision(ctx, "rejected_executable_l2");
                continue;
            }
        };
        let (pa, pb) = (quote_a.vwap, quote_b.vwap);
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
            let spec_a = ctx
                .specifications
                .get(&format!("{}:{}", s.exchange_a, s.pair))
                .map(|spec| spec.value().clone());
            let spec_b = ctx
                .specifications
                .get(&format!("{}:{}", s.exchange_b, s.pair))
                .map(|spec| spec.value().clone());
            tracing::info!(
                "[managed] Contract specification rejected {}: {} qty={} price={} step={} tick={} spec={:?}; {} qty={} price={} step={} tick={} spec={:?}",
                s.pair,
                s.exchange_a,
                s.target_qty_a,
                pa,
                s.mult_a,
                s.tick_a,
                spec_a,
                s.exchange_b,
                s.target_qty_b,
                pb,
                s.mult_b,
                s.tick_b,
                spec_b,
            );
            record_entry_decision(ctx, "rejected_contract_specification");
            continue;
        }
        let gross = if s.is_a_higher {
            (pa - pb) / pb * 100.0
        } else {
            (pb - pa) / pa * 100.0
        };
        let _initial_budget = match super::scanner_common::entry_execution_budget(
            gross,
            p.min_entry_spread,
            p.max_entry_spread,
            s.structural_spread,
            p.min_structural_deviation,
            s.break_even_pct,
        ) {
            Some(value) => value,
            None => {
                let required_floor = p
                    .min_entry_spread
                    .max(s.structural_spread + s.break_even_pct + p.min_structural_deviation);
                tracing::info!(
                    "[managed] Entry budget rejected {}: signal={:.4}% fresh_l2={:.4}% required={:.4}% structural={:.4}% break_even={:.4}% window={:.4}%..={:.4}%",
                    s.pair,
                    s.spread,
                    gross,
                    required_floor,
                    s.structural_spread,
                    s.break_even_pct,
                    p.min_entry_spread,
                    p.max_entry_spread,
                );
                record_entry_decision(ctx, "rejected_entry_budget");
                continue;
            }
        };
        // A large cross-venue deviation that is still expanding is momentum,
        // not confirmed mean reversion. Trade 2041 entered while executable L2
        // widened from 0.6481% to 0.7367% and then 0.8469%, immediately reaching
        // the hard stop. Re-sample after several Binance depth cycles and require
        // the edge to be stable or narrowing while still clearing the full budget.
        let initial_gross = gross;
        tokio::time::sleep(std::time::Duration::from_millis(
            ENTRY_DIRECTION_CONFIRMATION_MS,
        ))
        .await;
        if s.exchange_a == "binance" || s.exchange_b == "binance" {
            if let Err(error) = refresh_binance_execution_book(&s.pair, prices, http, ctx).await {
                tracing::warn!(
                    "[managed] Binance L2 confirmation refresh failed for {}: {}",
                    s.pair,
                    error,
                );
                record_entry_decision(ctx, "rejected_direction_confirmation_refresh");
                continue;
            }
        }
        let (quote_a, quote_b) = match (
            executable_quote(
                prices,
                ctx,
                &s.exchange_a,
                &s.pair,
                !s.is_a_higher,
                s.target_qty_a,
            ),
            executable_quote(
                prices,
                ctx,
                &s.exchange_b,
                &s.pair,
                s.is_a_higher,
                s.target_qty_b,
            ),
        ) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                record_entry_decision(ctx, "rejected_direction_confirmation_l2");
                continue;
            }
        };
        let (pa, pb) = (quote_a.vwap, quote_b.vwap);
        let gross = if s.is_a_higher {
            (pa - pb) / pb * 100.0
        } else {
            (pb - pa) / pa * 100.0
        };
        if entry_spread_is_widening(initial_gross, gross) {
            tracing::info!(
                "[managed] Direction confirmation rejected {}: executable L2 widened {:.4}% -> {:.4}%",
                s.pair,
                initial_gross,
                gross,
            );
            record_entry_decision(ctx, "rejected_widening_l2");
            continue;
        }
        let budget = match super::scanner_common::entry_execution_budget(
            gross,
            p.min_entry_spread,
            p.max_entry_spread,
            s.structural_spread,
            p.min_structural_deviation,
            s.break_even_pct,
        ) {
            Some(value) => value,
            None => {
                let required_floor = p
                    .min_entry_spread
                    .max(s.structural_spread + s.break_even_pct + p.min_structural_deviation);
                tracing::info!(
                    "[managed] Direction confirmation budget rejected {}: initial_l2={:.4}% confirmed_l2={:.4}% required={:.4}%",
                    s.pair,
                    initial_gross,
                    gross,
                    required_floor,
                );
                record_entry_decision(ctx, "rejected_direction_confirmation_budget");
                continue;
            }
        };
        let required = gross - budget;
        let half = (budget / 2.0).min(p.slippage_percent);
        if half < 0.0 {
            record_entry_decision(ctx, "rejected_slippage_budget");
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
            record_entry_decision(ctx, "rejected_authorization");
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
            None => {
                record_entry_decision(ctx, "rejected_reservation");
                continue;
            }
        };
        record_entry_decision(ctx, "trade_reserved");
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
        let (first, second, quote, buy, qty) = if first_a {
            ("A", "B", quote_a, !s.is_a_higher, s.target_qty_a)
        } else {
            ("B", "A", quote_b, s.is_a_higher, s.target_qty_b)
        };
        let second_exchange = if first_a {
            &s.exchange_b
        } else {
            &s.exchange_a
        };
        // A VWAP is not an executable IOC limit when the quantity spans more
        // than one level. Reach the last observed level, then reserve only the
        // remaining movement budget for races after the snapshot.
        let limit = entry_ioc_limit(quote, buy, half);
        let o = request(&t, first, qty, limit, false);
        // KuCoin may need a short private-order confirmation before its fill is
        // authoritative. Overlap that wait with independent Binance REST and
        // WS API depth requests instead of leaving the hedge quote to age for
        // the whole first leg.
        let (v, binance_refresh) = if second_exchange == "binance" {
            let (fill, refreshed) = tokio::join!(submit(j, &o, http, keys), async {
                let result = refresh_binance_execution_book(&s.pair, prices, http, ctx).await;
                (result, now_ms())
            });
            (fill?, Some(refreshed))
        } else {
            (submit(j, &o, http, keys).await?, None)
        };
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
        // The first venue's submit/query round trip is commonly longer than
        // the 750 ms execution-book freshness budget. The overlapping refresh
        // above should normally be fresh; repeat it only when that refresh has
        // already aged for 500 ms, preserving time to submit the bounded hedge.
        let mut binance_refresh_error = None;
        if on_grid && second_exchange == "binance" {
            let Some((mut refresh_result, refreshed_ms)) = binance_refresh else {
                j.state(id, "CLOSING", "HEDGE_MARKET_DATA_UNAVAILABLE")
                    .await?;
                return Ok(());
            };
            if refresh_result.is_ok() && now_ms() - refreshed_ms > 500 {
                refresh_result = refresh_binance_execution_book(&s.pair, prices, http, ctx).await;
            }
            if let Err(error) = refresh_result {
                tracing::warn!(
                    "[managed] Binance hedge L2 refresh failed for {}: {}; requiring a fresh streaming L2 book",
                    s.pair,
                    error
                );
                binance_refresh_error = Some(error);
            }
        }
        let hedge_quote = executable_quote(
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
        if let Some(reason) = hedge_unavailable_reason(
            on_grid,
            hedge_quote.is_some(),
            binance_refresh_error.is_some(),
        ) {
            j.state(id, "CLOSING", reason).await?;
            return Ok(());
        }
        if let Some(error) = binance_refresh_error {
            tracing::info!(
                "[managed] Using fresh Binance streaming L2 for {} hedge after snapshot refresh error: {}",
                s.pair,
                error
            );
        }
        let actual = v.average_price.unwrap();
        // Solve spread inequality using the actual first fill. This preserves the total budget.
        let bound = if buy {
            actual * (1.0 + required / 100.0)
        } else {
            actual / (1.0 + required / 100.0)
        };
        let hedge_quote = hedge_quote.unwrap();
        // Both average and last consumed level must fit inside the profit
        // boundary. Otherwise an IOC at `bound` can partially hedge again.
        if !hedge_quote_fits_bound(hedge_quote, buy, bound) {
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
    let mut hard_stop = false;
    let mut trailing_stop = false;
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
            a.remaining() * pa * super::utils::get_exchange_fee(&s.exchange_a, &s.pair, 0.001, ctx)
                + b.remaining() * pb * super::utils::get_exchange_fee(&s.exchange_b, &s.pair, 0.001, ctx);
        let entry_fees: f64 = orders
            .iter()
            .map(|o| {
                o.observation
                    .fee_with_reserve(super::utils::get_exchange_fee(&o.exchange, &s.pair, 0.001, ctx))
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
                * super::utils::get_exchange_fee(&s.exchange_a, &s.pair, 0.001, ctx)
                + b.remaining()
                    * qb.worst_price
                    * super::utils::get_exchange_fee(&s.exchange_b, &s.pair, 0.001, ctx);
            let worst_percent =
                (worst_pnl - entry_fees - worst_reserve) / a.entry_value.max(1e-12) * 100.0;
            profit = worst_percent >= t.plan.settings.take_profit_percent.max(0.05);
        }
        hard_stop = t.plan.settings.hard_stop_percent > 0.0
            && percent <= -t.plan.settings.hard_stop_percent;
        trailing_stop = trailing_stop_reached(
            percent,
            peak,
            t.plan.settings.trailing_activation_percent,
            t.plan.settings.trailing_stop_percent,
        );
    }
    if !closing && !timed && !funding_due && !profit && !hard_stop && !trailing_stop {
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
    } else if hard_stop {
        "HARD_STOP"
    } else if trailing_stop {
        "TRAILING_STOP"
    } else {
        "TP"
    };
    j.state(t.id, "CLOSING", reason).await?;
    // Exit starts only once. Partial closes remain journaled and only residuals are submitted.
    let emergency = closing || timed || funding_due || hard_stop || trailing_stop;
    let mut legs = vec![
        ("A", a.remaining(), quote_a.map(|q| q.worst_price)),
        ("B", b.remaining(), quote_b.map(|q| q.worst_price)),
    ];
    legs.sort_by_key(|(leg, _, _)| {
        let exchange = if *leg == "A" { &s.exchange_a } else { &s.exchange_b };
        std::cmp::Reverse(super::utils::liquidity_rank(exchange))
    });
    for (leg, qty, px) in legs {
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
        ctx.pair_cooldown
            .insert(s.pair.clone(), now_ms() + pair_cooldown_after_close(reason));
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

    #[test]
    fn risk_exits_quarantine_the_pair_longer_than_normal_exits() {
        assert_eq!(pair_cooldown_after_close("TP"), 60_000);
        assert_eq!(pair_cooldown_after_close("MANUAL_CLOSE"), 60_000);
        assert_eq!(pair_cooldown_after_close("HARD_STOP"), 1_800_000);
        assert_eq!(pair_cooldown_after_close("HEDGE_UNAVAILABLE"), 1_800_000);
    }

    #[test]
    fn armed_trailing_stop_never_sets_a_negative_net_floor() {
        assert!(!trailing_stop_reached(0.05, 0.60, 0.10, 1.0));
        assert!(trailing_stop_reached(-0.01, 0.60, 0.10, 1.0));
        assert!(trailing_stop_reached(0.29, 0.60, 0.10, 0.30));
        assert!(!trailing_stop_reached(-0.50, 0.05, 0.10, 0.30));
    }

    #[test]
    fn entry_direction_confirmation_rejects_an_expanding_l2_spread() {
        assert!(entry_spread_is_widening(0.693082, 0.736672));
        assert!(!entry_spread_is_widening(0.736672, 0.693082));
        assert!(!entry_spread_is_widening(0.693082, 0.693082));
    }

    #[test]
    fn entry_ioc_limit_reaches_the_last_consumed_depth_level() {
        let buy = crate::engine::book::DepthQuote {
            vwap: 101.0,
            worst_price: 102.0,
        };
        let sell = crate::engine::book::DepthQuote {
            vwap: 99.0,
            worst_price: 98.0,
        };
        assert_eq!(entry_ioc_limit(buy, true, 0.0), 102.0);
        assert_eq!(entry_ioc_limit(sell, false, 0.0), 98.0);
        assert!(entry_ioc_limit(buy, true, 0.05) > buy.worst_price);
        assert!(entry_ioc_limit(sell, false, 0.05) < sell.worst_price);
    }

    #[test]
    fn hedge_rejects_when_vwap_fits_but_last_level_does_not() {
        let sell = crate::engine::book::DepthQuote {
            vwap: 100.0,
            worst_price: 99.0,
        };
        assert!(!hedge_quote_fits_bound(sell, true, 99.5));
        assert!(hedge_quote_fits_bound(sell, true, 98.5));

        let buy = crate::engine::book::DepthQuote {
            vwap: 100.0,
            worst_price: 101.0,
        };
        assert!(!hedge_quote_fits_bound(buy, false, 100.5));
        assert!(hedge_quote_fits_bound(buy, false, 101.5));
    }

    #[test]
    fn fresh_ws_hedge_quote_survives_a_rest_refresh_failure() {
        assert_eq!(hedge_unavailable_reason(true, true, true), None);
        assert_eq!(
            hedge_unavailable_reason(true, false, true),
            Some("HEDGE_MARKET_DATA_UNAVAILABLE")
        );
        assert_eq!(
            hedge_unavailable_reason(false, true, true),
            Some("HEDGE_UNAVAILABLE")
        );
    }

    #[tokio::test]
    async fn successful_leverage_setup_is_not_repeated() {
        let calls = std::sync::atomic::AtomicU32::new(0);
        let result = retry_once(Ok(()), std::time::Duration::from_millis(10), || async {
            calls.fetch_add(1, Ordering::Relaxed);
            Err("unexpected retry".into())
        })
        .await;
        assert_eq!(result, Ok(()));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn failed_leverage_setup_gets_one_bounded_retry() {
        let calls = std::sync::atomic::AtomicU32::new(0);
        let recovered = retry_once(
            Err("initial transport failure".into()),
            std::time::Duration::from_millis(10),
            || async {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        )
        .await;
        assert_eq!(recovered, Ok(()));
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        let rejected = retry_once(
            Err("initial transport failure".into()),
            std::time::Duration::from_millis(10),
            || async { Err("still unavailable".into()) },
        )
        .await;
        assert_eq!(
            rejected,
            Err("initial: initial transport failure; retry: still unavailable".into())
        );
    }

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
