//! Order ownership, bounded entries and independent position supervision.
use super::{
    exchanges::{get_connector, traits::ExchangeConnector},
    journal::{aggregate, EntryReservation, Journal, JournalOrder, ManagedTrade, Plan},
    orders::{OrderObservation, OrderRequest, OrderState},
    real::ExchangeKeys,
    utils::now_ms,
};
use crate::state::{EngineContext, PriceMap};
use dashmap::{DashMap, DashSet};
use reqwest::Client;
use shared::{TechnicalSettings, TradeSignal, TradingSettings};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, OnceLock,
};

static ENTRY_BUSY: AtomicBool = AtomicBool::new(false);
static LAST_CANDIDATE_PREWARM: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);
static CANDIDATE_PREWARM_INFLIGHT: OnceLock<DashSet<String>> = OnceLock::new();
static CANDIDATE_PREWARM_LAST_ATTEMPT: OnceLock<DashMap<String, i64>> = OnceLock::new();
static BINANCE_DEPTH_PREWARM_INFLIGHT: OnceLock<DashSet<String>> = OnceLock::new();
static LAST_BINANCE_DEPTH_PREWARM: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);
static BINANCE_DEPTH_REST_FALLBACK_SUCCESS: AtomicU64 = AtomicU64::new(0);
static BINANCE_DEPTH_REST_FALLBACK_FAILURE: AtomicU64 = AtomicU64::new(0);
static KUCOIN_DEPTH_PREWARM_INFLIGHT: OnceLock<DashSet<String>> = OnceLock::new();
static KUCOIN_DEPTH_PREWARM_RETRY_AFTER: OnceLock<DashMap<String, i64>> = OnceLock::new();
static LAST_KUCOIN_DEPTH_PREWARM: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);
const FEE_TTL_MS: i64 = 3_600_000;
const OPEN_ORDERS_CLEAR_TTL_MS: i64 = 5_000;
const CANDIDATE_PREWARM_INTERVAL_MS: i64 = 1_000;
// A single transient venue timeout should not make a short-lived pair
// untradeable for half a minute. Calls still share the one-per-second lane.
const CANDIDATE_PREWARM_RETRY_MS: i64 = 5_000;
// The Oracle route can leave Binance's multiplexed market stream seconds old.
// Keep the already-qualified candidate set executable through the independent
// REST lane while that stream recovers. At limit=5 each depth request costs two
// weight, so the five-per-second ceiling is 600 weight/minute; discovery and
// all other callers still share the central 2,200/minute hard stop. A 200 ms
// cadence can round-robin three stale candidates before the scanner's 750 ms
// freshness deadline instead of letting the first candidate monopolize a
// 500 ms lane.
const BINANCE_DEPTH_PREWARM_INTERVAL_MS: i64 = 200;
const KUCOIN_DEPTH_PREWARM_INTERVAL_MS: i64 = 1_000;
const KUCOIN_DEPTH_PREWARM_RETRY_MIN_MS: i64 = 5_000;
const KUCOIN_DEPTH_PREWARM_RETRY_MAX_MS: i64 = 30_000;
const NORMAL_EXIT_PAIR_COOLDOWN_MS: i64 = 60_000;
const RISK_EXIT_PAIR_COOLDOWN_MS: i64 = 30 * 60_000;
const ENTRY_WIDENING_EPSILON_PCT: f64 = 0.000001;

pub fn binance_depth_rest_fallback_metrics() -> (u64, u64) {
    (
        BINANCE_DEPTH_REST_FALLBACK_SUCCESS.load(Ordering::Relaxed),
        BINANCE_DEPTH_REST_FALLBACK_FAILURE.load(Ordering::Relaxed),
    )
}

fn pair_cooldown_after_close(reason: &str) -> i64 {
    if matches!(reason, "TP" | "MANUAL_CLOSE" | "FUNDING_CAPTURE_COMPLETE") {
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

fn leverage_cache_key(exchange: &str, pair: &str, leverage: f64) -> String {
    format!("{exchange}:{pair}:{leverage}")
}

async fn ensure_leverage_cached(
    connector: &dyn ExchangeConnector,
    exchange: &str,
    pair: &str,
    leverage: f64,
    http: &Client,
    keys: &ExchangeKeys,
    ctx: &EngineContext,
) -> Result<(), String> {
    let key = leverage_cache_key(exchange, pair, leverage);
    if ctx.leverage_set.get(&key).is_some_and(|value| *value) {
        return Ok(());
    }
    connector.ensure_leverage(http, keys, pair, leverage).await?;
    ctx.leverage_set.insert(key, true);
    Ok(())
}

fn open_orders_cache_key(exchange: &str, pair: &str) -> String {
    super::utils::exchange_pair_key(exchange, pair)
}

fn open_orders_are_clear(ctx: &EngineContext, exchange: &str, pair: &str, now: i64) -> bool {
    ctx.open_orders_clear_ms
        .get(&open_orders_cache_key(exchange, pair))
        .is_some_and(|stamp| {
            now >= *stamp && now - *stamp <= OPEN_ORDERS_CLEAR_TTL_MS
        })
}

async fn refresh_open_orders_clear(
    connector: &dyn ExchangeConnector,
    exchange: &str,
    pair: &str,
    http: &Client,
    keys: &ExchangeKeys,
    ctx: &EngineContext,
) -> Result<(), String> {
    let key = open_orders_cache_key(exchange, pair);
    match connector.symbol_has_no_open_orders(http, keys, pair).await {
        Ok(true) => {
            ctx.open_orders_clear_ms.insert(key, now_ms());
            Ok(())
        }
        Ok(false) => {
            ctx.open_orders_clear_ms.remove(&key);
            Err(format!("{exchange}:{pair} has an open order"))
        }
        Err(error) => {
            ctx.open_orders_clear_ms.remove(&key);
            Err(error)
        }
    }
}

fn candidate_is_prepared(
    exchange_a: &str,
    exchange_b: &str,
    pair: &str,
    leverage: f64,
    ctx: &EngineContext,
) -> bool {
    let now = now_ms();
    [exchange_a, exchange_b].iter().all(|exchange| {
        let fee_key = super::utils::exchange_pair_key(exchange, pair);
        let fee_ready = ctx
            .fee_updated_ms
            .get(&fee_key)
            .is_some_and(|stamp| now >= *stamp && now - *stamp <= FEE_TTL_MS)
            && ctx.fee_rates.get(&fee_key).is_some_and(|value| {
                value.is_finite() && (0.0..=0.01).contains(value.value())
            });
        fee_ready
            && ctx
                .leverage_set
                .get(&leverage_cache_key(exchange, pair, leverage))
                .is_some_and(|value| *value)
            && super::funding_guard::is_cached(ctx, exchange, pair)
            && open_orders_are_clear(ctx, exchange, pair, now)
    })
}

fn cached_pair_is_flat(ctx: &EngineContext, exchange: &str, pair: &str) -> bool {
    ctx.positions.get(exchange).is_some_and(|positions| {
        positions
            .iter()
            .all(|position| !super::utils::symbols_match(&position.symbol, pair) || position.size.abs() <= 1e-12)
    })
}

fn order_ws_ready(exchange: &str) -> bool {
    match exchange {
        "binance" => super::exchanges::ws_trader::order_lane_ready(),
        "kucoin" => super::exchanges::ws_kucoin_order::is_ready(),
        _ => false,
    }
}

fn execution_depth_needs_prewarm(updated_ms: Option<i64>, now: i64) -> bool {
    !updated_ms.is_some_and(|updated| {
        super::utils::local_timestamp_is_fresh(now, updated, 500)
    })
}

fn kucoin_stale_depth_age_ms(error: &str) -> Option<u64> {
    error
        .strip_prefix("Stale KuCoin public depth snapshot: ")?
        .strip_suffix("ms")?
        .parse()
        .ok()
}

fn kucoin_depth_prewarm_retry_ms(stale_age_ms: Option<u64>) -> i64 {
    stale_age_ms
        .and_then(|age_ms| i64::try_from(age_ms).ok())
        .unwrap_or(KUCOIN_DEPTH_PREWARM_RETRY_MIN_MS)
        .clamp(
            KUCOIN_DEPTH_PREWARM_RETRY_MIN_MS,
            KUCOIN_DEPTH_PREWARM_RETRY_MAX_MS,
        )
}

/// Fill a stale candidate book through Binance REST while the streaming route
/// is catching up. This is bounded globally and never authorizes an entry: the
/// scanner and final preflight still require a <=750 ms full-quantity quote.
pub fn prewarm_binance_execution_book(
    pair: &str,
    prices: PriceMap,
    signal: crate::state::UpdateSignal,
    http: Client,
    ctx: Arc<EngineContext>,
) -> bool {
    let key = format!("binance:{pair}");
    let now = now_ms();
    let updated_ms = ctx.l2_books.get(&key).map(|book| book.updated_ms);
    if !execution_depth_needs_prewarm(updated_ms, now) {
        return false;
    }
    let inflight = BINANCE_DEPTH_PREWARM_INFLIGHT.get_or_init(DashSet::new);
    if inflight.contains(pair) || inflight.len() >= 2 {
        return false;
    }
    let previous = LAST_BINANCE_DEPTH_PREWARM.load(Ordering::Acquire);
    if now - previous < BINANCE_DEPTH_PREWARM_INTERVAL_MS
        || LAST_BINANCE_DEPTH_PREWARM
            .compare_exchange(previous, now, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        || !inflight.insert(pair.to_string())
    {
        return false;
    }
    let pair = pair.to_string();
    tokio::spawn(async move {
        match super::exchanges::binance::fetch_public_depth_snapshot_resilient(&http, &pair).await {
            Ok((book, transport)) => {
                if transport
                    == super::exchanges::binance::PublicDepthTransport::RestFallback
                {
                    BINANCE_DEPTH_REST_FALLBACK_SUCCESS.fetch_add(1, Ordering::Relaxed);
                }
                let key = format!("binance:{pair}");
                let newer = ctx
                    .l2_books
                    .get(&key)
                    .map(|existing| book.updated_ms > existing.updated_ms)
                    .unwrap_or(true);
                if newer {
                    if let (Some(bid), Some(ask), Some(mut venue)) = (
                        book.bids.first(),
                        book.asks.first(),
                        prices.get_mut("binance"),
                    ) {
                        crate::collector::binance::publish_depth_ticker(
                            &mut venue, &pair, *bid, *ask, book.updated_ms,
                        );
                    }
                    ctx.l2_books.insert(key, book);
                    signal.notify_one();
                }
            }
            Err(error) => {
                BINANCE_DEPTH_REST_FALLBACK_FAILURE.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    "[managed] Binance candidate depth prewarm failed for {}: {}",
                    pair,
                    error,
                );
            }
        }
        BINANCE_DEPTH_PREWARM_INFLIGHT
            .get_or_init(DashSet::new)
            .remove(&pair);
    });
    true
}

/// Fill a stale KuCoin candidate book through the bounded public depth20
/// endpoint while the demand-driven 10 ms stream is receiving its first
/// snapshot. This never bypasses the scanner's 750 ms freshness gate.
pub fn prewarm_kucoin_execution_book(
    pair: &str,
    multiplier: f64,
    prices: PriceMap,
    signal: crate::state::UpdateSignal,
    http: Client,
    ctx: Arc<EngineContext>,
) -> bool {
    let key = format!("kucoin:{pair}");
    let now = now_ms();
    let updated_ms = ctx.l2_books.get(&key).map(|book| book.updated_ms);
    if !execution_depth_needs_prewarm(updated_ms, now)
        || !multiplier.is_finite()
        || multiplier <= 0.0
    {
        return false;
    }
    // KuCoin stamps depth20 with the matching-engine update time. An idle
    // contract can therefore return the same correctly rejected stale book
    // on every request. Without a per-pair delay, the first retained route
    // monopolizes the global one-per-second lane and later routes never get a
    // chance to warm. A fresh Pro-stream update still reaches the scanner
    // immediately and remains mandatory in the final entry preflight.
    let retry_after = KUCOIN_DEPTH_PREWARM_RETRY_AFTER.get_or_init(DashMap::new);
    if let Some(deadline_ms) = retry_after.get(pair).map(|deadline| *deadline) {
        if now < deadline_ms {
            return false;
        }
        retry_after.remove(pair);
    }
    let inflight = KUCOIN_DEPTH_PREWARM_INFLIGHT.get_or_init(DashSet::new);
    if inflight.contains(pair) || inflight.len() >= 2 {
        return false;
    }
    let previous = LAST_KUCOIN_DEPTH_PREWARM.load(Ordering::Acquire);
    if now - previous < KUCOIN_DEPTH_PREWARM_INTERVAL_MS
        || LAST_KUCOIN_DEPTH_PREWARM
            .compare_exchange(previous, now, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        || !inflight.insert(pair.to_string())
    {
        return false;
    }
    *ctx.scanner_status
        .entry("kucoin_depth_prewarm_scheduled".to_string())
        .or_insert(0) += 1;
    let pair = pair.to_string();
    tokio::spawn(async move {
        let started_ms = now_ms();
        match super::exchanges::kucoin::fetch_public_depth_snapshot(&http, &pair, multiplier).await {
            Ok(book) => {
                *ctx.scanner_status
                    .entry("kucoin_depth_prewarm_success".to_string())
                    .or_insert(0) += 1;
                ctx.scanner_status.insert(
                    "kucoin_depth_prewarm_last_latency_ms".to_string(),
                    now_ms().saturating_sub(started_ms).max(0) as u64,
                );
                ctx.scanner_status.insert(
                    "kucoin_depth_prewarm_last_book_age_ms".to_string(),
                    now_ms().saturating_sub(book.updated_ms).max(0) as u64,
                );
                let key = format!("kucoin:{pair}");
                let newer = ctx
                    .l2_books
                    .get(&key)
                    .map(|existing| book.updated_ms > existing.updated_ms)
                    .unwrap_or(true);
                if newer {
                    if let Some(mut venue) = prices.get_mut("kucoin") {
                        crate::collector::kucoin::apply_depth_ticker(
                            &mut venue, &pair, &book.bids, &book.asks, book.updated_ms,
                        );
                    }
                    ctx.l2_books.insert(key, book);
                    signal.notify_one();
                }
            }
            Err(error) => {
                let stale_age_ms = kucoin_stale_depth_age_ms(&error);
                let retry_ms = kucoin_depth_prewarm_retry_ms(stale_age_ms);
                KUCOIN_DEPTH_PREWARM_RETRY_AFTER
                    .get_or_init(DashMap::new)
                    .insert(pair.clone(), now_ms().saturating_add(retry_ms));
                *ctx.scanner_status
                    .entry("kucoin_depth_prewarm_failure".to_string())
                    .or_insert(0) += 1;
                ctx.scanner_status.insert(
                    "kucoin_depth_prewarm_last_latency_ms".to_string(),
                    now_ms().saturating_sub(started_ms).max(0) as u64,
                );
                ctx.scanner_status.insert(
                    "kucoin_depth_prewarm_last_retry_ms".to_string(),
                    retry_ms as u64,
                );
                if let Some(age_ms) = stale_age_ms {
                    *ctx.scanner_status
                        .entry("kucoin_depth_prewarm_failure_stale".to_string())
                        .or_insert(0) += 1;
                    ctx.scanner_status.insert(
                        "kucoin_depth_prewarm_last_rejected_book_age_ms".to_string(),
                        age_ms,
                    );
                } else if error.contains("request failed")
                    || error.contains("returned HTTP")
                {
                    *ctx.scanner_status
                        .entry("kucoin_depth_prewarm_failure_transport".to_string())
                        .or_insert(0) += 1;
                } else {
                    *ctx.scanner_status
                        .entry("kucoin_depth_prewarm_failure_invalid".to_string())
                        .or_insert(0) += 1;
                }
                tracing::debug!(
                    "[managed] KuCoin candidate depth prewarm failed for {}: {}",
                    pair,
                    error,
                );
            }
        }
        KUCOIN_DEPTH_PREWARM_INFLIGHT
            .get_or_init(DashSet::new)
            .remove(&pair);
    });
    true
}

/// Warm the slow, pair-specific checks while L2 is approaching the entry
/// threshold. This never opens exposure. The final entry still fetches live
/// positions, orders and balances and re-prices both executable books.
/// Returns true when this route consumed the single scheduling slot for the tick.
pub fn prewarm_candidate(
    exchange_a: &str,
    exchange_b: &str,
    pair: &str,
    leverage: f64,
    http: Client,
    keys: Arc<ExchangeKeys>,
    ctx: Arc<EngineContext>,
) -> bool {
    if candidate_is_prepared(exchange_a, exchange_b, pair, leverage, &ctx) {
        return false;
    }
    let route_key = format!("{exchange_a}:{exchange_b}:{pair}:{leverage}");
    let inflight = CANDIDATE_PREWARM_INFLIGHT.get_or_init(DashSet::new);
    if inflight.contains(&route_key) {
        return true;
    }
    let now = now_ms();
    if CANDIDATE_PREWARM_LAST_ATTEMPT
        .get_or_init(DashMap::new)
        .get(&route_key)
        .is_some_and(|stamp| now >= *stamp && now - *stamp < CANDIDATE_PREWARM_RETRY_MS)
    {
        // Let the caller consider the next candidate instead of allowing one
        // persistently unsupported symbol to monopolize the prewarm lane.
        return false;
    }
    let previous = LAST_CANDIDATE_PREWARM.load(Ordering::Acquire);
    if now - previous < CANDIDATE_PREWARM_INTERVAL_MS
        || LAST_CANDIDATE_PREWARM
            .compare_exchange(previous, now, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
    {
        return true;
    }
    if !inflight.insert(route_key.clone()) {
        return true;
    }
    CANDIDATE_PREWARM_LAST_ATTEMPT
        .get_or_init(DashMap::new)
        .insert(route_key.clone(), now);
    let exchange_a = exchange_a.to_string();
    let exchange_b = exchange_b.to_string();
    let pair = pair.to_string();
    tokio::spawn(async move {
        let started_ms = now_ms();
        let result = match (get_connector(&exchange_a), get_connector(&exchange_b)) {
            (Some(ca), Some(cb)) => {
                let (
                    fee_a,
                    fee_b,
                    leverage_a,
                    leverage_b,
                    funding_a,
                    funding_b,
                    open_orders_a,
                    open_orders_b,
                ) = tokio::join!(
                    actual_taker_fee(ca.as_ref(), &exchange_a, &pair, &http, &keys, &ctx),
                    actual_taker_fee(cb.as_ref(), &exchange_b, &pair, &http, &keys, &ctx),
                    ensure_leverage_cached(
                        ca.as_ref(),
                        &exchange_a,
                        &pair,
                        leverage,
                        &http,
                        &keys,
                        &ctx,
                    ),
                    ensure_leverage_cached(
                        cb.as_ref(),
                        &exchange_b,
                        &pair,
                        leverage,
                        &http,
                        &keys,
                        &ctx,
                    ),
                    super::funding_guard::next(&http, &exchange_a, &pair, &ctx),
                    super::funding_guard::next(&http, &exchange_b, &pair, &ctx),
                    refresh_open_orders_clear(
                        ca.as_ref(),
                        &exchange_a,
                        &pair,
                        &http,
                        &keys,
                        &ctx,
                    ),
                    refresh_open_orders_clear(
                        cb.as_ref(),
                        &exchange_b,
                        &pair,
                        &http,
                        &keys,
                        &ctx,
                    ),
                );
                if fee_a.is_ok()
                    && fee_b.is_ok()
                    && leverage_a.is_ok()
                    && leverage_b.is_ok()
                    && funding_a.is_ok()
                    && funding_b.is_ok()
                    && open_orders_a.is_ok()
                    && open_orders_b.is_ok()
                {
                    Ok(())
                } else {
                    Err(format!(
                        "fees={:?}/{:?} leverage={:?}/{:?} funding={:?}/{:?} open_orders={:?}/{:?}",
                        fee_a.err(),
                        fee_b.err(),
                        leverage_a.err(),
                        leverage_b.err(),
                        funding_a.err(),
                        funding_b.err(),
                        open_orders_a.err(),
                        open_orders_b.err(),
                    ))
                }
            }
            _ => Err("connector unavailable".to_string()),
        };
        match result {
            Ok(()) => tracing::info!(
                "[managed] Candidate prewarm ready for {} {}-{} in {}ms",
                pair,
                exchange_a,
                exchange_b,
                now_ms().saturating_sub(started_ms),
            ),
            Err(error) => tracing::warn!(
                "[managed] Candidate prewarm incomplete for {} {}-{}: {}",
                pair,
                exchange_a,
                exchange_b,
                error,
            ),
        }
        CANDIDATE_PREWARM_INFLIGHT
            .get_or_init(DashSet::new)
            .remove(&route_key);
    });
    true
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
}
async fn submit(
    j: &Journal,
    o: &JournalOrder,
    http: &Client,
    keys: &ExchangeKeys,
) -> anyhow::Result<OrderObservation> {
    prepare_submission(j, o).await?;
    dispatch_prepared(j, o, http, keys).await
}

async fn prepare_submission(j: &Journal, o: &JournalOrder) -> anyhow::Result<()> {
    o.request.validate().map_err(anyhow::Error::msg)?;
    j.prepare(o).await?;
    j.execution_event(o.trade_id, &o.request.client_id, "SEND_STARTED", None)
        .await?;
    Ok(())
}

async fn dispatch_prepared(
    j: &Journal,
    o: &JournalOrder,
    http: &Client,
    keys: &ExchangeKeys,
) -> anyhow::Result<OrderObservation> {
    let conn = get_connector(&o.exchange).ok_or_else(|| anyhow::anyhow!("Unknown connector"))?;
    dispatch_using(
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
    prepare_submission(j, o).await?;
    dispatch_using(j, o, transport).await
}

async fn dispatch_using(
    j: &Journal,
    o: &JournalOrder,
    transport: &dyn OrderTransport,
) -> anyhow::Result<OrderObservation> {
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
    if !known.state.terminal() {
        j.execution_event(
            o.trade_id,
            &o.request.client_id,
            "RECONCILIATION_DEFERRED",
            Some("hot path performs no REST lookup or order replay".into()),
        )
        .await?;
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
    for mut s in signals.into_iter().take(3) {
        let signal_received_ms = now_ms();
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
        if !order_ws_ready(&s.exchange_a) || !order_ws_ready(&s.exchange_b) {
            record_entry_decision(ctx, "rejected_order_ws_not_ready");
            continue;
        }
        if [&s.exchange_a, &s.exchange_b]
            .iter()
            .any(|exchange| {
                exchange.as_str() == "kucoin"
                    && !crate::collector::kucoin::pro_depth_is_fresh(&s.pair, now_ms())
            })
        {
            record_entry_decision(ctx, "rejected_kucoin_pro_l2_not_ready");
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
        // The signal-to-order path performs no network preflight. Fees,
        // leverage and funding are prepared before the entry threshold, while
        // account snapshots are refreshed independently. Any missing or stale
        // prerequisite fails closed instead of turning a fleeting edge into a
        // seconds-long REST transaction.
        if !candidate_is_prepared(
            &s.exchange_a,
            &s.exchange_b,
            &s.pair,
            p.leverage as f64,
            ctx,
        ) {
            record_entry_decision(ctx, "rejected_candidate_not_prepared");
            continue;
        }
        if !cached_pair_is_flat(ctx, &s.exchange_a, &s.pair)
            || !cached_pair_is_flat(ctx, &s.exchange_b, &s.pair)
        {
            record_entry_decision(ctx, "rejected_existing_position");
            continue;
        }
        let fee_key_a = super::utils::exchange_pair_key(&s.exchange_a, &s.pair);
        let fee_key_b = super::utils::exchange_pair_key(&s.exchange_b, &s.pair);
        let (fee_a, fee_b) = match (
            ctx.fee_rates.get(&fee_key_a).map(|value| *value),
            ctx.fee_rates.get(&fee_key_b).map(|value| *value),
        ) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                record_entry_decision(ctx, "rejected_fee_cache");
                continue;
            }
        };
        s.break_even_pct = super::scanner_common::ioc_break_even_pct(fee_a, fee_b);
        let (available_a, available_b) = match (
            ctx.margin_balances.get(&s.exchange_a).map(|value| *value),
            ctx.margin_balances.get(&s.exchange_b).map(|value| *value),
        ) {
            (Some(a), Some(b)) if a.is_finite() && b.is_finite() => (a, b),
            _ => {
                record_entry_decision(ctx, "rejected_balance_cache");
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
        let (
            funding_deadline_ms,
            funding_settlement_ms,
            funding_rate_a,
            funding_rate_b,
            live_funding_edge_pct,
        ) = if s.funding_capture {
                let Some((info_a, info_b)) = super::funding_guard::cached_info(
                    ctx,
                    &s.exchange_a,
                    &s.pair,
                )
                .zip(super::funding_guard::cached_info(
                    ctx,
                    &s.exchange_b,
                    &s.pair,
                ))
                else {
                    record_entry_decision(ctx, "rejected_funding_cache");
                    continue;
                };
                let Some(raw_edge) = super::scanner_common::directional_funding_edge_pct(
                    &s.exchange_a,
                    &s.direction,
                    info_a.rate,
                    info_b.rate,
                )
                .filter(|edge| {
                    *edge >= super::scanner_common::FUNDING_CAPTURE_WATCH_EDGE_PCT
                })
                else {
                    record_entry_decision(ctx, "rejected_funding_edge");
                    continue;
                };
                let Some(edge) =
                    super::scanner_common::conservative_funding_credit_pct(raw_edge)
                else {
                    record_entry_decision(ctx, "rejected_funding_edge");
                    continue;
                };
                let Some((settlement, exit)) = super::funding_guard::capture_window(
                    info_a.next_ts,
                    info_b.next_ts,
                    now_ms(),
                    p.max_holding_time_ms,
                ) else {
                    record_entry_decision(ctx, "rejected_funding_capture_window");
                    continue;
                };
                (
                    Some(exit),
                    Some(settlement),
                    Some(info_a.rate),
                    Some(info_b.rate),
                    edge,
                )
            } else {
                let deadline = super::funding_guard::cached_next(ctx, &s.exchange_a, &s.pair)
                    .zip(super::funding_guard::cached_next(ctx, &s.exchange_b, &s.pair))
                    .and_then(|(a, b)| super::funding_guard::deadline(a, b, now_ms()));
                if deadline.is_none() {
                    record_entry_decision(ctx, "rejected_funding_cache");
                    continue;
                }
                (deadline, None, None, None, 0.0)
            };
        s.expected_funding_pct = live_funding_edge_pct;
        if !super::scanner::execution_books_are_fresh(
            ctx,
            &format!("{}:{}", s.exchange_a, s.pair),
            &format!("{}:{}", s.exchange_b, s.pair),
            now_ms(),
        ) {
            record_entry_decision(ctx, "rejected_l2_timestamp_skew");
            continue;
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
        let budget = if s.funding_capture {
            if !gross.is_finite()
                || gross.abs()
                    > p.max_entry_spread
                        .min(super::scanner_common::FUNDING_CAPTURE_MAX_BASIS_PCT)
            {
                record_entry_decision(ctx, "rejected_funding_basis");
                continue;
            }
            let initial_net = super::scanner::initial_l2_liquidation_net_pct(
                ctx,
                &format!("{}:{}", s.exchange_a, s.pair),
                &format!("{}:{}", s.exchange_b, s.pair),
                s.is_a_higher,
                s.target_qty_a,
                fee_a,
                fee_b,
                now_ms(),
            );
            match initial_net.and_then(|initial| {
                super::scanner_common::funding_capture_budget(
                    initial,
                    live_funding_edge_pct,
                    p.take_profit_percent.max(0.05),
                )
            }) {
                Some(value) => value,
                None => {
                    tracing::info!(
                        "[managed] Funding capture rejected {}: immediate_net={:?}% funding={:.4}% required_profit={:.4}%",
                        s.pair,
                        initial_net.map(|value| (value * 10_000.0).round() / 10_000.0),
                        live_funding_edge_pct,
                        p.take_profit_percent.max(0.05),
                    );
                    record_entry_decision(ctx, "rejected_funding_liquidation_budget");
                    continue;
                }
            }
        } else {
            match super::scanner_common::entry_execution_budget(
                gross,
                p.min_entry_spread,
                p.max_entry_spread,
                s.structural_spread,
                p.min_structural_deviation,
                s.break_even_pct,
            ) {
                Some(value) => value,
                None => {
                    let required_floor = p.min_entry_spread.max(
                        s.structural_spread + s.break_even_pct + p.min_structural_deviation,
                    );
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
            }
        };
        // Compare the current executable books with the snapshot that emitted
        // the signal. There is deliberately no fixed sleep or network preflight.
        if !s.funding_capture && entry_spread_is_widening(s.spread, gross) {
            tracing::info!(
                "[managed] Direction confirmation rejected {} after {}ms: executable L2 widened {:.4}% -> {:.4}%",
                s.pair,
                now_ms().saturating_sub(signal_received_ms),
                s.spread,
                gross,
            );
            record_entry_decision(ctx, "rejected_widening_l2");
            continue;
        }
        let half = (budget / 2.0).min(p.slippage_percent);
        if half < 0.0 {
            record_entry_decision(ctx, "rejected_slippage_budget");
            continue;
        }
        // Re-read local authorization immediately before reserving exposure.
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
        let plan = Plan {
            funding_deadline_ms,
            funding_settlement_ms,
            funding_rate_a,
            funding_rate_b,
            signal: s.clone(),
            settings: p.clone(),
        };
        let mut t = ManagedTrade {
            id: 0,
            state: "ENTRY".into(),
            reason: None,
            created_ms: now_ms(),
            plan: plan.clone(),
        };
        // Both IOC limits consume at most half of the total movement budget.
        // Persist both identities first, then poll both dedicated order sockets
        // in the same Tokio turn. The second leg no longer waits for the first
        // exchange's acknowledgement or fill report.
        let mut order_a = request(
            &t,
            "A",
            s.target_qty_a,
            entry_ioc_limit(quote_a, !s.is_a_higher, half),
            false,
        );
        let mut order_b = request(
            &t,
            "B",
            s.target_qty_b,
            entry_ioc_limit(quote_b, s.is_a_higher, half),
            false,
        );
        let id = match j.reserve_and_prepare_entry(plan, &[order_a.clone(), order_b.clone()]).await? {
            EntryReservation::Reserved(id) => id,
            EntryReservation::DailyLossLimit(daily) => {
                kill(ctx,j,format!("Daily VERIFIED loss limit reached for {}: {:.4} <= -{:.4} USDT",p.id,daily,p.daily_loss_limit_usdt)).await;
                return Ok(());
            }
            EntryReservation::PairLossQuarantine(losses) => {
                ctx.pair_cooldown.insert(s.pair.clone(), now_ms());
                tracing::info!("[managed] Pair loss quarantine rejected {}: {} VERIFIED losses in the last 7d",s.pair,losses);
                record_entry_decision(ctx, "rejected_pair_loss_quarantine");
                continue;
            }
            EntryReservation::Rejected => {
                record_entry_decision(ctx, "rejected_reservation");
                continue;
            }
        };
        t.id=id; order_a.trade_id=id; order_b.trade_id=id;
        record_entry_decision(ctx, "trade_reserved_and_prepared");
        let _guard = claim(ctx, id).ok_or_else(|| anyhow::anyhow!("Trade already being supervised"))?;
        let dispatch_clock = std::time::Instant::now();
        let (fill_a, fill_b) = tokio::join!(
            async {
                let offset_ns = dispatch_clock.elapsed().as_nanos();
                (offset_ns, dispatch_prepared(j, &order_a, http, keys).await)
            },
            async {
                let offset_ns = dispatch_clock.elapsed().as_nanos();
                (offset_ns, dispatch_prepared(j, &order_b, http, keys).await)
            },
        );
        let dispatch_gap_ns = fill_a.0.abs_diff(fill_b.0);
        tracing::info!(
            "[managed] Dual WS IOC tasks started for {} {}ms after signal; local task-start gap={}ns",
            s.pair,
            now_ms().saturating_sub(signal_received_ms),
            dispatch_gap_ns,
        );
        let fill_a = fill_a.1?;
        let fill_b = fill_b.1?;
        let orders = j.orders(id).await?;
        if !fill_a.state.terminal() || !fill_b.state.terminal() {
            j.state(id, "RECONCILING", "DUAL_ENTRY_UNKNOWN").await?;
            return Ok(());
        }
        let qty_a = fill_a.filled_quantity;
        let qty_b = fill_b.filled_quantity;
        if qty_a <= 1e-12 && qty_b <= 1e-12 {
            j.state(id, "CLOSING", "ENTRY_UNFILLED").await?;
        } else if (qty_a - qty_b).abs() > qty_a.max(qty_b) * 1e-8 {
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
        loop {
            let cycle_started = std::time::Instant::now();
            ctx.supervisor_tick_ms.store(now_ms(), Ordering::Release);
            let mut has_active_trades = false;
            match j.active().await {
                Ok(trades) => {
                    has_active_trades = !trades.is_empty();
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
            // Idle polling stays cheap, while an owned position is evaluated at
            // the cadence of KuCoin's validated increment@10ms execution book.
            // The delay is measured from cycle start so DB/valuation work does
            // not accumulate on top of the intended reaction interval.
            let cadence = if has_active_trades {
                std::time::Duration::from_millis(10)
            } else {
                std::time::Duration::from_millis(100)
            };
            tokio::time::sleep(cadence.saturating_sub(cycle_started.elapsed())).await;
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
    let synchronized_books = super::scanner::execution_books_are_fresh(
        ctx,
        &format!("{}:{}", s.exchange_a, s.pair),
        &format!("{}:{}", s.exchange_b, s.pair),
        now_ms(),
    );
    let (pxa, pxb) = if synchronized_books {
        (quote_a.map(|q| q.vwap), quote_b.map(|q| q.vwap))
    } else {
        (None, None)
    };
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
        let funding_credit_pct = if s.funding_capture
            && t.plan
                .funding_settlement_ms
                .is_some_and(|settlement| now_ms() >= settlement)
        {
            s.expected_funding_pct
        } else {
            0.0
        };
        let percent = (pnl - entry_fees - reserve) / a.entry_value.max(1e-12) * 100.0
            + funding_credit_pct;
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
        if s.funding_capture {
            "FUNDING_CAPTURE_COMPLETE"
        } else {
            "FUNDING_BOUNDARY"
        }
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
    let mut close_orders = Vec::with_capacity(2);
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
        close_orders.push(request(&t, leg, qty, limit, true));
    }
    // Both requests are reduce-only and independently bounded. Dispatch them
    // concurrently so a profitable exit is not exposed to the first venue's
    // acknowledgement latency before the second request even leaves the VPS.
    let close_results = futures_util::future::join_all(
        close_orders.iter().map(|order| submit(j, order, http, keys)),
    )
    .await;
    for result in close_results {
        let _ = result?;
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

    #[test]
    fn hot_path_requires_a_fresh_open_order_clear_verdict() {
        let ctx = EngineContext::default();
        assert!(!open_orders_are_clear(&ctx, "binance", "BTCUSDT", 10_000));

        ctx.open_orders_clear_ms
            .insert("binance:BTCUSDT".into(), 5_000);
        assert!(open_orders_are_clear(&ctx, "binance", "BTCUSDT", 10_000));
        assert!(!open_orders_are_clear(&ctx, "binance", "BTCUSDT", 10_001));
        assert!(!open_orders_are_clear(&ctx, "binance", "BTCUSDT", 4_999));
    }

    #[test]
    fn risk_exits_quarantine_the_pair_longer_than_normal_exits() {
        assert_eq!(pair_cooldown_after_close("TP"), 60_000);
        assert_eq!(pair_cooldown_after_close("MANUAL_CLOSE"), 60_000);
        assert_eq!(pair_cooldown_after_close("FUNDING_CAPTURE_COMPLETE"), 60_000);
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
    fn candidate_depth_prewarm_starts_before_execution_freshness_expires() {
        assert!(!execution_depth_needs_prewarm(Some(9_500), 10_000));
        assert!(execution_depth_needs_prewarm(Some(9_499), 10_000));
        assert!(execution_depth_needs_prewarm(None, 10_000));
        assert!(execution_depth_needs_prewarm(Some(11_001), 10_000));
    }

    #[test]
    fn binance_candidate_depth_lane_is_fast_but_rate_bounded() {
        const DEPTH_LIMIT_FIVE_WEIGHT: i64 = 2;
        let requests_per_minute = 60_000 / BINANCE_DEPTH_PREWARM_INTERVAL_MS;
        assert!(
            requests_per_minute * DEPTH_LIMIT_FIVE_WEIGHT <= 600,
            "candidate recovery must preserve REST headroom"
        );
        assert!(
            BINANCE_DEPTH_PREWARM_INTERVAL_MS * 3 < 750,
            "three qualified candidates must fit inside the execution freshness window"
        );
    }

    #[test]
    fn kucoin_stale_depth_error_preserves_rejected_book_age() {
        assert_eq!(
            kucoin_stale_depth_age_ms("Stale KuCoin public depth snapshot: 2431ms"),
            Some(2_431),
        );
        assert_eq!(kucoin_stale_depth_age_ms("request failed"), None);
    }

    #[test]
    fn kucoin_depth_retry_uses_stale_age_without_starving_other_pairs() {
        assert_eq!(kucoin_depth_prewarm_retry_ms(None), 5_000);
        assert_eq!(kucoin_depth_prewarm_retry_ms(Some(2_431)), 5_000);
        assert_eq!(kucoin_depth_prewarm_retry_ms(Some(8_000)), 8_000);
        assert_eq!(kucoin_depth_prewarm_retry_ms(Some(90_000)), 30_000);
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

    struct LostAck {
        submits: std::sync::atomic::AtomicU32,
    }
    #[async_trait::async_trait]
    impl OrderTransport for LostAck {
        async fn submit(&self, _: &OrderRequest) -> Result<OrderObservation, String> {
            self.submits.fetch_add(1, Ordering::Relaxed);
            Err("Connection lost after exchange accepted order".into())
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
    async fn lost_ack_is_deferred_without_replay_or_hot_path_query() {
        let j = journal().await;
        let o = order();
        let fake = LostAck {
            submits: std::sync::atomic::AtomicU32::new(0),
        };
        let observed = submit_using(&j, &o, &fake).await.unwrap();
        assert_eq!(observed.filled_quantity, 0.0);
        assert_eq!(observed.state, OrderState::Unknown);
        assert_eq!(fake.submits.load(Ordering::Relaxed), 1);
        // Duplicate invocation after a restart fails at the durable identity BEFORE transport.
        assert!(submit_using(&j, &o, &fake).await.is_err());
        assert_eq!(fake.submits.load(Ordering::Relaxed), 1);
        assert_eq!(
            j.orders(42).await.unwrap()[0].observation.filled_quantity,
            0.0
        );
    }
    #[tokio::test]
    async fn timeout_is_persisted_as_unknown_never_zero_fill_success() {
        let j = journal().await;
        let fake = LostAck {
            submits: std::sync::atomic::AtomicU32::new(0),
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
