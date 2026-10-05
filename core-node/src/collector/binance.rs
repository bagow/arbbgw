//! Binance USDT Futures discovery plus bounded executable-liquidity streams.
//! All-book-ticker finds candidates; bounded top-5 subscriptions verify them.
use anyhow::{anyhow, Context, Result};
use futures_util::{stream::FuturesUnordered, SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio_tungstenite::{client_async_tls, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{L2OrderbookMap, L2Snapshot, PriceMap, TickerData, UpdateSignal};

// The stable health probe now follows only BTCUSDT. At that bounded one-symbol
// volume, use Binance's fastest documented partial-depth cadence so the observed
// 250-350 ms route latency still leaves headroom inside the 750 ms freshness gate.
const DEPTH_STREAMS_PER_CONNECTION: usize = 5;
const DEPTH_UPDATE_INTERVAL_MS: u64 = 100;
// Once a route enters the executable candidate set, promote it to Binance's
// fastest documented partial-depth cadence. The bounded health probe uses the
// same cadence; unlike the old all-market stream it carries only one symbol.
const DYNAMIC_DEPTH_UPDATE_INTERVAL_MS: u64 = 100;
const DEPTH_SUBSCRIPTION_REFRESH_SECS: u64 = 1_800;
// Zurich-to-Binance routes can begin near 300 ms and drift past the 750 ms
// execution gate after roughly 35-45 seconds. Rotate independent lanes before
// that point; the stagger keeps at least one admitted socket publishing while
// another performs DNS/TCP/TLS/event-quality selection.
const BASELINE_DEPTH_SESSION_SECS: u64 = 33;
const DYNAMIC_DEPTH_SESSION_SECS: u64 = 25;
const SECOND_DYNAMIC_START_DELAY_SECS: u64 = 12;
// Poll faster than the engine's five-millisecond floor so subscription hand-off
// adds no extra scan cycle.
// This does not send extra exchange traffic: messages are emitted only when
// the deduplicated symbol set actually changes.
const DYNAMIC_DEPTH_REFRESH_MS: u64 = 2;
const DYNAMIC_DEPTH_RETENTION_SECS: u64 = 30;
const MAX_BASELINE_DEPTH_BACKLOG_MS: i64 = 5_000;
const MAX_BASELINE_CONSECUTIVE_STALE_DEPTH: u32 = 10;
// A stale frame is always dropped by apply_depth_update's 750 ms execution
// gate. Reconnect only for a sustained backlog or a catastrophic one-off lag;
// restarting on a short burst of delayed frames removes every candidate book
// and creates a larger blind window than the rejected frames. The dynamic
// connection multiplexes many symbols, so a frame count does not measure time:
// one delayed update round can contain more messages than the old reconnect
// threshold. Require a continuous wall-clock interval without any accepted
// depth instead. Stale frames still fail closed at the 750 ms execution gate.
const MAX_DYNAMIC_DEPTH_BACKLOG_MS: i64 = 10_000;
const MAX_DYNAMIC_STALE_DURATION_MS: u64 = 3_000;
const BASELINE_HEALTH_REFRESH_AGE_MS: i64 = 250;
// Binance documents the all-book websocket with a five-second update speed.
// Refresh the current all-symbol best book once per second so a valid confirmed
// edge is not hidden behind that discovery cadence. The endpoint costs weight
// 5, i.e. 300/minute here, and all
// Binance REST callers still share the 2,200/minute hard stop below the
// documented 2,400/minute limit.
const DISCOVERY_REST_REFRESH_SECS: u64 = 1;
// 24 ranked candidates plus the profile-wide maximum of 10 owned trades.
const MAX_DYNAMIC_DEPTH_SYMBOLS: usize = 40;
// Binance's documented USD-M market-stream endpoints. The `/public/*` relay
// accepts connections but has been observed delivering ever older events.
const BINANCE_RAW_WS_URL: &str = "wss://fstream.binance.com/ws";
const BINANCE_COMBINED_WS_URL: &str = "wss://fstream.binance.com/stream";
const MAX_MARKET_ROUTE_ATTEMPTS: usize = 4;
const MAX_INITIAL_EVENT_LAG_MS: i64 = 750;
const DEPTH_ROUTE_PROBE_EVENTS: usize = 20;
const MAX_DEPTH_ROUTE_LAG_DRIFT_MS: i64 = 250;
static MARKET_ROUTE_OFFSET: AtomicUsize = AtomicUsize::new(0);
static HEALTH_REST_FALLBACK_SUCCESS: AtomicU64 = AtomicU64::new(0);
static HEALTH_REST_FALLBACK_FAILURE: AtomicU64 = AtomicU64::new(0);
static TICKER_SOURCE_MS: std::sync::OnceLock<dashmap::DashMap<String, i64>> =
    std::sync::OnceLock::new();

fn ticker_source_map() -> &'static dashmap::DashMap<String, i64> {
    TICKER_SOURCE_MS.get_or_init(dashmap::DashMap::new)
}

pub(crate) fn ticker_source_ms(pair: &str) -> Option<i64> {
    ticker_source_map().get(pair).map(|stamp| *stamp)
}

pub(crate) fn record_ticker_source_ms(pair: &str, source_ms: i64) {
    if source_ms <= 0 {
        return;
    }
    ticker_source_map()
        .entry(pair.to_string())
        .and_modify(|current| *current = (*current).max(source_ms))
        .or_insert(source_ms);
}

/// Publish an L2 top only if it cannot rewind an already newer discovery tick.
/// Called with the venue price lock held by both WS and REST depth publishers.
pub(crate) fn publish_depth_ticker(
    venue: &mut std::collections::HashMap<String, TickerData>,
    pair: &str,
    bid: (f64, f64),
    ask: (f64, f64),
    source_ms: i64,
) {
    if source_ms <= 0 || ticker_source_ms(pair).is_some_and(|current| current > source_ms) {
        return;
    }
    if let Some(ticker) = venue.get_mut(pair) {
        ticker.bid = bid.0;
        ticker.ask = ask.0;
        ticker.price = (bid.0 + ask.0) / 2.0;
        ticker.bid_size = bid.0 * bid.1;
        ticker.ask_size = ask.0 * ask.1;
        ticker.last_updated = (source_ms / 1_000) as u64;
        record_ticker_source_ms(pair, source_ms);
    }
}

pub fn health_rest_fallback_metrics() -> (u64, u64) {
    (
        HEALTH_REST_FALLBACK_SUCCESS.load(Ordering::Relaxed),
        HEALTH_REST_FALLBACK_FAILURE.load(Ordering::Relaxed),
    )
}

type MarketWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn event_timestamp_ms(value: &Value) -> Option<i64> {
    value
        .get("E")
        .and_then(Value::as_i64)
        .or_else(|| value.get("data").and_then(event_timestamp_ms))
        .or_else(|| {
            value
                .as_array()
                .and_then(|items| items.iter().find_map(event_timestamp_ms))
        })
}

fn event_lag_ms(text: &str, received_ms: i64) -> Option<i64> {
    let value: Value = serde_json::from_str(text).ok()?;
    event_timestamp_ms(&value).map(|event_ms| received_ms - event_ms)
}

async fn connect_market_route(url: String, address: SocketAddr) -> Result<(MarketWs, String, i64)> {
    let probe_depth_stability = url.contains("@depth");
    let socket = tokio::time::timeout(
        Duration::from_secs(4),
        tokio::net::TcpStream::connect(address),
    )
    .await
    .map_err(|_| anyhow!("TCP timeout via {address}"))?
    .with_context(|| format!("TCP failure via {address}"))?;
    socket
        .set_nodelay(true)
        .with_context(|| format!("TCP_NODELAY failed via {address}"))?;
    let (mut ws, _) = tokio::time::timeout(Duration::from_secs(6), client_async_tls(url, socket))
        .await
        .map_err(|_| anyhow!("TLS/WebSocket timeout via {address}"))?
        .with_context(|| format!("TLS/WebSocket failure via {address}"))?;

    // From Zurich, a fast TLS handshake can still lead to a seconds-old event
    // queue. Select on the first exchange-stamped event that is actually usable.
    let first = tokio::time::timeout(Duration::from_secs(7), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let text = text.to_string();
                    if let Some(lag_ms) = event_lag_ms(&text, crate::engine::utils::now_ms()) {
                        return Ok((text, lag_ms));
                    }
                }
                Some(Ok(Message::Ping(payload))) => ws
                    .send(Message::Pong(payload))
                    .await
                    .with_context(|| format!("Pong failed via {address}"))?,
                Some(Ok(Message::Close(frame))) => {
                    anyhow::bail!("closed before first event via {address}: {frame:?}")
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(anyhow!("receive failure via {address}: {error}")),
                None => anyhow::bail!("ended before first event via {address}"),
            }
        }
    })
    .await
    .map_err(|_| anyhow!("first event timeout via {address}"))??;
    if !(0..=MAX_INITIAL_EVENT_LAG_MS).contains(&first.1) {
        anyhow::bail!("stale first event via {address}: lag_ms={}", first.1);
    }
    if !probe_depth_stability {
        return Ok((ws, first.0, first.1));
    }

    // The problematic Zurich routes often look fresh once and then accumulate
    // a queue immediately. Require a short stable series before admitting a
    // critical depth socket into the candidate path.
    let stable = tokio::time::timeout(Duration::from_secs(4), async {
        let mut latest = first;
        let mut min_lag = latest.1;
        let mut accepted = 1usize;
        while accepted < DEPTH_ROUTE_PROBE_EVENTS {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let text = text.to_string();
                    let Some(lag_ms) = event_lag_ms(&text, crate::engine::utils::now_ms()) else { continue; };
                    min_lag = min_lag.min(lag_ms);
                    if !(0..=MAX_INITIAL_EVENT_LAG_MS).contains(&lag_ms)
                        || lag_ms > min_lag + MAX_DEPTH_ROUTE_LAG_DRIFT_MS {
                        anyhow::bail!("unstable depth route via {address}: lag_ms={lag_ms} min_lag_ms={min_lag}");
                    }
                    latest = (text, lag_ms);
                    accepted += 1;
                }
                Some(Ok(Message::Ping(payload))) => ws.send(Message::Pong(payload)).await
                    .with_context(|| format!("probe Pong failed via {address}"))?,
                Some(Ok(Message::Close(frame))) => anyhow::bail!("closed during depth probe via {address}: {frame:?}"),
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(anyhow!("depth probe failed via {address}: {error}")),
                None => anyhow::bail!("ended during depth probe via {address}"),
            }
        }
        Ok(latest)
    }).await.map_err(|_| anyhow!("depth stability probe timed out via {address}"))??;
    Ok((ws, stable.0, stable.1))
}

async fn connect_market_ws(url: &str) -> Result<(MarketWs, String)> {
    const HOST: &str = "fstream.binance.com";
    let addresses =
        tokio::time::timeout(Duration::from_secs(3), tokio::net::lookup_host((HOST, 443)))
            .await
            .map_err(|_| anyhow!("Binance market WS DNS lookup timed out"))?
            .context("Binance market WS DNS lookup failed")?;

    // Oracle-to-Binance routing is asymmetric: individual DNS targets can
    // black-hole TCP or TLS while their siblings remain healthy. Race complete
    // TLS/WebSocket handshakes so one bad address cannot consume the caller's
    // entire reconnect budget. The original hostname stays in `url`, preserving
    // the correct TLS SNI and HTTP Host header.
    let mut addresses: Vec<_> = addresses.collect();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() {
        anyhow::bail!("Binance market WS DNS returned no addresses");
    }
    let address_count = addresses.len();
    let attempt_count = address_count.min(MAX_MARKET_ROUTE_ATTEMPTS);
    let offset = MARKET_ROUTE_OFFSET.fetch_add(attempt_count, Ordering::Relaxed) % address_count;
    let mut attempts = FuturesUnordered::new();
    for index in 0..attempt_count {
        let address = addresses[(offset + index) % address_count];
        let request_url = url.to_string();
        attempts.push(async move {
            connect_market_route(request_url, address)
                .await
                .map(|result| (result, address))
        });
    }

    let mut failures = Vec::new();
    while let Some(attempt) = attempts.next().await {
        match attempt {
            Ok(((ws, first_text, lag_ms), address)) => {
                info!(
                    "[binance_market_ws] selected route via {} first_event_lag_ms={}",
                    address, lag_ms
                );
                return Ok((ws, first_text));
            }
            Err(error) => failures.push(error.to_string()),
        }
    }
    Err(anyhow!(
        "all Binance market WS addresses failed: {}",
        failures.join("; ")
    ))
}

// The baseline exists only as a stable transport-health probe. Discovery uses the
// all-book ticker and every executable/owned symbol is followed by the dynamic
// stream below. One liquid symbol is sufficient for health and avoids the VPS
// receive backlog observed even on a five-symbol combined stream.
const FALLBACK_DEPTH_SYMBOLS: &[&str] = &["BTCUSDT"];

pub async fn run(
    prices: PriceMap,
    signal: UpdateSignal,
    l2_books: L2OrderbookMap,
    l2_wanted: std::sync::Arc<dashmap::DashSet<String>>,
    settings: std::sync::Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    // Track whether L2 depth stream is already spawned to avoid duplicates
    let mut children = tokio::task::JoinSet::new();
    let discovery_ready = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut backoff = Duration::from_secs(1);
    loop {
        let is_disabled = {
            let p = settings.read().await;
            !p.is_empty()
                && p.iter()
                    .all(|x| x.disabled_exchanges.contains(&"binance".to_string()))
        };
        if is_disabled {
            children.abort_all();
            while children.join_next().await.is_some() {}
            if backoff == Duration::from_secs(1) {
                info!("[binance] Disabled globally in profiles. Sleeping...");
            }
            backoff = Duration::from_secs(30);
            tokio::time::sleep(backoff).await;
            continue;
        }

        // Pre-flight ban check: use connector's centralized ban state.
        // CRITICAL: Do NOT send ANY HTTP request while banned — it extends the ban!
        if crate::engine::exchanges::binance::is_binance_banned() {
            let remaining = crate::engine::exchanges::binance::binance_ban_remaining_secs();
            error!(
                "[binance] IP still banned ({}s remaining). NO requests sent. Waiting...",
                remaining
            );
            // Wait ban duration + 30s safety margin, capped at 15 min
            let wait = (remaining as u64 + 30).min(900);
            tokio::time::sleep(Duration::from_secs(wait)).await;
            continue;
        }

        // Ban is cleared — do a lightweight ping to confirm connectivity
        let http_check = Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .build()
            .unwrap();
        match http_check
            .get("https://fapi.binance.com/fapi/v1/ping")
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let headers = resp.headers().clone();
                // Record response for weight tracking + ban detection
                crate::engine::exchanges::binance::record_binance_response(status, &headers);
                if status == 418 || status == 429 || status == 403 {
                    error!(
                        "[binance] Ping returned HTTP {}. Ban recorded by guard.",
                        status
                    );
                    // Guard already set the ban duration — just loop back
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
            }
            Err(e) => {
                error!("[binance] Ping failed: {}. Backing off 60s.", e);
                tokio::time::sleep(Duration::from_secs(60)).await;
                continue;
            }
        }

        info!("[binance] Connecting (ban check passed)...");

        // Spawn L2 depth stream ONCE (not on every reconnect)
        while children.try_join_next().is_some() {}
        if children.is_empty() {
            let l2_books_for_depth = l2_books.clone();
            let prices_for_depth = prices.clone();
            let l2_wanted_for_dynamic = l2_wanted.clone();
            let signal_for_depth = signal.clone();
            let signal_for_dynamic = signal.clone();
            let signal_for_health = signal.clone();
            let signal_for_discovery = signal.clone();
            let discovery_for_depth = discovery_ready.clone();
            let settings_for_health = settings.clone();
            let settings_for_discovery = settings.clone();
            children.spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                tokio::join!(
                    run_l2_depth_stream(
                        l2_books_for_depth.clone(),
                        prices_for_depth.clone(),
                        signal_for_depth,
                        discovery_for_depth,
                    ),
                    run_dynamic_l2_depth_stream(
                        l2_books_for_depth.clone(),
                        prices_for_depth.clone(),
                        l2_wanted_for_dynamic.clone(),
                        signal_for_dynamic.clone(),
                    ),
                    async {
                        tokio::time::sleep(Duration::from_secs(SECOND_DYNAMIC_START_DELAY_SECS))
                            .await;
                        run_dynamic_l2_depth_stream(
                            l2_books_for_depth.clone(),
                            prices_for_depth.clone(),
                            l2_wanted_for_dynamic,
                            signal_for_dynamic,
                        )
                        .await;
                    },
                    run_l2_health_fallback(
                        l2_books_for_depth.clone(),
                        prices_for_depth.clone(),
                        signal_for_health,
                        settings_for_health,
                    ),
                    run_l1_discovery_fallback(
                        l2_books_for_depth.clone(),
                        prices_for_depth.clone(),
                        signal_for_discovery,
                        settings_for_discovery,
                    )
                );
            });
        }

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"binance".to_string()))
                };
                if is_disabled {
                    break;
                }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal, &discovery_ready, &l2_books) => {
                match res {
                    Ok(_)  => { backoff = Duration::from_secs(1); }
                    Err(e) => warn!("[binance] WS disconnected: {}. Reconnecting in {:?}", e, backoff),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(120));
            }
            _ = disable_watcher => {
                info!("[binance] Exchange was disabled dynamically. Dropping connection.");
                backoff = Duration::from_secs(1);
            }
        }
    }
}

async fn fetch_all_book_tickers(http: &Client) -> Result<Vec<BinanceBookTicker>, String> {
    crate::engine::exchanges::binance::check_before_binance_rest()?;
    let response = http
        .get("https://fapi.binance.com/fapi/v1/ticker/bookTicker")
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .map_err(|error| format!("Binance book-ticker request failed: {error}"))?;
    let status = response.status();
    let headers = response.headers().clone();
    crate::engine::exchanges::binance::record_binance_response(status.as_u16(), &headers);
    if !status.is_success() {
        return Err(format!(
            "Binance book-ticker returned HTTP {}",
            status.as_u16()
        ));
    }
    response
        .json::<Vec<BinanceBookTicker>>()
        .await
        .map_err(|error| format!("Invalid Binance book-ticker response: {error}"))
}

/// The VPS WebSocket route can leave most of `!bookTicker` stale while keeping
/// the socket open. Poll the low-weight all-symbol best-book snapshot only for
/// active profiles. Candidate depth and the final IOC quote remain independently
/// verified by the dynamic stream and just-in-time depth endpoint.
async fn run_l1_discovery_fallback(
    l2_books: L2OrderbookMap,
    price_map: crate::state::PriceMap,
    signal: UpdateSignal,
    settings: std::sync::Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    let http = Client::builder()
        .user_agent("cryptoarb-binance-discovery/1")
        .build()
        .expect("Binance discovery client");
    let mut timer = tokio::time::interval(Duration::from_secs(DISCOVERY_REST_REFRESH_SECS));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_warning = std::time::Instant::now();
    loop {
        timer.tick().await;
        let active = settings.read().await.iter().any(|profile| {
            profile.is_active
                && !profile
                    .disabled_exchanges
                    .iter()
                    .any(|exchange| exchange == "binance")
        });
        if !active {
            continue;
        }
        match fetch_all_book_tickers(&http).await {
            Ok(tickers) => {
                let received_ms = crate::engine::utils::now_ms();
                let mut updated = 0usize;
                let mut venue = price_map.entry("binance".to_string()).or_default();
                for ticker in tickers {
                    updated += usize::from(apply_book_ticker(
                        &mut venue,
                        ticker,
                        received_ms,
                        Some(&l2_books),
                    ));
                }
                drop(venue);
                if updated > 0 {
                    signal.notify_one();
                } else if last_warning.elapsed() >= Duration::from_secs(30) {
                    warn!("[binance_l1_rest] Snapshot contained no fresh valid tickers");
                    last_warning = std::time::Instant::now();
                }
            }
            Err(error) => {
                if last_warning.elapsed() >= Duration::from_secs(30) {
                    warn!("[binance_l1_rest] Refresh failed: {}", error);
                    last_warning = std::time::Instant::now();
                }
            }
        }
    }
}

fn baseline_health_refresh_due(active: bool, updated_ms: Option<i64>, now_ms: i64) -> bool {
    active
        && updated_ms
            .map(|updated| now_ms < updated || now_ms - updated > BASELINE_HEALTH_REFRESH_AGE_MS)
            .unwrap_or(true)
}

/// Keep the Binance transport-health probe current when this VPS's WebSocket
/// route delivers old batches. The independent request/response WebSocket probe
/// runs only while a profile is active and accepts the same <=750 ms exchange
/// timestamp as an executable snapshot.
async fn run_l2_health_fallback(
    l2_books: L2OrderbookMap,
    price_map: crate::state::PriceMap,
    signal: UpdateSignal,
    settings: std::sync::Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    let key = "binance:BTCUSDT";
    let http = Client::builder()
        .user_agent("cryptoarb-binance-depth-fallback/1")
        .build()
        .expect("Binance depth fallback client");
    let mut last_warning = std::time::Instant::now();
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let active = settings.read().await.iter().any(|profile| {
            profile.is_active
                && !profile
                    .disabled_exchanges
                    .iter()
                    .any(|exchange| exchange == "binance")
        });
        let now = crate::engine::utils::now_ms();
        let updated_ms = l2_books.get(key).map(|book| book.updated_ms);
        if !baseline_health_refresh_due(active, updated_ms, now) {
            continue;
        }
        match crate::engine::exchanges::binance::fetch_public_depth_snapshot_resilient(
            &http, "BTCUSDT",
        )
        .await
        {
            Ok((book, transport)) => {
                if transport
                    == crate::engine::exchanges::binance::PublicDepthTransport::RestFallback
                {
                    HEALTH_REST_FALLBACK_SUCCESS.fetch_add(1, Ordering::Relaxed);
                }
                if l2_books
                    .get(key)
                    .map(|existing| existing.updated_ms >= book.updated_ms)
                    .unwrap_or(false)
                {
                    continue;
                }
                if let (Some(bid), Some(ask), Some(mut venue)) = (
                    book.bids.first(),
                    book.asks.first(),
                    price_map.get_mut("binance"),
                ) {
                    if let Some(ticker) = venue.get_mut("BTCUSDT") {
                        ticker.bid = bid.0;
                        ticker.ask = ask.0;
                        ticker.price = (bid.0 + ask.0) / 2.0;
                        ticker.bid_size = bid.0 * bid.1;
                        ticker.ask_size = ask.0 * ask.1;
                        ticker.last_updated = (book.updated_ms / 1000) as u64;
                    }
                }
                l2_books.insert(key.to_string(), book);
                signal.notify_one();
            }
            Err(error) => {
                HEALTH_REST_FALLBACK_FAILURE.fetch_add(1, Ordering::Relaxed);
                if last_warning.elapsed() >= Duration::from_secs(30) {
                    warn!("[binance_l2_health] WS and REST refresh failed: {}", error);
                    last_warning = std::time::Instant::now();
                }
            }
        }
    }
}

async fn handler(
    price_map: &PriceMap,
    signal: &UpdateSignal,
    discovery_ready: &std::sync::atomic::AtomicBool,
    l2_books: &L2OrderbookMap,
) -> Result<()> {
    // Seed 24h volume from REST miniTicker snapshot
    // Check ban before REST call (weight=40 for ticker/24hr!)
    if crate::engine::exchanges::binance::check_before_binance_rest().is_ok() {
        let http = Client::builder().user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36").build().unwrap();
        let mut target_url = "https://fapi.binance.com/fapi/v1/ticker/24hr".to_string();
        let mut resp = http
            .get(&target_url)
            .timeout(Duration::from_secs(15))
            .send()
            .await;

        if let Ok(ref r) = resp {
            let status = r.status().as_u16();
            crate::engine::exchanges::binance::record_binance_response(status, r.headers());
            if status == 403 || status == 418 || status == 429 {
                target_url = "https://api.codetabs.com/v1/proxy/?quest=https://fapi.binance.com/fapi/v1/ticker/24hr".to_string();
                tracing::warn!(
                    "[binance] Using proxy fallback for volume due to HTTP {}",
                    status
                );
                resp = http
                    .get(&target_url)
                    .timeout(Duration::from_secs(15))
                    .send()
                    .await;
            }
        }

        if let Ok(r) = resp {
            if let Ok(arr) = r.json::<Vec<Value>>().await {
                for t in &arr {
                    let sym = t["symbol"].as_str().unwrap_or("");
                    if !sym.ends_with("USDT") {
                        continue;
                    }
                    let vol: f64 = t["quoteVolume"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    if vol > 0.0 {
                        let mut entry = price_map.entry("binance".to_string()).or_default();
                        entry
                            .entry(sym.to_string())
                            .or_insert_with(|| TickerData {
                                vol,
                                ..Default::default()
                            })
                            .vol = vol;
                    }
                }
                discovery_ready.store(
                    price_map
                        .get("binance")
                        .map(|m| !m.is_empty())
                        .unwrap_or(false),
                    std::sync::atomic::Ordering::Release,
                );
                info!("[binance] Seeded volume for {} symbols", arr.len());
            }
        } else {
            warn!("[binance] Failed to fetch volume from REST/Proxy.");
        }
    } else {
        warn!("[binance] Skipping volume seed — IP banned.");
    }

    // The all-market book ticker is intentionally discovery-only. Binance documents
    // this aggregate stream with a five-second update speed, and execution needs full
    // quantity depth rather than a top quote. Fresh candidate depth is handled by the
    // bounded streams below and remains mandatory before an order can be sent.
    let (mut ws, first_text) = connect_market_ws(&format!("{BINANCE_RAW_WS_URL}/!bookTicker"))
        .await
        .context("Binance all-book-ticker connect failed")?;
    info!("[binance] Connected (all bookTicker discovery)");
    price_map.entry("binance".to_string()).or_default();
    let connected_at = std::time::Instant::now();

    let mut buffered = Some(Message::Text(first_text.into()));
    loop {
        let message = if let Some(message) = buffered.take() {
            message
        } else {
            match tokio::time::timeout(Duration::from_secs(30), ws.next()).await {
                Ok(Some(message)) => message?,
                _ => break,
            }
        };
        if connected_at.elapsed() > Duration::from_secs(300) {
            break;
        }
        match message {
            Message::Text(text) => {
                let now_ms = crate::engine::utils::now_ms();
                if let Ok(payload) = serde_json::from_str::<BookTickerPayload>(&text) {
                    let tickers = match payload {
                        BookTickerPayload::One(ticker) => vec![ticker],
                        BookTickerPayload::Many(tickers) => tickers,
                    };
                    let mut updated = false;
                    if let Some(mut venue) = price_map.get_mut("binance") {
                        for ticker in tickers {
                            if apply_book_ticker(&mut venue, ticker, now_ms, Some(l2_books)) {
                                updated = true;
                            }
                        }
                    }
                    if updated {
                        signal.notify_one();
                    }
                }
            }
            Message::Close(frame) => {
                warn!("[binance] All-book-ticker closed: {:?}", frame);
                break;
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct BinanceBookTicker {
    #[serde(rename = "E", alias = "time", default)]
    event_ms: i64,
    #[serde(rename = "s", alias = "symbol")]
    symbol: String,
    #[serde(rename = "b", alias = "bidPrice")]
    bid: String,
    #[serde(rename = "a", alias = "askPrice")]
    ask: String,
    #[serde(rename = "B", alias = "bidQty")]
    bid_quantity: String,
    #[serde(rename = "A", alias = "askQty")]
    ask_quantity: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BookTickerPayload {
    One(BinanceBookTicker),
    Many(Vec<BinanceBookTicker>),
}

fn apply_book_ticker(
    venue: &mut std::collections::HashMap<String, TickerData>,
    ticker: BinanceBookTicker,
    received_ms: i64,
    l2_books: Option<&L2OrderbookMap>,
) -> bool {
    if !ticker.symbol.ends_with("USDT") {
        return false;
    }
    // While a fresh execution-depth stream owns a candidate, its top of book
    // is authoritative for discovery too. The all-symbol REST/WS ticker can be
    // internally older despite a later response/event timestamp; allowing it
    // to overwrite depth creates a false signal that disappears at JIT refresh.
    // An old cache entry is not ownership: after a subscription gap it must
    // not suppress discovery indefinitely. This only restores L1; stale L2
    // remains stale and still cannot authorize execution.
    if l2_books.is_some_and(|books| {
        books
            .get(&format!("binance:{}", ticker.symbol))
            .is_some_and(|book| (0..=750).contains(&(received_ms - book.updated_ms)))
    }) {
        return false;
    }
    let bid = match ticker.bid.parse::<f64>() {
        Ok(value) if value.is_finite() && value > 0.0 => value,
        _ => return false,
    };
    let ask = match ticker.ask.parse::<f64>() {
        Ok(value) if value.is_finite() && value > bid => value,
        _ => return false,
    };
    let bid_quantity = ticker.bid_quantity.parse::<f64>().unwrap_or(0.0);
    let ask_quantity = ticker.ask_quantity.parse::<f64>().unwrap_or(0.0);
    if !bid_quantity.is_finite()
        || !ask_quantity.is_finite()
        || bid_quantity < 0.0
        || ask_quantity < 0.0
    {
        return false;
    }
    let event_ms = if ticker.event_ms > 0 {
        ticker.event_ms
    } else {
        received_ms
    };
    if !(0..=10_000).contains(&(received_ms - event_ms)) {
        return false;
    }
    // Second-precision ordering allowed an older WS/REST quote from the same
    // second to overwrite a newer one. Preserve exact exchange ordering so a
    // transient cross-exchange spread cannot be manufactured locally.
    if ticker_source_ms(&ticker.symbol).is_some_and(|current| current >= event_ms) {
        return false;
    }
    let event_secs = (event_ms / 1000) as u64;
    if venue
        .get(&ticker.symbol)
        .is_some_and(|current| current.last_updated > event_secs)
    {
        return false;
    }
    let mid = (bid + ask) / 2.0;
    record_ticker_source_ms(&ticker.symbol, event_ms);
    let entry = venue.entry(ticker.symbol).or_default();
    entry.bid = bid;
    entry.ask = ask;
    entry.price = mid;
    entry.bid_size = bid * bid_quantity;
    entry.ask_size = ask * ask_quantity;
    entry.last_updated = event_secs;
    true
}

/// Separate L2 depth streams for the bounded candidate universe.
/// Wait for initial REST discovery; retry fallback discovery promptly if it fails.
async fn run_l2_depth_stream(
    l2_books: L2OrderbookMap,
    price_map: crate::state::PriceMap,
    signal: UpdateSignal,
    discovery_complete: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    let _backoff = Duration::from_secs(1);
    loop {
        // The parent starts REST discovery concurrently. A one-second head start is
        // not enough: locking the fallback universe for 30 minutes loses most books.
        for _ in 0..80 {
            if discovery_complete.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        // Keep the liquid baseline stable. Short-lived/owned candidates are handled by
        // the separate dynamic stream, so they never force all baseline connections to
        // restart or create duplicate subscriptions here.
        let universe: std::collections::HashSet<String> = FALLBACK_DEPTH_SYMBOLS
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut symbols: Vec<String> = universe.into_iter().collect();
        symbols.sort_unstable();

        info!(
            "[binance_l2] Starting L2 subscriptions for {} symbols (requires {} connections)",
            symbols.len(),
            (symbols.len() + DEPTH_STREAMS_PER_CONNECTION - 1) / DEPTH_STREAMS_PER_CONNECTION
        );

        let mut handles = tokio::task::JoinSet::new();

        for chunk in symbols.chunks(DEPTH_STREAMS_PER_CONNECTION) {
            let chunk_symbols = chunk.to_vec();
            let l2_books_clone = l2_books.clone();
            let prices = price_map.clone();
            let signal = signal.clone();

            handles.spawn(async move {
                let mut backoff = Duration::from_secs(1);
                let max_age = Duration::from_secs(1800);
                let conn_start = std::time::Instant::now();
                let stream_url = depth_stream_url(&chunk_symbols);
                let stream_label = chunk_symbols.join(",");

                loop {
                    if conn_start.elapsed() > max_age {
                        break;
                    }

                    let session_start = std::time::Instant::now();
                    let mut backlog_detected = false;
                    let mut planned_rotation = false;
                    match tokio::time::timeout(
                        Duration::from_secs(10),
                        connect_market_ws(&stream_url),
                    )
                    .await
                    {
                        Ok(Ok((mut ws, first_text))) => {
                            let connected_at = std::time::Instant::now();
                            let mut received = 0u64;
                            let mut accepted = 0u64;
                            let mut stale = 0u64;
                            let mut stale_streak = 0u32;
                            let mut invalid = 0u64;
                            let mut last_lag_ms = 0i64;
                            let mut report_at = std::time::Instant::now();
                            let mut buffered = Some(Message::Text(first_text.into()));
                            loop {
                                if connected_at.elapsed()
                                    >= Duration::from_secs(BASELINE_DEPTH_SESSION_SECS)
                                {
                                    planned_rotation = true;
                                    info!("[binance_l2_chunk] Proactive route rotation");
                                    break;
                                }
                                let msg = if let Some(msg) = buffered.take() {
                                    Ok(msg)
                                } else {
                                    match tokio::time::timeout(Duration::from_secs(30), ws.next()).await {
                                        Ok(Some(msg)) => msg,
                                        _ => break,
                                    }
                                };
                                received += 1;
                                if report_at.elapsed() >= Duration::from_secs(10) {
                                    info!("[binance_l2_quality] symbols={} received={} accepted={} stale={} invalid={} last_lag_ms={}",
                                          stream_label, received, accepted, stale, invalid, last_lag_ms);
                                    received = 0;
                                    accepted = 0;
                                    stale = 0;
                                    invalid = 0;
                                    report_at = std::time::Instant::now();
                                }
                                if conn_start.elapsed() > max_age {
                                    break;
                                }
                                let msg = match msg {
                                    Ok(Message::Close(frame)) => {
                                        warn!("[binance_l2_chunk] Closed: {:?}", frame);
                                        break;
                                    }
                                    Ok(msg) => msg,
                                    Err(e) => {
                                        warn!("[binance_l2_chunk] Receive failed: {}", e);
                                        break;
                                    }
                                };
                                if let Message::Text(text) = msg {
                                    if let Ok(data) = serde_json::from_str::<Value>(&text) {
                                        if data.get("code").is_some() {
                                            warn!(
                                                "[binance_l2_chunk] Subscription rejected: {}",
                                                data
                                            );
                                            break;
                                        }
                                        match apply_depth_update(
                                            &data,
                                            &l2_books_clone,
                                            &prices,
                                            &signal,
                                        ) {
                                            DepthUpdate::Accepted(lag_ms) => {
                                                accepted += 1;
                                                stale_streak = 0;
                                                last_lag_ms = lag_ms;
                                            }
                                            DepthUpdate::Stale(lag_ms) => {
                                                stale += 1;
                                                stale_streak += 1;
                                                last_lag_ms = lag_ms;
                                                if depth_backlog_detected(
                                                    lag_ms,
                                                    stale_streak,
                                                    MAX_BASELINE_DEPTH_BACKLOG_MS,
                                                    MAX_BASELINE_CONSECUTIVE_STALE_DEPTH,
                                                ) {
                                                    backlog_detected = true;
                                                    warn!("[binance_l2_chunk] Backlog detected: lag_ms={} stale_streak={}; reconnecting", lag_ms, stale_streak);
                                                    break;
                                                }
                                            }
                                            DepthUpdate::Invalid => invalid += 1,
                                            DepthUpdate::Ignored => {}
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            error!("[binance_l2_chunk] Connect failed: {}", e);
                        }
                        Err(_) => {
                            warn!("[binance_l2_chunk] Connect timed out");
                        }
                    }
                    // A successful handshake alone must not reset retry pacing. Keep
                    // the last snapshot in the map: timestamp freshness expires it,
                    // while an active profile's WS health fallback may already have
                    // replaced it with a newer authoritative snapshot.
                    if planned_rotation {
                        backoff = Duration::from_secs(1);
                        continue;
                    }
                    let stable = !backlog_detected && session_start.elapsed() >= Duration::from_secs(60);
                    if stable {
                        backoff = Duration::from_secs(1);
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = next_backoff(backoff, stable);
                }
            });

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Periodically recycle long-lived transports. Candidate changes do not affect
        // this timer; the dynamic stream applies them independently within one second.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(DEPTH_SUBSCRIPTION_REFRESH_SECS)) => {},
            _ = handles.join_next() => {},
        }
        handles.abort_all();
        while handles.join_next().await.is_some() {}
    }
}

/// A dedicated low-volume connection follows short-lived discovery candidates.
/// Rebuilding the baseline combined streams is intentionally avoided: doing that
/// creates multi-second gaps and used to miss opportunities that vanished before
/// the 120-second baseline refresh.
async fn run_dynamic_l2_depth_stream(
    l2_books: L2OrderbookMap,
    price_map: crate::state::PriceMap,
    l2_wanted: std::sync::Arc<dashmap::DashSet<String>>,
    signal: UpdateSignal,
) {
    let mut backoff = Duration::from_secs(1);

    loop {
        let connection = tokio::time::timeout(
            Duration::from_secs(10),
            connect_market_ws(&format!(
                "{BINANCE_RAW_WS_URL}/btcusdt@depth5@{DYNAMIC_DEPTH_UPDATE_INTERVAL_MS}ms"
            )),
        )
        .await;
        let (mut ws, first_text) = match connection {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => {
                warn!("[binance_l2_dynamic] Connect failed: {}", error);
                tokio::time::sleep(backoff).await;
                backoff = next_backoff(backoff, false);
                continue;
            }
            Err(_) => {
                warn!("[binance_l2_dynamic] Connect timed out");
                tokio::time::sleep(backoff).await;
                backoff = next_backoff(backoff, false);
                continue;
            }
        };

        info!("[binance_l2_dynamic] Connected");
        let session_start = std::time::Instant::now();
        let mut refresh = tokio::time::interval(Duration::from_millis(DYNAMIC_DEPTH_REFRESH_MS));
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut active: std::collections::HashMap<String, std::time::Instant> =
            std::collections::HashMap::new();
        let mut request_id = 1u64;
        let mut received = 1u64;
        let mut accepted = 0u64;
        let mut stale = 0u64;
        let mut invalid = 0u64;
        let mut last_lag_ms = 0i64;
        let mut stale_since = None;
        let mut backlog_detected = false;
        let mut planned_rotation = false;
        let mut report_at = std::time::Instant::now();
        if let Ok(data) = serde_json::from_str::<Value>(&first_text) {
            match apply_depth_update(&data, &l2_books, &price_map, &signal) {
                DepthUpdate::Accepted(lag_ms) => {
                    accepted = 1;
                    last_lag_ms = lag_ms;
                }
                DepthUpdate::Stale(lag_ms) => {
                    stale = 1;
                    stale_since = Some(std::time::Instant::now());
                    last_lag_ms = lag_ms;
                }
                DepthUpdate::Invalid => invalid = 1,
                DepthUpdate::Ignored => {}
            }
        } else {
            invalid = 1;
        }

        loop {
            tokio::select! {
                _ = refresh.tick() => {
                    if session_start.elapsed() >= Duration::from_secs(DYNAMIC_DEPTH_SESSION_SECS) {
                        planned_rotation = true;
                        info!("[binance_l2_dynamic] Proactive route rotation");
                        break;
                    }
                    let now = std::time::Instant::now();
                    let mut wanted: Vec<String> = l2_wanted
                        .iter()
                        .filter_map(|key| key.strip_prefix("binance:").map(str::to_owned))
                        .filter(|symbol| symbol.ends_with("USDT"))
                        .collect();
                    wanted.sort_unstable();
                    wanted.dedup();
                    wanted.truncate(MAX_DYNAMIC_DEPTH_SYMBOLS);

                    let mut added = Vec::new();
                    for symbol in wanted {
                        if let Some(last_seen) = active.get_mut(&symbol) {
                            *last_seen = now;
                        } else if active.len() < MAX_DYNAMIC_DEPTH_SYMBOLS {
                            active.insert(symbol.clone(), now);
                            added.push(symbol);
                        }
                    }

                    let expired: Vec<String> = active
                        .iter()
                        .filter(|(_, last_seen)| {
                            now.duration_since(**last_seen)
                                > Duration::from_secs(DYNAMIC_DEPTH_RETENTION_SECS)
                        })
                        .map(|(symbol, _)| symbol.clone())
                        .collect();

                    if !added.is_empty() {
                        let params: Vec<String> = added
                            .iter()
                            .map(|symbol| format!("{}@depth5@{}ms", symbol.to_lowercase(), DYNAMIC_DEPTH_UPDATE_INTERVAL_MS))
                            .collect();
                        let request = serde_json::json!({"method":"SUBSCRIBE","params":params,"id":request_id});
                        request_id += 1;
                        if let Err(error) = ws.send(Message::Text(request.to_string())).await {
                            warn!("[binance_l2_dynamic] Subscribe failed: {}", error);
                            break;
                        }
                        info!(
                            "[binance_l2_dynamic] Subscribed {} candidates [{}] (active={})",
                            added.len(),
                            added.join(","),
                            active.len(),
                        );
                    }

                    if !expired.is_empty() {
                        let params: Vec<String> = expired
                            .iter()
                            .map(|symbol| format!("{}@depth5@{}ms", symbol.to_lowercase(), DYNAMIC_DEPTH_UPDATE_INTERVAL_MS))
                            .collect();
                        let request = serde_json::json!({"method":"UNSUBSCRIBE","params":params,"id":request_id});
                        request_id += 1;
                        if let Err(error) = ws.send(Message::Text(request.to_string())).await {
                            warn!("[binance_l2_dynamic] Unsubscribe failed: {}", error);
                            break;
                        }
                        for symbol in &expired {
                            active.remove(symbol);
                            l2_books.remove(&format!("binance:{}", symbol));
                        }
                        info!(
                            "[binance_l2_dynamic] Unsubscribed {} expired candidates [{}] (active={})",
                            expired.len(),
                            expired.join(","),
                            active.len(),
                        );
                    }
                }
                message = ws.next() => {
                    let message = match message {
                        Some(Ok(Message::Text(text))) => text,
                        Some(Ok(Message::Close(frame))) => {
                            warn!("[binance_l2_dynamic] Closed: {:?}", frame);
                            break;
                        }
                        Some(Ok(_)) => continue,
                        Some(Err(error)) => {
                            warn!("[binance_l2_dynamic] Receive failed: {}", error);
                            break;
                        }
                        None => break,
                    };
                    received += 1;
                    if let Ok(data) = serde_json::from_str::<Value>(&message) {
                        if data.get("code").is_some() {
                            warn!("[binance_l2_dynamic] Subscription rejected: {}", data);
                            break;
                        }
                        match apply_depth_update(&data, &l2_books, &price_map, &signal) {
                            DepthUpdate::Accepted(lag_ms) => {
                                accepted += 1;
                                stale_since = None;
                                last_lag_ms = lag_ms;
                            }
                            DepthUpdate::Stale(lag_ms) => {
                                stale += 1;
                                last_lag_ms = lag_ms;
                                let stale_for_ms = stale_since
                                    .get_or_insert_with(std::time::Instant::now)
                                    .elapsed()
                                    .as_millis()
                                    .min(u64::MAX as u128)
                                    as u64;
                                if dynamic_depth_backlog_detected(
                                    lag_ms,
                                    stale_for_ms,
                                    MAX_DYNAMIC_DEPTH_BACKLOG_MS,
                                    MAX_DYNAMIC_STALE_DURATION_MS,
                                ) {
                                    backlog_detected = true;
                                    warn!("[binance_l2_dynamic] Backlog detected: lag_ms={} stale_for_ms={}; reconnecting", lag_ms, stale_for_ms);
                                    break;
                                }
                            }
                            DepthUpdate::Invalid => invalid += 1,
                            DepthUpdate::Ignored => {}
                        }
                    } else {
                        invalid += 1;
                    }
                    if report_at.elapsed() >= Duration::from_secs(30) {
                        info!("[binance_l2_dynamic_quality] active={} received={} accepted={} stale={} invalid={} last_lag_ms={}",
                              active.len(), received, accepted, stale, invalid, last_lag_ms);
                        received = 0;
                        accepted = 0;
                        stale = 0;
                        invalid = 0;
                        report_at = std::time::Instant::now();
                    }
                }
            }
        }

        // A redundant dynamic route may still be publishing the same symbols.
        // Leave the last snapshot in place; its exchange timestamp expires it
        // fail-closed after 750 ms if no healthy route replaces it.
        if planned_rotation {
            backoff = Duration::from_secs(1);
            continue;
        }
        let stable = !backlog_detected && session_start.elapsed() >= Duration::from_secs(60);
        if stable {
            backoff = Duration::from_secs(1);
        }
        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff, stable);
    }
}

enum DepthUpdate {
    Accepted(i64),
    Stale(i64),
    Invalid,
    Ignored,
}

fn depth_backlog_detected(
    lag_ms: i64,
    stale_streak: u32,
    max_lag_ms: i64,
    max_stale_streak: u32,
) -> bool {
    lag_ms > max_lag_ms || stale_streak >= max_stale_streak
}

fn dynamic_depth_backlog_detected(
    lag_ms: i64,
    stale_for_ms: u64,
    max_lag_ms: i64,
    max_stale_duration_ms: u64,
) -> bool {
    lag_ms > max_lag_ms || stale_for_ms >= max_stale_duration_ms
}

fn apply_depth_update(
    data: &Value,
    l2_books: &L2OrderbookMap,
    prices: &PriceMap,
    signal: &UpdateSignal,
) -> DepthUpdate {
    if data.get("code").is_some() || data.get("result").is_some() {
        return DepthUpdate::Ignored;
    }
    let depth_data = data.get("data").unwrap_or(data);
    let symbol = data["stream"]
        .as_str()
        .and_then(|stream| stream.split('@').next())
        .or_else(|| depth_data["s"].as_str())
        .unwrap_or("")
        .to_uppercase();
    if symbol.is_empty() || !symbol.ends_with("USDT") {
        return DepthUpdate::Ignored;
    }

    let mut bids = parse_levels(depth_data, "bids");
    if bids.is_empty() {
        bids = parse_levels(depth_data, "b");
    }
    let mut asks = parse_levels(depth_data, "asks");
    if asks.is_empty() {
        asks = parse_levels(depth_data, "a");
    }
    if bids.is_empty() || asks.is_empty() || bids[0].0 >= asks[0].0 {
        return DepthUpdate::Invalid;
    }

    let received_ms = crate::engine::utils::now_ms();
    let exchange_ms = depth_data["E"].as_i64().unwrap_or(0);
    let lag_ms = received_ms - exchange_ms;
    if !(0..=750).contains(&lag_ms) || depth_data["st"].as_i64() == Some(2) {
        return DepthUpdate::Stale(lag_ms);
    }
    if l2_books
        .get(&format!("binance:{}", symbol))
        .map(|book| book.updated_ms >= exchange_ms)
        .unwrap_or(false)
    {
        return DepthUpdate::Ignored;
    }

    if let Some(mut venue) = prices.get_mut("binance") {
        publish_depth_ticker(&mut venue, &symbol, bids[0], asks[0], exchange_ms);
    }
    l2_books.insert(
        format!("binance:{}", symbol),
        L2Snapshot {
            bids,
            asks,
            updated_ms: exchange_ms,
        },
    );
    signal.notify_one();
    DepthUpdate::Accepted(lag_ms)
}

fn depth_stream_url(symbols: &[String]) -> String {
    let streams = symbols
        .iter()
        .map(|symbol| {
            format!(
                "{}@depth5@{}ms",
                symbol.to_lowercase(),
                DEPTH_UPDATE_INTERVAL_MS
            )
        })
        .collect::<Vec<_>>()
        .join("/");
    if symbols.len() == 1 {
        format!("{BINANCE_RAW_WS_URL}/{streams}")
    } else {
        format!("{BINANCE_COMBINED_WS_URL}?streams={streams}")
    }
}

/// Parse [[price, qty], ...] from depth snapshot
fn parse_levels(data: &Value, key: &str) -> Vec<(f64, f64)> {
    data[key]
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|level| {
                    let px: f64 = level[0].as_str()?.parse().ok()?;
                    let sz: f64 = level[1].as_str()?.parse().ok()?;
                    if px.is_finite() && sz.is_finite() && px > 0.0 && sz > 0.0 {
                        Some((px, sz))
                    } else {
                        None
                    }
                })
                .collect::<Option<Vec<_>>>()
        })
        .flatten()
        .unwrap_or_default()
}

fn next_backoff(current: Duration, stable: bool) -> Duration {
    if stable {
        Duration::from_secs(1)
    } else {
        (current * 2).min(Duration::from_secs(60))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_stream_uses_routed_combined_url() {
        assert_eq!(
            depth_stream_url(&["BTCUSDT".into(), "ETHUSDT".into()]),
            "wss://fstream.binance.com/stream?streams=btcusdt@depth5@100ms/ethusdt@depth5@100ms"
        );
    }

    #[test]
    fn single_baseline_depth_uses_raw_stream() {
        assert_eq!(
            depth_stream_url(&["BTCUSDT".into()]),
            "wss://fstream.binance.com/ws/btcusdt@depth5@100ms"
        );
    }

    #[test]
    fn baseline_depth_fits_one_connection() {
        assert!(!FALLBACK_DEPTH_SYMBOLS.is_empty());
        assert!(FALLBACK_DEPTH_SYMBOLS.len() <= DEPTH_STREAMS_PER_CONNECTION);
    }

    #[test]
    fn dynamic_candidate_handoff_is_faster_than_engine_ticks() {
        // Retention makes equal producer/consumer cadence safe without a busy poll.
        assert!(DYNAMIC_DEPTH_REFRESH_MS < 5);
    }

    #[test]
    fn bounded_baseline_and_candidates_use_fast_partial_depth() {
        assert_eq!(DEPTH_UPDATE_INTERVAL_MS, 100);
        assert_eq!(DYNAMIC_DEPTH_UPDATE_INTERVAL_MS, 100);
    }

    #[test]
    fn proactive_route_rotation_is_staggered_and_within_connection_budget() {
        assert!(SECOND_DYNAMIC_START_DELAY_SECS < DYNAMIC_DEPTH_SESSION_SECS);
        assert!(DYNAMIC_DEPTH_SESSION_SECS < BASELINE_DEPTH_SESSION_SECS);
        let five_minutes = 300u64;
        let baseline_races = five_minutes.div_ceil(BASELINE_DEPTH_SESSION_SECS);
        let dynamic_races = five_minutes.div_ceil(DYNAMIC_DEPTH_SESSION_SECS) * 2;
        let steady_attempts = (baseline_races + dynamic_races) * MAX_MARKET_ROUTE_ATTEMPTS as u64;
        // Preserve ample room below Binance's 300-attempt/5-minute IP limit for
        // discovery, execution sockets, transient failures, and operator probes.
        assert!(steady_attempts < 150);
    }

    #[test]
    fn route_quality_uses_exchange_event_time_through_all_payload_shapes() {
        let now = 1_900_000_000_000i64;
        for (value, expected_lag) in [
            (serde_json::json!({"E": now - 12}), 12),
            (
                serde_json::json!({"stream":"btcusdt@depth5@100ms","data":{"E":now - 13}}),
                13,
            ),
            (serde_json::json!([{"E":now - 14}]), 14),
        ] {
            assert_eq!(event_lag_ms(&value.to_string(), now), Some(expected_lag));
        }
        assert_eq!(event_lag_ms(r#"{"result":null,"id":1}"#, now), None);
    }

    #[test]
    fn ws_health_fallback_runs_only_for_active_stale_baseline() {
        assert!(!baseline_health_refresh_due(false, None, 10_000));
        assert!(baseline_health_refresh_due(true, None, 10_000));
        assert!(!baseline_health_refresh_due(true, Some(9_750), 10_000));
        assert!(baseline_health_refresh_due(true, Some(9_749), 10_000));
        assert!(baseline_health_refresh_due(true, Some(10_001), 10_000));
    }

    #[test]
    fn sustained_or_severe_depth_lag_reconnects() {
        assert!(!depth_backlog_detected(2_000, 1, 2_000, 3));
        assert!(!depth_backlog_detected(751, 2, 2_000, 3));
        assert!(depth_backlog_detected(2_001, 1, 2_000, 3));
        assert!(depth_backlog_detected(751, 3, 2_000, 3));
    }

    #[test]
    fn dynamic_depth_uses_elapsed_staleness_not_multiplexed_frame_count() {
        assert!(!dynamic_depth_backlog_detected(
            4_361,
            1,
            MAX_DYNAMIC_DEPTH_BACKLOG_MS,
            MAX_DYNAMIC_STALE_DURATION_MS,
        ));
        assert!(!dynamic_depth_backlog_detected(
            751,
            MAX_DYNAMIC_STALE_DURATION_MS - 1,
            MAX_DYNAMIC_DEPTH_BACKLOG_MS,
            MAX_DYNAMIC_STALE_DURATION_MS,
        ));
        assert!(dynamic_depth_backlog_detected(
            751,
            MAX_DYNAMIC_STALE_DURATION_MS,
            MAX_DYNAMIC_DEPTH_BACKLOG_MS,
            MAX_DYNAMIC_STALE_DURATION_MS,
        ));
        assert!(dynamic_depth_backlog_detected(
            MAX_DYNAMIC_DEPTH_BACKLOG_MS + 1,
            0,
            MAX_DYNAMIC_DEPTH_BACKLOG_MS,
            MAX_DYNAMIC_STALE_DURATION_MS,
        ));
    }

    #[test]
    fn malformed_level_rejects_entire_snapshot_side() {
        for bad in [
            serde_json::json!(["NaN", "1"]),
            serde_json::json!(["99", "0"]),
            serde_json::json!(["99"]),
            serde_json::json!(["bad", "1"]),
        ] {
            let data = serde_json::json!({"b": [["100", "2"], bad]});
            assert!(parse_levels(&data, "b").is_empty());
        }
        assert_eq!(
            parse_levels(&serde_json::json!({"b": [["100", "2"], ["99", "3"]]}), "b"),
            vec![(100.0, 2.0), (99.0, 3.0)]
        );
    }

    #[test]
    fn dynamic_depth_updates_executable_book_and_ticker() {
        let prices = PriceMap::default();
        prices.insert(
            "binance".into(),
            std::collections::HashMap::from([(
                "AINUSDT".into(),
                TickerData {
                    vol: 9_000_000.0,
                    ..Default::default()
                },
            )]),
        );
        let books = L2OrderbookMap::default();
        let signal = UpdateSignal::default();
        let now = crate::engine::utils::now_ms();
        let message = serde_json::json!({
            "E": now,
            "s": "AINUSDT",
            "b": [["0.1000", "200"]],
            "a": [["0.1002", "300"]]
        });

        assert!(matches!(
            apply_depth_update(&message, &books, &prices, &signal),
            DepthUpdate::Accepted(_)
        ));
        let book = books.get("binance:AINUSDT").unwrap();
        assert_eq!(book.bids, vec![(0.1, 200.0)]);
        assert_eq!(book.asks, vec![(0.1002, 300.0)]);
        let venue = prices.get("binance").unwrap();
        let ticker = &venue["AINUSDT"];
        assert_eq!(ticker.vol, 9_000_000.0);
        assert_eq!(ticker.bid_size, 20.0);
        assert_eq!(ticker.ask_size, 30.06);
    }

    #[test]
    fn dynamic_depth_rejects_old_exchange_timestamp() {
        let prices = PriceMap::default();
        let books = L2OrderbookMap::default();
        let signal = UpdateSignal::default();
        let message = serde_json::json!({
            "E": crate::engine::utils::now_ms() - 1_000,
            "s": "AINUSDT",
            "b": [["0.1000", "200"]],
            "a": [["0.1002", "300"]]
        });

        assert!(matches!(
            apply_depth_update(&message, &books, &prices, &signal),
            DepthUpdate::Stale(_)
        ));
        assert!(!books.contains_key("binance:AINUSDT"));
    }

    #[tokio::test]
    #[ignore = "live Binance connectivity check"]
    async fn live_dynamic_subscription_receives_partial_depth() {
        let (mut ws, first_text) = connect_market_ws(&format!(
            "{BINANCE_RAW_WS_URL}/btcusdt@depth5@{DYNAMIC_DEPTH_UPDATE_INTERVAL_MS}ms"
        ))
        .await
        .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut buffered = Some(Message::Text(first_text.into()));
        loop {
            let message = if let Some(message) = buffered.take() {
                message
            } else {
                tokio::time::timeout_at(deadline, ws.next())
                    .await
                    .expect("depth event timeout")
                    .expect("stream closed")
                    .expect("websocket error")
            };
            if let Message::Text(text) = message {
                let data: Value = serde_json::from_str(&text).unwrap();
                if data["s"] == "BTCUSDT" && data["b"].is_array() && data["a"].is_array() {
                    let lag_ms = crate::engine::utils::now_ms() - data["E"].as_i64().unwrap();
                    assert!((0..=750).contains(&lag_ms), "live depth lag was {lag_ms}ms");
                    break;
                }
            }
        }
    }

    #[tokio::test]
    #[ignore = "live Binance connectivity check"]
    async fn live_rest_discovery_receives_fresh_all_symbol_book_tickers() {
        let tickers = fetch_all_book_tickers(&Client::new()).await.unwrap();
        let received_ms = crate::engine::utils::now_ms();
        let mut venue = std::collections::HashMap::new();
        let accepted = tickers
            .into_iter()
            .map(|ticker| usize::from(apply_book_ticker(&mut venue, ticker, received_ms, None)))
            .sum::<usize>();
        assert!(accepted > 100, "only {accepted} fresh discovery tickers");
    }

    #[test]
    fn unstable_sessions_back_off_until_capped() {
        let mut delay = Duration::from_secs(1);
        for expected in [2, 4, 8, 16, 32, 60, 60] {
            delay = next_backoff(delay, false);
            assert_eq!(delay, Duration::from_secs(expected));
        }
        assert_eq!(next_backoff(delay, true), Duration::from_secs(1));
    }

    #[test]
    fn discovery_ticker_preserves_volume_and_rejects_stale_data() {
        let symbol = "DISCOVERYUSDT";
        let mut venue = std::collections::HashMap::from([(
            symbol.to_string(),
            TickerData {
                vol: 42.0,
                ..Default::default()
            },
        )]);
        let payload: BookTickerPayload = serde_json::from_value(serde_json::json!({
            "E": 1_000_000,
            "s": symbol,
            "b": "100.0",
            "B": "2.0",
            "a": "101.0",
            "A": "3.0"
        }))
        .unwrap();
        let ticker = match payload {
            BookTickerPayload::One(ticker) => ticker,
            BookTickerPayload::Many(_) => panic!("single ticker expected"),
        };
        assert!(apply_book_ticker(&mut venue, ticker, 1_000_100, None));
        let updated = &venue[symbol];
        assert_eq!(updated.vol, 42.0);
        assert_eq!(updated.bid, 100.0);
        assert_eq!(updated.ask, 101.0);

        let stale = BinanceBookTicker {
            event_ms: 900_000,
            symbol: symbol.into(),
            bid: "99".into(),
            ask: "100".into(),
            bid_quantity: "1".into(),
            ask_quantity: "1".into(),
        };
        assert!(!apply_book_ticker(&mut venue, stale, 1_000_100, None));
        assert_eq!(venue[symbol].bid, 100.0);
    }

    #[test]
    fn rest_book_ticker_uses_exchange_time_and_preserves_volume() {
        let symbol = "RESTBOOKUSDT";
        let ticker: BinanceBookTicker = serde_json::from_value(serde_json::json!({
            "symbol": symbol,
            "bidPrice": "100.0",
            "bidQty": "2.0",
            "askPrice": "101.0",
            "askQty": "3.0",
            "time": 1_000_000
        }))
        .unwrap();
        let mut venue = std::collections::HashMap::from([(
            symbol.to_string(),
            TickerData {
                vol: 42.0,
                ..Default::default()
            },
        )]);
        assert!(apply_book_ticker(&mut venue, ticker, 1_000_100, None));
        assert_eq!(venue[symbol].vol, 42.0);
        assert_eq!(venue[symbol].bid_size, 200.0);
        assert_eq!(venue[symbol].ask_size, 303.0);
        assert_eq!(venue[symbol].last_updated, 1_000);
    }

    #[test]
    fn older_websocket_ticker_cannot_replace_newer_rest_snapshot() {
        let symbol = "ORDERINGUSDT";
        let mut venue = std::collections::HashMap::from([(
            symbol.to_string(),
            TickerData {
                bid: 100.0,
                ask: 101.0,
                last_updated: 1_002,
                ..Default::default()
            },
        )]);
        let delayed = BinanceBookTicker {
            event_ms: 1_001_900,
            symbol: symbol.into(),
            bid: "90".into(),
            ask: "91".into(),
            bid_quantity: "1".into(),
            ask_quantity: "1".into(),
        };
        assert!(!apply_book_ticker(&mut venue, delayed, 1_002_100, None));
        assert_eq!(venue[symbol].bid, 100.0);
        assert_eq!(venue[symbol].last_updated, 1_002);
    }

    #[test]
    fn older_ticker_in_the_same_second_cannot_replace_newer_quote() {
        let symbol = "SAMESECONDUSDT";
        let mut venue = std::collections::HashMap::new();
        let newer = BinanceBookTicker {
            event_ms: 1_000_900,
            symbol: symbol.into(),
            bid: "100".into(),
            ask: "101".into(),
            bid_quantity: "1".into(),
            ask_quantity: "1".into(),
        };
        assert!(apply_book_ticker(&mut venue, newer, 1_001_000, None));

        let older = BinanceBookTicker {
            event_ms: 1_000_100,
            symbol: symbol.into(),
            bid: "90".into(),
            ask: "91".into(),
            bid_quantity: "1".into(),
            ask_quantity: "1".into(),
        };
        assert!(!apply_book_ticker(&mut venue, older, 1_001_050, None));
        assert_eq!(venue[symbol].bid, 100.0);
        assert_eq!(ticker_source_ms(symbol), Some(1_000_900));
    }

    #[test]
    fn discovery_ticker_cannot_overwrite_owned_execution_depth() {
        let books = L2OrderbookMap::default();
        books.insert(
            "binance:HUSDT".into(),
            L2Snapshot {
                bids: vec![(0.0614, 1_000.0)],
                asks: vec![(0.0615, 1_000.0)],
                updated_ms: 1_000_000,
            },
        );
        let mut venue = std::collections::HashMap::from([(
            "HUSDT".to_string(),
            TickerData {
                bid: 0.0614,
                ask: 0.0615,
                last_updated: 1_000,
                ..Default::default()
            },
        )]);
        let ticker = BinanceBookTicker {
            event_ms: 1_000_100,
            symbol: "HUSDT".into(),
            bid: "0.06199".into(),
            ask: "0.06200".into(),
            bid_quantity: "1000".into(),
            ask_quantity: "1000".into(),
        };
        assert!(!apply_book_ticker(
            &mut venue,
            ticker,
            1_000_200,
            Some(&books),
        ));
        assert_eq!(venue["HUSDT"].bid, 0.0614);
        assert_eq!(venue["HUSDT"].ask, 0.0615);
    }

    #[test]
    fn stale_depth_cache_does_not_disable_discovery_or_become_fresh() {
        let symbol = "STALECACHEUSDT";
        let books = L2OrderbookMap::default();
        books.insert(
            format!("binance:{symbol}"),
            L2Snapshot {
                bids: vec![(100.0, 1.0)],
                asks: vec![(101.0, 1.0)],
                updated_ms: 1_000_000,
            },
        );
        let mut venue = std::collections::HashMap::new();
        let ticker = BinanceBookTicker {
            event_ms: 1_001_000,
            symbol: symbol.into(),
            bid: "102".into(),
            ask: "103".into(),
            bid_quantity: "1".into(),
            ask_quantity: "1".into(),
        };
        assert!(apply_book_ticker(
            &mut venue,
            ticker,
            1_001_100,
            Some(&books)
        ));
        assert_eq!(venue[symbol].bid, 102.0);
        assert_eq!(
            books.get(&format!("binance:{symbol}")).unwrap().updated_ms,
            1_000_000
        );
        assert_eq!(ticker_source_ms(symbol), Some(1_001_000));
    }

    #[test]
    fn delayed_depth_after_discovery_recovery_cannot_relabel_an_older_price() {
        let symbol = "HANDOFFUSDT";
        let now = crate::engine::utils::now_ms();
        let prices = PriceMap::default();
        prices.insert(
            "binance".into(),
            std::collections::HashMap::from([(
                symbol.into(),
                TickerData {
                    bid: 102.0,
                    ask: 103.0,
                    ..Default::default()
                },
            )]),
        );
        record_ticker_source_ms(symbol, now - 100);
        let books = L2OrderbookMap::default();
        let signal = UpdateSignal::default();
        let delayed = serde_json::json!({"s":symbol,"E":now-200,
            "b":[["100","1"]],"a":[["101","1"]]});
        assert!(matches!(
            apply_depth_update(&delayed, &books, &prices, &signal),
            DepthUpdate::Accepted(_)
        ));
        assert_eq!(prices.get("binance").unwrap()[symbol].bid, 102.0);
        assert_eq!(ticker_source_ms(symbol), Some(now - 100));
        assert_eq!(
            books.get(&format!("binance:{symbol}")).unwrap().updated_ms,
            now - 200
        );
        let newer = serde_json::json!({"s":symbol,"E":now-50,
            "b":[["104","1"]],"a":[["105","1"]]});
        assert!(matches!(
            apply_depth_update(&newer, &books, &prices, &signal),
            DepthUpdate::Accepted(_)
        ));
        assert_eq!(prices.get("binance").unwrap()[symbol].bid, 104.0);
        assert_eq!(ticker_source_ms(symbol), Some(now - 50));
    }
}
