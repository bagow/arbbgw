//! Binance USDT Futures discovery plus bounded executable-liquidity streams.
//! All-book-ticker finds candidates; bounded top-5 subscriptions verify them.
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{L2OrderbookMap, L2Snapshot, PriceMap, TickerData, UpdateSignal};

// At 100 ms the deployment host accumulated a multi-second receive queue even with
// bounded transports. The 500 ms partial-depth stream keeps exchange timestamps inside
// the 750 ms execution freshness budget while cutting message volume by roughly 4x.
const DEPTH_STREAMS_PER_CONNECTION: usize = 5;
const DEPTH_UPDATE_INTERVAL_MS: u64 = 500;
const DEPTH_SUBSCRIPTION_REFRESH_SECS: u64 = 1_800;
const DYNAMIC_DEPTH_REFRESH_SECS: u64 = 1;
const DYNAMIC_DEPTH_RETENTION_SECS: u64 = 30;
const MAX_DEPTH_BACKLOG_MS: i64 = 2_000;
const MAX_CONSECUTIVE_STALE_DEPTH: u32 = 3;
// 24 ranked candidates plus the profile-wide maximum of 10 owned trades.
const MAX_DYNAMIC_DEPTH_SYMBOLS: usize = 40;
// Binance's documented USD-M market-stream endpoints. The `/public/*` relay
// accepts connections but has been observed delivering ever older events.
const BINANCE_RAW_WS_URL: &str = "wss://fstream.binance.com/ws";
const BINANCE_COMBINED_WS_URL: &str = "wss://fstream.binance.com/stream";

// The baseline exists only as a stable health/liquidity probe. Discovery uses the
// all-book ticker and every executable/owned symbol is followed by the dynamic
// stream below, so keeping more than one baseline connection only adds queueing.
const FALLBACK_DEPTH_SYMBOLS: &[&str] = &["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"];

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
            let discovery_for_depth = discovery_ready.clone();
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
                        l2_books_for_depth,
                        prices_for_depth,
                        l2_wanted_for_dynamic,
                        signal_for_dynamic,
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
            res = handler(&prices, &signal, &discovery_ready) => {
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

async fn handler(
    price_map: &PriceMap,
    signal: &UpdateSignal,
    discovery_ready: &std::sync::atomic::AtomicBool,
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

    // The all-market book ticker is intentionally discovery-only. Binance publishes it
    // every five seconds, which is too slow for execution but much cheaper than running
    // depth5 at 100 ms for the entire futures universe. Fresh candidate depth is handled
    // by the bounded streams below and remains mandatory before an order can be sent.
    let (mut ws, _) = connect_async_tls_with_config(
        &format!("{BINANCE_RAW_WS_URL}/!bookTicker"),
        None,
        false,
        None,
    )
    .await
    .context("Binance all-book-ticker connect failed")?;
    info!("[binance] Connected (all bookTicker discovery)");
    price_map.entry("binance".to_string()).or_default();
    let connected_at = std::time::Instant::now();

    while let Ok(Some(message)) = tokio::time::timeout(Duration::from_secs(30), ws.next()).await {
        if connected_at.elapsed() > Duration::from_secs(300) {
            break;
        }
        match message {
            Ok(Message::Text(text)) => {
                let now_ms = crate::engine::utils::now_ms();
                if let Ok(payload) = serde_json::from_str::<BookTickerPayload>(&text) {
                    let tickers = match payload {
                        BookTickerPayload::One(ticker) => vec![ticker],
                        BookTickerPayload::Many(tickers) => tickers,
                    };
                    let mut updated = false;
                    if let Some(mut venue) = price_map.get_mut("binance") {
                        for ticker in tickers {
                            if apply_book_ticker(&mut venue, ticker, now_ms) {
                                updated = true;
                            }
                        }
                    }
                    if updated {
                        signal.notify_one();
                    }
                }
            }
            Ok(Message::Close(frame)) => {
                warn!("[binance] All-book-ticker closed: {:?}", frame);
                break;
            }
            Ok(_) => {}
            Err(error) => anyhow::bail!("Binance all-book-ticker receive failed: {error}"),
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct BinanceBookTicker {
    #[serde(rename = "E", default)]
    event_ms: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid: String,
    #[serde(rename = "a")]
    ask: String,
    #[serde(rename = "B")]
    bid_quantity: String,
    #[serde(rename = "A")]
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
) -> bool {
    if !ticker.symbol.ends_with("USDT") {
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
    let mid = (bid + ask) / 2.0;
    let entry = venue.entry(ticker.symbol).or_default();
    entry.bid = bid;
    entry.ask = ask;
    entry.price = mid;
    entry.bid_size = bid * bid_quantity;
    entry.ask_size = ask * ask_quantity;
    entry.last_updated = (event_ms / 1000) as u64;
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
        let universe: std::collections::HashSet<String> =
            FALLBACK_DEPTH_SYMBOLS.iter().map(|s| s.to_string()).collect();
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
                    match tokio::time::timeout(
                        Duration::from_secs(10),
                        connect_async_tls_with_config(
                            &stream_url,
                            None,
                            false,
                            None,
                        ),
                    )
                    .await
                    {
                        Ok(Ok((mut ws, _))) => {
                            let mut received = 0u64;
                            let mut accepted = 0u64;
                            let mut stale = 0u64;
                            let mut stale_streak = 0u32;
                            let mut invalid = 0u64;
                            let mut last_lag_ms = 0i64;
                            let mut report_at = std::time::Instant::now();
                            while let Ok(Some(msg)) =
                                tokio::time::timeout(Duration::from_secs(30), ws.next()).await
                            {
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
                                                if depth_backlog_detected(lag_ms, stale_streak) {
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
                    // A successful handshake alone must not reset retry pacing.
                    // Expire this connection's books immediately after losing the stream.
                    for symbol in &chunk_symbols {
                        l2_books_clone.remove(&format!("binance:{}", symbol));
                    }
                    let stable = backlog_detected || session_start.elapsed() >= Duration::from_secs(60);
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
    let fallback: std::collections::HashSet<&str> =
        FALLBACK_DEPTH_SYMBOLS.iter().copied().collect();
    let mut backoff = Duration::from_secs(1);

    loop {
        let connection = tokio::time::timeout(
            Duration::from_secs(10),
            connect_async_tls_with_config(
                BINANCE_RAW_WS_URL,
                None,
                false,
                None,
            ),
        )
        .await;
        let (mut ws, _) = match connection {
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
        let mut refresh = tokio::time::interval(Duration::from_secs(DYNAMIC_DEPTH_REFRESH_SECS));
        refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut active: std::collections::HashMap<String, std::time::Instant> =
            std::collections::HashMap::new();
        let mut request_id = 1u64;
        let mut received = 0u64;
        let mut accepted = 0u64;
        let mut stale = 0u64;
        let mut invalid = 0u64;
        let mut last_lag_ms = 0i64;
        let mut stale_streak = 0u32;
        let mut backlog_detected = false;
        let mut report_at = std::time::Instant::now();

        loop {
            tokio::select! {
                _ = refresh.tick() => {
                    let now = std::time::Instant::now();
                    let mut wanted: Vec<String> = l2_wanted
                        .iter()
                        .filter_map(|key| key.strip_prefix("binance:").map(str::to_owned))
                        .filter(|symbol| symbol.ends_with("USDT") && !fallback.contains(symbol.as_str()))
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
                            .map(|symbol| format!("{}@depth5@{}ms", symbol.to_lowercase(), DEPTH_UPDATE_INTERVAL_MS))
                            .collect();
                        let request = serde_json::json!({"method":"SUBSCRIBE","params":params,"id":request_id});
                        request_id += 1;
                        if let Err(error) = ws.send(Message::Text(request.to_string())).await {
                            warn!("[binance_l2_dynamic] Subscribe failed: {}", error);
                            break;
                        }
                        info!("[binance_l2_dynamic] Subscribed {} candidates (active={})", added.len(), active.len());
                    }

                    if !expired.is_empty() {
                        let params: Vec<String> = expired
                            .iter()
                            .map(|symbol| format!("{}@depth5@{}ms", symbol.to_lowercase(), DEPTH_UPDATE_INTERVAL_MS))
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
                        info!("[binance_l2_dynamic] Unsubscribed {} expired candidates (active={})", expired.len(), active.len());
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
                                stale_streak = 0;
                                last_lag_ms = lag_ms;
                            }
                            DepthUpdate::Stale(lag_ms) => {
                                stale += 1;
                                stale_streak += 1;
                                last_lag_ms = lag_ms;
                                if depth_backlog_detected(lag_ms, stale_streak) {
                                    backlog_detected = true;
                                    warn!("[binance_l2_dynamic] Backlog detected: lag_ms={} stale_streak={}; reconnecting", lag_ms, stale_streak);
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

        for symbol in active.keys() {
            l2_books.remove(&format!("binance:{}", symbol));
        }
        let stable = backlog_detected || session_start.elapsed() >= Duration::from_secs(60);
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

fn depth_backlog_detected(lag_ms: i64, stale_streak: u32) -> bool {
    lag_ms > MAX_DEPTH_BACKLOG_MS || stale_streak >= MAX_CONSECUTIVE_STALE_DEPTH
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
        if let Some(ticker) = venue.get_mut(&symbol) {
            ticker.price = (bids[0].0 + asks[0].0) / 2.0;
            ticker.bid = bids[0].0;
            ticker.ask = asks[0].0;
            ticker.bid_size = bids[0].0 * bids[0].1;
            ticker.ask_size = asks[0].0 * asks[0].1;
            ticker.last_updated = (exchange_ms / 1000) as u64;
        }
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
    format!("{BINANCE_COMBINED_WS_URL}?streams={streams}")
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
            "wss://fstream.binance.com/stream?streams=btcusdt@depth5@500ms/ethusdt@depth5@500ms"
        );
    }

    #[test]
    fn baseline_depth_fits_one_connection() {
        assert!(!FALLBACK_DEPTH_SYMBOLS.is_empty());
        assert!(FALLBACK_DEPTH_SYMBOLS.len() <= DEPTH_STREAMS_PER_CONNECTION);
    }

    #[test]
    fn sustained_or_severe_depth_lag_reconnects() {
        assert!(!depth_backlog_detected(MAX_DEPTH_BACKLOG_MS, 1));
        assert!(!depth_backlog_detected(751, MAX_CONSECUTIVE_STALE_DEPTH - 1));
        assert!(depth_backlog_detected(MAX_DEPTH_BACKLOG_MS + 1, 1));
        assert!(depth_backlog_detected(751, MAX_CONSECUTIVE_STALE_DEPTH));
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
        let (mut ws, _) = connect_async_tls_with_config(
            BINANCE_RAW_WS_URL,
            None,
            false,
            None,
        )
        .await
        .unwrap();
        ws.send(Message::Text(
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": ["btcusdt@depth5@500ms"],
                "id": 1
            })
            .to_string(),
        ))
        .await
        .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let message = tokio::time::timeout_at(deadline, ws.next())
                .await
                .expect("depth event timeout")
                .expect("stream closed")
                .expect("websocket error");
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
        let mut venue = std::collections::HashMap::from([(
            "BTCUSDT".to_string(),
            TickerData {
                vol: 42.0,
                ..Default::default()
            },
        )]);
        let payload: BookTickerPayload = serde_json::from_value(serde_json::json!({
            "E": 1_000_000,
            "s": "BTCUSDT",
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
        assert!(apply_book_ticker(&mut venue, ticker, 1_000_100));
        let updated = &venue["BTCUSDT"];
        assert_eq!(updated.vol, 42.0);
        assert_eq!(updated.bid, 100.0);
        assert_eq!(updated.ask, 101.0);

        let stale = BinanceBookTicker {
            event_ms: 900_000,
            symbol: "BTCUSDT".into(),
            bid: "99".into(),
            ask: "100".into(),
            bid_quantity: "1".into(),
            ask_quantity: "1".into(),
        };
        assert!(!apply_book_ticker(&mut venue, stale, 1_000_100));
        assert_eq!(venue["BTCUSDT"].bid, 100.0);
    }
}
