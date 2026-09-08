//! Binance USDT Futures prices and executable liquidity from the same L2 stream.
//! Stable top-5 subscriptions @ 100ms, with REST volume discovery.
use anyhow::Result;
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{error, info, warn};

use crate::state::{L2OrderbookMap, L2Snapshot, PriceMap, TickerData, UpdateSignal};

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
            let l2_wanted_for_depth = l2_wanted.clone();
            let signal_for_depth = signal.clone();
            let discovery_for_depth = discovery_ready.clone();
            children.spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                run_l2_depth_stream(
                    l2_books_for_depth,
                    prices_for_depth,
                    l2_wanted_for_depth,
                    signal_for_depth,
                    discovery_for_depth,
                )
                .await;
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
            res = handler(&prices, &discovery_ready) => {
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

    // L2 updates both best prices and executable quantities. A second full-universe
    // bookTicker feed adds tens of thousands of redundant updates per second.
    // Refresh volume discovery periodically; the owned depth tasks stay connected.
    tokio::time::sleep(Duration::from_secs(300)).await;
    Ok(())
}

/// Separate L2 depth streams for the seeded universe, at most 200 symbols per connection.
/// Wait for initial REST discovery; retry fallback discovery promptly if it fails.
async fn run_l2_depth_stream(
    l2_books: L2OrderbookMap,
    price_map: crate::state::PriceMap,
    l2_wanted: std::sync::Arc<dashmap::DashSet<String>>,
    signal: UpdateSignal,
    discovery_complete: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    // Hardcoded fallback covers common pairs when REST/price_map is empty
    const FALLBACK: &[&str] = &[
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "BNBUSDT",
        "DOGEUSDT",
        "XRPUSDT",
        "PEPEUSDT",
        "SUIUSDT",
        "ADAUSDT",
        "TRXUSDT",
        "AVAXUSDT",
        "LINKUSDT",
        "DOTUSDT",
        "LTCUSDT",
        "MATICUSDT",
        "ARBUSDT",
        "OPUSDT",
        "APTUSDT",
        "NEARUSDT",
        "FILUSDT",
        "WIFUSDT",
        "SHIBUSDT",
        "BONKUSDT",
        "FLOKIUSDT",
        "WLDUSDT",
        "ORDIUSDT",
        "TIAUSDT",
        "SEIUSDT",
        "INJUSDT",
        "FETUSDT",
        "RUNEUSDT",
        "JUPUSDT",
        "ENAUSDT",
        "ONDOUSDT",
        "RENDERUSDT",
        "AAVEUSDT",
        "MKRUSDT",
        "LDOUSDT",
        "PENDLEUSDT",
        "TONUSDT",
        "NOTUSDT",
        "STXUSDT",
        "KATUSDT",
        "SPKUSDT",
        "HYPEUSDT",
        "VIRTUSDT",
        "BOMEUSDT",
        "JUPUSDT",
        "WUSDT",
        "APEUSDT",
        "GMTUSDT",
        "GALAUSDT",
        "FTMUSDT",
        "HBARUSDT",
        "FLOWUSDT",
        "ICPUSDT",
        "SANDUSDT",
        "MANAUSDT",
        "AXSUSDT",
        "CRVUSDT",
        "SNXUSDT",
        "ATOMUSDT",
        "XLMUSDT",
    ];

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
        // Build symbol list from l2_wanted (demand-driven) + fallback
        let wanted_syms: std::collections::HashSet<String> = l2_wanted
            .iter()
            .filter(|k| k.starts_with("binance:"))
            .map(|k| k[8..].to_string())
            .collect();

        // Keep depth streams stable. Candidate churn must not repeatedly disconnect
        // the books from which those same candidates are computed.
        let mut universe: std::collections::HashSet<String> = price_map
            .get("binance")
            .map(|m| m.keys().filter(|s| s.ends_with("USDT")).cloned().collect())
            .unwrap_or_default();
        let discovery_ready = discovery_complete.load(std::sync::atomic::Ordering::Acquire);
        universe.extend(wanted_syms);
        if universe.is_empty() {
            universe.extend(FALLBACK.iter().map(|s| s.to_string()));
        }
        let symbols: Vec<String> = universe.into_iter().collect();

        info!(
            "[binance_l2] Starting L2 subscriptions for {} symbols (requires {} connections)",
            symbols.len(),
            (symbols.len() + 199) / 200
        );

        let mut handles = tokio::task::JoinSet::new();

        for chunk in symbols.chunks(200) {
            let chunk_symbols = chunk.to_vec();
            let l2_books_clone = l2_books.clone();
            let prices = price_map.clone();
            let signal = signal.clone();

            handles.spawn(async move {
                let mut backoff = Duration::from_secs(1);
                let max_age = Duration::from_secs(1800);
                let conn_start = std::time::Instant::now();

                loop {
                    if conn_start.elapsed() > max_age {
                        break;
                    }

                    match connect_async_tls_with_config(
                        "wss://fstream.binance.com/public/stream",
                        None,
                        false,
                        None,
                    )
                    .await
                    {
                        Ok((mut ws, _)) => {
                            use futures_util::SinkExt;
                            let streams: Vec<String> = chunk_symbols
                                .iter()
                                .map(|s| format!("{}@depth5@100ms", s.to_lowercase()))
                                .collect();
                            let msg = serde_json::json!({
                                "method": "SUBSCRIBE",
                                "params": streams,
                                "id": 1
                            });

                            if let Err(e) = ws
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    msg.to_string().into(),
                                ))
                                .await
                            {
                                error!("[binance_l2_chunk] Subscribe failed: {}", e);
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }

                            backoff = Duration::from_secs(1);

                            while let Ok(Some(msg)) =
                                tokio::time::timeout(Duration::from_secs(30), ws.next()).await
                            {
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
                                        let (sym_str, depth_data) =
                                            if let Some(stream) = data["stream"].as_str() {
                                                let sym = stream
                                                    .split('@')
                                                    .next()
                                                    .unwrap_or("")
                                                    .to_uppercase();
                                                (sym, data["data"].clone())
                                            } else if let Some(s) = data["s"].as_str() {
                                                (s.to_uppercase(), data.clone())
                                            } else {
                                                continue;
                                            };

                                        if sym_str.is_empty() || !sym_str.ends_with("USDT") {
                                            continue;
                                        }

                                        let mut bids = parse_levels(&depth_data, "bids");
                                        if bids.is_empty() {
                                            bids = parse_levels(&depth_data, "b");
                                        }
                                        let mut asks = parse_levels(&depth_data, "asks");
                                        if asks.is_empty() {
                                            asks = parse_levels(&depth_data, "a");
                                        }

                                        if !bids.is_empty() && !asks.is_empty() {
                                            let now_ms = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_millis()
                                                as i64;
                                            let exchange_ms = depth_data["E"].as_i64().unwrap_or(0);
                                            if !(0..=750).contains(&(now_ms - exchange_ms))
                                                || depth_data["st"].as_i64() == Some(2)
                                            {
                                                continue;
                                            }
                                            if bids[0].0 >= asks[0].0 {
                                                continue;
                                            }
                                            if l2_books_clone
                                                .get(&format!("binance:{}", sym_str))
                                                .map(|b| b.updated_ms >= exchange_ms)
                                                .unwrap_or(false)
                                            {
                                                continue;
                                            }
                                            if let Some(mut venue) = prices.get_mut("binance") {
                                                if let Some(t) = venue.get_mut(&sym_str) {
                                                    t.price = (bids[0].0 + asks[0].0) / 2.0;
                                                    t.bid = bids[0].0;
                                                    t.ask = asks[0].0;
                                                    t.bid_size = bids[0].0 * bids[0].1;
                                                    t.ask_size = asks[0].0 * asks[0].1;
                                                    t.last_updated = (exchange_ms / 1000) as u64;
                                                }
                                            }
                                            l2_books_clone.insert(
                                                format!("binance:{}", sym_str),
                                                L2Snapshot {
                                                    bids,
                                                    asks,
                                                    updated_ms: exchange_ms,
                                                },
                                            );
                                            signal.notify_one();
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "[binance_l2_chunk] Connect failed: {}. Retry in {:?}",
                                e, backoff
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(60));
                        }
                    }
                }
            });

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Refresh the full instrument universe periodically; candidate churn does not reconnect streams.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(if discovery_ready { 1800 } else { 30 })) => {},
            _ = handles.join_next() => {},
        }
        handles.abort_all();
        while handles.join_next().await.is_some() {}
    }
}

/// Parse [[price, qty], ...] from depth snapshot
fn parse_levels(data: &Value, key: &str) -> Vec<(f64, f64)> {
    data[key]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|level| {
                    let px: f64 = level[0].as_str()?.parse().ok()?;
                    let sz: f64 = level[1].as_str()?.parse().ok()?;
                    if px.is_finite() && sz.is_finite() && px > 0.0 && sz > 0.0 {
                        Some((px, sz))
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}
