//! Binance USDT Futures WebSocket collector — individual real-time bookTicker streams
//! + L2 depth stream for active pairs (top 5 levels @ 100ms)
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
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
            children.spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                run_l2_depth_stream(
                    l2_books_for_depth,
                    prices_for_depth,
                    l2_wanted_for_depth,
                    signal_for_depth,
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
            res = handler(&prices, &signal, &l2_books) => {
                match res {
                    Ok(_)  => { warn!("[binance] WS closed, reconnecting..."); backoff = Duration::from_secs(5); }
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
    _l2_books: &L2OrderbookMap,
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
                info!("[binance] Seeded volume for {} symbols", arr.len());
            }
        } else {
            warn!("[binance] Failed to fetch volume from REST/Proxy.");
        }
    } else {
        warn!("[binance] Skipping volume seed — IP banned.");
    }

    // Individual public bookTicker streams; the aggregated stream updates every 5s.
    // NOTE: L2 depth stream and volume refresher are spawned ONCE in run(),
    // NOT here — spawning here caused N copies after N reconnects (task leak).
    let (mut ws, _) =
        connect_async_tls_with_config("wss://fstream.binance.com/public/stream", None, false, None)
            .await
            .context("Binance WS connect failed")?;
    let symbols: Vec<String> = price_map
        .get("binance")
        .map(|m| m.keys().filter(|s| s.ends_with("USDT")).cloned().collect())
        .unwrap_or_default();
    anyhow::ensure!(
        !symbols.is_empty(),
        "No Binance symbols available for subscription"
    );
    for chunk in symbols.chunks(100) {
        let streams: Vec<_> = chunk
            .iter()
            .map(|s| format!("{}@bookTicker", s.to_lowercase()))
            .collect();
        ws.send(Message::Text(serde_json::json!({"method":"SUBSCRIBE","params":streams,"id":uuid::Uuid::new_v4().to_string()}).to_string().into())).await?;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    info!(
        "[binance] Connected individual bookTicker streams: {} symbols",
        symbols.len()
    );
    let connected_at = std::time::Instant::now();

    #[derive(Deserialize)]
    struct BinanceTicker<'a> {
        #[serde(rename = "E")]
        event_ms: i64,
        #[serde(default)]
        st: Option<i64>,
        #[serde(rename = "s")]
        symbol: &'a str,
        #[serde(rename = "b")]
        bid: &'a str,
        #[serde(rename = "a")]
        ask: &'a str,
        #[serde(rename = "B")]
        bid_qty: &'a str,
        #[serde(rename = "A")]
        ask_qty: &'a str,
    }

    #[derive(Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        data: BinanceTicker<'a>,
    }

    // Ensure binance map exists for getting mutably later
    price_map.entry("binance".to_string()).or_default();

    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(30), ws.next()).await {
        if connected_at.elapsed() > Duration::from_secs(1800) {
            break;
        }
        if let Ok(Message::Text(text)) = msg {
            if let Ok(envelope) = serde_json::from_str::<Envelope>(&text) {
                let t = envelope.data;
                if t.st == Some(2)
                    || !(0..=750).contains(&(crate::engine::utils::now_ms() - t.event_ms))
                {
                    continue;
                }
                if !t.symbol.ends_with("USDT") {
                    continue;
                }
                let bid: f64 = match t.bid.parse() {
                    Ok(v) if f64::is_finite(v) && v > 0.0 => v,
                    _ => continue,
                };
                let ask: f64 = match t.ask.parse() {
                    Ok(v) if f64::is_finite(v) && v > 0.0 => v,
                    _ => continue,
                };
                if bid >= ask {
                    continue;
                }
                let bid_qty: f64 = t.bid_qty.parse().unwrap_or(0.0);
                let ask_qty: f64 = t.ask_qty.parse().unwrap_or(0.0);
                let mid = (bid + ask) / 2.0;

                if let Some(mut ex_map) = price_map.get_mut("binance") {
                    let ts = (t.event_ms / 1000) as u64;
                    if let Some(entry) = ex_map.get_mut(t.symbol) {
                        entry.bid = bid;
                        entry.ask = ask;
                        entry.price = mid;
                        entry.bid_size = bid_qty * mid;
                        entry.ask_size = ask_qty * mid;
                        entry.last_updated = ts;
                        if entry.vol == 0.0 {
                            // Unknown volume stays zero until a real ticker is received.
                        }
                    } else {
                        ex_map.insert(
                            t.symbol.to_string(),
                            TickerData {
                                bid,
                                ask,
                                price: mid,
                                vol: 0.0, // Unknown volume cannot pass the liquidity filter
                                funding_rate: 0.0,
                                bid_size: bid_qty * mid,
                                ask_size: ask_qty * mid,
                                last_updated: ts,
                            },
                        );
                    }
                }
                signal.notify_one();
            }
        }
    }
    Ok(())
}

/// Separate L2 depth stream for top-volume pairs (dynamic, top 200 by volume).
/// Rebuilds symbol list from price_map on each reconnect — handles REST seed failures.
async fn run_l2_depth_stream(
    l2_books: L2OrderbookMap,
    price_map: crate::state::PriceMap,
    l2_wanted: std::sync::Arc<dashmap::DashSet<String>>,
    signal: UpdateSignal,
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
                                if let Ok(Message::Text(text)) = msg {
                                    if let Ok(data) = serde_json::from_str::<Value>(&text) {
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
                                            if let Some(mut venue) = prices.get_mut("binance") {
                                                if let Some(t) = venue.get_mut(&sym_str) {
                                                    t.bid = bids[0].0;
                                                    t.ask = asks[0].0;
                                                    t.bid_size = bids[0].0 * bids[0].1;
                                                    t.ask_size = asks[0].0 * asks[0].1;
                                                    t.last_updated = (exchange_ms / 1000) as u64;
                                                }
                                            }
                                            if l2_books_clone
                                                .get(&format!("binance:{}", sym_str))
                                                .map(|b| b.updated_ms >= exchange_ms)
                                                .unwrap_or(false)
                                            {
                                                continue;
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
            _ = tokio::time::sleep(Duration::from_secs(1800)) => {},
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
