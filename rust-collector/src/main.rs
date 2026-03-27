//! rust-collector – CEX arbitrage spread monitor
//!
//! Connects to 11 exchanges over WebSocket, maintains a live price map (DashMap),
//! computes all spread combinations, and pushes batches to the Cloudflare Worker
//! every PUSH_INTERVAL_SECS seconds (default 5).
//!
//! Config (env vars or .env file):
//!   WORKER_URL          – e.g. https://cex-spread-scanner.workers.dev
//!   COLLECTOR_TOKEN     – Bearer token matching the Worker secret
//!   LOCAL_ONLY          – if "true", print spreads to stdout instead of pushing
//!   PUSH_INTERVAL_SECS  – push cadence in seconds (default 5)
//!   MIN_VOL_MILLIONS    – minimum 24h quote volume in millions USD (default 10)
//!   RUST_LOG            – tracing filter, e.g. "info" or "debug"

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{debug, error, info, warn};

// ─── Types ────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TickerData {
    pub bid: f64,
    pub ask: f64,
    pub price: f64,
    pub vol: f64,
    #[serde(rename = "fundingRate")]
    pub funding_rate: f64,
    /// Unix timestamp (seconds) when this entry was last updated.
    /// Used to filter stale data from delisted/halted contracts.
    #[serde(skip)]
    pub last_updated: u64,
}

impl Default for TickerData {
    fn default() -> Self {
        Self {
            bid: 0.0,
            ask: 0.0,
            price: 0.0,
            vol: 0.0,
            funding_rate: 0.0,
            last_updated: 0,
        }
    }
}

/// exchange_name -> symbol -> TickerData
type PriceMap = Arc<DashMap<String, HashMap<String, TickerData>>>;

#[derive(Debug, Clone, Serialize)]
struct SpreadRow {
    pair: String,
    #[serde(rename = "exchangeA")]
    exchange_a: String,
    #[serde(rename = "exchangeB")]
    exchange_b: String,
    #[serde(rename = "priceA")]
    price_a: f64,
    #[serde(rename = "priceB")]
    price_b: f64,
    #[serde(rename = "bidA")]
    bid_a: f64,
    #[serde(rename = "askA")]
    ask_a: f64,
    #[serde(rename = "bidB")]
    bid_b: f64,
    #[serde(rename = "askB")]
    ask_b: f64,
    #[serde(rename = "volA")]
    vol_a: f64,
    #[serde(rename = "volB")]
    vol_b: f64,
    #[serde(rename = "fundingA")]
    funding_a: f64,
    #[serde(rename = "fundingB")]
    funding_b: f64,
    spread: f64,
    direction: String,
}

// ─── Config ───────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Config {
    worker_url: String,
    token: String,
    local_only: bool,
    push_interval: Duration,
    min_vol_m: f64,
    /// Direct paper-trader URL (e.g. http://localhost:3001). Empty = disabled.
    paper_trader_url: String,
    /// Token for paper-trader /internal/spreads (same as INTERNAL_TOKEN in paper-trader.env)
    paper_trader_token: String,
    /// Direct real-trader URL (e.g. http://localhost:3002). Empty = disabled.
    real_trader_url: String,
    /// Token for real-trader /internal/spreads (same as INTERNAL_TOKEN in real-trader.env)
    real_trader_token: String,
}

impl Config {
    fn from_env() -> Self {
        let worker_url = std::env::var("WORKER_URL")
            .unwrap_or_else(|_| "https://cex-spread-scanner.workers.dev".into());
        let token = std::env::var("COLLECTOR_TOKEN").unwrap_or_default();
        let local_only = std::env::var("LOCAL_ONLY")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);
        let push_interval = std::env::var("PUSH_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5);
        let min_vol_m = std::env::var("MIN_VOL_MILLIONS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(10.0);

        let paper_trader_url = std::env::var("PAPER_TRADER_URL")
            .unwrap_or_else(|_| "http://localhost:3001".into());
        let paper_trader_token = std::env::var("PAPER_TRADER_TOKEN").unwrap_or_default();

        let real_trader_url = std::env::var("REAL_TRADER_URL")
            .unwrap_or_else(|_| "http://localhost:3002".into());
        let real_trader_token = std::env::var("REAL_TRADER_TOKEN").unwrap_or_default();

        Config {
            worker_url,
            token,
            local_only,
            push_interval: Duration::from_secs(push_interval),
            min_vol_m,
            paper_trader_url,
            paper_trader_token,
            real_trader_url,
            real_trader_token,
        }
    }
}

// ─── Spread computation ───────────────────────────────────────────────────────

const SPREAD_THRESHOLD: f64 = -1.0;
const SPREAD_MAX: f64 = 50.0;
const TOP_N: usize = 1000;
/// Maximum seconds since last update before a ticker is considered stale.
/// Delisted / halted contracts stop sending WS updates, so their old prices
/// linger in the map and create phantom spreads.
const STALE_SECS: u64 = 120;
/// If ask > bid * INTRA_SPREAD_LIMIT the ticker data is clearly corrupt.
const INTRA_SPREAD_LIMIT: f64 = 1.30; // 30% intra-exchange spread is impossible on a live market

fn compute_spreads(price_map: &PriceMap, min_vol_m: f64) -> Vec<SpreadRow> {
    let exchanges: Vec<String> = price_map.iter().map(|e| e.key().clone()).collect();
    let mut rows: Vec<SpreadRow> = Vec::new();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    for i in 0..exchanges.len() {
        for j in (i + 1)..exchanges.len() {
            let name_a = &exchanges[i];
            let name_b = &exchanges[j];

            let map_a = match price_map.get(name_a) {
                Some(m) => m,
                None => continue,
            };
            let map_b = match price_map.get(name_b) {
                Some(m) => m,
                None => continue,
            };

            for (sym, data_a) in map_a.iter() {
                let data_b = match map_b.get(sym.as_str()) {
                    Some(d) => d,
                    None => continue,
                };

                // Skip stale tickers (delisted/halted contracts stop sending WS updates)
                if data_a.last_updated > 0 && now_secs.saturating_sub(data_a.last_updated) > STALE_SECS {
                    continue;
                }
                if data_b.last_updated > 0 && now_secs.saturating_sub(data_b.last_updated) > STALE_SECS {
                    continue;
                }

                // Skip tickers with invalid bid or ask (0 = delisted / no orderbook)
                if data_a.bid <= 0.0 || data_a.ask <= 0.0 || data_b.bid <= 0.0 || data_b.ask <= 0.0 {
                    continue;
                }

                // Skip corrupt tickers where the intra-exchange spread is unrealistically wide
                if data_a.ask > data_a.bid * INTRA_SPREAD_LIMIT
                    || data_b.ask > data_b.bid * INTRA_SPREAD_LIMIT
                {
                    continue;
                }

                // Skip low-liquidity pairs (if volume is provided by the stream)
                let min_v = min_vol_m * 1_000_000.0;
                if (data_a.vol > 0.0 && data_a.vol < min_v)
                    || (data_b.vol > 0.0 && data_b.vol < min_v)
                {
                    continue;
                }

                let a_over_b = if data_b.ask > 0.0 {
                    (data_a.bid - data_b.ask) / data_b.ask * 100.0
                } else {
                    f64::NEG_INFINITY
                };
                let b_over_a = if data_a.ask > 0.0 {
                    (data_b.bid - data_a.ask) / data_a.ask * 100.0
                } else {
                    f64::NEG_INFINITY
                };

                let (spread, direction) = if a_over_b >= b_over_a {
                    (a_over_b, format!("{name_a}>{name_b}"))
                } else {
                    (b_over_a, format!("{name_b}>{name_a}"))
                };

                if spread < SPREAD_THRESHOLD || spread > SPREAD_MAX {
                    continue;
                }

                let spread = (spread * 10000.0).round() / 10000.0;

                rows.push(SpreadRow {
                    pair: sym.clone(),
                    exchange_a: name_a.clone(),
                    exchange_b: name_b.clone(),
                    price_a: data_a.price,
                    price_b: data_b.price,
                    bid_a: data_a.bid,
                    ask_a: data_a.ask,
                    bid_b: data_b.bid,
                    ask_b: data_b.ask,
                    vol_a: data_a.vol,
                    vol_b: data_b.vol,
                    funding_a: data_a.funding_rate,
                    funding_b: data_b.funding_rate,
                    spread,
                    direction,
                });
            }
        }
    }

    rows.sort_by(|a, b| {
        b.spread
            .partial_cmp(&a.spread)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows.truncate(TOP_N);
    rows
}

/// Build a lightweight maps payload for the Worker: { exchange: [symbol, ...] }
fn build_maps_payload(price_map: &PriceMap) -> Value {
    let mut out = serde_json::Map::new();
    for entry in price_map.iter() {
        let ex = entry.key().clone();
        let symbols: Vec<Value> = entry
            .value()
            .iter()
            .map(|(sym, _)| Value::String(sym.clone()))
            .collect();
        out.insert(ex, Value::Array(symbols));
    }
    Value::Object(out)
}

/// Sanitize f64 — NaN/Infinity serialize as JSON null which breaks paper-trader deserialization.
#[inline]
fn safe_f64(v: f64) -> f64 {
    if v.is_finite() { v } else { 0.0 }
}

/// Build a FILTERED maps payload for paper-trader.
/// Only includes exchanges/symbols that appear in the given spreads list.
/// Using filtered maps keeps the payload small and avoids NaN/Inf serialization issues.
fn build_filtered_maps_payload(price_map: &PriceMap, spreads: &[SpreadRow]) -> Value {
    // Collect all (exchange, symbol) pairs we need
    let mut needed: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    for row in spreads {
        needed.entry(row.exchange_a.clone()).or_default().insert(row.pair.clone());
        needed.entry(row.exchange_b.clone()).or_default().insert(row.pair.clone());
    }

    let mut out = serde_json::Map::new();
    for (ex, syms) in &needed {
        let mut sym_map = serde_json::Map::new();
        if let Some(ex_map) = price_map.get(ex) {
            for sym in syms {
                if let Some(data) = ex_map.get(sym.as_str()) {
                    sym_map.insert(sym.clone(), json!({
                        "bid":        safe_f64(data.bid),
                        "ask":        safe_f64(data.ask),
                        "price":      safe_f64(data.price),
                        "vol":        safe_f64(data.vol),
                        "fundingRate": safe_f64(data.funding_rate)
                    }));
                }
            }
        }
        out.insert(ex.clone(), Value::Object(sym_map));
    }
    Value::Object(out)
}

fn build_full_maps_payload(price_map: &PriceMap) -> Value {
    let mut out = serde_json::Map::new();
    for entry in price_map.iter() {
        let ex = entry.key().clone();
        let mut sym_map = serde_json::Map::new();
        for (sym, data) in entry.value().iter() {
            sym_map.insert(sym.clone(), json!({
                "bid":        safe_f64(data.bid),
                "ask":        safe_f64(data.ask),
                "price":      safe_f64(data.price),
                "vol":        safe_f64(data.vol),
                "fundingRate": safe_f64(data.funding_rate)
            }));
        }
        out.insert(ex, Value::Object(sym_map));
    }
    Value::Object(out)
}

// ─── Push loop ────────────────────────────────────────────────────────────────

async fn push_loop(price_map: PriceMap, mut config: Config, http: Client) {
    let mut interval = tokio::time::interval(config.push_interval);
    interval.tick().await; // skip immediate first tick

    loop {
        interval.tick().await;
        let start = Instant::now();

        let mut ex_counts: Vec<String> = price_map
            .iter()
            .map(|e| format!("{}={}", e.key(), e.value().len()))
            .collect();
        ex_counts.sort();
        info!("PriceMap: [{}]", ex_counts.join(", "));

        let spreads = compute_spreads(&price_map, config.min_vol_m);
        info!(
            "Computed {} spread rows in {:?}",
            spreads.len(),
            start.elapsed()
        );

        if config.local_only {
            for row in spreads.iter().take(20) {
                println!(
                    "{:<10} {:>6.3}%  {} vs {}  bid_a={:.4} ask_b={:.4}",
                    row.pair, row.spread, row.exchange_a, row.exchange_b, row.bid_a, row.ask_b
                );
            }
            println!("--- {} spreads total ---\n", spreads.len());
        } else {
            if config.token.is_empty() {
                warn!("COLLECTOR_TOKEN is empty — skipping push");
                continue;
            }

            let maps_payload = build_maps_payload(&price_map);
            let body = json!({ "spreads": spreads, "maps": maps_payload });

            // ── 1. Push to paper-trader directly (localhost, fire-and-forget) ──
            // Send FULL ticker map to paper-trader so it always sees prices for open trades.
            if !config.paper_trader_url.is_empty() && !config.paper_trader_token.is_empty() {
                let pt_url = format!("{}/internal/spreads", config.paper_trader_url.trim_end_matches('/'));
                let full_maps = build_full_maps_payload(&price_map);
                let pt_body = json!({ "spreads": spreads, "maps": full_maps });
                let http2 = http.clone();
                let token2 = config.paper_trader_token.clone();
                tokio::spawn(async move {
                    match http2
                        .post(&pt_url)
                        .bearer_auth(&token2)
                        .json(&pt_body)
                        .timeout(Duration::from_secs(10))
                        .send()
                        .await
                    {
                        Ok(r) => {
                            if r.status().is_success() {
                                debug!("paper-trader ← {} spreads → {}", pt_body["spreads"].as_array().map(|a| a.len()).unwrap_or(0), r.status());
                            } else {
                                let status = r.status();
                                let body = r.text().await.unwrap_or_default();
                                warn!("paper-trader push failed {}: {}", status, body);
                            }
                        },
                        Err(e) => warn!("paper-trader push error: {}", e),
                    }
                });
            }

            // ── 1.b. Push to real-trader directly (localhost, fire-and-forget) ──
            if !config.real_trader_url.is_empty() && !config.real_trader_token.is_empty() {
                let rt_url = format!("{}/internal/spreads", config.real_trader_url.trim_end_matches('/'));
                let full_maps = build_full_maps_payload(&price_map);
                let rt_body = json!({ "spreads": spreads, "maps": full_maps });
                let http3 = http.clone();
                let token3 = config.real_trader_token.clone();
                tokio::spawn(async move {
                    match http3
                        .post(&rt_url)
                        .bearer_auth(&token3)
                        .json(&rt_body)
                        .timeout(Duration::from_secs(10))
                        .send()
                        .await
                    {
                        Ok(r) => {
                            if r.status().is_success() {
                                debug!("real-trader ← {} spreads → {}", rt_body["spreads"].as_array().map(|a| a.len()).unwrap_or(0), r.status());
                            } else {
                                let status = r.status();
                                let body = r.text().await.unwrap_or_default();
                                warn!("real-trader push failed {}: {}", status, body);
                            }
                        },
                        Err(e) => warn!("real-trader push error: {}", e),
                    }
                });
            }

            // ── 2. Push snapshot to Cloudflare Worker (for frontend display) ──
            let url = format!("{}/api/update", config.worker_url.trim_end_matches('/'));

            match http
                .post(&url)
                .bearer_auth(&config.token)
                .json(&body)
                .timeout(Duration::from_secs(15))
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        if let Ok(json) = resp.json::<Value>().await {
                            if let Some(cfg) = json.get("config") {
                                if let Some(int_secs) =
                                    cfg.get("push_interval").and_then(|v| v.as_u64())
                                {
                                    if int_secs > 0 && int_secs != config.push_interval.as_secs() {
                                        info!(
                                            "Updating push_interval from {}s to {}s",
                                            config.push_interval.as_secs(),
                                            int_secs
                                        );
                                        config.push_interval = Duration::from_secs(int_secs);
                                        interval = tokio::time::interval(config.push_interval);
                                        interval.tick().await; // burn immediate tick
                                    }
                                }
                            }
                        }
                        info!("Pushed {} spreads → worker {}", spreads.len(), status);
                    } else {
                        let body = resp.text().await.unwrap_or_default();
                        error!("Worker push failed {}: {}", status, body);
                    }
                }
                Err(e) => error!("Worker push error: {}", e),
            }
        }
    }
}

// ─── Helper: upsert into PriceMap ─────────────────────────────────────────────

fn upsert(price_map: &PriceMap, exchange: &str, sym: String, mut data: TickerData) {
    data.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    price_map
        .entry(exchange.to_string())
        .or_default()
        .insert(sym, data);
}

// ─── WebSocket handlers ───────────────────────────────────────────────────────
//
// Each exchange task:
//  1. Connect to WS endpoint
//  2. Send subscription message
//  3. Loop: receive → parse → update PriceMap
//  4. Reconnect on disconnect with exponential backoff

async fn connect_ws(
    url: &str,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    let (ws, _) = connect_async_tls_with_config(url, None, false, None)
        .await
        .context("WS connect failed")?;
    Ok(ws)
}

macro_rules! ws_task {
    ($name:expr, $price_map:expr, $body:expr) => {{
        let price_map = Arc::clone(&$price_map);
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                info!("[{}] Connecting...", $name);
                match $body(&price_map).await {
                    Ok(_) => warn!("[{}] WS closed normally, reconnecting...", $name),
                    Err(e) => error!("[{}] Error: {}. Reconnecting in {:?}", $name, e, backoff),
                }
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        })
    }};
}

// ── Binance ───────────────────────────────────────────────────────────────────
async fn binance_handler(price_map: &PriceMap) -> Result<()> {
    let mut ws = connect_ws("wss://fstream.binance.com/ws/!ticker@arr").await?;
    info!("[binance] Connected");
    while let Some(msg) = ws.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            let arr: Value = serde_json::from_str(&text)?;
            if let Some(items) = arr.as_array() {
                for item in items {
                    let sym = item["s"].as_str().unwrap_or("").to_string();
                    if sym.is_empty() || !sym.ends_with("USDT") {
                        continue;
                    }
                    let price: f64 = item["c"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    let bid: f64 = item["b"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(price);
                    let ask: f64 = item["a"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(price);
                    let vol: f64 = item["q"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    if price <= 0.0 {
                        continue;
                    }
                    upsert(
                        price_map,
                        "binance",
                        sym,
                        TickerData {
                            bid,
                            ask,
                            price,
                            vol,
                            funding_rate: 0.0,
                            last_updated: 0,
                        },
                    );
                }
            }
        }
    }
    Ok(())
}

// ── Bybit ─────────────────────────────────────────────────────────────────────
async fn bybit_handler(price_map: &PriceMap) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://api.bybit.com/v5/market/tickers?category=linear").send().await?.json().await?;
    let symbols: Vec<String> = res["result"]["list"].as_array().unwrap_or(&vec![])
        .iter()
        .filter_map(|i| {
            let s = i["symbol"].as_str()?;
            if s.ends_with("USDT") { Some(s.to_string()) } else { None }
        })
        .collect();
    info!("[bybit] Found {} USDT perps", symbols.len());

    let mut ws = connect_ws("wss://stream.bybit.com/v5/public/linear").await?;
    info!("[bybit] Connected, subscribing...");

    // Bybit allows max 10 args per request
    for chunk in symbols.chunks(10) {
        let args: Vec<String> = chunk.iter().map(|s| format!("tickers.{}", s)).collect();
        ws.send(Message::Text(json!({"op": "subscribe", "args": args}).to_string().into())).await?;
    }

    let mut ping_interval = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                        if !v["topic"].as_str().map(|t| t.starts_with("tickers.")).unwrap_or(false) {
                            continue;
                        }
                        let d = &v["data"];
                        let sym = d["symbol"].as_str().unwrap_or("").to_string();
                        if sym.is_empty() || !sym.ends_with("USDT") { continue; }
                        let price: f64 = d["lastPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let bid: f64 = d["bid1Price"].as_str().and_then(|s| s.parse().ok()).unwrap_or(price);
                        let ask: f64 = d["ask1Price"].as_str().and_then(|s| s.parse().ok()).unwrap_or(price);
                        let vol: f64 = d["turnover24h"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let funding: f64 = d["fundingRate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        if price <= 0.0 { continue; }
                        upsert(price_map, "bybit", sym, TickerData { bid, ask, price, vol, funding_rate: funding, last_updated: 0 });
                    }
                    Some(Ok(Message::Ping(p))) => { ws.send(Message::Pong(p)).await?; }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => return Err(e.into()),
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                ws.send(Message::Text(json!({"op":"ping"}).to_string().into())).await?;
            }
        }
    }
    Ok(())
}

// ── OKX ───────────────────────────────────────────────────────────────────────
async fn okx_handler(price_map: &PriceMap) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://www.okx.com/api/v5/public/instruments?instType=SWAP").send().await?.json().await?;
    let symbols: Vec<String> = res["data"].as_array().unwrap_or(&vec![])
        .iter()
        .filter_map(|i| {
            let s = i["instId"].as_str()?;
            if s.ends_with("-USDT-SWAP") { Some(s.to_string()) } else { None }
        })
        .collect();
    info!("[okx] Found {} USDT perps", symbols.len());

    let mut ws = connect_ws("wss://ws.okx.com:8443/ws/v5/public").await?;
    info!("[okx] Connected, subscribing tickers...");

    // OKX limit 10 args per request ? Play safe with 10
    for chunk in symbols.chunks(10) {
        let args: Vec<Value> = chunk.iter().map(|s| json!({"channel": "tickers", "instId": s})).collect();
        ws.send(Message::Text(json!({"op": "subscribe", "args": args}).to_string().into())).await?;
    }

    let mut ping_interval = tokio::time::interval(Duration::from_secs(25));

    loop {
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if text == "pong" { continue; }
                        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                        if let Some(arr) = v["data"].as_array() {
                            for item in arr {
                                let inst_id = item["instId"].as_str().unwrap_or("");
                                // BTC-USDT-SWAP -> BTCUSDT
                                let parts: Vec<&str> = inst_id.split('-').collect();
                                if parts.len() < 2 { continue; }
                                let sym = format!("{}{}", parts[0], parts[1]);
                                if !sym.ends_with("USDT") { continue; }
                                let price: f64 = item["last"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                let bid: f64 = item["bidPx"].as_str().and_then(|s| s.parse().ok()).unwrap_or(price);
                                let ask: f64 = item["askPx"].as_str().and_then(|s| s.parse().ok()).unwrap_or(price);
                                // volCcy24h is in base coin → convert to USD by multiplying by price
                                let vol: f64 = item["volCcy24h"].as_str()
                                    .and_then(|s| s.parse::<f64>().ok())
                                    .map(|v| v * price)
                                    .unwrap_or(0.0);
                                let funding: f64 = item["fundingRate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                                if price <= 0.0 { continue; }
                                upsert(price_map, "okx", sym, TickerData { bid, ask, price, vol, funding_rate: funding, last_updated: 0 });
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => return Err(e.into()),
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                ws.send(Message::Text("ping".into())).await?;
            }
        }
    }
    Ok(())
}

// ── Gate.io ───────────────────────────────────────────────────────────────────
async fn gate_handler(price_map: &PriceMap) -> Result<()> {
    let mut ws = connect_ws("wss://fx-ws.gateio.ws/v4/ws/usdt").await?;
    info!("[gate] Connected, subscribing...");

    let sub = json!({
        "time": 0,
        "channel": "futures.tickers",
        "event": "subscribe",
        "payload": ["!all"]
    });
    ws.send(Message::Text(sub.to_string().into())).await?;

    while let Some(msg) = ws.next().await {
        if let Ok(Message::Text(text)) = msg {
            let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            if v["channel"].as_str() != Some("futures.tickers") {
                continue;
            }
            for item in v["result"].as_array().unwrap_or(&Vec::new()) {
                let contract = item["contract"].as_str().unwrap_or("");
                let sym = contract.replace('_', "");
                if !sym.ends_with("USDT") {
                    continue;
                }
                let price: f64 = item["last"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let bid: f64 = item["highest_bid"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(price);
                let ask: f64 = item["lowest_ask"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(price);
                let vol: f64 = item["volume_24h_quote"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let funding: f64 = item["funding_rate"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                if price <= 0.0 {
                    continue;
                }
                upsert(
                    price_map,
                    "gate",
                    sym,
                    TickerData {
                        bid,
                        ask,
                        price,
                        vol,
                        funding_rate: funding,
                        last_updated: 0,
                    },
                );
            }
        }
    }
    Ok(())
}

// ── MEXC ──────────────────────────────────────────────────────────────────────
async fn mexc_handler(price_map: &PriceMap) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://contract.mexc.com/api/v1/contract/detail").send().await?.json().await?;
    let symbols: Vec<String> = res["data"].as_array().unwrap_or(&vec![])
        .iter()
        .filter_map(|i| {
            let s = i["symbol"].as_str()?;
            if s.ends_with("_USDT") { Some(s.to_string()) } else { None }
        })
        .collect();
    info!("[mexc] Found {} USDT perps", symbols.len());

    let mut ws = connect_ws("wss://contract.mexc.com/edge").await?;
    info!("[mexc] Connected, subscribing...");

    // MEXC individual subscriptions
    for sym in symbols {
        ws.send(Message::Text(json!({
            "method": "sub.ticker",
            "param": { "symbol": sym }
        }).to_string().into())).await?;
    }

    let mut ping_interval = tokio::time::interval(Duration::from_secs(20));

    loop {
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                        let channel = v["channel"].as_str().unwrap_or("");
                        if channel == "push.ticker" || channel == "push.ticker.batch" || channel == "push.symbol" {
                            let d = &v["data"];
                            let raw_sym = d["symbol"].as_str().unwrap_or("");
                            let sym = raw_sym.replace('_', "");
                            if !sym.ends_with("USDT") { continue; }
                            let price: f64 = d["lastPrice"].as_f64().unwrap_or(0.0);
                            let bid: f64 = d["bid1"].as_f64().or_else(|| d["bidPrice"].as_f64()).unwrap_or(price);
                            let ask: f64 = d["ask1"].as_f64().or_else(|| d["askPrice"].as_f64()).unwrap_or(price);
                            let vol: f64 = d["amount24"].as_f64().unwrap_or(0.0);
                            let funding: f64 = d["fundingRate"].as_f64().unwrap_or(0.0);
                            if price <= 0.0 { continue; }
                            upsert(price_map, "mexc", sym, TickerData { bid, ask, price, vol, funding_rate: funding, last_updated: 0 });
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => return Err(e.into()),
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                ws.send(Message::Text(json!({"method":"ping"}).to_string().into())).await?;
            }
        }
    }
    Ok(())
}

// ── Bitget ────────────────────────────────────────────────────────────────────
async fn bitget_handler(price_map: &PriceMap) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES").send().await?.json().await?;
    let symbols: Vec<String> = res["data"].as_array().unwrap_or(&vec![])
        .iter()
        .filter_map(|i| {
            let s = i["symbol"].as_str()?;
            if s.ends_with("USDT") { Some(s.to_string()) } else { None }
        })
        .collect();
    info!("[bitget] Found {} USDT perps", symbols.len());

    let mut ws = connect_ws("wss://ws.bitget.com/v2/ws/public").await?;
    info!("[bitget] Connected, subscribing...");

    // Bitget limit 10 args per request
    for chunk in symbols.chunks(10) {
        let args: Vec<Value> = chunk.iter().map(|s| json!({"instType": "USDT-FUTURES", "channel": "ticker", "instId": s})).collect();
        ws.send(Message::Text(json!({"op": "subscribe", "args": args}).to_string().into())).await?;
    }

    let mut ping_interval = tokio::time::interval(Duration::from_secs(25));

    loop {
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if text == "pong" { continue; }
                        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                        for item in v["data"].as_array().unwrap_or(&Vec::new()) {
                            let sym = item["instId"].as_str().unwrap_or("").to_string();
                            if !sym.ends_with("USDT") { continue; }
                            let price: f64 = item["lastPr"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let bid: f64 = item["bidPr"].as_str().and_then(|s| s.parse().ok()).unwrap_or(price);
                            let ask: f64 = item["askPr"].as_str().and_then(|s| s.parse().ok()).unwrap_or(price);
                            let vol: f64 = item["usdtVolume"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let funding: f64 = item["fundingRate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            if price <= 0.0 { continue; }
                            upsert(price_map, "bitget", sym, TickerData { bid, ask, price, vol, funding_rate: funding, last_updated: 0 });
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => return Err(e.into()),
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                ws.send(Message::Text("ping".into())).await?;
            }
        }
    }
    Ok(())
}

// ── HTX (Huobi) ───────────────────────────────────────────────────────────────
async fn htx_handler(price_map: &PriceMap) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://api.hbdm.com/linear-swap-api/v1/swap_contract_info").send().await?.json().await?;
    let symbols: Vec<String> = res["data"].as_array().unwrap_or(&vec![])
        .iter()
        .filter_map(|i| {
            let s = i["contract_code"].as_str()?;
            // Only subscribe to ACTIVE contracts (contract_status == 1).
            // status=3 means delisted/settling — their prices are stale and create fake spreads.
            let status = i["contract_status"].as_i64().unwrap_or(0);
            if s.ends_with("-USDT") && status == 1 { Some(s.to_string()) } else { None }
        })
        .collect();
    info!("[htx] Found {} USDT perps", symbols.len());

    let mut ws = connect_ws("wss://api.hbdm.com/linear-swap-ws").await?;
    info!("[htx] Connected, subscribing...");

    // Subscribe to bbo (best bid/offer) for real bid/ask prices
    // Subscribe to detail for volume data
    for sym in &symbols {
        ws.send(Message::Text(json!({
            "sub": format!("market.{}.bbo", sym),
            "id": format!("htx_bbo_{}", sym)
        }).to_string().into())).await?;
        ws.send(Message::Text(json!({
            "sub": format!("market.{}.detail", sym),
            "id": format!("htx_detail_{}", sym)
        }).to_string().into())).await?;
    }

    loop {
        match ws.next().await {
            Some(Ok(Message::Binary(bin))) => {
                use std::io::Read;
                let mut decoder = flate2::read::GzDecoder::new(bin.as_slice());
                let mut text = String::new();
                if decoder.read_to_string(&mut text).is_err() { continue; }
                let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);

                // HTX heartbeat: respond with pong
                if let Some(ping_ts) = v["ping"].as_i64() {
                    ws.send(Message::Text(json!({"pong": ping_ts}).to_string().into())).await?;
                    continue;
                }

                let ch = match v["ch"].as_str() { Some(c) => c, None => continue };
                let parts: Vec<&str> = ch.splitn(3, '.').collect();
                if parts.len() < 3 { continue; }
                let sym = parts[1].replace('-', ""); // BTC-USDT → BTCUSDT
                if !sym.ends_with("USDT") { continue; }
                let channel_type = parts[2]; // "bbo" or "detail"

                let tick = &v["tick"];

                if channel_type == "bbo" {
                    // bbo tick: { "bid": 0.1234, "ask": 0.1235, ... }
                    let bid: f64 = tick["bid"].as_f64().unwrap_or(0.0);
                    let ask: f64 = tick["ask"].as_f64().unwrap_or(0.0);
                    let price: f64 = if bid > 0.0 && ask > 0.0 { (bid + ask) / 2.0 } else { bid.max(ask) };
                    if bid <= 0.0 || ask <= 0.0 { continue; }
                    // Merge bid/ask into existing entry (preserve vol from detail)
                    let existing_vol = price_map
                        .get("htx")
                        .and_then(|m| m.get(&sym).map(|d| d.vol))
                        .unwrap_or(0.0);
                    upsert(price_map, "htx", sym, TickerData { bid, ask, price, vol: existing_vol, funding_rate: 0.0, last_updated: 0 });
                } else if channel_type == "detail" {
                    // detail tick: { "close": 0.1234, "vol": 123456.0 }
                    let close: f64 = tick["close"].as_f64().unwrap_or(0.0);
                    let vol: f64 = tick["vol"].as_f64().unwrap_or(0.0) * close; // vol in base coin → USD
                    if close <= 0.0 { continue; }
                    // Update vol; preserve bid/ask from bbo if already set
                    let existing = price_map
                        .get("htx")
                        .and_then(|m| m.get(&sym).cloned());
                    let (bid, ask, price) = match existing {
                        Some(ref d) if d.bid > 0.0 => (d.bid, d.ask, d.price),
                        _ => (close, close, close), // bbo not yet received
                    };
                    upsert(price_map, "htx", sym, TickerData { bid, ask, price, vol, funding_rate: 0.0, last_updated: 0 });
                }
            }
            Some(Ok(Message::Close(_))) | None => break,
            Some(Err(e)) => return Err(e.into()),
            _ => {}
        }
    }
    Ok(())
}

// ── KuCoin ────────────────────────────────────────────────────────────────────
async fn kucoin_handler(price_map: &PriceMap) -> Result<()> {
    let http = Client::new();

    // Step 1: Get all active USDT-margined contracts from REST
    let contracts: Value = http
        .get("https://api-futures.kucoin.com/api/v1/contracts/active")
        .timeout(Duration::from_secs(10))
        .send().await?.json().await?;
    let symbols: Vec<String> = contracts["data"]
        .as_array().unwrap_or(&vec![])
        .iter()
        .filter_map(|c| {
            let sym = c["symbol"].as_str()?;
            // Keep only USDT-margined contracts (e.g. XBTUSDTM, ETHUSDTM)
            if sym.ends_with("USDTM") { Some(sym.to_string()) } else { None }
        })
        .collect();
    info!("[kucoin] Found {} active USDT-M contracts", symbols.len());

    // Step 2: Get WS token
    let resp: Value = http
        .post("https://api-futures.kucoin.com/api/v1/bullet-public")
        .send().await?.json().await?;
    let token = resp["data"]["token"].as_str()
        .ok_or_else(|| anyhow!("No KuCoin WS token"))?;
    let endpoint = resp["data"]["instanceServers"][0]["endpoint"].as_str()
        .unwrap_or("wss://ws-api-futures.kucoin.com");
    let ws_url = format!("{}?token={}&connectId=rustcollector", endpoint, token);

    let mut ws = connect_ws(&ws_url).await?;
    info!("[kucoin] Connected, subscribing to all {} USDT-M futures...", symbols.len());

    // Step 3: Subscribe in batches of 100 symbols
    for chunk in symbols.chunks(100) {
        let topic = format!("/contractMarket/tickerV2:{}", chunk.join(","));
        ws.send(Message::Text(json!({
            "id": "sub",
            "type": "subscribe",
            "topic": topic,
            "privateChannel": false,
            "response": true
        }).to_string().into())).await?;
    }

    let mut ping_interval = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if text == "Ping" || text.contains("Ping") { ws.send(Message::Text("Pong".into())).await?; continue; }
                        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                        if v["type"].as_str() != Some("message") { continue; }
                        let raw_sym = v["topic"].as_str().unwrap_or("").replace("/contractMarket/tickerV2:", "");
                        let sym = raw_sym
                            .replace("XBTUSDTM", "BTCUSDT")
                            .replace("USDTM", "USDT");
                        if sym.is_empty() || !sym.ends_with("USDT") { continue; }
                        let d = &v["data"];
                        let bid: f64 = d["bestBidPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let ask: f64 = d["bestAskPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let price: f64 = d["price"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid + ask) / 2.0);
                        if price <= 0.0 { continue; }
                        upsert(price_map, "kucoin", sym, TickerData { bid, ask, price, vol: 0.0, funding_rate: 0.0, last_updated: 0 });
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => return Err(e.into()),
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                ws.send(Message::Text(json!({"id":"ping","type":"ping"}).to_string().into())).await?;
            }
        }
    }
    Ok(())
}

// ── BingX ─────────────────────────────────────────────────────────────────────
async fn bingx_handler(price_map: &PriceMap) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker").send().await?.json().await?;
    let symbols: Vec<String> = res["data"].as_array().unwrap_or(&vec![])
        .iter()
        .filter_map(|i| {
            let s = i["symbol"].as_str()?;
            if s.ends_with("-USDT") { Some(s.to_string()) } else { None }
        })
        .collect();
    info!("[bingx] Found {} USDT perps", symbols.len());

    let mut ws = connect_ws("wss://open-api-swap.bingx.com/swap-market").await?;
    info!("[bingx] Connected, subscribing...");

    // Bingx requires a string comma separated
    // Limit args per subscribe: BingX seems to support bulk but maybe not all 600 at once. Let's do 100
    for chunk in symbols.chunks(50) {
        let topic = chunk.iter().map(|s| format!("{}@ticker", s)).collect::<Vec<String>>().join(",");
        ws.send(Message::Text(json!({
            "id": "bingx1",
            "reqType": "sub",
            "dataType": topic
        }).to_string().into())).await?;
    }

    let mut ping_interval = tokio::time::interval(Duration::from_secs(20));

    loop {
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                        let items = if let Some(arr) = v["data"].as_array() {
                            arr.clone()
                        } else if let Some(obj) = v["data"].as_object() {
                            vec![v["data"].clone()]
                        } else {
                            vec![]
                        };
                        for item in items {
                            let raw_sym = item["s"].as_str().or_else(|| item["symbol"].as_str()).unwrap_or("");
                            let sym = raw_sym.replace('-', "");
                            if !sym.ends_with("USDT") { continue; }
                            let price: f64 = item["c"].as_str()
                                .or_else(|| item["lastPrice"].as_str())
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            let bid: f64 = item["b"].as_str()
                                .or_else(|| item["bidPrice"].as_str())
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(price);
                            let ask: f64 = item["a"].as_str()
                                .or_else(|| item["askPrice"].as_str())
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(price);
                            let vol: f64 = item["q"].as_str()
                                .or_else(|| item["quoteVolume"].as_str())
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            if price <= 0.0 { continue; }
                            upsert(price_map, "bingx", sym, TickerData { bid, ask, price, vol, funding_rate: 0.0, last_updated: 0 });
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        // BingX sends gzip-compressed JSON for bulk data
                        use std::io::Read;
                        let mut decoder = flate2::read::GzDecoder::new(bin.as_slice());
                        let mut text = String::new();
                        if decoder.read_to_string(&mut text).is_err() { continue; }
                        if text == "Ping" || text.contains("Ping") { ws.send(Message::Text("Pong".into())).await?; continue; }
                        if text.contains("Pong") { continue; }
                        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                        let items = if let Some(arr) = v["data"].as_array() {
                            arr.clone()
                        } else if let Some(obj) = v["data"].as_object() {
                            vec![v["data"].clone()]
                        } else {
                            vec![]
                        };
                        for item in items {
                            let raw_sym = item["s"].as_str().or_else(|| item["symbol"].as_str()).unwrap_or("");
                            let sym = raw_sym.replace('-', "");
                            if !sym.ends_with("USDT") { continue; }
                            let price: f64 = item["c"].as_str()
                                .or_else(|| item["lastPrice"].as_str())
                                .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            let bid: f64 = item["b"].as_str()
                                .or_else(|| item["bidPrice"].as_str())
                                .and_then(|s| s.parse().ok()).unwrap_or(price);
                            let ask: f64 = item["a"].as_str()
                                .or_else(|| item["askPrice"].as_str())
                                .and_then(|s| s.parse().ok()).unwrap_or(price);
                            let vol: f64 = item["q"].as_str()
                                .or_else(|| item["quoteVolume"].as_str())
                                .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                            if price <= 0.0 { continue; }
                            upsert(price_map, "bingx", sym, TickerData { bid, ask, price, vol, funding_rate: 0.0, last_updated: 0 });
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => return Err(e.into()),
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                ws.send(Message::Text("Ping".into())).await?;
            }
        }
    }
    Ok(())
}

// ─── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env if present
    let _ = dotenvy::dotenv();

    // Init tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env();
    info!(
        "Starting rust-collector | local_only={} push_interval={:?} min_vol={}M",
        config.local_only, config.push_interval, config.min_vol_m
    );
    if !config.local_only {
        info!("Pushing to: {}", config.worker_url);
    }

    let price_map: PriceMap = Arc::new(DashMap::new());

    // Spawn one task per exchange
    let _handles = vec![
        ws_task!("binance", price_map, |pm: &PriceMap| {
            let pm = Arc::clone(pm);
            async move { binance_handler(&pm).await }
        }),
        ws_task!("bybit", price_map, |pm: &PriceMap| {
            let pm = Arc::clone(pm);
            async move { bybit_handler(&pm).await }
        }),
        ws_task!("okx", price_map, |pm: &PriceMap| {
            let pm = Arc::clone(pm);
            async move { okx_handler(&pm).await }
        }),
        ws_task!("gate", price_map, |pm: &PriceMap| {
            let pm = Arc::clone(pm);
            async move { gate_handler(&pm).await }
        }),
        ws_task!("mexc", price_map, |pm: &PriceMap| {
            let pm = Arc::clone(pm);
            async move { mexc_handler(&pm).await }
        }),
        ws_task!("bitget", price_map, |pm: &PriceMap| {
            let pm = Arc::clone(pm);
            async move { bitget_handler(&pm).await }
        }),
        ws_task!("htx", price_map, |pm: &PriceMap| {
            let pm = Arc::clone(pm);
            async move { htx_handler(&pm).await }
        }),
        ws_task!("kucoin", price_map, |pm: &PriceMap| {
            let pm = Arc::clone(pm);
            async move { kucoin_handler(&pm).await }
        }),
        ws_task!("bingx", price_map, |pm: &PriceMap| {
            let pm = Arc::clone(pm);
            async move { bingx_handler(&pm).await }
        }),
    ];

    // Give exchanges a moment to receive their first batch of data
    info!("Waiting 3s for initial data...");
    sleep(Duration::from_secs(3)).await;

    let http = Client::builder().timeout(Duration::from_secs(30)).build()?;

    // Run the push loop on the main task (blocks forever)
    push_loop(price_map, config, http).await;

    Ok(())
}
