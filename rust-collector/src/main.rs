//! rust-collector – CEX arbitrage spread monitor (event-driven)
//!
//! Architecture:
//!   - Each exchange handler updates PriceMap and fires a Notify signal
//!   - push_loop reacts within 50ms (debounce) and pushes to paper/real traders
//!   - Cloudflare Worker gets a periodic push (PUSH_INTERVAL_SECS) for dashboard display

use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{sync::Notify, time::sleep};
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
    #[serde(skip)]
    pub last_updated: u64,
}

impl Default for TickerData {
    fn default() -> Self {
        Self { bid: 0.0, ask: 0.0, price: 0.0, vol: 0.0, funding_rate: 0.0, last_updated: 0 }
    }
}

type PriceMap    = Arc<DashMap<String, HashMap<String, TickerData>>>;
type UpdateSignal = Arc<Notify>;

#[derive(Debug, Clone, Serialize)]
struct SpreadRow {
    pair: String,
    #[serde(rename = "exchangeA")] exchange_a: String,
    #[serde(rename = "exchangeB")] exchange_b: String,
    #[serde(rename = "priceA")]    price_a: f64,
    #[serde(rename = "priceB")]    price_b: f64,
    #[serde(rename = "bidA")]      bid_a: f64,
    #[serde(rename = "askA")]      ask_a: f64,
    #[serde(rename = "bidB")]      bid_b: f64,
    #[serde(rename = "askB")]      ask_b: f64,
    #[serde(rename = "volA")]      vol_a: f64,
    #[serde(rename = "volB")]      vol_b: f64,
    #[serde(rename = "fundingA")]  funding_a: f64,
    #[serde(rename = "fundingB")]  funding_b: f64,
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
    paper_trader_url: String,
    paper_trader_token: String,
    real_trader_url: String,
    real_trader_token: String,
}

impl Config {
    fn from_env() -> Self {
        Config {
            worker_url: std::env::var("WORKER_URL")
                .unwrap_or_else(|_| "https://cex-spread-scanner.workers.dev".into()),
            token: std::env::var("COLLECTOR_TOKEN").unwrap_or_default(),
            local_only: std::env::var("LOCAL_ONLY")
                .map(|v| v.to_lowercase() == "true").unwrap_or(false),
            push_interval: Duration::from_secs(
                std::env::var("PUSH_INTERVAL_SECS").ok()
                    .and_then(|v| v.parse().ok()).unwrap_or(5)),
            min_vol_m: std::env::var("MIN_VOL_MILLIONS").ok()
                .and_then(|v| v.parse().ok()).unwrap_or(10.0),
            paper_trader_url: std::env::var("PAPER_TRADER_URL")
                .unwrap_or_else(|_| "http://localhost:8080".into()),
            paper_trader_token: std::env::var("PAPER_TRADER_TOKEN").unwrap_or_default(),
            real_trader_url: std::env::var("REAL_TRADER_URL")
                .unwrap_or_else(|_| "http://localhost:3002".into()),
            real_trader_token: std::env::var("REAL_TRADER_TOKEN").unwrap_or_default(),
        }
    }
}

// ─── Spread computation ───────────────────────────────────────────────────────

const SPREAD_THRESHOLD: f64 = -1.0;
const SPREAD_MAX: f64 = 50.0;
const TOP_N: usize = 1000;
const STALE_SECS: u64 = 30;
const INTRA_SPREAD_LIMIT: f64 = 1.02;

/// Tokens that are structurally decoupled across exchanges (different underlying assets,
/// political tokens, in-delisting, or commodities) and should never appear in arb scans.
const BLACKLIST: &[&str] = &[
    "USOILUSDT",  // commodity, not a crypto derivative
    "XMRUSDT",    // Monero — delisted on most CEXes, extreme spread
    "TRUMPUSDT",  // political meme, price decoupled per exchange
    "LUNAUSDT",   // collapsed asset, legacy remnants
    "BTTUSDT",    // BitTorrent rebrand — different contracts per exchange
    "BTCSTUSDT",  // Binance liquid staking token, not available everywhere
];

/// Maximum allowed absolute difference between funding rates on two exchanges (per 8h).
/// If > this value, the spread is likely a "kimchi premium" / one-sided squeeze, not arb.
const MAX_FUNDING_DIFF: f64 = 0.005; // 0.5% per 8h

fn compute_spreads(price_map: &PriceMap, min_vol_m: f64) -> Vec<SpreadRow> {
    let exchanges: Vec<String> = price_map.iter().map(|e| e.key().clone()).collect();
    let mut rows = Vec::new();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();

    for i in 0..exchanges.len() {
        for j in (i + 1)..exchanges.len() {
            let name_a = &exchanges[i];
            let name_b = &exchanges[j];
            let map_a = match price_map.get(name_a) { Some(m) => m, None => continue };
            let map_b = match price_map.get(name_b) { Some(m) => m, None => continue };

            for (sym, data_a) in map_a.iter() {
                let data_b = match map_b.get(sym.as_str()) { Some(d) => d, None => continue };

                // ── Blacklist filter ─────────────────────────────────────────
                if BLACKLIST.iter().any(|bl| sym.eq_ignore_ascii_case(bl)) { continue; }

                // ── Stale data filter ────────────────────────────────────────
                if data_a.last_updated > 0 && now_secs.saturating_sub(data_a.last_updated) > STALE_SECS { continue; }
                if data_b.last_updated > 0 && now_secs.saturating_sub(data_b.last_updated) > STALE_SECS { continue; }

                // ── Valid quotes ─────────────────────────────────────────────
                if data_a.bid <= 0.0 || data_a.ask <= 0.0 || data_b.bid <= 0.0 || data_b.ask <= 0.0 { continue; }
                if data_a.ask > data_a.bid * INTRA_SPREAD_LIMIT || data_b.ask > data_b.bid * INTRA_SPREAD_LIMIT { continue; }

                // ── Volume filter (strict: 0 vol = no data = skip) ───────────
                let min_v = min_vol_m * 1_000_000.0;
                if data_a.vol <= 0.0 || data_a.vol < min_v || data_b.vol <= 0.0 || data_b.vol < min_v { continue; }

                // ── Funding rate divergence filter ────────────────────────────
                // If both exchanges report non-zero funding, check for extreme divergence.
                // A large funding diff signals a structurally one-sided market (kimchi premium),
                // not a real arbitrage opportunity.
                if data_a.funding_rate != 0.0 && data_b.funding_rate != 0.0 {
                    let funding_diff = (data_a.funding_rate - data_b.funding_rate).abs();
                    if funding_diff > MAX_FUNDING_DIFF { continue; }
                }

                let a_over_b = (data_a.bid - data_b.ask) / data_b.ask * 100.0;
                let b_over_a = (data_b.bid - data_a.ask) / data_a.ask * 100.0;
                let (spread, direction) = if a_over_b >= b_over_a {
                    (a_over_b, format!("{name_a}>{name_b}"))
                } else {
                    (b_over_a, format!("{name_b}>{name_a}"))
                };

                if spread < SPREAD_THRESHOLD || spread > SPREAD_MAX { continue; }
                rows.push(SpreadRow {
                    pair: sym.clone(),
                    exchange_a: name_a.clone(), exchange_b: name_b.clone(),
                    price_a: data_a.price, price_b: data_b.price,
                    bid_a: data_a.bid, ask_a: data_a.ask,
                    bid_b: data_b.bid, ask_b: data_b.ask,
                    vol_a: data_a.vol, vol_b: data_b.vol,
                    funding_a: data_a.funding_rate, funding_b: data_b.funding_rate,
                    spread: (spread * 10000.0).round() / 10000.0, direction,
                });
            }
        }
    }
    rows.sort_by(|a, b| b.spread.partial_cmp(&a.spread).unwrap_or(std::cmp::Ordering::Equal));
    rows.truncate(TOP_N);
    rows
}

#[inline] fn safe_f64(v: f64) -> f64 { if v.is_finite() { v } else { 0.0 } }

fn build_maps_payload(price_map: &PriceMap) -> Value {
    let mut out = serde_json::Map::new();
    for entry in price_map.iter() {
        let ex = entry.key().clone();
        let syms: Vec<Value> = entry.value().iter().map(|(s, _)| Value::String(s.clone())).collect();
        out.insert(ex, Value::Array(syms));
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
                "bid": safe_f64(data.bid), "ask": safe_f64(data.ask),
                "price": safe_f64(data.price), "vol": safe_f64(data.vol),
                "fundingRate": safe_f64(data.funding_rate)
            }));
        }
        out.insert(ex, Value::Object(sym_map));
    }
    Value::Object(out)
}

// ─── Push helpers ─────────────────────────────────────────────────────────────

/// Push spreads to paper-trader and real-trader (event-driven, low-latency path).
async fn push_to_traders(spreads: &[SpreadRow], price_map: &PriceMap, config: &Config, http: &Client) {
    let full_maps = build_full_maps_payload(price_map);

    if !config.paper_trader_url.is_empty() && !config.paper_trader_token.is_empty() {
        let pt_url = format!("{}/internal/spreads", config.paper_trader_url.trim_end_matches('/'));
        let body = json!({ "spreads": spreads, "maps": full_maps });
        let http2 = http.clone();
        let token2 = config.paper_trader_token.clone();
        let n = spreads.len();
        tokio::spawn(async move {
            match http2.post(&pt_url).bearer_auth(&token2).json(&body)
                .timeout(Duration::from_secs(5)).send().await {
                Ok(r) if r.status().is_success() => debug!("paper-trader <- {} spreads OK", n),
                Ok(r) => { let s=r.status(); let b=r.text().await.unwrap_or_default(); warn!("paper-trader push failed {}: {}", s, b); }
                Err(e) => warn!("paper-trader push error: {}", e),
            }
        });
    }

    if !config.real_trader_url.is_empty() && !config.real_trader_token.is_empty() {
        let rt_url = format!("{}/internal/spreads", config.real_trader_url.trim_end_matches('/'));
        let body = json!({ "spreads": spreads, "maps": full_maps });
        let http3 = http.clone();
        let token3 = config.real_trader_token.clone();
        let n = spreads.len();
        tokio::spawn(async move {
            match http3.post(&rt_url).bearer_auth(&token3).json(&body)
                .timeout(Duration::from_secs(5)).send().await {
                Ok(r) if r.status().is_success() => debug!("real-trader <- {} spreads OK", n),
                Ok(r) => { let s=r.status(); let b=r.text().await.unwrap_or_default(); warn!("real-trader push failed {}: {}", s, b); }
                Err(e) => warn!("real-trader push error: {}", e),
            }
        });
    }
}

/// Push snapshot to Cloudflare Worker (periodic, for dashboard display).
async fn push_to_worker(spreads: &[SpreadRow], price_map: &PriceMap, config: &mut Config, http: &Client, interval: &mut tokio::time::Interval) {
    let maps = build_maps_payload(price_map);
    let body = json!({ "spreads": spreads, "maps": maps });
    let url = format!("{}/api/update", config.worker_url.trim_end_matches('/'));
    match http.post(&url).bearer_auth(&config.token).json(&body)
        .timeout(Duration::from_secs(15)).send().await {
        Ok(resp) => {
            let status = resp.status();
            if status.is_success() {
                if let Ok(json) = resp.json::<Value>().await {
                    if let Some(secs) = json.get("config")
                        .and_then(|c| c.get("push_interval")).and_then(|v| v.as_u64()) {
                        if secs > 0 && secs != config.push_interval.as_secs() {
                            info!("Updating push_interval {}s -> {}s", config.push_interval.as_secs(), secs);
                            config.push_interval = Duration::from_secs(secs);
                            *interval = tokio::time::interval(config.push_interval);
                            interval.tick().await;
                        }
                    }
                }
                info!("Pushed {} spreads -> worker {}", spreads.len(), status);
            } else {
                let b = resp.text().await.unwrap_or_default();
                error!("Worker push failed {}: {}", status, b);
            }
        }
        Err(e) => error!("Worker push error: {}", e),
    }
}

// ─── Push loop (event-driven) ─────────────────────────────────────────────────

async fn push_loop(price_map: PriceMap, signal: UpdateSignal, mut config: Config, http: Client) {
    if config.local_only {
        // Local-only mode: just print every second
        loop {
            sleep(Duration::from_secs(1)).await;
            let spreads = compute_spreads(&price_map, config.min_vol_m);
            for row in spreads.iter().take(20) {
                println!("{:<10} {:>6.3}%  {} vs {}  bid_a={:.4} ask_b={:.4}",
                    row.pair, row.spread, row.exchange_a, row.exchange_b, row.bid_a, row.ask_b);
            }
            println!("--- {} spreads ---\n", spreads.len());
        }
    }

    if config.token.is_empty() { warn!("COLLECTOR_TOKEN is empty — worker push disabled"); }

    let mut worker_interval = tokio::time::interval(config.push_interval);
    worker_interval.tick().await; // skip immediate first tick

    // Debounce window: after first signal, wait up to 50ms for more updates
    // then compute once with the freshest data.
    const DEBOUNCE_MS: u64 = 50;

    loop {
        tokio::select! {
            // ── Event-driven path: new data arrived ──────────────────────────
            _ = signal.notified() => {
                // Debounce: collect all updates in a short window
                sleep(Duration::from_millis(DEBOUNCE_MS)).await;

                let spreads = compute_spreads(&price_map, config.min_vol_m);
                push_to_traders(&spreads, &price_map, &config, &http).await;

                let mut ex_counts: Vec<String> = price_map.iter()
                    .map(|e| format!("{}={}", e.key(), e.value().len())).collect();
                ex_counts.sort();
                debug!("Event push: {} spreads | map: [{}]", spreads.len(), ex_counts.join(", "));
            }

            // ── Periodic path: push to Cloudflare Worker for dashboard ───────
            _ = worker_interval.tick() => {
                if config.token.is_empty() { continue; }
                let spreads = compute_spreads(&price_map, config.min_vol_m);
                let mut ex_counts: Vec<String> = price_map.iter()
                    .map(|e| format!("{}={}", e.key(), e.value().len())).collect();
                ex_counts.sort();
                info!("Worker push: {} spreads | map: [{}]", spreads.len(), ex_counts.join(", "));
                push_to_worker(&spreads, &price_map, &mut config, &http, &mut worker_interval).await;
            }
        }
    }
}

// ─── upsert ───────────────────────────────────────────────────────────────────

fn upsert(price_map: &PriceMap, signal: &UpdateSignal, exchange: &str, sym: String, mut data: TickerData) {
    data.last_updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    price_map.entry(exchange.to_string()).or_default().insert(sym, data);
    signal.notify_one(); // wake event-driven push loop
}

// ─── WebSocket connect helper ─────────────────────────────────────────────────

async fn connect_ws(url: &str) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
> {
    let (ws, _) = connect_async_tls_with_config(url, None, false, None)
        .await.context("WS connect failed")?;
    Ok(ws)
}

macro_rules! ws_task {
    ($name:expr, $price_map:expr, $signal:expr, $body:expr) => {{
        let price_map = Arc::clone(&$price_map);
        let signal    = Arc::clone(&$signal);
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                info!("[{}] Connecting...", $name);
                match $body(&price_map, &signal).await {
                    Ok(_)  => warn!("[{}] WS closed, reconnecting...", $name),
                    Err(e) => error!("[{}] Error: {}. Reconnecting in {:?}", $name, e, backoff),
                }
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        })
    }};
}

// ── Binance ───────────────────────────────────────────────────────────────────
async fn binance_handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let mut ws = connect_ws("wss://fstream.binance.com/ws/!ticker@arr").await?;
    info!("[binance] Connected");
    while let Some(msg) = ws.next().await {
        if let Ok(Message::Text(text)) = msg {
            let arr: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            for item in arr.as_array().unwrap_or(&vec![]) {
                let sym = item["s"].as_str().unwrap_or("").to_string();
                if sym.is_empty() || !sym.ends_with("USDT") { continue; }
                let bid: f64 = match item["b"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                let ask: f64 = match item["a"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                let price = item["c"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid + ask) / 2.0);
                let vol   = item["q"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                upsert(price_map, signal, "binance", sym, TickerData { bid, ask, price, vol, funding_rate: 0.0, last_updated: 0 });
            }
        }
    }
    Ok(())
}

// ── Bybit ─────────────────────────────────────────────────────────────────────
async fn bybit_handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://api.bybit.com/v5/market/tickers?category=linear").send().await?.json().await?;
    let symbols: Vec<String> = res["result"]["list"].as_array().unwrap_or(&vec![])
        .iter().filter_map(|i| { let s = i["symbol"].as_str()?; if s.ends_with("USDT") { Some(s.to_string()) } else { None } }).collect();
    info!("[bybit] Found {} USDT perps", symbols.len());
    let mut ws = connect_ws("wss://stream.bybit.com/v5/public/linear").await?;
    for chunk in symbols.chunks(10) {
        let args: Vec<String> = chunk.iter().map(|s| format!("tickers.{}", s)).collect();
        ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string().into())).await?;
    }
    let mut ping = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    if !v["topic"].as_str().map(|t| t.starts_with("tickers.")).unwrap_or(false) { continue; }
                    let d = &v["data"];
                    let sym = d["symbol"].as_str().unwrap_or("").to_string();
                    if sym.is_empty() || !sym.ends_with("USDT") { continue; }
                    let bid: f64 = match d["bid1Price"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                    let ask: f64 = match d["ask1Price"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                    let price   = d["lastPrice"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
                    let vol     = d["turnover24h"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let funding = d["fundingRate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    upsert(price_map, signal, "bybit", sym, TickerData { bid, ask, price, vol, funding_rate: funding, last_updated: 0 });
                }
                Some(Ok(Message::Ping(p))) => { ws.send(Message::Pong(p)).await?; }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => { ws.send(Message::Text(json!({"op":"ping"}).to_string().into())).await?; }
        }
    }
    Ok(())
}

// ── OKX ───────────────────────────────────────────────────────────────────────
async fn okx_handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://www.okx.com/api/v5/public/instruments?instType=SWAP").send().await?.json().await?;
    let symbols: Vec<String> = res["data"].as_array().unwrap_or(&vec![])
        .iter().filter_map(|i| { let s = i["instId"].as_str()?; if s.ends_with("-USDT-SWAP") { Some(s.to_string()) } else { None } }).collect();
    info!("[okx] Found {} USDT perps", symbols.len());
    let mut ws = connect_ws("wss://ws.okx.com:8443/ws/v5/public").await?;
    for chunk in symbols.chunks(10) {
        let args: Vec<Value> = chunk.iter().map(|s| json!({"channel":"tickers","instId":s})).collect();
        ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string().into())).await?;
    }
    let mut ping = tokio::time::interval(Duration::from_secs(25));
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    if text == "pong" { continue; }
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    for item in v["data"].as_array().unwrap_or(&vec![]) {
                        let inst_id = item["instId"].as_str().unwrap_or("");
                        let parts: Vec<&str> = inst_id.split('-').collect();
                        if parts.len() < 2 { continue; }
                        let sym = format!("{}{}", parts[0], parts[1]);
                        if !sym.ends_with("USDT") { continue; }
                        let bid: f64 = match item["bidPx"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                        let ask: f64 = match item["askPx"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                        let price   = item["last"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
                        let vol     = item["volCcy24h"].as_str().and_then(|s| s.parse::<f64>().ok()).map(|v| v*price).unwrap_or(0.0);
                        let funding = item["fundingRate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        upsert(price_map, signal, "okx", sym, TickerData { bid, ask, price, vol, funding_rate: funding, last_updated: 0 });
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => { ws.send(Message::Text("ping".into())).await?; }
        }
    }
    Ok(())
}

// ── Gate.io ───────────────────────────────────────────────────────────────────
async fn gate_handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let mut ws = connect_ws("wss://fx-ws.gateio.ws/v4/ws/usdt").await?;
    info!("[gate] Connected");
    ws.send(Message::Text(json!({"time":0,"channel":"futures.tickers","event":"subscribe","payload":["!all"]}).to_string().into())).await?;
    while let Some(msg) = ws.next().await {
        if let Ok(Message::Text(text)) = msg {
            let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            if v["channel"].as_str() != Some("futures.tickers") { continue; }
            for item in v["result"].as_array().unwrap_or(&vec![]) {
                let sym = item["contract"].as_str().unwrap_or("").replace('_', "");
                if !sym.ends_with("USDT") { continue; }
                let bid: f64 = match item["highest_bid"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                let ask: f64 = match item["lowest_ask"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                let price   = item["last"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
                let vol     = item["volume_24h_quote"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                let funding = item["funding_rate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                upsert(price_map, signal, "gate", sym, TickerData { bid, ask, price, vol, funding_rate: funding, last_updated: 0 });
            }
        }
    }
    Ok(())
}

// ── MEXC ──────────────────────────────────────────────────────────────────────
async fn mexc_handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://contract.mexc.com/api/v1/contract/detail").send().await?.json().await?;
    let symbols: Vec<String> = res["data"].as_array().unwrap_or(&vec![])
        .iter().filter_map(|i| { let s = i["symbol"].as_str()?; if s.ends_with("_USDT") { Some(s.to_string()) } else { None } }).collect();
    info!("[mexc] Found {} USDT perps", symbols.len());
    let mut ws = connect_ws("wss://contract.mexc.com/edge").await?;
    for sym in &symbols {
        ws.send(Message::Text(json!({"method":"sub.ticker","param":{"symbol":sym}}).to_string().into())).await?;
    }
    let mut ping = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    let ch = v["channel"].as_str().unwrap_or("");
                    if ch == "push.ticker" || ch == "push.ticker.batch" || ch == "push.symbol" {
                        let d = &v["data"];
                        let sym = d["symbol"].as_str().unwrap_or("").replace('_', "");
                        if !sym.ends_with("USDT") { continue; }
                        let bid: f64 = match d["bid1"].as_f64().or_else(|| d["bidPrice"].as_f64()) { Some(v) if v > 0.0 => v, _ => continue };
                        let ask: f64 = match d["ask1"].as_f64().or_else(|| d["askPrice"].as_f64()) { Some(v) if v > 0.0 => v, _ => continue };
                        let price   = d["lastPrice"].as_f64().unwrap_or((bid+ask)/2.0);
                        let vol     = d["amount24"].as_f64().unwrap_or(0.0);
                        let funding = d["fundingRate"].as_f64().unwrap_or(0.0);
                        upsert(price_map, signal, "mexc", sym, TickerData { bid, ask, price, vol, funding_rate: funding, last_updated: 0 });
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => { ws.send(Message::Text(json!({"method":"ping"}).to_string().into())).await?; }
        }
    }
    Ok(())
}

// ── Bitget ─────────────────────────────────────────────────────────────────────
async fn bitget_handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES").send().await?.json().await?;
    let symbols: Vec<String> = res["data"].as_array().unwrap_or(&vec![])
        .iter().filter_map(|i| { let s = i["symbol"].as_str()?; if s.ends_with("USDT") { Some(s.to_string()) } else { None } }).collect();
    info!("[bitget] Found {} USDT perps", symbols.len());
    let mut ws = connect_ws("wss://ws.bitget.com/v2/ws/public").await?;
    for chunk in symbols.chunks(10) {
        let args: Vec<Value> = chunk.iter().map(|s| json!({"instType":"USDT-FUTURES","channel":"ticker","instId":s})).collect();
        ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string().into())).await?;
    }
    let mut ping = tokio::time::interval(Duration::from_secs(25));
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    if text == "pong" { continue; }
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    for item in v["data"].as_array().unwrap_or(&vec![]) {
                        let sym = item["instId"].as_str().unwrap_or("").to_string();
                        if !sym.ends_with("USDT") { continue; }
                        let bid: f64 = match item["bidPr"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                        let ask: f64 = match item["askPr"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                        let price   = item["lastPr"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
                        let vol     = item["usdtVolume"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let funding = item["fundingRate"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        upsert(price_map, signal, "bitget", sym, TickerData { bid, ask, price, vol, funding_rate: funding, last_updated: 0 });
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => { ws.send(Message::Text("ping".into())).await?; }
        }
    }
    Ok(())
}

// ── HTX (Huobi) ───────────────────────────────────────────────────────────────
async fn htx_handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://api.hbdm.com/linear-swap-api/v1/swap_contract_info").send().await?.json().await?;
    let symbols: Vec<String> = res["data"].as_array().unwrap_or(&vec![])
        .iter().filter_map(|i| {
            let s = i["contract_code"].as_str()?;
            let status = i["contract_status"].as_i64().unwrap_or(0);
            if s.ends_with("-USDT") && status == 1 { Some(s.to_string()) } else { None }
        }).collect();
    info!("[htx] Found {} USDT perps", symbols.len());
    let mut ws = connect_ws("wss://api.hbdm.com/linear-swap-ws").await?;
    for sym in &symbols {
        ws.send(Message::Text(json!({"sub":format!("market.{}.bbo",sym),"id":format!("bbo_{}",sym)}).to_string().into())).await?;
        ws.send(Message::Text(json!({"sub":format!("market.{}.detail",sym),"id":format!("det_{}",sym)}).to_string().into())).await?;
    }
    loop {
        match ws.next().await {
            Some(Ok(Message::Binary(bin))) => {
                use std::io::Read;
                let mut dec = flate2::read::GzDecoder::new(bin.as_slice());
                let mut text = String::new();
                if dec.read_to_string(&mut text).is_err() { continue; }
                let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                if let Some(ts) = v["ping"].as_i64() {
                    ws.send(Message::Text(json!({"pong":ts}).to_string().into())).await?;
                    continue;
                }
                let ch = match v["ch"].as_str() { Some(c) => c, None => continue };
                let parts: Vec<&str> = ch.splitn(3, '.').collect();
                if parts.len() < 3 { continue; }
                let sym = parts[1].replace('-', "");
                if !sym.ends_with("USDT") { continue; }
                let tick = &v["tick"];
                if parts[2] == "bbo" {
                    let bid: f64 = match tick["bid"].as_f64() { Some(v) if v > 0.0 => v, _ => continue };
                    let ask: f64 = match tick["ask"].as_f64() { Some(v) if v > 0.0 => v, _ => continue };
                    let existing_vol = price_map.get("htx").and_then(|m| m.get(&sym).map(|d| d.vol)).unwrap_or(0.0);
                    upsert(price_map, signal, "htx", sym, TickerData { bid, ask, price: (bid+ask)/2.0, vol: existing_vol, funding_rate: 0.0, last_updated: 0 });
                } else if parts[2] == "detail" {
                    let close = tick["close"].as_f64().unwrap_or(0.0);
                    if close <= 0.0 { continue; }
                    let vol = tick["vol"].as_f64().unwrap_or(0.0) * close;
                    let existing = price_map.get("htx").and_then(|m| m.get(&sym).cloned());
                    if let Some(ref d) = existing {
                        if d.bid > 0.0 && d.ask > 0.0 {
                            upsert(price_map, signal, "htx", sym, TickerData { bid: d.bid, ask: d.ask, price: d.price, vol, funding_rate: 0.0, last_updated: 0 });
                        }
                    }
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
async fn kucoin_handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    let contracts: Value = http.get("https://api-futures.kucoin.com/api/v1/contracts/active")
        .timeout(Duration::from_secs(10)).send().await?.json().await?;
    let symbols: Vec<String> = contracts["data"].as_array().unwrap_or(&vec![])
        .iter().filter_map(|c| { let s = c["symbol"].as_str()?; if s.ends_with("USDTM") { Some(s.to_string()) } else { None } }).collect();
    info!("[kucoin] Found {} USDT-M contracts", symbols.len());

    // Seed volume from REST (WS tickerV2 doesn't carry volume)
    for sym in &symbols {
        let normalized = sym.replace("XBTUSDTM", "BTCUSDT").replace("USDTM", "USDT");
        let url = format!("https://api-futures.kucoin.com/api/v1/ticker?symbol={}", sym);
        if let Ok(resp) = http.get(&url).timeout(Duration::from_secs(5)).send().await {
            if let Ok(data) = resp.json::<Value>().await {
                let vol_usd = data["data"]["turnoverOf24h"].as_f64().unwrap_or(0.0);
                if vol_usd > 0.0 {
                    let mut entry = price_map.entry("kucoin".to_string()).or_default();
                    entry.entry(normalized).or_insert_with(|| TickerData { vol: vol_usd, ..Default::default() }).vol = vol_usd;
                }
            }
        }
    }

    let resp: Value = http.post("https://api-futures.kucoin.com/api/v1/bullet-public").send().await?.json().await?;
    let token    = resp["data"]["token"].as_str().ok_or_else(|| anyhow!("No KuCoin WS token"))?;
    let endpoint = resp["data"]["instanceServers"][0]["endpoint"].as_str().unwrap_or("wss://ws-api-futures.kucoin.com");
    let mut ws   = connect_ws(&format!("{}?token={}&connectId=rustcollector", endpoint, token)).await?;
    info!("[kucoin] Connected, subscribing...");
    for chunk in symbols.chunks(100) {
        let topic = format!("/contractMarket/tickerV2:{}", chunk.join(","));
        ws.send(Message::Text(json!({"id":"sub","type":"subscribe","topic":topic,"privateChannel":false,"response":true}).to_string().into())).await?;
    }

    let mut ping = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    if v["type"].as_str() != Some("message") { continue; }
                    let raw = v["topic"].as_str().unwrap_or("").replace("/contractMarket/tickerV2:", "");
                    let sym = raw.replace("XBTUSDTM", "BTCUSDT").replace("USDTM", "USDT");
                    if sym.is_empty() || !sym.ends_with("USDT") { continue; }
                    let d = &v["data"];
                    let bid: f64 = match d["bestBidPrice"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                    let ask: f64 = match d["bestAskPrice"].as_str().and_then(|s| s.parse().ok()) { Some(v) if v > 0.0 => v, _ => continue };
                    let price = d["price"].as_str().and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
                    let existing_vol = price_map.get("kucoin").and_then(|m| m.get(&sym).map(|d| d.vol)).unwrap_or(0.0);
                    upsert(price_map, signal, "kucoin", sym, TickerData { bid, ask, price, vol: existing_vol, funding_rate: 0.0, last_updated: 0 });
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => { ws.send(Message::Text(json!({"id":"ping","type":"ping"}).to_string().into())).await?; }
        }
    }
    Ok(())
}

// ── BingX ─────────────────────────────────────────────────────────────────────
async fn bingx_handler(price_map: &PriceMap, signal: &UpdateSignal) -> Result<()> {
    let http = Client::new();
    let res: Value = http.get("https://open-api.bingx.com/openApi/swap/v2/quote/ticker").send().await?.json().await?;
    let symbols: Vec<String> = res["data"].as_array().unwrap_or(&vec![])
        .iter().filter_map(|i| { let s = i["symbol"].as_str()?; if s.ends_with("-USDT") { Some(s.to_string()) } else { None } }).collect();
    info!("[bingx] Found {} USDT perps", symbols.len());
    let mut ws = connect_ws("wss://open-api-swap.bingx.com/swap-market").await?;
    for chunk in symbols.chunks(50) {
        let topic = chunk.iter().map(|s| format!("{}@ticker", s)).collect::<Vec<_>>().join(",");
        ws.send(Message::Text(json!({"id":"bingx1","reqType":"sub","dataType":topic}).to_string().into())).await?;
    }

    let parse_bingx_item = |item: &Value| -> Option<(String, TickerData)> {
        let raw = item["s"].as_str().or_else(|| item["symbol"].as_str()).unwrap_or("");
        let sym = raw.replace('-', "");
        if !sym.ends_with("USDT") { return None; }
        let bid = match item["b"].as_str().or_else(|| item["bidPrice"].as_str()).and_then(|s| s.parse::<f64>().ok()) { Some(v) if v > 0.0 => v, _ => return None };
        let ask = match item["a"].as_str().or_else(|| item["askPrice"].as_str()).and_then(|s| s.parse::<f64>().ok()) { Some(v) if v > 0.0 => v, _ => return None };
        let price = item["c"].as_str().or_else(|| item["lastPrice"].as_str()).and_then(|s| s.parse().ok()).unwrap_or((bid+ask)/2.0);
        let vol   = item["q"].as_str().or_else(|| item["quoteVolume"].as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
        Some((sym, TickerData { bid, ask, price, vol, funding_rate: 0.0, last_updated: 0 }))
    };

    let mut ping = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    let items = if let Some(a) = v["data"].as_array() { a.clone() } else if v["data"].is_object() { vec![v["data"].clone()] } else { vec![] };
                    for item in &items { if let Some((sym, td)) = parse_bingx_item(item) { upsert(price_map, signal, "bingx", sym, td); } }
                }
                Some(Ok(Message::Binary(bin))) => {
                    use std::io::Read;
                    let mut dec = flate2::read::GzDecoder::new(bin.as_slice());
                    let mut text = String::new();
                    if dec.read_to_string(&mut text).is_err() { continue; }
                    if text.contains("Ping") { ws.send(Message::Text("Pong".into())).await?; continue; }
                    let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    let items = if let Some(a) = v["data"].as_array() { a.clone() } else if v["data"].is_object() { vec![v["data"].clone()] } else { vec![] };
                    for item in &items { if let Some((sym, td)) = parse_bingx_item(item) { upsert(price_map, signal, "bingx", sym, td); } }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => { ws.send(Message::Text("Ping".into())).await?; }
        }
    }
    Ok(())
}

// ─── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();

    let config = Config::from_env();
    info!("Starting rust-collector (event-driven) | local_only={} worker_interval={:?} min_vol={}M",
        config.local_only, config.push_interval, config.min_vol_m);

    let price_map: PriceMap    = Arc::new(DashMap::new());
    let signal:    UpdateSignal = Arc::new(Notify::new());

    let _handles = vec![
        ws_task!("binance", price_map, signal, |pm: &PriceMap, sig: &UpdateSignal| { let (pm, sig) = (Arc::clone(pm), Arc::clone(sig)); async move { binance_handler(&pm, &sig).await } }),
        ws_task!("bybit",   price_map, signal, |pm: &PriceMap, sig: &UpdateSignal| { let (pm, sig) = (Arc::clone(pm), Arc::clone(sig)); async move { bybit_handler(&pm, &sig).await } }),
        ws_task!("okx",     price_map, signal, |pm: &PriceMap, sig: &UpdateSignal| { let (pm, sig) = (Arc::clone(pm), Arc::clone(sig)); async move { okx_handler(&pm, &sig).await } }),
        ws_task!("gate",    price_map, signal, |pm: &PriceMap, sig: &UpdateSignal| { let (pm, sig) = (Arc::clone(pm), Arc::clone(sig)); async move { gate_handler(&pm, &sig).await } }),
        ws_task!("mexc",    price_map, signal, |pm: &PriceMap, sig: &UpdateSignal| { let (pm, sig) = (Arc::clone(pm), Arc::clone(sig)); async move { mexc_handler(&pm, &sig).await } }),
        ws_task!("bitget",  price_map, signal, |pm: &PriceMap, sig: &UpdateSignal| { let (pm, sig) = (Arc::clone(pm), Arc::clone(sig)); async move { bitget_handler(&pm, &sig).await } }),
        ws_task!("htx",     price_map, signal, |pm: &PriceMap, sig: &UpdateSignal| { let (pm, sig) = (Arc::clone(pm), Arc::clone(sig)); async move { htx_handler(&pm, &sig).await } }),
        ws_task!("kucoin",  price_map, signal, |pm: &PriceMap, sig: &UpdateSignal| { let (pm, sig) = (Arc::clone(pm), Arc::clone(sig)); async move { kucoin_handler(&pm, &sig).await } }),
        ws_task!("bingx",   price_map, signal, |pm: &PriceMap, sig: &UpdateSignal| { let (pm, sig) = (Arc::clone(pm), Arc::clone(sig)); async move { bingx_handler(&pm, &sig).await } }),
    ];

    info!("Waiting 5s for initial data...");
    sleep(Duration::from_secs(5)).await;

    let http = Client::builder().timeout(Duration::from_secs(30)).build()?;
    push_loop(price_map, signal, config, http).await;
    Ok(())
}
