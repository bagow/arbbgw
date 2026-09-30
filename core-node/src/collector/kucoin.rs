//! KuCoin Futures WebSocket collector
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio_tungstenite::{
    client_async_tls, connect_async_tls_with_config, tungstenite::Message,
};
use tracing::{info, warn};

use crate::state::{L2OrderbookMap, L2Snapshot, PriceMap, TickerData, UpdateSignal};

static PRO_DEPTH_UPDATED_MS: OnceLock<dashmap::DashMap<String, i64>> = OnceLock::new();
const CLASSIC_SYMBOLS_PER_SHARD: usize = 150;
const CLASSIC_DEPTH_BASELINE_SYMBOL: &str = "XBTUSDTM";

fn is_classic_depth_baseline(symbol: &str) -> bool {
    symbol == CLASSIC_DEPTH_BASELINE_SYMBOL
}

fn pro_depth_updates() -> &'static dashmap::DashMap<String, i64> {
    PRO_DEPTH_UPDATED_MS.get_or_init(dashmap::DashMap::new)
}

/// Entry must never silently fall back to the 100 ms Classic depth stream.
pub fn pro_depth_is_fresh(pair: &str, now_ms: i64) -> bool {
    pro_depth_updates()
        .get(pair)
        .is_some_and(|updated| (0..=750).contains(&now_ms.saturating_sub(*updated)))
}

pub async fn run(
    prices: PriceMap,
    signal: UpdateSignal,
    l2_books: L2OrderbookMap,
    l2_wanted: std::sync::Arc<dashmap::DashSet<String>>,
    settings: std::sync::Arc<tokio::sync::RwLock<Vec<crate::settings::TradingSettings>>>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let is_disabled = {
            let p = settings.read().await;
            !p.is_empty()
                && p.iter()
                    .all(|x| x.disabled_exchanges.contains(&"kucoin".to_string()))
        };
        if is_disabled {
            if backoff == Duration::from_secs(1) {
                info!("[kucoin] Disabled globally in profiles. Sleeping...");
            }
            backoff = Duration::from_secs(5);
            tokio::time::sleep(backoff).await;
            continue;
        }

        info!("[kucoin] Connecting...");

        let disable_watcher = async {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let is_disabled = {
                    let p = settings.read().await;
                    !p.is_empty()
                        && p.iter()
                            .all(|x| x.disabled_exchanges.contains(&"kucoin".to_string()))
                };
                if is_disabled {
                    break;
                }
            }
        };

        tokio::select! {
            res = handler(&prices, &signal, &l2_books, &l2_wanted) => {
                match res {
                    Ok(_) => {
                        // `handler` normally returns after its planned instrument-universe
                        // refresh.  Do not add the failure backoff to that deterministic
                        // rotation: all Classic and Pro streams are already down while the
                        // next generation is being created.
                        info!("[kucoin] Refreshing instrument universe immediately...");
                        backoff = Duration::from_secs(1);
                        continue;
                    }
                    Err(e) => {
                        warn!("[kucoin] WS disconnected: {}. Reconnecting in {:?}", e, backoff);
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
            _ = disable_watcher => {
                info!("[kucoin] Exchange was disabled dynamically. Dropping connection.");
                backoff = Duration::from_secs(1);
            }
        }
    }
}

async fn handler(
    price_map: &PriceMap,
    signal: &UpdateSignal,
    l2_books: &L2OrderbookMap,
    l2_wanted: &std::sync::Arc<dashmap::DashSet<String>>,
) -> Result<()> {
    let http = Client::builder().user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36").build().unwrap();
    let contracts: Value = http
        .get("https://api-futures.kucoin.com/api/v1/contracts/active")
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .json()
        .await?;
    let mut symbols: Vec<String> = Vec::new();
    let mut sym_map = std::collections::HashMap::new();

    for c in contracts["data"].as_array().unwrap_or(&vec![]) {
        if let Some(s) = c["symbol"].as_str() {
            if s.ends_with("USDTM") {
                symbols.push(s.to_string());
                let normalized = s.replace("XBTUSDTM", "BTCUSDT").replace("USDTM", "USDT");
                let mult = c["multiplier"].as_f64().unwrap_or(1.0);
                sym_map.insert(s.to_string(), (normalized.clone(), mult));

                let vol_usd = c["turnoverOf24h"].as_f64().unwrap_or(0.0);
                if vol_usd > 0.0 {
                    let mut entry = price_map.entry("kucoin".to_string()).or_default();
                    entry
                        .entry(normalized)
                        .or_insert_with(|| TickerData {
                            vol: vol_usd,
                            ..Default::default()
                        })
                        .vol = vol_usd;
                }
            }
        }
    }
    info!(
        "[kucoin] Found {} USDT-M contracts (volumes seeded)",
        symbols.len()
    );

    // Discovery needs only top-of-book ticker data. Keep the full universe on a
    // small number of sockets and reserve depth bandwidth for one health
    // baseline plus demand-driven Pro subscriptions below.

    let sym_map = std::sync::Arc::new(sym_map);
    let normalized_map = std::sync::Arc::new(
        sym_map
            .iter()
            .map(|(raw, (normalized, multiplier))| {
                (normalized.clone(), (raw.clone(), *multiplier))
            })
            .collect::<HashMap<_, _>>(),
    );
    let mut children = tokio::task::JoinSet::new();

    for (idx, chunk) in symbols.chunks(CLASSIC_SYMBOLS_PER_SHARD).enumerate() {
        let chunk_symbols = chunk.to_vec();
        let p_map = price_map.clone();
        let sig = signal.clone();
        let l2 = l2_books.clone();
        let s_map = sym_map.clone();

        children.spawn(async move {
            let mut backoff = tokio::time::Duration::from_secs(1);
            loop {
                if let Err(e) = run_ws(idx, &chunk_symbols, &p_map, &sig, &l2, &*s_map).await {
                    tracing::warn!(
                        "[kucoin] Shard {} WS error: {}. Reconnecting in {:?}...",
                        idx,
                        e,
                        backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(tokio::time::Duration::from_secs(5));
                } else {
                    backoff = tokio::time::Duration::from_secs(1);
                }
            }
        });
    }

    // KuCoin's Classic depth-5 stream above is a 100 ms discovery baseline.
    // Near-entry and owned pairs additionally use the Pro top-500 incremental
    // stream (snapshot followed by absolute deltas every 10 ms), with no REST
    // snapshot in the execution path.
    let fast_prices = price_map.clone();
    let fast_signal = signal.clone();
    let fast_l2 = l2_books.clone();
    let fast_wanted = l2_wanted.clone();
    let fast_raw_symbols = sym_map.clone();
    children.spawn(async move {
        let mut backoff = Duration::from_millis(250);
        loop {
            match run_pro_depth(
                &fast_prices,
                &fast_signal,
                &fast_l2,
                &fast_wanted,
                &normalized_map,
                &fast_raw_symbols,
            )
            .await
            {
                Ok(()) => backoff = Duration::from_millis(250),
                Err(error) => {
                    warn!(
                        "[kucoin_pro_l2] disconnected: {}; reconnecting in {:?}",
                        error, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    });

    tokio::select! {
        child = children.join_next() => {
            match child {
                Some(Ok(())) => anyhow::bail!("KuCoin collector child exited unexpectedly"),
                Some(Err(error)) => anyhow::bail!("KuCoin collector child failed: {error}"),
                None => anyhow::bail!("KuCoin collector has no live children"),
            }
        },
        _ = tokio::time::sleep(Duration::from_secs(1800)) => {},
    }
    // Dropping JoinSet aborts all children before reloading the instrument universe.
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct OrderedPrice(f64);

impl PartialEq for OrderedPrice {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == Ordering::Equal
    }
}

impl Eq for OrderedPrice {}

impl PartialOrd for OrderedPrice {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedPrice {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Default)]
struct ProBook {
    sequence: i64,
    bids: BTreeMap<OrderedPrice, f64>,
    asks: BTreeMap<OrderedPrice, f64>,
}

fn apply_pro_side(
    side: &mut BTreeMap<OrderedPrice, f64>,
    raw: Option<&Vec<Value>>,
) -> Result<()> {
    let raw = raw.ok_or_else(|| anyhow!("KuCoin Pro depth side is missing"))?;
    for level in raw {
        let values = level
            .as_array()
            .ok_or_else(|| anyhow!("KuCoin Pro depth level is not an array"))?;
        let price = values
            .first()
            .and_then(number)
            .filter(|value| *value > 0.0)
            .ok_or_else(|| anyhow!("KuCoin Pro depth price is invalid"))?;
        let contracts = values
            .get(1)
            .and_then(number)
            .filter(|value| *value >= 0.0)
            .ok_or_else(|| anyhow!("KuCoin Pro depth size is invalid"))?;
        let key = OrderedPrice(price);
        if contracts == 0.0 {
            side.remove(&key);
        } else {
            side.insert(key, contracts);
        }
    }
    Ok(())
}

fn apply_pro_book_message(book: &mut ProBook, message: &Value) -> Result<bool> {
    let kind = message["t"]
        .as_str()
        .ok_or_else(|| anyhow!("KuCoin Pro depth message has no type"))?;
    let data = &message["d"];
    let start = data["O"]
        .as_i64()
        .ok_or_else(|| anyhow!("KuCoin Pro depth message has no start sequence"))?;
    let end = data["C"]
        .as_i64()
        .ok_or_else(|| anyhow!("KuCoin Pro depth message has no end sequence"))?;
    if start <= 0 || end < start {
        anyhow::bail!("KuCoin Pro depth sequence is invalid");
    }
    match kind {
        "snapshot" => {
            if start != end {
                anyhow::bail!("KuCoin Pro snapshot spans multiple sequences");
            }
            book.bids.clear();
            book.asks.clear();
        }
        "delta" if end <= book.sequence => return Ok(false),
        "delta" if book.sequence == 0 => return Ok(false),
        "delta" if start > book.sequence.saturating_add(1) => {
            anyhow::bail!(
                "KuCoin Pro depth sequence gap {}..{} after {}",
                start,
                end,
                book.sequence
            );
        }
        "delta" => {}
        other => anyhow::bail!("unsupported KuCoin Pro depth message type {other}"),
    }
    apply_pro_side(&mut book.bids, data["b"].as_array())?;
    apply_pro_side(&mut book.asks, data["a"].as_array())?;
    book.sequence = end;
    Ok(true)
}

fn pro_book_snapshot(book: &ProBook, multiplier: f64) -> Option<(Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    if !multiplier.is_finite() || multiplier <= 0.0 {
        return None;
    }
    let bids: Vec<_> = book
        .bids
        .iter()
        .rev()
        .take(50)
        .map(|(price, contracts)| (price.0, contracts * multiplier))
        .collect();
    let asks: Vec<_> = book
        .asks
        .iter()
        .take(50)
        .map(|(price, contracts)| (price.0, contracts * multiplier))
        .collect();
    if bids.is_empty()
        || asks.is_empty()
        || bids[0].0 >= asks[0].0
        || bids
            .iter()
            .chain(&asks)
            .any(|(price, quantity)| !price.is_finite() || !quantity.is_finite() || *quantity <= 0.0)
    {
        return None;
    }
    Some((bids, asks))
}

fn pro_depth_subscription(raw_symbol: &str, subscribe: bool) -> Value {
    json!({
        "id": uuid::Uuid::new_v4().simple().to_string(),
        "action": if subscribe { "SUBSCRIBE" } else { "UNSUBSCRIBE" },
        "channel": "obu",
        "tradeType": "FUTURES",
        "symbol": raw_symbol,
        "depth": "increment@10ms",
        "rpiFilter": 0,
        "response": true,
    })
}

async fn connect_kucoin_pro_public() -> Result<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
> {
    const HOST: &str = "x-push-futures.kucoin.com";
    const URL: &str = "wss://x-push-futures.kucoin.com";
    let addresses = tokio::net::lookup_host((HOST, 443)).await?;
    let mut attempts = futures_util::stream::FuturesUnordered::new();
    for address in addresses {
        attempts.push(async move {
            let socket = tokio::time::timeout(
                Duration::from_secs(4),
                tokio::net::TcpStream::connect(address),
            )
            .await
            .map_err(|_| anyhow!("KuCoin Pro L2 TCP timeout via {address}"))??;
            socket.set_nodelay(true)?;
            tokio::time::timeout(Duration::from_secs(6), client_async_tls(URL, socket))
                .await
                .map_err(|_| anyhow!("KuCoin Pro L2 TLS timeout via {address}"))?
                .map(|(socket, _)| socket)
                .map_err(anyhow::Error::from)
        });
    }
    let mut failures = Vec::new();
    while let Some(result) = attempts.next().await {
        match result {
            Ok(socket) => return Ok(socket),
            Err(error) => failures.push(error.to_string()),
        }
    }
    anyhow::bail!("all KuCoin Pro L2 addresses failed: {}", failures.join("; "))
}

async fn run_pro_depth(
    price_map: &PriceMap,
    signal: &UpdateSignal,
    l2_books: &L2OrderbookMap,
    l2_wanted: &std::sync::Arc<dashmap::DashSet<String>>,
    normalized_symbols: &HashMap<String, (String, f64)>,
    raw_symbols: &HashMap<String, (String, f64)>,
) -> Result<()> {
    let mut ws = connect_kucoin_pro_public().await?;
    let welcome_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let welcome: Value = loop {
        let message = tokio::time::timeout_at(welcome_deadline, ws.next())
            .await
            .map_err(|_| anyhow!("KuCoin Pro L2 welcome timeout"))?
            .ok_or_else(|| anyhow!("KuCoin Pro L2 closed before welcome"))??;
        match message {
            Message::Text(text) => break serde_json::from_str(&text)?,
            Message::Binary(bytes) => break serde_json::from_slice(&bytes)?,
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await?,
            Message::Pong(_) => {}
            Message::Close(frame) => {
                anyhow::bail!("KuCoin Pro L2 closed before welcome: {frame:?}")
            }
            _ => {}
        }
    };
    if welcome["message"].as_str() != Some("welcome") {
        anyhow::bail!("KuCoin Pro L2 rejected connection: {welcome}");
    }
    info!("[kucoin_pro_l2] connected to 10 ms incremental futures depth");
    // Keep the reader independently polled while subscription maintenance and
    // pings use the sink. Repeatedly cancelling a combined stream read on every
    // 20 ms maintenance tick can otherwise delay a fragmented snapshot frame.
    let (mut writer, mut reader) = ws.split();

    let mut subscribed = HashSet::<String>::new();
    let mut retained = HashMap::<String, Instant>::new();
    let mut books = HashMap::<String, ProBook>::new();
    let mut refresh = tokio::time::interval(Duration::from_millis(20));
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ping = tokio::time::interval(Duration::from_secs(15));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_received = Instant::now();
    loop {
        tokio::select! {
            _ = refresh.tick() => {
                let now = Instant::now();
                for key in l2_wanted.iter() {
                    if let Some(normalized) = key.key().strip_prefix("kucoin:") {
                        if let Some((raw, _)) = normalized_symbols.get(normalized) {
                            retained.insert(raw.clone(), now);
                        }
                    }
                }
                retained.retain(|_, seen| now.duration_since(*seen) < Duration::from_secs(15));
                let desired: HashSet<String> = retained.keys().cloned().collect();
                for raw in desired.difference(&subscribed) {
                    tracing::debug!("[kucoin_pro_l2] subscribing {raw}");
                    writer.send(Message::Text(
                        pro_depth_subscription(raw, true).to_string().into(),
                    )).await?;
                }
                for raw in subscribed.difference(&desired) {
                    writer.send(Message::Text(
                        pro_depth_subscription(raw, false).to_string().into(),
                    )).await?;
                    books.remove(raw);
                }
                subscribed = desired;
            }
            message = reader.next() => match message {
                Some(Ok(message @ (Message::Text(_) | Message::Binary(_)))) => {
                    last_received = Instant::now();
                    let value: Value = match message {
                        Message::Text(text) => serde_json::from_str(&text)?,
                        Message::Binary(bytes) => serde_json::from_slice(&bytes)?,
                        _ => unreachable!("match arm only accepts text or binary messages"),
                    };
                    if value["code"].as_i64().is_some() || value["result"].as_bool() == Some(false) {
                        anyhow::bail!("KuCoin Pro L2 server error: {value}");
                    }
                    if value["T"].as_str() != Some("obu.FUTURES")
                        || value["dp"].as_str() != Some("increment@10ms")
                    {
                        tracing::debug!("[kucoin_pro_l2] control message: {value}");
                        continue;
                    }
                    let raw = value["d"]["s"]
                        .as_str()
                        .ok_or_else(|| anyhow!("KuCoin Pro L2 update has no symbol"))?;
                    if !subscribed.contains(raw) {
                        continue;
                    }
                    let (normalized, multiplier) = raw_symbols
                        .get(raw)
                        .map(|(normalized, multiplier)| (normalized.as_str(), *multiplier))
                        .ok_or_else(|| anyhow!("unknown KuCoin Pro L2 symbol {raw}"))?;
                    let source_ms = value["d"]["M"]
                        .as_i64()
                        .map(|timestamp_ns| timestamp_ns / 1_000_000)
                        .ok_or_else(|| anyhow!("KuCoin Pro L2 update has no matching-engine timestamp"))?;
                    let now_ms = crate::engine::utils::now_ms();
                    if !(0..=750).contains(&now_ms.saturating_sub(source_ms)) {
                        continue;
                    }
                    let book = books.entry(raw.to_string()).or_default();
                    if !apply_pro_book_message(book, &value)? {
                        continue;
                    }
                    let Some((bids, asks)) = pro_book_snapshot(book, multiplier) else {
                        anyhow::bail!("KuCoin Pro L2 produced an invalid/crossed book for {raw}");
                    };
                    if let Some(mut venue) = price_map.get_mut("kucoin") {
                        let _ = apply_depth_ticker(&mut venue, normalized, &bids, &asks, source_ms);
                    }
                    l2_books.insert(
                        format!("kucoin:{normalized}"),
                        L2Snapshot { bids, asks, updated_ms: source_ms },
                    );
                    pro_depth_updates().insert(normalized.to_string(), source_ms);
                    signal.notify_one();
                }
                Some(Ok(Message::Ping(payload))) => writer.send(Message::Pong(payload)).await?,
                Some(Ok(Message::Close(frame))) => anyhow::bail!("KuCoin Pro L2 closed: {frame:?}"),
                Some(Err(error)) => return Err(error.into()),
                None => anyhow::bail!("KuCoin Pro L2 stream ended"),
                _ => {}
            },
            _ = ping.tick() => {
                if last_received.elapsed() > Duration::from_secs(25) {
                    anyhow::bail!("KuCoin Pro L2 pong/data timeout");
                }
                writer.send(Message::Text(json!({
                    "id": uuid::Uuid::new_v4().simple().to_string(),
                    "op": "ping",
                    "timestamp": crate::engine::utils::now_ms(),
                }).to_string().into())).await?;
            }
        }
    }
}

fn number(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse::<f64>().ok()))
        .filter(|value| value.is_finite())
}

fn parse_depth_side(
    raw: &[Vec<serde_json::Value>],
    multiplier: f64,
) -> Option<Vec<(f64, f64)>> {
    if !multiplier.is_finite() || multiplier <= 0.0 {
        return None;
    }
    raw.iter()
        .map(|level| {
            let price = number(level.first()?)?;
            let contracts = number(level.get(1)?)?;
            let quantity = contracts * multiplier;
            (price > 0.0 && quantity.is_finite() && quantity > 0.0)
                .then_some((price, quantity))
        })
        .collect()
}

fn validated_depth_snapshot(
    raw_bids: Option<&[Vec<serde_json::Value>]>,
    raw_asks: Option<&[Vec<serde_json::Value>]>,
    multiplier: f64,
) -> Option<(Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    let bids = parse_depth_side(raw_bids?, multiplier)?;
    let asks = parse_depth_side(raw_asks?, multiplier)?;
    if bids.is_empty()
        || asks.is_empty()
        || bids[0].0 >= asks[0].0
        || bids.windows(2).any(|levels| levels[1].0 > levels[0].0)
        || asks.windows(2).any(|levels| levels[1].0 < levels[0].0)
    {
        return None;
    }
    Some((bids, asks))
}

fn price_update_is_newer(last: Option<(i64, bool)>, source_ms: i64, is_depth: bool) -> bool {
    match last {
        None => true,
        Some((last_ms, last_was_depth)) => {
            source_ms > last_ms || (source_ms == last_ms && is_depth && !last_was_depth)
        }
    }
}

fn apply_depth_ticker(
    venue: &mut std::collections::HashMap<String, TickerData>,
    symbol: &str,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
    source_ms: i64,
) -> bool {
    let (Some(&(bid, bid_quantity)), Some(&(ask, ask_quantity))) = (bids.first(), asks.first())
    else {
        return false;
    };
    if source_ms <= 0
        || ![bid, ask, bid_quantity, ask_quantity]
            .iter()
            .all(|value| value.is_finite())
        || bid <= 0.0
        || ask <= bid
        || bid_quantity <= 0.0
        || ask_quantity <= 0.0
    {
        return false;
    }
    let entry = venue.entry(symbol.to_string()).or_default();
    entry.bid = bid;
    entry.ask = ask;
    entry.price = (bid + ask) / 2.0;
    entry.bid_size = bid * bid_quantity;
    entry.ask_size = ask * ask_quantity;
    entry.last_updated = (source_ms / 1000) as u64;
    true
}

async fn run_ws(
    shard_idx: usize,
    symbols: &[String],
    price_map: &PriceMap,
    signal: &UpdateSignal,
    l2_books: &L2OrderbookMap,
    sym_map: &std::collections::HashMap<String, (String, f64)>,
) -> Result<()> {
    let http = Client::new();
    let resp: Value = http
        .post("https://api-futures.kucoin.com/api/v1/bullet-public")
        .send()
        .await?
        .json()
        .await?;
    let token = resp["data"]["token"]
        .as_str()
        .ok_or_else(|| anyhow!("No KuCoin WS token"))?;
    let endpoint = resp["data"]["instanceServers"][0]["endpoint"]
        .as_str()
        .unwrap_or("wss://ws-api-futures.kucoin.com");
    let connect_id = format!("cn_{}_{}", shard_idx, uuid::Uuid::new_v4());
    let (mut ws, _) = connect_async_tls_with_config(
        &format!("{}?token={}&connectId={}", endpoint, token, connect_id),
        None,
        false,
        None,
    )
    .await
    .context("KuCoin WS connect failed")?;

    // Subscribe to L1 ticker stream for this shard's symbols
    for chunk in symbols.chunks(50) {
        let topic = format!("/contractMarket/tickerV2:{}", chunk.join(","));
        let sub_id = format!("sub_{}_{}", shard_idx, uuid::Uuid::new_v4());
        ws.send(Message::Text(json!({"id": sub_id, "type": "subscribe", "topic": topic, "privateChannel": false, "response": true}).to_string().into())).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // A permanent BTC depth stream supplies venue health and warmup headroom.
    // Executable candidate/position books use the demand-driven Pro 10 ms feed;
    // subscribing all ~680 contracts here floods small VPS receive queues and
    // starves the independent Binance feed.
    for symbol in symbols
        .iter()
        .filter(|symbol| is_classic_depth_baseline(symbol))
    {
        let topic = format!("/contractMarket/level2Depth5:{symbol}");
        ws.send(Message::Text(json!({"id":uuid::Uuid::new_v4().to_string(),"type":"subscribe","topic":topic,"privateChannel":false,"response":true}).to_string().into())).await?;
        tokio::time::sleep(Duration::from_millis(110)).await;
    }

    // Reconnect after 30 min to refresh L2 subscription list (new cross-exchange pairs may appear)
    let conn_start = std::time::Instant::now();
    let max_conn_age = Duration::from_secs(1800);

    #[derive(Deserialize)]
    struct KucoinMsg<'a> {
        topic: Option<String>,
        #[serde(rename = "type")]
        msg_type: &'a str,
        #[serde(borrow)]
        data: Option<KucoinData<'a>>,
    }

    #[derive(Deserialize)]
    struct KucoinData<'a> {
        timestamp: Option<i64>,
        ts: Option<i64>,
        sequence: Option<i64>,
        bids: Option<Vec<Vec<serde_json::Value>>>,
        asks: Option<Vec<Vec<serde_json::Value>>>,
        symbol: Option<&'a str>,
        #[serde(
            deserialize_with = "de_opt_f64_or_str",
            rename = "bestBidPrice",
            default
        )]
        bid: Option<f64>,
        #[serde(
            deserialize_with = "de_opt_f64_or_str",
            rename = "bestAskPrice",
            default
        )]
        ask: Option<f64>,
        #[serde(
            deserialize_with = "de_opt_f64_or_str",
            rename = "bestBidSize",
            default
        )]
        bid_qty: Option<f64>,
        #[serde(
            deserialize_with = "de_opt_f64_or_str",
            rename = "bestAskSize",
            default
        )]
        ask_qty: Option<f64>,
        #[serde(deserialize_with = "de_opt_f64_or_str", default)]
        price: Option<f64>,
    }

    fn de_opt_f64_or_str<'de, D>(deserializer: D) -> std::result::Result<Option<f64>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Any<'a> {
            Float(f64),
            Str(&'a str),
        }
        Ok(match Option::<Any>::deserialize(deserializer)? {
            Some(Any::Float(f)) => Some(f),
            Some(Any::Str(s)) => s.parse().ok(),
            None => None,
        })
    }

    // Ensure kucoin map exists for mutably getting later
    price_map.entry("kucoin".to_string()).or_default();

    let mut ping = tokio::time::interval(Duration::from_secs(12));
    let mut sequences: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    // L1 ticker and depth5 are independent topics. Retain millisecond ordering
    // across both and prefer executable depth when timestamps are equal.
    let mut latest_prices: std::collections::HashMap<String, (i64, bool)> =
        std::collections::HashMap::new();
    loop {
        // Reconnect periodically to refresh L2 subscriptions (new pairs may appear)
        if conn_start.elapsed() > max_conn_age {
            info!("[kucoin] Refreshing L2 subscriptions after 30min...");
            break;
        }
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(v) = serde_json::from_str::<KucoinMsg>(&text) {
                        if v.msg_type == "error" { anyhow::bail!("KuCoin subscription rejected"); }
                        if v.msg_type != "message" { continue; }
                        let topic = v.topic.as_deref().unwrap_or("");
                        let raw_sym_from_topic = topic.split(':').nth(1);

                        if let Some(d) = v.data {
                            let raw_sym = d.symbol.or(raw_sym_from_topic);
                            if let Some(raw_sym) = raw_sym {
                                if let Some((sym, mult)) = sym_map.get(raw_sym) {
                                    let now_ms = crate::engine::utils::now_ms();
                                    let depth = topic.starts_with("/contractMarket/level2Depth5:");
                                    let source_ms = if depth {
                                        d.timestamp.or_else(|| d.ts.map(|n| n / 1_000_000))
                                    } else {
                                        d.ts.map(|n| n / 1_000_000)
                                    };
                                    let Some(source_ms) = source_ms.filter(|t|(0..=750).contains(&(now_ms-*t))) else {continue;};
                                    if let Some(seq) = d.sequence {
                                        if sequences.get(topic).map(|old|seq<=*old).unwrap_or(false) {continue;}
                                        sequences.insert(topic.to_owned(),seq);
                                    } else {continue;}
                                    if depth {
                                        let Some((bids, asks)) = validated_depth_snapshot(
                                            d.bids.as_deref(),
                                            d.asks.as_deref(),
                                            *mult,
                                        ) else { continue; };
                                        let book_key = format!("kucoin:{}", sym);
                                        if l2_books
                                            .get(&book_key)
                                            .map(|book| book.updated_ms >= source_ms)
                                            .unwrap_or(false)
                                        {
                                            continue;
                                        }
                                        if price_update_is_newer(
                                            latest_prices.get(sym).copied(),
                                            source_ms,
                                            true,
                                        ) {
                                            if let Some(mut venue) = price_map.get_mut("kucoin") {
                                                if apply_depth_ticker(
                                                    &mut venue,
                                                    sym,
                                                    &bids,
                                                    &asks,
                                                    source_ms,
                                                ) {
                                                    latest_prices.insert(sym.to_string(), (source_ms, true));
                                                }
                                            }
                                        }
                                        l2_books.insert(
                                            book_key,
                                            L2Snapshot {
                                                bids,
                                                asks,
                                                updated_ms: source_ms,
                                            },
                                        );
                                        signal.notify_one();
                                        continue;
                                    }
                                    if l2_books
                                        .get(&format!("kucoin:{}", sym))
                                        .map(|book| book.updated_ms >= source_ms)
                                        .unwrap_or(false)
                                    {
                                        continue;
                                    }
                                    let bid: f64 = match d.bid { Some(v) if v.is_finite() && v > 0.0 => v, _ => continue };
                                    let ask: f64 = match d.ask { Some(v) if v.is_finite() && v > 0.0 => v, _ => continue };
                                    if bid >= ask { continue; }
                                    let bid_qty = d.bid_qty.unwrap_or(0.0);
                                    let ask_qty = d.ask_qty.unwrap_or(0.0);
                                    let price = d.price.unwrap_or((bid+ask)/2.0);
                                    if !bid_qty.is_finite()
                                        || !ask_qty.is_finite()
                                        || bid_qty < 0.0
                                        || ask_qty < 0.0
                                        || !price.is_finite()
                                        || price <= 0.0
                                        || !price_update_is_newer(
                                            latest_prices.get(sym).copied(),
                                            source_ms,
                                            false,
                                        )
                                    {
                                        continue;
                                    }

                                    if let Some(mut ex_map) = price_map.get_mut("kucoin") {
                                        let ts = (source_ms / 1000) as u64;
                                        if let Some(entry) = ex_map.get_mut(sym) {
                                            entry.bid = bid;
                                            entry.ask = ask;
                                            entry.price = price;
                                            entry.bid_size = bid_qty * mult * price;
                                            entry.ask_size = ask_qty * mult * price;
                                            entry.last_updated = ts;
                                        } else {
                                            ex_map.insert(sym.to_string(), TickerData {
                                                bid, ask, price, vol: 0.0, funding_rate: 0.0,
                                                bid_size: bid_qty * mult * price, ask_size: ask_qty * mult * price,
                                                last_updated: ts
                                            });
                                        }
                                        latest_prices.insert(sym.to_string(), (source_ms, false));
                                    }
                                    signal.notify_one();
                                }
                            }
                        }
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(e)) => return Err(e.into()),
                _ => {}
            },
            _ = ping.tick() => {
                let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
                ws.send(Message::Text(json!({"id": format!("{}", ts), "type": "ping"}).to_string().into())).await?;
            }

        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn levels(value: serde_json::Value) -> Vec<Vec<serde_json::Value>> {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn classic_discovery_keeps_one_depth_baseline_and_five_universe_sockets() {
        assert!(is_classic_depth_baseline("XBTUSDTM"));
        assert!(!is_classic_depth_baseline("ETHUSDTM"));
        assert_eq!(681usize.div_ceil(CLASSIC_SYMBOLS_PER_SHARD), 5);
    }

    #[test]
    fn depth_snapshot_is_strict_sorted_and_converts_contracts_to_base_units() {
        let bids = levels(serde_json::json!([["100", 2], ["99", "3"]]));
        let asks = levels(serde_json::json!([["101", 4], ["102", "5"]]));
        let (parsed_bids, parsed_asks) =
            validated_depth_snapshot(Some(&bids), Some(&asks), 10.0).unwrap();
        assert_eq!(parsed_bids, vec![(100.0, 20.0), (99.0, 30.0)]);
        assert_eq!(parsed_asks, vec![(101.0, 40.0), (102.0, 50.0)]);

        let malformed = levels(serde_json::json!([["100", 2], ["bad", 3]]));
        assert!(validated_depth_snapshot(Some(&malformed), Some(&asks), 10.0).is_none());
        let unsorted = levels(serde_json::json!([["99", 2], ["100", 3]]));
        assert!(validated_depth_snapshot(Some(&unsorted), Some(&asks), 10.0).is_none());
        let crossed = levels(serde_json::json!([["101", 2]]));
        assert!(validated_depth_snapshot(Some(&crossed), Some(&asks), 10.0).is_none());
    }

    #[test]
    fn depth_top_updates_discovery_price_without_losing_volume() {
        let mut venue = std::collections::HashMap::from([(
            "TAKEUSDT".to_string(),
            TickerData {
                vol: 42.0,
                funding_rate: 0.001,
                ..Default::default()
            },
        )]);
        assert!(apply_depth_ticker(
            &mut venue,
            "TAKEUSDT",
            &[(100.0, 2.0), (99.0, 3.0)],
            &[(101.0, 4.0), (102.0, 5.0)],
            1_000_100,
        ));
        let ticker = &venue["TAKEUSDT"];
        assert_eq!(ticker.bid, 100.0);
        assert_eq!(ticker.ask, 101.0);
        assert_eq!(ticker.bid_size, 200.0);
        assert_eq!(ticker.ask_size, 404.0);
        assert_eq!(ticker.vol, 42.0);
        assert_eq!(ticker.funding_rate, 0.001);
        assert_eq!(ticker.last_updated, 1_000);
    }

    #[test]
    fn newest_topic_wins_and_depth_wins_equal_timestamp() {
        assert!(price_update_is_newer(None, 100, false));
        assert!(price_update_is_newer(Some((100, false)), 100, true));
        assert!(!price_update_is_newer(Some((100, true)), 100, false));
        assert!(!price_update_is_newer(Some((101, true)), 100, true));
        assert!(price_update_is_newer(Some((100, true)), 101, false));
    }

    #[test]
    fn pro_incremental_book_applies_absolute_updates_and_removals() {
        let mut book = ProBook::default();
        let snapshot = json!({
            "t": "snapshot",
            "d": {
                "O": 10,
                "C": 10,
                "b": [["100", "2"], ["99", "3"]],
                "a": [["101", "4"], ["102", "5"]]
            }
        });
        assert!(apply_pro_book_message(&mut book, &snapshot).unwrap());
        let delta = json!({
            "t": "delta",
            "d": {
                "O": 11,
                "C": 12,
                "b": [["100", "0"], ["100.5", "7"]],
                "a": [["101", "6"]]
            }
        });
        assert!(apply_pro_book_message(&mut book, &delta).unwrap());
        let (bids, asks) = pro_book_snapshot(&book, 0.1).unwrap();
        assert_eq!(book.sequence, 12);
        assert_eq!(bids.iter().map(|level| level.0).collect::<Vec<_>>(), vec![100.5, 99.0]);
        assert_eq!(asks.iter().map(|level| level.0).collect::<Vec<_>>(), vec![101.0, 102.0]);
        assert!((bids[0].1 - 0.7).abs() < 1e-12);
        assert!((bids[1].1 - 0.3).abs() < 1e-12);
        assert!((asks[0].1 - 0.6).abs() < 1e-12);
        assert!((asks[1].1 - 0.5).abs() < 1e-12);
    }

    #[test]
    fn pro_incremental_book_rejects_sequence_gaps_and_crosses() {
        let mut book = ProBook::default();
        apply_pro_book_message(
            &mut book,
            &json!({
                "t": "snapshot",
                "d": {"O": 20, "C": 20, "b": [["100", "1"]], "a": [["101", "1"]]}
            }),
        )
        .unwrap();
        assert!(apply_pro_book_message(
            &mut book,
            &json!({
                "t": "delta",
                "d": {"O": 22, "C": 22, "b": [], "a": [["101", "2"]]}
            }),
        )
        .is_err());
        apply_pro_book_message(
            &mut book,
            &json!({
                "t": "delta",
                "d": {"O": 21, "C": 21, "b": [["102", "1"]], "a": []}
            }),
        )
        .unwrap();
        assert!(pro_book_snapshot(&book, 1.0).is_none());
    }

    #[test]
    fn pro_depth_subscription_requests_the_ten_millisecond_orderbook() {
        let subscribe = pro_depth_subscription("XBTUSDTM", true);
        assert_eq!(subscribe["action"], "SUBSCRIBE");
        assert_eq!(subscribe["channel"], "obu");
        assert_eq!(subscribe["tradeType"], "FUTURES");
        assert_eq!(subscribe["symbol"], "XBTUSDTM");
        assert_eq!(subscribe["depth"], "increment@10ms");
        assert_eq!(subscribe["rpiFilter"], 0);
        assert_eq!(subscribe["response"], true);

        let unsubscribe = pro_depth_subscription("XBTUSDTM", false);
        assert_eq!(unsubscribe["action"], "UNSUBSCRIBE");
        assert_eq!(unsubscribe["channel"], "obu");
        assert_eq!(unsubscribe["depth"], "increment@10ms");
    }

    #[tokio::test]
    #[ignore = "read-only live KuCoin Pro WebSocket probe"]
    async fn live_pro_incremental_depth_receives_snapshot_without_rest() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
        let prices = std::sync::Arc::new(dashmap::DashMap::new());
        prices.insert("kucoin".to_string(), HashMap::new());
        let signal = std::sync::Arc::new(tokio::sync::Notify::new());
        let books = std::sync::Arc::new(dashmap::DashMap::new());
        let wanted = std::sync::Arc::new(dashmap::DashSet::new());
        wanted.insert("kucoin:BTCUSDT".to_string());
        let normalized = HashMap::from([(
            "BTCUSDT".to_string(),
            ("XBTUSDTM".to_string(), 0.001),
        )]);
        let raw = HashMap::from([(
            "XBTUSDTM".to_string(),
            ("BTCUSDT".to_string(), 0.001),
        )]);
        let result = tokio::time::timeout(
            Duration::from_secs(8),
            run_pro_depth(&prices, &signal, &books, &wanted, &normalized, &raw),
        )
        .await;
        if let Ok(Err(error)) = result {
            panic!("KuCoin Pro L2 probe failed before timeout: {error}");
        }
        let book = books
            .get("kucoin:BTCUSDT")
            .expect("KuCoin Pro L2 did not publish a BTC snapshot");
        assert!(!book.bids.is_empty());
        assert!(!book.asks.is_empty());
        assert!(pro_depth_is_fresh(
            "BTCUSDT",
            crate::engine::utils::now_ms()
        ));
    }
}
