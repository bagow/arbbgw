//! KuCoin Futures WebSocket collector
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{info, warn};

use crate::state::{L2OrderbookMap, L2Snapshot, PriceMap, TickerData, UpdateSignal};

pub async fn run(
    prices: PriceMap,
    signal: UpdateSignal,
    l2_books: L2OrderbookMap,
    _l2_wanted: std::sync::Arc<dashmap::DashSet<String>>,
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
            res = handler(&prices, &signal, &l2_books) => {
                match res {
                    Ok(_)  => { warn!("[kucoin] WS closed, reconnecting..."); backoff = Duration::from_secs(1); }
                    Err(e) => warn!("[kucoin] WS disconnected: {}. Reconnecting in {:?}", e, backoff),
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

    // Split symbols into chunks of 150 to stay under Kucoin's 400 topics per WS limit
    // 150 symbols * 2 topics (L1 + L2) = 300 topics per connection

    let sym_map = std::sync::Arc::new(sym_map);
    let mut children = tokio::task::JoinSet::new();

    for (idx, chunk) in symbols.chunks(60).enumerate() {
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

    tokio::select! {
        _ = children.join_next() => {},
        _ = tokio::time::sleep(Duration::from_secs(1800)) => {},
    }
    // Dropping JoinSet aborts all children before reloading the instrument universe.
    Ok(())
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

    // Futures depth5 uses one symbol per topic. Every shard owns both streams.
    for symbol in symbols {
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
}
