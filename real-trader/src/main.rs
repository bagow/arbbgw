//! real-trader — Live order execution engine for CEX arbitrage
//!
//! Places REAL orders on exchanges.
//!
//! HTTP endpoints:
//!   POST /internal/spreads  ← receives spread data from rust-collector
//!   GET  /api/trading/trades  ← active trades
//!   DELETE /api/trading/trades ← clear open trades
//!
//! Supported exchanges: Gate.io (v4 futures), HTX (linear-swap), KuCoin (futures)
//!
//! Safety: DRY_RUN=true (default) logs orders but does NOT execute them.
//!         Set DRY_RUN=false in .env to enable real order placement.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    extract::{State, Json},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Router,
};
use base64::Engine;
use governor::{Quota, RateLimiter};
use governor::state::{NotKeyed, InMemoryState};
use governor::clock::DefaultClock;
use hmac::{Hmac, Mac};
use reqwest::{Client, header as rh};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Sha256;
use std::num::NonZeroU32;
use tokio::sync::{Mutex, RwLock};
use tokio_rusqlite::Connection;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info, warn};
use uuid::Uuid;

// ─── Types ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerData {
    pub bid: f64,
    pub ask: f64,
    pub price: f64,
    pub vol: f64,
    #[serde(rename = "fundingRate", default)]
    pub funding_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadRow {
    pub pair: String,
    #[serde(rename = "exchangeA")]
    pub exchange_a: String,
    #[serde(rename = "exchangeB")]
    pub exchange_b: String,
    #[serde(rename = "priceA")]
    pub price_a: f64,
    #[serde(rename = "priceB")]
    pub price_b: f64,
    #[serde(rename = "bidA")]
    pub bid_a: f64,
    #[serde(rename = "askA")]
    pub ask_a: f64,
    #[serde(rename = "bidB")]
    pub bid_b: f64,
    #[serde(rename = "askB")]
    pub ask_b: f64,
    #[serde(rename = "volA", default)]
    pub vol_a: f64,
    #[serde(rename = "volB", default)]
    pub vol_b: f64,
    #[serde(rename = "fundingA", default)]
    pub funding_a: f64,
    #[serde(rename = "fundingB", default)]
    pub funding_b: f64,
    pub spread: f64,
    pub direction: String,
    #[serde(default)]
    pub has_structural_spread: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadPayload {
    pub spreads: Vec<SpreadRow>,
    pub maps: HashMap<String, HashMap<String, TickerData>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingSettings {
    pub id: String,
    pub is_active: bool,
    pub min_entry_spread: f64,
    pub max_entry_spread: f64,
    pub trade_volume: f64,
    pub take_profit_percent: f64,
    pub trailing_stop_percent: f64,
    pub max_active_trades: i64,
    pub commission_rate: f64,
    pub slippage_percent: f64,
    pub funding_rate_8h: f64,
    pub min_volume_millions: f64,
    pub collector_interval_secs: i64,
    pub pending_commands: Option<String>,
    pub disabled_exchanges: Vec<String>,
}

impl Default for TradingSettings {
    fn default() -> Self {
        Self {
            id: "real-default".to_string(),
            is_active: false,
            min_entry_spread: 1.0,
            max_entry_spread: 10.0,
            trade_volume: 100.0,
            take_profit_percent: 0.2,
            trailing_stop_percent: 0.3,
            max_active_trades: 3,
            commission_rate: 0.05,
            slippage_percent: 0.05,
            funding_rate_8h: 0.01,
            min_volume_millions: 10.0,
            collector_interval_secs: 5,
            pending_commands: None,
            disabled_exchanges: vec![],
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: i64,
    pub pair: String,
    pub exchange_a: String,
    pub exchange_b: String,
    pub direction: String,
    pub volume: f64,
    pub entry_price_a: f64,
    pub entry_price_b: f64,
    pub entry_spread: f64,
    pub entry_time: i64,
    pub highest_spread_reached: f64,
    pub status: String,
    pub exit_price_a: Option<f64>,
    pub exit_price_b: Option<f64>,
    pub exit_spread: Option<f64>,
    pub exit_time: Option<i64>,
    pub pnl: Option<f64>,
    pub commission_cost: Option<f64>,
    pub funding_cost: Option<f64>,
    pub last_price_a: Option<f64>,
    pub last_price_b: Option<f64>,
    pub last_spread: Option<f64>,
    pub curr_gross_pnl: Option<f64>,
    pub pnl_a: Option<f64>,
    pub pnl_b: Option<f64>,
    pub close_reason: Option<String>,
    pub order_id_a: Option<String>,
    pub order_id_b: Option<String>,
    // Enriched fields (not in DB)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_price_a: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_price_b: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_spread: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_pnl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gross_pnl: Option<f64>,
}

// ─── Exchange config ──────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct ExchangeKeys {
    gate_key: String,
    gate_secret: String,
    htx_key: String,
    htx_secret: String,
    kucoin_key: String,
    kucoin_secret: String,
    kucoin_passphrase: String,
}

impl ExchangeKeys {
    fn from_env() -> Self {
        Self {
            gate_key: std::env::var("GATE_KEY").unwrap_or_default(),
            gate_secret: std::env::var("GATE_SECRET").unwrap_or_default(),
            htx_key: std::env::var("HTX_KEY").unwrap_or_default(),
            htx_secret: std::env::var("HTX_SECRET").unwrap_or_default(),
            kucoin_key: std::env::var("KUCOIN_KEY").unwrap_or_default(),
            kucoin_secret: std::env::var("KUCOIN_SECRET").unwrap_or_default(),
            kucoin_passphrase: std::env::var("KUCOIN_PASSPHRASE").unwrap_or_default(),
        }
    }

    fn supports(&self, exchange: &str) -> bool {
        match exchange {
            "gate" => !self.gate_key.is_empty() && !self.gate_secret.is_empty(),
            "htx" => !self.htx_key.is_empty() && !self.htx_secret.is_empty(),
            "kucoin" => !self.kucoin_key.is_empty() && !self.kucoin_secret.is_empty(),
            _ => false,
        }
    }
}

// ─── App state ────────────────────────────────────────────────────────────────

/// Per-exchange token-bucket rate limiters.
/// Limits: Gate=8/s (conservative vs 100/10s), HTX=8/s (vs 10/s), KuCoin=8/s (vs 30/3s).
/// Shared via Arc so all concurrent engine calls draw from the same bucket.
#[derive(Clone)]
struct ExchangeRateLimiters {
    gate:   Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    htx:    Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    kucoin: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

impl ExchangeRateLimiters {
    fn new() -> Self {
        let q = |n: u32| Quota::per_second(NonZeroU32::new(n).unwrap());
        Self {
            gate:   Arc::new(RateLimiter::direct(q(8))),
            htx:    Arc::new(RateLimiter::direct(q(8))),
            kucoin: Arc::new(RateLimiter::direct(q(8))),
        }
    }

    /// Wait until the given exchange's bucket allows another request.
    async fn wait(&self, exchange: &str) {
        match exchange {
            "gate"   => self.gate.until_ready().await,
            "htx"    => self.htx.until_ready().await,
            "kucoin" => self.kucoin.until_ready().await,
            _        => {},
        }
    }
}

#[derive(Clone)]
struct AppState {
    db: Connection,
    http: Client,
    worker_url: String,
    collector_token: String,
    internal_token: String,
    dry_run: bool,
    keys: ExchangeKeys,
    engine_lock: Arc<Mutex<()>>,
    /// In-memory settings — refreshed from profiles DB every 5s by background task.
    /// Hot path reads this with zero network latency.
    settings: Arc<RwLock<TradingSettings>>,
    limiters: ExchangeRateLimiters,
    last_push_time: Arc<Mutex<std::time::Instant>>,
}

// ─── DB init ──────────────────────────────────────────────────────────────────

async fn init_db(db: &Connection) -> Result<()> {
    db.call(|conn| {
        conn.execute_batch("
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT NOT NULL,
                exchange_a TEXT NOT NULL,
                exchange_b TEXT NOT NULL,
                direction TEXT NOT NULL,
                volume REAL NOT NULL,
                entry_price_a REAL NOT NULL,
                entry_price_b REAL NOT NULL,
                entry_spread REAL NOT NULL,
                entry_time INTEGER NOT NULL,
                highest_spread_reached REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                exit_price_a REAL,
                exit_price_b REAL,
                exit_spread REAL,
                exit_time INTEGER,
                pnl REAL,
                commission_cost REAL,
                funding_cost REAL,
                last_price_a REAL,
                last_price_b REAL,
                last_spread REAL,
                curr_gross_pnl REAL,
                pnl_a REAL,
                pnl_b REAL,
                close_reason TEXT,
                order_id_a TEXT,
                order_id_b TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
        ").map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
    }).await?;
    Ok(())
}

// ─── Row reader ───────────────────────────────────────────────────────────────

fn row_to_trade(row: &rusqlite::Row<'_>) -> rusqlite::Result<Trade> {
    Ok(Trade {
        id: row.get(0)?,
        pair: row.get(1)?,
        exchange_a: row.get(2)?,
        exchange_b: row.get(3)?,
        direction: row.get(4)?,
        volume: row.get(5)?,
        entry_price_a: row.get(6)?,
        entry_price_b: row.get(7)?,
        entry_spread: row.get(8)?,
        entry_time: row.get(9)?,
        highest_spread_reached: row.get(10)?,
        status: row.get(11)?,
        exit_price_a: row.get(12)?,
        exit_price_b: row.get(13)?,
        exit_spread: row.get(14)?,
        exit_time: row.get(15)?,
        pnl: row.get(16)?,
        commission_cost: row.get(17)?,
        funding_cost: row.get(18)?,
        last_price_a: row.get(19)?,
        last_price_b: row.get(20)?,
        last_spread: row.get(21)?,
        curr_gross_pnl: row.get(22)?,
        pnl_a: row.get(23).ok(),
        pnl_b: row.get(24).ok(),
        close_reason: row.get(25).ok(),
        order_id_a: row.get(26).ok(),
        order_id_b: row.get(27).ok(),
        current_price_a: None,
        current_price_b: None,
        current_spread: None,
        current_pnl: None,
        gross_pnl: None,
    })
}

// ─── HMAC helpers ─────────────────────────────────────────────────────────────

fn hmac_sha256_hex(secret: &[u8], data: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key size");
    mac.update(data.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn hmac_sha256_b64(secret: &[u8], data: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key size");
    mac.update(data.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ─── Gate.io Futures API ──────────────────────────────────────────────────────
// Docs: https://www.gate.io/docs/developers/apiv4/en/#futures

fn gate_sign(key: &str, secret: &str, method: &str, path: &str, body_str: &str) -> rh::HeaderMap {
    let ts = now_secs().to_string();
    let body_hash = {
        use sha2::Digest;
        let hash = sha2::Sha512::digest(body_str.as_bytes());
        hex::encode(hash)
    };
    let sign_str = format!("{}\n{}\n{}\n{}\n{}", method, path, "", body_hash, ts);
    let sig = {
        use hmac::Mac;
        let mut mac = Hmac::<sha2::Sha512>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(sign_str.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    };
    let mut headers = rh::HeaderMap::new();
    headers.insert("KEY", key.parse().unwrap());
    headers.insert("SIGN", sig.parse().unwrap());
    headers.insert("Timestamp", ts.parse().unwrap());
    headers.insert("Content-Type", "application/json".parse().unwrap());
    headers
}

/// Place a Gate.io futures perpetual order.
/// contract format: "BTC_USDT"
/// size: number of contracts (positive = long, negative = short)
async fn gate_place_order(
    http: &Client,
    key: &str,
    secret: &str,
    contract: &str,
    size: i64,
    dry_run: bool,
) -> Result<String> {
    let path = "/api/v4/futures/usdt/orders";
    let body = json!({
        "contract": contract,
        "size": size,
        "price": "0",       // 0 = market order
        "tif": "ioc",       // immediate-or-cancel for market
        "reduce_only": false,
        "close": false,
        "text": "t-realtrader"
    });
    let body_str = body.to_string();

    if dry_run {
        info!("[DRY_RUN] gate_place_order contract={} size={}", contract, size);
        return Ok(format!("dry_{}", Uuid::new_v4()));
    }

    let headers = gate_sign(key, secret, "POST", path, &body_str);
    let resp = http
        .post(format!("https://api.gateio.ws{}", path))
        .headers(headers)
        .body(body_str)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .context("Gate.io order request failed")?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow::anyhow!("Gate order error {}: {}", status, text));
    }
    let v: Value = serde_json::from_str(&text).context("Gate order response parse error")?;
    let order_id = v["id"].as_i64().map(|i| i.to_string())
        .or_else(|| v["id"].as_str().map(String::from))
        .unwrap_or_else(|| "unknown".to_string());
    info!("[gate] Order placed contract={} size={} order_id={}", contract, size, order_id);
    Ok(order_id)
}

// ─── HTX Linear Swap API ──────────────────────────────────────────────────────
// Docs: https://huobiapi.github.io/docs/usdt_swap/v1/en

fn htx_sign(key: &str, secret: &str, method: &str, host: &str, path: &str) -> String {
    let ts = chrono_like_ts();
    let params = format!(
        "AccessKeyId={}&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp={}",
        key, urlencoding_ts(&ts)
    );
    let sign_str = format!("{}\n{}\n{}\n{}", method, host, path, params);
    let sig = hmac_sha256_b64(secret.as_bytes(), &sign_str);
    format!(
        "{}?{}&Signature={}",
        path,
        params,
        urlencoding_ts(&sig)
    )
}

fn chrono_like_ts() -> String {
    // HTX HMAC signing requires ISO 8601 UTC without the Z suffix.
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string()
}

fn urlencoding_ts(s: &str) -> String {
    s.replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D")
        .replace(':', "%3A")
}

/// Place an HTX linear-swap order.
/// contract_code format: "BTC-USDT"
/// direction: "buy" | "sell", offset: "open" | "close"
async fn htx_place_order(
    http: &Client,
    key: &str,
    secret: &str,
    contract_code: &str,
    direction: &str,  // "buy" or "sell"
    volume: f64,      // number of contracts (1 contract = 1 USD typically)
    dry_run: bool,
) -> Result<String> {
    if dry_run {
        info!("[DRY_RUN] htx_place_order contract={} direction={} volume={}", contract_code, direction, volume);
        return Ok(format!("dry_{}", Uuid::new_v4()));
    }

    let host = "api.hbdm.com";
    let path = "/linear-swap-api/v1/swap_order";
    let signed_url = htx_sign(key, secret, "POST", host, path);

    let body = json!({
        "contract_code": contract_code,
        "client_order_id": now_ms(),
        "volume": volume.ceil() as i64,
        "direction": direction,
        "offset": "open",
        "lever_rate": 1,
        "order_price_type": "optimal_20",   // taker market order
    });

    let resp = http
        .post(format!("https://{}{}", host, signed_url))
        .json(&body)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .context("HTX order request failed")?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow::anyhow!("HTX order error {}: {}", status, text));
    }
    let v: Value = serde_json::from_str(&text).context("HTX order response parse")?;
    let order_id = v["data"]["order_id"].as_i64().map(|i| i.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    info!("[htx] Order placed contract={} direction={} order_id={}", contract_code, direction, order_id);
    Ok(order_id)
}

// ─── KuCoin Futures API ───────────────────────────────────────────────────────
// Docs: https://docs.kucoin.com/futures/en

fn kucoin_sign(secret: &str, passphrase: &str, key: &str, ts: &str, method: &str, path: &str, body_str: &str) -> (String, String) {
    let pre_sign = format!("{}{}{}{}", ts, method, path, body_str);
    let sig = hmac_sha256_b64(secret.as_bytes(), &pre_sign);
    let pass_sig = hmac_sha256_b64(secret.as_bytes(), passphrase);
    (sig, pass_sig)
}

/// Place a KuCoin Futures order.
/// symbol format: "BTCUSDTM" (for USDT-margined) or "XBTUSDM" for BTC-margined
/// side: "buy" | "sell"
/// leverage: integer
async fn kucoin_place_order(
    http: &Client,
    key: &str,
    secret: &str,
    passphrase: &str,
    symbol: &str,
    side: &str,
    size: i64,        // number of lots (auto-calculated)
    dry_run: bool,
) -> Result<String> {
    if dry_run {
        info!("[DRY_RUN] kucoin_place_order symbol={} side={} size={}", symbol, side, size);
        return Ok(format!("dry_{}", Uuid::new_v4()));
    }

    let ts = now_ms().to_string();
    let path = "/api/v1/orders";
    let body = json!({
        "clientOid": Uuid::new_v4().to_string(),
        "symbol": symbol,
        "side": side,
        "type": "market",
        "size": size,
        "leverage": "1",
    });
    let body_str = body.to_string();

    let (sig, pass_sig) = kucoin_sign(secret, passphrase, key, &ts, "POST", path, &body_str);

    let resp = http
        .post(format!("https://api-futures.kucoin.com{}", path))
        .header("KC-API-KEY", key)
        .header("KC-API-SIGN", sig)
        .header("KC-API-TIMESTAMP", ts)
        .header("KC-API-PASSPHRASE", pass_sig)
        .header("KC-API-KEY-VERSION", "2")
        .header("Content-Type", "application/json")
        .body(body_str)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .context("KuCoin order request failed")?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow::anyhow!("KuCoin order error {}: {}", status, text));
    }
    let v: Value = serde_json::from_str(&text).context("KuCoin order parse")?;
    let order_id = v["data"]["orderId"].as_str().unwrap_or("unknown").to_string();
    info!("[kucoin] Order placed symbol={} side={} order_id={}", symbol, side, order_id);
    Ok(order_id)
}

// ─── Order dispatcher ─────────────────────────────────────────────────────────

/// Open a position on a given exchange.
/// is_short: true = sell/short, false = buy/long
/// volume_usd: notional USD volume
async fn open_position(
    http: &Client,
    keys: &ExchangeKeys,
    exchange: &str,
    pair: &str,       // e.g. "BTCUSDT"
    is_short: bool,
    volume_usd: f64,
    price: f64,
    dry_run: bool,
) -> Result<String> {
    let side = if is_short { "sell" } else { "buy" };
    let contracts = (volume_usd / price.max(0.0001)).ceil() as i64;
    let contracts = contracts.max(1);

    match exchange {
        "gate" => {
            // Gate.io: symbol BTC_USDT, size negative = short
            let contract = pair.replace("USDT", "_USDT");
            let size = if is_short { -contracts } else { contracts };
            gate_place_order(http, &keys.gate_key, &keys.gate_secret, &contract, size, dry_run).await
        }
        "htx" => {
            // HTX: contract BTC-USDT, direction buy/sell
            let contract_code = format!("{}-{}", &pair[..pair.len()-4], &pair[pair.len()-4..]);
            htx_place_order(http, &keys.htx_key, &keys.htx_secret, &contract_code, side, contracts as f64, dry_run).await
        }
        "kucoin" => {
            // KuCoin futures: BTCUSDTM
            let symbol = format!("{}M", pair);
            kucoin_place_order(http, &keys.kucoin_key, &keys.kucoin_secret, &keys.kucoin_passphrase, &symbol, side, contracts, dry_run).await
        }
        other => Err(anyhow::anyhow!("Exchange {} not supported for real trading", other))
    }
}

/// Close a position on a given exchange (opposite of opening side).
async fn close_position(
    http: &Client,
    keys: &ExchangeKeys,
    exchange: &str,
    pair: &str,
    was_short: bool,  // true if we originally shorted this leg
    volume_usd: f64,
    price: f64,
    dry_run: bool,
) -> Result<String> {
    // To close: opposite direction
    open_position(http, keys, exchange, pair, !was_short, volume_usd, price, dry_run).await
}

// ─── Partial Fill Guard ───────────────────────────────────────────────────────

/// Query how many contracts were actually filled for a given order.
/// Called in parallel for both legs right after tokio::join! opens them.
/// On any error returns `requested_contracts` (optimistic: assume full fill).
async fn get_order_fill(
    http: &Client, keys: &ExchangeKeys,
    exchange: &str, pair: &str, order_id: &str, requested_contracts: f64,
) -> f64 {
    if order_id.starts_with("dry_") { return requested_contracts; }
    let result = match exchange {
        "gate"   => fill_gate(http, keys, order_id).await,
        "htx"    => fill_htx(http, keys, pair, order_id).await,
        "kucoin" => fill_kucoin(http, keys, order_id).await,
        _        => return requested_contracts,
    };
    match result {
        Ok(f)  => f,
        Err(e) => { warn!("[fill] {} {}: {} — assuming full fill", exchange, order_id, e); requested_contracts }
    }
}

/// Gate.io: GET /api/v4/futures/usdt/orders/{id} → abs(size) - abs(left) = filled
async fn fill_gate(http: &Client, keys: &ExchangeKeys, order_id: &str) -> Result<f64> {
    let path = format!("/api/v4/futures/usdt/orders/{}", order_id);
    let headers = gate_sign(&keys.gate_key, &keys.gate_secret, "GET", &path, "");
    let v: Value = http
        .get(format!("https://api.gateio.ws{}", path))
        .headers(headers)
        .timeout(Duration::from_secs(5))
        .send().await?.json().await?;
    let size = v["size"].as_i64().unwrap_or(0).abs();
    let left = v["left"].as_i64().unwrap_or(0).abs();
    Ok((size - left).max(0) as f64)
}

/// HTX: POST /linear-swap-api/v1/swap_order_info → data[0].trade_volume
async fn fill_htx(http: &Client, keys: &ExchangeKeys, pair: &str, order_id: &str) -> Result<f64> {
    let host = "api.hbdm.com";
    let path = "/linear-swap-api/v1/swap_order_info";
    let signed_url = htx_sign(&keys.htx_key, &keys.htx_secret, "POST", host, path);
    let contract_code = format!("{}-{}", &pair[..pair.len()-4], &pair[pair.len()-4..]);
    let body = json!({ "order_id": order_id.parse::<i64>().unwrap_or(0), "contract_code": contract_code });
    let v: Value = http
        .post(format!("https://{}{}", host, signed_url))
        .json(&body)
        .timeout(Duration::from_secs(5))
        .send().await?.json().await?;
    Ok(v["data"][0]["trade_volume"].as_f64().unwrap_or(0.0))
}

/// KuCoin: GET /api/v1/orders/{id} → data.dealSize (string field)
async fn fill_kucoin(http: &Client, keys: &ExchangeKeys, order_id: &str) -> Result<f64> {
    let ts = now_ms().to_string();
    let path = format!("/api/v1/orders/{}", order_id);
    let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &keys.kucoin_key, &ts, "GET", &path, "");
    let v: Value = http
        .get(format!("https://api-futures.kucoin.com{}", path))
        .header("KC-API-KEY", &keys.kucoin_key)
        .header("KC-API-SIGN", sig)
        .header("KC-API-TIMESTAMP", &ts)
        .header("KC-API-PASSPHRASE", pass_sig)
        .header("KC-API-KEY-VERSION", "2")
        .timeout(Duration::from_secs(5))
        .send().await?.json().await?;
    Ok(v["data"]["dealSize"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0))
}

// ─── Worker communication ─────────────────────────────────────────────────────

/// Load active real-trader profile from local profiles DB.
/// Column order mirrors vps-api trading_profiles table:
/// 0=id 1=name 2=mode 3=is_active 4=min_entry_spread 5=max_entry_spread
/// 6=trade_volume 7=take_profit 8=trailing_stop 9=max_trades 10=commission
/// 11=slippage 12=funding_rate_8h 13=min_vol_millions 14=collector_secs 15=pending_commands
/// 19=disabled_exchanges
async fn load_settings_from_db(profiles_db: &Connection) -> TradingSettings {
    profiles_db.call(|conn| {
        let result = conn.query_row(
            "SELECT * FROM trading_profiles WHERE mode='real' ORDER BY is_active DESC, created_at ASC LIMIT 1",
            [],
            |row| Ok(TradingSettings {
                id: row.get(0).unwrap_or_else(|_| "real-default".to_string()),
                is_active: row.get::<_, i64>(3).unwrap_or(0) != 0,
                min_entry_spread: row.get(4).unwrap_or(1.0),
                max_entry_spread: row.get(5).unwrap_or(10.0),
                trade_volume: row.get(6).unwrap_or(100.0),
                take_profit_percent: row.get(7).unwrap_or(0.2),
                trailing_stop_percent: row.get(8).unwrap_or(0.3),
                max_active_trades: row.get(9).unwrap_or(3),
                commission_rate: row.get(10).unwrap_or(0.05),
                slippage_percent: row.get(11).unwrap_or(0.05),
                funding_rate_8h: row.get(12).unwrap_or(0.01),
                min_volume_millions: row.get(13).unwrap_or(10.0),
                collector_interval_secs: row.get(14).unwrap_or(5),
                pending_commands: row.get(15).ok().flatten(),
                disabled_exchanges: {
                    let s: Option<String> = row.get(19).unwrap_or(None);
                    s.and_then(|js| serde_json::from_str(&js).ok()).unwrap_or_default()
                },
            }),
        );
        Ok(result.unwrap_or_default())
    }).await.unwrap_or_default()
}

/// Fetch active settings from in-memory RwLock (nanosecond latency).
/// Settings are refreshed from profiles DB every 5s by a background task.
async fn fetch_settings(state: &AppState) -> TradingSettings {
    state.settings.read().await.clone()
}

async fn ack_command(state: &AppState, command: &str) {
    let url = format!("{}/api/real-trader/commands/ack", state.worker_url.trim_end_matches('/'));
    let _ = state.http
        .post(&url)
        .bearer_auth(&state.internal_token)
        .json(&json!({ "command": command }))
        .timeout(Duration::from_secs(5))
        .send()
        .await;
}

async fn push_active_trades(state: &AppState, trades: &[Trade]) {
    let url = format!("{}/api/real-trader/active", state.worker_url.trim_end_matches('/'));
    if let Err(e) = state.http
        .post(&url)
        .bearer_auth(&state.collector_token)
        .json(trades)
        .timeout(Duration::from_secs(5))
        .send()
        .await
    {
        error!("Failed to push active trades to Worker: {}", e);
    }
}

async fn archive_trade(state: &AppState, trade: &Trade) {
    let url = format!("{}/api/real-trader/archive", state.worker_url.trim_end_matches('/'));
    if let Err(e) = state.http
        .post(&url)
        .bearer_auth(&state.collector_token)
        .json(trade)
        .timeout(Duration::from_secs(10))
        .send()
        .await
    {
        error!("Failed to archive real trade {}: {}", trade.id, e);
    }
}

// ─── Trading Engine ───────────────────────────────────────────────────────────

async fn run_trading_engine(state: &AppState, payload: &SpreadPayload) {
    let _lock = state.engine_lock.lock().await;

    let settings = fetch_settings(state).await;
    if !settings.is_active {
        return;
    }

    let now = now_ms();
    let maps = &payload.maps;

    let get_ticker = |exchange: &str, pair: &str| -> Option<&TickerData> {
        maps.get(exchange)?.get(pair)
    };

    // Load open trades
    let open_trades: Vec<Trade> = match state.db.call(|conn| {
        let mut stmt = conn.prepare(
            "SELECT id,pair,exchange_a,exchange_b,direction,volume,
                    entry_price_a,entry_price_b,entry_spread,entry_time,
                    highest_spread_reached,status,
                    exit_price_a,exit_price_b,exit_spread,exit_time,pnl,
                    commission_cost,funding_cost,
                    last_price_a,last_price_b,last_spread,curr_gross_pnl,
                    pnl_a,pnl_b,close_reason,order_id_a,order_id_b
             FROM trades WHERE status='OPEN'"
        )?;
        let rows = stmt
            .query_map([], row_to_trade)?
            .filter_map(|r| r.ok())
            .collect::<Vec<_>>();
        Ok(rows)
    }).await {
        Ok(t) => t,
        Err(e) => { error!("DB error loading trades: {}", e); return; }
    };

    let mut num_active = open_trades.len() as i64;
    let mut open_pair_keys: std::collections::HashSet<String> = open_trades
        .iter()
        .map(|t| format!("{}_{}_{}", t.pair, t.exchange_a, t.exchange_b))
        .collect();
    let mut open_tickers: std::collections::HashSet<String> =
        open_trades.iter().map(|t| t.pair.clone()).collect();

    // --- Process open trades ---
    for trade in &open_trades {
        let data_a = match get_ticker(&trade.exchange_a, &trade.pair) {
            Some(d) => d, None => continue,
        };
        let data_b = match get_ticker(&trade.exchange_b, &trade.pair) {
            Some(d) => d, None => continue,
        };

        let is_a_higher = trade.direction.starts_with(&trade.exchange_a);

        let mid_spread = if is_a_higher {
            (data_a.price - data_b.price) / data_b.price * 100.0
        } else {
            (data_b.price - data_a.price) / data_a.price * 100.0
        };

        let slippage = settings.slippage_percent / 100.0;
        let c_price_a = data_a.price;
        let c_price_b = data_b.price;

        let exit_price_a = if is_a_higher { c_price_a * (1.0 + slippage) } else { c_price_a * (1.0 - slippage) };
        let exit_price_b = if is_a_higher { c_price_b * (1.0 - slippage) } else { c_price_b * (1.0 + slippage) };

        let coins_a = trade.volume / trade.entry_price_a;
        let coins_b = trade.volume / trade.entry_price_b;

        let pnl_a = if is_a_higher { (trade.entry_price_a - exit_price_a) * coins_a } else { (exit_price_a - trade.entry_price_a) * coins_a };
        let pnl_b = if is_a_higher { (exit_price_b - trade.entry_price_b) * coins_b } else { (trade.entry_price_b - exit_price_b) * coins_b };
        let gross_pnl = pnl_a + pnl_b;

        let hours_open = (now - trade.entry_time) as f64 / 3_600_000.0;
        let comm_cost = trade.volume * (settings.commission_rate / 100.0) * 4.0;
        let funding_cost_a = trade.volume * data_a.funding_rate * (if !is_a_higher { 1.0 } else { -1.0 }) * (hours_open / 8.0);
        let funding_cost_b = trade.volume * data_b.funding_rate * (if is_a_higher { 1.0 } else { -1.0 }) * (hours_open / 8.0);
        let funding_cost = funding_cost_a + funding_cost_b;
        let net_pnl = gross_pnl - comm_cost - funding_cost;

        let prev_best = if trade.highest_spread_reached.abs() > 50.0 { net_pnl } else { trade.highest_spread_reached };
        let best_pnl = if net_pnl > prev_best { net_pnl } else { prev_best };

        let target_pnl_usd = trade.volume * (settings.take_profit_percent / 100.0);
        let stop_drop_usd = trade.volume * (settings.trailing_stop_percent / 100.0);

        let triggered_tp = net_pnl >= target_pnl_usd;
        let triggered_stop = (best_pnl - net_pnl) >= stop_drop_usd;
        let triggered_zero_spread = mid_spread <= 0.0;

        if triggered_tp || triggered_stop || triggered_zero_spread {
            let reason = if triggered_tp { "TP" } else if triggered_stop { "STOP" } else { "ZERO_SPREAD" };
            info!("Closing real trade {} {} — reason={} net_pnl={:.4}", trade.id, trade.pair, reason, net_pnl);

            // Close BOTH legs concurrently — sequential closing loses time while spread moves.
            // Pre-acquire rate limit tokens before firing to avoid 429 on either leg.
            tokio::join!(
                state.limiters.wait(&trade.exchange_a),
                state.limiters.wait(&trade.exchange_b)
            );
            let (close_a, close_b) = tokio::join!(
                close_position(
                    &state.http, &state.keys,
                    &trade.exchange_a, &trade.pair,
                    is_a_higher,
                    trade.volume, c_price_a, state.dry_run
                ),
                close_position(
                    &state.http, &state.keys,
                    &trade.exchange_b, &trade.pair,
                    !is_a_higher,
                    trade.volume, c_price_b, state.dry_run
                )
            );

            if let Err(e) = &close_a { error!("[CLOSE] Failed on {}: {}", trade.exchange_a, e); }
            if let Err(e) = &close_b { error!("[CLOSE] Failed on {}: {}", trade.exchange_b, e); }

            let trade_id = trade.id;
            let ep_a = exit_price_a;
            let ep_b = exit_price_b;
            let cc = comm_cost;
            let fc = funding_cost;

            if let Err(e) = state.db.call(move |conn| {
                conn.execute(
                    "UPDATE trades SET status='CLOSED',
                        exit_price_a=?,exit_price_b=?,exit_spread=?,exit_time=?,
                        pnl=?,commission_cost=?,funding_cost=?,highest_spread_reached=?,
                        pnl_a=?,pnl_b=?,close_reason=?
                     WHERE id=?",
                    params![ep_a, ep_b, mid_spread, now, net_pnl, cc, fc, best_pnl, pnl_a, pnl_b, reason, trade_id],
                ).map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
            }).await {
                error!("DB error closing trade {}: {}", trade.id, e);
                continue;
            }

            let mut closed = trade.clone();
            closed.status = "CLOSED".to_string();
            closed.close_reason = Some(reason.to_string());
            closed.exit_price_a = Some(exit_price_a);
            closed.exit_price_b = Some(exit_price_b);
            closed.exit_spread = Some(mid_spread);
            closed.exit_time = Some(now);
            closed.pnl = Some(net_pnl);
            closed.pnl_a = Some(pnl_a);
            closed.pnl_b = Some(pnl_b);
            closed.commission_cost = Some(comm_cost);
            closed.funding_cost = Some(funding_cost);

            let state_clone = state.clone();
            tokio::spawn(async move {
                archive_trade(&state_clone, &closed).await;
            });

            num_active -= 1;
            let key = format!("{}_{}_{}", trade.pair, trade.exchange_a, trade.exchange_b);
            open_pair_keys.remove(&key);
            open_tickers.remove(&trade.pair);
        } else {
            let trade_id = trade.id;
            if let Err(e) = state.db.call(move |conn| {
                conn.execute(
                    "UPDATE trades SET highest_spread_reached=?, last_price_a=?, last_price_b=?, last_spread=?, curr_gross_pnl=?, pnl_a=?, pnl_b=? WHERE id=?",
                    params![best_pnl, c_price_a, c_price_b, mid_spread, gross_pnl, pnl_a, pnl_b, trade_id],
                ).map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
            }).await {
                error!("DB error updating trade {}: {}", trade.id, e);
            }
        }
    }

    // --- Open new trades ---
    for row in &payload.spreads {
        if num_active >= settings.max_active_trades { break; }
        if row.spread < settings.min_entry_spread || row.spread > settings.max_entry_spread { continue; }
        if open_tickers.contains(&row.pair) { continue; }

        // Only trade pairs available on supported exchanges
        if !state.keys.supports(&row.exchange_a) { continue; }
        if !state.keys.supports(&row.exchange_b) { continue; }

        // Skip disabled exchanges (configured via UI)
        if settings.disabled_exchanges.contains(&row.exchange_a) ||
           settings.disabled_exchanges.contains(&row.exchange_b) { continue; }

        let key = format!("{}_{}_{}", row.pair, row.exchange_a, row.exchange_b);
        let key_rev = format!("{}_{}_{}", row.pair, row.exchange_b, row.exchange_a);
        if open_pair_keys.contains(&key) || open_pair_keys.contains(&key_rev) { continue; }

        let slippage = settings.slippage_percent / 100.0;
        let is_a_higher = row.direction.starts_with(&row.exchange_a);

        let entry_price_a = if is_a_higher { row.price_a * (1.0 - slippage) } else { row.price_a * (1.0 + slippage) };
        let entry_price_b = if is_a_higher { row.price_b * (1.0 + slippage) } else { row.price_b * (1.0 - slippage) };

        info!("Opening REAL trade {} {} {} spread={:.4}% [DRY_RUN={}]",
            row.pair, row.exchange_a, row.exchange_b, row.spread, state.dry_run);

        // ── Concurrent Firing: both legs sent simultaneously ─────────────────
        // Pre-acquire rate limit tokens for BOTH exchanges first, then fire
        // simultaneously. This prevents 429 from collapsing one leg mid-join.
        tokio::join!(
            state.limiters.wait(&row.exchange_a),
            state.limiters.wait(&row.exchange_b)
        );
        let (order_a, order_b) = tokio::join!(
            open_position(
                &state.http, &state.keys,
                &row.exchange_a, &row.pair,
                is_a_higher,
                settings.trade_volume, row.price_a, state.dry_run
            ),
            open_position(
                &state.http, &state.keys,
                &row.exchange_b, &row.pair,
                !is_a_higher,
                settings.trade_volume, row.price_b, state.dry_run
            )
        );

        let order_id_a = match &order_a {
            Ok(id) => Some(id.clone()),
            Err(e) => { error!("[LEG_A] Failed to open on {}: {}", row.exchange_a, e); None }
        };
        let order_id_b = match &order_b {
            Ok(id) => Some(id.clone()),
            Err(e) => { error!("[LEG_B] Failed to open on {}: {}", row.exchange_b, e); None }
        };

        // ── Leg Risk Fallback ────────────────────────────────────────────────
        // If only one leg filled, we have a naked directional position.
        // Emergency close the open leg with exponential backoff retry.
        if !state.dry_run {
            match (order_id_a.is_some(), order_id_b.is_some()) {
                (true, false) => {
                    error!("🚨 LEG ASYMMETRY: {} OPEN, {} FAILED — emergency close A!",
                        row.exchange_a, row.exchange_b);
                    let (http, keys) = (state.http.clone(), state.keys.clone());
                    let (exchange, pair) = (row.exchange_a.clone(), row.pair.clone());
                    let (volume, price, dry_run) = (settings.trade_volume, row.price_a, state.dry_run);
                    tokio::spawn(async move {
                        for attempt in 1u32..=5 {
                            match close_position(&http, &keys, &exchange, &pair, is_a_higher, volume, price, dry_run).await {
                                Ok(id) => { error!("🛡️  FALLBACK: A closed OK order_id={}", id); break; }
                                Err(e) => {
                                    let wait_ms = 300 * 2u64.pow(attempt - 1);
                                    error!("FALLBACK attempt {}/5 failed: {} — retry in {}ms", attempt, e, wait_ms);
                                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                                }
                            }
                        }
                    });
                    continue;
                }
                (false, true) => {
                    error!("🚨 LEG ASYMMETRY: {} FAILED, {} OPEN — emergency close B!",
                        row.exchange_a, row.exchange_b);
                    let (http, keys) = (state.http.clone(), state.keys.clone());
                    let (exchange, pair) = (row.exchange_b.clone(), row.pair.clone());
                    let (volume, price, dry_run) = (settings.trade_volume, row.price_b, state.dry_run);
                    tokio::spawn(async move {
                        for attempt in 1u32..=5 {
                            match close_position(&http, &keys, &exchange, &pair, !is_a_higher, volume, price, dry_run).await {
                                Ok(id) => { error!("🛡️  FALLBACK: B closed OK order_id={}", id); break; }
                                Err(e) => {
                                    let wait_ms = 300 * 2u64.pow(attempt - 1);
                                    error!("FALLBACK attempt {}/5 failed: {} — retry in {}ms", attempt, e, wait_ms);
                                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                                }
                            }
                        }
                    });
                    continue;
                }
                (false, false) => { error!("Both legs failed, skipping."); continue; }
                (true, true) => { /* Both filled — proceed to record trade */ }
            }
        } else if order_id_a.is_none() || order_id_b.is_none() {
            continue; // dry_run: simulate skip
        }

        // ── Partial Fill Guard ────────────────────────────────────────────────
        // Market orders on thin pairs may fill partially. Verify actual volume
        // before recording, so PnL and trailing-stop math is accurate.
        let requested_contracts = (settings.trade_volume / row.price_a.max(0.001)).ceil();
        let actual_vol = if !state.dry_run {
            let oid_a = order_id_a.as_deref().unwrap_or("");
            let oid_b = order_id_b.as_deref().unwrap_or("");
            let (fill_a, fill_b) = tokio::join!(
                get_order_fill(&state.http, &state.keys, &row.exchange_a, &row.pair, oid_a, requested_contracts),
                get_order_fill(&state.http, &state.keys, &row.exchange_b, &row.pair, oid_b, requested_contracts)
            );
            let max_fill = fill_a.max(fill_b);
            let min_fill = fill_a.min(fill_b);
            if max_fill > 0.0 && (max_fill - min_fill) / max_fill > 0.01 {
                warn!("⚠️  PARTIAL FILL: {} fill_a={:.1} fill_b={:.1} — recording min={:.1} contracts",
                    row.pair, fill_a, fill_b, min_fill);
            }
            // Record min fill converted back to USD
            let effective_contracts = if min_fill > 0.0 { min_fill } else { requested_contracts };
            effective_contracts * row.price_a
        } else {
            settings.trade_volume
        };

        let pair = row.pair.clone();
        let ex_a = row.exchange_a.clone();
        let ex_b = row.exchange_b.clone();
        let dir = row.direction.clone();
        let vol = actual_vol;
        let spread = row.spread;

        match state.db.call(move |conn| {
            conn.execute(
                "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,
                                     entry_price_a,entry_price_b,entry_spread,entry_time,
                                     highest_spread_reached,status,pnl_a,pnl_b,close_reason,order_id_a,order_id_b)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![pair, ex_a, ex_b, dir, vol, entry_price_a, entry_price_b, spread, now_ms(),
                    -9999.0, "OPEN", 0.0, 0.0, None::<String>, order_id_a, order_id_b],
            ).map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
        }).await {
            Ok(_) => {
                num_active += 1;
                open_pair_keys.insert(key);
                open_tickers.insert(row.pair.clone());
            }
            Err(e) => error!("DB error opening trade: {}", e),
        }
    }


    // Push active trades to Cloudflare Worker
    let final_trades: Vec<Trade> = match state.db.call(|conn| {
        let mut stmt = conn.prepare(
            "SELECT id,pair,exchange_a,exchange_b,direction,volume,
                    entry_price_a,entry_price_b,entry_spread,entry_time,
                    highest_spread_reached,status,
                    exit_price_a,exit_price_b,exit_spread,exit_time,pnl,
                    commission_cost,funding_cost,
                    last_price_a,last_price_b,last_spread,curr_gross_pnl,
                    pnl_a,pnl_b,close_reason,order_id_a,order_id_b
             FROM trades WHERE status='OPEN'"
        )?;
        let rows = stmt
            .query_map([], row_to_trade)?
            .filter_map(|r| r.ok())
            .collect::<Vec<_>>();
        Ok(rows)
    }).await {
        Ok(t) => t,
        Err(_) => vec![],
    };

    let mut should_push = false;
    {
        let mut last_push = state.last_push_time.lock().await;
        if last_push.elapsed() > Duration::from_secs(30) || final_trades.len() != open_trades.len() {
            should_push = true;
            *last_push = std::time::Instant::now();
        }
    }
    if should_push {
        let state_clone = state.clone();
        tokio::spawn(async move {
            push_active_trades(&state_clone, &final_trades).await;
        });
    }
}

// ─── Axum helpers ─────────────────────────────────────────────────────────────

fn json_ok(data: impl Serialize) -> Response {
    (StatusCode::OK, axum::Json(data)).into_response()
}

fn json_err(status: StatusCode, msg: &str) -> Response {
    (status, axum::Json(json!({ "error": msg }))).into_response()
}

fn check_token(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == format!("Bearer {}", expected))
        .unwrap_or(false)
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

async fn handle_internal_spreads(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SpreadPayload>,
) -> Response {
    if !check_token(&headers, &state.internal_token) {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    let spreads_count = payload.spreads.len();
    info!("real-trader received {} spreads [dry_run={}]", spreads_count, state.dry_run);

    let state2 = state.clone();
    tokio::spawn(async move {
        run_trading_engine(&state2, &payload).await;
    });

    json_ok(json!({ "ok": true, "spreads": spreads_count }))
}

async fn handle_get_trades(State(state): State<AppState>) -> Response {
    let trades = match state.db.call(|conn| {
        let mut stmt = conn.prepare(
            "SELECT id,pair,exchange_a,exchange_b,direction,volume,
                    entry_price_a,entry_price_b,entry_spread,entry_time,
                    highest_spread_reached,status,
                    exit_price_a,exit_price_b,exit_spread,exit_time,pnl,
                    commission_cost,funding_cost,
                    last_price_a,last_price_b,last_spread,curr_gross_pnl,
                    pnl_a,pnl_b,close_reason,order_id_a,order_id_b
             FROM trades WHERE status='OPEN' ORDER BY entry_time DESC"
        )?;
        let rows = stmt
            .query_map([], row_to_trade)?
            .filter_map(|r| r.ok())
            .collect::<Vec<_>>();
        Ok(rows)
    }).await {
        Ok(t) => t,
        Err(e) => { error!("DB error: {}", e); return json_err(StatusCode::INTERNAL_SERVER_ERROR, "DB error"); }
    };
    json_ok(json!({ "active": trades, "count": trades.len() }))
}

async fn handle_delete_trades(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if !check_token(&headers, &state.internal_token) {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }
    if let Err(e) = state.db.call(|conn| {
        conn.execute("DELETE FROM trades", []).map_err(|e| tokio_rusqlite::Error::Rusqlite(e))
    }).await {
        return json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }
    json_ok(json!({ "ok": true }))
}

async fn handle_health() -> Response {
    json_ok(json!({ "status": "ok", "service": "real-trader" }))
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("real_trader=info".parse()?)
        )
        .init();

    let worker_url = std::env::var("WORKER_URL")
        .unwrap_or_else(|_| "https://api.arb.bagow.com".into());
    let collector_token = std::env::var("COLLECTOR_TOKEN").unwrap_or_default();
    let internal_token = std::env::var("INTERNAL_TOKEN").unwrap_or_default();
    let port: u16 = std::env::var("REAL_TRADER_PORT")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(3002);
    let dry_run = std::env::var("DRY_RUN")
        .map(|v| v.to_lowercase() != "false")
        .unwrap_or(true);

    let keys = ExchangeKeys::from_env();

    if dry_run {
        warn!("🧪 DRY_RUN=true — No real orders will be placed! Set DRY_RUN=false to enable.");
    } else {
        warn!("🔴 DRY_RUN=false — REAL orders WILL be placed on exchanges!");
    }

    info!("Supported exchanges: gate={} htx={} kucoin={}",
        keys.supports("gate"), keys.supports("htx"), keys.supports("kucoin"));

    let db_path = std::env::var("DB_PATH").unwrap_or_else(|_| "real-trader.db".to_string());
    let db = tokio_rusqlite::Connection::open(&db_path).await
        .context("Failed to open SQLite DB")?;
    init_db(&db).await?;

    let http = Client::builder()
        .user_agent("real-trader/1.0")
        .build()?;

    // ── Local settings from profiles DB (replaces Cloudflare HTTP fetch) ──────
    // real-trader reads trading_profiles directly from vps-api.db instead of
    // calling the Cloudflare Worker. Hot path gets settings in nanoseconds.
    let profiles_db_path = std::env::var("PROFILES_DB_PATH")
        .unwrap_or_else(|_| "vps-api.db".to_string());
    let profiles_db_opt = match tokio_rusqlite::Connection::open(&profiles_db_path).await {
        Ok(c) => { info!("Profiles DB: {}", profiles_db_path); Some(c) }
        Err(e) => { warn!("Cannot open profiles DB '{}': {} — using defaults", profiles_db_path, e); None }
    };
    let initial_settings = match &profiles_db_opt {
        Some(pdb) => load_settings_from_db(pdb).await,
        None => TradingSettings::default(),
    };
    info!("Settings: active={} min_spread={:.3} vol={} max_trades={}",
        initial_settings.is_active, initial_settings.min_entry_spread,
        initial_settings.trade_volume, initial_settings.max_active_trades);
    let settings: Arc<RwLock<TradingSettings>> = Arc::new(RwLock::new(initial_settings));

    // Background refresh — polls profiles DB every 5s, handles CLEAR_TRADES locally.
    if let Some(pdb) = profiles_db_opt {
        let settings_bg = Arc::clone(&settings);
        let trades_db_bg = db.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let fresh = load_settings_from_db(&pdb).await;
                if fresh.pending_commands.as_deref() == Some("CLEAR_TRADES") {
                    let pid = fresh.id.clone();
                    info!("Executing CLEAR_TRADES for profile {}", pid);
                    let _ = trades_db_bg.call(|conn| {
                        conn.execute("DELETE FROM trades", []).map_err(tokio_rusqlite::Error::Rusqlite)
                    }).await;
                    let _ = pdb.call(move |conn| {
                        conn.execute(
                            "UPDATE trading_profiles SET pending_commands=NULL WHERE id=?",
                            rusqlite::params![pid],
                        ).map_err(tokio_rusqlite::Error::Rusqlite)
                    }).await;
                }
                *settings_bg.write().await = fresh;
            }
        });
    }

    let state = AppState {
        db,
        http,
        worker_url,
        collector_token,
        internal_token,
        dry_run,
        keys,
        engine_lock: Arc::new(Mutex::new(())),
        settings,
        limiters: ExchangeRateLimiters::new(),
        last_push_time: Arc::new(Mutex::new(std::time::Instant::now())),
    };

    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_headers(Any)
        .allow_origin(Any);

    let app = Router::new()
        .route("/internal/spreads", post(handle_internal_spreads))
        .route("/api/trading/trades", get(handle_get_trades))
        .route("/api/trading/trades", delete(handle_delete_trades))
        .route("/health", get(handle_health))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("real-trader listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
