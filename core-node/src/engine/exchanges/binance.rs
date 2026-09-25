use async_trait::async_trait;
use dashmap::DashSet;
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use super::traits::{CloseReceipt, ExchangeConnector, PositionInfo, TradeAuditResult};

// ── LEVERAGE & MARGIN TYPE CACHE ───────────────────────────────────────────
// Prevents re-setting leverage/margin on every tick (was ~100 WS reqs per 100ms!).
// Key = "SYMBOL:LEV" e.g. "BEATUSDT:10".
// Once set successfully, never retry until process restart.
static LEVERAGE_CACHE: OnceLock<DashSet<String>> = OnceLock::new();
fn leverage_cache() -> &'static DashSet<String> {
    LEVERAGE_CACHE.get_or_init(|| DashSet::new())
}

static MARGIN_TYPE_CACHE: OnceLock<DashSet<String>> = OnceLock::new();
fn margin_type_cache() -> &'static DashSet<String> {
    MARGIN_TYPE_CACHE.get_or_init(|| DashSet::new())
}

// ── REST FALLBACK RATE LIMITER ──────────────────────────────────────────────
// Prevents REST fallback spam when WS is dead.
// Each method gets its own cooldown (last REST call timestamp).
static REST_FALLBACK_BALANCE_MS: AtomicI64 = AtomicI64::new(0);
static REST_FALLBACK_POSITIONS_MS: AtomicI64 = AtomicI64::new(0);
static REST_FALLBACK_LEVERAGE_MS: AtomicI64 = AtomicI64::new(0);
static WS_CONSECUTIVE_FAILURES: AtomicU32 = AtomicU32::new(0);

const REST_FALLBACK_COOLDOWN_MS: i64 = 3_000; // 1 REST fallback per 3 seconds per method
const WS_UNHEALTHY_LOG_THRESHOLD: u32 = 5;

/// Check if REST fallback is allowed for a given method.
/// A dead WS is exactly when the bounded REST fallback is needed. The shared
/// Binance weight guard and this per-method cooldown limit REST traffic.
fn check_rest_fallback_allowed(last_call: &AtomicI64, method: &str) -> Result<(), String> {
    let now = now_ms() as i64;
    let last = last_call.load(Ordering::Relaxed);
    if now - last < REST_FALLBACK_COOLDOWN_MS {
        let remaining = (REST_FALLBACK_COOLDOWN_MS - (now - last)) / 1000;
        return Err(format!(
            "Binance REST fallback {} on cooldown ({}s remaining)",
            method, remaining
        ));
    }
    last_call.store(now, Ordering::Relaxed);
    Ok(())
}

/// Record a successful WS response (resets failure counter).
fn record_ws_success() {
    WS_CONSECUTIVE_FAILURES.store(0, Ordering::Relaxed);
}

/// Record a WS failure.
fn record_ws_failure() {
    let prev = WS_CONSECUTIVE_FAILURES.fetch_add(1, Ordering::Relaxed);
    if prev + 1 == WS_UNHEALTHY_LOG_THRESHOLD {
        tracing::warn!(
            "[binance] WS unhealthy after {} consecutive failures; bounded REST fallbacks remain enabled",
            prev + 1
        );
    }
}

/// PUBLIC: Reset WS failure counter (called when WS successfully reconnects).
pub fn reset_ws_failures() {
    let prev = WS_CONSECUTIVE_FAILURES.swap(0, Ordering::Relaxed);
    if prev >= WS_UNHEALTHY_LOG_THRESHOLD {
        tracing::info!(
            "[binance] WS failure counter reset after reconnect (was {})",
            prev
        );
    }
}
use crate::engine::real::{binance_sign, now_ms, ExchangeKeys};

pub struct BinanceConnector;

// ── IP BAN + WEIGHT TRACKING ────────────────────────────────────────────────
// Single source of truth for ALL Binance REST rate limiting.
// Used by: engine connector, collector, ema_warmup, prewarm.
//
// Binance Futures limits:
//   - 2400 weight per minute (per IP)
//   - 429 = rate limit hit → must stop immediately
//   - 418 = IP auto-ban (after ignoring 429) → 2 min to 3 DAYS
//   - ANY request during ban EXTENDS the ban!

static BINANCE_BANNED_UNTIL_MS: AtomicI64 = AtomicI64::new(0);
static LAST_BAN_LOG_MS: AtomicI64 = AtomicI64::new(0);
static BAN_COUNT: AtomicU32 = AtomicU32::new(0);
static USED_WEIGHT_1M: AtomicU32 = AtomicU32::new(0);
static WEIGHT_WINDOW_RESET_MS: AtomicI64 = AtomicI64::new(0);

const WEIGHT_LIMIT: u32 = 2400;
const WEIGHT_HARD_STOP: u32 = 2200; // Block new requests above this

/// PUBLIC: Check ban + weight before ANY Binance REST request.
/// Call from collector, ema_warmup, or any code that hits fapi.binance.com.
pub fn check_before_binance_rest() -> Result<(), String> {
    let now = now_ms() as i64;

    // 1. Check IP ban
    let banned_until = BINANCE_BANNED_UNTIL_MS.load(Ordering::Relaxed);
    if banned_until > 0 && now < banned_until {
        let remaining_s = (banned_until - now) / 1000;
        let last_log = LAST_BAN_LOG_MS.load(Ordering::Relaxed);
        if now - last_log > 60_000 {
            LAST_BAN_LOG_MS.store(now, Ordering::Relaxed);
            tracing::warn!(
                "[binance] 🚫 IP banned for {}s more. ALL REST requests blocked.",
                remaining_s
            );
        }
        return Err(format!("Binance IP banned for {}s more", remaining_s));
    }
    // Ban expired? Clear.
    if banned_until > 0 && now >= banned_until {
        BINANCE_BANNED_UNTIL_MS.store(0, Ordering::Relaxed);
        BAN_COUNT.store(0, Ordering::Relaxed);
        tracing::info!("[binance] ✅ IP ban expired. Resuming normal operations.");
    }

    // 2. Check weight — reset window if expired
    let reset_at = WEIGHT_WINDOW_RESET_MS.load(Ordering::Relaxed);
    if now > reset_at {
        USED_WEIGHT_1M.store(0, Ordering::Relaxed);
        WEIGHT_WINDOW_RESET_MS.store(now + 60_000, Ordering::Relaxed);
    }

    // 3. Block if weight too high
    let used = USED_WEIGHT_1M.load(Ordering::Relaxed);
    if used >= WEIGHT_HARD_STOP {
        let wait_s = (reset_at - now).max(0) / 1000;
        tracing::warn!(
            "[binance] ⚠️ Weight {}/{} near limit! Blocking. Reset in {}s.",
            used,
            WEIGHT_LIMIT,
            wait_s
        );
        return Err(format!("Binance weight {}/{} too high", used, WEIGHT_LIMIT));
    }

    Ok(())
}

/// PUBLIC: Is Binance currently banned? (no-request check)
pub fn is_binance_banned() -> bool {
    let until = BINANCE_BANNED_UNTIL_MS.load(Ordering::Relaxed);
    until > 0 && (now_ms() as i64) < until
}

/// PUBLIC: Remaining ban time in seconds (0 if not banned).
pub fn binance_ban_remaining_secs() -> i64 {
    let until = BINANCE_BANNED_UNTIL_MS.load(Ordering::Relaxed);
    if until <= 0 {
        return 0;
    }
    ((until - now_ms() as i64) / 1000).max(0)
}

/// PUBLIC: How much weight remains in the current window.
pub fn binance_remaining_weight() -> u32 {
    let now = now_ms() as i64;
    let reset_at = WEIGHT_WINDOW_RESET_MS.load(Ordering::Relaxed);
    if now > reset_at {
        return WEIGHT_LIMIT;
    }
    WEIGHT_LIMIT.saturating_sub(USED_WEIGHT_1M.load(Ordering::Relaxed))
}

/// PUBLIC: Set a ban directly (e.g. from Binance error "banned until TIMESTAMP").
pub fn set_binance_ban(until_ms: i64) {
    BINANCE_BANNED_UNTIL_MS.fetch_max(until_ms, Ordering::Relaxed);
    BAN_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// PUBLIC: Record response from Binance REST. Tracks weight + detects bans.
pub fn record_binance_response(status: u16, headers: &reqwest::header::HeaderMap) {
    let now = now_ms() as i64;

    // Parse X-MBX-USED-WEIGHT-1M
    if let Some(w) = headers.get("X-MBX-USED-WEIGHT-1M") {
        if let Ok(weight) = w.to_str().unwrap_or("").parse::<u32>() {
            let old = USED_WEIGHT_1M.swap(weight, Ordering::Relaxed);
            let reset_at = WEIGHT_WINDOW_RESET_MS.load(Ordering::Relaxed);
            if now > reset_at || weight < old {
                WEIGHT_WINDOW_RESET_MS.store(now + 60_000, Ordering::Relaxed);
            }
            if weight > 1800 {
                tracing::warn!("[binance] ⚠️ HIGH WEIGHT: {}/{}", weight, WEIGHT_LIMIT);
            }
        }
    }

    // 429: rate limit
    if status == 429 {
        let ban_count = BAN_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        let base_s: i64 = match ban_count {
            1 => 120,
            2 => 300,
            3 => 600,
            _ => 1800,
        };
        let retry_s = headers
            .get("Retry-After")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let cooldown = retry_s.max(base_s);
        let ban_until = now + cooldown * 1000 + 10_000;
        BINANCE_BANNED_UNTIL_MS.fetch_max(ban_until, Ordering::Relaxed);
        tracing::error!(
            "[binance] 🔴 RATE LIMIT (429)! Ban #{} — backoff {}s.",
            ban_count,
            cooldown
        );
    }

    // 418: IP auto-ban
    if status == 418 {
        let ban_count = BAN_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        let base_s: i64 = match ban_count {
            1 => 600,
            2 => 1800,
            3 => 7200,
            _ => 21600,
        };
        let retry_s = headers
            .get("Retry-After")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let cooldown = retry_s.max(base_s);
        let ban_until = now + cooldown * 1000 + 30_000;
        BINANCE_BANNED_UNTIL_MS.fetch_max(ban_until, Ordering::Relaxed);
        tracing::error!(
            "[binance] 💀 IP BANNED (418)! Ban #{} — NO requests for {}s! \
             Every request EXTENDS the ban!",
            ban_count,
            cooldown
        );
    }
}

fn parse_public_depth_snapshot(body: &Value, received_ms: i64) -> Result<crate::state::L2Snapshot, String> {
    fn levels(body: &Value, key: &str) -> Result<Vec<(f64, f64)>, String> {
        body[key]
            .as_array()
            .ok_or_else(|| format!("Binance depth response missing {key}"))?
            .iter()
            .map(|level| {
                let values = level
                    .as_array()
                    .ok_or_else(|| format!("Invalid Binance depth {key} level"))?;
                let price = values.first().and_then(Value::as_str)
                    .and_then(|value| value.parse::<f64>().ok())
                    .ok_or_else(|| format!("Invalid Binance depth {key} price"))?;
                let quantity = values.get(1).and_then(Value::as_str)
                    .and_then(|value| value.parse::<f64>().ok())
                    .ok_or_else(|| format!("Invalid Binance depth {key} quantity"))?;
                if !price.is_finite() || !quantity.is_finite() || price <= 0.0 || quantity <= 0.0 {
                    return Err(format!("Non-positive Binance depth {key} level"));
                }
                Ok((price, quantity))
            })
            .collect()
    }

    let bids = levels(body, "bids")?;
    let asks = levels(body, "asks")?;
    if bids.is_empty()
        || asks.is_empty()
        || bids[0].0 >= asks[0].0
        || bids.windows(2).any(|levels| levels[0].0 < levels[1].0)
        || asks.windows(2).any(|levels| levels[0].0 > levels[1].0)
    {
        return Err("Invalid or unsorted Binance depth response".into());
    }
    let exchange_ms = body["E"]
        .as_i64()
        .ok_or("Binance depth response missing exchange timestamp")?;
    if !(0..=750).contains(&(received_ms - exchange_ms)) {
        return Err(format!("Stale Binance depth snapshot: {}ms", received_ms - exchange_ms));
    }
    Ok(crate::state::L2Snapshot { bids, asks, updated_ms: exchange_ms })
}

fn parse_public_depth_ws_response(
    body: &Value,
    received_ms: i64,
) -> Result<crate::state::L2Snapshot, String> {
    if body["status"].as_i64() != Some(200) {
        return Err(format!(
            "Binance public WS depth returned status {:?}: {}",
            body["status"].as_i64(),
            body.get("error").unwrap_or(&Value::Null)
        ));
    }
    let result = body["result"]
        .as_object()
        .ok_or("Binance public WS depth response missing result")?;
    // Binance's current USD-M schema documents E/T inside result while bids
    // and asks are top-level. Accept nested levels too so a schema correction
    // cannot silently remove the independent hedge snapshot.
    let normalized = json!({
        "E": result
            .get("E")
            .or_else(|| body.get("E"))
            .cloned()
            .unwrap_or(Value::Null),
        "bids": body
            .get("bids")
            .or_else(|| result.get("bids"))
            .cloned()
            .unwrap_or(Value::Null),
        "asks": body
            .get("asks")
            .or_else(|| result.get("asks"))
            .cloned()
            .unwrap_or(Value::Null),
    });
    parse_public_depth_snapshot(&normalized, received_ms)
}

/// Fetch a just-in-time public top-5 snapshot for the final IOC preflight.
/// This remains fail-closed and uses the same 750 ms exchange timestamp budget as WS depth.
pub async fn fetch_public_depth_snapshot(http: &Client, pair: &str) -> Result<crate::state::L2Snapshot, String> {
    check_before_binance_rest()?;
    let symbol = crate::engine::utils::normalize_symbol(pair);
    let response = http
        .get("https://fapi.binance.com/fapi/v1/depth")
        .query(&[("symbol", symbol.as_str()), ("limit", "5")])
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .map_err(|error| format!("Binance public depth request failed: {error}"))?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .json::<Value>()
        .await
        .map_err(|error| format!("Invalid Binance public depth response: {error}"))?;
    check_rate_limits(status, &headers, Some(&body)).await?;
    if !status.is_success() {
        return Err(format!("Binance public depth returned HTTP {}", status.as_u16()));
    }
    parse_public_depth_snapshot(&body, now_ms() as i64)
}

/// Fetch the same bounded top-5 snapshot through Binance's independent
/// request/response WebSocket API. The public `depth` method needs no API key.
pub async fn fetch_public_depth_snapshot_ws(
    pair: &str,
) -> Result<crate::state::L2Snapshot, String> {
    use crate::engine::exchanges::ws_trader::send_ws_request;

    let response = send_ws_request(json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "method": "depth",
        "params": {
            "symbol": crate::engine::utils::normalize_symbol(pair),
            "limit": 5,
        }
    }))
    .await
    .map_err(|error| format!("Binance public WS depth request failed: {error}"))?;
    parse_public_depth_ws_response(&response, now_ms() as i64)
}

// ── Private helpers (used internally by connector methods) ───────────────────

fn check_ip_ban() -> Result<(), String> {
    check_before_binance_rest()
}

fn detect_ban_from_response(body: &Value) {
    let code = body["code"].as_i64().unwrap_or(0);
    let msg = body["msg"].as_str().unwrap_or("");
    if code == -1003 || code == -1015 {
        if let Some(pos) = msg.find("banned until ") {
            let after = &msg[pos + 13..];
            let ts_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(ban_ts) = ts_str.parse::<i64>() {
                set_binance_ban(ban_ts);
                crate::engine::risk_manager::ban_exchange("binance", ban_ts);
            }
        } else {
            let cooldown = now_ms() as i64 + 300_000;
            set_binance_ban(cooldown);
        }
    }
}

async fn check_rate_limits(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
    body: Option<&Value>,
) -> Result<(), String> {
    // Track weight + ban from headers
    record_binance_response(status.as_u16(), headers);
    // Check body for API-level ban codes
    if let Some(b) = body {
        detect_ban_from_response(b);
    }
    if status.as_u16() == 429 {
        return Err("Binance 429 rate limit hit".into());
    }
    if status.as_u16() == 418 {
        return Err("Binance 418 IP banned".into());
    }
    Ok(())
}

async fn execute_binance_order(
    _http: &Client,
    keys: &ExchangeKeys,
    params: &serde_json::Map<String, Value>,
    _api_timeout_ms: u64,
    managed: bool,
) -> Result<Value, String> {
    check_ip_ban()?;
    use crate::engine::exchanges::ws_trader::send_ws_request;

    // 1. Build signature for WS (must include apiKey)
    let mut ws_params = params.clone();
    ws_params.insert("apiKey".to_string(), serde_json::json!(keys.binance_key));

    let mut ws_sorted_keys: Vec<&String> = ws_params.keys().collect();
    ws_sorted_keys.sort();
    let ws_qs: String = ws_sorted_keys
        .iter()
        .map(|k| {
            let v = &ws_params[k.as_str()];
            let vs = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                _ => v.to_string(),
            };
            format!("{}={}", k, vs)
        })
        .collect::<Vec<_>>()
        .join("&");

    let ws_sig = binance_sign(&keys.binance_secret, &ws_qs);
    ws_params.insert("signature".to_string(), serde_json::json!(ws_sig));

    let req_id = uuid::Uuid::new_v4().to_string();
    let ws_req = serde_json::json!({
        "id": req_id,
        "method": "order.place",
        "params": ws_params
    });

    match send_ws_request(ws_req).await {
        Ok(v_ws) => {
            if let Some(status) = v_ws.get("status") {
                if status.as_i64() == Some(200) {
                    if let Some(res) = v_ws.get("result") {
                        return Ok(res.clone());
                    }
                }
            }
            if let Some(err) = v_ws.get("error") {
                detect_ban_from_response(err);
                if managed
                    && matches!(
                        err["code"].as_i64(),
                        Some(
                            -1021
                                | -1022
                                | -1100
                                | -1102
                                | -1111
                                | -1121
                                | -2010
                                | -2019
                                | -2022
                                | -4003
                                | -4004
                                | -4005
                                | -4014
                                | -4023
                                | -4164
                        )
                    )
                {
                    return Ok(json!({"managed_rejection":true}));
                }
                tracing::error!("[binance_ws] order error: {}", err);
                return Err(format!(
                    "Binance WS API Error: {}",
                    err["msg"].as_str().unwrap_or("?")
                ));
            }
            Ok(v_ws.get("result").cloned().unwrap_or(v_ws))
        }
        Err(e) => Err(format!("Binance submit outcome UNKNOWN: {}", e)),
    }
}

fn format_binance_qty(phys_amount: f64, step_size: f64) -> String {
    let step = step_size.max(1e-12);
    let precision = if step < 1.0 {
        let s = format!("{:.8}", step);
        let decimals = s
            .trim_end_matches('0')
            .split('.')
            .nth(1)
            .unwrap_or("")
            .len();
        decimals.min(8)
    } else {
        0
    };
    let rounded = (phys_amount / step).round() * step;
    let mut formatted = format!("{:.*}", precision, rounded);
    if formatted.ends_with('.') {
        formatted.pop();
    }
    formatted
}

fn format_binance_price(price: f64, tick_size: f64) -> String {
    let tick = tick_size.max(1e-12);
    let precision = if tick < 1.0 {
        let s = format!("{:.8}", tick);
        let decimals = s
            .trim_end_matches('0')
            .split('.')
            .nth(1)
            .unwrap_or("")
            .len();
        decimals.min(8)
    } else {
        0
    };
    let rounded = (price / tick).round() * tick;
    let mut formatted = format!("{:.*}", precision, rounded);
    if formatted.ends_with('.') {
        formatted.pop();
    }
    formatted
}

#[async_trait]
impl ExchangeConnector for BinanceConnector {
    async fn symbol_has_no_open_orders(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
    ) -> Result<bool, String> {
        let qs = format!("symbol={pair}&timestamp={}", now_ms());
        let v = managed_binance_get(http, keys, "/fapi/v1/openOrders", &qs).await?;
        Ok(v.as_array()
            .ok_or("Invalid Binance open orders")?
            .is_empty())
    }

    async fn funding_events(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::engine::orders::FundingEvent>, String> {
        use crate::engine::orders::{identity, number, FundingEvent};
        let mut out = Vec::new();
        for page in 1..=100 {
            let q=format!("symbol={pair}&incomeType=FUNDING_FEE&startTime={from}&endTime={to}&limit=1000&page={page}&timestamp={}",now_ms());
            let v = managed_binance_get(http, keys, "/fapi/v1/income", &q).await?;
            let rows = v.as_array().ok_or("Invalid Binance funding ledger")?;
            for r in rows {
                if r["symbol"].as_str() != Some(pair)
                    || r["incomeType"].as_str() != Some("FUNDING_FEE")
                    || r["asset"].as_str() != Some("USDT")
                {
                    return Err("Binance funding attribution mismatch".into());
                }
                let time_ms = number(r, "time")? as i64;
                if time_ms < from || time_ms > to {
                    return Err("Binance funding outside requested range".into());
                }
                out.push(FundingEvent {
                    id: identity(r, "tranId").ok_or("Missing funding identity")?,
                    time_ms,
                    cost: -number(r, "income")?,
                });
            }
            if rows.len() < 1000 {
                return Ok(out);
            }
        }
        Err("Binance funding pagination incomplete".into())
    }
    fn supports_managed_orders(&self) -> bool {
        true
    }
    async fn submit_managed(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        r: &crate::engine::orders::OrderRequest,
    ) -> Result<crate::engine::orders::OrderObservation, String> {
        r.validate()?;
        let mut p=json!({"symbol":r.pair,"side":if r.is_buy{"BUY"}else{"SELL"},"quantity":format_binance_qty(r.quantity,r.multiplier),"newClientOrderId":r.client_id,"timestamp":now_ms(),"newOrderRespType":"RESULT","type":if r.limit_price>0.0{"LIMIT"}else{"MARKET"}}).as_object().unwrap().clone();
        if r.reduce_only {
            p.insert("reduceOnly".into(), json!("true"));
        }
        if r.limit_price > 0.0 {
            p.insert(
                "price".into(),
                json!(format_binance_price(
                    crate::engine::orders::bounded_price(r.limit_price, r.tick_size, r.is_buy),
                    r.tick_size
                )),
            );
            p.insert("timeInForce".into(), json!("IOC"));
        }
        let v = execute_binance_order(http, keys, &p, 2000, true).await?;
        if v["managed_rejection"].as_bool() == Some(true) {
            return Ok(crate::engine::orders::OrderObservation::rejected());
        }
        match parse_managed_binance(&v, r) {
            Ok(observation) => Ok(observation),
            Err(parse_error) => {
                let order_id = filled_order_missing_average(&v, &parse_error);
                let Some(order_id) = order_id else {
                    return Err(parse_error);
                };
                // Binance can acknowledge a filled IOC with executedQty and an
                // orderId while omitting both avgPrice and cumQuote. The order is
                // already live exchange state, so recover it immediately by its
                // authoritative identity instead of discarding that identity and
                // waiting for generic unknown-submit reconciliation.
                tracing::warn!(
                    "[binance] Filled order.place response omitted average price for {}; querying orderId={} immediately",
                    r.client_id,
                    order_id,
                );
                self.query_managed(http, keys, r, Some(&order_id))
                    .await
                    .map_err(|query_error| {
                        format!(
                            "{}; immediate orderId={} recovery failed: {}",
                            parse_error, order_id, query_error
                        )
                    })
            }
        }
    }
    async fn query_managed(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        r: &crate::engine::orders::OrderRequest,
        oid: Option<&str>,
    ) -> Result<crate::engine::orders::OrderObservation, String> {
        let selector = match oid {
            Some(id) => format!("orderId={}", id),
            None => format!("origClientOrderId={}", r.client_id),
        };
        let qs = format!("symbol={}&{}&timestamp={}", r.pair, selector, now_ms());
        let v = managed_binance_get(http, keys, "/fapi/v1/order", &qs).await?;
        let mut o = parse_managed_binance(&v, r)?;
        if o.filled_quantity > 0.0 {
            let accounting: Result<Option<f64>, String> = async {
                let id = o.order_id.as_deref().ok_or("Missing exchange order id")?;
                let qs = format!(
                    "symbol={}&orderId={}&limit=1000&timestamp={}",
                    r.pair,
                    id,
                    now_ms()
                );
                if let Ok(fills) = managed_binance_get(http, keys, "/fapi/v1/userTrades", &qs).await
                {
                    if let Some(arr) = fills.as_array() {
                        let mut fee = 0.0;
                        let mut qty = 0.0;
                        let mut seen = std::collections::HashSet::new();
                        let mut valid = true;
                        for f in arr {
                            if crate::engine::orders::identity(f, "orderId").as_deref() != Some(id)
                            {
                                continue;
                            }
                            let fid =
                                crate::engine::orders::identity(f, "id").ok_or("Fill has no id")?;
                            if !seen.insert(fid) {
                                continue;
                            }
                            if f["commissionAsset"].as_str() != Some("USDT") {
                                valid = false;
                                break;
                            }
                            qty += crate::engine::orders::number(f, "qty")?;
                            fee += crate::engine::orders::number(f, "commission")?;
                        }
                        if valid && (qty - o.filled_quantity).abs() <= r.quantity * 1e-8 {
                            return Ok(Some(fee));
                        }
                    }
                }
                Ok(None)
            }
            .await;
            if let Ok(fee) = accounting {
                o.fee = fee;
            }
        } else if o.state.terminal() {
            o.fee = Some(0.0);
        }
        Ok(o)
    }
    fn name(&self) -> &'static str {
        "binance"
    }

    async fn place_order(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        is_buy: bool,
        phys_amount: f64,
        dry_run: bool,
        target_price: f64,
        use_ioc: bool,
        _leverage: f64,
        multiplier: f64,
        slippage_percent: f64,
        tick_size: f64,
        api_timeout_ms: u64,
    ) -> Result<(Option<String>, f64, f64), String> {
        let side = if is_buy { "BUY" } else { "SELL" };
        if dry_run {
            return Ok((
                Some(format!("dry_{}_{}", self.name(), now_ms())),
                target_price,
                0.0,
            ));
        }

        let ts = now_ms();
        let qty_str = format_binance_qty(phys_amount, multiplier);
        if qty_str.is_empty() || qty_str == "0" {
            return Err(format!(
                "Calculated quantity is 0 for amt={} mult={}, check multiplier mismatch",
                phys_amount, multiplier
            ));
        }

        let price = if is_buy {
            target_price * (1.0 + slippage_percent / 100.0)
        } else {
            target_price * (1.0 - slippage_percent / 100.0)
        };
        let price_str = format_binance_price(price, tick_size);

        let mut params = serde_json::Map::new();
        params.insert("symbol".to_string(), serde_json::json!(pair));
        params.insert("side".to_string(), serde_json::json!(side));
        params.insert("quantity".to_string(), serde_json::json!(qty_str));
        params.insert("timestamp".to_string(), serde_json::json!(ts));

        // MARKET vs IOC LIMIT
        if use_ioc && target_price > 0.0 {
            tracing::info!(
                "[binance] IOC {} {} target={:.8} slip={}% → sent_price={} tick={}",
                pair,
                side,
                target_price,
                slippage_percent,
                price_str,
                tick_size
            );
            params.insert("type".to_string(), serde_json::json!("LIMIT"));
            params.insert("timeInForce".to_string(), serde_json::json!("IOC"));
            params.insert("price".to_string(), serde_json::json!(price_str));
        } else {
            params.insert("type".to_string(), serde_json::json!("MARKET"));
        }
        // CRITICAL: Without RESULT, Binance WS returns ACK (status=NEW) with no fill data.
        // With RESULT, the WS response contains the actual execution result inline.
        params.insert("newOrderRespType".to_string(), serde_json::json!("RESULT"));

        let v = execute_binance_order(http, keys, &params, api_timeout_ms, false).await?;

        // Diagnostic: log order request and response
        let resp_oid = v["orderId"]
            .as_i64()
            .map(|i| i.to_string())
            .unwrap_or_else(|| "none".to_string());
        let resp_status_str = v["status"].as_str().unwrap_or("?");
        tracing::info!(
            "[binance] place_order {} side={} qty={} → orderId={} status={}",
            pair,
            side,
            qty_str,
            resp_oid,
            resp_status_str
        );

        let order_id = v["orderId"].as_i64().map(|id| id.to_string());
        let order_status = v["status"].as_str().unwrap_or("?");

        if order_id.is_some() {
            // Extract inline fill data from WS response
            let exec_qty: f64 = v["executedQty"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| v["executedQty"].as_f64())
                .unwrap_or(0.0);
            let avg_price: f64 = v["avgPrice"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| v["avgPrice"].as_f64())
                .unwrap_or(0.0);
            let cum_fee: f64 = v["cumCommission"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| v["cumCommission"].as_f64())
                .unwrap_or(0.0)
                .abs();

            // Log FULL WS response for diagnostics
            let resp_price = v["price"].as_str().unwrap_or("?");
            let resp_side = v["side"].as_str().unwrap_or("?");
            let resp_orig_qty = v["origQty"].as_str().unwrap_or("?");
            let resp_tif = v["timeInForce"].as_str().unwrap_or("?");
            tracing::info!(
                "[binance] place_order {} WS result: status={} execQty={} avgPrice={} fee={} | sent_price={} side={} origQty={} tif={}",
                pair, order_status, exec_qty, avg_price, cum_fee,
                resp_price, resp_side, resp_orig_qty, resp_tif
            );

            // ── WORKAROUND: Binance Futures WS API returns avgPrice=0 for MARKET orders ──
            // Calculate from cumQuote / executedQty as primary fallback.
            let mut final_avg_price = avg_price;
            if final_avg_price <= 0.0 && exec_qty > 0.0 {
                let cum_quote: f64 = v["cumQuote"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| v["cumQuote"].as_f64())
                    .unwrap_or(0.0);
                if cum_quote > 0.0 {
                    final_avg_price = cum_quote / exec_qty;
                    tracing::info!(
                        "[binance] avgPrice=0 fix: cumQuote={:.8}/execQty={:.4} → avgPrice={:.8} for {}",
                        cum_quote, exec_qty, final_avg_price, pair
                    );
                }
            }
            // If still 0 after cumQuote fallback but order was FILLED, query REST for real avgPrice.
            if final_avg_price <= 0.0
                && exec_qty > 0.0
                && (order_status == "FILLED" || order_status == "PARTIALLY_FILLED")
            {
                tracing::warn!(
                    "[binance] avgPrice still 0 for FILLED {} — querying REST order status",
                    pair
                );
                let oid_str = order_id.as_deref().unwrap_or("0");
                let rest_ts = now_ms();
                let rest_qs = format!("orderId={}&symbol={}&timestamp={}", oid_str, pair, rest_ts);
                let rest_sig = binance_sign(&keys.binance_secret, &rest_qs);
                let rest_url = format!(
                    "https://fapi.binance.com/fapi/v1/order?{}&signature={}",
                    rest_qs, rest_sig
                );
                match http
                    .get(&rest_url)
                    .header("X-MBX-APIKEY", &keys.binance_key)
                    .timeout(Duration::from_millis(api_timeout_ms.max(2000)))
                    .send()
                    .await
                {
                    Ok(resp) => {
                        let status_code = resp.status().as_u16();
                        let headers = resp.headers().clone();
                        record_binance_response(status_code, &headers);
                        if let Ok(body) = resp.json::<serde_json::Value>().await {
                            let rest_avg: f64 = body["avgPrice"]
                                .as_str()
                                .and_then(|s| s.parse().ok())
                                .or_else(|| body["avgPrice"].as_f64())
                                .unwrap_or(0.0);
                            if rest_avg > 0.0 {
                                final_avg_price = rest_avg;
                                tracing::info!(
                                    "[binance] REST fallback avgPrice={:.8} for {}",
                                    final_avg_price,
                                    pair
                                );
                            }
                            let rest_fee: f64 = body["cumCommission"]
                                .as_str()
                                .and_then(|s| s.parse::<f64>().ok())
                                .or_else(|| body["cumCommission"].as_f64())
                                .unwrap_or(cum_fee)
                                .abs();
                            if rest_fee > cum_fee {
                                return Ok((order_id, final_avg_price, rest_fee));
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("[binance] REST order query failed for {}: {}", pair, e);
                    }
                }
            }

            // For IOC/EXPIRED: if order was cancelled/expired without fill, return immediately (0ms delay)
            if (order_status == "EXPIRED"
                || order_status == "CANCELED"
                || order_status == "REJECTED")
                && exec_qty < 1e-12
            {
                tracing::warn!(
                    "[binance] place_order {} IOC order {} — NOT FILLED (status={})",
                    pair,
                    order_id.as_deref().unwrap_or("?"),
                    order_status
                );
                return Ok((None, 0.0, 0.0));
            }

            Ok((order_id, final_avg_price, cum_fee))
        } else {
            tracing::error!(
                "[binance] place_order {} — no orderId in WS response: {:?}",
                pair,
                v
            );
            Err("Binance failed to return orderId".to_string())
        }
    }

    async fn close_position(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        is_buy: bool,
        phys_amount: f64,
        dry_run: bool,
        leverage: f64,
        target_price: f64,
        _use_ioc: bool,
        slippage_percent: f64,
        multiplier: f64,
        tick_size: f64,
        _retries: u32,
        _delay_ms: u64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        let side = if is_buy { "BUY" } else { "SELL" };
        if dry_run {
            return Ok(CloseReceipt {
                final_price: target_price,
                total_fee: 0.0,
                closed_volume: phys_amount,
            });
        }

        let qty_str = format_binance_qty(phys_amount, multiplier);
        if qty_str.is_empty() || qty_str == "0" || phys_amount < 1e-12 {
            return Err("Amounts too small to close".to_string());
        }

        let price = if is_buy {
            target_price * (1.0 + slippage_percent / 100.0)
        } else {
            target_price * (1.0 - slippage_percent / 100.0)
        };
        let price_str = format_binance_price(price, tick_size);

        let mut params = serde_json::Map::new();
        params.insert("symbol".to_string(), serde_json::json!(pair));
        params.insert("side".to_string(), serde_json::json!(side));
        params.insert("quantity".to_string(), serde_json::json!(qty_str));
        params.insert("reduceOnly".to_string(), serde_json::json!("true"));

        let ts = now_ms();
        params.insert("timestamp".to_string(), serde_json::json!(ts));

        // Exit always uses IOC limit when target_price is available.
        // use_ioc controls entry order type, not exit — exit must never be a pure market order.
        let type_str = if target_price > 0.0 {
            "LIMIT"
        } else {
            "MARKET"
        };
        params.insert("type".to_string(), serde_json::json!(type_str));

        if target_price > 0.0 {
            params.insert("timeInForce".to_string(), serde_json::json!("IOC"));
            params.insert("price".to_string(), serde_json::json!(price_str));
        }
        // CRITICAL: request RESULT response type so WS returns fill data inline
        params.insert("newOrderRespType".to_string(), serde_json::json!("RESULT"));

        let _ = self.ensure_leverage(http, keys, pair, leverage).await;

        let v_resp = execute_binance_order(http, keys, &params, api_timeout_ms, false).await;
        if let Err(e) = &v_resp {
            tracing::error!("[binance] normal close rejected: {}", e);
        }

        // Extract fill data directly from WS response (no REST needed)
        if let Ok(ref v) = v_resp {
            let exec_qty: f64 = v["executedQty"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| v["executedQty"].as_f64())
                .unwrap_or(0.0);
            let mut avg_price: f64 = v["avgPrice"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| v["avgPrice"].as_f64())
                .unwrap_or(0.0);
            if avg_price <= 0.0 && exec_qty > 1e-12 {
                let cum_quote: f64 = v["cumQuote"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| v["cumQuote"].as_f64())
                    .unwrap_or(0.0);
                if cum_quote > 0.0 {
                    avg_price = cum_quote / exec_qty;
                } else if target_price > 0.0 {
                    avg_price = target_price;
                }
            }
            let cum_fee: f64 = v["cumCommission"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| v["cumCommission"].as_f64())
                .unwrap_or(0.0)
                .abs();
            let status = v["status"].as_str().unwrap_or("?");

            tracing::info!(
                "[binance] close {} WS result: status={} execQty={} avgPrice={} fee={}",
                pair,
                status,
                exec_qty,
                avg_price,
                cum_fee
            );

            if exec_qty > 1e-12 && avg_price > 0.0 {
                return Ok(CloseReceipt {
                    final_price: avg_price,
                    total_fee: cum_fee,
                    closed_volume: exec_qty,
                });
            }
        }

        // Fallback aggressive close if IOC didn't fill
        tracing::error!(
            "[binance] fallback market close triggered for {} (qty {})",
            pair,
            phys_amount
        );
        let mut _fb_price_str = String::new();
        if target_price > 0.0 {
            let p = if is_buy {
                target_price * 1.02
            } else {
                target_price * 0.98
            };
            let rp = if is_buy {
                (p / tick_size.max(1e-8)).ceil() * tick_size.max(1e-8)
            } else {
                (p / tick_size.max(1e-8)).floor() * tick_size.max(1e-8)
            };
            let mut s = format!("{:.8}", rp).trim_end_matches('0').to_string();
            if s.ends_with('.') {
                s.pop();
            }
            if !s.is_empty() && s != "0" {
                _fb_price_str = s;
            }
        }

        // CRITICAL: fallback MUST be a true MARKET order to guarantee closure.
        // Previous LIMIT IOC with 2% slippage still expired on thin pairs,
        // leaving unhedged positions open and triggering kill-switch.
        let ts_fb = now_ms();
        let mut fb_params = serde_json::Map::new();
        fb_params.insert("symbol".to_string(), serde_json::json!(pair));
        fb_params.insert("side".to_string(), serde_json::json!(side));
        fb_params.insert("type".to_string(), serde_json::json!("MARKET"));
        fb_params.insert("quantity".to_string(), serde_json::json!(qty_str));
        fb_params.insert("reduceOnly".to_string(), serde_json::json!("true"));
        fb_params.insert("timestamp".to_string(), serde_json::json!(ts_fb));
        fb_params.insert("newOrderRespType".to_string(), serde_json::json!("RESULT"));

        let v_fb = match execute_binance_order(http, keys, &fb_params, api_timeout_ms, false).await
        {
            Ok(v) => v,
            Err(e) => {
                // If reduce-only order is rejected, position is already closed (filled by initial order)
                if e.contains("ReduceOnly Order is rejected") {
                    tracing::info!(
                        "[binance] position already closed (ReduceOnly rejected) for {}",
                        pair
                    );
                    return Ok(CloseReceipt {
                        final_price: target_price,
                        total_fee: 0.0,
                        closed_volume: phys_amount,
                    });
                }
                return Err(format!("Fallback close rejected: {}", e));
            }
        };

        // Extract fill from fallback WS response
        let fb_exec_qty: f64 = v_fb["executedQty"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| v_fb["executedQty"].as_f64())
            .unwrap_or(0.0);
        let fb_avg_price: f64 = v_fb["avgPrice"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| v_fb["avgPrice"].as_f64())
            .unwrap_or(0.0);
        let fb_fee: f64 = v_fb["cumCommission"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| v_fb["cumCommission"].as_f64())
            .unwrap_or(0.0)
            .abs();

        if fb_exec_qty > 1e-12 && fb_avg_price > 0.0 {
            return Ok(CloseReceipt {
                final_price: fb_avg_price,
                total_fee: fb_fee,
                closed_volume: fb_exec_qty,
            });
        }

        tracing::warn!(
            "[binance] fallback close unverified for {}. Assuming filled to prevent CLOSING trap.",
            pair
        );
        Ok(CloseReceipt {
            final_price: target_price,
            total_fee: 0.0,
            closed_volume: phys_amount,
        })
    }

    async fn trim_position(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        is_buy: bool,
        phys_excess: f64,
        dry_run: bool,
        multiplier: f64,
        tick_size: f64,
        fallback_price: f64,
        api_timeout_ms: u64,
    ) -> Result<CloseReceipt, String> {
        self.close_position(
            http,
            keys,
            pair,
            is_buy,
            phys_excess,
            dry_run,
            1.0,
            fallback_price,
            true,
            0.0,
            multiplier,
            tick_size,
            15,
            100,
            api_timeout_ms,
        )
        .await
    }

    async fn fetch_order(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        order_id: &str,
        _multiplier: f64,
    ) -> Result<(f64, f64, f64), String> {
        if order_id.starts_with("dry_") {
            return Ok((0.0, 0.0, 0.0));
        }
        use crate::engine::exchanges::ws_trader::send_ws_request;

        // Step 1: Get order status via WS
        let ts = now_ms();
        let oid: i64 = order_id
            .parse()
            .map_err(|_| "Invalid orderId".to_string())?;
        let qs = format!(
            "apiKey={}&orderId={}&symbol={}&timestamp={}",
            keys.binance_key, oid, pair, ts
        );
        let sig = binance_sign(&keys.binance_secret, &qs);
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "id": req_id,
            "method": "v1/order.status",
            "params": {
                "apiKey": keys.binance_key,
                "symbol": pair,
                "orderId": oid,
                "timestamp": ts,
                "signature": sig
            }
        });
        let v_val = match send_ws_request(ws_req).await {
            Ok(resp) => {
                if let Some(err) = resp.get("error") {
                    return Err(format!("Binance WS order.status error: {}", err));
                }
                resp["result"].clone()
            }
            Err(e) => {
                tracing::warn!(
                    "[binance] WS fetch_order failed ({}). Trying REST fallback...",
                    e
                );
                let rest_qs = format!("orderId={}&symbol={}&timestamp={}", oid, pair, ts);
                let sig_rest = binance_sign(&keys.binance_secret, &rest_qs);
                let full_qs = format!("{}&signature={}", rest_qs, sig_rest);
                let url = format!("https://fapi.binance.com/fapi/v1/order?{}", full_qs);
                let resp = _http
                    .get(&url)
                    .header("X-MBX-APIKEY", &keys.binance_key)
                    .timeout(std::time::Duration::from_millis(2000))
                    .send()
                    .await
                    .map_err(|e| format!("Binance REST fetch_order HTTP error: {}", e))?;
                resp.json::<Value>()
                    .await
                    .map_err(|e| format!("Binance REST fetch_order JSON error: {}", e))?
            }
        };
        let v = &v_val;

        let exec_qty: f64 = v["executedQty"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let status = v["status"].as_str().unwrap_or("");

        if exec_qty < 1e-12 {
            if status == "EXPIRED" || status == "CANCELED" || status == "REJECTED" {
                tracing::warn!(
                    "[binance] fetch_order {} status={} — definitively unfilled",
                    order_id,
                    status
                );
                return Ok((0.0, 0.0, 0.0));
            }
            return Err("Not filled yet".to_string());
        }

        let price = v["avgPrice"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        if price <= 0.0 {
            return Ok((0.0, 0.0, 0.0));
        }

        // Step 2: Fetch commission via WS myTrades
        let ts2 = now_ms();
        let qs2 = format!(
            "apiKey={}&orderId={}&symbol={}&timestamp={}",
            keys.binance_key, oid, pair, ts2
        );
        let sig2 = binance_sign(&keys.binance_secret, &qs2);
        let req_id2 = uuid::Uuid::new_v4().to_string();
        let ws_req2 = serde_json::json!({
            "id": req_id2,
            "method": "v1/myTrades",
            "params": {
                "apiKey": keys.binance_key,
                "symbol": pair,
                "orderId": oid,
                "timestamp": ts2,
                "signature": sig2
            }
        });
        let fee = match send_ws_request(ws_req2).await {
            Ok(resp2) => resp2["result"]
                .as_array()
                .map(|arr| {
                    arr.iter().fold(0.0_f64, |acc, t| {
                        let comm = t["commission"]
                            .as_str()
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let asset = t["commissionAsset"].as_str().unwrap_or("USDT");
                        acc + match asset {
                            "USDT" => comm,
                            _ => {
                                if price > 0.0 {
                                    comm * price
                                } else {
                                    comm
                                }
                            }
                        }
                    })
                })
                .unwrap_or(0.0),
            Err(_) => 0.0,
        };
        Ok((price, fee, exec_qty))
    }

    async fn fetch_balances(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<(f64, f64), String> {
        check_ip_ban()?;
        use crate::engine::exchanges::ws_trader::send_ws_request;
        let ts = now_ms();
        let qs = format!("apiKey={}&timestamp={}", keys.binance_key, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "id": req_id,
            "method": "v2/account.balance",
            "params": {
                "apiKey": keys.binance_key,
                "timestamp": ts,
                "signature": sig
            }
        });
        let result_arr = match send_ws_request(ws_req).await {
            Ok(resp) => {
                record_ws_success();
                if let Some(err) = resp.get("error") {
                    detect_ban_from_response(err);
                    return Err(format!("Binance WS balance error: {}", err));
                }
                resp["result"].as_array().cloned().unwrap_or_default()
            }
            Err(e) => {
                record_ws_failure();
                // Check ban + REST fallback cooldown before making REST request
                check_ip_ban()?;
                check_rest_fallback_allowed(&REST_FALLBACK_BALANCE_MS, "balance")?;
                tracing::warn!(
                    "[binance] WS balance fetch failed ({}). Trying REST fallback...",
                    e
                );
                // CRITICAL: use fresh timestamp — the original `ts` may be 5+ seconds stale
                // after WS timeout, causing Binance -1021 recvWindow errors
                let fresh_ts = now_ms();
                let rest_qs = format!("timestamp={}", fresh_ts);
                let sig_rest = binance_sign(&keys.binance_secret, &rest_qs);
                let full_qs = format!("{}&signature={}", rest_qs, sig_rest);
                let url = format!("https://fapi.binance.com/fapi/v2/balance?{}", full_qs);
                let resp = _http
                    .get(&url)
                    .header("X-MBX-APIKEY", &keys.binance_key)
                    .timeout(std::time::Duration::from_millis(5000))
                    .send()
                    .await
                    .map_err(|e| format!("Binance REST balance HTTP error: {}", e))?;

                // Track weight from REST response headers
                let status = resp.status();
                let headers = resp.headers().clone();
                let body: Value = resp
                    .json()
                    .await
                    .map_err(|e| format!("Binance REST balance JSON error: {}", e))?;
                record_binance_response(status.as_u16(), &headers);
                detect_ban_from_response(&body);
                if let Some(_code) = body.get("code") {
                    return Err(format!("Binance REST balance API error: {}", body));
                }
                body.as_array().cloned().unwrap_or_default()
            }
        };

        let mut total_balance = 0.0f64;
        let mut available_balance = 0.0f64;

        for item in result_arr {
            let asset = item["asset"].as_str().unwrap_or("");
            if asset == "USDT" {
                total_balance = item["balance"]
                    .as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| item["balance"].as_f64())
                    .unwrap_or(0.0);
                available_balance = item["availableBalance"]
                    .as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| item["availableBalance"].as_f64())
                    .unwrap_or(0.0);
                break;
            }
        }

        Ok((total_balance, available_balance))
    }

    async fn fetch_taker_fee(&self, http: &Client, keys: &ExchangeKeys, pair: &str) -> Result<f64, String> {
        // USD-M Futures currently documents commission rate as a signed REST
        // endpoint. The former `v1/account.commissionRate` WS method is not part
        // of the API and returns an application error, which used to block every
        // otherwise executable entry before the REST fallback was considered.
        check_before_binance_rest()?;
        let timestamp = now_ms();
        let query = format!("symbol={}&timestamp={}", pair, timestamp);
        let signature = binance_sign(&keys.binance_secret, &query);
        let url = format!(
            "https://fapi.binance.com/fapi/v1/commissionRate?{}&signature={}",
            query, signature
        );
        let response = http
            .get(&url)
            .header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|error| format!("Binance fee HTTP error: {}", error))?;
        let status = response.status();
        let headers = response.headers().clone();
        let body: Value = response
            .json()
            .await
            .map_err(|error| format!("Binance fee JSON error: {}", error))?;
        record_binance_response(status.as_u16(), &headers);
        detect_ban_from_response(&body);
        if !status.is_success() {
            return Err(format!(
                "Binance fee rejected code={} msg={}",
                body["code"], body["msg"]
            ));
        }
        parse_binance_taker_fee(&body)
    }

    async fn ensure_leverage(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        leverage: f64,
    ) -> Result<(), String> {
        check_ip_ban()?;
        let lev = leverage as i64;
        if !leverage.is_finite() || lev < 1 || leverage != lev as f64 {
            return Err("Invalid leverage".into());
        }
        let cache_key = format!("{pair}:{lev}");
        for (path, params) in [
            (
                "/fapi/v1/marginType",
                format!("marginType=CROSSED&symbol={pair}"),
            ),
            ("/fapi/v1/leverage", format!("leverage={lev}&symbol={pair}")),
        ] {
            if (path.ends_with("marginType") && margin_type_cache().contains(pair))
                || (path.ends_with("leverage") && leverage_cache().contains(&cache_key))
            {
                continue;
            }
            let qs = format!("{params}&timestamp={}", now_ms());
            let signature = binance_sign(&keys.binance_secret, &qs);
            let response = http
                .post(format!(
                    "https://fapi.binance.com{path}?{qs}&signature={signature}"
                ))
                .header("X-MBX-APIKEY", &keys.binance_key)
                .timeout(Duration::from_secs(3))
                .send()
                .await
                .map_err(|_| "Binance leverage transport failure")?;
            let status = response.status();
            record_binance_response(status.as_u16(), response.headers());
            let v: Value = response
                .json()
                .await
                .map_err(|_| "Invalid Binance leverage response")?;
            if path.ends_with("marginType") {
                if !status.is_success() && v["code"].as_i64() != Some(-4046) {
                    return Err("Binance cross margin not confirmed".into());
                }
                margin_type_cache().insert(pair.to_string());
            } else {
                if !status.is_success()
                    || v["symbol"].as_str() != Some(pair)
                    || v["leverage"].as_i64() != Some(lev)
                {
                    return Err("Binance leverage not confirmed".into());
                }
                leverage_cache().insert(cache_key.clone());
            }
        }
        Ok(())
    }

    async fn fetch_positions(
        &self,
        _http: &Client,
        keys: &ExchangeKeys,
    ) -> Result<Vec<PositionInfo>, String> {
        check_ip_ban()?;
        use crate::engine::exchanges::ws_trader::send_ws_request;
        let ts = now_ms();
        let qs = format!("apiKey={}&timestamp={}", keys.binance_key, ts);
        let sig = binance_sign(&keys.binance_secret, &qs);
        let req_id = uuid::Uuid::new_v4().to_string();
        let ws_req = serde_json::json!({
            "id": req_id,
            "method": "v2/account.position",
            "params": {
                "apiKey": keys.binance_key,
                "timestamp": ts,
                "signature": sig
            }
        });
        let result_arr = match send_ws_request(ws_req).await {
            Ok(resp) => {
                record_ws_success();
                if let Some(err) = resp.get("error") {
                    detect_ban_from_response(err);
                    return Err(format!("Binance WS position error: {}", err));
                }
                resp["result"]
                    .as_array()
                    .cloned()
                    .ok_or("Malformed Binance position list")?
            }
            Err(e) => {
                record_ws_failure();
                // Check ban + REST fallback cooldown before making REST request
                check_ip_ban()?;
                check_rest_fallback_allowed(&REST_FALLBACK_POSITIONS_MS, "positions")?;
                tracing::warn!(
                    "[binance] WS positions fetch failed ({}). Trying REST fallback...",
                    e
                );
                // CRITICAL: use fresh timestamp to avoid -1021 recvWindow errors
                let fresh_ts = now_ms();
                let rest_qs = format!("timestamp={}", fresh_ts);
                let sig_rest = binance_sign(&keys.binance_secret, &rest_qs);
                let full_qs = format!("{}&signature={}", rest_qs, sig_rest);
                let url = format!("https://fapi.binance.com/fapi/v2/positionRisk?{}", full_qs);
                let resp = _http
                    .get(&url)
                    .header("X-MBX-APIKEY", &keys.binance_key)
                    .timeout(std::time::Duration::from_millis(5000))
                    .send()
                    .await
                    .map_err(|e| format!("Binance REST positions HTTP error: {}", e))?;

                // Track weight from REST response headers
                let status = resp.status();
                let headers = resp.headers().clone();
                let body: Value = resp
                    .json()
                    .await
                    .map_err(|e| format!("Binance REST positions JSON error: {}", e))?;
                record_binance_response(status.as_u16(), &headers);
                detect_ban_from_response(&body);
                if let Some(_code) = body.get("code") {
                    return Err(format!("Binance REST positions API error: {}", body));
                }
                body.as_array()
                    .cloned()
                    .ok_or("Malformed Binance position list")?
            }
        };

        let mut positions = Vec::new();
        for p in result_arr {
            let amt = crate::engine::orders::number(&p, "positionAmt")?;
            let symbol = p["symbol"]
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or("Missing Binance position symbol")?;
            if p["positionSide"].as_str() != Some("BOTH") {
                return Err("Managed execution requires one-way position mode".into());
            }
            if amt.abs() < 1e-12 {
                continue;
            }
            positions.push(PositionInfo {
                symbol: symbol.to_string(),
                size: amt,
                entry_price: p["entryPrice"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| p["entryPrice"].as_f64())
                    .unwrap_or(0.0),
                unrealized_pnl: p["unRealizedProfit"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| p["unRealizedProfit"].as_f64())
                    .unwrap_or(0.0),
                realized_pnl: 0.0,
                accumulated_fee: 0.0,
                accumulated_funding: 0.0,
            });
        }
        Ok(positions)
    }

    async fn audit_trade(
        &self,
        http: &Client,
        keys: &ExchangeKeys,
        pair: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<TradeAuditResult, String> {
        let ts = now_ms();
        let sym = pair.replace('_', "").replace('-', "").to_uppercase();
        let rest_qs = format!(
            "symbol={}&startTime={}&endTime={}&timestamp={}",
            sym, start_ts, end_ts, ts
        );
        let sig = binance_sign(&keys.binance_secret, &rest_qs);
        let url = format!(
            "https://fapi.binance.com/fapi/v1/userTrades?{}&signature={}",
            rest_qs, sig
        );

        let resp = http
            .get(&url)
            .header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(std::time::Duration::from_millis(5000))
            .send()
            .await
            .map_err(|e| format!("Binance audit userTrades HTTP error: {}", e))?;

        let body: Value = resp
            .json()
            .await
            .map_err(|e| format!("Binance audit userTrades JSON error: {}", e))?;
        let trades = match body.as_array() {
            Some(arr) => arr,
            None => return Ok(TradeAuditResult::default()),
        };

        if trades.is_empty() {
            return Ok(TradeAuditResult::default());
        }

        let mut total_fee = 0.0_f64;
        let mut total_realized_pnl = 0.0_f64;
        let mut sum_qty = 0.0_f64;
        let mut first_price = None;
        let mut last_price = None;

        for t in trades {
            let comm: f64 = t["commission"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| t["commission"].as_f64())
                .unwrap_or(0.0);
            let pnl: f64 = t["realizedPnl"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| t["realizedPnl"].as_f64())
                .unwrap_or(0.0);
            let price: f64 = t["price"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| t["price"].as_f64())
                .unwrap_or(0.0);
            let qty: f64 = t["qty"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| t["qty"].as_f64())
                .unwrap_or(0.0);

            total_fee += comm.abs();
            total_realized_pnl += pnl;
            sum_qty += qty;

            if first_price.is_none() && price > 0.0 {
                first_price = Some(price);
            }
            if price > 0.0 {
                last_price = Some(price);
            }
        }

        // Also fetch funding income for the symbol in this time range
        let mut funding_cost = 0.0_f64;
        let income_qs = format!(
            "symbol={}&incomeType=FUNDING_FEE&startTime={}&endTime={}&timestamp={}",
            sym, start_ts, end_ts, ts
        );
        let income_sig = binance_sign(&keys.binance_secret, &income_qs);
        let income_url = format!(
            "https://fapi.binance.com/fapi/v1/income?{}&signature={}",
            income_qs, income_sig
        );
        if let Ok(inc_resp) = http
            .get(&income_url)
            .header("X-MBX-APIKEY", &keys.binance_key)
            .timeout(std::time::Duration::from_millis(4000))
            .send()
            .await
        {
            if let Ok(inc_arr) = inc_resp.json::<Value>().await {
                if let Some(arr) = inc_arr.as_array() {
                    for inc in arr {
                        let amt: f64 = inc["income"]
                            .as_str()
                            .and_then(|s| s.parse().ok())
                            .or_else(|| inc["income"].as_f64())
                            .unwrap_or(0.0);
                        funding_cost -= amt; // negative income is cost paid
                    }
                }
            }
        }

        Ok(TradeAuditResult {
            total_fee,
            entry_price: first_price,
            exit_price: last_price,
            realized_pnl: Some(total_realized_pnl),
            funding_cost: Some(funding_cost),
            filled_qty: sum_qty,
        })
    }
}

fn parse_managed_binance(
    v: &Value,
    r: &crate::engine::orders::OrderRequest,
) -> Result<crate::engine::orders::OrderObservation, String> {
    use crate::engine::orders::{identity, number, OrderObservation, OrderState};
    if v["symbol"].as_str() != Some(r.pair.as_str())
        || v["clientOrderId"].as_str() != Some(r.client_id.as_str())
    {
        return Err("Order identity mismatch".into());
    }
    let qty = number(v, "executedQty")?;
    let state = match v["status"].as_str() {
        Some("FILLED") => OrderState::Filled,
        Some("CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH") => OrderState::Cancelled,
        Some("REJECTED") => OrderState::Rejected,
        Some("PARTIALLY_FILLED") => OrderState::PartiallyFilled,
        Some("NEW") => OrderState::Submitted,
        _ => return Err("Unknown Binance order status".into()),
    };
    let price = if qty > 0.0 {
        let p = number(v, "avgPrice").unwrap_or(0.0);
        Some(if p > 0.0 {
            p
        } else {
            number(v, "cumQuote")? / qty
        })
    } else {
        None
    };
    Ok(OrderObservation {
        order_id: identity(v, "orderId"),
        state,
        filled_quantity: qty,
        average_price: price,
        fee: if qty == 0.0 && state.terminal() {
            Some(0.0)
        } else {
            None
        },
    })
}

fn filled_order_missing_average(v: &Value, parse_error: &str) -> Option<String> {
    if parse_error != "Missing numeric field cumQuote"
        || v["status"].as_str() != Some("FILLED")
        || crate::engine::orders::number(v, "executedQty").ok()? <= 0.0
    {
        return None;
    }
    crate::engine::orders::identity(v, "orderId")
}
async fn managed_binance_get(
    http: &Client,
    keys: &ExchangeKeys,
    path: &str,
    qs: &str,
) -> Result<Value, String> {
    check_ip_ban()?;
    let sig = binance_sign(&keys.binance_secret, qs);
    let res = http
        .get(format!(
            "https://fapi.binance.com{}?{}&signature={}",
            path, qs, sig
        ))
        .header("X-MBX-APIKEY", &keys.binance_key)
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .map_err(|_| "Binance query transport failure")?;
    let status = res.status();
    let headers = res.headers().clone();
    let v: Value = res.json().await.map_err(|_| "Binance query invalid JSON")?;
    let _ = check_rate_limits(status, &headers, Some(&v)).await;
    if !status.is_success() {
        return Err(format!("Binance query rejected code={}", v["code"]));
    }
    Ok(v)
}

fn parse_binance_taker_fee(value: &Value) -> Result<f64, String> {
    let fee = value["takerCommissionRate"]
        .as_str()
        .and_then(|text| text.parse::<f64>().ok())
        .or_else(|| value["takerCommissionRate"].as_f64())
        .ok_or_else(|| "Binance fee response has no takerCommissionRate".to_string())?;
    if !fee.is_finite() || !(0.0..=0.01).contains(&fee) {
        return Err("Binance returned an invalid taker fee".into());
    }
    Ok(fee)
}

#[cfg(test)]
mod fee_tests {
    use super::*;

    #[test]
    fn parses_documented_rest_commission_response() {
        assert_eq!(
            parse_binance_taker_fee(&serde_json::json!({
                "symbol": "ONEUSDT",
                "makerCommissionRate": "0.000200",
                "takerCommissionRate": "0.000500"
            }))
            .unwrap(),
            0.0005
        );
    }

    #[test]
    fn rejects_missing_or_out_of_range_commission() {
        assert!(parse_binance_taker_fee(&serde_json::json!({})).is_err());
        assert!(parse_binance_taker_fee(&serde_json::json!({
            "takerCommissionRate": "0.02"
        }))
        .is_err());
    }

    #[test]
    fn filled_ioc_without_average_keeps_identity_for_immediate_query() {
        let response = serde_json::json!({
            "status": "FILLED",
            "executedQty": "2620",
            "avgPrice": "0",
            "orderId": 8413195446_i64
        });
        assert_eq!(
            filled_order_missing_average(&response, "Missing numeric field cumQuote"),
            Some("8413195446".to_string())
        );
        assert_eq!(
            filled_order_missing_average(&response, "Order identity mismatch"),
            None
        );
    }

    #[test]
    fn parses_fresh_documented_public_depth_response() {
        let snapshot = parse_public_depth_snapshot(
            &serde_json::json!({
                "lastUpdateId": 42,
                "E": 9_500,
                "T": 9_499,
                "bids": [["100.0", "2.0"], ["99.0", "3.0"]],
                "asks": [["101.0", "4.0"], ["102.0", "5.0"]]
            }),
            10_000,
        )
        .unwrap();
        assert_eq!(snapshot.updated_ms, 9_500);
        assert_eq!(snapshot.bids[0], (100.0, 2.0));
        assert_eq!(snapshot.asks[0], (101.0, 4.0));
    }

    #[test]
    fn public_depth_parser_rejects_stale_crossed_or_unsorted_books() {
        let valid = serde_json::json!({
            "E": 9_000,
            "bids": [["100.0", "2.0"], ["99.0", "3.0"]],
            "asks": [["101.0", "4.0"], ["102.0", "5.0"]]
        });
        assert!(parse_public_depth_snapshot(&valid, 10_000).is_err());
        let crossed = serde_json::json!({
            "E": 10_000,
            "bids": [["101.0", "2.0"]],
            "asks": [["101.0", "4.0"]]
        });
        assert!(parse_public_depth_snapshot(&crossed, 10_000).is_err());
        let unsorted = serde_json::json!({
            "E": 10_000,
            "bids": [["99.0", "2.0"], ["100.0", "3.0"]],
            "asks": [["101.0", "4.0"]]
        });
        assert!(parse_public_depth_snapshot(&unsorted, 10_000).is_err());
        let missing_exchange_time = serde_json::json!({
            "bids": [["100.0", "2.0"]],
            "asks": [["101.0", "4.0"]]
        });
        assert!(parse_public_depth_snapshot(&missing_exchange_time, 10_000).is_err());
    }

    #[test]
    fn parses_documented_public_ws_depth_response() {
        let snapshot = parse_public_depth_ws_response(
            &serde_json::json!({
                "id": "request-1",
                "status": 200,
                "result": {"lastUpdateId": 42, "E": 9_750, "T": 9_749},
                "bids": [["100.0", "2.0"], ["99.0", "3.0"]],
                "asks": [["101.0", "4.0"], ["102.0", "5.0"]]
            }),
            10_000,
        )
        .unwrap();
        assert_eq!(snapshot.updated_ms, 9_750);
        assert_eq!(snapshot.bids[0], (100.0, 2.0));
        assert_eq!(snapshot.asks[0], (101.0, 4.0));
    }

    #[test]
    fn public_ws_depth_parser_accepts_nested_levels_but_rejects_errors() {
        let nested = serde_json::json!({
            "id": "request-1",
            "status": 200,
            "result": {
                "E": 10_000,
                "bids": [["100.0", "2.0"]],
                "asks": [["101.0", "4.0"]]
            }
        });
        assert!(parse_public_depth_ws_response(&nested, 10_000).is_ok());
        assert!(parse_public_depth_ws_response(
            &serde_json::json!({"status": 400, "error": {"code": -1}}),
            10_000,
        )
        .is_err());
    }

    #[tokio::test]
    #[ignore = "live Binance public depth check"]
    async fn live_public_depth_is_fresh_and_valid() {
        let snapshot = fetch_public_depth_snapshot(&Client::new(), "BTCUSDT")
            .await
            .unwrap();
        assert!(!snapshot.bids.is_empty());
        assert!(!snapshot.asks.is_empty());
        assert!((0..=750).contains(&(now_ms() - snapshot.updated_ms)));
    }

    #[tokio::test]
    #[ignore = "live Binance public WS depth check"]
    async fn live_public_ws_depth_is_fresh_and_valid() {
        let snapshot = fetch_public_depth_snapshot_ws("BTCUSDT").await.unwrap();
        assert!(!snapshot.bids.is_empty());
        assert!(!snapshot.asks.is_empty());
        assert!((0..=750).contains(&(now_ms() - snapshot.updated_ms)));
    }
}
