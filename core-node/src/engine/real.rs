//! Real trading engine for core-node.
//!
//! run_cycle() is called directly by the engine loop (no HTTP).
//! Order placement, Leg Fallback, and Partial Fill Guard are all here.
//! Exchange signing is done inline following the same pattern as real-trader.

use std::{sync::Arc, time::Duration};
use anyhow::Result;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use reqwest::Client;
use rusqlite::params;
use serde_json::{json, Value};
use sha2::Sha256;
use tokio_rusqlite::Connection;
use tracing::{error, info};
use uuid::Uuid;

use crate::settings::TradingSettings;
use shared::SpreadRow;

type HmacSha256 = Hmac<Sha256>;

// ─── Exchange keys ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ExchangeKeys {
    pub gate_key:    String, pub gate_secret:    String,
    pub htx_key:     String, pub htx_secret:     String,
    pub kucoin_key:  String, pub kucoin_secret:  String, pub kucoin_passphrase: String,
}

impl ExchangeKeys {
    pub fn from_env() -> Self {
        Self {
            gate_key:    std::env::var("GATE_KEY").unwrap_or_default(),
            gate_secret: std::env::var("GATE_SECRET").unwrap_or_default(),
            htx_key:     std::env::var("HTX_KEY").unwrap_or_default(),
            htx_secret:  std::env::var("HTX_SECRET").unwrap_or_default(),
            kucoin_key:         std::env::var("KUCOIN_KEY").unwrap_or_default(),
            kucoin_secret:      std::env::var("KUCOIN_SECRET").unwrap_or_default(),
            kucoin_passphrase:  std::env::var("KUCOIN_PASSPHRASE").unwrap_or_default(),
        }
    }
    pub fn supports(&self, ex: &str) -> bool {
        match ex {
            "gate"   => !self.gate_key.is_empty(),
            "htx"    => !self.htx_key.is_empty(),
            "kucoin" => !self.kucoin_key.is_empty(),
            _ => false,
        }
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn now_secs() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

fn hmac_sha256_hex(secret: &[u8], data: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC any key");
    mac.update(data.as_bytes()); hex::encode(mac.finalize().into_bytes())
}
fn hmac_sha256_b64(secret: &[u8], data: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC any key");
    mac.update(data.as_bytes()); base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}
fn htx_ts() -> String { chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string() }
fn url_enc(s: &str) -> String {
    s.replace('+', "%2B").replace('/', "%2F").replace('=', "%3D").replace(':', "%3A")
}
fn htx_sign(key: &str, secret: &str, method: &str, host: &str, path: &str) -> String {
    let ts = htx_ts();
    let params = format!("AccessKeyId={}&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp={}", key, url_enc(&ts));
    let sig = hmac_sha256_b64(secret.as_bytes(), &format!("{}\n{}\n{}\n{}", method, host, path, params));
    format!("{}?{}&Signature={}", path, params, url_enc(&sig))
}
fn kucoin_sign(secret: &str, passphrase: &str, ts: &str, method: &str, path: &str, body: &str) -> (String, String) {
    (hmac_sha256_b64(secret.as_bytes(), &format!("{}{}{}{}", ts, method, path, body)),
     hmac_sha256_b64(secret.as_bytes(), passphrase))
}
fn gate_sign(key: &str, secret: &str, method: &str, path: &str, body: &str) -> reqwest::header::HeaderMap {
    use sha2::Digest;
    let ts = now_secs().to_string();
    let body_hash = hex::encode(sha2::Sha512::digest(body.as_bytes()));
    let sign_str = format!("{}\n{}\n\n{}\n{}", method, path, body_hash, ts);
    let sig = { let mut mac = Hmac::<sha2::Sha512>::new_from_slice(secret.as_bytes()).unwrap(); mac.update(sign_str.as_bytes()); hex::encode(mac.finalize().into_bytes()) };
    let mut h = reqwest::header::HeaderMap::new();
    h.insert("KEY", key.parse().unwrap()); h.insert("SIGN", sig.parse().unwrap());
    h.insert("Timestamp", ts.parse().unwrap()); h.insert("Content-Type", "application/json".parse().unwrap());
    h
}

// ─── Order placement ──────────────────────────────────────────────────────────

async fn place_order(http: &Client, keys: &ExchangeKeys, exchange: &str, pair: &str, is_short: bool, volume_usd: f64, price: f64, dry_run: bool) -> Result<String> {
    let side = if is_short { "sell" } else { "buy" };
    let contracts = ((volume_usd / price.max(1e-12)).ceil() as i64).max(1);
    match exchange {
        "gate" => {
            let contract = pair.replace("USDT", "_USDT");
            let size = if is_short { -contracts } else { contracts };
            let path = "/api/v4/futures/usdt/orders";
            let body = json!({"contract":contract,"size":size,"price":"0","tif":"ioc","reduce_only":false,"close":false,"text":"t-corenode"});
            let body_str = body.to_string();
            if dry_run { info!("[DRY] gate order {} size={}", contract, size); return Ok(format!("dry_{}", Uuid::new_v4())); }
            let resp = http.post(format!("https://api.gateio.ws{}", path)).headers(gate_sign(&keys.gate_key, &keys.gate_secret, "POST", path, &body_str)).body(body_str).timeout(Duration::from_secs(10)).send().await?;
            let v: Value = resp.json().await?;
            Ok(v["id"].as_i64().map(|i| i.to_string()).unwrap_or_else(|| v["id"].as_str().unwrap_or("unknown").to_string()))
        }
        "htx" => {
            let cc = format!("{}-{}", &pair[..pair.len()-4], &pair[pair.len()-4..]);
            let host = "api.hbdm.com"; let path = "/linear-swap-api/v1/swap_order";
            let body = json!({"contract_code":cc,"client_order_id":now_ms(),"volume":contracts,"direction":side,"offset":"open","lever_rate":1,"order_price_type":"optimal_20"});
            if dry_run { info!("[DRY] htx order {} {}", cc, side); return Ok(format!("dry_{}", Uuid::new_v4())); }
            let resp = http.post(format!("https://{}{}", host, htx_sign(&keys.htx_key, &keys.htx_secret, "POST", host, path))).json(&body).timeout(Duration::from_secs(10)).send().await?;
            let v: Value = resp.json().await?;
            Ok(v["data"]["order_id"].as_i64().map(|i| i.to_string()).unwrap_or_else(|| "unknown".to_string()))
        }
        "kucoin" => {
            let sym = format!("{}M", pair);
            let ts = now_ms().to_string(); let path = "/api/v1/orders";
            let body = json!({"clientOid":Uuid::new_v4().to_string(),"symbol":sym,"side":side,"type":"market","size":contracts,"leverage":"1"});
            let body_str = body.to_string();
            let (sig, pass_sig) = kucoin_sign(&keys.kucoin_secret, &keys.kucoin_passphrase, &ts, "POST", path, &body_str);
            if dry_run { info!("[DRY] kucoin order {} {}", sym, side); return Ok(format!("dry_{}", Uuid::new_v4())); }
            let resp = http.post(format!("https://api-futures.kucoin.com{}", path))
                .header("KC-API-KEY", &keys.kucoin_key).header("KC-API-SIGN", sig)
                .header("KC-API-TIMESTAMP", &ts).header("KC-API-PASSPHRASE", pass_sig)
                .header("KC-API-KEY-VERSION", "2").header("Content-Type", "application/json")
                .body(body_str).timeout(Duration::from_secs(10)).send().await?;
            let v: Value = resp.json().await?;
            Ok(v["data"]["orderId"].as_str().unwrap_or("unknown").to_string())
        }
        _ => Err(anyhow::anyhow!("Unknown exchange: {}", exchange)),
    }
}

async fn close_order(http: &Client, keys: &ExchangeKeys, exchange: &str, pair: &str, was_short: bool, vol: f64, price: f64, dry_run: bool) -> Result<String> {
    place_order(http, keys, exchange, pair, !was_short, vol, price, dry_run).await
}

// ─── Engine cycle ─────────────────────────────────────────────────────────────

/// Called on every WS tick. Manages real trades in the DB.
pub async fn run_cycle(spreads: Vec<SpreadRow>, prices: crate::state::PriceMap, settings: TradingSettings, http: Client, db: Connection) {
    if !settings.is_active { return; }

    let dry_run = std::env::var("DRY_RUN").map(|v| v != "false").unwrap_or(true);
    let keys = Arc::new(ExchangeKeys::from_env());

    // Load active trades
    let trades = load_open_trades(&db).await;
    let num_active = trades.len() as i64;

    let commission = settings.commission_rate / 100.0;
    let funding    = settings.funding_rate_8h / 100.0;
    let stop_pct   = settings.trailing_stop_percent / 100.0;

    // ── Close qualifying trades ───────────────────────────────────────────────
    let mut live_updates = Vec::new();
    for trade in &trades {
        let is_a_higher = trade.direction.starts_with(&trade.exchange_a);
        
        let data_a = match prices.get(&trade.exchange_a).and_then(|m| m.get(&trade.pair).cloned()) {
            Some(d) => d,
            None => continue,
        };
        let data_b = match prices.get(&trade.exchange_b).and_then(|m| m.get(&trade.pair).cloned()) {
            Some(d) => d,
            None => continue,
        };

        if data_a.bid <= 0.0 || data_a.ask <= 0.0 || data_b.bid <= 0.0 || data_b.ask <= 0.0 { 
            continue; 
        }

        let c_price_a = data_a.price;
        let c_price_b = data_b.price;
        
        let exit_price_a = if is_a_higher { data_a.ask } else { data_a.bid };
        let exit_price_b = if is_a_higher { data_b.bid } else { data_b.ask };

        let contracts = trade.volume / trade.entry_price_a.max(1e-12);
        let pnl_a = if is_a_higher { (trade.entry_price_a - exit_price_a) * contracts } else { (exit_price_a - trade.entry_price_a) * contracts };
        let pnl_b = if is_a_higher { (exit_price_b - trade.entry_price_b) * contracts } else { (trade.entry_price_b - exit_price_b) * contracts };
        let gross_pnl = pnl_a + pnl_b;
        let comm_cost = 4.0 * commission * trade.volume;
        let fund_cost = funding * trade.volume;
        let net_pnl   = gross_pnl - (comm_cost + fund_cost);
        let net_pct   = net_pnl / trade.volume * 100.0;
        
        let current_spread = if is_a_higher {
            (data_a.price - data_b.price) / data_b.price.max(1e-12) * 100.0
        } else {
            (data_b.price - data_a.price) / data_a.price.max(1e-12) * 100.0
        };
        
        let exit_spread = if is_a_higher {
            (data_a.ask - data_b.bid) / data_b.bid.max(1e-12) * 100.0
        } else {
            (data_b.ask - data_a.bid) / data_a.bid.max(1e-12) * 100.0
        };

        let top_spread = trade.highest_spread.max(current_spread);
        
        let triggered_tp   = net_pct >= settings.take_profit_percent;
        let stop_limit     = top_spread - settings.trailing_stop_percent;
        let triggered_stop = top_spread > 0.0 && current_spread < stop_limit;
        let zero_spread    = exit_spread <= 0.0 && net_pnl >= 0.0; // hard exit for safety

        if triggered_tp || triggered_stop || zero_spread {
            let reason = if triggered_tp { "TP" } else if zero_spread { "ZERO_SPREAD" } else { "STOP" };
            info!("[real] Closing {} {} reason={} net={:.4} exit_sp={:.4}", trade.id, trade.pair, reason, net_pnl, exit_spread);
            let http = http.clone(); let keys = Arc::clone(&keys);
            let exa = trade.exchange_a.clone(); let exb = trade.exchange_b.clone();
            let pair = trade.pair.clone(); let vol = trade.volume;
            // Execute trade orders on the exchanges using the calculated prices
            tokio::join!(
                close_order(&http, &keys, &exa, &pair, is_a_higher, vol, exit_price_a, dry_run),
                close_order(&http, &keys, &exb, &pair, !is_a_higher, vol, exit_price_b, dry_run)
            );
            let id = trade.id; let np = net_pnl; let gp = gross_pnl; let cs = current_spread;
            let cpa = exit_price_a; let cpb = exit_price_b; let pa = pnl_a; let pb = pnl_b; let r = reason.to_string();
            let _ = db.call(move |conn| {
                conn.execute("UPDATE trades SET status='CLOSED',exit_price_a=?,exit_price_b=?,exit_spread=?,exit_time=?,pnl=?,gross_pnl=?,pnl_a=?,pnl_b=?,curr_gross_pnl=?,close_reason=?,commission_cost=?,funding_cost=?,slippage_cost=? WHERE id=?",
                    params![cpa, cpb, cs, now_ms(), np, gp, pa, pb, gp, r, comm_cost, fund_cost, 0.0, id])?;
                Ok(())
            }).await;
        } else {
            // Did not close, track live PnL for UI
            live_updates.push((trade.id, c_price_a, c_price_b, current_spread, net_pnl));

            if top_spread > trade.highest_spread {
                let id = trade.id;
                let _ = db.call(move |conn| {
                    conn.execute("UPDATE trades SET highest_spread_reached=? WHERE id=?", params![top_spread, id])?;
                    Ok(())
                }).await;
            }
        }
    }

    static LAST_LIVE_FLUSH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let current_ms = now_ms() as u64;
    let elapsed = current_ms.saturating_sub(LAST_LIVE_FLUSH.load(std::sync::atomic::Ordering::Relaxed));
    if elapsed > 1000 && !live_updates.is_empty() {
        LAST_LIVE_FLUSH.store(current_ms, std::sync::atomic::Ordering::Relaxed);
        let db_clone = db.clone();
        tokio::spawn(async move {
            let _ = db_clone.call(move |conn| {
                let tx = conn.transaction()?;
                {
                    let mut stmt = tx.prepare_cached("UPDATE trades SET last_price_a=?, last_price_b=?, last_spread=?, curr_gross_pnl=? WHERE id=?")?;
                    for (id, pa, pb, cs, pnl) in live_updates {
                        stmt.execute(params![pa, pb, cs, pnl, id])?;
                    }
                }
                tx.commit()?;
                Ok::<(), tokio_rusqlite::Error>(())
            }).await;
        });
    }

    // ── Open new real trades ──────────────────────────────────────────────────
    if num_active >= settings.max_active_trades { return; }
    let open_pairs: std::collections::HashSet<String> = trades.iter().map(|t| t.pair.clone()).collect();

    for row in spreads.iter().take(50) {
        if row.spread < settings.min_entry_spread || row.spread > settings.max_entry_spread { continue; }
        if open_pairs.contains(&row.pair) { continue; }
        if !keys.supports(&row.exchange_a) || !keys.supports(&row.exchange_b) { continue; }
        if settings.disabled_exchanges.contains(&row.exchange_a) || settings.disabled_exchanges.contains(&row.exchange_b) { continue; }

        let is_a_higher = row.direction.starts_with(&row.exchange_a);
        let entry_a = if is_a_higher { row.bid_a } else { row.ask_a };
        let entry_b = if is_a_higher { row.ask_b } else { row.bid_b };
        info!("[real] Opening {} {} {} spread={:.4}% [dry={}]", row.pair, row.exchange_a, row.exchange_b, row.spread, dry_run);

        let (oa, ob) = tokio::join!(
            place_order(&http, &keys, &row.exchange_a, &row.pair, is_a_higher, settings.trade_volume, entry_a, dry_run),
            place_order(&http, &keys, &row.exchange_b, &row.pair, !is_a_higher, settings.trade_volume, entry_b, dry_run)
        );
        let oid_a = match oa { Ok(id) => Some(id), Err(e) => { error!("[LEG_A] {}: {}", row.exchange_a, e); None } };
        let oid_b = match ob { Ok(id) => Some(id), Err(e) => { error!("[LEG_B] {}: {}", row.exchange_b, e); None } };

        if !dry_run {
            match (oid_a.is_some(), oid_b.is_some()) {
                (true, false) => {
                    error!("🚨 LEG ASYMMETRY: A open, B failed — emergency closing A");
                    let (h, k, e, p, v) = (http.clone(), Arc::clone(&keys), row.exchange_a.clone(), row.pair.clone(), settings.trade_volume);
                    tokio::spawn(async move { for attempt in 1u32..=5 {
                        match close_order(&h, &k, &e, &p, is_a_higher, v, entry_a, false).await {
                            Ok(id) => { error!("🛡️ FALLBACK A closed OK {}", id); break; }
                            Err(e) => { let w = 300 * 2u64.pow(attempt-1); error!("FALLBACK {}/5: {} — {}ms", attempt, e, w); tokio::time::sleep(Duration::from_millis(w)).await; }
                        }
                    }});
                    continue;
                }
                (false, true) => {
                    error!("🚨 LEG ASYMMETRY: A failed, B open — emergency closing B");
                    let (h, k, e, p, v) = (http.clone(), Arc::clone(&keys), row.exchange_b.clone(), row.pair.clone(), settings.trade_volume);
                    tokio::spawn(async move { for attempt in 1u32..=5 {
                        match close_order(&h, &k, &e, &p, !is_a_higher, v, entry_b, false).await {
                            Ok(id) => { error!("🛡️ FALLBACK B closed OK {}", id); break; }
                            Err(e) => { let w = 300 * 2u64.pow(attempt-1); error!("FALLBACK {}/5: {} — {}ms", attempt, e, w); tokio::time::sleep(Duration::from_millis(w)).await; }
                        }
                    }});
                    continue;
                }
                (false, false) => { error!("Both legs failed, skipping"); continue; }
                (true, true) => {}
            }
        } else if oid_a.is_none() || oid_b.is_none() { continue; }

        let pair = row.pair.clone(); let exa = row.exchange_a.clone(); let exb = row.exchange_b.clone();
        let dir = row.direction.clone(); let vol = settings.trade_volume; let sp = row.spread;
        let commission = settings.commission_rate / 100.0;
        let slippage = settings.slippage_percent / 100.0;
        let comm_cost = 4.0 * commission * vol;
        let slip_cost = 4.0 * slippage * vol;
        let profile_id = settings.id.clone();
        let _ = db.call(move |conn| {
            conn.execute(
                "INSERT INTO trades (pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,entry_spread,entry_time,highest_spread_reached,status,pnl_a,pnl_b,close_reason,profile_id,commission_cost,slippage_cost,funding_cost,order_id_a,order_id_b) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params![pair, exa, exb, dir, vol, entry_a, entry_b, sp, now_ms(), sp, "OPEN", 0.0, 0.0, None::<String>, profile_id, comm_cost, slip_cost, 0.0, oid_a, oid_b]
            )?;
            Ok(())
        }).await;
        break; // one trade per cycle
    }
}

#[derive(Debug, Clone)]
struct Trade { id: i64, pair: String, exchange_a: String, exchange_b: String, direction: String, volume: f64, entry_price_a: f64, entry_price_b: f64, highest_spread: f64 }

async fn load_open_trades(db: &Connection) -> Vec<Trade> {
    db.call(|conn| {
        let mut stmt = conn.prepare("SELECT id,pair,exchange_a,exchange_b,direction,volume,entry_price_a,entry_price_b,highest_spread_reached FROM trades WHERE status='OPEN'")?;
        let rows = stmt.query_map([], |row| Ok(Trade { id: row.get(0)?, pair: row.get(1)?, exchange_a: row.get(2)?, exchange_b: row.get(3)?, direction: row.get(4)?, volume: row.get(5)?, entry_price_a: row.get(6)?, entry_price_b: row.get(7)?, highest_spread: row.get(8)? }))?.filter_map(|r| r.ok()).collect();
        Ok(rows)
    }).await.unwrap_or_default()
}
