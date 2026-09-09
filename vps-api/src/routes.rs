use crate::db::{row_to_profile, row_to_trade};
use crate::models::SpreadPayload;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use rusqlite::params;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::AppState;

fn json_err(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({ "error": msg }))).into_response()
}

fn history_trade_view(t: crate::models::Trade) -> Value {
            let mut v = serde_json::to_value(&t).unwrap_or_default();
            let display_comm = t.commission_cost.unwrap_or(0.0);
            let display_slip = t.slippage_cost.unwrap_or(0.0);

            if let Some(obj) = v.as_object_mut() {
                obj.insert("commission_cost".into(), json!(display_comm));
                // Entry slippage (IOC target vs fill)
                obj.insert("slippage_cost".into(), json!(display_slip));
                // Exit slippage (separate for display)
                obj.insert(
                    "exit_slippage_cost".into(),
                    json!(t.exit_slippage_cost.unwrap_or(0.0)),
                );
                let fund_cost = t.funding_cost.unwrap_or(0.0);
                obj.insert("funding_cost".into(), json!(fund_cost));
                // Gross PnL: actual fill-based PnL (slippage already baked in)
                let actual_gross = t.gross_pnl.or_else(|| t.pnl_a.zip(t.pnl_b).map(|(a,b)|a+b));
                obj.insert("gross_pnl".into(), json!(actual_gross));
                // Preserve the ledger's optional net PnL; pending fees/funding are not zero.
                obj.insert("pnl".into(), json!(t.pnl));
            }
            v
}

fn check_auth(headers: &HeaderMap, secret: &str) -> bool {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|v| !secret.is_empty() && v == format!("Bearer {}", secret))
        .unwrap_or(false)
}

fn check_basic_auth(headers: &HeaderMap, state: &AppState) -> Option<String> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    if !auth.starts_with("Basic ") {
        return None;
    }
    use base64::{engine::general_purpose, Engine as _};
    let decoded = general_purpose::STANDARD.decode(&auth[6..]).ok()?;
    let dec_str = String::from_utf8(decoded).ok()?;
    let mut parts = dec_str.splitn(2, ':');
    let user = parts.next()?;
    let pass = parts.next()?;

    if user == "dima" && pass == state.pass_dima {
        Some("dima".into())
    } else if user == "sergei" && pass == state.pass_sergei {
        Some("sergei".into())
    } else {
        None
    }
}

// ─── POST /api/update (Rust Collector pushes here) ───
pub async fn handle_update(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<SpreadPayload>,
) -> Response {
    if !check_auth(&headers, &state.collector_token) {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Save snapshot
    let snapshot_val = json!({ "ts": now, "data": payload.spreads }).to_string();
    let _ = state
        .db
        .call(move |conn| {
            Ok(conn.execute(
                "INSERT INTO server_cache (key, value, expires_at) VALUES ('snapshot', ?, ?)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
                params![snapshot_val, now + 180_000],
            )?)
        })
        .await;

    // Save maps summary
    let mut map_summaries: HashMap<String, Vec<String>> = HashMap::new();
    for (ex, data) in &payload.maps {
        let keys = if let Some(obj) = data.as_object() {
            obj.keys().cloned().collect()
        } else if let Some(arr) = data.as_array() {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        } else {
            vec![]
        };
        map_summaries.insert(ex.clone(), keys);
    }
    let map_val = json!({ "ts": now, "data": map_summaries }).to_string();
    let _ = state
        .db
        .call(move |conn| {
            Ok(conn.execute(
                "INSERT INTO server_cache (key, value, expires_at) VALUES ('exchange_maps', ?, ?)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
                params![map_val, now + 180_000],
            )?)
        })
        .await;

    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

// ─── GET /api/trading/settings/all ───
pub async fn handle_get_all_settings(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };
    let profiles = match state
        .db
        .call(move |conn| {
            let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE mode = 'real' AND (owner=? OR id='real-default')")?;
            let items: Vec<_> = stmt
                .query_map([username], row_to_profile)?
                .filter_map(|r| r.ok())
                .collect();
            Ok(items)
        })
        .await
    {
        Ok(p) => p,
        Err(_) => vec![],
    };
    (StatusCode::OK, Json(profiles)).into_response()
}

// ─── GET /api/logs/core-node ───
#[derive(serde::Deserialize)]
pub struct LogsQuery {
    mode: Option<String>,
}
pub async fn handle_get_logs(
    State(_state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<LogsQuery>,
) -> Response {
    if check_basic_auth(&headers, &_state).is_none() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"Unauthorized"})),
        )
            .into_response();
    }
    static LOGS_CACHE: OnceLock<Mutex<Option<(Instant, String)>>> = OnceLock::new();
    let cache = LOGS_CACHE.get_or_init(|| Mutex::new(None));

    {
        let guard = cache.lock().unwrap();
        if let Some((ts, ref s)) = *guard {
            if ts.elapsed().as_secs() < 3 {
                return (StatusCode::OK, s.clone()).into_response();
            }
        }
    }

    let mode = query.mode.unwrap_or_default();
    let n_lines = if mode == "trade" { "1000" } else { "1500" };
    let child = tokio::process::Command::new("journalctl")
        .args(["-u", "core-node", "-n", n_lines, "-r", "--no-pager"])
        .output();

    let result = tokio::time::timeout(std::time::Duration::from_millis(2000), child).await;

    match result {
        Ok(Ok(out)) => {
            let mut s = String::from_utf8_lossy(&out.stdout).to_string();
            if s.trim().is_empty() {
                s = format!(
                    "No logs available yet from core-node.\nStderr: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
            if let Ok(mut guard) = cache.lock() {
                *guard = Some((Instant::now(), s.clone()));
            }
            (StatusCode::OK, s).into_response()
        }
        Ok(Err(e)) => {
            let fallback = cache
                .lock()
                .ok()
                .and_then(|g| g.as_ref().map(|(_, s)| s.clone()))
                .unwrap_or_else(|| format!("Log read error: {}", e));
            (StatusCode::OK, fallback).into_response()
        }
        Err(_) => {
            // Timeout — return cached if available or friendly message
            let fallback = cache
                .lock()
                .ok()
                .and_then(|g| g.as_ref().map(|(_, s)| s.clone()))
                .unwrap_or_else(|| "⏳ Journal busy, refreshing in next cycle…".to_string());
            (StatusCode::OK, fallback).into_response()
        }
    }
}

// ─── GET /api/profiles ───
pub async fn handle_get_profiles(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };

    let profiles = match state.db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE owner = ? OR id = 'real-default' ORDER BY created_at ASC")?;
        let items: Vec<_> = stmt.query_map([username], row_to_profile)?.filter_map(|r| r.ok()).collect();
        Ok(items)
    }).await {
        Ok(p) => p,
        Err(_) => vec![],
    };

    (StatusCode::OK, Json(profiles)).into_response()
}

// ─── GET /api/unified ───
#[derive(serde::Deserialize)]
pub struct UnifiedQuery {
    profile_id: Option<String>,
}

pub async fn handle_get_unified(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<UnifiedQuery>,
) -> Response {
    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };

    // 1. Get Snapshot and Exchange Maps
    let snapshot = state
        .db
        .call(|conn| {
            let row: Result<String, rusqlite::Error> = conn.query_row(
                "SELECT value FROM server_cache WHERE key = 'snapshot'",
                [],
                |r| r.get(0),
            );
            let val: serde_json::Value = if let Ok(v) = row {
                serde_json::from_str(&v).unwrap_or(json!({}))
            } else {
                json!({})
            };
            Ok::<serde_json::Value, tokio_rusqlite::Error>(val)
        })
        .await
        .unwrap_or(json!({}));

    let exchanges_raw = state
        .db
        .call(|conn| {
            let row: Result<String, rusqlite::Error> = conn.query_row(
                "SELECT value FROM server_cache WHERE key = 'exchange_maps'",
                [],
                |r| r.get(0),
            );
            let val: serde_json::Value = if let Ok(v) = row {
                serde_json::from_str(&v).unwrap_or(json!({}))
            } else {
                json!({})
            };
            Ok::<serde_json::Value, tokio_rusqlite::Error>(val)
        })
        .await
        .unwrap_or(json!({}));

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let today_midnight_ms = now_ms - (now_ms % 86_400_000); // Rounds down to midnight UTC
    let daily_exch_stats = state.real_db.call(move |conn| {
        let mut map: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        let mut stmt = match conn.prepare("SELECT exchange_a, exchange_b, net_pnl_a, net_pnl_b FROM trades WHERE status='CLOSED' AND accounting_status='VERIFIED' AND exit_time >= ?") {
            Ok(s) => s,
            Err(_) => return Ok::<_, tokio_rusqlite::Error>(map)
        };
        let rows: Vec<_> = stmt.query_map([today_midnight_ms], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, f64>(2)?,
                r.get::<_, f64>(3)?,
            ))
        }).map(|iter| iter.filter_map(|r| r.ok()).collect::<Vec<_>>()).unwrap_or_default();

        // Reconciled leg PnL is attributed to its actual venue.
        for (exa, exb, net_a,net_b) in rows {
            *map.entry(exa).or_insert(0.0) += net_a;
            *map.entry(exb).or_insert(0.0) += net_b;
        }
        Ok::<_, tokio_rusqlite::Error>(map)
    }).await.unwrap_or_default();

    let db_closed_pnl_today_total: f64 = daily_exch_stats.values().sum();
    let mut exchanges = exchanges_raw;
    if let Some(obj) = exchanges.as_object_mut() {
        let mut daily_pnl_map = serde_json::Map::new();
        let mut daily_pct_map = serde_json::Map::new();
        let balances = obj.get("balances").and_then(|b| b.as_object());
        let snapshot_ts = obj
            .get("snapshot_ts")
            .and_then(|v| v.as_i64())
            .or_else(|| obj.get("ts").and_then(|v| v.as_i64()));
        let balances_ts = obj
            .get("balances_ts")
            .and_then(|v| v.as_i64())
            .or(snapshot_ts);
        let positions_ts = obj
            .get("positions_ts")
            .and_then(|v| v.as_i64())
            .or(snapshot_ts);
        let fees_ts = obj.get("fees_ts").and_then(|v| v.as_i64()).or(snapshot_ts);
        let snapshot_age_ms = snapshot_ts.map(|ts| (now_ms - ts).max(0)).unwrap_or(-1);
        let balances_age_ms = balances_ts.map(|ts| (now_ms - ts).max(0)).unwrap_or(-1);
        let positions_age_ms = positions_ts.map(|ts| (now_ms - ts).max(0)).unwrap_or(-1);
        let fees_age_ms = fees_ts.map(|ts| (now_ms - ts).max(0)).unwrap_or(-1);
        let current_balances: HashMap<String, f64> = balances
            .map(|bals| {
                bals.iter()
                    .filter_map(|(ex, val)| val.as_f64().map(|v| (ex.clone(), v)))
                    .collect()
            })
            .unwrap_or_default();

        // Versioned baseline key: reset baseline when balance methodology changes
        // (e.g. wallet -> equity normalization), so deltas stay comparable.
        let baseline_schema_version = "v2_equity";
        let day_key = format!(
            "cex_balance_baseline:{}:{}",
            baseline_schema_version,
            now_ms / 86_400_000
        );
        let (baseline_balances, baseline_ts_ms) = state
            .db
            .call({
                let day_key = day_key.clone();
                let current_balances = current_balances.clone();
                move |conn| {
                    let _: Result<usize, rusqlite::Error> = conn.execute(
                        "CREATE TABLE IF NOT EXISTS server_cache (key TEXT PRIMARY KEY, value TEXT, expires_at INTEGER)",
                        [],
                    );
                    let row: Result<String, rusqlite::Error> = conn.query_row(
                        "SELECT value FROM server_cache WHERE key = ?",
                        [day_key.clone()],
                        |r| r.get(0),
                    );
                    if let Ok(raw) = row {
                        let mut parsed_balances: HashMap<String, f64> = HashMap::new();
                        let mut parsed_ts = now_ms;
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) {
                            if let Some(obj) = v.as_object() {
                                if let Some(ts) = obj.get("ts").and_then(|x| x.as_i64()) {
                                    parsed_ts = ts;
                                }
                                if let Some(bals) = obj.get("balances").and_then(|x| x.as_object()) {
                                    for (ex, val) in bals {
                                        if let Some(f) = val.as_f64() {
                                            parsed_balances.insert(ex.clone(), f);
                                        }
                                    }
                                }
                            }
                        }
                        if parsed_balances.is_empty() {
                            parsed_balances = serde_json::from_str::<HashMap<String, f64>>(&raw)
                                .unwrap_or_default();
                        }
                        Ok::<(HashMap<String, f64>, i64), tokio_rusqlite::Error>((
                            parsed_balances,
                            parsed_ts,
                        ))
                    } else {
                        let baseline_json = serde_json::json!({
                            "ts": now_ms,
                            "balances": current_balances.clone()
                        })
                        .to_string();
                        let _ = conn.execute(
                            "INSERT INTO server_cache(key,value,expires_at) VALUES(?,?,?) \
                             ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
                            rusqlite::params![day_key, baseline_json, now_ms + (3 * 86_400_000)],
                        );
                        Ok::<(HashMap<String, f64>, i64), tokio_rusqlite::Error>((
                            current_balances,
                            now_ms,
                        ))
                    }
                }
            })
            .await
            .unwrap_or_else(|_| (HashMap::new(), now_ms));

        let mut cex_balance_delta_today = serde_json::Map::new();
        let mut cex_balance_delta_total_today = 0.0_f64;
        for (ex, curr) in &current_balances {
            let base = baseline_balances.get(ex).copied().unwrap_or(*curr);
            let delta = curr - base;
            cex_balance_delta_total_today += delta;
            cex_balance_delta_today.insert(ex.clone(), json!(delta));
        }
        let mut cex_balance_current = serde_json::Map::new();
        let mut cex_balance_baseline = serde_json::Map::new();
        for (ex, curr) in &current_balances {
            cex_balance_current.insert(ex.clone(), json!(curr));
            cex_balance_baseline.insert(
                ex.clone(),
                json!(baseline_balances.get(ex).copied().unwrap_or(*curr)),
            );
        }

        for (ex, pnl) in daily_exch_stats {
            daily_pnl_map.insert(ex.clone(), json!(pnl));
            if let Some(bals) = balances {
                if let Some(bal) = bals.get(&ex).and_then(|v| v.as_f64()) {
                    if bal > 0.0 {
                        daily_pct_map.insert(ex, json!((pnl / bal) * 100.0));
                    }
                }
            }
        }
        obj.insert("daily_pnl".to_string(), json!(daily_pnl_map));
        obj.insert("daily_pnl_pct".to_string(), json!(daily_pct_map));
        obj.insert("snapshot_age_ms".to_string(), json!(snapshot_age_ms));
        obj.insert("snapshot_ts".to_string(), json!(snapshot_ts.unwrap_or(-1)));
        obj.insert("balances_ts".to_string(), json!(balances_ts.unwrap_or(-1)));
        obj.insert(
            "positions_ts".to_string(),
            json!(positions_ts.unwrap_or(-1)),
        );
        obj.insert("fees_ts".to_string(), json!(fees_ts.unwrap_or(-1)));
        obj.insert("balances_age_ms".to_string(), json!(balances_age_ms));
        obj.insert("positions_age_ms".to_string(), json!(positions_age_ms));
        obj.insert("fees_age_ms".to_string(), json!(fees_age_ms));
        obj.insert(
            "cex_balance_delta_today".to_string(),
            json!(cex_balance_delta_today),
        );
        obj.insert(
            "cex_balance_delta_total_today".to_string(),
            json!(cex_balance_delta_total_today),
        );
        obj.insert(
            "db_closed_pnl_today_total".to_string(),
            json!(db_closed_pnl_today_total),
        );
        obj.insert(
            "cex_vs_db_delta_today".to_string(),
            json!(cex_balance_delta_total_today - db_closed_pnl_today_total),
        );
        // Wallet movements also contain transfers and funding timing. They cannot
        // be reconciled by distributing trade PnL equally across venues.
        for key in [
            "db_open_live_pnl_total",
            "db_expected_balance_delta_total",
            "cex_vs_db_expected_delta",
            "db_open_live_pnl_by_exchange",
            "db_expected_delta_by_exchange",
            "cex_vs_db_expected_by_exchange",
        ] {
            obj.insert(key.to_string(), Value::Null);
        }
        obj.insert(
            "cex_balance_current".to_string(),
            json!(cex_balance_current),
        );
        obj.insert(
            "cex_balance_baseline_today".to_string(),
            json!(cex_balance_baseline),
        );
        obj.insert("cex_baseline_ts_ms".to_string(), json!(baseline_ts_ms));
    }

    // 2. Get Profiles
    let profiles = state.db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE owner = ? OR id = 'real-default' ORDER BY created_at ASC")?;
        let items: Vec<_> = stmt.query_map([username], row_to_profile)?.filter_map(|r| r.ok()).collect();
        Ok(items)
    }).await.unwrap_or_else(|_| vec![]);

    let pid = query
        .profile_id
        .unwrap_or_else(|| "real-default".to_string());
    let _profile_mode = profiles
        .iter()
        .find(|p| p.id == pid)
        .map(|p| p.mode.clone())
        .unwrap_or_else(|| "real".into());
    let trade_db = state.real_db.clone();

    let active_trades = trade_db
        .call(|conn| {
            let mut stmt =
                conn.prepare("SELECT * FROM trades WHERE status IN ('ENTRY','RECONCILING','OPEN','CLOSING') ORDER BY entry_time DESC")?;
            let items: Vec<_> = stmt
                .query_map([], row_to_trade)?
                .filter_map(|r| r.ok())
                .collect();
            Ok(items)
        })
        .await
        .unwrap_or_else(|_| vec![]);

    // Enrich active trades with live data from DB columns written by core-node.
    // STRICT RULE: We trust ALL values from DB — core-node writes them from exchange API.
    // NO local PnL recalculation — dashboard is a pure DB mirror.
    let active_enriched: Vec<serde_json::Value> = active_trades
        .iter()
        .map(|t| {
            let mut v = serde_json::to_value(t).unwrap_or_default();
            if let Some(obj) = v.as_object_mut() {
                // Current prices from DB (written by core-node's live_update loop)
                obj.insert(
                    "current_price_a".into(),
                    json!(t.last_price_a.unwrap_or(t.entry_price_a)),
                );
                obj.insert(
                    "current_price_b".into(),
                    json!(t.last_price_b.unwrap_or(t.entry_price_b)),
                );
                obj.insert("current_spread".into(), json!(t.last_spread.unwrap_or(0.0)));
                obj.insert("gross_pnl".into(), json!(t.curr_gross_pnl));
                let valuation_fresh = t
                    .valuation_ms
                    .map(|at| now_ms >= at && now_ms - at <= 5000)
                    .unwrap_or(false);
                obj.insert(
                    "current_pnl".into(),
                    json!(if valuation_fresh {
                        t.current_net_estimate
                    } else {
                        None
                    }),
                );
                obj.insert("valuation_fresh".into(), json!(valuation_fresh));
                // Costs: directly from DB
                obj.insert(
                    "commission_cost".into(),
                    json!(t.commission_cost.unwrap_or(0.0)),
                );
                // Entry slippage (IOC target vs fill)
                obj.insert(
                    "slippage_cost".into(),
                    json!(t.slippage_cost.unwrap_or(0.0)),
                );
                // Exit slippage (separate for display)
                obj.insert(
                    "exit_slippage_cost".into(),
                    json!(t.exit_slippage_cost.unwrap_or(0.0)),
                );
                obj.insert("funding_cost".into(), json!(t.funding_cost.unwrap_or(0.0)));
            }
            v
        })
        .collect();

    let history_trades_raw = trade_db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT * FROM trades WHERE status='CLOSED' ORDER BY exit_time DESC LIMIT 50",
            )?;
            let items: Vec<_> = stmt
                .query_map([], row_to_trade)?
                .filter_map(|r| {
                    if let Err(e) = &r {
                        tracing::error!("row_to_trade parse error: {:?}", e);
                    }
                    r.ok()
                })
                .collect();
            Ok(items)
        })
        .await
        .unwrap_or_else(|_| vec![]);

    let history_trades: Vec<serde_json::Value> = history_trades_raw.into_iter().map(history_trade_view).collect();

    // 4. Compute aggregate stats — cached for 30s to avoid 131K+ row scans every poll
    static STATS_CACHE: OnceLock<Mutex<HashMap<String, (Instant, Value)>>> = OnceLock::new();
    let cache = STATS_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let cache_key = pid.clone();
    let cached = {
        let lock = cache.lock().unwrap();
        lock.get(&cache_key).and_then(|(ts, v)| {
            if ts.elapsed().as_secs() < 30 {
                Some(v.clone())
            } else {
                None
            }
        })
    };
    let stats = if let Some(v) = cached {
        v
    } else {
        let fresh = trade_db.call({
        let p = pid.clone();
        move |conn| {
            crate::statistics::profile_stats(conn, &p).map_err(Into::into)
        }
    }).await.unwrap_or_else(|e| {
        tracing::error!("Stats failed for [{}]: {}", pid, e);
        json!({
            "total_pnl": 0.0,
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "avg_pnl": 0.0,
            "avg_duration": 0,
            "equity_curve": Vec::<serde_json::Value>::new(),
            "pnl_by_exchange": {},
            "top_tokens": Vec::<serde_json::Value>::new(),
            "top_exchange_pairs": Vec::<serde_json::Value>::new()
        })
    });
        cache
            .lock()
            .unwrap()
            .insert(cache_key, (Instant::now(), fresh.clone()));
        fresh
    };

    // 5. Check kill-switch status from server_cache
    let kill_switch_active = state
        .db
        .call(|conn| {
            let val: Result<String, _> = conn.query_row(
                "SELECT value FROM server_cache WHERE key = 'kill_switch_active'",
                [],
                |r| r.get(0),
            );
            Ok::<bool, tokio_rusqlite::Error>(val.unwrap_or_else(|_| "0".to_string()) == "1")
        })
        .await
        .unwrap_or(false);

    // Return payload matching frontend expectations
    (
        StatusCode::OK,
        Json(json!({
            "snapshot": snapshot,
            "exchanges": exchanges,
            "profiles": profiles,
            "trades": {
                "active": active_enriched,
                "history": history_trades,
                "stats": stats
            },
            "profile_active": active_enriched,
            "profile_history": history_trades,
            "profile_stats": stats,
            "kill_switch_active": kill_switch_active,
        })),
    )
        .into_response()
}

// ─── POST /api/kill-switch/reset ───
pub async fn handle_kill_switch_reset(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if check_basic_auth(&headers, &state).is_none() {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    let result = state
        .db
        .call(|conn| {
            conn.execute(
            "INSERT INTO server_cache(key,value,expires_at) VALUES('kill_switch_active','0',0) \
             ON CONFLICT(key) DO UPDATE SET value='0'",
            [],
        )?;
            Ok(())
        })
        .await;

    match result {
        Ok(_) => {
            tracing::info!("[api] Kill-switch reset requested by user");
            (StatusCode::OK, Json(json!({"ok": true, "message": "Kill-switch reset. Engine will resume within 2 seconds."}))).into_response()
        }
        Err(e) => json_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("DB error: {}", e),
        ),
    }
}

// ─── GET /api/profiles/:id ───
pub async fn handle_get_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };

    let profile = state
        .db
        .call(move |conn| {
            let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE id=? AND (owner=? OR id='real-default')")?;
            let items: Vec<_> = stmt
                .query_map(params![id,username], row_to_profile)?
                .filter_map(|r| r.ok())
                .collect();
            Ok(items.into_iter().next())
        })
        .await
        .unwrap_or(None);

    match profile {
        Some(p) => (StatusCode::OK, Json(p)).into_response(),
        None => json_err(StatusCode::NOT_FOUND, "Profile not found"),
    }
}

// ─── POST /api/profiles ───
pub async fn handle_post_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    if let Err(e) = validate_profile_input(&payload) {
        return json_err(StatusCode::BAD_REQUEST, &e);
    }

    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let new_id = uuid::Uuid::new_v4().to_string();

    // Extract only what the frontend sends — fill everything else with sensible defaults.
    let name = payload
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("New Profile")
        .to_string();
    let mode = payload
        .get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("real")
        .to_string();

    let (default_vol, default_tp, default_stop, default_max_trades) = if mode == "real" {
        (100.0_f64, 0.2_f64, 0.3_f64, 3_i64)
    } else {
        (10.0_f64, 0.2_f64, 0.3_f64, 3_i64)
    };

    let id_clone = new_id.clone();
    let un_clone = username.clone();
    let res = state
        .db
        .call(move |conn| {
            Ok(conn.execute(
                "INSERT INTO trading_profiles
                (id,name,mode,is_active,min_entry_spread,max_entry_spread,trade_volume,
                 take_profit_percent,trailing_stop_percent,max_active_trades,commission_rate,
                 slippage_percent,funding_rate_8h,min_volume_millions,collector_interval_secs,
                 owner,created_at,updated_at,disabled_exchanges,min_depth_percent)
            VALUES (?,?,?,0,1.0,10.0,?,?,?,?,0.05,0.05,0.01,10.0,5,?,?,?,'[]',100.0)",
                params![
                    id_clone,
                    name,
                    mode,
                    default_vol,
                    default_tp,
                    default_stop,
                    default_max_trades,
                    un_clone,
                    now,
                    now
                ],
            )?)
        })
        .await;

    if let Err(e) = res {
        return json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }

    // Return the freshly-inserted row from DB so the frontend gets accurate data.
    let profile = state
        .db
        .call(move |conn| {
            let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE id=?")?;
            Ok(stmt.query_row([new_id], row_to_profile).ok())
        })
        .await
        .unwrap_or(None);

    match profile {
        Some(p) => (
            StatusCode::CREATED,
            Json(serde_json::to_value(p).unwrap_or_default()),
        )
            .into_response(),
        None => json_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Profile created but could not be fetched",
        ),
    }
}

// ─── PATCH /api/profiles/:id ───
pub async fn handle_patch_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(payload): Json<Value>,
) -> Response {
    if let Err(e) = validate_profile_input(&payload) {
        return json_err(StatusCode::BAD_REQUEST, &e);
    }

    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };

    // simplified update loop
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // We update is_active, min_entry_spread, max_entry_spread etc
    if let Some(obj) = payload.as_object() {
        let mut sets: Vec<String> = vec![];
        let mut params_vec: Vec<rusqlite::types::Value> = vec![];

        for (k, v) in obj {
            match k.as_str() {
                "is_active" => {
                    sets.push("is_active=?".to_string());
                    let active = v.as_bool().unwrap_or(v.as_i64().unwrap_or(0) > 0);
                    params_vec.push(rusqlite::types::Value::Integer(if active { 1 } else { 0 }));
                }
                "min_entry_spread"
                | "max_entry_spread"
                | "trade_volume"
                | "take_profit_percent"
                | "trailing_stop_percent"
                | "commission_rate"
                | "slippage_percent"
                | "funding_rate_8h"
                | "min_volume_millions"
                | "min_structural_deviation"
                | "min_depth_percent" => {
                    if let Some(f) = v.as_f64() {
                        sets.push(format!("{}=?", k));
                        params_vec.push(rusqlite::types::Value::Real(f));
                    }
                }
                "max_active_trades"
                | "collector_interval_secs"
                | "leverage"
                | "hft_timeout_ms"
                | "max_holding_time_ms" => {
                    if let Some(i) = v.as_i64() {
                        sets.push(format!("{}=?", k));
                        params_vec.push(rusqlite::types::Value::Integer(i));
                    }
                }
                "name" | "mode" | "pending_commands" | "margin_mode" | "strategy_type" => {
                    if let Some(s) = v.as_str() {
                        sets.push(format!("{}=?", k));
                        params_vec.push(rusqlite::types::Value::Text(s.to_string()));
                    }
                }
                "disabled_exchanges" => {
                    let s = if v.is_array() {
                        serde_json::to_string(v).unwrap_or("[]".into())
                    } else {
                        v.as_str().unwrap_or("[]").to_string()
                    };
                    sets.push(format!("{}=?", k));
                    params_vec.push(rusqlite::types::Value::Text(s));
                }
                _ => {}
            }
        }

        if sets.is_empty() {
            return json_err(StatusCode::BAD_REQUEST, "No valid fields");
        }

        sets.push("updated_at=?".to_string());
        params_vec.push(rusqlite::types::Value::Integer(now));

        let id_clone = id.clone();
        let query = format!("UPDATE trading_profiles SET {} WHERE id=? AND (owner IS NULL OR owner=? OR id = 'real-default')", sets.join(", "));

        // Push final params
        params_vec.push(rusqlite::types::Value::Text(id_clone));
        params_vec.push(rusqlite::types::Value::Text(username));

        if let Err(e) = state
            .db
            .call(move |conn| {
                let mut stmt = conn.prepare(&query)?;
                stmt.execute(rusqlite::params_from_iter(params_vec))?;
                Ok(())
            })
            .await
        {
            tracing::error!("Failed to update profile {}: {:?}", id, e);
            return json_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to update profile in database",
            );
        }
    }

    // Return updated profile
    let profile = state
        .db
        .call(move |conn| {
            let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE id=?")?;
            Ok(stmt.query_row([id], row_to_profile).ok())
        })
        .await
        .unwrap_or(None);

    match profile {
        Some(p) => (StatusCode::OK, Json(p)).into_response(),
        None => json_err(StatusCode::NOT_FOUND, "Profile not found"),
    }
}

// ─── DELETE /api/profiles/:id ───
pub async fn handle_delete_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };

    if id == "real-default" {
        return json_err(StatusCode::BAD_REQUEST, "Cannot delete default profiles");
    }

    // A profile with a ledger must remain addressable for recovery and audit.
    let profile_id = id.clone();
    let has_trades = state
        .real_db
        .call(move |conn| {
            Ok(conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM trades WHERE profile_id=?)",
                [profile_id],
                |r| r.get::<_, bool>(0),
            )?)
        })
        .await;
    match has_trades {
        Ok(false) => {}
        Ok(true) => {
            return json_err(
                StatusCode::CONFLICT,
                "Profile has trade history; disable it instead",
            )
        }
        Err(_) => {
            return json_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Cannot verify trade history",
            )
        }
    }
    let result = state
        .db
        .call(move |conn| {
            Ok(conn.execute(
                "DELETE FROM trading_profiles WHERE id=? AND owner=? AND is_active=0",
                params![id, username],
            )?)
        })
        .await;
    match result {
        Ok(1) => (StatusCode::OK, Json(json!({"success":true}))).into_response(),
        Ok(_) => json_err(StatusCode::CONFLICT, "Profile must be owned and inactive"),
        Err(_) => json_err(StatusCode::INTERNAL_SERVER_ERROR, "Cannot delete profile"),
    }
}

// Durable trading history cannot be deleted through the UI.
pub async fn handle_clear_trades(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(_id): Path<String>,
) -> Response {
    if check_basic_auth(&headers, &state).is_none() {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }
    json_err(
        StatusCode::CONFLICT,
        "Trade history is an execution ledger and cannot be cleared",
    )
}

async fn can_manage_profile(state: &AppState, id: String, username: String) -> bool {
    state.db.call(move |conn| {
        Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM trading_profiles WHERE id=? AND (owner=? OR id='real-default'))", params![id,username], |r|r.get::<_,bool>(0))?)
    }).await.unwrap_or(false)
}

// ─── POST /api/profiles/:id/market-close-all ───
pub async fn handle_market_close_all(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };

    if !can_manage_profile(&state, id.clone(), username).await {
        return json_err(StatusCode::FORBIDDEN, "Profile access denied");
    }
    let r1 = state.real_db.call(move |conn| {
        conn.execute("UPDATE trades SET manual_close_requested=1,close_reason='MANUAL_CLOSE' WHERE profile_id=? AND status IN ('ENTRY','RECONCILING','OPEN','CLOSING')", params![id])?;
        Ok(())
    }).await;

    if let Err(e) = r1 {
        return json_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Cannot request close: {e}"),
        );
    }

    (StatusCode::OK, Json(json!({ "success": true }))).into_response()
}

// ─── POST /api/trades/:id/close ───
pub async fn handle_market_close_trade(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(trade_id): Path<i64>,
) -> Response {
    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };

    let profile_id = state
        .real_db
        .call(move |conn| {
            Ok(conn
                .query_row(
                    "SELECT profile_id FROM trades WHERE id=?",
                    [trade_id],
                    |r| r.get::<_, String>(0),
                )
                .ok())
        })
        .await
        .ok()
        .flatten();
    let Some(profile_id) = profile_id else {
        return json_err(StatusCode::NOT_FOUND, "Trade not found");
    };
    if !can_manage_profile(&state, profile_id, username).await {
        return json_err(StatusCode::FORBIDDEN, "Profile access denied");
    }
    let r1 = state
        .real_db
        .call(move |conn| {
            conn.execute(
                "UPDATE trades SET manual_close_requested=1,close_reason='MANUAL_CLOSE' WHERE id=? AND status IN ('ENTRY','RECONCILING','OPEN','CLOSING')",
                params![trade_id],
            )?;
            Ok(())
        })
        .await;

    if let Err(e) = r1 {
        return json_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Cannot request close: {e}"),
        );
    }

    (StatusCode::OK, Json(json!({ "success": true }))).into_response()
}

fn legacy_execution_setting(key: &str) -> bool {
    matches!(
        key,
        "entry_verify_retries"
            | "entry_verify_delay_ms"
            | "close_verify_retries"
            | "close_verify_delay_ms"
            | "emergency_close_retries"
            | "exit_sanity_pct"
            | "balance_refresh_secs"
            | "multiplier_refresh_secs"
            | "funding_cache_secs"
            | "live_update_throttle_ms"
    )
}

// ─── GET /api/technical-settings ───
pub async fn handle_get_technical_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if check_basic_auth(&headers, &state).is_none() {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    let settings = state.db.call(|conn| {
        let mut stmt = conn.prepare("SELECT key, value, description, category FROM technical_settings ORDER BY category, key")?;
        let rows: Vec<Value> = stmt.query_map([], |row| {
            Ok(json!({
                "key": row.get::<_, String>(0)?,
                "value": row.get::<_, String>(1)?,
                "description": row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                "category": row.get::<_, Option<String>>(3)?.unwrap_or_else(|| "general".into()),
            }))
        })?.filter_map(|r| r.ok()).collect();
        Ok(rows)
    }).await.unwrap_or_default();

    let settings: Vec<_> = settings
        .into_iter()
        .filter(|s| !legacy_execution_setting(s["key"].as_str().unwrap_or("")))
        .collect();
    (StatusCode::OK, Json(json!(settings))).into_response()
}

// ─── PATCH /api/technical-settings ───
pub async fn handle_patch_technical_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Response {
    if check_basic_auth(&headers, &state).is_none() {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    let obj = match payload.as_object() {
        Some(o) => o.clone(),
        None => {
            return json_err(
                StatusCode::BAD_REQUEST,
                "Expected JSON object { key: value, ... }",
            )
        }
    };

    let defaults = serde_json::to_value(shared::TechnicalSettings::default())
        .expect("Static technical defaults");
    for (key, value) in &obj {
        let default = defaults.get(key);
        if default.is_none() || legacy_execution_setting(key) {
            return json_err(
                StatusCode::BAD_REQUEST,
                &format!("Unsupported setting {key}"),
            );
        }
        if key == "entry_order_type" && value.as_str() != Some("ioc") {
            return json_err(
                StatusCode::BAD_REQUEST,
                "Only bounded IOC entries are enabled",
            );
        }
        if default.unwrap().is_number() {
            let n = value
                .as_f64()
                .or_else(|| value.as_str().and_then(|x| x.parse::<f64>().ok()));
            if !n.map(|x| x.is_finite() && x >= 0.0).unwrap_or(false) {
                return json_err(
                    StatusCode::BAD_REQUEST,
                    &format!("Invalid numeric setting {key}"),
                );
            }
            if matches!(
                key.as_str(),
                "ema_tau_secs" | "max_spreads_per_cycle" | "api_timeout_ms"
            ) && n == Some(0.0)
            {
                return json_err(StatusCode::BAD_REQUEST, &format!("{key} must be positive"));
            }
        }
    }

    let result = state
        .db
        .call(move |conn| {
            let tx = conn.transaction()?;
            for (key, value) in &obj {
                let val_str = match value {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => value.to_string(),
                };
                tx.execute(
                    "UPDATE technical_settings SET value = ? WHERE key = ?",
                    params![val_str, key],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await;

    match result {
        Ok(_) => (StatusCode::OK, Json(json!({ "success": true }))).into_response(),
        Err(e) => json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct PositionCloseReq {
    pub exchange: String,
    pub symbol: String,
    pub size: f64,
}

pub async fn handle_close_exchange_position(
    axum::extract::State(state): axum::extract::State<crate::AppState>,
    headers: HeaderMap,
    axum::Json(payload): axum::Json<PositionCloseReq>,
) -> impl axum::response::IntoResponse {
    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };
    let (pair, exchange) = (payload.symbol.clone(), payload.exchange.clone());
    let profile_id = state.real_db.call(move |c| {
        Ok(c.query_row("SELECT profile_id FROM trades WHERE pair=? AND (exchange_a=? OR exchange_b=?) AND status IN ('ENTRY','RECONCILING','OPEN','CLOSING')",params![pair,exchange,exchange],|r|r.get::<_,String>(0)).ok())
    }).await.ok().flatten();
    let Some(profile_id) = profile_id else {
        return json_err(
            StatusCode::CONFLICT,
            "No owned trade for this position; reconciliation required",
        );
    };
    if !can_manage_profile(&state, profile_id, username).await {
        return json_err(StatusCode::FORBIDDEN, "Profile access denied");
    }
    let client = reqwest::Client::new();
    let res = client
        .post("http://127.0.0.1:3003/api/positions/close")
        .timeout(std::time::Duration::from_secs(5))
        .json(&payload)
        .send()
        .await;

    match res {
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status.is_success() {
                (
                    axum::http::StatusCode::OK,
                    axum::Json(serde_json::json!({ "success": true, "message": body })),
                )
                    .into_response()
            } else {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(serde_json::json!({ "error": body })),
                )
                    .into_response()
            }
        }
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({ "error": format!("core-node unreachable: {}", e) })),
        )
            .into_response(),
    }
}

// ─── GET /api/klines/:exchange/:pair/:interval/:limit ───
// Proxy to exchange REST kline APIs for chart rendering
#[derive(serde::Deserialize)]
pub struct KlinesQuery {
    #[serde(rename = "endTime")]
    end_time: Option<i64>,
}

pub async fn handle_get_klines(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((exchange, pair, interval, limit)): Path<(String, String, String, i32)>,
    Query(query): Query<KlinesQuery>,
) -> Response {
    if check_basic_auth(&headers, &state).is_none() {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    let limit = limit.min(1500).max(1);
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();

    let klines = match exchange.as_str() {
        "binance" => fetch_binance_klines(&client, &pair, &interval, limit, query.end_time).await,
        "bybit" => fetch_bybit_klines(&client, &pair, &interval, limit, query.end_time).await,
        "gate" => fetch_gate_klines(&client, &pair, &interval, limit, query.end_time).await,
        "kucoin" => fetch_kucoin_klines(&client, &pair, &interval, limit, query.end_time).await,
        "okx" => fetch_okx_klines(&client, &pair, &interval, limit, query.end_time).await,
        "htx" => fetch_htx_klines(&client, &pair, &interval, limit, query.end_time).await,
        "bingx" => fetch_bingx_klines(&client, &pair, &interval, limit, query.end_time).await,
        "xt" => fetch_xt_klines(&client, &pair, &interval, limit, query.end_time).await,
        "bitget" => fetch_bitget_klines(&client, &pair, &interval, limit, query.end_time).await,
        "toobit" => fetch_toobit_klines(&client, &pair, &interval, limit, query.end_time).await,
        "ascendex" => fetch_ascendex_klines(&client, &pair, &interval, limit, query.end_time).await,
        "bitmex" => fetch_bitmex_klines(&client, &pair, &interval, limit, query.end_time).await,
        _ => Ok(vec![]),
    };

    match klines {
        Ok(k) => (StatusCode::OK, Json(json!({ "klines": k }))).into_response(),
        Err(e) => {
            tracing::warn!("[klines] {} {} error: {}", exchange, pair, e);
            (StatusCode::OK, Json(json!({ "klines": [], "error": e }))).into_response()
        }
    }
}

#[derive(serde::Serialize)]
struct Kline {
    ts: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

async fn fetch_binance_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let mut url = format!(
        "https://fapi.binance.com/fapi/v1/klines?symbol={}&interval={}&limit={}",
        pair, interval, limit
    );
    if let Some(et) = end_time {
        url.push_str(&format!("&endTime={}", et));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Vec<Vec<Value>> = resp.json().await.map_err(|e| e.to_string())?;
    Ok(data
        .iter()
        .filter_map(|c| {
            Some(Kline {
                ts: c.get(0)?.as_i64()?,
                open: c.get(1)?.as_str()?.parse().ok()?,
                high: c.get(2)?.as_str()?.parse().ok()?,
                low: c.get(3)?.as_str()?.parse().ok()?,
                close: c.get(4)?.as_str()?.parse().ok()?,
                volume: c.get(5)?.as_str()?.parse().ok()?,
            })
        })
        .collect())
}

async fn fetch_bybit_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let bybit_iv = match interval {
        "1m" => "1",
        "3m" => "3",
        "5m" => "5",
        "15m" => "15",
        "1h" => "60",
        _ => "1",
    };
    let mut url = format!(
        "https://api.bybit.com/v5/market/kline?category=linear&symbol={}&interval={}&limit={}",
        pair, bybit_iv, limit
    );
    if let Some(et) = end_time {
        url.push_str(&format!("&end={}", et));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["result"]["list"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut klines: Vec<Kline> = list
        .iter()
        .filter_map(|c| {
            let arr = c.as_array()?;
            Some(Kline {
                ts: arr.get(0)?.as_str()?.parse().ok()?,
                open: arr.get(1)?.as_str()?.parse().ok()?,
                high: arr.get(2)?.as_str()?.parse().ok()?,
                low: arr.get(3)?.as_str()?.parse().ok()?,
                close: arr.get(4)?.as_str()?.parse().ok()?,
                volume: arr.get(5)?.as_str()?.parse().ok()?,
            })
        })
        .collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

async fn fetch_gate_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    // Gate uses underscore format: BTC_USDT
    let gate_pair = if pair.ends_with("USDT") {
        format!("{}_USDT", &pair[..pair.len() - 4])
    } else {
        pair.to_string()
    };
    let gate_iv = match interval {
        "1m" => "1m",
        "3m" => "3m",
        "5m" => "5m",
        "15m" => "15m",
        "1h" => "1h",
        _ => "1m",
    };
    let mut url = format!(
        "https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={}&interval={}&limit={}",
        gate_pair, gate_iv, limit
    );
    if let Some(et) = end_time {
        url.push_str(&format!("&to={}", et / 1000));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Vec<Value> = resp.json().await.map_err(|e| e.to_string())?;
    Ok(data
        .iter()
        .filter_map(|c| {
            Some(Kline {
                ts: c["t"]
                    .as_f64()
                    .or_else(|| c["t"].as_i64().map(|v| v as f64))? as i64
                    * 1000,
                open: c["o"].as_str()?.parse().ok()?,
                high: c["h"].as_str()?.parse().ok()?,
                low: c["l"].as_str()?.parse().ok()?,
                close: c["c"].as_str()?.parse().ok()?,
                volume: c["v"]
                    .as_f64()
                    .or_else(|| c["v"].as_i64().map(|v| v as f64))
                    .unwrap_or(0.0),
            })
        })
        .collect())
}

async fn fetch_kucoin_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let kc_pair = if pair.ends_with("USDT") {
        format!("{}USDTM", &pair[..pair.len() - 4])
    } else {
        pair.to_string()
    };
    let kc_iv = match interval {
        "1m" => "1",
        "3m" => "3",
        "5m" => "5",
        "15m" => "15",
        "1h" => "60",
        _ => "1",
    };
    let now_s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let granularity_min: i64 = kc_iv.parse().unwrap_or(1);
    let interval_ms = granularity_min * 60 * 1000;

    // KuCoin API returns max 200 per request — paginate to reach `limit`
    let mut all_klines: Vec<Kline> = Vec::new();
    let mut cursor_end_ms = end_time.unwrap_or(now_s * 1000);
    let mut remaining = limit;
    let max_pages = 3; // max 3 pages = 600 candles

    for _ in 0..max_pages {
        if remaining <= 0 {
            break;
        }
        let batch = remaining.min(200);
        let cursor_start_ms = cursor_end_ms - (batch as i64 * interval_ms);
        let url = format!(
            "https://api-futures.kucoin.com/api/v1/kline/query?symbol={}&granularity={}&from={}&to={}",
            kc_pair, granularity_min, cursor_start_ms, cursor_end_ms
        );
        let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
        let data: Value = resp.json().await.map_err(|e| e.to_string())?;
        let list = data["data"].as_array().cloned().unwrap_or_default();
        if list.is_empty() {
            break;
        }

        let batch_klines: Vec<Kline> = list
            .iter()
            .filter_map(|c| {
                let arr = c.as_array()?;
                Some(Kline {
                    ts: arr
                        .get(0)?
                        .as_f64()
                        .or_else(|| arr.get(0)?.as_i64().map(|v| v as f64))?
                        as i64,
                    open: arr.get(1)?.as_f64()?,
                    high: arr.get(2)?.as_f64()?,
                    low: arr.get(3)?.as_f64()?,
                    close: arr.get(4)?.as_f64()?,
                    volume: arr.get(5)?.as_f64().unwrap_or(0.0),
                })
            })
            .collect();

        remaining -= batch_klines.len() as i32;
        cursor_end_ms = cursor_start_ms; // next page ends where this one started
        all_klines.extend(batch_klines);
    }

    all_klines.sort_by_key(|k| k.ts);
    all_klines.dedup_by_key(|k| k.ts);
    Ok(all_klines)
}

async fn fetch_okx_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    // OKX uses format: BTC-USDT-SWAP
    let okx_pair = if pair.ends_with("USDT") {
        format!("{}-USDT-SWAP", &pair[..pair.len() - 4])
    } else {
        pair.to_string()
    };
    let mut url = format!(
        "https://www.okx.com/api/v5/market/candles?instId={}&bar={}&limit={}",
        okx_pair,
        interval,
        limit.min(300)
    );
    if let Some(et) = end_time {
        url.push_str(&format!("&after={}", et));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["data"].as_array().cloned().unwrap_or_default();
    let mut klines: Vec<Kline> = list
        .iter()
        .filter_map(|c| {
            let arr = c.as_array()?;
            Some(Kline {
                ts: arr.get(0)?.as_str()?.parse().ok()?,
                open: arr.get(1)?.as_str()?.parse().ok()?,
                high: arr.get(2)?.as_str()?.parse().ok()?,
                low: arr.get(3)?.as_str()?.parse().ok()?,
                close: arr.get(4)?.as_str()?.parse().ok()?,
                volume: arr.get(5)?.as_str()?.parse().unwrap_or(0.0),
            })
        })
        .collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

async fn fetch_htx_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    _end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let htx_pair = pair.to_lowercase();
    let htx_iv = match interval {
        "1m" => "1min",
        "3m" => "3min",
        "5m" => "5min",
        "15m" => "15min",
        "1h" => "60min",
        _ => "1min",
    };
    let url = format!(
        "https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code={}&period={}&size={}",
        htx_pair, htx_iv, limit.min(2000)
    );
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["data"].as_array().cloned().unwrap_or_default();
    Ok(list
        .iter()
        .filter_map(|c| {
            Some(Kline {
                ts: c["id"].as_i64()? * 1000,
                open: c["open"].as_f64()?,
                high: c["high"].as_f64()?,
                low: c["low"].as_f64()?,
                close: c["close"].as_f64()?,
                volume: c["vol"].as_f64().unwrap_or(0.0),
            })
        })
        .collect())
}

async fn fetch_bingx_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    // BingX uses format: BTC-USDT
    let bingx_pair = if pair.ends_with("USDT") {
        format!("{}-USDT", &pair[..pair.len() - 4])
    } else {
        pair.to_string()
    };
    let bingx_iv = match interval {
        "1m" => "1m",
        "3m" => "3m",
        "5m" => "5m",
        "15m" => "15m",
        "1h" => "1h",
        _ => "1m",
    };
    let mut url =
        format!(
        "https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol={}&interval={}&limit={}",
        bingx_pair, bingx_iv, limit.min(1440)
    );
    if let Some(et) = end_time {
        url.push_str(&format!("&endTime={}", et));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data.as_array().cloned().unwrap_or_default();
    Ok(list
        .iter()
        .filter_map(|c| {
            Some(Kline {
                ts: c["time"].as_i64()?,
                open: c["open"].as_str()?.parse().ok()?,
                high: c["high"].as_str()?.parse().ok()?,
                low: c["low"].as_str()?.parse().ok()?,
                close: c["close"].as_str()?.parse().ok()?,
                volume: c["volume"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
            })
        })
        .collect())
}

async fn fetch_xt_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    // XT uses lowercase with underscore: btc_usdt
    let xt_pair = if pair.ends_with("USDT") {
        format!("{}_usdt", pair[..pair.len() - 4].to_lowercase())
    } else {
        pair.to_lowercase()
    };
    let xt_iv = match interval {
        "1m" => "1m",
        "3m" => "3m",
        "5m" => "5m",
        "15m" => "15m",
        "1h" => "1h",
        _ => "1m",
    };
    let mut url = format!(
        "https://fapi.xt.com/future/market/v1/public/q/kline?symbol={}&interval={}&limit={}",
        xt_pair,
        xt_iv,
        limit.min(200)
    );
    if let Some(et) = end_time {
        url.push_str(&format!("&endTime={}", et));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["result"].as_array().cloned().unwrap_or_default();
    Ok(list
        .iter()
        .filter_map(|c| {
            Some(Kline {
                ts: c["t"].as_i64()?,
                open: c["o"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| c["o"].as_f64())?,
                high: c["h"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| c["h"].as_f64())?,
                low: c["l"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| c["l"].as_f64())?,
                close: c["c"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| c["c"].as_f64())?,
                volume: c["v"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| c["v"].as_f64())
                    .unwrap_or(0.0),
            })
        })
        .collect())
}

async fn fetch_bitget_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    // Bitget uses format: BTCUSDT (same as input)
    let bg_iv = match interval {
        "1m" => "1m",
        "3m" => "3m",
        "5m" => "5m",
        "15m" => "15m",
        "1h" => "1H",
        _ => "1m",
    };
    let mut url = format!(
        "https://api.bitget.com/api/v2/mix/market/candles?productType=USDT-FUTURES&symbol={}&granularity={}&limit={}",
        pair, bg_iv, limit.min(1000)
    );
    if let Some(et) = end_time {
        url.push_str(&format!("&endTime={}", et));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["data"].as_array().cloned().unwrap_or_default();
    let mut klines: Vec<Kline> = list
        .iter()
        .filter_map(|c| {
            let arr = c.as_array()?;
            Some(Kline {
                ts: arr.get(0)?.as_str()?.parse().ok()?,
                open: arr.get(1)?.as_str()?.parse().ok()?,
                high: arr.get(2)?.as_str()?.parse().ok()?,
                low: arr.get(3)?.as_str()?.parse().ok()?,
                close: arr.get(4)?.as_str()?.parse().ok()?,
                volume: arr
                    .get(5)?
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
            })
        })
        .collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

async fn fetch_toobit_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let mut url = format!(
        "https://api.toobit.com/api/v1/klines?symbol={}&interval={}&limit={}",
        pair,
        interval,
        limit.min(1000)
    );
    if let Some(et) = end_time {
        url.push_str(&format!("&endTime={}", et));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data.as_array().cloned().unwrap_or_default();
    let mut klines: Vec<Kline> = list
        .iter()
        .filter_map(|c| {
            let arr = c.as_array()?;
            Some(Kline {
                ts: arr.get(0)?.as_i64()?,
                open: arr.get(1)?.as_str()?.parse().ok()?,
                high: arr.get(2)?.as_str()?.parse().ok()?,
                low: arr.get(3)?.as_str()?.parse().ok()?,
                close: arr.get(4)?.as_str()?.parse().ok()?,
                volume: arr
                    .get(5)?
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
            })
        })
        .collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

async fn fetch_ascendex_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let asc_pair = if pair.ends_with("USDT") {
        format!("{}-PERP", &pair[..pair.len() - 4])
    } else {
        pair.to_string()
    };
    let asc_iv = match interval {
        "1m" => "1",
        "3m" => "3",
        "5m" => "5",
        "15m" => "15",
        "1h" => "60",
        _ => "1",
    };
    let mut url = format!(
        "https://ascendex.com/api/pro/v1/futures/barhist?symbol={}&interval={}&n={}",
        asc_pair,
        asc_iv,
        limit.min(500)
    );
    if let Some(et) = end_time {
        url.push_str(&format!("&to={}", et));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["data"].as_array().cloned().unwrap_or_default();
    let mut klines: Vec<Kline> = list
        .iter()
        .filter_map(|c| {
            let item = &c["data"];
            Some(Kline {
                ts: item["ts"].as_i64()?,
                open: item["o"].as_str()?.parse().ok()?,
                high: item["h"].as_str()?.parse().ok()?,
                low: item["l"].as_str()?.parse().ok()?,
                close: item["c"].as_str()?.parse().ok()?,
                volume: item["v"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
            })
        })
        .collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

async fn fetch_bitmex_klines(
    client: &reqwest::Client,
    pair: &str,
    interval: &str,
    limit: i32,
    end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let bm_pair = if pair == "BTCUSDT" { "XBTUSDT" } else { pair };
    let bm_iv = match interval {
        "1m" => "1m",
        "5m" => "5m",
        "1h" => "1h",
        "1d" => "1d",
        _ => "1m",
    };
    let mut url = format!(
        "https://www.bitmex.com/api/v1/trade/bucketed?binSize={}&partial=false&symbol={}&count={}&reverse=true",
        bm_iv, bm_pair, limit.min(500)
    );
    if let Some(et) = end_time {
        // BitMEX uses ISO timestamp
        let dt = chrono::DateTime::from_timestamp_millis(et).unwrap_or_default();
        url.push_str(&format!("&endTime={}", dt.to_rfc3339()));
    }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let list: Vec<Value> = resp.json().await.map_err(|e| e.to_string())?;
    let mut klines: Vec<Kline> = list
        .iter()
        .filter_map(|c| {
            let ts_str = c["timestamp"].as_str()?;
            let dt = chrono::DateTime::parse_from_rfc3339(ts_str).ok()?;
            Some(Kline {
                ts: dt.timestamp_millis(),
                open: c["open"].as_f64()?,
                high: c["high"].as_f64()?,
                low: c["low"].as_f64()?,
                close: c["close"].as_f64()?,
                volume: c["volume"].as_f64().unwrap_or(0.0),
            })
        })
        .collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

// ─── GET /api/system/metrics ───
// Server health: CPU, RAM, Disk, Uptime

struct CpuSample {
    _timestamp: Instant,
    work: u64,
    total: u64,
}

fn read_cpu_metrics() -> (f64, f64, f64, f64, usize) {
    static PREV_CPU: OnceLock<Mutex<Option<CpuSample>>> = OnceLock::new();
    let prev_lock = PREV_CPU.get_or_init(|| Mutex::new(None));

    let stat_content = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let mut cores = 0;
    let mut cpu_line = "";

    for line in stat_content.lines() {
        if line.starts_with("cpu ") {
            cpu_line = line;
        } else if line.starts_with("cpu") {
            cores += 1;
        }
    }
    if cores == 0 {
        cores = 1;
    }

    let mut usage_pct = 0.0;
    if !cpu_line.is_empty() {
        let parts: Vec<u64> = cpu_line
            .split_whitespace()
            .skip(1)
            .filter_map(|s| s.parse().ok())
            .collect();
        if parts.len() >= 4 {
            let user = parts[0];
            let nice = parts[1];
            let system = parts[2];
            let idle = parts[3];
            let iowait = parts.get(4).copied().unwrap_or(0);
            let irq = parts.get(5).copied().unwrap_or(0);
            let softirq = parts.get(6).copied().unwrap_or(0);
            let steal = parts.get(7).copied().unwrap_or(0);

            let work = user + nice + system + irq + softirq + steal;
            let total = work + idle + iowait;

            if let Ok(mut guard) = prev_lock.lock() {
                if let Some(prev) = guard.as_ref() {
                    let diff_work = work.saturating_sub(prev.work);
                    let diff_total = total.saturating_sub(prev.total);
                    if diff_total > 0 {
                        usage_pct = (diff_work as f64 / diff_total as f64) * 100.0;
                    }
                }
                *guard = Some(CpuSample {
                    _timestamp: Instant::now(),
                    work,
                    total,
                });
            }
        }
    }

    let loadavg_content = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    let load_parts: Vec<f64> = loadavg_content
        .split_whitespace()
        .take(3)
        .filter_map(|s| s.parse().ok())
        .collect();

    let l1 = load_parts.first().copied().unwrap_or(0.0);
    let l5 = load_parts.get(1).copied().unwrap_or(0.0);
    let l15 = load_parts.get(2).copied().unwrap_or(0.0);

    (usage_pct.min(100.0).max(0.0), l1, l5, l15, cores)
}

fn read_ram_metrics() -> (f64, f64, f64, f64) {
    let content = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let mut total_kb: f64 = 0.0;
    let mut avail_kb: f64 = 0.0;

    for line in content.lines() {
        if line.starts_with("MemTotal:") {
            total_kb = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
        } else if line.starts_with("MemAvailable:") {
            avail_kb = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
        }
    }

    let total_mb = total_kb / 1024.0;
    let free_mb = avail_kb / 1024.0;
    let used_mb = (total_mb - free_mb).max(0.0);
    let used_pct = if total_mb > 0.0 {
        (used_mb / total_mb) * 100.0
    } else {
        0.0
    };

    (total_mb, used_mb, free_mb, used_pct)
}

async fn read_disk_metrics() -> (f64, f64, f64, f64) {
    let out = tokio::process::Command::new("df")
        .args(["-k", "/"])
        .output()
        .await;

    if let Ok(output) = out {
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(line) = stdout.lines().nth(1) {
            let tokens: Vec<&str> = line.split_whitespace().collect();
            if tokens.len() >= 4 {
                let total_kb: f64 = tokens[1].parse().unwrap_or(0.0);
                let used_kb: f64 = tokens[2].parse().unwrap_or(0.0);
                let avail_kb: f64 = tokens[3].parse().unwrap_or(0.0);

                let total_gb = total_kb / (1024.0 * 1024.0);
                let used_gb = used_kb / (1024.0 * 1024.0);
                let free_gb = avail_kb / (1024.0 * 1024.0);
                let used_pct = if total_gb > 0.0 {
                    (used_gb / total_gb) * 100.0
                } else {
                    0.0
                };

                return (total_gb, used_gb, free_gb, used_pct);
            }
        }
    }

    (0.0, 0.0, 0.0, 0.0)
}

fn read_uptime() -> u64 {
    std::fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|s| s.split_whitespace().next()?.parse::<f64>().ok())
        .map(|f| f as u64)
        .unwrap_or(0)
}

pub async fn handle_get_system_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if check_basic_auth(&headers, &state).is_none() {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    let (cpu_usage, l1, l5, l15, cores) = read_cpu_metrics();
    let (ram_total, ram_used, ram_free, ram_pct) = read_ram_metrics();
    let (disk_total, disk_used, disk_free, disk_pct) = read_disk_metrics().await;
    let uptime = read_uptime();

    let res = json!({
        "cpu": {
            "usage_pct": (cpu_usage * 10.0).round() / 10.0,
            "load_1m": l1,
            "load_5m": l5,
            "load_15m": l15,
            "cores": cores
        },
        "ram": {
            "total_mb": ram_total.round(),
            "used_mb": ram_used.round(),
            "free_mb": ram_free.round(),
            "used_pct": (ram_pct * 10.0).round() / 10.0
        },
        "disk": {
            "total_gb": (disk_total * 10.0).round() / 10.0,
            "used_gb": (disk_used * 10.0).round() / 10.0,
            "free_gb": (disk_free * 10.0).round() / 10.0,
            "used_pct": (disk_pct * 10.0).round() / 10.0
        },
        "uptime_secs": uptime
    });

    (StatusCode::OK, Json(res)).into_response()
}

fn validate_profile_input(payload: &Value) -> Result<(), String> {
    let obj = payload.as_object().ok_or("Expected profile object")?;
    for (key, value) in obj {
        let positive = matches!(
            key.as_str(),
            "trade_volume" | "max_active_trades" | "leverage" | "collector_interval_secs"
        );
        let numeric = positive
            || matches!(
                key.as_str(),
                "min_entry_spread"
                    | "max_entry_spread"
                    | "take_profit_percent"
                    | "trailing_stop_percent"
                    | "commission_rate"
                    | "slippage_percent"
                    | "min_volume_millions"
                    | "min_structural_deviation"
                    | "min_depth_percent"
                    | "max_holding_time_ms"
                    | "hft_timeout_ms"
            );
        if numeric {
            let n = value
                .as_f64()
                .ok_or_else(|| format!("{key} must be numeric"))?;
            if !n.is_finite() || n < 0.0 || (positive && n == 0.0) {
                return Err(format!("Invalid {key}"));
            }
        }
        if (key == "strategy_type" && value.as_str() != Some("ema"))
            || (key == "margin_mode" && value.as_str() != Some("cross"))
        {
            return Err("Managed execution requires EMA and cross margin".into());
        }
    }
    if let (Some(a), Some(b)) = (
        payload["min_entry_spread"].as_f64(),
        payload["max_entry_spread"].as_f64(),
    ) {
        if a > b {
            return Err("Minimum spread exceeds maximum".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod integrity_api_tests {
    use super::*;
    #[tokio::test]
    async fn history_does_not_invent_net_pnl_before_reconciliation() {
        let db = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
        crate::db::init_trades_db(&db).await.unwrap();
        let trade = db.call(|c| {
            c.execute_batch("INSERT INTO trades(pair,exchange_a,exchange_b,direction,volume,
              entry_price_a,entry_price_b,entry_spread,entry_time,highest_spread_reached,status,
              gross_pnl,commission_cost,funding_cost,accounting_status)
              VALUES('BTCUSDT','binance','kucoin','binance>kucoin',10,100,99,1,1,1,'CLOSED',
              1.0,NULL,NULL,'FEES_PENDING');")?;
            c.query_row("SELECT * FROM trades",[],row_to_trade).map_err(Into::into)
        }).await.unwrap();
        let view = history_trade_view(trade);
        assert!(view["pnl"].is_null());
        assert_eq!(view["gross_pnl"],1.0);
        assert_eq!(view["accounting_status"],"FEES_PENDING");
    }
    #[tokio::test]
    async fn settings_require_auth_and_respect_owner_and_named_columns() {
        let db = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
        crate::db::init_db(&db).await.unwrap();
        db.call(|c| {
            c.execute_batch("INSERT INTO trading_profiles(id,name,owner,created_at,updated_at,
              leverage,min_depth_percent,hft_timeout_ms,max_holding_time_ms,min_structural_deviation)
              VALUES('mine','Mine','dima',1,1,3,275,4321,123456,0.7),
                    ('theirs','Theirs','sergei',1,1,4,300,5432,234567,0.8);")?;
            Ok(())
        }).await.unwrap();
        let state = AppState {
            db,
            real_db: tokio_rusqlite::Connection::open_in_memory().await.unwrap(),
            collector_token: "test-collector".into(),
            pass_dima: "test-password".into(),
            pass_sergei: "different-test-password".into(),
        };
        assert_eq!(handle_get_all_settings(State(state.clone()), HeaderMap::new()).await.status(), StatusCode::UNAUTHORIZED);
        let mut headers = HeaderMap::new();
        use base64::Engine as _;
        headers.insert("authorization", format!("Basic {}",base64::engine::general_purpose::STANDARD.encode("dima:test-password")).parse().unwrap());
        assert_eq!(handle_get_profile(State(state.clone()),headers.clone(),Path("theirs".into())).await.status(),StatusCode::NOT_FOUND);
        let response = handle_get_all_settings(State(state),headers).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(),1_000_000).await.unwrap();
        let profiles: Value = serde_json::from_slice(&body).unwrap();
        let profiles = profiles.as_array().unwrap();
        assert!(!profiles.iter().any(|p|p["id"]=="theirs"));
        let mine = profiles.iter().find(|p|p["id"]=="mine").unwrap();
        assert_eq!(mine["leverage"],3);
        assert_eq!(mine["min_depth_percent"],275.0);
        assert_eq!(mine["hft_timeout_ms"],4321);
        assert_eq!(mine["max_holding_time_ms"],123456);
        assert_eq!(mine["min_structural_deviation"],0.7);
    }
    #[tokio::test]
    async fn empty_ledger_reports_unknown_wallet_reconciliation_and_rejects_clear() {
        let db = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
        let real_db = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
        crate::db::init_db(&db).await.unwrap();
        crate::db::init_trades_db(&real_db).await.unwrap();
        let state = AppState {
            db,
            real_db,
            collector_token: "test-collector".into(),
            pass_dima: "test-password".into(),
            pass_sergei: "different-test-password".into(),
        };
        let mut headers = HeaderMap::new();
        use base64::Engine as _;
        headers.insert(
            "authorization",
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode("dima:test-password")
            )
            .parse()
            .unwrap(),
        );
        let response = handle_get_unified(
            State(state.clone()),
            headers.clone(),
            Query(UnifiedQuery {
                profile_id: Some("real-default".into()),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["profile_stats"]["total_trades"], 0);
        assert!(value["exchanges"]
            .get("db_expected_balance_delta_total")
            .unwrap()
            .is_null());
        assert_eq!(
            handle_clear_trades(State(state.clone()), headers, Path("real-default".into()))
                .await
                .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            handle_get_unified(
                State(state),
                HeaderMap::new(),
                Query(UnifiedQuery { profile_id: None })
            )
            .await
            .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    #[test]
    fn old_development_bearer_does_not_authenticate() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer local_dev".parse().unwrap());
        assert!(!check_auth(&headers, "configured-test-secret"));
        headers.insert(
            "authorization",
            "Bearer configured-test-secret".parse().unwrap(),
        );
        assert!(check_auth(&headers, "configured-test-secret"));
    }
}
