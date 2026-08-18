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

fn check_auth(headers: &HeaderMap, secret: &str) -> bool {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == format!("Bearer {}", secret) || v == "Bearer local_dev")
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
pub async fn handle_get_all_settings(State(state): State<AppState>) -> Response {
    let profiles = match state
        .db
        .call(|conn| {
            let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE mode = 'real'")?;
            let items: Vec<_> = stmt
                .query_map([], row_to_profile)?
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
    let n_lines = if mode == "trade" { "200" } else { "300" };
    let child = tokio::process::Command::new("journalctl")
        .args([
            "-u",
            "core-node",
            "-n",
            n_lines,
            "-r",
            "--no-pager",
        ])
        .output();

    let result = tokio::time::timeout(std::time::Duration::from_millis(2000), child).await;

    match result {
        Ok(Ok(out)) => {
            let mut s = String::from_utf8_lossy(&out.stdout).to_string();
            if s.trim().is_empty() {
                s = format!("No logs available yet from core-node.\nStderr: {}", String::from_utf8_lossy(&out.stderr));
            }
            if let Ok(mut guard) = cache.lock() {
                *guard = Some((Instant::now(), s.clone()));
            }
            (StatusCode::OK, s).into_response()
        }
        Ok(Err(e)) => {
            let fallback = cache.lock().ok().and_then(|g| g.as_ref().map(|(_, s)| s.clone()))
                .unwrap_or_else(|| format!("Log read error: {}", e));
            (StatusCode::OK, fallback).into_response()
        }
        Err(_) => {
            // Timeout — return cached if available or friendly message
            let fallback = cache.lock().ok().and_then(|g| g.as_ref().map(|(_, s)| s.clone()))
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
        let mut stmt = match conn.prepare("SELECT exchange_a, exchange_b, COALESCE(pnl,0.0) FROM trades WHERE status='CLOSED' AND exit_time >= ?") {
            Ok(s) => s,
            Err(_) => return Ok::<_, tokio_rusqlite::Error>(map)
        };
        let rows: Vec<_> = stmt.query_map([today_midnight_ms], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, f64>(2)?,
            ))
        }).map(|iter| iter.filter_map(|r| r.ok()).collect::<Vec<_>>()).unwrap_or_default();

        // Canonical per-exchange day PnL allocation:
        // each closed trade contributes half of final net pnl to each leg exchange.
        for (exa, exb, net_pnl) in rows {
            let half = net_pnl / 2.0;
            *map.entry(exa).or_insert(0.0) += half;
            *map.entry(exb).or_insert(0.0) += half;
        }
        Ok::<_, tokio_rusqlite::Error>(map)
    }).await.unwrap_or_default();

    let db_closed_pnl_today_total: f64 = daily_exch_stats.values().sum();
    let db_open_live_pnl_total: f64 = state
        .real_db
        .call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT COALESCE(SUM(
                    COALESCE(curr_gross_pnl,
                        COALESCE(pnl_a,0.0)+COALESCE(pnl_b,0.0)
                        - COALESCE(commission_cost,0.0)
                        - COALESCE(funding_cost,0.0)
                    )
                ), 0.0)
                 FROM trades
                 WHERE status IN ('OPEN','CLOSING')",
            )?;
            let v: f64 = stmt.query_row([], |r| r.get(0))?;
            Ok::<f64, tokio_rusqlite::Error>(v)
        })
        .await
        .unwrap_or(0.0);
    let db_open_live_pnl_by_exchange: HashMap<String, f64> = state
        .real_db
        .call(|conn| {
            let mut by_ex: HashMap<String, f64> = HashMap::new();
            let mut stmt = conn.prepare(
                "SELECT exchange_a, exchange_b,
                        COALESCE(curr_gross_pnl,
                            COALESCE(pnl_a,0.0)+COALESCE(pnl_b,0.0)
                            - COALESCE(commission_cost,0.0)
                            - COALESCE(funding_cost,0.0)
                        ) as net_live
                 FROM trades
                 WHERE status IN ('OPEN','CLOSING')",
            )?;
            let rows: Vec<(String, String, f64)> = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .filter_map(|r| r.ok())
                .collect();
            for (ex_a, ex_b, net_live) in rows {
                let half = net_live / 2.0;
                *by_ex.entry(ex_a).or_insert(0.0) += half;
                *by_ex.entry(ex_b).or_insert(0.0) += half;
            }
            Ok::<HashMap<String, f64>, tokio_rusqlite::Error>(by_ex)
        })
        .await
        .unwrap_or_default();
    let mut exchanges = exchanges_raw.clone();
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
        let fees_ts = obj
            .get("fees_ts")
            .and_then(|v| v.as_i64())
            .or(snapshot_ts);
        let snapshot_age_ms = snapshot_ts
            .map(|ts| (now_ms - ts).max(0))
            .unwrap_or(-1);
        let balances_age_ms = balances_ts
            .map(|ts| (now_ms - ts).max(0))
            .unwrap_or(-1);
        let positions_age_ms = positions_ts
            .map(|ts| (now_ms - ts).max(0))
            .unwrap_or(-1);
        let fees_age_ms = fees_ts
            .map(|ts| (now_ms - ts).max(0))
            .unwrap_or(-1);
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
        obj.insert("positions_ts".to_string(), json!(positions_ts.unwrap_or(-1)));
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
        let db_expected_balance_delta_total = db_closed_pnl_today_total + db_open_live_pnl_total;
        obj.insert(
            "db_open_live_pnl_total".to_string(),
            json!(db_open_live_pnl_total),
        );
        obj.insert(
            "db_expected_balance_delta_total".to_string(),
            json!(db_expected_balance_delta_total),
        );
        obj.insert(
            "cex_vs_db_expected_delta".to_string(),
            json!(cex_balance_delta_total_today - db_expected_balance_delta_total),
        );
        let mut db_open_live_pnl_by_exchange_json = serde_json::Map::new();
        for (ex, v) in &db_open_live_pnl_by_exchange {
            db_open_live_pnl_by_exchange_json.insert(ex.clone(), json!(v));
        }
        let mut db_expected_delta_by_exchange = serde_json::Map::new();
        let mut cex_vs_db_expected_by_exchange = serde_json::Map::new();
        for (ex, cex_delta_val) in &cex_balance_delta_today {
            let cex_delta = cex_delta_val.as_f64().unwrap_or(0.0);
            let db_closed = daily_pnl_map.get(ex).and_then(|v| v.as_f64()).unwrap_or(0.0);
            let db_open_live = db_open_live_pnl_by_exchange.get(ex).copied().unwrap_or(0.0);
            let db_expected = db_closed + db_open_live;
            db_expected_delta_by_exchange.insert(ex.clone(), json!(db_expected));
            cex_vs_db_expected_by_exchange.insert(ex.clone(), json!(cex_delta - db_expected));
        }
        obj.insert("cex_balance_current".to_string(), json!(cex_balance_current));
        obj.insert("cex_balance_baseline_today".to_string(), json!(cex_balance_baseline));
        obj.insert("cex_baseline_ts_ms".to_string(), json!(baseline_ts_ms));
        obj.insert(
            "db_open_live_pnl_by_exchange".to_string(),
            json!(db_open_live_pnl_by_exchange_json),
        );
        obj.insert(
            "db_expected_delta_by_exchange".to_string(),
            json!(db_expected_delta_by_exchange),
        );
        obj.insert(
            "cex_vs_db_expected_by_exchange".to_string(),
            json!(cex_vs_db_expected_by_exchange),
        );
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
                conn.prepare("SELECT * FROM trades WHERE status IN ('OPEN','CLOSING') ORDER BY entry_time DESC")?;
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
                let db_pnl_a = t.pnl_a.unwrap_or(0.0);
                let db_pnl_b = t.pnl_b.unwrap_or(0.0);
                let actual_gross = db_pnl_a + db_pnl_b;
                obj.insert("gross_pnl".into(), json!(actual_gross));
                obj.insert("pnl_a".into(), json!(db_pnl_a));
                obj.insert("pnl_b".into(), json!(db_pnl_b));
                // Net PnL: trust curr_gross_pnl from DB
                obj.insert(
                    "current_pnl".into(),
                    json!(t.curr_gross_pnl.unwrap_or(actual_gross)),
                );
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

    let history_trades_raw = trade_db.call(|conn| {
        let mut stmt = conn.prepare("SELECT * FROM trades WHERE status='CLOSED' ORDER BY exit_time DESC LIMIT 50")?;
        let items: Vec<_> = stmt.query_map([], row_to_trade)?.filter_map(|r| {
            if let Err(e) = &r {
                tracing::error!("row_to_trade parse error: {:?}", e);
            }
            r.ok()
        }).collect();
        Ok(items)
    }).await.unwrap_or_else(|_| vec![]);

    let history_trades: Vec<serde_json::Value> = history_trades_raw
        .into_iter()
        .map(|t| {
            let mut v = serde_json::to_value(&t).unwrap_or_default();
            let display_comm = t.commission_cost.unwrap_or(0.0);
            let display_slip = t.slippage_cost.unwrap_or(0.0);

            if let Some(obj) = v.as_object_mut() {
                obj.insert("commission_cost".into(), json!(display_comm));
                // Entry slippage (IOC target vs fill)
                obj.insert("slippage_cost".into(), json!(display_slip));
                // Exit slippage (separate for display)
                obj.insert("exit_slippage_cost".into(), json!(t.exit_slippage_cost.unwrap_or(0.0)));
                let fund_cost = t.funding_cost.unwrap_or(0.0);
                obj.insert("funding_cost".into(), json!(fund_cost));
                // Gross PnL: actual fill-based PnL (slippage already baked in)
                let actual_gross = t.gross_pnl.unwrap_or_else(|| t.pnl_a.unwrap_or(0.0) + t.pnl_b.unwrap_or(0.0));
                let calculated_net_pnl = t.pnl.unwrap_or(actual_gross - display_comm - fund_cost);
                obj.insert("gross_pnl".into(), json!(actual_gross));
                obj.insert("pnl".into(), json!(calculated_net_pnl));
            }
            v
        })
        .collect();

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
            // Aggregate totals
            let mut stmt = conn.prepare("SELECT COUNT(*), SUM(pnl), SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END), SUM(exit_time - entry_time), COUNT(exit_time) FROM trades WHERE status='CLOSED' AND profile_id=?")?;
            struct Agg { count: i64, pnl: Option<f64>, wins: Option<i64>, total_duration: Option<i64>, closed_count: Option<i64> }
            let agg = stmt.query_row([p.clone()], |r| Ok(Agg {
                count: r.get(0)?,
                pnl: r.get(1)?,
                wins: r.get(2)?,
                total_duration: r.get(3)?,
                closed_count: r.get(4)?
            }))?;
            
            let total_trades = agg.count;
            let total_pnl = agg.pnl.unwrap_or(0.0);
            let wins = agg.wins.unwrap_or(0);
            let losses = total_trades - wins;
            let avg_pnl = if total_trades > 0 { total_pnl / total_trades as f64 } else { 0.0 };
            
            let avg_duration = if let (Some(td), Some(cc)) = (agg.total_duration, agg.closed_count) {
                if cc > 0 { td / cc } else { 0 }
            } else { 0 };

            tracing::debug!("Stats for [{}]: count={} pnl={:.2} wins={}", p, total_trades, total_pnl, wins);

            // Equity curve (last 1000)
            let mut stmt = conn.prepare("SELECT pnl, exchange_a, exchange_b FROM trades WHERE status='CLOSED' AND profile_id=? ORDER BY exit_time ASC LIMIT 1000")?;
            let equity_curve: Vec<serde_json::Value> = stmt.query_map([p.clone()], |r| {
                let pnl: f64 = r.get(0)?;
                let ex_a: String = r.get(1)?;
                let ex_b: String = r.get(2)?;
                Ok(json!({ "pnl": pnl, "ex_a": ex_a, "ex_b": ex_b }))
            })?.filter_map(|r| r.ok()).collect();

            // Top tokens by PnL
            let mut stmt = conn.prepare("SELECT pair, SUM(pnl) as tpnl FROM trades WHERE status='CLOSED' AND profile_id=? GROUP BY pair ORDER BY tpnl DESC LIMIT 10")?;
            let top_tokens: Vec<serde_json::Value> = stmt.query_map([p.clone()], |r| {
                Ok(json!({ "token": r.get::<_, String>(0)?, "pnl": r.get::<_, f64>(1)? }))
            })?.filter_map(|r| r.ok()).collect();

            // PnL by Exchange and Best Exchange Pairs
            let mut stmt = conn.prepare("SELECT exchange_a, exchange_b, SUM(pnl) FROM trades WHERE status='CLOSED' AND profile_id=? GROUP BY exchange_a, exchange_b")?;
            let rows: Vec<(String, String, f64)> = stmt.query_map([p.clone()], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.filter_map(|r| r.ok()).collect();
            
            let mut pnl_by_exchange: HashMap<String, f64> = HashMap::new();
            let mut exchange_pairs: HashMap<String, f64> = HashMap::new();
            
            for (ex_a, ex_b, pnl) in rows {
                *pnl_by_exchange.entry(ex_a.clone()).or_insert(0.0) += pnl / 2.0;
                *pnl_by_exchange.entry(ex_b.clone()).or_insert(0.0) += pnl / 2.0;

                let mut pair_arr = [ex_a, ex_b];
                pair_arr.sort();
                let pair_key = format!("{}-{}", pair_arr[0], pair_arr[1]);
                *exchange_pairs.entry(pair_key).or_insert(0.0) += pnl;
            }

            let mut top_exchange_pairs: Vec<serde_json::Value> = exchange_pairs.into_iter()
                .map(|(pair, pnl)| json!({ "pair": pair, "pnl": pnl }))
                .collect();
            top_exchange_pairs.sort_by(|a, b| {
                b.get("pnl").unwrap().as_f64().unwrap().total_cmp(&a.get("pnl").unwrap().as_f64().unwrap())
            });
            top_exchange_pairs.truncate(10);

            Ok(json!({
                "total_pnl": total_pnl,
                "total_trades": total_trades,
                "wins": wins,
                "losses": losses,
                "avg_pnl": avg_pnl,
                "avg_duration": avg_duration,
                "equity_curve": equity_curve,
                "pnl_by_exchange": pnl_by_exchange,
                "top_tokens": top_tokens,
                "top_exchange_pairs": top_exchange_pairs
            }))
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
    let kill_switch_active = state.db.call(|conn| {
        let val: Result<String, _> = conn.query_row(
            "SELECT value FROM server_cache WHERE key = 'kill_switch_active'",
            [],
            |r| r.get(0),
        );
        Ok::<bool, tokio_rusqlite::Error>(val.unwrap_or_else(|_| "0".to_string()) == "1")
    }).await.unwrap_or(false);

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

    let result = state.db.call(|conn| {
        conn.execute(
            "INSERT INTO server_cache(key,value,expires_at) VALUES('kill_switch_active','0',0) \
             ON CONFLICT(key) DO UPDATE SET value='0'",
            [],
        )?;
        Ok(())
    }).await;

    match result {
        Ok(_) => {
            tracing::info!("[api] Kill-switch reset requested by user");
            (StatusCode::OK, Json(json!({"ok": true, "message": "Kill-switch reset. Engine will resume within 2 seconds."}))).into_response()
        }
        Err(e) => json_err(StatusCode::INTERNAL_SERVER_ERROR, &format!("DB error: {}", e)),
    }
}

// ─── GET /api/profiles/:id ───
pub async fn handle_get_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let _username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };

    let (_maps_json, _) = state
        .real_db
        .call(move |conn| {
            let mut stmt =
                conn.prepare("SELECT value FROM server_cache WHERE key='exchange_maps'")?;
            let json_str: String = stmt.query_row([], |row| row.get(0))?;
            Ok::<(String, ()), tokio_rusqlite::Error>((json_str, ()))
        })
        .await
        .unwrap_or_else(|_| ("{}".to_string(), ()));

    let profile = state
        .db
        .call(move |conn| {
            let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE id = ?")?;
            let items: Vec<_> = stmt
                .query_map([id], row_to_profile)?
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
                "max_active_trades" | "collector_interval_secs" | "leverage" | "hft_timeout_ms" => {
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

    let _id2 = id.clone();
    let id3 = id.clone();
    let _ = state
        .db
        .call(move |conn| {
            conn.execute(
                "DELETE FROM trading_profiles WHERE id=? AND owner=?",
                params![id.clone(), username],
            )?;
            Ok(())
        })
        .await;
    // Clear trades from DB
    let _ = state
        .real_db
        .call(move |conn| {
            conn.execute("DELETE FROM trades WHERE profile_id=?", params![id3])?;
            Ok(())
        })
        .await;

    (StatusCode::OK, Json(json!({ "success": true }))).into_response()
}

// ─── DELETE /api/profiles/:id/trades ───
pub async fn handle_clear_trades(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if check_basic_auth(&headers, &state).is_none() {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    let _id2 = id.clone();
    let id3 = id.clone();
    let _ = state
        .db
        .call(move |conn| {
            conn.execute(
                "UPDATE trading_profiles SET pending_commands='CLEAR_TRADES' WHERE id=?",
                params![id],
            )?;
            Ok(())
        })
        .await;
    let _ = state
        .real_db
        .call(move |conn| {
            conn.execute("DELETE FROM trades WHERE profile_id=?", params![id3])?;
            Ok(())
        })
        .await;

    (StatusCode::OK, Json(json!({ "success": true }))).into_response()
}

// ─── POST /api/profiles/:id/market-close-all ───
pub async fn handle_market_close_all(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(_id): Path<String>,
) -> Response {
    if check_basic_auth(&headers, &state).is_none() {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    let r1 = state.real_db.call(move |conn| {
        conn.execute("UPDATE trades SET close_reason='MANUAL_CLOSE' WHERE status IN ('OPEN','CLOSING')", params![])?;
        Ok(())
    }).await;

    if let Err(e) = r1 {
        tracing::error!("Failed to close all on real DB: {:?}", e);
    }

    (StatusCode::OK, Json(json!({ "success": true }))).into_response()
}

// ─── POST /api/trades/:id/close ───
pub async fn handle_market_close_trade(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(trade_id): Path<i64>,
) -> Response {
    if check_basic_auth(&headers, &state).is_none() {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    let r1 = state
        .real_db
        .call(move |conn| {
            conn.execute(
                "UPDATE trades SET close_reason='MANUAL_CLOSE' WHERE id=? AND status IN ('OPEN','CLOSING')",
                params![trade_id],
            )?;
            Ok(())
        })
        .await;

    if let Err(e) = r1 {
        tracing::error!("Failed to close trade {} on real DB: {:?}", trade_id, e);
    }

    (StatusCode::OK, Json(json!({ "success": true }))).into_response()
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
    axum::extract::State(_state): axum::extract::State<crate::AppState>,
    axum::Json(payload): axum::Json<PositionCloseReq>,
) -> impl axum::response::IntoResponse {
    let client = reqwest::Client::new();
    let res = client
        .post("http://127.0.0.1:3003/api/positions/close")
        .json(&payload)
        .send()
        .await;

    match res {
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status.is_success() {
                (axum::http::StatusCode::OK, axum::Json(serde_json::json!({ "success": true, "message": body }))).into_response()
            } else {
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, axum::Json(serde_json::json!({ "error": body }))).into_response()
            }
        }
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({ "error": format!("core-node unreachable: {}", e) })),
        ).into_response(),
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
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let mut url = format!(
        "https://fapi.binance.com/fapi/v1/klines?symbol={}&interval={}&limit={}",
        pair, interval, limit
    );
    if let Some(et) = end_time { url.push_str(&format!("&endTime={}", et)); }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Vec<Vec<Value>> = resp.json().await.map_err(|e| e.to_string())?;
    Ok(data.iter().filter_map(|c| {
        Some(Kline {
            ts: c.get(0)?.as_i64()?,
            open: c.get(1)?.as_str()?.parse().ok()?,
            high: c.get(2)?.as_str()?.parse().ok()?,
            low: c.get(3)?.as_str()?.parse().ok()?,
            close: c.get(4)?.as_str()?.parse().ok()?,
            volume: c.get(5)?.as_str()?.parse().ok()?,
        })
    }).collect())
}

async fn fetch_bybit_klines(
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let bybit_iv = match interval {
        "1m" => "1", "3m" => "3", "5m" => "5", "15m" => "15", "1h" => "60",
        _ => "1",
    };
    let mut url = format!(
        "https://api.bybit.com/v5/market/kline?category=linear&symbol={}&interval={}&limit={}",
        pair, bybit_iv, limit
    );
    if let Some(et) = end_time { url.push_str(&format!("&end={}", et)); }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["result"]["list"].as_array().cloned().unwrap_or_default();
    let mut klines: Vec<Kline> = list.iter().filter_map(|c| {
        let arr = c.as_array()?;
        Some(Kline {
            ts: arr.get(0)?.as_str()?.parse().ok()?,
            open: arr.get(1)?.as_str()?.parse().ok()?,
            high: arr.get(2)?.as_str()?.parse().ok()?,
            low: arr.get(3)?.as_str()?.parse().ok()?,
            close: arr.get(4)?.as_str()?.parse().ok()?,
            volume: arr.get(5)?.as_str()?.parse().ok()?,
        })
    }).collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

async fn fetch_gate_klines(
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    // Gate uses underscore format: BTC_USDT
    let gate_pair = if pair.ends_with("USDT") {
        format!("{}_USDT", &pair[..pair.len()-4])
    } else {
        pair.to_string()
    };
    let gate_iv = match interval {
        "1m" => "1m", "3m" => "3m", "5m" => "5m", "15m" => "15m", "1h" => "1h",
        _ => "1m",
    };
    let mut url = format!(
        "https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={}&interval={}&limit={}",
        gate_pair, gate_iv, limit
    );
    if let Some(et) = end_time { url.push_str(&format!("&to={}", et / 1000)); }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Vec<Value> = resp.json().await.map_err(|e| e.to_string())?;
    Ok(data.iter().filter_map(|c| {
        Some(Kline {
            ts: c["t"].as_f64().or_else(|| c["t"].as_i64().map(|v| v as f64))? as i64 * 1000,
            open: c["o"].as_str()?.parse().ok()?,
            high: c["h"].as_str()?.parse().ok()?,
            low: c["l"].as_str()?.parse().ok()?,
            close: c["c"].as_str()?.parse().ok()?,
            volume: c["v"].as_f64().or_else(|| c["v"].as_i64().map(|v| v as f64)).unwrap_or(0.0),
        })
    }).collect())
}

async fn fetch_kucoin_klines(
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let kc_pair = if pair.ends_with("USDT") {
        format!("{}USDTM", &pair[..pair.len()-4])
    } else {
        pair.to_string()
    };
    let kc_iv = match interval {
        "1m" => "1", "3m" => "3", "5m" => "5", "15m" => "15", "1h" => "60",
        _ => "1",
    };
    let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let granularity_min: i64 = kc_iv.parse().unwrap_or(1);
    let interval_ms = granularity_min * 60 * 1000;

    // KuCoin API returns max 200 per request — paginate to reach `limit`
    let mut all_klines: Vec<Kline> = Vec::new();
    let mut cursor_end_ms = end_time.unwrap_or(now_s * 1000);
    let mut remaining = limit;
    let max_pages = 3; // max 3 pages = 600 candles

    for _ in 0..max_pages {
        if remaining <= 0 { break; }
        let batch = remaining.min(200);
        let cursor_start_ms = cursor_end_ms - (batch as i64 * interval_ms);
        let url = format!(
            "https://api-futures.kucoin.com/api/v1/kline/query?symbol={}&granularity={}&from={}&to={}",
            kc_pair, granularity_min, cursor_start_ms, cursor_end_ms
        );
        let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
        let data: Value = resp.json().await.map_err(|e| e.to_string())?;
        let list = data["data"].as_array().cloned().unwrap_or_default();
        if list.is_empty() { break; }

        let batch_klines: Vec<Kline> = list.iter().filter_map(|c| {
            let arr = c.as_array()?;
            Some(Kline {
                ts: arr.get(0)?.as_f64().or_else(|| arr.get(0)?.as_i64().map(|v| v as f64))? as i64,
                open: arr.get(1)?.as_f64()?,
                high: arr.get(2)?.as_f64()?,
                low: arr.get(3)?.as_f64()?,
                close: arr.get(4)?.as_f64()?,
                volume: arr.get(5)?.as_f64().unwrap_or(0.0),
            })
        }).collect();

        remaining -= batch_klines.len() as i32;
        cursor_end_ms = cursor_start_ms; // next page ends where this one started
        all_klines.extend(batch_klines);
    }

    all_klines.sort_by_key(|k| k.ts);
    all_klines.dedup_by_key(|k| k.ts);
    Ok(all_klines)
}

async fn fetch_okx_klines(
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    // OKX uses format: BTC-USDT-SWAP
    let okx_pair = if pair.ends_with("USDT") {
        format!("{}-USDT-SWAP", &pair[..pair.len()-4])
    } else {
        pair.to_string()
    };
    let mut url = format!(
        "https://www.okx.com/api/v5/market/candles?instId={}&bar={}&limit={}",
        okx_pair, interval, limit.min(300)
    );
    if let Some(et) = end_time { url.push_str(&format!("&after={}", et)); }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["data"].as_array().cloned().unwrap_or_default();
    let mut klines: Vec<Kline> = list.iter().filter_map(|c| {
        let arr = c.as_array()?;
        Some(Kline {
            ts: arr.get(0)?.as_str()?.parse().ok()?,
            open: arr.get(1)?.as_str()?.parse().ok()?,
            high: arr.get(2)?.as_str()?.parse().ok()?,
            low: arr.get(3)?.as_str()?.parse().ok()?,
            close: arr.get(4)?.as_str()?.parse().ok()?,
            volume: arr.get(5)?.as_str()?.parse().unwrap_or(0.0),
        })
    }).collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

async fn fetch_htx_klines(
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, _end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let htx_pair = pair.to_lowercase();
    let htx_iv = match interval {
        "1m" => "1min", "3m" => "3min", "5m" => "5min", "15m" => "15min", "1h" => "60min",
        _ => "1min",
    };
    let url = format!(
        "https://api.hbdm.com/linear-swap-ex/market/history/kline?contract_code={}&period={}&size={}",
        htx_pair, htx_iv, limit.min(2000)
    );
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["data"].as_array().cloned().unwrap_or_default();
    Ok(list.iter().filter_map(|c| {
        Some(Kline {
            ts: c["id"].as_i64()? * 1000,
            open: c["open"].as_f64()?,
            high: c["high"].as_f64()?,
            low: c["low"].as_f64()?,
            close: c["close"].as_f64()?,
            volume: c["vol"].as_f64().unwrap_or(0.0),
        })
    }).collect())
}

async fn fetch_bingx_klines(
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    // BingX uses format: BTC-USDT
    let bingx_pair = if pair.ends_with("USDT") {
        format!("{}-USDT", &pair[..pair.len()-4])
    } else {
        pair.to_string()
    };
    let bingx_iv = match interval {
        "1m" => "1m", "3m" => "3m", "5m" => "5m", "15m" => "15m", "1h" => "1h",
        _ => "1m",
    };
    let mut url = format!(
        "https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol={}&interval={}&limit={}",
        bingx_pair, bingx_iv, limit.min(1440)
    );
    if let Some(et) = end_time { url.push_str(&format!("&endTime={}", et)); }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data.as_array().cloned().unwrap_or_default();
    Ok(list.iter().filter_map(|c| {
        Some(Kline {
            ts: c["time"].as_i64()?,
            open: c["open"].as_str()?.parse().ok()?,
            high: c["high"].as_str()?.parse().ok()?,
            low: c["low"].as_str()?.parse().ok()?,
            close: c["close"].as_str()?.parse().ok()?,
            volume: c["volume"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
        })
    }).collect())
}

async fn fetch_xt_klines(
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    // XT uses lowercase with underscore: btc_usdt
    let xt_pair = if pair.ends_with("USDT") {
        format!("{}_usdt", pair[..pair.len()-4].to_lowercase())
    } else {
        pair.to_lowercase()
    };
    let xt_iv = match interval {
        "1m" => "1m", "3m" => "3m", "5m" => "5m", "15m" => "15m", "1h" => "1h",
        _ => "1m",
    };
    let mut url = format!(
        "https://fapi.xt.com/future/market/v1/public/q/kline?symbol={}&interval={}&limit={}",
        xt_pair, xt_iv, limit.min(200)
    );
    if let Some(et) = end_time { url.push_str(&format!("&endTime={}", et)); }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["result"].as_array().cloned().unwrap_or_default();
    Ok(list.iter().filter_map(|c| {
        Some(Kline {
            ts: c["t"].as_i64()?,
            open: c["o"].as_str().and_then(|s| s.parse().ok()).or_else(|| c["o"].as_f64())?,
            high: c["h"].as_str().and_then(|s| s.parse().ok()).or_else(|| c["h"].as_f64())?,
            low: c["l"].as_str().and_then(|s| s.parse().ok()).or_else(|| c["l"].as_f64())?,
            close: c["c"].as_str().and_then(|s| s.parse().ok()).or_else(|| c["c"].as_f64())?,
            volume: c["v"].as_str().and_then(|s| s.parse().ok()).or_else(|| c["v"].as_f64()).unwrap_or(0.0),
        })
    }).collect())
}

async fn fetch_bitget_klines(
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    // Bitget uses format: BTCUSDT (same as input)
    let bg_iv = match interval {
        "1m" => "1m", "3m" => "3m", "5m" => "5m", "15m" => "15m", "1h" => "1H",
        _ => "1m",
    };
    let mut url = format!(
        "https://api.bitget.com/api/v2/mix/market/candles?productType=USDT-FUTURES&symbol={}&granularity={}&limit={}",
        pair, bg_iv, limit.min(1000)
    );
    if let Some(et) = end_time { url.push_str(&format!("&endTime={}", et)); }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["data"].as_array().cloned().unwrap_or_default();
    let mut klines: Vec<Kline> = list.iter().filter_map(|c| {
        let arr = c.as_array()?;
        Some(Kline {
            ts: arr.get(0)?.as_str()?.parse().ok()?,
            open: arr.get(1)?.as_str()?.parse().ok()?,
            high: arr.get(2)?.as_str()?.parse().ok()?,
            low: arr.get(3)?.as_str()?.parse().ok()?,
            close: arr.get(4)?.as_str()?.parse().ok()?,
            volume: arr.get(5)?.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
        })
    }).collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

async fn fetch_toobit_klines(
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let mut url = format!(
        "https://api.toobit.com/api/v1/klines?symbol={}&interval={}&limit={}",
        pair, interval, limit.min(1000)
    );
    if let Some(et) = end_time { url.push_str(&format!("&endTime={}", et)); }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data.as_array().cloned().unwrap_or_default();
    let mut klines: Vec<Kline> = list.iter().filter_map(|c| {
        let arr = c.as_array()?;
        Some(Kline {
            ts: arr.get(0)?.as_i64()?,
            open: arr.get(1)?.as_str()?.parse().ok()?,
            high: arr.get(2)?.as_str()?.parse().ok()?,
            low: arr.get(3)?.as_str()?.parse().ok()?,
            close: arr.get(4)?.as_str()?.parse().ok()?,
            volume: arr.get(5)?.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
        })
    }).collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

async fn fetch_ascendex_klines(
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let asc_pair = if pair.ends_with("USDT") {
        format!("{}-PERP", &pair[..pair.len()-4])
    } else {
        pair.to_string()
    };
    let asc_iv = match interval {
        "1m" => "1", "3m" => "3", "5m" => "5", "15m" => "15", "1h" => "60",
        _ => "1",
    };
    let mut url = format!(
        "https://ascendex.com/api/pro/v1/futures/barhist?symbol={}&interval={}&n={}",
        asc_pair, asc_iv, limit.min(500)
    );
    if let Some(et) = end_time { url.push_str(&format!("&to={}", et)); }
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    let data: Value = resp.json().await.map_err(|e| e.to_string())?;
    let list = data["data"].as_array().cloned().unwrap_or_default();
    let mut klines: Vec<Kline> = list.iter().filter_map(|c| {
        let item = &c["data"];
        Some(Kline {
            ts: item["ts"].as_i64()?,
            open: item["o"].as_str()?.parse().ok()?,
            high: item["h"].as_str()?.parse().ok()?,
            low: item["l"].as_str()?.parse().ok()?,
            close: item["c"].as_str()?.parse().ok()?,
            volume: item["v"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0),
        })
    }).collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

async fn fetch_bitmex_klines(
    client: &reqwest::Client, pair: &str, interval: &str, limit: i32, end_time: Option<i64>,
) -> Result<Vec<Kline>, String> {
    let bm_pair = if pair == "BTCUSDT" { "XBTUSDT" } else { pair };
    let bm_iv = match interval {
        "1m" => "1m", "5m" => "5m", "1h" => "1h", "1d" => "1d",
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
    let mut klines: Vec<Kline> = list.iter().filter_map(|c| {
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
    }).collect();
    klines.sort_by_key(|k| k.ts);
    Ok(klines)
}

