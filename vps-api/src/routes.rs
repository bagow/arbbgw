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
use std::time::{SystemTime, UNIX_EPOCH, Instant};

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
    let _ = state.db.call(move |conn| {
        Ok(conn.execute(
            "INSERT INTO server_cache (key, value, expires_at) VALUES ('snapshot', ?, ?)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
            params![snapshot_val, now + 180_000],
        )?)
    }).await;

    // Save maps summary
    let mut map_summaries: HashMap<String, Vec<String>> = HashMap::new();
    for (ex, data) in &payload.maps {
        let keys = if let Some(obj) = data.as_object() {
            obj.keys().cloned().collect()
        } else if let Some(arr) = data.as_array() {
            arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect()
        } else {
            vec![]
        };
        map_summaries.insert(ex.clone(), keys);
    }
    let map_val = json!({ "ts": now, "data": map_summaries }).to_string();
    let _ = state.db.call(move |conn| {
        Ok(conn.execute(
            "INSERT INTO server_cache (key, value, expires_at) VALUES ('exchange_maps', ?, ?)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
            params![map_val, now + 180_000],
        )?)
    }).await;

    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

// ─── GET /api/trading/settings/all ───
pub async fn handle_get_all_settings(State(state): State<AppState>) -> Response {
    let profiles = match state.db.call(|conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE mode = 'real'")?;
        let items: Vec<_> = stmt.query_map([], row_to_profile)?.filter_map(|r| r.ok()).collect();
        Ok(items)
    }).await {
        Ok(p) => p,
        Err(_) => vec![],
    };
    (StatusCode::OK, Json(profiles)).into_response()
}

// ─── GET /api/logs/core-node ───
pub async fn handle_get_logs(State(_state): State<AppState>, headers: HeaderMap) -> Response {
    if check_basic_auth(&headers, &_state).is_none() {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error":"Unauthorized"}))).into_response();
    }
    match tokio::process::Command::new("sudo")
        .args(["journalctl", "-u", "core-node", "-n", "3000", "-r", "--no-pager"])
        .output().await 
    {
        Ok(out) => {
            let mut s = String::from_utf8_lossy(&out.stdout).to_string();
            if s.trim().is_empty() {
                s = String::from_utf8_lossy(&out.stderr).to_string();
            }
            (StatusCode::OK, s).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
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
    let snapshot = state.real_db.call(|conn| {
        let row: Result<String, rusqlite::Error> = conn.query_row("SELECT value FROM server_cache WHERE key = 'snapshot'", [], |r| r.get(0));
        let val: serde_json::Value = if let Ok(v) = row { serde_json::from_str(&v).unwrap_or(json!({})) } else { json!({}) };
        Ok::<serde_json::Value, tokio_rusqlite::Error>(val)
    }).await.unwrap_or(json!({}));

    let exchanges_raw = state.real_db.call(|conn| {
        let row: Result<String, rusqlite::Error> = conn.query_row("SELECT value FROM server_cache WHERE key = 'exchange_maps'", [], |r| r.get(0));
        let val: serde_json::Value = if let Ok(v) = row { serde_json::from_str(&v).unwrap_or(json!({})) } else { json!({}) };
        Ok::<serde_json::Value, tokio_rusqlite::Error>(val)
    }).await.unwrap_or(json!({}));

    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
    let today_midnight_ms = now_ms - (now_ms % 86_400_000); // Rounds down to midnight UTC
    let daily_exch_stats = state.real_db.call(move |conn| {
        let mut map: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        let mut stmt = match conn.prepare("SELECT exchange_a, exchange_b, pnl_a, pnl_b, commission_cost, funding_cost, slippage_cost FROM trades WHERE status='CLOSED' AND exit_time >= ?") {
            Ok(s) => s,
            Err(_) => return Ok::<_, tokio_rusqlite::Error>(map)
        };
        let rows: Vec<_> = stmt.query_map([today_midnight_ms], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<f64>>(2)?.unwrap_or(0.0),
                r.get::<_, Option<f64>>(3)?.unwrap_or(0.0),
                r.get::<_, Option<f64>>(4)?.unwrap_or(0.0),
                r.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
                r.get::<_, Option<f64>>(6)?.unwrap_or(0.0),
            ))
        }).map(|iter| iter.filter_map(|r| r.ok()).collect::<Vec<_>>()).unwrap_or_default();
        
        for (exa, exb, pnla, pnlb, comm, fund, slip) in rows {
            let shared_costs = (comm + fund + slip) / 2.0;
            *map.entry(exa).or_insert(0.0) += pnla - shared_costs;
            *map.entry(exb).or_insert(0.0) += pnlb - shared_costs;
        }
        Ok::<_, tokio_rusqlite::Error>(map)
    }).await.unwrap_or_default();

    let mut exchanges = exchanges_raw.clone();
    if let Some(obj) = exchanges.as_object_mut() {
        let mut daily_pnl_map = serde_json::Map::new();
        let mut daily_pct_map = serde_json::Map::new();
        let balances = obj.get("balances").and_then(|b| b.as_object());
        
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
    }

    // 2. Get Profiles
    let profiles = state.db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE owner = ? OR id = 'real-default' ORDER BY created_at ASC")?;
        let items: Vec<_> = stmt.query_map([username], row_to_profile)?.filter_map(|r| r.ok()).collect();
        Ok(items)
    }).await.unwrap_or_else(|_| vec![]);

    let pid = query.profile_id.unwrap_or_else(|| "real-default".to_string());
    let _profile_mode = profiles.iter()
        .find(|p| p.id == pid)
        .map(|p| p.mode.clone())
        .unwrap_or_else(|| "real".into());
    let trade_db = state.real_db.clone();

    let active_trades = trade_db.call({
        let p = pid.clone();
        move |conn| {
            let mut stmt = conn.prepare("SELECT * FROM trades WHERE status='OPEN' AND profile_id=?")?;
            let items: Vec<_> = stmt.query_map([p], row_to_trade)?.filter_map(|r| r.ok()).collect();
            Ok(items)
        }
    }).await.unwrap_or_else(|_| vec![]);

    // Enrich active trades with live data from DB columns written by core-node.
    // STRICT RULE: We trust ALL values from DB — core-node writes them from exchange API.
    // NO local PnL recalculation — dashboard is a pure DB mirror.
    let active_enriched: Vec<serde_json::Value> = active_trades.iter().map(|t| {
        let mut v = serde_json::to_value(t).unwrap_or_default();
        if let Some(obj) = v.as_object_mut() {
            // Current prices from DB (written by core-node's live_update loop)
            obj.insert("current_price_a".into(), json!(t.last_price_a.unwrap_or(t.entry_price_a)));
            obj.insert("current_price_b".into(), json!(t.last_price_b.unwrap_or(t.entry_price_b)));
            obj.insert("current_spread".into(), json!(t.last_spread.unwrap_or(0.0)));
            // PnL: trust DB values computed by core-node from exchange API
            let db_pnl_a = t.pnl_a.unwrap_or(0.0);
            let db_pnl_b = t.pnl_b.unwrap_or(0.0);
            let gross = db_pnl_a + db_pnl_b;
            obj.insert("gross_pnl".into(), json!(gross));
            obj.insert("pnl_a".into(), json!(db_pnl_a));
            obj.insert("pnl_b".into(), json!(db_pnl_b));
            // Net PnL: trust curr_gross_pnl from DB (= gross - commission - funding)
            obj.insert("current_pnl".into(), json!(t.curr_gross_pnl.unwrap_or(gross)));
            // Costs: directly from DB
            obj.insert("commission_cost".into(), json!(t.commission_cost.unwrap_or(0.0)));
            obj.insert("slippage_cost".into(), json!(t.slippage_cost.unwrap_or(0.0)));
            obj.insert("funding_cost".into(), json!(t.funding_cost.unwrap_or(0.0)));
        }
        v
    }).collect();

    let history_trades_raw = trade_db.call({
        let p = pid.clone();
        move |conn| {
            let mut stmt = conn.prepare("SELECT * FROM trades WHERE status='CLOSED' AND profile_id=? ORDER BY exit_time DESC LIMIT 50")?;
            let items: Vec<_> = stmt.query_map([p], row_to_trade)?.filter_map(|r| {
                if let Err(e) = &r {
                    tracing::error!("row_to_trade parse error: {:?}", e);
                }
                r.ok()
            }).collect();
            Ok(items)
        }
    }).await.unwrap_or_else(|_| vec![]);

    let history_trades: Vec<serde_json::Value> = history_trades_raw.into_iter().map(|t| {
        let mut v = serde_json::to_value(&t).unwrap_or_default();
        let display_comm = t.commission_cost.unwrap_or(0.0);
        let display_slip = t.slippage_cost.unwrap_or(0.0);
        
        if let Some(obj) = v.as_object_mut() {
            obj.insert("commission_cost".into(), json!(display_comm));
            obj.insert("slippage_cost".into(), json!(display_slip));
            if t.funding_cost.is_none() { obj.insert("funding_cost".into(), json!(0.0)); }
        }
        v
    }).collect();

    // 4. Compute aggregate stats — cached for 30s to avoid 131K+ row scans every poll
    static STATS_CACHE: OnceLock<Mutex<HashMap<String, (Instant, Value)>>> = OnceLock::new();
    let cache = STATS_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let cache_key = pid.clone();
    let cached = {
        let lock = cache.lock().unwrap();
        lock.get(&cache_key).and_then(|(ts, v)| {
            if ts.elapsed().as_secs() < 30 { Some(v.clone()) } else { None }
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
        cache.lock().unwrap().insert(cache_key, (Instant::now(), fresh.clone()));
        fresh
    };

    // Return payload matching frontend expectations
    (StatusCode::OK, Json(json!({
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
    }))).into_response()
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

    let (_maps_json, _) = state.real_db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT value FROM server_cache WHERE key='exchange_maps'")?;
        let json_str: String = stmt.query_row([], |row| row.get(0))?;
        Ok::<(String, ()), tokio_rusqlite::Error>((json_str, ()))
    }).await.unwrap_or_else(|_| ("{}".to_string(), ()));

    let profile = state.db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE id = ?")?;
        let items: Vec<_> = stmt.query_map([id], row_to_profile)?.filter_map(|r| r.ok()).collect();
        Ok(items.into_iter().next())
    }).await.unwrap_or(None);

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

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let new_id = uuid::Uuid::new_v4().to_string();

    // Extract only what the frontend sends — fill everything else with sensible defaults.
    let name  = payload.get("name" ).and_then(|v| v.as_str()).unwrap_or("New Profile").to_string();
    let mode  = payload.get("mode" ).and_then(|v| v.as_str()).unwrap_or("real").to_string();

    let (default_vol, default_tp, default_stop, default_max_trades) = if mode == "real" {
        (100.0_f64, 0.2_f64, 0.3_f64, 3_i64)
    } else {
        (10.0_f64, 0.2_f64, 0.3_f64, 3_i64)
    };

    let id_clone = new_id.clone();
    let un_clone = username.clone();
    let res = state.db.call(move |conn| {
        Ok(conn.execute(
            "INSERT INTO trading_profiles
                (id,name,mode,is_active,min_entry_spread,max_entry_spread,trade_volume,
                 take_profit_percent,trailing_stop_percent,max_active_trades,commission_rate,
                 slippage_percent,funding_rate_8h,min_volume_millions,collector_interval_secs,
                 owner,created_at,updated_at,disabled_exchanges)
            VALUES (?,?,?,0,1.0,10.0,?,?,?,?,0.05,0.05,0.01,10.0,5,?,?,?,'[]')",
            params![
                id_clone, name, mode, default_vol,
                default_tp, default_stop, default_max_trades,
                un_clone, now, now
            ],
        )?)
    }).await;

    if let Err(e) = res {
        return json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }

    // Return the freshly-inserted row from DB so the frontend gets accurate data.
    let profile = state.db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE id=?")?;
        Ok(stmt.query_row([new_id], row_to_profile).ok())
    }).await.unwrap_or(None);

    match profile {
        Some(p) => (StatusCode::CREATED, Json(serde_json::to_value(p).unwrap_or_default())).into_response(),
        None    => json_err(StatusCode::INTERNAL_SERVER_ERROR, "Profile created but could not be fetched"),
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
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    
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
                },
                "min_entry_spread" | "max_entry_spread" | "trade_volume" | "take_profit_percent" | 
                "trailing_stop_percent" | "commission_rate" | "slippage_percent" | "funding_rate_8h" | "min_volume_millions" => {
                    if let Some(f) = v.as_f64() {
                        sets.push(format!("{}=?", k));
                        params_vec.push(rusqlite::types::Value::Real(f));
                    }
                },
                "max_active_trades" | "collector_interval_secs" | "leverage" => {
                    if let Some(i) = v.as_i64() {
                        sets.push(format!("{}=?", k));
                        params_vec.push(rusqlite::types::Value::Integer(i));
                    }
                },
                "name" | "mode" | "pending_commands" | "margin_mode" => {
                    if let Some(s) = v.as_str() {
                        sets.push(format!("{}=?", k));
                        params_vec.push(rusqlite::types::Value::Text(s.to_string()));
                    }
                },
                "disabled_exchanges" => {
                    let s = if v.is_array() { serde_json::to_string(v).unwrap_or("[]".into()) } else { v.as_str().unwrap_or("[]").to_string() };
                    sets.push(format!("{}=?", k));
                    params_vec.push(rusqlite::types::Value::Text(s));
                },
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

        if let Err(e) = state.db.call(move |conn| {
            let mut stmt = conn.prepare(&query)?;
            stmt.execute(rusqlite::params_from_iter(params_vec))?;
            Ok(())
        }).await {
            tracing::error!("Failed to update profile {}: {:?}", id, e);
            return json_err(StatusCode::INTERNAL_SERVER_ERROR, "Failed to update profile in database");
        }
    }

    // Return updated profile
    let profile = state.db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE id=?")?;
        Ok(stmt.query_row([id], row_to_profile).ok())
    }).await.unwrap_or(None);

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

    let _id2 = id.clone(); let id3 = id.clone();
    let _ = state.db.call(move |conn| {
        conn.execute("DELETE FROM trading_profiles WHERE id=? AND owner=?", params![id.clone(), username])?;
        Ok(())
    }).await;
    // Clear trades from DB
    let _ = state.real_db.call(move |conn| {
        conn.execute("DELETE FROM trades WHERE profile_id=?", params![id3])?;
        Ok(())
    }).await;

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

    let _id2 = id.clone(); let id3 = id.clone();
    let _ = state.db.call(move |conn| {
        conn.execute("UPDATE trading_profiles SET pending_commands='CLEAR_TRADES' WHERE id=?", params![id])?;
        Ok(())
    }).await;
    let _ = state.real_db.call(move |conn| {
        conn.execute("DELETE FROM trades WHERE profile_id=?", params![id3])?;
        Ok(())
    }).await;

    (StatusCode::OK, Json(json!({ "success": true }))).into_response()
}

// ─── POST /api/profiles/:id/market-close-all ───
pub async fn handle_market_close_all(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if check_basic_auth(&headers, &state).is_none() {
        return json_err(StatusCode::UNAUTHORIZED, "Unauthorized");
    }

    let _id2 = id.clone();
    let r1 = state.real_db.call(move |conn| {
        conn.execute("UPDATE trades SET close_reason='MANUAL_CLOSE' WHERE profile_id=? AND status='OPEN'", params![id])?;
        Ok(())
    }).await;
    
    if let Err(e) = r1 { tracing::error!("Failed to close all on real DB: {:?}", e); }

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

    let r1 = state.real_db.call(move |conn| {
        conn.execute("UPDATE trades SET close_reason='MANUAL_CLOSE' WHERE id=? AND status='OPEN'", params![trade_id])?;
        Ok(())
    }).await;
    
    if let Err(e) = r1 { tracing::error!("Failed to close trade {} on real DB: {:?}", trade_id, e); }

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
        None => return json_err(StatusCode::BAD_REQUEST, "Expected JSON object { key: value, ... }"),
    };

    let result = state.db.call(move |conn| {
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
    }).await;

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
    let res = client.post("http://127.0.0.1:3003/api/positions/close")
        .json(&payload)
        .send()
        .await;
        
    match res {
        Ok(resp) => {
            if resp.status().is_success() {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        },
        Err(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
    }
}
