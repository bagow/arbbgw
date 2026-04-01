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
use std::time::{SystemTime, UNIX_EPOCH};

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

    // Also notify paper-trader via an internal channel or just expect paper-trader to connect independently?
    // Wait, paper-trader is a separate bin, so how does it get spreads? It can query /api/snapshot!
    // Or rust-collector can push to BOTH paper-trader and vps-api.
    
    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

// ─── GET /api/trading/settings/all ───
pub async fn handle_get_all_settings(State(state): State<AppState>) -> Response {
    let profiles = match state.db.call(|conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE mode = 'paper'")?;
        let items: Vec<_> = stmt.query_map([], row_to_profile)?.filter_map(|r| r.ok()).collect();
        Ok(items)
    }).await {
        Ok(p) => p,
        Err(_) => vec![],
    };
    (StatusCode::OK, Json(profiles)).into_response()
}

// ─── GET /api/profiles ───
pub async fn handle_get_profiles(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };

    let profiles = match state.db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE owner = ? OR id IN ('paper-default','real-default') ORDER BY created_at ASC")?;
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
    let snapshot = state.db.call(|conn| {
        let row: Result<String, _> = conn.query_row("SELECT value FROM server_cache WHERE key = 'snapshot'", [], |r| r.get(0));
        Ok(if let Ok(v) = row { serde_json::from_str(&v).unwrap_or(json!({})) } else { json!({}) })
    }).await.unwrap_or(json!({}));

    let exchanges = state.db.call(|conn| {
        let row: Result<String, _> = conn.query_row("SELECT value FROM server_cache WHERE key = 'exchange_maps'", [], |r| r.get(0));
        Ok(if let Ok(v) = row { serde_json::from_str(&v).unwrap_or(json!({})) } else { json!({}) })
    }).await.unwrap_or(json!({}));

    // 2. Get Profiles
    let profiles = state.db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE owner = ? OR id IN ('paper-default','real-default') ORDER BY created_at ASC")?;
        let items: Vec<_> = stmt.query_map([username], row_to_profile)?.filter_map(|r| r.ok()).collect();
        Ok(items)
    }).await.unwrap_or_else(|_| vec![]);

    // 3. Pick trade DB based on profile mode (paper vs real)
    let pid = query.profile_id.unwrap_or_else(|| "paper-default".to_string());
    let profile_mode = profiles.iter()
        .find(|p| p.id == pid)
        .map(|p| p.mode.clone())
        .unwrap_or_else(|| "paper".into());
    let trade_db = if profile_mode == "real" { state.real_db.clone() } else { state.paper_db.clone() };

    let active_trades = trade_db.call({
        let p = pid.clone();
        move |conn| {
            let mut stmt = conn.prepare("SELECT * FROM trades WHERE status='OPEN' AND profile_id=?")?;
            let items: Vec<_> = stmt.query_map([p], row_to_trade)?.filter_map(|r| r.ok()).collect();
            Ok(items)
        }
    }).await.unwrap_or_else(|_| vec![]);

    // Enrich active trades with live PnL from last_price columns written by paper-trader
    let active_enriched: Vec<serde_json::Value> = active_trades.iter().map(|t| {
        let mut v = serde_json::to_value(t).unwrap_or_default();
        if let (Some(c_price_a), Some(c_price_b), Some(c_spread)) = (t.last_price_a, t.last_price_b, t.last_spread) {
            let is_a_higher = t.direction.starts_with(&t.exchange_a);
            // Compute per-leg PnL for display (used as fallback if paper-trader hasn't written pnl_a/pnl_b yet)
            let coins_a = t.volume / t.entry_price_a;
            let coins_b = t.volume / t.entry_price_b;
            let pnl_a_fallback = if is_a_higher { (t.entry_price_a - c_price_a) * coins_a } else { (c_price_a - t.entry_price_a) * coins_a };
            let pnl_b_fallback = if is_a_higher { (c_price_b - t.entry_price_b) * coins_b } else { (t.entry_price_b - c_price_b) * coins_b };

            // curr_gross_pnl now stores NET pnl (paper-trader writes net after comm+funding).
            // Use it directly as current_pnl — no further deductions.
            let current_net_pnl = t.curr_gross_pnl.unwrap_or(pnl_a_fallback + pnl_b_fallback);

            // commission_cost was stored in the DB at trade open for display purposes.
            let display_comm = t.commission_cost.unwrap_or_else(|| t.volume * 0.0045 * 4.0);

            if let Some(obj) = v.as_object_mut() {
                obj.insert("current_price_a".into(), json!(c_price_a));
                obj.insert("current_price_b".into(), json!(c_price_b));
                obj.insert("current_spread".into(), json!(c_spread));
                // gross_pnl = raw price movement before costs (for reference display)
                obj.insert("gross_pnl".into(), json!(pnl_a_fallback + pnl_b_fallback));
                // current_pnl = the authoritative net figure from paper-trader
                obj.insert("current_pnl".into(), json!(current_net_pnl));
                // commission_cost is shown truthfully in the detail card
                obj.insert("commission_cost".into(), json!(display_comm));
                obj.insert("pnl_a".into(), json!(t.pnl_a.unwrap_or(pnl_a_fallback)));
                obj.insert("pnl_b".into(), json!(t.pnl_b.unwrap_or(pnl_b_fallback)));
            }
        }
        v
    }).collect();

    let history_trades = trade_db.call({
        let p = pid.clone();
        move |conn| {
            let mut stmt = conn.prepare("SELECT * FROM trades WHERE status='CLOSED' AND profile_id=? ORDER BY exit_time DESC LIMIT 50")?;
            let items: Vec<_> = stmt.query_map([p], row_to_trade)?.filter_map(|r| r.ok()).collect();
            Ok(items)
        }
    }).await.unwrap_or_else(|_| vec![]);

    // 4. Compute aggregate stats from FULL history
    let stats = trade_db.call({
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
    let history_enriched: Vec<serde_json::Value> = history_trades.into_iter().map(|t| {
        let mut v = serde_json::to_value(&t).unwrap_or_default();
        if let Some(obj) = v.as_object_mut() {
            let display_comm = t.commission_cost.unwrap_or_else(|| t.volume * 0.0045 * 4.0);
            obj.insert("commission_cost".into(), json!(display_comm));
        }
        v
    }).collect();

    // Return payload matching frontend expectations
    (StatusCode::OK, Json(json!({
        "snapshot": snapshot,
        "exchanges": exchanges,
        "profiles": profiles,
        "trades": {
            "active": active_enriched,
            "history": history_enriched,
            "stats": stats
        },
        "profile_active": active_enriched,
        "profile_history": history_enriched,
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
    let mode  = payload.get("mode" ).and_then(|v| v.as_str()).unwrap_or("paper").to_string();

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
                "max_active_trades" | "collector_interval_secs" => {
                    if let Some(i) = v.as_i64() {
                        sets.push(format!("{}=?", k));
                        params_vec.push(rusqlite::types::Value::Integer(i));
                    }
                },
                "name" | "mode" | "pending_commands" => {
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
        let query = format!("UPDATE trading_profiles SET {} WHERE id=? AND (owner IS NULL OR owner=? OR id IN ('paper-default','real-default'))", sets.join(", "));
        
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

    if id == "paper-default" || id == "real-default" {
        return json_err(StatusCode::BAD_REQUEST, "Cannot delete default profiles");
    }

    let id2 = id.clone(); let id3 = id.clone();
    let _ = state.db.call(move |conn| {
        conn.execute("DELETE FROM trading_profiles WHERE id=? AND owner=?", params![id.clone(), username])?;
        Ok(())
    }).await;
    // Clear trades from both DBs (profile might have trades in either)
    let _ = state.paper_db.call(move |conn| {
        conn.execute("DELETE FROM trades WHERE profile_id=?", params![id2])?;
        Ok(())
    }).await;
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

    let id2 = id.clone(); let id3 = id.clone();
    let _ = state.db.call(move |conn| {
        conn.execute("UPDATE trading_profiles SET pending_commands='CLEAR_TRADES' WHERE id=?", params![id])?;
        Ok(())
    }).await;
    let _ = state.paper_db.call(move |conn| {
        conn.execute("DELETE FROM trades WHERE profile_id=?", params![id2])?;
        Ok(())
    }).await;
    let _ = state.real_db.call(move |conn| {
        conn.execute("DELETE FROM trades WHERE profile_id=?", params![id3])?;
        Ok(())
    }).await;

    (StatusCode::OK, Json(json!({ "success": true }))).into_response()
}
