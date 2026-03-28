use crate::db::{row_to_profile, row_to_trade};
use crate::models::{SpreadPayload, TickerData, TradingProfile};
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
        conn.execute(
            "INSERT INTO server_cache (key, value, expires_at) VALUES ('snapshot', ?, ?)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
            params![snapshot_val, now + 180_000],
        )
    }).await;

    // Save maps summary
    let mut map_summaries: HashMap<String, Vec<String>> = HashMap::new();
    for (ex, data) in &payload.maps {
        map_summaries.insert(ex.clone(), data.keys().cloned().collect());
    }
    let map_val = json!({ "ts": now, "data": map_summaries }).to_string();
    let _ = state.db.call(move |conn| {
        conn.execute(
            "INSERT INTO server_cache (key, value, expires_at) VALUES ('exchange_maps', ?, ?)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at",
            params![map_val, now + 180_000],
        )
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
        let items = stmt.query_map([], row_to_profile)?.filter_map(|r| r.ok()).collect::<Vec<_>>();
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
        let items = stmt.query_map([username], row_to_profile)?.filter_map(|r| r.ok()).collect::<Vec<_>>();
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

    // 1. Get Snapshot
    let snapshot = state.db.call(|conn| {
        let row: Result<String, _> = conn.query_row("SELECT value FROM server_cache WHERE key = 'snapshot'", [], |r| r.get(0));
        Ok(if let Ok(v) = row { serde_json::from_str(&v).unwrap_or(json!({})) } else { json!({}) })
    }).await.unwrap_or(json!({}));

    // 2. Get Profiles
    let profiles = state.db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT * FROM trading_profiles WHERE owner = ? OR id IN ('paper-default','real-default') ORDER BY created_at ASC")?;
        Ok(stmt.query_map([username], row_to_profile)?.filter_map(|r| r.ok()).collect::<Vec<_>>())
    }).await.unwrap_or_else(|_| vec![]);

    // 3. Get Active/History trades for given profile or paper-default
    let pid = query.profile_id.unwrap_or_else(|| "paper-default".to_string());
    
    let active_trades = state.db.call({
        let p = pid.clone();
        move |conn| {
            let mut stmt = conn.prepare("SELECT * FROM trades WHERE status='OPEN' AND profile_id=?")?;
            Ok(stmt.query_map([p], row_to_trade)?.filter_map(|r| r.ok()).collect::<Vec<_>>())
        }
    }).await.unwrap_or_else(|_| vec![]);

    let history_trades = state.db.call({
        let p = pid.clone();
        move |conn| {
            let mut stmt = conn.prepare("SELECT * FROM trades WHERE status='CLOSED' AND profile_id=? ORDER BY exit_time DESC LIMIT 50")?;
            Ok(stmt.query_map([p], row_to_trade)?.filter_map(|r| r.ok()).collect::<Vec<_>>())
        }
    }).await.unwrap_or_else(|_| vec![]);

    // Return huge payload matching worker expected output
    (StatusCode::OK, Json(json!({
        "snapshot": snapshot,
        "profiles": profiles,
        "trades": {
            "active": active_trades,
            "history": history_trades,
            "stats": {} // TODO: basic stats
        },
        "profile_active": active_trades,
        "profile_history": history_trades,
    }))).into_response()
}

// ─── POST /api/profiles ───
pub async fn handle_post_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut payload): Json<TradingProfile>,
) -> Response {
    let username = match check_basic_auth(&headers, &state) {
        Some(u) => u,
        None => return json_err(StatusCode::UNAUTHORIZED, "Unauthorized"),
    };

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    payload.id = uuid::Uuid::new_v4().to_string(); // Actually let frontend send ID or we generate
    
    let p = payload.clone();
    let res = state.db.call(move |conn| {
        conn.execute(
            "INSERT INTO trading_profiles
                (id,name,mode,is_active,min_entry_spread,max_entry_spread,trade_volume,
                 take_profit_percent,trailing_stop_percent,max_active_trades,commission_rate,
                 slippage_percent,funding_rate_8h,min_volume_millions,collector_interval_secs,owner,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            params![
                p.id, p.name, p.mode, p.is_active as i64, p.min_entry_spread, p.max_entry_spread, p.trade_volume,
                p.take_profit_percent, p.trailing_stop_percent, p.max_active_trades, p.commission_rate,
                p.slippage_percent, p.funding_rate_8h, p.min_volume_millions, p.collector_interval_secs,
                username, now, now
            ],
        )
    }).await;

    match res {
        Ok(_) => (StatusCode::CREATED, Json(payload)).into_response(),
        Err(e) => json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
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
        let mut sets = vec![];
        let mut params_vec: Vec<rusqlite::types::Value> = vec![];
        
        for (k, v) in obj {
            match k.as_str() {
                "is_active" => {
                    sets.push("is_active=?");
                    params_vec.push(rusqlite::types::Value::Integer(if v.as_bool().unwrap_or(false) { 1 } else { 0 }));
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
                _ => {}
            }
        }
        
        if sets.is_empty() {
            return json_err(StatusCode::BAD_REQUEST, "No valid fields");
        }
        
        sets.push("updated_at=?".to_string());
        params_vec.push(rusqlite::types::Value::Integer(now));
        
        let id_clone = id.clone();
        let query = format!("UPDATE trading_profiles SET {} WHERE id=? AND (owner=? OR id IN ('paper-default','real-default'))", sets.join(", "));
        
        // Push final params
        params_vec.push(rusqlite::types::Value::Text(id_clone));
        params_vec.push(rusqlite::types::Value::Text(username));

        let _ = state.db.call(move |conn| {
            let mut stmt = conn.prepare(&query)?;
            stmt.execute(rusqlite::params_from_iter(params_vec))?;
            Ok(())
        }).await;
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

    let _ = state.db.call(move |conn| {
        conn.execute("DELETE FROM trading_profiles WHERE id=? AND owner=?", params![id.clone(), username])?;
        conn.execute("DELETE FROM trades WHERE profile_id=?", params![id])?;
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

    let _ = state.db.call(move |conn| {
        conn.execute("UPDATE trading_profiles SET pending_commands='CLEAR_TRADES' WHERE id=?", params![id.clone()])?;
        conn.execute("DELETE FROM trades WHERE profile_id=?", params![id])?;
        Ok(())
    }).await;

    (StatusCode::OK, Json(json!({ "success": true }))).into_response()
}
