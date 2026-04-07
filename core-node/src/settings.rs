//! core-node settings module — loads profiles from vps-api.db
//!
//! The canonical TradingSettings struct lives in the `shared` crate.
//! This module only handles SQLite ↔ struct mapping.

pub use shared::TradingSettings;

use tokio_rusqlite::Connection;

// Explicit SELECT column list — NEVER use SELECT * because column indices
// depend on the physical schema order on VPS (may differ from code expectations
// if columns were added via ALTER TABLE historically).
//
// NOTE: disabled_exchanges is intentionally excluded — it may not exist on older DBs.
// It is fetched separately in parse_row with a safe fallback.
const PROFILE_SELECT: &str = "
    SELECT
        id,
        name,
        mode,
        is_active,
        min_entry_spread,
        max_entry_spread,
        trade_volume,
        take_profit_percent,
        trailing_stop_percent,
        max_active_trades,
        commission_rate,
        slippage_percent,
        funding_rate_8h,
        min_volume_millions,
        collector_interval_secs,
        pending_commands,
        disabled_exchanges,
        leverage,
        margin_mode
    FROM trading_profiles
";

/// Load settings for a given mode ('real' | 'paper') from the profiles DB.
pub async fn load_settings(db: &Connection, mode: &str) -> TradingSettings {
    let mode = mode.to_string();
    db.call(move |conn| {
        let sql = format!("{} WHERE mode=? ORDER BY is_active DESC LIMIT 1", PROFILE_SELECT);
        let result = conn.query_row(&sql, rusqlite::params![mode], |row| Ok(parse_row(row)));
        Ok(result.unwrap_or_default())
    }).await.unwrap_or_default()
}

/// Load ALL profiles for a given mode — used to run a trade engine cycle per profile.
pub async fn load_all_settings(db: &Connection, mode: &str) -> Vec<TradingSettings> {
    let mode = mode.to_string();
    db.call(move |conn| {
        let sql = format!("{} WHERE mode=? AND is_active=1", PROFILE_SELECT);
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params![mode], |row| Ok(parse_row(row)))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }).await.unwrap_or_default()
}

fn get_f64(row: &rusqlite::Row, idx: usize) -> Option<f64> {
    match row.get::<_, f64>(idx) {
        Ok(v) => Some(v),
        Err(_) => row.get::<_, i64>(idx).ok().map(|v| v as f64),
    }
}

fn get_i64(row: &rusqlite::Row, idx: usize) -> Option<i64> {
    match row.get::<_, i64>(idx) {
        Ok(v) => Some(v),
        Err(_) => row.get::<_, f64>(idx).ok().map(|v| v as i64),
    }
}

// Column indices match the PROFILE_SELECT list above (0-based).
fn parse_row(row: &rusqlite::Row) -> TradingSettings {
    // Parse disabled_exchanges JSON string from DB (col 16) into Vec<String>.
    // Falls back to empty vec on any parse error.
    let disabled_exchanges: Vec<String> = row
        .get::<_, Option<String>>(16)
        .ok()
        .flatten()
        .and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok())
        .unwrap_or_default();

    TradingSettings {
        id:                      row.get(0).unwrap_or_else(|_| "default".to_string()),
        // col 1 = name (unused in TradingSettings)
        // col 2 = mode (unused in TradingSettings)
        is_active:               get_i64(row, 3).unwrap_or(0) != 0,
        min_entry_spread:        get_f64(row, 4).unwrap_or(0.5),
        max_entry_spread:        get_f64(row, 5).unwrap_or(10.0),
        trade_volume:            get_f64(row, 6).unwrap_or(10.0),
        take_profit_percent:     get_f64(row, 7).unwrap_or(0.2),
        trailing_stop_percent:   get_f64(row, 8).unwrap_or(0.3),
        max_active_trades:       get_i64(row, 9).unwrap_or(3),
        commission_rate:         get_f64(row, 10).unwrap_or(0.05),
        slippage_percent:        get_f64(row, 11).unwrap_or(0.05),
        funding_rate_8h:         get_f64(row, 12).unwrap_or(0.01),
        min_volume_millions:     get_f64(row, 13).unwrap_or(10.0),
        collector_interval_secs: get_i64(row, 14).unwrap_or(5),
        pending_commands:        row.get(15).ok().flatten(),
        disabled_exchanges,
        leverage:                get_i64(row, 17).unwrap_or(10),
        margin_mode:             row.get::<_, Option<String>>(18).ok().flatten().unwrap_or_else(|| "cross".to_string()),
    }
}

/// Spawn background task refreshing ALL profiles from DB every `interval_secs`.
pub fn spawn_all_settings_refresher(
    db: Connection,
    mode: &'static str,
    settings: std::sync::Arc<tokio::sync::RwLock<Vec<TradingSettings>>>,
    interval_secs: u64,
) {
    tokio::spawn(async move {
        let m = mode.to_string();
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
            let fresh = load_all_settings(&db, &m).await;
            *settings.write().await = fresh;
        }
    });
}
